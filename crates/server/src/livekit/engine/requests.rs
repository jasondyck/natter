//! The client's `SignalRequest`s: SDP and trickle, publishing, muting,
//! subscribing and permissions, as LiveKit's `rtc.ParticipantImpl` handles
//! them.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use livekit_protocol as lk;
use lk::{leave_request, request_response, signal_request, signal_response};
use str0m::change::{SdpAnswer, SdpOffer};
use str0m::media::{Direction, MediaKind, Mid, Pt};
use str0m::{Candidate, CandidateKind};
use tracing::{debug, info, warn};

use super::{
    ConnId, DownState, Engine, Participant, Prefs, Side, StreamState, dynacast_codecs, leave,
    negotiated_pts, new_downtrack, refuse, sdk_name, stop_downtrack, subscribed, track_info,
    variant_name,
};
use crate::livekit::fallback::{self, Pref};
use crate::livekit::ids;
use crate::livekit::sdp::msid_tracks;
use crate::livekit::video;
use crate::livekit::webhook;

impl Engine {
    pub(super) fn signal(&mut self, i: usize, msg: signal_request::Message) {
        use signal_request::Message as M;
        debug!(identity = %self.participants[i].identity, "signal in: {}", variant_name(&msg));
        match msg {
            M::Offer(sd) => self.publisher_offer(i, sd),
            M::Answer(sd) => self.subscriber_answer(i, sd),
            M::Trickle(t) => self.trickle(i, t),
            M::AddTrack(req) => self.add_track(i, req),
            M::Mute(req) => self.mute(i, req),
            M::Subscription(req) => self.subscription(i, req),
            M::Leave(_) => self.disconnect(i, lk::DisconnectReason::ClientInitiated),
            M::UpdateMetadata(req) => self.update_metadata(i, req),
            // Video layer and adaptive-stream settings come with video.
            M::TrackSetting(req) => self.track_settings(i, req),
            M::PingReq(ping) => {
                self.participants[i].signal_rtt = Some(Duration::from_millis(ping.rtt as u64));
            }
            M::UpdateLayers(_) | M::UpdateVideoTrack(_) => {}
            M::SyncState(state) => self.sync_state(i, state),
            M::SubscriptionPermission(req) => self.subscription_permission(i, req),
            M::UpdateAudioTrack(_) => {}
            M::PublishDataTrackRequest(req) => self.publish_data_track(i, req),
            M::UnpublishDataTrackRequest(req) => self.unpublish_data_track(i, req),
            M::UpdateDataSubscription(req) => self.update_data_subscription(i, req),
            // Data blobs are off, as by default in LiveKit
            // (`enable_participant_data_blob`): nothing is ever stored.
            M::StoreDataBlobRequest(req) => {
                self.participants[i].reply(
                    req.request_id,
                    request_response::Reason::NotAllowed,
                    "data blob not enabled",
                );
            }
            M::GetDataBlobRequest(req) => {
                let (reason, message) = if req.key.is_none() {
                    (
                        request_response::Reason::InvalidRequest,
                        "data blob key is required",
                    )
                } else if !self.rooms[&self.participants[i].room]
                    .members
                    .iter()
                    .any(|&j| self.participants[j].identity == req.participant_identity)
                {
                    (request_response::Reason::NotFound, "participant not found")
                } else {
                    (request_response::Reason::NotFound, "data blob not found")
                };
                self.participants[i].reply(req.request_id, reason, message);
            }
            other => debug!(
                identity = %self.participants[i].identity,
                "unsupported signal request: {}",
                signal_name(&other)
            ),
        }
    }

    /// What a resuming client says it has (LiveKit's `onSyncState`): its
    /// publications must all be known here, or it starts over with a full
    /// reconnect; then its disabled tracks and subscriptions apply.
    fn sync_state(&mut self, i: usize, state: lk::SyncState) {
        let p = &self.participants[i];
        let known = |sid: &str| {
            p.published.iter().any(|t| t.info.sid == sid)
                || p.pending_tracks.iter().any(|(s, _)| s == sid)
        };
        let unknown = state
            .publish_tracks
            .iter()
            .map(|t| t.track.as_ref().map(|ti| ti.sid.as_str()))
            .find(|sid| sid.is_none_or(|s| !known(s)));
        let unknown_data = state
            .publish_data_tracks
            .iter()
            .map(|t| t.info.as_ref().map(|i| i.sid.as_str()))
            .find(|sid| sid.is_none_or(|s| !p.data_tracks.iter().any(|t| t.info.sid == s)))
            .map(|_| "data track");
        if let Some(what) = unknown
            .map(|s| s.unwrap_or("track without info"))
            .or(unknown_data)
        {
            warn!(identity = %p.identity, track = what, "unknown publication on resume; full reconnect");
            let p = &mut self.participants[i];
            p.send(leave(
                lk::DisconnectReason::StateMismatch,
                leave_request::Action::Reconnect,
            ));
            p.closing = true;
            self.reap_soon = true;
            return;
        }
        if !state.track_sids_disabled.is_empty() {
            self.track_settings(
                i,
                lk::UpdateTrackSettings {
                    track_sids: state.track_sids_disabled,
                    disabled: true,
                    ..Default::default()
                },
            );
        }
        if let Some(sub) = state.subscription {
            self.subscription(i, sub);
        }
    }

    fn publisher_offer(&mut self, i: usize, sd: lk::SessionDescription) {
        let p = &mut self.participants[i];
        let offer = match SdpOffer::from_sdp_string(&fallback::filter_sdp(&sd.sdp, p.fallback.pref))
        {
            Ok(o) => o,
            Err(e) => {
                warn!(identity = %p.identity, "bad publisher offer: {e}");
                return;
            }
        };
        p.offered_tracks = if sd.mid_to_track_id.is_empty() {
            msid_tracks(&sd.sdp)
        } else {
            sd.mid_to_track_id.clone()
        };
        match p.publisher.rtc.sdp_api().accept_offer(offer) {
            Ok(answer) => {
                p.publisher.dirty = true;
                // Tracks whose m-line the client no longer sends on: it
                // unpublished them.
                let stopped: Vec<String> = p
                    .published
                    .iter()
                    .filter(|t| t.backup_of.is_none())
                    .filter(|t| {
                        t.mid.is_some_and(|m| {
                            p.publisher.rtc.media(m).is_none_or(|media| {
                                !matches!(
                                    media.direction(),
                                    Direction::RecvOnly | Direction::SendRecv
                                )
                            })
                        })
                    })
                    .map(|t| t.info.sid.clone())
                    .collect();
                for sid in stopped {
                    self.unpublish(i, &sid, false);
                }
                let p = &mut self.participants[i];
                if p.single_pc() {
                    p.slots = recv_slots(&sd.sdp);
                    let pts = negotiated_pts(&p.publisher.rtc);
                    open_negotiated(&self.metrics.lk, p, pts);
                    p.reoffer_requested = false;
                    p.sub_pending_since = None;
                }
                let mut sdp = answer.to_sdp_string();
                // Each video track in the codec its add_track asked for.
                for (mid, cid) in &p.offered_tracks {
                    let asked = p
                        .pending_tracks
                        .iter()
                        .find(|(_, r)| &r.cid == cid && r.r#type() == lk::TrackType::Video)
                        .and_then(|(_, r)| r.simulcast_codecs.first())
                        .map(|c| c.codec.clone())
                        .or_else(|| {
                            p.published
                                .iter()
                                .find(|t| t.kind == MediaKind::Video && t.info.mid == *mid)
                                .map(|t| t.info.mime_type.clone())
                        });
                    if let Some(codec) = asked.filter(|c| !c.is_empty()) {
                        let name = codec.rsplit('/').next().unwrap_or(&codec);
                        sdp = crate::livekit::sdp::prefer_codec(&sdp, mid, name);
                    }
                }
                // One-shot (WHEP): every m-line stays open, as there is no
                // later offer to open one in; tracks fill them as they come.
                if p.one_shot {
                    self.touched.push((i, Side::Subscriber));
                } else if p.single_pc() {
                    let used: HashSet<Mid> = p
                        .downtracks
                        .iter()
                        .filter_map(|d| match d.state {
                            DownState::Open(m) | DownState::ToStop(m) => Some(m),
                            _ => None,
                        })
                        .collect();
                    let free: Vec<String> = p
                        .slots
                        .iter()
                        .filter(|(m, _)| !used.contains(m))
                        .map(|(m, _)| m.to_string())
                        .collect();
                    sdp = crate::livekit::sdp::inactive_slots(&sdp, &free);
                }
                let sdp = fallback::filter_sdp(&sdp, p.fallback.pref);
                debug!(identity = %p.identity, %sdp, "publisher answer");
                p.send(signal_response::Message::Answer(lk::SessionDescription {
                    r#type: "answer".into(),
                    sdp,
                    id: sd.id,
                    ..Default::default()
                }));
                self.touched.push((i, Side::Publisher));
            }
            Err(e) => warn!(identity = %p.identity, "publisher offer rejected: {e}"),
        }
    }

    fn subscriber_answer(&mut self, i: usize, sd: lk::SessionDescription) {
        let p = &mut self.participants[i];
        let Some(pending) = p.sub_pending.take() else {
            warn!(identity = %p.identity, "subscriber answer with no offer pending");
            return;
        };
        p.sub_pending_since = None;
        let answer =
            match SdpAnswer::from_sdp_string(&fallback::filter_sdp(&sd.sdp, p.fallback.pref)) {
                Ok(a) => a,
                Err(e) => {
                    warn!(identity = %p.identity, "bad subscriber answer: {e}");
                    sub!(p).rtc.disconnect();
                    return;
                }
            };
        if let Err(e) = sub!(p).rtc.sdp_api().accept_answer(pending, answer) {
            warn!(identity = %p.identity, "subscriber answer rejected: {e}");
            sub!(p).rtc.disconnect();
            return;
        }
        sub!(p).dirty = true;
        let pts = negotiated_pts(&sub!(p).rtc);
        open_negotiated(&self.metrics.lk, p, pts);
        self.touched.push((i, Side::Subscriber));
    }

    fn trickle(&mut self, i: usize, t: lk::TrickleRequest) {
        #[derive(serde::Deserialize)]
        struct Init {
            #[serde(default)]
            candidate: String,
        }
        let Ok(init) = serde_json::from_str::<Init>(&t.candidate_init) else {
            return;
        };
        if init.candidate.is_empty() {
            return;
        }
        let Ok(candidate) = Candidate::from_sdp_string(&init.candidate) else {
            debug!("unparsable candidate {}", init.candidate);
            return;
        };
        let side = if t.target == lk::SignalTarget::Subscriber as i32 {
            Side::Subscriber
        } else {
            Side::Publisher
        };
        let p = &mut self.participants[i];
        if p.fallback.pref == Pref::Tcp && !fallback::is_tcp(&init.candidate) {
            return;
        }
        let tr = p.transport(side);
        if candidate.kind() == CandidateKind::Relayed {
            tr.relays.push(candidate.addr());
        }
        tr.rtc.add_remote_candidate(candidate);
        tr.dirty = true;
        self.touched.push((i, side));
    }

    fn add_track(&mut self, i: usize, req: lk::AddTrackRequest) {
        if let Ok(kind @ (lk::TrackType::Audio | lk::TrackType::Video)) =
            lk::TrackType::try_from(req.r#type)
        {
            self.metrics
                .lk
                .publish_counter
                .inc(&[kind.as_str_name(), "attempt"]);
        }
        let p = &mut self.participants[i];
        let source = req.source();
        if !p.grant.can_publish_source(source) {
            p.reply(
                0,
                request_response::Reason::NotAllowed,
                "not allowed to publish this source",
            );
            return;
        }
        let kind = req.r#type();
        if kind == lk::TrackType::Data {
            p.reply(
                0,
                request_response::Reason::UnsupportedType,
                "data tracks are not supported yet",
            );
            return;
        }
        // A backup codec for a published track (the client publishes it
        // when dynacast asks for that codec): answered with the track as it
        // is, and bound to its m-line as that track's backup.
        if let Some(t) = p
            .published
            .iter()
            .find(|t| !req.sid.is_empty() && t.info.sid == req.sid && t.backup_of.is_none())
        {
            let mut req = req;
            if let Some(backup) = t.info.codecs.get(1) {
                req.simulcast_codecs = vec![lk::SimulcastCodec {
                    codec: backup.mime_type.clone(),
                    cid: req.cid.clone(),
                    ..Default::default()
                }];
            }
            p.send(signal_response::Message::TrackPublished(
                lk::TrackPublishedResponse {
                    cid: req.cid.clone(),
                    track: Some(t.info.clone()),
                },
            ));
            p.pending_tracks.push((req.sid.clone(), req));
            return;
        }
        let sid = if req.sid.is_empty() {
            ids::track(kind, source)
        } else {
            req.sid.clone()
        };
        let info = track_info(&sid, &req, None, None, p.obs);
        p.send(signal_response::Message::TrackPublished(
            lk::TrackPublishedResponse {
                cid: req.cid.clone(),
                track: Some(info),
            },
        ));
        p.publish_asked.insert(sid.clone(), Instant::now());
        p.pending_tracks.push((sid, req));
    }

    pub(super) fn mute(&mut self, i: usize, req: lk::MuteTrackRequest) {
        let p = &mut self.participants[i];
        // The backup codec's stream mutes with its track.
        for b in p
            .published
            .iter_mut()
            .filter(|t| t.backup_of.as_ref() == Some(&req.sid))
        {
            b.info.muted = req.muted;
        }
        let Some(t) = p
            .published
            .iter_mut()
            .find(|t| t.info.sid == req.sid && t.backup_of.is_none())
        else {
            return;
        };
        if t.info.muted == req.muted {
            return;
        }
        t.info.muted = req.muted;
        t.scorer.set_muted(req.muted);
        let (sid, subscribers) = (t.info.sid.clone(), t.subscribers.clone());
        // Unmuted video: subscribers start again at a keyframe, and the
        // publisher hears again which qualities are wanted (LiveKit's
        // ForceUpdate), since it may have restarted every encoding.
        if !req.muted && t.kind == MediaKind::Video {
            let update = video::subscribed_quality(&sid, &dynacast_codecs(t));
            t.dynacast_lower = None;
            p.send(signal_response::Message::SubscribedQualityUpdate(update));
            for conn in subscribers {
                if let Some(&j) = self.by_conn.get(&conn)
                    && let Some(d) = self.participants[j]
                        .downtracks
                        .iter_mut()
                        .find(|d| d.track_sid == sid)
                {
                    d.layer = None;
                    d.stream_state = StreamState::Inactive;
                }
            }
        }
        let p = &mut self.participants[i];
        p.version += 1;
        let room = p.room.clone();
        self.participant_update(&room, i);
        let p = &self.participants[i];
        if let Some(t) = p
            .published
            .iter()
            .find(|t| t.info.sid == sid && t.backup_of.is_none())
        {
            let event = if req.muted {
                webhook::TRACK_MUTED
            } else {
                webhook::TRACK_UNMUTED
            };
            let (who, info) = (p.who(), t.info.clone());
            self.hook(event, &room, Some(who), Some(info));
        }
    }

    /// Ends a publication: its subscribers lose it and the room hears.
    /// With `tell`, the publisher gets `TrackUnpublished` (an admin
    /// revoked it; a client that unpublished knows).
    pub(super) fn unpublish(&mut self, i: usize, sid: &str, tell: bool) {
        let p = &mut self.participants[i];
        let mut subscribers = vec![];
        let mut removed = None;
        // The track and its backup codec's stream.
        p.published.retain(|t| {
            let ours = t.info.sid == sid;
            if ours {
                subscribers.extend(t.subscribers.iter().copied());
                if t.backup_of.is_none() {
                    removed = Some(t.info.clone());
                }
            }
            !ours
        });
        let Some(removed) = removed else {
            return;
        };
        // It holds indices into `published`: built again as packets come.
        p.ssrc_track.clear();
        let who = p.who();
        info!(identity = %p.identity, track = %sid, tell, "unpublished");
        if tell {
            p.send(signal_response::Message::TrackUnpublished(
                lk::TrackUnpublishedResponse {
                    track_sid: sid.to_owned(),
                },
            ));
        }
        p.version += 1;
        let room = p.room.clone();
        subscribers.sort_unstable();
        subscribers.dedup();
        for conn in subscribers {
            if let Some(&j) = self.by_conn.get(&conn) {
                stop_downtrack(&mut self.participants[j], sid);
                self.touched.push((j, Side::Subscriber));
            }
        }
        let publisher = self.participants[i].conn;
        for &j in &self.rooms[&room].members {
            self.participants[j]
                .refused
                .retain(|(t, c)| !(t == sid && *c == publisher));
        }
        self.participant_update(&room, i);
        self.room_update(&room);
        self.hook(webhook::TRACK_UNPUBLISHED, &room, Some(who), Some(removed));
    }

    /// A subscriber's settings for tracks (adaptive stream: shown or not,
    /// and at what size), kept for tracks it subscribes to later.
    fn track_settings(&mut self, i: usize, req: lk::UpdateTrackSettings) {
        let p = &mut self.participants[i];
        for sid in &req.track_sids {
            p.track_settings.insert(sid.clone(), req.clone());
        }
        let wanted: Vec<(usize, ConnId, String)> = p
            .downtracks
            .iter()
            .enumerate()
            .filter(|(_, d)| d.kind == MediaKind::Video && req.track_sids.contains(&d.track_sid))
            .map(|(k, d)| (k, d.publisher, d.track_sid.clone()))
            .collect();
        for (k, publisher, sid) in wanted {
            let Some(&pi) = self.by_conn.get(&publisher) else {
                continue;
            };
            let Some(t) = self.participants[pi]
                .published
                .iter()
                .find(|t| t.info.sid == sid)
            else {
                continue;
            };
            let max = video::max_layer(&t.info.layers, &req);
            let d = &mut self.participants[i].downtracks[k];
            if d.max_layer != max || d.fps != req.fps {
                debug!(track = %sid, ?max, fps = req.fps, "subscriber settings");
                d.max_layer = max;
                d.fps = req.fps;
            }
        }
    }

    pub(super) fn subscription(&mut self, i: usize, req: lk::UpdateSubscription) {
        let mut sids: HashSet<String> = req.track_sids.into_iter().collect();
        for pt in req.participant_tracks {
            sids.extend(pt.track_sids);
        }
        let room = self.participants[i].room.clone();
        let conn = self.participants[i].conn;
        if req.subscribe && !self.participants[i].grant.can_subscribe() {
            return;
        }
        let members = self.rooms[&room].members.clone();
        let prefs = Prefs::of(&self.participants[i]);
        let mut add = vec![];
        let mut remove = vec![];
        let mut refused = vec![];
        for j in members {
            if j == i {
                continue;
            }
            let publisher_sid = self.participants[j].sid.clone();
            let publisher_conn = self.participants[j].conn;
            let (pubr, me) = (&self.participants[j], &self.participants[i]);
            let denied: Vec<String> = pubr
                .published
                .iter()
                .filter(|t| sids.contains(&t.info.sid) && t.mid.is_some() && t.backup_of.is_none())
                .filter(|t| !pubr.permits(&t.info.sid, me))
                .map(|t| t.info.sid.clone())
                .collect();
            for t in self.participants[j].published.iter_mut() {
                if !sids.contains(&t.info.sid) || t.mid.is_none() || t.backup_of.is_some() {
                    continue;
                }
                let has = t.subscribers.contains(&conn);
                if req.subscribe && !has && denied.contains(&t.info.sid) {
                    refused.push((t.info.sid.clone(), publisher_sid.clone(), publisher_conn));
                } else if req.subscribe && !has {
                    t.subscribers.push(conn);
                    add.push(new_downtrack(t, &publisher_sid, publisher_conn, &prefs));
                    self.metrics.lk.subscribe_counter.inc(&["attempt", ""]);
                } else if !req.subscribe && has {
                    t.subscribers.retain(|&c| c != conn);
                    remove.push(t.info.sid.clone());
                }
            }
        }
        let p = &mut self.participants[i];
        p.downtracks.extend(add);
        for sid in remove {
            stop_downtrack(p, &sid);
        }
        if !req.subscribe {
            p.refused.retain(|(t, _)| !sids.contains(t));
        }
        for (sid, publisher_sid, publisher_conn) in refused {
            refuse(p, sid, &publisher_sid, publisher_conn);
        }
        self.touched.push((i, Side::Subscriber));
    }

    /// A publisher's `SubscriptionPermission` (LiveKit's
    /// `UpdateSubscriptionPermission`): every update replaces the last.
    /// Subscribers it now refuses lose those tracks; refused ones it now
    /// allows get them.
    fn subscription_permission(&mut self, i: usize, req: lk::SubscriptionPermission) {
        let room = self.participants[i].room.clone();
        let members = self.rooms[&room].members.clone();
        let perms = if req.all_participants {
            None
        } else {
            let mut m = HashMap::new();
            for tp in req.track_permissions {
                let identity = if tp.participant_identity.is_empty() {
                    if tp.participant_sid.is_empty() {
                        // LiveKit refuses the whole update and keeps the
                        // permissions it had.
                        warn!(identity = %self.participants[i].identity, "subscription permission without identity or sid");
                        return;
                    }
                    let Some(&j) = members
                        .iter()
                        .find(|&&j| self.participants[j].sid == tp.participant_sid)
                    else {
                        debug!(sid = %tp.participant_sid, "subscription permission for unknown participant");
                        continue;
                    };
                    self.participants[j].identity.clone()
                } else {
                    tp.participant_identity.clone()
                };
                m.insert(identity, tp);
            }
            Some(m)
        };
        self.participants[i].sub_permission = perms;

        let publisher_sid = self.participants[i].sid.clone();
        let publisher_conn = self.participants[i].conn;
        for j in members {
            if j == i {
                continue;
            }
            let (pubr, sub) = (&self.participants[i], &self.participants[j]);
            let mut revoke = vec![];
            let mut grant = vec![];
            for t in pubr
                .published
                .iter()
                .filter(|t| t.mid.is_some() && t.backup_of.is_none())
            {
                let allowed = pubr.permits(&t.info.sid, sub);
                if !allowed && t.subscribers.contains(&sub.conn) {
                    revoke.push(t.info.sid.clone());
                } else if allowed
                    && sub
                        .refused
                        .iter()
                        .any(|(s, c)| s == &t.info.sid && *c == pubr.conn)
                {
                    grant.push(t.info.sid.clone());
                }
            }
            if revoke.is_empty() && grant.is_empty() {
                continue;
            }
            let conn = self.participants[j].conn;
            let prefs = Prefs::of(&self.participants[j]);
            let mut add = vec![];
            for t in self.participants[i].published.iter_mut() {
                if t.backup_of.is_some() {
                    continue;
                }
                if revoke.contains(&t.info.sid) {
                    t.subscribers.retain(|&c| c != conn);
                } else if grant.contains(&t.info.sid) {
                    t.subscribers.push(conn);
                    add.push(new_downtrack(t, &publisher_sid, publisher_conn, &prefs));
                    self.metrics.lk.subscribe_counter.inc(&["attempt", ""]);
                }
            }
            let s = &mut self.participants[j];
            info!(identity = %s.identity, publisher = %publisher_sid, revoked = revoke.len(), granted = grant.len(), "subscription permission changed");
            for sid in revoke {
                stop_downtrack(s, &sid);
                refuse(s, sid, &publisher_sid, publisher_conn);
            }
            s.refused.retain(|(t, _)| !grant.contains(t));
            s.downtracks.extend(add);
            self.touched.push((j, Side::Subscriber));
        }
        // Its data tracks too (LiveKit's `maybeRevokeDataTrackSubscriptions`
        // and the reconcile that follows).
        for j in self.rooms[&room].members.clone() {
            if j != i && !self.participants[j].data_wants.is_empty() {
                self.reconcile_data(j);
            }
        }
    }

    fn update_metadata(&mut self, i: usize, req: lk::UpdateParticipantMetadata) {
        let p = &mut self.participants[i];
        if !p.grant.can_update_own_metadata() {
            p.reply(
                req.request_id,
                request_response::Reason::NotAllowed,
                "does not have permission to update own metadata",
            );
            return;
        }
        p.apply_info(req.name, req.metadata, req.attributes);
        p.version += 1;
        p.reply(req.request_id, request_response::Reason::Ok, "");
        let room = p.room.clone();
        self.participant_update(&room, i);
    }

    // ---- Transport events -----------------------------------------------
}

/// A subscription is up: LiveKit's subscribe counter and time.
/// The subscriber side's answer is in: what it negotiated opens, what it
/// stopped goes.
pub(super) fn open_negotiated(
    lk: &crate::livekit::prom::LkMetrics,
    p: &mut Participant,
    (opus_pt, red_pt): (Option<Pt>, Option<Pt>),
) {
    let (sdk, pkind) = (sdk_name(&p.client), p.kind.as_str_name());
    for d in &mut p.downtracks {
        if let DownState::Negotiating(m) = d.state {
            d.state = DownState::Open(m);
            subscribed(lk, d, sdk, pkind);
            // str0m keeps RED on the Opus params only if both sides
            // negotiated it.
            d.opus_pt = opus_pt;
            d.red_pt = red_pt.filter(|_| d.kind == MediaKind::Audio);
        }
    }
    p.downtracks
        .retain(|d| !matches!(d.state, DownState::NegotiatingStop(_)));
}

/// The m-lines a client offered as receive-only, by mid and kind.
fn recv_slots(sdp: &str) -> Vec<(Mid, MediaKind)> {
    let mut out = vec![];
    let mut kind = None;
    let mut mid = None;
    let mut recvonly = false;
    let mut flush = |kind: Option<MediaKind>, mid: Option<Mid>, recvonly: bool| {
        if let (Some(k), Some(m), true) = (kind, mid, recvonly) {
            out.push((m, k));
        }
    };
    for line in sdp.lines().map(str::trim_end) {
        if let Some(rest) = line.strip_prefix("m=") {
            flush(kind, mid, recvonly);
            kind = match rest.split(' ').next() {
                Some("audio") => Some(MediaKind::Audio),
                Some("video") => Some(MediaKind::Video),
                _ => None,
            };
            mid = None;
            recvonly = false;
        } else if let Some(m) = line.strip_prefix("a=mid:") {
            mid = Some(Mid::from(m));
        } else if line == "a=recvonly" {
            recvonly = true;
        }
    }
    flush(kind, mid, recvonly);
    out
}

fn signal_name(msg: &signal_request::Message) -> &'static str {
    use signal_request::Message as M;
    match msg {
        M::Simulate(_) => "simulate",
        M::Ping(_) | M::PingReq(_) => "ping",
        M::PublishDataTrackRequest(_)
        | M::UnpublishDataTrackRequest(_)
        | M::UpdateDataSubscription(_) => "data_track",
        M::StoreDataBlobRequest(_) | M::GetDataBlobRequest(_) => "data_blob",
        _ => "other",
    }
}
