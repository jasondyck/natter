//! str0m's events for a participant's two peer connections: connection
//! state, media slots opening (`bind_track`), stats and keyframe requests.

use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

use livekit_protocol as lk;
use lk::participant_info;
use str0m::bwe::BweKind;
use str0m::format::Codec;
use str0m::media::{Direction, MediaKind, Mid, Rids};
use str0m::{Event, IceConnectionState};
use tracing::{debug, info};

use super::{
    Engine, Prefs, Published, Side, StreamCounts, negotiated_pts, new_downtrack, refuse, sdk_name,
    source, track_info, type_name,
};
use crate::livekit::audio_level::{self, AudioLevel};
use crate::livekit::fallback;
use crate::livekit::quality::{self, Scorer};
use crate::livekit::red::ToPrimary;
use crate::livekit::rtc_config::Congestion;
use crate::livekit::video;
use crate::livekit::webhook;

impl Engine {
    pub(super) fn event(&mut self, i: usize, side: Side, event: Event) {
        if i >= self.participants.len() {
            return;
        }
        match event {
            Event::RtpPacket(pkt) if side == Side::Publisher => self.forward(i, pkt),
            // A subscriber lost the picture (PLI or FIR): ask the publisher
            // of that track, on the layer it gets, for a keyframe.
            Event::KeyframeRequest(req) => {
                let p = &self.participants[i];
                let Some(d) = p.downtracks.iter().find(|d| d.state.mid() == Some(req.mid)) else {
                    return;
                };
                let Some(layer) = d.layer else {
                    return;
                };
                let (conn, sid) = (d.publisher, d.track_sid.clone());
                let Some(&pi) = self.by_conn.get(&conn) else {
                    return;
                };
                let cannot = self.participants[i]
                    .downtracks
                    .iter()
                    .find(|d| d.track_sid == sid)
                    .is_some_and(|d| d.cannot_primary);
                if let Some(t) = source(&self.participants[pi].published, &sid, cannot) {
                    self.request_keyframe(pi, t, layer, Instant::now());
                }
            }
            Event::Connected => {
                let p = &mut self.participants[i];
                p.transport(side).connected = true;
                if p.one_shot {
                    p.cleanup_at = None;
                }
                info!(identity = %p.identity, ?side, "media connected");
                self.metrics.media_connected_total.fetch_add(1, Relaxed);
                let transport = match side {
                    Side::Publisher => "PUBLISHER",
                    Side::Subscriber => "SUBSCRIBER",
                };
                for state in ["ice_connected", "connected", "fully_established"] {
                    self.metrics.lk.peer_connection.inc(&[transport, state]);
                }
                self.metrics
                    .lk
                    .service_operation
                    .inc(&["peer_connection", "success", ""]);
                // Active once the primary transport is up: the subscriber
                // PC, or the one PC of a single-PC client.
                let primary = side == Side::Subscriber || p.single_pc();
                if primary && p.state != participant_info::State::Active {
                    p.state = participant_info::State::Active;
                    let now = Instant::now();
                    p.active_at = Some(now);
                    let (sdk, protocol) = (sdk_name(&p.client), p.protocol.to_string());
                    let lk = &self.metrics.lk;
                    lk.participant_join.inc(&["rtc_active", "false", ""]);
                    lk.participant_join.inc(&["rtc_success", "false", sdk]);
                    lk.start_time.observe(
                        &[&protocol, "false", sdk],
                        now.duration_since(p.started).as_millis() as f64,
                    );
                    p.version += 1;
                    let room = p.room.clone();
                    self.participant_update(&room, i);
                    let info = self.participants[i].info();
                    self.hook(webhook::PARTICIPANT_JOINED, &room, Some(info), None);
                }
            }
            Event::IceConnectionStateChange(
                IceConnectionState::Connected | IceConnectionState::Completed,
            ) => {
                let t = self.participants[i].transport(side);
                t.connected_at.get_or_insert_with(Instant::now);
            }
            Event::IceConnectionStateChange(IceConnectionState::Disconnected) => {
                let short = self.participants[i]
                    .transport(side)
                    .connected_at
                    .is_some_and(|at| at.elapsed() < fallback::SHORT_CONNECTION);
                self.media_lost(i, side, short);
            }
            Event::MediaAdded(added)
                if side == Side::Publisher
                    && matches!(added.direction, Direction::RecvOnly | Direction::SendRecv) =>
            {
                self.bind_track(i, added.mid, added.kind);
            }
            Event::ChannelOpen(id, label) => {
                let p = &mut self.participants[i];
                // Data reaches it on the subscriber PC, or in single-PC mode
                // on its own channels.
                let primary = (side == Side::Publisher) == p.single_pc();
                let reliable = label == "_reliable";
                match side {
                    Side::Publisher => p.pub_channels.push((id, label)),
                    Side::Subscriber => p.sub_channels.push((id, label)),
                }
                if primary && reliable {
                    self.replay_joining(i);
                }
            }
            Event::ChannelClose(id) => {
                let p = &mut self.participants[i];
                p.pub_channels.retain(|(c, _)| *c != id);
                p.sub_channels.retain(|(c, _)| *c != id);
            }
            Event::ChannelData(data) => {
                let p = &self.participants[i];
                let channels = match side {
                    Side::Publisher => &p.pub_channels,
                    Side::Subscriber => &p.sub_channels,
                };
                let label = channels
                    .iter()
                    .find(|(c, _)| *c == data.id)
                    .map(|(_, l)| l.clone())
                    .unwrap_or_default();
                self.data(i, &label, &data.data);
            }
            Event::EgressBitrateEstimate(kind) => {
                let p = &mut self.participants[i];
                let sending = p.sending_side();
                let bps = match kind {
                    BweKind::Twcc { estimate, .. } if p.congestion == Congestion::Twcc => {
                        estimate.as_u64()
                    }
                    BweKind::Remb { estimate, .. } if p.congestion == Congestion::Remb => {
                        estimate.as_u64()
                    }
                    _ => return,
                };
                if side != sending {
                    return;
                }
                let old = p.cc.estimate.replace(bps);
                // Act at once on a drop or a rise of a tenth; otherwise at
                // the next periodic allocation.
                if old.is_none_or(|o| bps < o || bps > o + o / 10) {
                    self.allocate(i, Instant::now());
                }
            }
            Event::TwccFeedback(packets) => {
                let now = Instant::now();
                let p = &mut self.participants[i];
                p.cc.detector.on_feedback(&packets, now);
                p.cc.twcc_seen = true;
                // Act on congestion as soon as it shows.
                let e = p.cc.detector.evidence(now);
                if e.queueing || e.loss {
                    self.allocate(i, now);
                }
            }
            // Per stream, every STATS_INTERVAL: the quality score, and
            // LiveKit's stream metrics (`telemetry/stats.go`).
            Event::MediaIngressStats(s) if side == Side::Publisher => {
                let p = &mut self.participants[i];
                if let Some(t) = p.published.iter_mut().find(|t| t.mid == Some(s.mid)) {
                    let last = match t.stats_last.iter_mut().find(|(r, _)| *r == s.rid) {
                        Some((_, last)) => last,
                        None => {
                            t.stats_last.push((s.rid, StreamCounts::default()));
                            &mut t.stats_last.last_mut().expect("just pushed").1
                        }
                    };
                    let d = counts_delta(last, [s.packets, s.bytes, 0, 0, s.nacks, s.plis, s.firs]);
                    let window = window(d[0], s.loss);
                    t.scorer.update(&window, Instant::now());
                    let lk = &self.metrics.lk;
                    lk.packets_in.fetch_add(d[0], Relaxed);
                    lk.bytes_in.fetch_add(d[1], Relaxed);
                    lk.stream(
                        "incoming",
                        t.info.source().as_str_name(),
                        type_name(t.kind),
                        (d[4], d[5], d[6]),
                        window.lost,
                        0,
                        window.packets,
                        Some(jitter_us(s.jitter, t.kind)),
                        s.rtt,
                    );
                }
            }
            Event::MediaEgressStats(s) if side == Side::Subscriber => {
                let p = &mut self.participants[i];
                if let Some(d) = p
                    .downtracks
                    .iter_mut()
                    .find(|d| d.state.mid() == Some(s.mid))
                {
                    let c = counts_delta(
                        &mut d.stats_last,
                        [
                            s.packets,
                            s.bytes,
                            s.packets_resent,
                            s.bytes_resent,
                            s.nacks,
                            s.plis,
                            s.firs,
                        ],
                    );
                    let window = window(c[0], s.loss);
                    d.scorer.update(&window, Instant::now());
                    let lk = &self.metrics.lk;
                    lk.packets_out.fetch_add(c[0] - c[2].min(c[0]), Relaxed);
                    lk.bytes_out.fetch_add(c[1] - c[3].min(c[1]), Relaxed);
                    lk.packets_rtx.fetch_add(c[2], Relaxed);
                    lk.bytes_rtx.fetch_add(c[3], Relaxed);
                    lk.stream(
                        "outgoing",
                        d.source.as_str_name(),
                        type_name(d.kind),
                        (c[4], c[5], c[6]),
                        window.lost,
                        0,
                        window.packets,
                        s.remote.as_ref().map(|r| jitter_us(r.jitter, d.kind)),
                        s.rtt,
                    );
                }
            }
            _ => {}
        }
    }

    /// Ties an m-line the publisher opened to the `add_track` it belongs
    /// to, and starts forwarding it.
    fn bind_track(&mut self, i: usize, mid: Mid, kind: MediaKind) {
        let p = &mut self.participants[i];
        if p.published.iter().any(|t| t.mid == Some(mid)) {
            return;
        }
        let cid = p.offered_tracks.get(&mid.to_string()).cloned();
        let want = match kind {
            MediaKind::Audio => lk::TrackType::Audio,
            MediaKind::Video => lk::TrackType::Video,
        };
        let pos = cid
            .as_ref()
            .and_then(|cid| p.pending_tracks.iter().position(|(_, r)| &r.cid == cid))
            .or_else(|| {
                p.pending_tracks
                    .iter()
                    .position(|(_, r)| r.r#type() == want)
            });
        let Some(pos) = pos else {
            debug!(identity = %p.identity, %mid, "m-line with no add_track; ignoring");
            return;
        };
        let (sid, req) = p.pending_tracks.remove(pos);
        let (red_pt, mime) = match kind {
            MediaKind::Audio => {
                let (_, red_pt) = negotiated_pts(&p.publisher.rtc);
                let mime = if red_pt.is_some() {
                    "audio/red"
                } else {
                    "audio/opus"
                };
                (red_pt, mime)
            }
            // The codec add_track asked for (our answer put it first, so
            // that is what the client sends), else the client's first.
            MediaKind::Video => {
                let rtc = &p.publisher.rtc;
                let negotiated: Vec<Codec> = rtc
                    .media(mid)
                    .map(|m| m.remote_pts().to_vec())
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|pt| rtc.codec_config().params().iter().find(|x| x.pt() == pt))
                    .map(|x| x.spec().codec)
                    .collect();
                let asked = req.simulcast_codecs.first().map(|c| c.codec.as_str());
                let asked = asked.and_then(video::mime_for);
                let codec = negotiated
                    .iter()
                    .copied()
                    .find(|&c| asked.is_some() && video::mime(c) == asked)
                    .or(negotiated.first().copied());
                (None, codec.and_then(video::mime).unwrap_or("video/VP8"))
            }
        };
        let mut info = track_info(&sid, &req, Some(&mid.to_string()), Some(mime), p.obs);
        match p.publisher.rtc.media(mid).map(|m| m.rids_rx()) {
            Some(Rids::Specific(rids)) => info.simulcast = rids.len() > 1,
            // VP9/AV1 without rids: SVC, every layer in one stream, as
            // LiveKit marks it (not simulcast; layers without rids).
            _ if matches!(mime, "video/VP9" | "video/AV1") => {
                info.simulcast = false;
                let layers = info
                    .layers
                    .iter_mut()
                    .chain(info.codecs.iter_mut().flat_map(|c| c.layers.iter_mut()));
                for l in layers {
                    l.rid.clear();
                }
            }
            _ => {}
        }
        let fec = true;
        let backup_of = p
            .published
            .iter()
            .any(|t| t.info.sid == sid && t.backup_of.is_none())
            .then(|| sid.clone());
        p.published.push(Published {
            info,
            kind,
            mid: Some(mid),
            layers: vec![],
            pts: vec![],
            bound_at: Instant::now(),
            keyframe_seen: false,
            size_from_media: p.one_shot && kind == MediaKind::Video,
            seeded_at: None,
            red_pt,
            level: AudioLevel::new(audio_level::Config::default(), 48_000),
            to_primary: ToPrimary::default(),
            subscribers: vec![],
            ever_subscribed: false,
            scorer: Scorer::new(quality::loss_weight(mime, fec)),
            stats_last: vec![],
            svc: None,
            backup_of: backup_of.clone(),
            regressed: false,
            // Publishers start sending every layer of the primary codec,
            // and no backup until asked.
            dynacast: [Some(lk::VideoQuality::High), None],
            dynacast_lower: None,
        });
        p.version += 1;
        let t_index = p.published.len() - 1;
        let room = p.room.clone();
        // A backup codec's stream: the track now lists its m-line; it isn't
        // a track of its own, so nobody subscribes to it as one.
        if backup_of.is_some() {
            if let Some(t) = p
                .published
                .iter_mut()
                .find(|t| t.info.sid == sid && t.backup_of.is_none())
                && let Some(c) = t.info.codecs.iter_mut().find(|c| c.mime_type == mime)
            {
                c.mid = mid.to_string();
            }
            info!(identity = %p.identity, track = %sid, %mime, "backup codec published");
            self.participant_update(&room, i);
            return;
        }
        info!(identity = %p.identity, track = %sid, %mime, "published");
        let kind = type_name(p.published[t_index].kind);
        self.metrics.lk.publish_counter.inc(&[kind, "success"]);
        if let Some(at) = p.publish_asked.remove(&sid) {
            self.metrics.lk.pubsub(
                "publish",
                p.published[t_index].info.source().as_str_name(),
                kind,
                at.elapsed(),
                sdk_name(&p.client),
                p.kind.as_str_name(),
            );
        }
        let (who, info) = (p.who(), p.published[t_index].info.clone());
        self.hook(webhook::TRACK_PUBLISHED, &room, Some(who), Some(info));
        self.agents_on_publish(i);

        // Auto-subscribe everyone else who wants it.
        let publisher_sid = self.participants[i].sid.clone();
        let publisher_conn = self.participants[i].conn;
        let members = self.rooms[&room].members.clone();
        for j in members {
            if j == i {
                continue;
            }
            let (auto, can, conn) = {
                let s = &self.participants[j];
                (s.auto_subscribe, s.grant.can_subscribe(), s.conn)
            };
            if !auto || !can {
                continue;
            }
            if !self.participants[i].permits(&sid, &self.participants[j]) {
                refuse(
                    &mut self.participants[j],
                    sid.clone(),
                    &publisher_sid,
                    publisher_conn,
                );
                continue;
            }
            let prefs = Prefs::of(&self.participants[j]);
            let t = &mut self.participants[i].published[t_index];
            t.subscribers.push(conn);
            let d = new_downtrack(t, &publisher_sid, publisher_conn, &prefs);
            self.metrics.lk.subscribe_counter.inc(&["attempt", ""]);
            self.participants[j].downtracks.push(d);
            // Negotiate it now rather than at that transport's next timer.
            self.touched.push((j, Side::Subscriber));
        }
        if let Some(r) = self.rooms.get_mut(&room) {
            r.speakers.remove(&publisher_sid);
        }
        self.participant_update(&room, i);
        self.room_update(&room);
    }
}

/// What changed since the last stats event, which becomes `last`.
fn counts_delta(last: &mut StreamCounts, now: StreamCounts) -> StreamCounts {
    let d = std::array::from_fn(|k| now[k].saturating_sub(last[k]));
    *last = now;
    d
}

/// Jitter in RTP timestamp units, in microseconds.
fn jitter_us(jitter: u32, kind: MediaKind) -> f64 {
    let rate = match kind {
        MediaKind::Audio => 48_000.0,
        MediaKind::Video => 90_000.0,
    };
    f64::from(jitter) / rate * 1e6
}

fn window(delta: u64, loss: Option<f32>) -> quality::Window {
    let delta = delta as u32;
    let loss = f64::from(loss.unwrap_or(0.0)).clamp(0.0, 0.99);
    let lost = (f64::from(delta) * loss / (1.0 - loss)).round() as u32;
    quality::Window {
        duration: crate::livekit::rtc_config::STATS_INTERVAL,
        packets: delta,
        lost,
        ..Default::default()
    }
}
