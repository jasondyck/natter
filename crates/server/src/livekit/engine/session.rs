//! Joining, resuming and leaving: a participant's connections from the
//! signalling side's `Join` to the moment `reap` drops them, as LiveKit's
//! `rtc.Room.Join`, `ResumeParticipant` and `RemoveParticipant`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant, SystemTime};

use livekit_protocol as lk;
use lk::{leave_request, participant_info, signal_response};
use str0m::channel::{ChannelConfig, Reliability};
use tracing::{info, warn};

use super::{
    Cc, ConnId, DownState, Engine, Join, LIVEKIT_VERSION, Participant, Prefs, ROOM_UPDATE_INTERVAL,
    Resume, Room, Side, Transport, admin, data_track, enabled_codecs, leave, new_downtrack, refuse,
    response, sdk_name, server_info, stop_downtrack,
};
use crate::livekit::fallback::{self, Fallback, Pref};
use crate::livekit::ids;
use crate::livekit::webhook;

const PING_INTERVAL: i32 = 5;
const PING_TIMEOUT: i32 = 15;
/// How long a client gets to answer a subscriber offer.
const OFFER_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a participant whose media path failed has to resume, as
/// LiveKit's `disconnectCleanupDuration`.
const RESUME_AFTER_MEDIA_LOSS: Duration = Duration::from_secs(5);
/// A one-shot (WHIP) session that hasn't connected by then is dropped
/// (chosen: LiveKit fails ICE after 15 s and DTLS 10-20 s after that).
const ONE_SHOT_CONNECT: Duration = Duration::from_secs(30);

impl Engine {
    /// Hands a reconnected client its session back: new signalling, an ICE
    /// restart, and the room's state again (LiveKit's resume).
    /// A peer connection lost its media path, or never found one.
    pub(super) fn media_lost(&mut self, i: usize, side: Side, short: bool) {
        let p = &mut self.participants[i];
        // As LiveKit: no way to ask a one-shot client to resume.
        if p.one_shot {
            if !p.closing {
                info!(identity = %p.identity, "WHIP media path lost; closing");
                p.closing = true;
                self.reap_soon = true;
            }
            return;
        }
        let p = &self.participants[i];
        if p.closing || p.cleanup_at.is_some() {
            return;
        }
        warn!(identity = %p.identity, ?side, "media path lost; asking client to resume");
        self.transport_failed(i, short);
        let p = &mut self.participants[i];
        p.tcp_check_by = None;
        // As LiveKit: tell the client to resume (new signalling, ICE
        // restart) and give it a few seconds to.
        p.send(leave(
            lk::DisconnectReason::MediaFailure,
            leave_request::Action::Resume,
        ));
        p.cleanup_at = Some(Instant::now() + RESUME_AFTER_MEDIA_LOSS);
    }

    /// A peer connection failed: maybe hold the client to another kind of
    /// candidate from its next ICE restart on (`fallback`).
    fn transport_failed(&mut self, i: usize, short: bool) {
        let now = Instant::now();
        let p = &mut self.participants[i];
        let path = [Some(&p.publisher), p.subscriber.as_ref()]
            .into_iter()
            .flatten()
            .filter_map(Transport::path)
            .max();
        let tls = p
            .ice_servers
            .iter()
            .flat_map(|s| &s.urls)
            .any(|u| u.starts_with("turns:"));
        if let Some(pref) = p.fallback.on_failure(path, short, p.ice_tcp, tls, now) {
            info!(identity = %p.identity, ?path, short, ?pref, "transport fallback");
            let key = (p.room.to_string(), p.identity.clone());
            self.ice_prefs
                .retain(|_, (_, at)| now.duration_since(*at) < fallback::REMEMBER);
            self.ice_prefs.insert(key, (pref, now));
        }
    }

    pub(super) fn resume(&mut self, r: Resume) {
        let found = self.participants.iter().position(|p| {
            p.sid == r.sid && p.identity == r.identity && *p.room == *r.room && !p.closing
        });
        let Some(i) = found else {
            let _ = r.tx.try_send(response(leave(
                lk::DisconnectReason::StateMismatch,
                leave_request::Action::Reconnect,
            )));
            return;
        };
        // A peer connection the client saw fail young moves it on, as
        // LiveKit's `HandleClientReconnect`.
        let failed = match lk::ReconnectReason::try_from(r.reason) {
            Ok(lk::ReconnectReason::RrPublisherFailed) => Some(Side::Publisher),
            Ok(lk::ReconnectReason::RrSubscriberFailed) => Some(Side::Subscriber),
            _ => None,
        };
        if let Some(side) = failed {
            let p = &mut self.participants[i];
            let t = match side {
                Side::Subscriber => p.subscriber.as_ref().unwrap_or(&p.publisher),
                Side::Publisher => &p.publisher,
            };
            if t.connected_at
                .is_some_and(|at| at.elapsed() < fallback::SHORT_CONNECTION)
            {
                p.fallback.resumed();
                self.transport_failed(i, true);
            }
        }
        // Tracks name their subscribers and publishers by connection, and
        // the session has a new one.
        let old = self.participants[i].conn;
        for other in &mut self.participants {
            for t in &mut other.published {
                for c in &mut t.subscribers {
                    if *c == old {
                        *c = r.conn;
                    }
                }
            }
            for d in &mut other.downtracks {
                if d.publisher == old {
                    d.publisher = r.conn;
                }
            }
        }
        let p = &mut self.participants[i];
        info!(identity = %p.identity, "resumed");
        self.metrics.resumes_total.fetch_add(1, Relaxed);
        p.conn = r.conn;
        p.tx = r.tx;
        p.backlogged = false;
        p.cleanup_at = None;
        // Whatever offer was in flight died with the old connection; the
        // next offer supersedes it.
        p.sub_pending = None;
        p.sub_pending_since = None;
        p.reoffer_requested = false;
        if !p.single_pc() {
            p.restart_ice = true;
            for d in &mut p.downtracks {
                d.state = match d.state {
                    DownState::Negotiating(_) => DownState::ToOpen,
                    DownState::NegotiatingStop(m) => DownState::ToStop(m),
                    s => s,
                };
            }
        }
        p.ice_servers = r.ice_servers;
        p.send(signal_response::Message::Reconnect(lk::ReconnectResponse {
            server_info: Some(server_info(&self.node_id, &self.region)),
            ice_servers: p.ice_servers_now(),
            client_configuration: p.client_configuration(),
            last_message_seq: p.last_reliable_seq,
        }));
        // The client restarts ICE with this preference: count afresh.
        p.fallback.resumed();
        p.publisher.connected_at = None;
        if let Some(s) = &mut p.subscriber {
            s.connected_at = None;
        }
        p.arm_tcp_check(Instant::now());
        let room = p.room.clone();
        let everyone: Vec<lk::ParticipantInfo> = self.rooms[&room]
            .members
            .iter()
            .map(|&j| &self.participants[j])
            .filter(|o| !o.hidden() || o.conn == r.conn)
            .map(Participant::info)
            .collect();
        let room_info = self.rooms[&room].info(&self.participants);
        let p = &mut self.participants[i];
        p.send(signal_response::Message::Update(lk::ParticipantUpdate {
            participants: everyone,
        }));
        p.send(signal_response::Message::RoomUpdate(lk::RoomUpdate {
            room: Some(room_info),
        }));
        self.membership_changed = true;
        self.rebuild_index();
        self.touched.push((i, Side::Subscriber));
    }

    fn refuse_join(&self, join: &Join, reason: lk::DisconnectReason) {
        self.placement.leave(&join.room);
        let _ = join
            .tx
            .try_send(response(leave(reason, leave_request::Action::Disconnect)));
    }

    pub(super) fn join(&mut self, join: Join) {
        let now = Instant::now();
        if self.shutdown.is_some() {
            self.refuse_join(&join, lk::DisconnectReason::ServerShutdown);
            return;
        }
        if self.placement.total() >= self.max_participants {
            warn!(max = self.max_participants, "server full; join refused");
            self.metrics.joins_refused_total.fetch_add(1, Relaxed);
            self.refuse_join(&join, lk::DisconnectReason::JoinFailure);
            return;
        }
        let name: Arc<str> = self
            .rooms
            .keys()
            .find(|k| ***k == *join.room)
            .cloned()
            .unwrap_or_else(|| Arc::from(join.room.as_str()));
        let placement = Arc::clone(&self.placement);
        let created = !self.rooms.contains_key(&name);
        self.rooms.entry(name.clone()).or_insert_with(|| {
            // The room's own placement, for as long as it exists.
            placement.hold(&name);
            Room::new(
                name.clone(),
                join.room_config.as_ref(),
                &self.room_defaults,
                now,
            )
        });
        if created {
            self.hook(webhook::ROOM_STARTED, &name, None, None);
            let agents = join
                .room_config
                .as_ref()
                .map(|c| c.agents.clone())
                .unwrap_or_default();
            self.start_dispatches(&name, &agents);
        }
        let room = self.rooms.get_mut(&name).expect("just inserted");
        // Deleted by an admin, its last participants still on their way.
        if room.deleted {
            self.refuse_join(&join, lk::DisconnectReason::RoomDeleted);
            return;
        }

        // A second connection with the same identity replaces the first.
        for &i in &room.members {
            let other = &mut self.participants[i];
            if other.identity == join.identity && !other.closing {
                other.send(leave(
                    lk::DisconnectReason::DuplicateIdentity,
                    leave_request::Action::Disconnect,
                ));
                other.closing = true;
                self.reap_soon = true;
            }
        }
        let dependent = join.grant.agent
            || join.grant.recorder
            || matches!(
                join.kind,
                participant_info::Kind::Agent | participant_info::Kind::Egress
            );
        let active = room
            .members
            .iter()
            .filter(|&&i| !self.participants[i].closing && !self.participants[i].is_dependent())
            .count();
        if room.max_participants > 0 && !dependent && active as u32 >= room.max_participants {
            self.refuse_join(&join, lk::DisconnectReason::JoinFailure);
            return;
        }
        if !dependent {
            room.had_participants = true;
            room.idle_closed = false;
        }

        let pref = self
            .ice_prefs
            .get(&(name.to_string(), join.identity.clone()))
            .filter(|(_, at)| at.elapsed() < fallback::REMEMBER)
            .map_or(Pref::Any, |(pref, _)| *pref);
        let mut p = Participant {
            conn: join.conn,
            sid: ids::new(ids::PARTICIPANT),
            identity: join.identity,
            name: join.name,
            metadata: join.metadata,
            attributes: join.attributes,
            kind: join.kind,
            grant: join.grant,
            state: participant_info::State::Joining,
            joined_at: SystemTime::now(),
            version: 1,
            room: name.clone(),
            auto_subscribe: join.auto_subscribe,
            auto_subscribe_data: join.auto_subscribe_data,
            data_tracks: vec![],
            data_wants: vec![],
            // LiveKit starts each participant's handles at random.
            next_data_handle: u16::from(crate::util::random_bytes::<1>()[0]),
            data_rate: Default::default(),
            reliable_ready: false,
            joining_first: HashMap::new(),
            joining_written: HashMap::new(),
            joining_held: vec![],
            joining_held_bytes: 0,
            adaptive_stream: join.adaptive_stream,
            track_settings: HashMap::new(),
            congestion: join.congestion,
            allow_pause: join.allow_pause,
            cc: Cc {
                backoff: 1.0,
                ..Default::default()
            },
            protocol: join.protocol,
            obs: join.client.browser.contains("OBS"),
            client: join.client,
            tx: join.tx,
            backlogged: false,
            closing: false,
            publisher: Transport::new(join.publisher),
            subscriber: join.subscriber.map(Transport::new),
            slots: vec![],
            reoffer_requested: false,
            pending_tracks: vec![],
            publish_asked: HashMap::new(),
            offered_tracks: HashMap::new(),
            published: vec![],
            ssrc_track: vec![],
            downtracks: vec![],
            sub_pending: None,
            sub_pending_since: None,
            sub_offer_id: 0,
            negotiate_after: None,
            negotiated: false,
            pub_channels: vec![],
            sub_channels: vec![],
            quality_sent: HashMap::new(),
            cleanup_at: None,
            restart_ice: false,
            fallback: Fallback::new(pref),
            ice_tcp: join.ice_tcp,
            ice_servers: join.ice_servers,
            signal_rtt: None,
            tcp_check_by: None,
            one_shot: join.one_shot,
            started: join.started,
            active_at: None,
            last_reliable_seq: 0,
            sub_permission: None,
            refused: vec![],
            mid_sent: vec![],
        };
        if p.one_shot {
            p.cleanup_at = Some(Instant::now() + ONE_SHOT_CONNECT);
        }
        p.arm_tcp_check(now);

        let others: Vec<lk::ParticipantInfo> = room
            .members
            .iter()
            .map(|&i| &self.participants[i])
            .filter(|o| !o.closing && !o.hidden())
            .map(Participant::info)
            .collect();
        let mut room_info = room.info(&self.participants);
        room_info.num_participants += u32::from(!p.is_dependent());
        room_info.num_publishers += u32::from(p.info().is_publisher);
        room_info.active_recording |= p.is_recorder();
        p.send(signal_response::Message::Join(lk::JoinResponse {
            room: Some(room_info),
            participant: Some(p.info()),
            other_participants: others,
            server_version: LIVEKIT_VERSION.into(),
            subscriber_primary: !p.single_pc(),
            sif_trailer: room.trailer.clone(),
            ping_timeout: PING_TIMEOUT,
            ping_interval: PING_INTERVAL,
            server_info: Some(server_info(&self.node_id, &self.region)),
            enabled_publish_codecs: enabled_codecs(),
            ice_servers: p.ice_servers_now(),
            client_configuration: p.client_configuration(),
            // Publishers connect their publisher PC at join, unless the
            // transport fallback has narrowed the candidates (LiveKit's
            // `hasICEFallback`).
            fast_publish: p.grant.can_publish() && p.fallback.pref == Pref::Any,
            ..Default::default()
        }));

        // The subscriber PC starts with LiveKit's data channels, so the
        // client can receive data before anyone publishes media. In
        // single-PC mode the client's own channels carry data both ways.
        if let Some(sub) = &mut p.subscriber {
            let mut change = sub.rtc.sdp_api();
            let lossy = Reliability::MaxRetransmits { retransmits: 0 };
            for (label, ordered, reliability) in [
                ("_reliable", true, Reliability::Reliable),
                ("_lossy", false, lossy),
                (data_track::CHANNEL, false, lossy),
            ] {
                change.add_channel_with_config(ChannelConfig {
                    label: label.into(),
                    ordered,
                    reliability,
                    ..Default::default()
                });
            }
            if let Some((offer, pending)) = change.apply() {
                p.send_sub_offer(offer, pending, HashMap::new(), now);
            }
        }

        // Forward what is already published here.
        if p.auto_subscribe && p.grant.can_subscribe() {
            let prefs = Prefs::of(&p);
            for &i in &room.members {
                let other = &mut self.participants[i];
                // Leaving (or replaced by this join): their tracks end
                // within one housekeeping pass, so don't offer them.
                if other.closing {
                    continue;
                }
                let refused: Vec<String> = other
                    .published
                    .iter()
                    .filter(|t| t.mid.is_some() && t.backup_of.is_none())
                    .filter(|t| !other.permits(&t.info.sid, &p))
                    .map(|t| t.info.sid.clone())
                    .collect();
                for t in other
                    .published
                    .iter_mut()
                    .filter(|t| t.mid.is_some() && t.backup_of.is_none())
                    .filter(|t| !refused.contains(&t.info.sid))
                {
                    p.downtracks
                        .push(new_downtrack(t, &other.sid, other.conn, &prefs));
                    self.metrics.lk.subscribe_counter.inc(&["attempt", ""]);
                    t.subscribers.push(p.conn);
                }
                for sid in refused {
                    refuse(&mut p, sid, &other.sid, other.conn);
                }
            }
        }

        info!(room = %name, identity = %p.identity, sid = %p.sid, "joined");
        let lk = &self.metrics.lk;
        lk.participant_join
            .inc(&["rtc_connected", "", sdk_name(&p.client)]);
        lk.peer_connection.inc(&["PUBLISHER", "started"]);
        if p.subscriber.is_some() {
            lk.peer_connection.inc(&["SUBSCRIBER", "started"]);
        }
        self.participants.push(p);
        self.membership_changed = true;
        self.rebuild_index();
        let i = self.by_conn[&join.conn];
        self.room_update(&name);
        self.want_existing_data_tracks(i);
        self.agents_on_join(i);
    }

    /// Drops participants who left or broke, tells their rooms, and closes
    /// rooms that stayed empty long enough.
    pub(super) fn reap(&mut self, now: Instant) {
        self.expire_rpcs(now);
        self.count_tracks(now);
        self.remove_lingering_agents(now);
        // Held to ICE/TCP and still not connected: TCP fails too.
        for i in 0..self.participants.len() {
            let p = &mut self.participants[i];
            if p.tcp_check_by.is_none_or(|by| now < by) {
                continue;
            }
            p.tcp_check_by = None;
            let unconnected = [Some(&p.publisher), p.subscriber.as_ref()]
                .into_iter()
                .flatten()
                .any(|t| t.connected_at.is_none());
            if unconnected {
                info!(identity = %p.identity, "ICE/TCP did not connect");
                self.media_lost(i, Side::Publisher, true);
            }
        }
        for p in &mut self.participants {
            if p.backlogged {
                warn!(identity = %p.identity, "signalling backlog full; dropping");
                p.closing = true;
            }
            if p.cleanup_at.is_some_and(|at| now >= at) && !p.closing {
                warn!(identity = %p.identity, "did not resume in time");
                p.closing = true;
            }
            if p.cleanup_at.is_none()
                && p.sub_pending_since
                    .is_some_and(|at| now.duration_since(at) > OFFER_TIMEOUT)
            {
                warn!(identity = %p.identity, "no answer to subscriber offer");
                p.closing = true;
            }
        }
        let gone: Vec<usize> = (0..self.participants.len())
            .filter(|&i| !self.participants[i].is_alive())
            .collect();
        if !gone.is_empty() {
            let mut left: Vec<(Arc<str>, lk::ParticipantInfo, ConnId, Vec<String>)> = vec![];
            // For webhooks once the index is rebuilt: the participant,
            // whether it ever became active, and its tracks.
            let mut departed = vec![];
            let mut data_left = vec![];
            let mut identities = vec![];
            for &i in gone.iter().rev() {
                let mut p = self.participants.swap_remove(i);
                identities.push((p.room.clone(), p.identity.clone()));
                if let Some(at) = p.active_at {
                    self.metrics
                        .lk
                        .session_duration
                        .observe(&[&p.protocol.to_string()], at.elapsed().as_millis() as f64);
                }
                let was_active = p.state == participant_info::State::Active;
                self.placement.leave(&p.room);
                p.publisher.rtc.disconnect();
                sub!(p).rtc.disconnect();
                p.state = participant_info::State::Disconnected;
                p.version += 1;
                info!(identity = %p.identity, "left");
                if self.webhooks.is_some() {
                    let tracks: Vec<lk::TrackInfo> = p
                        .published
                        .iter()
                        .filter(|t| t.backup_of.is_none() && t.mid.is_some())
                        .map(|t| t.info.clone())
                        .collect();
                    departed.push((p.room.clone(), p.info(), was_active, tracks));
                }
                let tracks = p
                    .published
                    .iter()
                    .filter(|t| t.backup_of.is_none())
                    .map(|t| t.info.sid.clone())
                    .collect();
                let data_sids: Vec<String> =
                    p.data_tracks.iter().map(|t| t.info.sid.clone()).collect();
                data_left.push((p.room.clone(), p.conn, data_sids));
                if !p.hidden() {
                    left.push((p.room.clone(), p.info(), p.conn, tracks));
                } else {
                    left.push((
                        p.room.clone(),
                        lk::ParticipantInfo::default(),
                        p.conn,
                        tracks,
                    ));
                }
            }
            // Removed last to first: report them in the order they were.
            left.reverse();
            departed.reverse();
            self.rebuild_index();
            // Unless another session took its identity (LiveKit ignores
            // the replaced one's removal).
            for (room, identity) in identities {
                let replaced = self
                    .participants
                    .iter()
                    .any(|p| p.room == room && p.identity == identity);
                if !replaced {
                    self.agents_on_leave(&room, &identity);
                }
            }
            // As LiveKit: the tracks close with the participant, then it
            // has left (or never connected).
            for (room, info, was_active, tracks) in departed {
                let who = lk::ParticipantInfo {
                    sid: info.sid.clone(),
                    identity: info.identity.clone(),
                    ..Default::default()
                };
                for t in tracks {
                    self.hook(
                        webhook::TRACK_UNPUBLISHED,
                        &room,
                        Some(who.clone()),
                        Some(t),
                    );
                }
                let event = if was_active {
                    webhook::PARTICIPANT_LEFT
                } else {
                    webhook::PARTICIPANT_CONNECTION_ABORTED
                };
                self.hook(event, &room, Some(info), None);
            }
            for (room, info, conn, tracks) in left {
                for p in self.participants.iter_mut().filter(|p| p.room == room) {
                    for sid in &tracks {
                        stop_downtrack(p, sid);
                    }
                    for t in &mut p.published {
                        t.subscribers.retain(|&c| c != conn);
                    }
                    p.refused.retain(|&(_, c)| c != conn);
                    p.quality_sent.remove(&info.sid);
                    if !info.sid.is_empty() {
                        p.send(signal_response::Message::Update(lk::ParticipantUpdate {
                            participants: vec![info.clone()],
                        }));
                    }
                }
                if let Some(r) = self.rooms.get_mut(&room) {
                    r.speakers.remove(&info.sid);
                    r.left_at = Some(now);
                }
                self.room_update(&room);
            }
            // Its data tracks are gone, and it no longer receives any.
            for (room, conn, sids) in data_left {
                for p in self.participants.iter_mut().filter(|p| p.room == room) {
                    for t in &mut p.data_tracks {
                        t.subscribers.retain(|&(c, _)| c != conn);
                    }
                }
                if !sids.is_empty() {
                    self.data_tracks_gone(&room, &sids);
                }
            }
            self.membership_changed = true;
        }
        let due: Vec<Arc<str>> = self
            .rooms
            .iter()
            .filter(|(_, r)| {
                r.counts_pending
                    && r.counts_sent_at
                        .is_none_or(|at| now.duration_since(at) >= ROOM_UPDATE_INTERVAL)
            })
            .map(|(name, _)| name.clone())
            .collect();
        for name in due {
            self.room_update(&name);
        }
        // Idle with agents or recorders still in it: they go first.
        let idle: Vec<Arc<str>> = self
            .rooms
            .iter()
            .filter(|(_, r)| {
                !r.members.is_empty()
                    && !r.deleted
                    && !r.idle_closed
                    && r.idle(&self.participants, now)
            })
            .map(|(name, _)| name.clone())
            .collect();
        for name in idle {
            info!(room = %name, "room idle; closing");
            let r = self.rooms.get_mut(&name).expect("listed");
            r.idle_closed = true;
            for i in r.members.clone() {
                self.disconnect(i, lk::DisconnectReason::RoomClosed);
            }
        }
        // Stopping: everyone still here leaves, as LiveKit's
        // `RoomManager.Stop` closing every room.
        if self.shutdown.is_some() {
            let staying: Vec<usize> = self
                .rooms
                .values()
                .flat_map(|r| r.members.iter().copied())
                .filter(|&i| !self.participants[i].closing)
                .collect();
            for i in staying {
                self.disconnect(i, lk::DisconnectReason::ServerShutdown);
            }
        }
        let shutting_down = self.shutdown.is_some();
        let before = self.rooms.len();
        let placement = Arc::clone(&self.placement);
        let (webhooks, participants) = (&self.webhooks, &self.participants);
        let lk = &self.metrics.lk;
        self.rooms.retain(|name, r| {
            let keep = !(r.expired(participants, now) || shutting_down && r.members.is_empty());
            if !keep {
                info!(room = %name, "room closed");
                lk.room_duration
                    .observe(&[], r.created.elapsed().unwrap_or_default().as_secs_f64());
                placement.leave(name);
                if let Some(w) = webhooks {
                    let reason = if shutting_down {
                        lk::RoomEndReason::RoomEndServerShutdown
                    } else if r.deleted {
                        lk::RoomEndReason::RoomEndApiDelete
                    } else {
                        lk::RoomEndReason::RoomEndIdleTimeout
                    };
                    w.notify(lk::WebhookEvent {
                        event: webhook::ROOM_FINISHED.into(),
                        room: Some(r.info(participants)),
                        room_end_reason: reason as i32,
                        ..Default::default()
                    });
                }
            }
            keep
        });
        if self.rooms.len() != before {
            self.membership_changed = true;
        }
        if self.rooms.is_empty()
            && let Some(replies) = self.shutdown.as_mut()
        {
            for reply in replies.drain(..) {
                let _ = reply.send(Ok(admin::AdminReply::Done));
            }
        }
    }
}
