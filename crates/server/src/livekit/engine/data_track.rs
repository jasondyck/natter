//! Data tracks, as LiveKit's (`participant_data_track.go`, `datatrack.go`,
//! the data-track half of `subscriptionmanager.go`): a publisher announces
//! a track under its own handle, subscribers ask for it by sid and get
//! their own handle for it, and packets on the `_data_track` channels are
//! forwarded with the handle swapped. The server reads only the header.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use livekit_protocol as lk;
use lk::{
    data_track_frame_encoding, data_track_schema_encoding, request_response, signal_response,
};
use tracing::{debug, info, warn};

use super::super::ids;
use super::{ConnId, Engine, Participant};

/// The channel data-track packets travel on, both ways.
pub(super) const CHANNEL: &str = "_data_track";

/// A wanted track that doesn't turn up is given up after this (LiveKit's
/// `notFoundTimeout`).
const NOT_FOUND_TIMEOUT: Duration = Duration::from_secs(60);
/// Longest custom frame or schema encoding name (LiveKit's
/// `max_data_track_custom_encoding_length` default).
const MAX_CUSTOM_ENCODING: usize = 32;

/// A data track a participant publishes.
pub(super) struct DataTrack {
    pub info: lk::DataTrackInfo,
    /// Who receives it, under which of their handles.
    pub subscribers: Vec<(ConnId, u16)>,
}

/// A data track a participant wants, by sid; subscribed once it exists and
/// its publisher allows it.
pub(super) struct DataWant {
    pub sid: String,
    pub since: Instant,
    /// What the subscriber was last told of its permission.
    pub allowed: Option<bool>,
}

/// The handle in a data-track packet, if its header parses as LiveKit's
/// `datatrack.Header.Unmarshal` would (others are dropped there).
fn packet_handle(buf: &[u8]) -> Option<u16> {
    const HEADER: usize = 12;
    if buf.len() < HEADER {
        return None;
    }
    let handle = u16::from_be_bytes([buf[2], buf[3]]);
    if buf[0] & 0b100 == 0 {
        return Some(handle);
    }
    // Extensions: a size in 32-bit words (less one), then id/size/data
    // entries until it runs out or a zero id starts the padding.
    let words = usize::from(u16::from_be_bytes([*buf.get(12)?, *buf.get(13)?]));
    let mut remaining = (words + 1) * 4 - 2;
    let mut at = HEADER + 2;
    while remaining > 0 {
        let id = *buf.get(at)?;
        if id == 0 {
            return (buf.len() >= at + remaining).then_some(handle);
        }
        let size = usize::from(*buf.get(at + 1)?);
        remaining = remaining.checked_sub(2 + size)?;
        at += 2 + size;
        if buf.len() < at {
            return None;
        }
    }
    Some(handle)
}

fn custom_ok(name: &str) -> bool {
    !name.is_empty() && name.len() <= MAX_CUSTOM_ENCODING
}

impl Participant {
    fn next_data_handle(&mut self) -> u16 {
        self.next_data_handle = self.next_data_handle.wrapping_add(1);
        if self.next_data_handle == 0 {
            self.next_data_handle = 1;
        }
        self.next_data_handle
    }

    fn refuse_data_track(
        &mut self,
        req: lk::PublishDataTrackRequest,
        reason: request_response::Reason,
        message: &str,
    ) {
        warn!(identity = %self.identity, name = %req.name, ?reason, "data track refused");
        self.send(signal_response::Message::RequestResponse(
            lk::RequestResponse {
                reason: reason as i32,
                message: message.into(),
                request: Some(request_response::Request::PublishDataTrack(req)),
                ..Default::default()
            },
        ));
    }
}

impl Engine {
    /// `PublishDataTrackRequest` (LiveKit's `HandlePublishDataTrackRequest`).
    pub(super) fn publish_data_track(&mut self, i: usize, req: lk::PublishDataTrackRequest) {
        use request_response::Reason;
        let p = &mut self.participants[i];
        if !p.grant.can_publish_data() {
            return p.refuse_data_track(
                req,
                Reason::NotAllowed,
                "does not have permission to publish data",
            );
        }
        if req.pub_handle == 0 || req.pub_handle > 65535 {
            return p.refuse_data_track(
                req,
                Reason::InvalidHandle,
                "handle should be > 0 AND < 65536",
            );
        }
        if req.name.is_empty() || req.name.len() > 256 {
            return p.refuse_data_track(
                req,
                Reason::InvalidName,
                "name should not be empty and should not exceed 256 characters",
            );
        }
        let frame_ok = match req.frame_encoding.as_ref().and_then(|e| e.value.as_ref()) {
            Some(data_track_frame_encoding::Value::Custom(c)) => custom_ok(c),
            _ => true,
        };
        let schema_ok = match req
            .schema
            .as_ref()
            .and_then(|s| s.encoding.as_ref())
            .and_then(|e| e.value.as_ref())
        {
            Some(data_track_schema_encoding::Value::Custom(c)) => custom_ok(c),
            _ => true,
        };
        if !frame_ok || !schema_ok {
            return p.refuse_data_track(
                req,
                Reason::InvalidRequest,
                "encoding identifier is empty or exceeds the maximum length",
            );
        }
        for t in &p.data_tracks {
            if t.info.pub_handle == req.pub_handle {
                return p.refuse_data_track(
                    req,
                    Reason::DuplicateHandle,
                    "a data track with same handle already exists",
                );
            }
            if t.info.name == req.name {
                return p.refuse_data_track(
                    req,
                    Reason::DuplicateName,
                    "a data track with same name already exists",
                );
            }
        }
        let info = lk::DataTrackInfo {
            pub_handle: req.pub_handle,
            sid: ids::new("DTR_"),
            name: req.name,
            encryption: req.encryption,
            frame_encoding: req.frame_encoding,
            schema: req.schema,
        };
        info!(identity = %p.identity, track = %info.sid, name = %info.name, "data track published");
        p.send(signal_response::Message::PublishDataTrackResponse(
            lk::PublishDataTrackResponse {
                info: Some(info.clone()),
            },
        ));
        p.data_tracks.push(DataTrack {
            info: info.clone(),
            subscribers: vec![],
        });
        p.version += 1;
        let room = p.room.clone();
        self.participant_update(&room, i);
        self.room_update(&room);
        // Everyone who auto-subscribes to data tracks wants it.
        let now = Instant::now();
        for j in self.rooms[&room].members.clone() {
            let s = &mut self.participants[j];
            if j == i || s.closing || !s.auto_subscribe_data {
                continue;
            }
            if !s.data_wants.iter().any(|w| w.sid == info.sid) {
                s.data_wants.push(DataWant {
                    sid: info.sid.clone(),
                    since: now,
                    allowed: None,
                });
            }
            self.reconcile_data(j);
        }
    }

    /// `UnpublishDataTrackRequest`.
    pub(super) fn unpublish_data_track(&mut self, i: usize, req: lk::UnpublishDataTrackRequest) {
        let p = &mut self.participants[i];
        let Some(pos) = p
            .data_tracks
            .iter()
            .position(|t| t.info.pub_handle == req.pub_handle)
        else {
            p.send(signal_response::Message::RequestResponse(
                lk::RequestResponse {
                    reason: request_response::Reason::NotFound as i32,
                    request: Some(request_response::Request::UnpublishDataTrack(req)),
                    ..Default::default()
                },
            ));
            return;
        };
        let track = p.data_tracks.remove(pos);
        info!(identity = %p.identity, track = %track.info.sid, "data track unpublished");
        p.send(signal_response::Message::UnpublishDataTrackResponse(
            lk::UnpublishDataTrackResponse {
                info: Some(track.info.clone()),
            },
        ));
        p.version += 1;
        let room = p.room.clone();
        self.data_tracks_gone(&room, &[track.info.sid]);
        self.participant_update(&room, i);
        self.room_update(&room);
    }

    /// `UpdateDataSubscription`: what the subscriber wants changes, then is
    /// reconciled.
    pub(super) fn update_data_subscription(&mut self, i: usize, req: lk::UpdateDataSubscription) {
        let now = Instant::now();
        let p = &mut self.participants[i];
        for u in req.updates {
            if u.subscribe {
                if !p.data_wants.iter().any(|w| w.sid == u.track_sid) {
                    p.data_wants.push(DataWant {
                        sid: u.track_sid,
                        since: now,
                        allowed: None,
                    });
                }
            } else {
                p.data_wants.retain(|w| w.sid != u.track_sid);
            }
        }
        self.reconcile_data(i);
    }

    /// Tracks that are gone: nobody wants them any more (LiveKit unsubscribes
    /// when the source track is removed).
    pub(super) fn data_tracks_gone(&mut self, room: &str, sids: &[String]) {
        let Some(r) = self.rooms.get(room) else {
            return;
        };
        for j in r.members.clone() {
            let s = &mut self.participants[j];
            let before = s.data_wants.len();
            s.data_wants.retain(|w| !sids.contains(&w.sid));
            if s.data_wants.len() != before {
                self.reconcile_data(j);
            }
        }
    }

    /// Wants existing data tracks in its room, if it auto-subscribes.
    pub(super) fn want_existing_data_tracks(&mut self, i: usize) {
        if !self.participants[i].auto_subscribe_data {
            return;
        }
        let now = Instant::now();
        let room = self.participants[i].room.clone();
        let sids: Vec<String> = self.rooms[&room]
            .members
            .iter()
            .filter(|&&j| j != i && !self.participants[j].closing)
            .flat_map(|&j| {
                self.participants[j]
                    .data_tracks
                    .iter()
                    .map(|t| t.info.sid.clone())
            })
            .collect();
        let p = &mut self.participants[i];
        for sid in sids {
            p.data_wants.push(DataWant {
                sid,
                since: now,
                allowed: None,
            });
        }
        self.reconcile_data(i);
    }

    /// Brings participant `j`'s data-track subscriptions in line with what it
    /// wants and what publishers allow, and sends its handle map when that
    /// changed (LiveKit's `reconcileDataTrackSubscription` and
    /// `notifyDataTrackSubscriberHandles`).
    pub(super) fn reconcile_data(&mut self, j: usize) {
        let now = Instant::now();
        let room = self.participants[j].room.clone();
        let Some(r) = self.rooms.get(&room) else {
            return;
        };
        let members = r.members.clone();
        let conn = self.participants[j].conn;
        let before = self.handles(j, &members);
        let can_subscribe = self.participants[j].grant.can_subscribe();
        let wants: Vec<String> = self.participants[j]
            .data_wants
            .iter()
            .map(|w| w.sid.clone())
            .collect();
        let mut drop_wants = vec![];
        let mut told = vec![];
        for sid in &wants {
            let found = members.iter().copied().find_map(|k| {
                let t = self.participants[k]
                    .data_tracks
                    .iter()
                    .position(|t| &t.info.sid == sid)?;
                (k != j && !self.participants[k].closing).then_some((k, t))
            });
            let Some((k, t)) = found else {
                let since = self.participants[j]
                    .data_wants
                    .iter()
                    .find(|w| &w.sid == sid)
                    .map(|w| w.since);
                if since.is_some_and(|at| now.duration_since(at) > NOT_FOUND_TIMEOUT) {
                    drop_wants.push(sid.clone());
                }
                continue;
            };
            if !can_subscribe {
                continue;
            }
            let allowed = self.participants[k].permits(sid, &self.participants[j]);
            told.push((sid.clone(), self.participants[k].sid.clone(), allowed));
            let subscribed = self.participants[k].data_tracks[t]
                .subscribers
                .iter()
                .any(|&(c, _)| c == conn);
            if allowed && !subscribed {
                let handle = self.participants[j].next_data_handle();
                self.participants[k].data_tracks[t]
                    .subscribers
                    .push((conn, handle));
                debug!(track = %sid, handle, "data track subscribed");
            } else if !allowed && subscribed {
                self.participants[k].data_tracks[t]
                    .subscribers
                    .retain(|&(c, _)| c != conn);
            }
        }
        // Subscribed to what it no longer wants.
        for &k in &members {
            for t in &mut self.participants[k].data_tracks {
                if !wants.contains(&t.info.sid) {
                    t.subscribers.retain(|&(c, _)| c != conn);
                }
            }
        }
        let s = &mut self.participants[j];
        s.data_wants.retain(|w| !drop_wants.contains(&w.sid));
        for (sid, publisher_sid, allowed) in told {
            let Some(w) = s.data_wants.iter_mut().find(|w| w.sid == sid) else {
                continue;
            };
            if w.allowed != Some(allowed) {
                w.allowed = Some(allowed);
                s.send(signal_response::Message::SubscriptionPermissionUpdate(
                    lk::SubscriptionPermissionUpdate {
                        participant_sid: publisher_sid,
                        track_sid: sid,
                        allowed,
                    },
                ));
            }
        }
        let after = self.handles(j, &members);
        if after != before {
            let sub_handles = after
                .into_iter()
                .map(|(h, (publisher_identity, publisher_sid, track_sid))| {
                    (
                        u32::from(h),
                        lk::data_track_subscriber_handles::PublishedDataTrack {
                            publisher_identity,
                            publisher_sid,
                            track_sid,
                        },
                    )
                })
                .collect();
            self.participants[j].send(signal_response::Message::DataTrackSubscriberHandles(
                lk::DataTrackSubscriberHandles { sub_handles },
            ));
        }
    }

    /// Participant `j`'s handles: handle → (publisher identity, publisher
    /// sid, track sid).
    fn handles(&self, j: usize, members: &[usize]) -> HashMap<u16, (String, String, String)> {
        let conn = self.participants[j].conn;
        let mut out = HashMap::new();
        for &k in members {
            let p = &self.participants[k];
            for t in &p.data_tracks {
                for &(c, h) in &t.subscribers {
                    if c == conn {
                        out.insert(h, (p.identity.clone(), p.sid.clone(), t.info.sid.clone()));
                    }
                }
            }
        }
        out
    }

    /// A packet on a publisher's `_data_track` channel, to each subscriber
    /// of its track under that subscriber's handle.
    pub(super) fn data_track_packet(&mut self, i: usize, bytes: &[u8]) {
        let Some(handle) = packet_handle(bytes) else {
            debug!(identity = %self.participants[i].identity, "unparsable data track packet");
            return;
        };
        let Some(t) = self.participants[i]
            .data_tracks
            .iter()
            .find(|t| t.info.pub_handle == u32::from(handle))
        else {
            return;
        };
        let subscribers = t.subscribers.clone();
        let mut out = bytes.to_vec();
        for (conn, h) in subscribers {
            let Some(&j) = self.by_conn.get(&conn) else {
                continue;
            };
            out[2..4].copy_from_slice(&h.to_be_bytes());
            self.deliver_on(j, CHANNEL, &out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::packet_handle;

    #[test]
    fn header_as_livekit_reads_it() {
        // No extensions: handle 0x0102.
        let mut p = vec![0b0001_1000, 0, 1, 2, 0, 7, 0, 1, 0, 0, 0, 9, 0xaa];
        assert_eq!(packet_handle(&p), Some(0x0102));
        assert_eq!(packet_handle(&p[..11]), None);
        // One extension (id 1, 3 bytes) padded to a word: size word 1.
        p[0] |= 0b100;
        p.truncate(12);
        p.extend([0, 1, 1, 3, 9, 9, 9, 0]);
        assert_eq!(packet_handle(&p), Some(0x0102));
        // Extension longer than the packet.
        p[15] = 200;
        assert_eq!(packet_handle(&p), None);
    }
}

/// How much a subscriber's `_data_track` channel may hold, as LiveKit's
/// unreliable `DataChannelWriter`: past 100 ms at the rate it drains
/// (`datachannel_data_track_target_latency`), and past 8 KiB, writes are
/// dropped rather than queued.
const TARGET_LATENCY: Duration = Duration::from_millis(100);
const MIN_BUFFERED: usize = 8 * 1024;
/// LiveKit's `BitrateCalculator` in busy-only mode: drained bytes over the
/// time the channel was backlogged, in 100 ms windows over 2 s.
const RATE_WINDOW: Duration = Duration::from_millis(100);
const RATE_SPAN: Duration = Duration::from_secs(2);

#[derive(Default)]
pub(super) struct DrainRate {
    windows: std::collections::VecDeque<(Instant, usize, Duration)>,
    active: Option<(Instant, usize, Duration)>,
    bytes: usize,
    busy: Duration,
    last: Option<Instant>,
    last_buffered: usize,
}

impl DrainRate {
    /// Whether a write may go out with `buffered` bytes already waiting.
    pub fn writable(&self, buffered: usize) -> bool {
        let Some(bps) = self.bitrate() else {
            return true;
        };
        let limit = (bps as f64 * TARGET_LATENCY.as_secs_f64() / 8.0) as usize;
        !(buffered > limit && buffered > MIN_BUFFERED)
    }

    /// `written` bytes went into the channel, which now holds `buffered`.
    pub fn wrote(&mut self, written: usize, buffered: usize, now: Instant) {
        let backlogged = buffered > written;
        let busy = match self.last {
            Some(last) if backlogged => now.saturating_duration_since(last),
            _ => Duration::ZERO,
        };
        self.last = Some(now);
        let drained =
            (written as i64 - (buffered as i64 - self.last_buffered as i64)).max(0) as usize;
        self.last_buffered = buffered;
        let active = self.active.get_or_insert((now, 0, Duration::ZERO));
        if now.duration_since(active.0) >= RATE_WINDOW {
            self.windows.push_back(*active);
            *active = (now, 0, Duration::ZERO);
            while let Some(&(start, bytes, busy)) = self.windows.front() {
                if now.duration_since(start) <= RATE_SPAN + RATE_WINDOW {
                    break;
                }
                self.bytes -= bytes;
                self.busy -= busy;
                self.windows.pop_front();
            }
            if self.windows.is_empty() {
                self.bytes = 0;
                self.busy = Duration::ZERO;
            }
        }
        let active = self.active.as_mut().expect("set above");
        self.bytes += drained;
        active.1 += drained;
        self.busy += busy;
        active.2 += busy;
    }

    fn bitrate(&self) -> Option<u64> {
        if self.busy < RATE_WINDOW {
            return None;
        }
        Some(self.bytes as u64 * 8 * 1000 / self.busy.as_millis() as u64)
    }
}
