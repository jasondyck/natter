//! Data packets on the data channels: chat, RPC, streams and user
//! packets, routed to their destinations and held for joiners.

use std::time::Instant;

use livekit_protocol as lk;
use lk::{data_packet, data_stream};
use prost::Message as _;
use tracing::debug;

use super::{CachedData, Engine, RELIABLE_CACHE_BYTES, RELIABLE_CACHE_TTL, Side, data_track};

/// How many user-packet nonces a room remembers (LiveKit's deduper size).
const USER_PACKET_NONCES: usize = 100;
/// Unsequenced reliable data held for a joiner (server-sent: RoomService
/// SendData), at most (LiveKit's `cMaxJoiningUnsequencedReliableBytes`).
const JOINING_HELD_BYTES: usize = 100_000;

impl Engine {
    /// Relays a `DataPacket` to the room, stamped with who sent it.
    pub(super) fn data(&mut self, i: usize, label: &str, bytes: &[u8]) {
        let p = &self.participants[i];
        if !p.grant.can_publish_data() {
            return;
        }
        if label == data_track::CHANNEL {
            return self.data_track_packet(i, bytes);
        }
        let Ok(mut packet) = lk::DataPacket::decode(bytes) else {
            debug!(identity = %p.identity, "undecodable data packet");
            return;
        };
        let kind = if label == "_lossy" {
            data_packet::Kind::Lossy
        } else {
            data_packet::Kind::Reliable
        };
        packet.kind = kind as i32;
        // Resent after a resume, or out of order: already relayed.
        if kind == data_packet::Kind::Reliable && packet.sequence > 0 {
            if packet.sequence <= p.last_reliable_seq {
                debug!(identity = %p.identity, seq = packet.sequence, "reliable data already relayed");
                return;
            }
            self.participants[i].last_reliable_seq = packet.sequence;
        }
        let p = &self.participants[i];
        let agent = p.is_agent();
        // As LiveKit's `onDataMessage`: the server vouches for the sender,
        // except that an agent may speak for someone else in chat and text
        // streams (marked generated); transcriptions come only from agents.
        let mut vouch = true;
        // The top-level sid is always the sender's, hidden or not, as
        // LiveKit sets it on every packet.
        packet.participant_sid = p.sid.clone();
        let speaking_for_other = agent
            && !packet.participant_identity.is_empty()
            && packet.participant_identity != p.identity;
        match &mut packet.value {
            Some(data_packet::Value::User(u)) => {
                if p.hidden() {
                    u.participant_sid.clear();
                    u.participant_identity.clear();
                } else {
                    u.participant_sid = p.sid.clone();
                    u.participant_identity = p.identity.clone();
                }
                if packet.destination_identities.is_empty() {
                    packet.destination_identities = u.destination_identities.clone();
                } else {
                    u.destination_identities = packet.destination_identities.clone();
                }
            }
            Some(data_packet::Value::Transcription(_)) if !agent => return,
            Some(data_packet::Value::ChatMessage(c)) if speaking_for_other => {
                c.generated = true;
                vouch = false;
            }
            Some(data_packet::Value::StreamHeader(h)) if speaking_for_other => {
                if let Some(data_stream::header::ContentHeader::TextHeader(t)) =
                    &mut h.content_header
                {
                    t.generated = true;
                    vouch = false;
                }
            }
            _ => {}
        }
        if vouch {
            if p.hidden() {
                packet.participant_identity.clear();
            } else {
                packet.participant_identity = p.identity.clone();
            }
        }
        // The same user packet twice (by nonce) is relayed once.
        if let Some(data_packet::Value::User(u)) = &packet.value
            && let Ok(nonce) = <[u8; 16]>::try_from(u.nonce.as_slice())
            && let Some(r) = self.rooms.get_mut(&self.participants[i].room)
        {
            if r.nonces.contains(&nonce) {
                debug!("duplicate user packet; dropped");
                return;
            }
            if r.nonces.len() == USER_PACKET_NONCES {
                r.nonces.pop_front();
            }
            r.nonces.push_back(nonce);
        }
        // Replies to an RPC the server made (RoomService PerformRpc).
        if let Some(v) = &packet.value
            && matches!(
                v,
                data_packet::Value::RpcAck(_) | data_packet::Value::RpcResponse(_)
            )
            && self.rpc_reply(i, v)
        {
            return;
        }
        let p = &self.participants[i];
        // Server-side only; not forwarded as data.
        if matches!(packet.value, Some(data_packet::Value::Metrics(_))) {
            return;
        }
        // Deprecated, but still honoured: destinations by sid.
        let destination_sids: Vec<String> = match &packet.value {
            Some(data_packet::Value::User(u)) => u.destination_sids.clone(),
            _ => vec![],
        };
        let encoded = packet.encode_to_vec();
        let room = p.room.clone();
        let sender = p.sid.clone();
        let seq = packet.sequence;
        let origin =
            (kind == data_packet::Kind::Reliable && seq > 0).then_some((sender.as_str(), seq));
        if origin.is_some()
            && let Some(r) = self.rooms.get_mut(&room)
        {
            let now = Instant::now();
            r.reliable_bytes += encoded.len();
            r.reliable.push_back(CachedData {
                at: now,
                sender: sender.clone(),
                seq,
                data: encoded.clone(),
                destinations: packet.destination_identities.clone(),
            });
            while let Some(old) = r.reliable.front()
                && (now.duration_since(old.at) > RELIABLE_CACHE_TTL
                    || r.reliable_bytes > RELIABLE_CACHE_BYTES)
            {
                r.reliable_bytes -= old.data.len();
                r.reliable.pop_front();
            }
        }
        let to = (
            packet.destination_identities.as_slice(),
            destination_sids.as_slice(),
        );
        self.deliver_to_room(&room, Some(i), to, kind, &encoded, origin);
    }

    /// Delivers a data packet to the room's members but `except`; only to
    /// those `to` names by identity or sid, when it names any.
    pub(super) fn deliver_to_room(
        &mut self,
        room: &str,
        except: Option<usize>,
        (identities, sids): (&[String], &[String]),
        kind: data_packet::Kind,
        encoded: &[u8],
        origin: Option<(&str, u32)>,
    ) {
        let named = !identities.is_empty() || !sids.is_empty();
        let members = self.rooms[room].members.clone();
        for j in members {
            let o = &self.participants[j];
            if Some(j) == except
                || named && !identities.contains(&o.identity) && !sids.contains(&o.sid)
            {
                continue;
            }
            self.deliver(j, kind, encoded, origin);
        }
    }

    /// A data packet to participant `j`, on its channel for `kind`. Reliable
    /// data for someone still joining waits (LiveKit's `SendDataMessage`):
    /// from a sender (`origin`: its sid and sequence number) it is replayed
    /// from the room's cache, else held.
    pub(super) fn deliver(
        &mut self,
        j: usize,
        kind: data_packet::Kind,
        encoded: &[u8],
        origin: Option<(&str, u32)>,
    ) {
        if kind == data_packet::Kind::Lossy {
            return self.deliver_on(j, "_lossy", encoded);
        }
        let p = &mut self.participants[j];
        if !p.reliable_ready {
            match origin {
                Some((sender, seq)) => {
                    p.joining_first.entry(sender.to_owned()).or_insert(seq);
                }
                None if p.joining_held_bytes + encoded.len() <= JOINING_HELD_BYTES => {
                    p.joining_held_bytes += encoded.len();
                    p.joining_held.push(encoded.to_vec());
                }
                None => debug!(identity = %p.identity, "reliable data dropped while joining"),
            }
            return;
        }
        if let Some((sender, seq)) = origin
            && let Some(&written) = p.joining_written.get(sender)
        {
            if seq <= written {
                return;
            }
            p.joining_written.remove(sender);
        }
        self.deliver_on(j, "_reliable", encoded);
    }

    /// Its reliable channel is open: what it missed while joining goes out
    /// (LiveKit's `replayJoiningReliableMessages`).
    pub(super) fn replay_joining(&mut self, j: usize) {
        let p = &mut self.participants[j];
        if p.reliable_ready {
            return;
        }
        p.reliable_ready = true;
        let first = std::mem::take(&mut p.joining_first);
        let held = std::mem::take(&mut p.joining_held);
        p.joining_held_bytes = 0;
        let identity = p.identity.clone();
        let mut out = vec![];
        if let Some(r) = self.rooms.get(&p.room) {
            for c in &r.reliable {
                let Some(&from) = first.get(&c.sender) else {
                    continue;
                };
                if c.seq < from
                    || (!c.destinations.is_empty() && !c.destinations.contains(&identity))
                {
                    continue;
                }
                let w = p.joining_written.entry(c.sender.clone()).or_insert(c.seq);
                *w = (*w).max(c.seq);
                out.push(c.data.clone());
            }
        }
        if !out.is_empty() || !held.is_empty() {
            debug!(%identity, replayed = out.len(), held = held.len(), "reliable data after joining");
        }
        for data in out.iter().chain(&held) {
            self.deliver_on(j, "_reliable", data);
        }
    }

    /// Bytes to participant `j` on its data channel `label`: the
    /// subscriber PC's, or in single-PC mode the client's own.
    pub(super) fn deliver_on(&mut self, j: usize, label: &str, encoded: &[u8]) {
        let r = &mut self.participants[j];
        let channels = if r.single_pc() {
            &r.pub_channels
        } else {
            &r.sub_channels
        };
        let Some(&(id, _)) = channels.iter().find(|(_, l)| l == label) else {
            return;
        };
        let rated = label == data_track::CHANNEL;
        let now = Instant::now();
        if let Some(mut ch) = sub!(r).rtc.channel(id) {
            if rated && !r.data_rate.writable(ch.buffered_amount()) {
                return;
            }
            let written = ch.write(true, encoded).map_or(0, |_| encoded.len());
            if rated {
                r.data_rate.wrote(written, ch.buffered_amount(), now);
            }
            sub!(r).dirty = true;
            self.touched.push((j, Side::Subscriber));
        }
    }

    // ---- Room-wide updates ----------------------------------------------
}
