//! The engine's side of LiveKit's RoomService (`docs/livekit/protocol.md`
//! §3): what an admin may do to a room and the people in it, as LiveKit's
//! `RoomManager` does it (`roommanager.go`).

use std::time::{Duration, Instant};

use livekit_protocol as lk;
use lk::{data_packet, leave_request, signal_response};
use prost::Message as _;
use tokio::sync::oneshot;
use tracing::{debug, info};

use super::{Engine, Participant, Room, Side, leave, stop_downtrack};

/// What an admin asks of a shard (boxed whole in `Command::Admin`).
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum AdminOp {
    CreateRoom(lk::CreateRoomRequest),
    ListRooms,
    DeleteRoom(String),
    ListParticipants(String),
    GetParticipant(lk::RoomParticipantIdentity),
    RemoveParticipant(lk::RoomParticipantIdentity),
    MuteTrack(lk::MuteRoomTrackRequest),
    UpdateParticipant(lk::UpdateParticipantRequest),
    UpdateSubscriptions(lk::UpdateSubscriptionsRequest),
    SendData(lk::SendDataRequest),
    UpdateRoomMetadata(lk::UpdateRoomMetadataRequest),
    PerformRpc(lk::PerformRpcRequest),
    CreateDispatch(lk::AgentDispatch),
    DeleteDispatch(lk::DeleteAgentDispatchRequest),
    ListDispatch(lk::ListAgentDispatchRequest),
    /// The server is stopping: close every room (SERVER_SHUTDOWN), and
    /// answer once they have all gone.
    Shutdown,
}

#[derive(Debug)]
pub enum AdminReply {
    Room(lk::Room),
    Rooms(Vec<lk::Room>),
    Participants(Vec<lk::ParticipantInfo>),
    Participant(lk::ParticipantInfo),
    Track(Option<lk::TrackInfo>),
    Rpc(String),
    Dispatch(lk::AgentDispatch),
    Dispatches(Vec<lk::AgentDispatch>),
    Done,
}

/// A Twirp error: its code (`not_found`, …) and message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminError {
    pub code: &'static str,
    pub msg: String,
}

impl AdminError {
    pub fn new(code: &'static str, msg: impl Into<String>) -> Self {
        AdminError {
            code,
            msg: msg.into(),
        }
    }
}

pub type AdminResult = Result<AdminReply, AdminError>;
pub type AdminTx = oneshot::Sender<AdminResult>;

fn room_not_found() -> AdminError {
    AdminError::new("not_found", "requested room does not exist")
}

fn participant_not_found() -> AdminError {
    AdminError::new("not_found", "participant does not exist")
}

/// LiveKit's data channel RPC limits and built-in error codes
/// (`protocol/utils/data_channel_rpc.go`).
const RPC_MAX_ROUND_TRIP: Duration = Duration::from_millis(2000);
const RPC_MAX_PAYLOAD: usize = 15360;
const RPC_DEFAULT_TIMEOUT: Duration = Duration::from_millis(10_000);
pub(super) mod rpc_code {
    pub const APPLICATION_ERROR: u32 = 1500;
    pub const CONNECTION_TIMEOUT: u32 = 1501;
    pub const RESPONSE_TIMEOUT: u32 = 1502;
    pub const RECIPIENT_DISCONNECTED: u32 = 1503;
    pub const REQUEST_PAYLOAD_TOO_LARGE: u32 = 1402;
}

/// The Twirp code an RPC error code maps to (LiveKit's `PsrpcError`), and
/// the message of a built-in one.
fn rpc_twirp(code: u32) -> (&'static str, &'static str) {
    match code {
        rpc_code::APPLICATION_ERROR => ("internal", "Application error in method handler"),
        rpc_code::CONNECTION_TIMEOUT => ("canceled", "Connection timeout"),
        rpc_code::RESPONSE_TIMEOUT => ("canceled", "Response timeout"),
        rpc_code::RECIPIENT_DISCONNECTED => ("unavailable", "Recipient disconnected"),
        1504 => ("internal", "Response payload too large"),
        1505 => ("internal", "Failed to send"),
        1400 => ("invalid_argument", "Method not supported at destination"),
        1401 => ("not_found", "Recipient not found"),
        rpc_code::REQUEST_PAYLOAD_TOO_LARGE => ("malformed", "Request payload too large"),
        1403 => ("unimplemented", "RPC not supported by server"),
        1404 => ("unimplemented", "Unsupported RPC version"),
        _ => ("internal", ""),
    }
}

/// An RPC error as LiveKit reports it over Twirp: `RpcError <code>:
/// <message>[: <data>]`.
fn rpc_error_with(code: u32, message: &str, data: &str) -> AdminError {
    let msg = if data.is_empty() {
        format!("RpcError {code}: {message}")
    } else {
        format!("RpcError {code}: {message}: {data}")
    };
    AdminError::new(rpc_twirp(code).0, msg)
}

/// A built-in RPC error.
fn rpc_error(code: u32) -> AdminError {
    rpc_error_with(code, rpc_twirp(code).1, "")
}

/// A server-initiated RPC waiting on its recipient.
pub(super) struct PendingRpc {
    pub conn: super::ConnId,
    pub reply: AdminTx,
    /// Until acknowledged: when it times out as not delivered.
    pub ack_by: Option<Instant>,
    pub respond_by: Instant,
}

impl Engine {
    pub(super) fn admin(&mut self, op: AdminOp, reply: AdminTx) {
        debug!(?op, "admin");
        let result = match op {
            AdminOp::CreateRoom(req) => Ok(self.create_room(req)),
            AdminOp::ListRooms => Ok(AdminReply::Rooms(
                self.rooms
                    .values()
                    .filter(|r| !r.deleted)
                    .map(|r| r.info(&self.participants))
                    .collect(),
            )),
            AdminOp::DeleteRoom(name) => self.delete_room(&name),
            AdminOp::ListParticipants(room) => self.room_index(&room).map(|_| {
                AdminReply::Participants(
                    self.rooms[room.as_str()]
                        .members
                        .iter()
                        .map(|&i| &self.participants[i])
                        .filter(|p| !p.closing)
                        .map(Participant::info)
                        .collect(),
                )
            }),
            AdminOp::GetParticipant(r) => self
                .member(&r.room, &r.identity)
                .map(|i| AdminReply::Participant(self.participants[i].info())),
            AdminOp::RemoveParticipant(r) => self.member(&r.room, &r.identity).map(|i| {
                info!(identity = %r.identity, room = %r.room, "removed by admin");
                self.disconnect(i, lk::DisconnectReason::ParticipantRemoved);
                AdminReply::Done
            }),
            AdminOp::MuteTrack(req) => self.admin_mute(req),
            AdminOp::UpdateParticipant(req) => self.admin_update_participant(req),
            AdminOp::UpdateSubscriptions(req) => self.member(&req.room, &req.identity).map(|i| {
                self.subscription(
                    i,
                    lk::UpdateSubscription {
                        track_sids: req.track_sids,
                        subscribe: req.subscribe,
                        participant_tracks: req.participant_tracks,
                    },
                );
                AdminReply::Done
            }),
            AdminOp::SendData(req) => self.admin_send_data(req),
            AdminOp::UpdateRoomMetadata(req) => self.room_index(&req.room).map(|_| {
                let room = self.rooms.get_mut(req.room.as_str()).expect("checked");
                room.metadata = req.metadata;
                let name = room.name.clone();
                self.room_update_forced(&name);
                AdminReply::Room(self.rooms[&name].info(&self.participants))
            }),
            AdminOp::PerformRpc(req) => {
                // Answered when the recipient responds or times out.
                return self.perform_rpc(req, reply);
            }
            AdminOp::CreateDispatch(d) => self.create_dispatch(d),
            AdminOp::DeleteDispatch(req) => self.delete_dispatch(req),
            AdminOp::ListDispatch(req) => self.list_dispatch(&req),
            AdminOp::Shutdown => {
                info!(rooms = self.rooms.len(), "shutting down");
                // Answered by `reap` once the rooms have gone.
                self.shutdown.get_or_insert_default().push(reply);
                return;
            }
        };
        let _ = reply.send(result);
    }

    /// The room, unless it doesn't exist (or is being deleted).
    pub(super) fn room_index(&self, room: &str) -> Result<(), AdminError> {
        match self.rooms.get(room) {
            Some(r) if !r.deleted => Ok(()),
            _ => Err(room_not_found()),
        }
    }

    fn member(&self, room: &str, identity: &str) -> Result<usize, AdminError> {
        self.room_index(room)?;
        self.rooms[room]
            .members
            .iter()
            .copied()
            .find(|&i| self.participants[i].identity == identity && !self.participants[i].closing)
            .ok_or_else(participant_not_found)
    }

    /// Asks a participant to leave for `reason`, and closes it.
    pub(super) fn disconnect(&mut self, i: usize, reason: lk::DisconnectReason) {
        let p = &mut self.participants[i];
        p.send(leave(reason, leave_request::Action::Disconnect));
        p.closing = true;
    }

    /// LiveKit's CreateRoom: the room as asked, or as it already is. The
    /// signalling side placed it here, taking the reference the room keeps.
    fn create_room(&mut self, req: lk::CreateRoomRequest) -> AdminReply {
        let now = Instant::now();
        if let Some(r) = self.rooms.get(req.name.as_str())
            && !r.deleted
        {
            self.placement.leave(&req.name);
            return AdminReply::Room(r.info(&self.participants));
        }
        let config = lk::RoomConfiguration {
            empty_timeout: req.empty_timeout,
            departure_timeout: req.departure_timeout,
            max_participants: req.max_participants,
            metadata: req.metadata,
            ..Default::default()
        };
        let name: std::sync::Arc<str> = req.name.as_str().into();
        let room = Room::new(name.clone(), Some(&config), &self.room_defaults, now);
        let info = room.info(&self.participants);
        info!(room = %name, "room created by admin");
        self.rooms.insert(name.clone(), room);
        self.membership_changed = true;
        self.hook(super::webhook::ROOM_STARTED, &name, None, None);
        self.start_dispatches(&name, &req.agents);
        AdminReply::Room(info)
    }

    /// Everyone leaves (ROOM_DELETED) and the room goes once they have.
    fn delete_room(&mut self, name: &str) -> AdminResult {
        self.room_index(name)?;
        info!(room = %name, "room deleted by admin");
        let members = self.rooms[name].members.clone();
        for i in members {
            self.disconnect(i, lk::DisconnectReason::RoomDeleted);
        }
        if let Some(r) = self.rooms.get_mut(name) {
            r.deleted = true;
        }
        Ok(AdminReply::Done)
    }

    /// LiveKit's `SetTrackMuted` from an admin: the client is told to mute
    /// (or unmute, if the server allows remote unmute), and the room sees
    /// the change.
    fn admin_mute(&mut self, req: lk::MuteRoomTrackRequest) -> AdminResult {
        let i = self.member(&req.room, &req.identity)?;
        if !req.muted && !self.remote_unmute {
            return Err(AdminError::new(
                "failed_precondition",
                "remote unmute not enabled",
            ));
        }
        let p = &mut self.participants[i];
        p.send(signal_response::Message::Mute(lk::MuteTrackRequest {
            sid: req.track_sid.clone(),
            muted: req.muted,
        }));
        self.mute(
            i,
            lk::MuteTrackRequest {
                sid: req.track_sid.clone(),
                muted: req.muted,
            },
        );
        let track = self.participants[i]
            .published
            .iter()
            .find(|t| t.info.sid == req.track_sid && t.backup_of.is_none())
            .map(|t| t.info.clone());
        Ok(AdminReply::Track(track))
    }

    /// Name, metadata and attributes as a client's own update (without its
    /// permission check), then the permission, revoking what it no longer
    /// allows (LiveKit's `SetPermission`).
    fn admin_update_participant(&mut self, req: lk::UpdateParticipantRequest) -> AdminResult {
        let i = self.member(&req.room, &req.identity)?;
        let p = &mut self.participants[i];
        if !req.name.is_empty() {
            p.name = req.name;
        }
        if !req.metadata.is_empty() {
            p.metadata = req.metadata;
        }
        for (k, v) in req.attributes {
            if v.is_empty() {
                p.attributes.remove(&k);
            } else {
                p.attributes.insert(k, v);
            }
        }
        if let Some(perm) = req.permission {
            let g = &mut p.grant;
            g.can_publish = Some(perm.can_publish);
            g.can_publish_data = Some(perm.can_publish_data);
            g.can_subscribe = Some(perm.can_subscribe);
            g.can_publish_sources = perm
                .can_publish_sources()
                .map(|s| s.as_str_name().to_ascii_lowercase())
                .collect();
            g.can_update_own_metadata = Some(perm.can_update_metadata);
            g.hidden = perm.hidden;
            g.recorder = perm.recorder;
            g.agent = perm.agent;
            g.can_subscribe_metrics = Some(perm.can_subscribe_metrics);
            g.can_manage_agent_session = Some(perm.can_manage_agent_session);
            // Publications it may no longer make.
            let revoked: Vec<String> = p
                .published
                .iter()
                .filter(|t| t.backup_of.is_none() && !p.grant.can_publish_source(t.info.source()))
                .map(|t| t.info.sid.clone())
                .collect();
            for sid in revoked {
                self.unpublish(i, &sid, true);
            }
            // Subscriptions it may no longer have.
            if !self.participants[i].grant.can_subscribe() {
                self.unsubscribe_all(i);
            }
        }
        let p = &mut self.participants[i];
        p.version += 1;
        let room = p.room.clone();
        self.participant_update(&room, i);
        Ok(AdminReply::Participant(self.participants[i].info()))
    }

    /// Ends every subscription participant `i` has.
    fn unsubscribe_all(&mut self, i: usize) {
        let conn = self.participants[i].conn;
        let room = self.participants[i].room.clone();
        let sids: Vec<String> = self.participants[i]
            .downtracks
            .iter()
            .map(|d| d.track_sid.clone())
            .collect();
        for &j in &self.rooms[&room].members.clone() {
            for t in &mut self.participants[j].published {
                t.subscribers.retain(|&c| c != conn);
            }
        }
        let p = &mut self.participants[i];
        for sid in sids {
            stop_downtrack(p, &sid);
        }
        self.touched.push((i, Side::Subscriber));
    }

    /// LiveKit's SendData: a user packet from the server to the room, or
    /// to the identities (or sids) it names.
    fn admin_send_data(&mut self, req: lk::SendDataRequest) -> AdminResult {
        self.room_index(&req.room)?;
        if !req.nonce.is_empty() && req.nonce.len() != 16 {
            return Err(AdminError::new(
                "invalid_argument",
                format!(
                    "nonce should be 16-bytes or not present, got: {} bytes",
                    req.nonce.len()
                ),
            ));
        }
        let kind = req.kind();
        let packet = lk::DataPacket {
            kind: kind as i32,
            destination_identities: req.destination_identities.clone(),
            value: Some(data_packet::Value::User(lk::UserPacket {
                payload: req.data,
                destination_sids: req.destination_sids.clone(),
                destination_identities: req.destination_identities.clone(),
                topic: req.topic,
                nonce: req.nonce,
                ..Default::default()
            })),
            ..Default::default()
        };
        let encoded = packet.encode_to_vec();
        let members = self.rooms[req.room.as_str()].members.clone();
        for j in members {
            let p = &self.participants[j];
            let named = !req.destination_identities.is_empty() || !req.destination_sids.is_empty();
            if named
                && !req.destination_identities.contains(&p.identity)
                && !req.destination_sids.contains(&p.sid)
            {
                continue;
            }
            self.deliver(j, kind, &encoded, None);
        }
        Ok(AdminReply::Done)
    }

    /// LiveKit's `PerformRpc`: an RPC request to one participant, as if
    /// from a caller identified by the request id; its ack and response
    /// come back over data and answer the admin.
    fn perform_rpc(&mut self, req: lk::PerformRpcRequest, reply: AdminTx) {
        if req.destination_identity.is_empty() {
            let _ = reply.send(Err(AdminError::new(
                "invalid_argument",
                "destination identity is required",
            )));
            return;
        }
        let i = match self.member(&req.room, &req.destination_identity) {
            Ok(i) => i,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        if req.payload.len() > RPC_MAX_PAYLOAD {
            let _ = reply.send(Err(rpc_error(rpc_code::REQUEST_PAYLOAD_TOO_LARGE)));
            return;
        }
        let timeout = if req.response_timeout_ms > 0 {
            Duration::from_millis(req.response_timeout_ms.into())
        } else {
            RPC_DEFAULT_TIMEOUT
        };
        let id = uuid::Uuid::new_v4().to_string();
        let rtt = self.participants[i].signal_rtt.unwrap_or_default();
        let packet = lk::DataPacket {
            kind: data_packet::Kind::Reliable as i32,
            participant_identity: id.clone(),
            value: Some(data_packet::Value::RpcRequest(lk::RpcRequest {
                id: id.clone(),
                method: req.method,
                payload: req.payload,
                response_timeout_ms: timeout.saturating_sub(rtt).as_millis() as u32,
                version: 1,
                ..Default::default()
            })),
            ..Default::default()
        };
        let now = Instant::now();
        self.deliver(
            i,
            data_packet::Kind::Reliable,
            &packet.encode_to_vec(),
            None,
        );
        self.rpcs.insert(
            id,
            PendingRpc {
                conn: self.participants[i].conn,
                reply,
                ack_by: Some(now + RPC_MAX_ROUND_TRIP),
                respond_by: now + timeout,
            },
        );
    }

    /// An RPC ack or response from participant `i` to a server RPC.
    /// Whether it was one (and so isn't relayed).
    pub(super) fn rpc_reply(&mut self, i: usize, value: &data_packet::Value) -> bool {
        let conn = self.participants[i].conn;
        match value {
            data_packet::Value::RpcAck(ack) => match self.rpcs.get_mut(&ack.request_id) {
                Some(r) if r.conn == conn => {
                    r.ack_by = None;
                    true
                }
                _ => false,
            },
            data_packet::Value::RpcResponse(res) => {
                // A compressed payload is relayed and the RPC times out,
                // as LiveKit does (requests go out as version 1, which
                // clients answer uncompressed).
                let result = match &res.value {
                    Some(lk::rpc_response::Value::Payload(p)) => Ok(AdminReply::Rpc(p.clone())),
                    Some(lk::rpc_response::Value::Error(e)) => {
                        Err(rpc_error_with(e.code, &e.message, &e.data))
                    }
                    Some(lk::rpc_response::Value::CompressedPayload(_)) => return false,
                    None => Ok(AdminReply::Rpc(String::new())),
                };
                if self
                    .rpcs
                    .get(&res.request_id)
                    .is_none_or(|r| r.conn != conn)
                {
                    return false;
                }
                let r = self.rpcs.remove(&res.request_id).expect("checked");
                let _ = r.reply.send(result);
                true
            }
            _ => false,
        }
    }

    /// Server RPCs that timed out, or whose recipient left.
    pub(super) fn expire_rpcs(&mut self, now: Instant) {
        let gone: Vec<String> = self
            .rpcs
            .iter()
            .filter(|(_, r)| {
                r.ack_by.is_some_and(|by| now >= by)
                    || now >= r.respond_by
                    || !self.by_conn.contains_key(&r.conn)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in gone {
            let Some(r) = self.rpcs.remove(&id) else {
                continue;
            };
            let code = if !self.by_conn.contains_key(&r.conn) {
                rpc_code::RECIPIENT_DISCONNECTED
            } else if r.ack_by.is_some_and(|by| now >= by) {
                rpc_code::CONNECTION_TIMEOUT
            } else {
                rpc_code::RESPONSE_TIMEOUT
            };
            let _ = r.reply.send(Err(rpc_error(code)));
        }
    }
}
