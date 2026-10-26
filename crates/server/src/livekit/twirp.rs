//! LiveKit's RoomService over Twirp (`docs/livekit/protocol.md` §3):
//! `POST /twirp/livekit.RoomService/<Rpc>`, protobuf or JSON, with an
//! access token carrying the admin grants. Server SDKs and `lk room …`
//! talk to it unchanged.
//!
//! The work happens on the shard that owns the room (`engine::admin`);
//! this side checks grants, finds the shard and speaks Twirp.

use std::sync::LazyLock;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use livekit_protocol as lk;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, Kind, ReflectMessage as _, SerializeOptions};
use serde::de::DeserializeOwned;
use tokio::sync::oneshot;

use super::cluster;
use super::engine::{AdminError, AdminOp, AdminReply, Command};
use super::signal::RtcState;
use super::token::{self, ClaimGrants};

/// How long an RPC to a shard may take before the caller hears so.
const SHARD_WAIT: Duration = Duration::from_secs(5);
/// PerformRpc waits on a participant: its own timeout, and this more.
const RPC_SLACK: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Protobuf,
    Json,
}

/// A Twirp error response: `{"code", "msg"}` with the code's HTTP status.
/// A bad route also names the route tried, as Twirp's
/// `twirp_invalid_route`.
fn twirp_error(e: &AdminError, route: &str) -> Response {
    let status = match e.code {
        "invalid_argument" | "malformed" | "out_of_range" => StatusCode::BAD_REQUEST,
        "unauthenticated" => StatusCode::UNAUTHORIZED,
        "permission_denied" => StatusCode::FORBIDDEN,
        "not_found" | "bad_route" => StatusCode::NOT_FOUND,
        "already_exists" | "aborted" => StatusCode::CONFLICT,
        "failed_precondition" => StatusCode::PRECONDITION_FAILED,
        "canceled" | "deadline_exceeded" => StatusCode::REQUEST_TIMEOUT,
        "resource_exhausted" => StatusCode::TOO_MANY_REQUESTS,
        "unimplemented" => StatusCode::NOT_IMPLEMENTED,
        "unavailable" => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let mut body = serde_json::json!({ "code": e.code, "msg": e.msg });
    if e.code == "bad_route" {
        body["meta"] = serde_json::json!({ "twirp_invalid_route": format!("POST {route}") });
    }
    (status, axum::Json(body)).into_response()
}

fn err(code: &'static str, msg: impl Into<String>) -> AdminError {
    AdminError::new(code, msg)
}

fn decode<T: Message + Default + DeserializeOwned>(
    enc: Encoding,
    body: &[u8],
) -> Result<T, AdminError> {
    match enc {
        Encoding::Protobuf => T::decode(body).map_err(|e| {
            err(
                "malformed",
                format!("the request could not be decoded: {e}"),
            )
        }),
        // An empty JSON body is an empty message.
        Encoding::Json if body.iter().all(u8::is_ascii_whitespace) => Ok(T::default()),
        Encoding::Json => serde_json::from_slice(body).map_err(|e| {
            err(
                "malformed",
                format!("the json request could not be decoded: {e}"),
            )
        }),
    }
}

/// The messages Twirp answers with, by their full protobuf names.
trait Reply: Message {
    const NAME: &'static str;
}

macro_rules! reply {
    ($($t:ident),* $(,)?) => {
        $(impl Reply for lk::$t {
            const NAME: &'static str = concat!("livekit.", stringify!($t));
        })*
    };
}

reply!(
    Room,
    ListRoomsResponse,
    DeleteRoomResponse,
    ListParticipantsResponse,
    ParticipantInfo,
    RemoveParticipantResponse,
    MuteRoomTrackResponse,
    UpdateSubscriptionsResponse,
    SendDataResponse,
    PerformRpcResponse,
    AgentDispatch,
    ListAgentDispatchResponse,
    EgressInfo,
    ListEgressResponse,
);

static DESCRIPTORS: LazyLock<DescriptorPool> = LazyLock::new(|| {
    DescriptorPool::decode(include_bytes!(concat!(env!("OUT_DIR"), "/livekit.bin")).as_slice())
        .expect("descriptors from build.rs")
});

/// JSON as Twirp's Go server writes it (protojson with proto field names
/// and every field present: zeros, empty lists, null for unset messages),
/// rather than pbjson's camelCase without defaults. SDKs read either;
/// people with `curl` and hand-written clients see LiveKit's keys.
fn protojson<T: Reply>(msg: &T) -> Vec<u8> {
    let desc = DESCRIPTORS
        .get_message_by_name(T::NAME)
        .expect("descriptor for every Reply");
    let message = DynamicMessage::decode(desc, msg.encode_to_vec().as_slice())
        .expect("prost and its own descriptor agree");
    let options = SerializeOptions::new()
        .use_proto_field_name(true)
        .skip_default_fields(false);
    let mut json = message
        .serialize_with_options(serde_json::value::Serializer, &options)
        .expect("serializing to memory");
    unset_messages(&message, &mut json);
    serde_json::to_vec(&json).expect("serializing to memory")
}

/// protojson writes an unset message field as null; prost-reflect leaves it
/// out. Fields of a oneof (proto3 `optional` too) stay out, as in Go.
fn unset_messages(message: &DynamicMessage, json: &mut serde_json::Value) {
    let serde_json::Value::Object(object) = json else {
        return;
    };
    for field in message.descriptor().fields() {
        let Kind::Message(kind) = field.kind() else {
            continue;
        };
        if field.is_map() || field.containing_oneof().is_some() {
            continue;
        }
        let key = field.name();
        let nested = kind.full_name().starts_with("livekit.");
        if field.is_list() {
            if let (Some(items), Some(serde_json::Value::Array(out))) =
                (message.get_field(&field).as_list(), object.get_mut(key))
                && nested
            {
                for (item, out) in items.iter().zip(out) {
                    if let Some(m) = item.as_message() {
                        unset_messages(m, out);
                    }
                }
            }
        } else if !message.has_field(&field) {
            object.insert(key.to_owned(), serde_json::Value::Null);
        } else if nested
            && let (Some(m), Some(out)) =
                (message.get_field(&field).as_message(), object.get_mut(key))
        {
            unset_messages(m, out);
        }
    }
}

fn encode<T: Reply>(enc: Encoding, msg: &T) -> Response {
    match enc {
        Encoding::Protobuf => (
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/protobuf"),
            )],
            msg.encode_to_vec(),
        )
            .into_response(),
        Encoding::Json => (
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            protojson(msg),
        )
            .into_response(),
    }
}

/// The verified grants, or Twirp's unauthenticated (LiveKit answers every
/// auth failure on these APIs so, `twirpAuthError`).
fn grants(state: &RtcState, headers: &HeaderMap) -> Result<ClaimGrants, AdminError> {
    let unauthenticated = |m: &str| err("unauthenticated", m);
    let value = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| unauthenticated("permissions denied"))?;
    let token = value
        .strip_prefix("Bearer ")
        .ok_or_else(|| unauthenticated("invalid authorization header. Must start with Bearer "))?;
    token::verify(token, &state.keys, std::time::SystemTime::now())
        .map(|(_, g)| g)
        .map_err(|e| unauthenticated(&format!("invalid authorization token: {e}")))
}

/// LiveKit's `EnsureAdminPermission`: roomAdmin for this very room.
fn ensure_admin(g: &ClaimGrants, room: &str) -> Result<(), AdminError> {
    let v = g.video.as_ref();
    ensure(v.is_some_and(|v| v.room_admin && v.room == room))
}

fn ensure(allowed: bool) -> Result<(), AdminError> {
    if allowed {
        Ok(())
    } else {
        Err(err("unauthenticated", "permissions denied"))
    }
}

/// Runs `op` on `shard` and waits (up to `wait`) for its answer.
async fn on_shard(
    state: &RtcState,
    shard: usize,
    op: AdminOp,
    wait: Duration,
) -> Result<AdminReply, AdminError> {
    let (tx, rx) = oneshot::channel();
    state.engine.shards[shard]
        .commands
        .send(Command::Admin {
            op: Box::new(op),
            reply: tx,
        })
        .map_err(|_| err("unavailable", "media engine unavailable"))?;
    match tokio::time::timeout(wait, rx).await {
        Ok(Ok(r)) => r,
        Ok(Err(_)) => Err(err("internal", "media engine dropped the request")),
        Err(_) => Err(err("deadline_exceeded", "media engine did not answer")),
    }
}

/// The shard of an existing room.
fn shard_of(state: &RtcState, room: &str) -> Result<usize, AdminError> {
    state
        .engine
        .placement
        .find(room)
        .ok_or_else(|| err("not_found", "requested room does not exist"))
}

async fn in_room(state: &RtcState, room: &str, op: AdminOp) -> Result<AdminReply, AdminError> {
    on_shard(state, shard_of(state, room)?, op, SHARD_WAIT).await
}

/// The shard's reply, if it is `$v`, as `$out` in the request's encoding.
macro_rules! answer {
    ($enc:expr, $reply:expr, $v:ident($x:ident) => $out:expr) => {
        match $reply {
            AdminReply::$v($x) => encode($enc, &$out),
            _ => return Err(err("internal", "unexpected reply")),
        }
    };
}

/// The request's encoding, from its Content-Type.
fn encoding(headers: &HeaderMap) -> Result<Encoding, AdminError> {
    let ct = crate::util::content_type(headers).to_ascii_lowercase();
    match ct.as_str() {
        "application/protobuf" | "application/x-protobuf" => Ok(Encoding::Protobuf),
        "application/json" => Ok(Encoding::Json),
        _ => Err(err("bad_route", format!("unexpected Content-Type: {ct:?}"))),
    }
}

/// The response, recorded in LiveKit's Twirp metrics.
fn finish(
    state: &RtcState,
    service: &str,
    method: &str,
    started: std::time::Instant,
    headers: &HeaderMap,
    result: Result<Response, AdminError>,
) -> Response {
    let (resp, code) = match result {
        Ok(r) => (r, ""),
        Err(e) => (
            twirp_error(&e, &format!("/twirp/livekit.{service}/{method}")),
            e.code,
        ),
    };
    // Recorded once, where the request came in.
    if !forwarded(state, headers) {
        state.metrics.lk.twirp(
            service,
            method,
            resp.status().as_u16(),
            code,
            started.elapsed(),
        );
    }
    resp
}

/// Passed on by another node.
fn forwarded(state: &RtcState, headers: &HeaderMap) -> bool {
    state.cluster.as_ref().is_some_and(|c| c.forwarded(headers))
}

/// This node's rooms, from every shard.
async fn local_rooms(state: &RtcState) -> Result<Vec<lk::Room>, AdminError> {
    let mut rooms = vec![];
    for shard in 0..state.engine.shards.len() {
        if let AdminReply::Rooms(r) = on_shard(state, shard, AdminOp::ListRooms, SHARD_WAIT).await?
        {
            rooms.extend(r);
        }
    }
    Ok(rooms)
}

/// The other nodes' rooms, asked of each with the caller's token. A node
/// that doesn't answer is left out.
async fn remote_rooms(c: &cluster::Cluster, headers: &HeaderMap) -> Vec<lk::Room> {
    let mut out = HeaderMap::new();
    if let Some(auth) = headers.get(header::AUTHORIZATION) {
        out.insert(header::AUTHORIZATION, auth.clone());
    }
    out.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/protobuf"),
    );
    let me = c.id();
    let nodes = c.live_nodes().await.unwrap_or_default();
    let asks = nodes.iter().filter(|n| n.id != me).map(|n| {
        let out = &out;
        async move {
            let resp = c
                .forward(
                    n,
                    axum::http::Method::POST,
                    "/twirp/livekit.RoomService/ListRooms",
                    out,
                    lk::ListRoomsRequest::default().encode_to_vec().into(),
                )
                .await;
            if !resp.status().is_success() {
                tracing::warn!(node = %n.id, status = %resp.status(), "node did not list its rooms");
                return vec![];
            }
            let body = axum::body::to_bytes(resp.into_body(), 64 << 20)
                .await
                .unwrap_or_default();
            lk::ListRoomsResponse::decode(body.as_ref())
                .map(|r| r.rooms)
                .unwrap_or_default()
        }
    });
    futures_util::future::join_all(asks)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// The room a Twirp request concerns, and whether it creates it.
fn request_room(service: &str, method: &str, enc: Encoding, body: &[u8]) -> Option<(String, bool)> {
    macro_rules! room {
        ($t:ty, $f:ident) => {
            decode::<$t>(enc, body).ok().map(|r| r.$f)
        };
    }
    let room = match (service, method) {
        ("RoomService", "CreateRoom") => {
            return room!(lk::CreateRoomRequest, name).map(|r| (r, true));
        }
        ("AgentDispatchService", "CreateDispatch") => {
            return room!(lk::CreateAgentDispatchRequest, room).map(|r| (r, true));
        }
        ("AgentDispatchService", "DeleteDispatch") => room!(lk::DeleteAgentDispatchRequest, room),
        ("AgentDispatchService", "ListDispatch") => room!(lk::ListAgentDispatchRequest, room),
        ("RoomService", "DeleteRoom") => room!(lk::DeleteRoomRequest, room),
        ("RoomService", "ListParticipants") => room!(lk::ListParticipantsRequest, room),
        ("RoomService", "GetParticipant" | "RemoveParticipant") => {
            room!(lk::RoomParticipantIdentity, room)
        }
        ("RoomService", "MutePublishedTrack") => room!(lk::MuteRoomTrackRequest, room),
        ("RoomService", "UpdateParticipant") => room!(lk::UpdateParticipantRequest, room),
        ("RoomService", "UpdateSubscriptions") => room!(lk::UpdateSubscriptionsRequest, room),
        ("RoomService", "SendData") => room!(lk::SendDataRequest, room),
        ("RoomService", "UpdateRoomMetadata") => room!(lk::UpdateRoomMetadataRequest, room),
        ("RoomService", "PerformRpc") => room!(lk::PerformRpcRequest, room),
        ("Egress", "StartTrackEgress") => room!(lk::TrackEgressRequest, room_name),
        _ => None,
    };
    room.map(|r| (r, false))
}

/// A request for a room on another node, passed to it as it came; `None`
/// to handle it here. Rooms are placed only for callers who may create
/// them (so a bad token can't), and CreateDispatch only as LiveKit would
/// (`room.auto_create`).
async fn on_room_node(
    state: &RtcState,
    service: &str,
    method: &str,
    enc: Encoding,
    headers: &HeaderMap,
    body: &Bytes,
) -> Option<Response> {
    let c = state.cluster.as_ref()?;
    if c.forwarded(headers) {
        return None;
    }
    let (room, create) = request_room(service, method, enc, body)?;
    let may_create = create
        && grants(state, headers).is_ok_and(|g| match method {
            "CreateRoom" => g.video.as_ref().is_some_and(|v| v.room_create),
            _ => state.auto_create && ensure_admin(&g, &room).is_ok(),
        });
    match c.route(&room, may_create).await {
        Ok(cluster::Route::Remote(node)) => Some(
            c.forward(
                &node,
                axum::http::Method::POST,
                &format!("/twirp/livekit.{service}/{method}"),
                headers,
                body.clone(),
            )
            .await,
        ),
        Ok(_) => None,
        Err(e) => {
            tracing::warn!("routing room {room:?}: {e}");
            None
        }
    }
}

/// `POST /twirp/livekit.RoomService/{method}`.
pub async fn room_service(
    State(state): State<RtcState>,
    Path(method): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    serve(&state, "RoomService", &method, &headers, body).await
}

async fn serve(
    state: &RtcState,
    service: &'static str,
    method: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let started = std::time::Instant::now();
    let result = async {
        let enc = encoding(headers)?;
        if let Some(resp) = on_room_node(state, service, method, enc, headers, &body).await {
            return Ok(resp);
        }
        match service {
            "RoomService" => dispatch(state, method, enc, headers, &body).await,
            "Egress" => egress(state, method, enc, headers, &body).await,
            _ => agent_dispatch(state, method, enc, headers, &body).await,
        }
    }
    .await;
    finish(state, service, method, started, headers, result)
}

/// `POST /twirp/livekit.AgentDispatchService/{method}`: explicit agent
/// dispatch, as LiveKit's `AgentDispatchService` (roomAdmin for the room).
pub async fn agent_dispatch_service(
    State(state): State<RtcState>,
    Path(method): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    serve(&state, "AgentDispatchService", &method, &headers, body).await
}

/// `POST /twirp/livekit.Egress/{method}`: LiveKit's egress API, for track
/// egress (roomRecord). natter records in the server itself, so it needs
/// no egress service or Redis.
pub async fn egress_service(
    State(state): State<RtcState>,
    Path(method): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    serve(&state, "Egress", &method, &headers, body).await
}

async fn egress(
    state: &RtcState,
    method: &str,
    enc: Encoding,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response, AdminError> {
    let g = grants(state, headers)?;
    ensure(g.video.as_ref().is_some_and(|v| v.room_record))?;
    let not_found = || err("not_found", "egress does not exist");
    Ok(match method {
        "StartTrackEgress" => {
            let req: lk::TrackEgressRequest = decode(enc, body)?;
            let now = crate::egress::unix_nanos();
            let info = lk::EgressInfo {
                egress_id: super::ids::new(super::ids::EGRESS),
                room_name: req.room_name.clone(),
                source_type: lk::EgressSourceType::Sdk as i32,
                status: lk::EgressStatus::EgressStarting as i32,
                started_at: now,
                updated_at: now,
                request: Some(lk::egress_info::Request::Track(req.clone())),
                ..Default::default()
            };
            let reply = in_room(state, &req.room_name, AdminOp::StartTrackEgress(info));
            answer!(enc, reply.await?, Egress(info) => *info)
        }
        "ListEgress" => {
            let req: lk::ListEgressRequest = decode(enc, body)?;
            let items = state.recordings.list(&req);
            if !req.egress_id.is_empty() && items.is_empty() {
                return Err(not_found());
            }
            encode(
                enc,
                &lk::ListEgressResponse {
                    items,
                    next_page_token: None,
                },
            )
        }
        "StopEgress" => {
            let req: lk::StopEgressRequest = decode(enc, body)?;
            let info = state.recordings.get(&req.egress_id).ok_or_else(not_found)?;
            if crate::egress::ended(&info) {
                let status =
                    lk::EgressStatus::try_from(info.status).map_or("UNKNOWN", |s| s.as_str_name());
                return Err(err(
                    "failed_precondition",
                    format!("egress with status {status} cannot be stopped"),
                ));
            }
            in_room(
                state,
                &info.room_name,
                AdminOp::StopEgress(req.egress_id.clone()),
            )
            .await?;
            // The writer finishes the file now; what it was told so far.
            let mut info = state.recordings.get(&req.egress_id).ok_or_else(not_found)?;
            if info.status == lk::EgressStatus::EgressActive as i32 {
                info.status = lk::EgressStatus::EgressEnding as i32;
            }
            encode(enc, &info)
        }
        "StartRoomCompositeEgress"
        | "StartWebEgress"
        | "StartParticipantEgress"
        | "StartTrackCompositeEgress"
        | "StartEgress"
        | "UpdateLayout"
        | "UpdateStream" => {
            return Err(err(
                "unimplemented",
                "natter records single tracks only (StartTrackEgress)",
            ));
        }
        _ => {
            return Err(err(
                "bad_route",
                format!("no handler for path \"/twirp/livekit.Egress/{method}\""),
            ));
        }
    })
}

/// The shard of a room for its dispatches. LiveKit asks the room's node
/// over psrpc, so a room that doesn't exist reads as no node answering.
fn dispatch_shard(state: &RtcState, room: &str) -> Result<usize, AdminError> {
    state
        .engine
        .placement
        .find(room)
        .ok_or_else(|| err("unavailable", "no response from servers"))
}

async fn agent_dispatch(
    state: &RtcState,
    method: &str,
    enc: Encoding,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response, AdminError> {
    let g = grants(state, headers)?;
    // Decodes `$req`, checks roomAdmin for its room and runs `$op` there.
    macro_rules! room_op {
        ($req:ty, $op:ident) => {{
            let req: $req = decode(enc, body)?;
            ensure_admin(&g, &req.room)?;
            let shard = dispatch_shard(state, &req.room)?;
            on_shard(state, shard, AdminOp::$op(req), SHARD_WAIT).await?
        }};
    }
    Ok(match method {
        "CreateDispatch" => {
            let req: lk::CreateAgentDispatchRequest = decode(enc, body)?;
            ensure_admin(&g, &req.room)?;
            super::agent::validate_deployment(&req.deployment)
                .map_err(|e| err("invalid_argument", e))?;
            // LiveKit creates the room if need be (`room.auto_create`).
            if state.auto_create {
                let shard = state.engine.placement.join(&req.room);
                let create = lk::CreateRoomRequest {
                    name: req.room.clone(),
                    ..Default::default()
                };
                on_shard(state, shard, AdminOp::CreateRoom(create), SHARD_WAIT).await?;
            }
            let dispatch = lk::AgentDispatch {
                id: super::ids::new(super::ids::AGENT_DISPATCH),
                agent_name: req.agent_name,
                room: req.room.clone(),
                metadata: req.metadata,
                restart_policy: req.restart_policy,
                deployment: req.deployment,
                attributes: req.attributes,
                state: None,
            };
            let shard = dispatch_shard(state, &req.room)?;
            let reply = on_shard(state, shard, AdminOp::CreateDispatch(dispatch), SHARD_WAIT);
            answer!(enc, reply.await?, Dispatch(d) => d)
        }
        "DeleteDispatch" => {
            answer!(enc, room_op!(lk::DeleteAgentDispatchRequest, DeleteDispatch), Dispatch(d) => d)
        }
        "ListDispatch" => answer!(
            enc,
            room_op!(lk::ListAgentDispatchRequest, ListDispatch),
            Dispatches(agent_dispatches) => lk::ListAgentDispatchResponse { agent_dispatches }
        ),
        _ => {
            return Err(err(
                "bad_route",
                format!("no handler for path \"/twirp/livekit.AgentDispatchService/{method}\""),
            ));
        }
    })
}

async fn dispatch(
    state: &RtcState,
    method: &str,
    enc: Encoding,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response, AdminError> {
    let g = grants(state, headers)?;
    let video = g.video.clone().unwrap_or_default();
    // Decodes `$req`, checks roomAdmin for its room and runs the op (`$op`
    // of the request) on the room's shard.
    macro_rules! room_op {
        ($req:ty, $op:ident) => {
            room_op!($req, |req| AdminOp::$op(req))
        };
        ($req:ty, |$r:ident| $op:expr) => {{
            let $r: $req = decode(enc, body)?;
            ensure_admin(&g, &$r.room)?;
            in_room(state, &$r.room.clone(), $op).await?
        }};
    }
    Ok(match method {
        "CreateRoom" => {
            let req: lk::CreateRoomRequest = decode(enc, body)?;
            ensure(video.room_create)?;
            if req.name.is_empty() {
                return Err(err("invalid_argument", "room name cannot be empty"));
            }
            // The room keeps this reference (or the engine drops it if the
            // room already exists).
            let shard = state.engine.placement.join(&req.name);
            let reply = on_shard(state, shard, AdminOp::CreateRoom(req), SHARD_WAIT);
            answer!(enc, reply.await?, Room(r) => r)
        }
        "ListRooms" => {
            let req: lk::ListRoomsRequest = decode(enc, body)?;
            ensure(video.room_list)?;
            let mut rooms = local_rooms(state).await?;
            // Every other node's too, unless asked by one of them.
            if let Some(c) = &state.cluster
                && !c.forwarded(headers)
            {
                rooms.extend(remote_rooms(c, headers).await);
            }
            if !req.names.is_empty() {
                rooms.retain(|r| req.names.contains(&r.name));
            }
            rooms.sort_by(|a, b| a.name.cmp(&b.name));
            encode(enc, &lk::ListRoomsResponse { rooms })
        }
        "DeleteRoom" => {
            let req: lk::DeleteRoomRequest = decode(enc, body)?;
            ensure(video.room_create)?;
            in_room(state, &req.room, AdminOp::DeleteRoom(req.room.clone())).await?;
            encode(enc, &lk::DeleteRoomResponse {})
        }
        "ListParticipants" => answer!(
            enc,
            room_op!(lk::ListParticipantsRequest, |r| AdminOp::ListParticipants(r.room.clone())),
            Participants(participants) => lk::ListParticipantsResponse { participants }
        ),
        "GetParticipant" => {
            answer!(enc, room_op!(lk::RoomParticipantIdentity, GetParticipant), Participant(p) => p)
        }
        "RemoveParticipant" => {
            room_op!(lk::RoomParticipantIdentity, RemoveParticipant);
            encode(enc, &lk::RemoveParticipantResponse {})
        }
        "MutePublishedTrack" => answer!(
            enc,
            room_op!(lk::MuteRoomTrackRequest, MuteTrack),
            Track(track) => lk::MuteRoomTrackResponse { track }
        ),
        "UpdateParticipant" => answer!(
            enc,
            room_op!(lk::UpdateParticipantRequest, UpdateParticipant),
            Participant(p) => p
        ),
        "UpdateSubscriptions" => {
            room_op!(lk::UpdateSubscriptionsRequest, UpdateSubscriptions);
            encode(enc, &lk::UpdateSubscriptionsResponse {})
        }
        "SendData" => {
            room_op!(lk::SendDataRequest, SendData);
            encode(enc, &lk::SendDataResponse {})
        }
        "UpdateRoomMetadata" => {
            answer!(enc, room_op!(lk::UpdateRoomMetadataRequest, UpdateRoomMetadata), Room(r) => r)
        }
        "PerformRpc" => {
            let req: lk::PerformRpcRequest = decode(enc, body)?;
            ensure_admin(&g, &req.room)?;
            let timeout = match req.response_timeout_ms {
                0 => Duration::from_secs(10),
                ms => Duration::from_millis(ms.into()),
            };
            let shard = shard_of(state, &req.room)?;
            let reply = on_shard(state, shard, AdminOp::PerformRpc(req), timeout + RPC_SLACK);
            answer!(enc, reply.await?, Rpc(payload) => lk::PerformRpcResponse { payload })
        }
        // Cloud-only in LiveKit; its open-source server answers "not
        // implemented" after the same checks.
        "ForwardParticipant" | "MoveParticipant" => {
            let (room, dest) = if method == "ForwardParticipant" {
                let r: lk::ForwardParticipantRequest = decode(enc, body)?;
                (r.room, r.destination_room)
            } else {
                let r: lk::MoveParticipantRequest = decode(enc, body)?;
                (r.room, r.destination_room)
            };
            ensure(video.room_admin && video.room == room && video.destination_room == dest)?;
            if room == dest {
                return Err(err(
                    "invalid_argument",
                    "destination room cannot be the same as source room",
                ));
            }
            return Err(err("unimplemented", "not implemented"));
        }
        _ => {
            return Err(err(
                "bad_route",
                format!("no handler for path \"/twirp/livekit.RoomService/{method}\""),
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shape Twirp's Go server gives a room (checked against LiveKit
    /// 1.13.10): proto field names, zeros and empty lists written out,
    /// enums by name, 64-bit integers as strings, unset messages null.
    #[test]
    fn protojson_as_twirp() {
        let room = lk::Room {
            name: "x".into(),
            creation_time: 5,
            enabled_codecs: vec![lk::Codec {
                mime: "audio/opus".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let json: serde_json::Value = serde_json::from_slice(&protojson(&room)).unwrap();
        assert_eq!(json["name"], "x");
        assert_eq!(json["creation_time"], "5");
        assert_eq!(json["num_participants"], 0);
        assert_eq!(json["active_recording"], false);
        assert_eq!(json["metadata"], "");
        assert_eq!(json["enabled_codecs"][0]["fmtp_line"], "");
        assert!(json["version"].is_null() && json.get("version").is_some());
        assert!(json.get("numParticipants").is_none());

        let p = lk::ParticipantInfo {
            identity: "a".into(),
            kind: lk::participant_info::Kind::Agent as i32,
            permission: Some(lk::ParticipantPermission::default()),
            ..Default::default()
        };
        let json: serde_json::Value = serde_json::from_slice(&protojson(&p)).unwrap();
        assert_eq!(json["kind"], "AGENT");
        assert_eq!(json["state"], "JOINING");
        assert_eq!(
            json["permission"]["can_publish_sources"],
            serde_json::json!([])
        );
        assert_eq!(json["tracks"], serde_json::json!([]));
        assert_eq!(json["attributes"], serde_json::json!({}));
    }
}
