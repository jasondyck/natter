//! `/rtc`: LiveKit's signalling WebSocket (dual peer connection, "v0").
//!
//! The access token comes as `Authorization: Bearer` or `?access_token=`.
//! Once verified, both peer connections are built off the engine thread
//! (creating a DTLS context takes milliseconds) and handed to the engine
//! with the grants. From then on every `SignalRequest` goes to the engine
//! and every `SignalResponse` back, except ping/pong, answered here.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use livekit_protocol as lk;
use lk::{participant_info, signal_request, signal_response};
use tokio::sync::mpsc;
use tracing::debug;

use super::cluster;
use super::engine::{self, Command, Join};
use super::rtc_config;
use super::token::{self, ClaimGrants, Keys};
use super::wire::{self, Format};
use crate::auth::AuthUser;
use crate::util::unix_ms;

/// How long a minted refresh token lasts, and how often one is sent.
const REFRESH_TTL: Duration = Duration::from_secs(10 * 60);
const REFRESH_EVERY: Duration = Duration::from_secs(5 * 60);
/// WebSocket ping frames, as LiveKit's `pingWorker`. A dead client shows as
/// a failed write or read; LiveKit never drops one for not sending signal
/// pings (the Go SDK sends none).
const WS_PING_EVERY: Duration = Duration::from_secs(10);
/// Signal messages queued for a client that isn't reading.
pub const QUEUE: usize = 1024;
/// LiveKit's `limit.signal_message_size_limit`.
const MAX_MESSAGE: usize = 2 << 20;

#[derive(Clone)]
pub struct RtcState {
    pub engine: engine::Handle,
    pub keys: Arc<Keys>,
    /// Agent workers, for `/agent`.
    pub agents: super::agent::Agents,
    /// ICE servers besides natter's own TURN (`rtc.turn_servers`,
    /// `rtc.stun_servers`).
    pub ice_servers: Arc<crate::config::IceServers>,
    /// Joins may create rooms (`room.auto_create`).
    pub auto_create: bool,
    pub metrics: Arc<crate::metrics::Metrics>,
    /// The other nodes, when there are (Redis configured).
    pub cluster: Option<super::cluster::Cluster>,
    pub next_conn: Arc<AtomicU64>,
    /// Live WHIP sessions.
    pub whip: Arc<super::whip::Sessions>,
}

impl RtcState {
    /// ICE servers for participant `id`: natter's TURN, then the others.
    pub fn ice_servers_for(&self, id: &str) -> impl Iterator<Item = lk::IceServer> {
        let turn = self.engine.turn.iter().map(move |t| t.ice_server(id));
        turn.chain(self.ice_servers.for_participant(id))
    }
}

pub struct Verified {
    pub api_key: String,
    pub grants: ClaimGrants,
    pub room: String,
}

/// The access token: `Authorization: Bearer`, else `?access_token=`.
pub(crate) fn bearer(headers: &HeaderMap, query: &HashMap<String, String>) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_owned)
        .or_else(|| query.get("access_token").cloned())
}

pub fn verify(
    headers: &HeaderMap,
    query: &HashMap<String, String>,
    state: &RtcState,
) -> Result<Verified, (StatusCode, String)> {
    let keys: &Keys = &state.keys;
    let token = bearer(headers, query).ok_or((
        StatusCode::UNAUTHORIZED,
        "no permissions to access the room".into(),
    ))?;
    let (api_key, grants) = token::verify(&token, keys, SystemTime::now())
        .map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))?;
    let video = grants.video.clone().unwrap_or_default();
    if !video.room_join || video.room.is_empty() {
        return Err((StatusCode::UNAUTHORIZED, "permissions denied".into()));
    }
    if grants.identity.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "identity cannot be empty".into()));
    }
    // Without auto-create only those who may create rooms make them
    // (LiveKit's `ValidateCreateRoom`); with other nodes, `route` checks.
    if !state.auto_create
        && !video.room_create
        && state.cluster.is_none()
        && state.engine.placement.find(&video.room).is_none()
    {
        return Err((
            StatusCode::NOT_FOUND,
            "requested room does not exist".into(),
        ));
    }
    Ok(Verified {
        api_key,
        room: video.room,
        grants,
    })
}

/// Where a verified client's room is handled: here (`None`) or on another
/// node, placing it if new (and it may be created); or why not at all.
async fn route(
    state: &RtcState,
    headers: &HeaderMap,
    v: &Verified,
    resume: bool,
    place: bool,
) -> Result<Option<cluster::NodeRecord>, Response> {
    let Some(c) = &state.cluster else {
        return Ok(None);
    };
    // Passed on by the node the client reached.
    if c.forwarded(headers) {
        return Ok(None);
    }
    let may_create = state.auto_create || v.grants.video.as_ref().is_some_and(|g| g.room_create);
    match c.route(&v.room, place && !resume && may_create).await {
        Ok(cluster::Route::Local) => Ok(None),
        Ok(cluster::Route::Remote(n)) => Ok(Some(n)),
        // A resume or a validation finds out on its own.
        Ok(cluster::Route::Missing) if resume || (!place && may_create) => Ok(None),
        Ok(cluster::Route::Missing) => {
            Err((StatusCode::NOT_FOUND, "requested room does not exist").into_response())
        }
        Err(e) => {
            tracing::warn!("routing room {:?}: {e}", v.room);
            Err((StatusCode::SERVICE_UNAVAILABLE, "no nodes available").into_response())
        }
    }
}

/// The request's path and query, to pass it on as it came.
fn path_and_query(uri: &axum::http::Uri) -> String {
    uri.path_and_query()
        .map_or_else(|| uri.path().to_owned(), |p| p.as_str().to_owned())
}

/// `GET /rtc/validate`: the same checks as joining, over plain HTTP, so a
/// client can say why a join failed. `/rtc/v1/validate` (`V1`) also checks
/// that a `join_request` given is readable.
pub async fn validate<const V1: bool>(
    State(state): State<RtcState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let checked = verify(&headers, &query, &state).and_then(|v| {
        match query
            .get("join_request")
            .filter(|_| V1)
            .map(|j| join_request(j))
        {
            Some(Err(e)) => Err((StatusCode::BAD_REQUEST, e)),
            _ => Ok(v),
        }
    });
    let mut resp = match checked {
        Ok(v) => match route(&state, &headers, &v, false, false).await {
            Ok(_) => "success".into_response(),
            Err(r) => r,
        },
        Err((code, msg)) => (code, msg).into_response(),
    };
    allow_any_origin(&mut resp);
    resp
}

/// Validation is fetched cross-origin by browser SDKs; LiveKit allows any.
fn allow_any_origin(resp: &mut Response) {
    resp.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        header::HeaderValue::from_static("*"),
    );
}

pub async fn rtc(
    upgrade: WebSocketUpgrade,
    State(state): State<RtcState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    uri: axum::http::Uri,
) -> Response {
    serve(upgrade, state, query, headers, uri, |query, started| {
        let flag = |k: &str, default: bool| {
            query
                .get(k)
                .map_or(default, |v| matches!(v.as_str(), "1" | "true"))
        };
        Ok(Opts {
            auto_subscribe: flag("auto_subscribe", true),
            auto_subscribe_data: flag("auto_subscribe_data_track", false),
            adaptive_stream: flag("adaptive_stream", false),
            allow_pause: query
                .get("subscriber_allow_pause")
                .map(|v| matches!(v.as_str(), "1" | "true")),
            resume: flag("reconnect", false).then(|| query.get("sid").cloned().unwrap_or_default()),
            client: client_info(query),
            reason: query
                .get("reconnect_reason")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            ice_tcp: query.get("sdk").is_none_or(|s| s != "go"),
            join: None,
            started,
        })
    })
    .await
}

/// `/rtc/v1`: single peer connection. Everything `/rtc` takes as query
/// parameters comes in `join_request`, a base64url `WrappedJoinRequest`
/// (optionally gzipped), which may also carry the first offer and tracks
/// to publish at once.
pub async fn rtc_v1(
    upgrade: WebSocketUpgrade,
    State(state): State<RtcState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    uri: axum::http::Uri,
) -> Response {
    serve(upgrade, state, query, headers, uri, |query, started| {
        let join = match query.get("join_request").map(|j| join_request(j)) {
            Some(Ok(j)) => j,
            Some(Err(e)) => return Err((StatusCode::BAD_REQUEST, e.to_string())),
            None => return Err((StatusCode::BAD_REQUEST, "join_request missing".into())),
        };
        let settings = join.connection_settings.unwrap_or(lk::ConnectionSettings {
            auto_subscribe: true,
            ..Default::default()
        });
        Ok(Opts {
            auto_subscribe: settings.auto_subscribe,
            auto_subscribe_data: settings.auto_subscribe_data_track.unwrap_or(false),
            adaptive_stream: settings.adaptive_stream,
            allow_pause: settings.subscriber_allow_pause,
            resume: join.reconnect.then(|| join.participant_sid.clone()),
            client: join.client_info.clone().unwrap_or_default(),
            reason: join.reconnect_reason,
            ice_tcp: join
                .client_info
                .as_ref()
                .is_none_or(|c| c.sdk() != lk::client_info::Sdk::Go),
            join: Some(join),
            started,
        })
    })
    .await
}

/// What `/rtc` and `/rtc/v1` share: checks the token, reads the session's
/// options (`opts`, or why they are refused), and hands the
/// WebSocket to the room's node or starts the session here.
async fn serve(
    upgrade: WebSocketUpgrade,
    state: RtcState,
    query: HashMap<String, String>,
    headers: HeaderMap,
    uri: axum::http::Uri,
    opts: impl FnOnce(
        &HashMap<String, String>,
        std::time::Instant,
    ) -> Result<Opts, (StatusCode, String)>,
) -> Response {
    // Join latency counts from here, before the token is checked, as
    // LiveKit's (`rtcservice.go` `serve`).
    let started = std::time::Instant::now();
    let verified = match verify(&headers, &query, &state) {
        Ok(v) => v,
        Err((code, msg)) => return (code, msg).into_response(),
    };
    let opts = match opts(&query, started) {
        Ok(o) => o,
        Err(refused) => return refused.into_response(),
    };
    match route(&state, &headers, &verified, opts.resume.is_some(), true).await {
        Ok(None) => {}
        Ok(Some(node)) => {
            let cluster = state.cluster.clone().expect("routed by the cluster");
            return relay_to(upgrade, cluster, node, &uri, headers);
        }
        Err(r) => return r,
    }
    upgrade
        .max_message_size(MAX_MESSAGE)
        .max_frame_size(MAX_MESSAGE)
        .on_upgrade(move |ws| session(ws, state, verified, opts))
}

/// Hands the client's WebSocket to the room's node.
fn relay_to(
    upgrade: WebSocketUpgrade,
    cluster: cluster::Cluster,
    node: cluster::NodeRecord,
    uri: &axum::http::Uri,
    headers: HeaderMap,
) -> Response {
    let path = path_and_query(uri);
    upgrade
        .max_message_size(MAX_MESSAGE)
        .max_frame_size(MAX_MESSAGE)
        .on_upgrade(move |ws| cluster::proxy_ws(ws, cluster, node, path, headers))
}

/// `/rtc`'s query parameters as the client's info (LiveKit's
/// `ParseClientInfo`, without its user-agent parsing).
pub fn client_info(query: &HashMap<String, String>) -> lk::ClientInfo {
    use lk::client_info::{Capability, Sdk};
    let get = |k: &str| query.get(k).cloned().unwrap_or_default();
    let int = |k: &str| query.get(k).and_then(|v| v.parse().ok()).unwrap_or(0);
    let sdk = match get("sdk").as_str() {
        "js" => Sdk::Js,
        "ios" | "swift" => Sdk::Swift,
        "android" => Sdk::Android,
        "flutter" => Sdk::Flutter,
        "go" => Sdk::Go,
        "unity" => Sdk::Unity,
        "reactnative" => Sdk::ReactNative,
        "rust" => Sdk::Rust,
        "python" => Sdk::Python,
        "cpp" => Sdk::Cpp,
        "unityweb" => Sdk::UnityWeb,
        "node" => Sdk::Node,
        "esp32" => Sdk::Esp32,
        _ => Sdk::Unknown,
    };
    lk::ClientInfo {
        sdk: sdk as i32,
        protocol: int("protocol"),
        client_protocol: int("client_protocol"),
        version: get("version"),
        os: get("os"),
        os_version: get("os_version"),
        browser: get("browser"),
        browser_version: get("browser_version"),
        device_model: get("device_model"),
        network: get("network"),
        capabilities: get("capabilities")
            .split(',')
            .filter_map(|c| Capability::from_str_name(c.trim()))
            .map(|c| c as i32)
            .collect(),
        ..Default::default()
    }
}

fn join_request(param: &str) -> Result<lk::JoinRequest, String> {
    use base64::Engine;
    use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
    use prost::Message as _;
    let bytes = URL_SAFE_NO_PAD
        .decode(param.trim_end_matches('='))
        .or_else(|_| URL_SAFE.decode(param))
        .map_err(|e| format!("join_request: {e}"))?;
    let wrapped = lk::WrappedJoinRequest::decode(bytes.as_slice())
        .map_err(|e| format!("join_request: {e}"))?;
    let inner = if wrapped.compression == lk::wrapped_join_request::Compression::Gzip as i32 {
        use std::io::Read;
        let mut out = vec![];
        flate2::read::GzDecoder::new(wrapped.join_request.as_slice())
            .take(MAX_MESSAGE as u64)
            .read_to_end(&mut out)
            .map_err(|e| format!("join_request gzip: {e}"))?;
        out
    } else {
        wrapped.join_request
    };
    lk::JoinRequest::decode(inner.as_slice()).map_err(|e| format!("join_request: {e}"))
}

struct Opts {
    auto_subscribe: bool,
    /// Data tracks too (LiveKit's `auto_subscribe_data_track`, off unless
    /// asked for).
    auto_subscribe_data: bool,
    adaptive_stream: bool,
    /// The client's `subscriber_allow_pause`, overriding the server's.
    allow_pause: Option<bool>,
    /// Resuming this participant sid (`reconnect=1&sid=…`).
    resume: Option<String>,
    client: lk::ClientInfo,
    /// `ReconnectReason`, when resuming.
    reason: i32,
    /// The SDK does ICE/TCP: all but Go's (LiveKit `SupportsICETCP`).
    ice_tcp: bool,
    /// Set for `/rtc/v1`: single PC, with whatever the join carried.
    join: Option<lk::JoinRequest>,
    /// When the request came in.
    started: std::time::Instant,
}

async fn session(socket: WebSocket, state: RtcState, v: Verified, opts: Opts) {
    let started = opts.started;
    let protocol = opts.client.protocol;
    let (sink, stream) = socket.split();
    let format = Format::Protobuf;

    let conn = state.next_conn.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = mpsc::channel::<lk::SignalResponse>(QUEUE);
    let ice_servers: Vec<lk::IceServer> = state.ice_servers_for(&conn.to_string()).collect();
    if let Some(sid) = opts.resume.clone() {
        // The session lives where its room does; no room, no session.
        let Some(shard) = state.engine.placement.find(&v.room) else {
            let leave = signal(signal_response::Message::Leave(lk::LeaveRequest {
                reason: lk::DisconnectReason::StateMismatch as i32,
                action: lk::leave_request::Action::Reconnect as i32,
                can_reconnect: true,
                ..Default::default()
            }));
            let mut sink = sink;
            let _ = sink.send(wire::encode(&leave, format)).await;
            let _ = sink.close().await;
            return;
        };
        let commands = state.engine.shards[shard].commands.clone();
        let resume = engine::Resume {
            conn,
            room: v.room.clone(),
            identity: v.grants.identity.clone(),
            sid,
            tx,
            ice_servers,
            reason: opts.reason,
        };
        if commands.send(Command::Resume(Box::new(resume))).is_err() {
            return;
        }
        // A single-PC client's sync state and ICE-restart offer may ride
        // in the request.
        if let Some(j) = opts.join {
            let sync = j.sync_state.map(signal_request::Message::SyncState);
            let offer = j.publisher_offer.map(signal_request::Message::Offer);
            send_signals(&commands, conn, sync.into_iter().chain(offer));
        }
        return relay(
            sink,
            stream,
            rx,
            state,
            commands,
            v,
            conn,
            format,
            true,
            (started, protocol),
        )
        .await;
    }
    // Place the room before building the peer connections: their host
    // candidate is the shard's own port. The engine releases the
    // placement when this participant goes, so every early return below
    // must release it instead.
    let shard = state.engine.placement.join(&v.room);
    let commands = state.engine.shards[shard].commands.clone();
    let (advertised, tcp, media) = (
        state.engine.shards[shard].advertised,
        state.engine.shards[shard].tcp,
        state.engine.media,
    );
    let single = opts.join.is_some();
    let rtcs = tokio::task::spawn_blocking(move || {
        (
            rtc_config::publisher(advertised, tcp, media, single),
            (!single).then(|| rtc_config::subscriber(advertised, tcp, media)),
        )
    })
    .await;
    let Ok((publisher, subscriber)) = rtcs else {
        state.engine.placement.leave(&v.room);
        return;
    };

    let video = v.grants.video.clone().unwrap_or_default();
    let kind = participant_info::Kind::from_str_name(&v.grants.kind.to_ascii_uppercase())
        .unwrap_or(participant_info::Kind::Standard);
    let join = Join {
        conn,
        room: v.room.clone(),
        identity: v.grants.identity.clone(),
        name: v.grants.name.clone(),
        metadata: v.grants.metadata.clone(),
        attributes: v.grants.attributes.clone(),
        kind,
        grant: video,
        room_config: v.grants.room_config.clone(),
        auto_subscribe: opts.auto_subscribe,
        auto_subscribe_data: opts.auto_subscribe_data,
        adaptive_stream: opts.adaptive_stream,
        congestion: media.congestion,
        allow_pause: opts.allow_pause.unwrap_or(media.allow_pause),
        protocol: opts.client.protocol,
        client: opts.client.clone(),
        publisher,
        subscriber,
        tx,
        ice_servers,
        ice_tcp: opts.ice_tcp,
        one_shot: false,
        watch: None,
        started,
    };
    if commands.send(Command::Join(Box::new(join))).is_err() {
        state.engine.placement.leave(&v.room);
        return;
    }
    state.metrics.lk.participant_join.inc(&["rtc_init", "", ""]);
    // Single-PC joins may publish at once: tracks first, then the offer
    // that carries them.
    if let Some(jr) = opts.join {
        let tracks = jr
            .add_track_requests
            .into_iter()
            .map(signal_request::Message::AddTrack);
        let offer = jr.publisher_offer.map(signal_request::Message::Offer);
        send_signals(&commands, conn, tracks.chain(offer));
    }

    relay(
        sink,
        stream,
        rx,
        state,
        commands,
        v,
        conn,
        format,
        false,
        (started, protocol),
    )
    .await;
}

/// Relays one WebSocket to the engine until either side ends it.
#[allow(clippy::too_many_arguments)]
// Clippy would move the engine send into a match guard inside select!.
#[allow(clippy::collapsible_match)]
async fn relay(
    mut sink: SplitSink<WebSocket, Message>,
    mut stream: SplitStream<WebSocket>,
    mut rx: mpsc::Receiver<lk::SignalResponse>,
    state: RtcState,
    commands: engine::Commands,
    v: Verified,
    conn: u64,
    mut format: Format,
    resumed: bool,
    // When the signal connection was made, and the client's protocol.
    (started, protocol): (std::time::Instant, i32),
) {
    let lk = &state.metrics.lk;
    let refresh = |grants: &ClaimGrants| -> Option<lk::SignalResponse> {
        let secret = state.keys.secret(&v.api_key)?;
        let token = token::sign(&v.api_key, secret, grants, REFRESH_TTL, SystemTime::now());
        Some(signal(signal_response::Message::RefreshToken(token)))
    };
    let mut refresh_timer = tokio::time::interval(REFRESH_EVERY);
    let mut ws_ping = tokio::time::interval(WS_PING_EVERY);
    let mut joined = resumed;

    loop {
        tokio::select! {
            msg = stream.next() => {
                let Some(Ok(msg)) = msg else { break };
                if matches!(msg, Message::Close(_)) {
                    break;
                }
                let (req, f) = match wire::decode::<lk::SignalRequest>(&msg, "signal request") {
                    Ok(Some(r)) => r,
                    Ok(None) => continue,
                    Err(e) => {
                        debug!(conn, "{e}");
                        continue;
                    }
                };
                format = f;
                lk.messages.inc(&["signal", "success", "request"]);
                match req.message {
                    Some(signal_request::Message::Ping(ts)) => {
                        let pong = signal(signal_response::Message::Pong(unix_ms()));
                        let _ = ts;
                        if sink.send(wire::encode(&pong, format)).await.is_err() { break }
                    }
                    Some(signal_request::Message::PingReq(ping)) => {
                        let pong = signal(signal_response::Message::PongResp(lk::Pong {
                            last_ping_timestamp: ping.timestamp,
                            timestamp: unix_ms(),
                        }));
                        if sink.send(wire::encode(&pong, format)).await.is_err() { break }
                        // The engine times ICE/TCP by the signalling RTT.
                        if ping.rtt > 0 {
                            let msg = Box::new(signal_request::Message::PingReq(ping));
                            if commands.send(Command::Signal { conn, msg }).is_err() { break }
                        }
                    }
                    Some(msg) => {
                        if commands.send(Command::Signal { conn, msg: Box::new(msg) }).is_err() {
                            break;
                        }
                    }
                    None => {}
                }
            }
            resp = rx.recv() => {
                let Some(resp) = resp else { break };
                // Any leave ends this connection; with RESUME the client
                // comes back on a new one, as with LiveKit.
                let leaving = matches!(&resp.message, Some(signal_response::Message::Leave(_)));
                let is_join = matches!(&resp.message, Some(signal_response::Message::Join(_)));
                let operation = match &resp.message {
                    Some(signal_response::Message::Join(_)) => Some("participant_join"),
                    Some(signal_response::Message::Offer(_)) => Some("offer"),
                    Some(signal_response::Message::Answer(_)) => Some("answer"),
                    _ => None,
                };
                if sink.send(wire::encode(&resp, format)).await.is_err() {
                    break;
                }
                lk.messages.inc(&["signal", "success", "response"]);
                if let Some(op) = operation {
                    lk.service_operation.inc(&[op, "success", ""]);
                }
                if is_join && !joined {
                    joined = true;
                    lk.participant_join.inc(&["signal_connected", "", ""]);
                    lk.join_latency.observe(
                        &[&protocol.to_string()],
                        started.elapsed().as_millis() as f64,
                    );
                    if let Some(r) = refresh(&v.grants)
                        && sink.send(wire::encode(&r, format)).await.is_err()
                    {
                        break;
                    }
                }
                if leaving {
                    break;
                }
            }
            _ = refresh_timer.tick(), if joined => {
                if let Some(r) = refresh(&v.grants)
                    && sink.send(wire::encode(&r, format)).await.is_err()
                {
                    break;
                }
            }
            _ = ws_ping.tick() => {
                if sink.send(Message::Ping(Default::default())).await.is_err() {
                    break;
                }
            }
        }
    }
    let _ = commands.send(Command::Close { conn });
    let _ = sink.close().await;
}

fn signal(message: signal_response::Message) -> lk::SignalResponse {
    lk::SignalResponse {
        message: Some(message),
    }
}

/// Passes `msgs` to the engine as `conn`'s requests.
pub fn send_signals(
    commands: &engine::Commands,
    conn: u64,
    msgs: impl IntoIterator<Item = signal_request::Message>,
) {
    for msg in msgs {
        let _ = commands.send(Command::Signal {
            conn,
            msg: Box::new(msg),
        });
    }
}

/// Longest room name, as natter has always allowed.
const MAX_ROOM_LEN: usize = 64;
/// A minted join token only has to last until the client connects; the
/// session then gets refresh tokens.
const JOIN_TOKEN_TTL: Duration = Duration::from_secs(10 * 60);

/// `POST /api/rtc/token`: natter's signed-in users get a LiveKit token for
/// any room, with their account as identity. natter is the token server
/// for its own web client, as an app backend is for LiveKit.
pub async fn token(
    user: AuthUser,
    State(state): State<RtcState>,
    axum::Json(req): axum::Json<natter_protocol::RtcTokenRequest>,
) -> Response {
    let room = req.room.trim();
    if room.is_empty() || room.chars().count() > MAX_ROOM_LEN {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(natter_protocol::ApiError {
                error: format!("room must be 1–{MAX_ROOM_LEN} characters"),
            }),
        )
            .into_response();
    }
    let Some((key, secret)) = state.keys.any() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let grants = ClaimGrants {
        identity: user.id.to_string(),
        name: user.name,
        video: Some(super::token::VideoGrant {
            room_join: true,
            room: room.to_owned(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let token = token::sign(key, secret, &grants, JOIN_TOKEN_TTL, SystemTime::now());
    let ice_servers = state
        .ice_servers_for(&format!("u{}", user.id))
        .map(|s| natter_protocol::IceServer {
            urls: s.urls,
            username: s.username,
            credential: s.credential,
        })
        .collect();
    axum::Json(natter_protocol::RtcToken { token, ice_servers }).into_response()
}
