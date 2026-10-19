//! Several natter nodes as one server, as LiveKit's multi-node mode
//! (`pkg/routing`, `docs/livekit/protocol.md` §7): with `redis` configured,
//! nodes register in Redis with their stats, and each room lives on one
//! node, recorded in Redis when it is first placed. Any node accepts
//! clients and API calls; what concerns a room on another node is passed
//! to that node (signalling WebSockets, Twirp and WHIP requests, proxied
//! over HTTP), so nodes must reach each other's HTTP listener. Media goes
//! from clients straight to the room's node, which hands out its own ICE
//! candidates and TURN.
//!
//! A new room goes to a node chosen as LiveKit's `node_selector` chooses:
//! among nodes serving and heard from in the last 5 s, filtered by load
//! (`cpuload`, `sysload`, `regionaware`), then the lowest by `sort_by`
//! (or the lower of two random ones, `twochoice`). Unlike LiveKit's plain
//! `HSET`, placing a room is compare-and-set, so two nodes racing to place
//! the same room agree on one.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::ws::{Message, WebSocket};
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite;
use tracing::{debug, info, warn};

use super::agent::{Agents, WorkerSummary};
use super::placement::Placement;
use super::token::{self, ClaimGrants, Keys, VideoGrant};
use crate::metrics::Metrics;

const NODES: &str = "natter:nodes";
const ROOMS: &str = "natter:room_node_map";
const WHIP: &str = "natter:whip:";
const AGENT_WORKERS: &str = "natter:agent_workers";
/// LiveKit's `stats_update_interval`.
const STATS_EVERY: Duration = Duration::from_secs(2);
/// LiveKit's `AvailableSeconds`: a node not heard from for this long gets
/// no rooms, and its rooms are placed afresh.
const AVAILABLE_SECONDS: i64 = 5;
/// A room just placed here may not exist yet; it keeps its entry this long.
const PLACED_GRACE: Duration = Duration::from_secs(15);
/// Requests between nodes.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(30);
/// Marks a request passed on by another node, to handle here: a node
/// token, so clients can't skip the routing.
pub const LOCAL_HEADER: &str = "x-natter-local";
/// Identity of the tokens nodes call each other with.
const NODE_IDENTITY: &str = "natter-node:";

/// LiveKit's `redis` keys natter uses.
#[derive(Clone, Debug, Default)]
pub struct RedisConfig {
    pub address: String,
    pub username: String,
    pub password: String,
    pub db: i64,
    pub use_tls: bool,
}

/// LiveKit's `node_selector`.
#[derive(Clone, Debug)]
pub struct SelectorConfig {
    pub kind: String,
    pub sort_by: String,
    pub algorithm: String,
    pub cpu_load_limit: f32,
    pub sysload_limit: f32,
    /// Name, latitude, longitude.
    pub regions: Vec<(String, f64, f64)>,
}

impl Default for SelectorConfig {
    fn default() -> Self {
        SelectorConfig {
            kind: "any".into(),
            sort_by: "random".into(),
            algorithm: "lowest".into(),
            cpu_load_limit: 0.9,
            sysload_limit: 0.9,
            regions: vec![],
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub redis: RedisConfig,
    pub selector: SelectorConfig,
    /// Where other nodes reach this one's HTTP listener.
    pub url: String,
    pub region: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeState {
    Serving,
    ShuttingDown,
}

/// A node as Redis holds it (LiveKit's `livekit.Node`, with the URL other
/// nodes reach it at).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NodeRecord {
    pub id: String,
    pub url: String,
    pub region: String,
    pub state: NodeState,
    pub num_cpus: u32,
    pub stats: NodeStats,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct NodeStats {
    pub started_at: i64,
    pub updated_at: i64,
    pub num_rooms: u64,
    pub num_clients: u64,
    pub num_tracks_in: u64,
    pub num_tracks_out: u64,
    pub cpu_load: f32,
    pub load_avg_last1min: f32,
    pub bytes_in_per_sec: f64,
    pub bytes_out_per_sec: f64,
}

impl NodeRecord {
    /// LiveKit's `IsAvailable`: heard from recently.
    pub fn available(&self, now: i64) -> bool {
        now - self.stats.updated_at < AVAILABLE_SECONDS
    }

    fn sysload(&self) -> f32 {
        self.stats.load_avg_last1min / self.num_cpus.max(1) as f32
    }
}

/// Where a room is handled.
#[derive(Debug)]
pub enum Route {
    Local,
    Remote(NodeRecord),
    /// Nowhere, and it may not be created here.
    Missing,
}

#[derive(Clone)]
pub struct Cluster(Arc<Inner>);

struct Inner {
    keys: Arc<Keys>,
    me: Mutex<NodeRecord>,
    redis: ConnectionManager,
    selector: SelectorConfig,
    /// Rooms this node has claimed in Redis, and when, so it can give back
    /// those that have closed.
    claimed: Mutex<HashMap<String, Instant>>,
    http: hyper_util::client::legacy::Client<
        hyper_util::client::legacy::connect::HttpConnector,
        Body,
    >,
}

fn unix_now() -> i64 {
    crate::util::unix_secs() as i64
}

fn redis_url(c: &RedisConfig) -> String {
    let scheme = if c.use_tls { "rediss" } else { "redis" };
    let auth = match (c.username.is_empty(), c.password.is_empty()) {
        (_, true) => String::new(),
        (true, false) => format!(":{}@", urlencode(&c.password)),
        (false, false) => format!("{}:{}@", urlencode(&c.username), urlencode(&c.password)),
    };
    format!("{scheme}://{auth}{}/{}", c.address, c.db)
}

fn urlencode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

/// Compare-and-set of a room's node: set it to ARGV[3] if it is ARGV[2]
/// ('' for unset). Returns the node it is now.
const PLACE: &str = r"
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if (cur == false and ARGV[2] == '') or cur == ARGV[2] then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
  return ARGV[3]
end
return cur
";

/// Deletes a room's entry if it is still ARGV[2].
const RELEASE: &str = r"
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
  return redis.call('HDEL', KEYS[1], ARGV[1])
end
return 0
";

impl Cluster {
    pub async fn connect(config: Config, node_id: &str, keys: Arc<Keys>) -> Result<Self> {
        if config.redis.use_tls {
            anyhow::bail!("redis.use_tls is not supported yet");
        }
        let cluster = Self::open(config, keys, |url, region| {
            let now = unix_now();
            NodeRecord {
                id: node_id.to_owned(),
                url: url.trim_end_matches('/').to_owned(),
                region,
                state: NodeState::Serving,
                num_cpus: std::thread::available_parallelism().map_or(1, |n| n.get() as u32),
                stats: NodeStats {
                    started_at: now,
                    updated_at: now,
                    ..Default::default()
                },
            }
        })
        .await?;
        cluster.register().await?;
        cluster.remove_dead_nodes().await;
        Ok(cluster)
    }

    /// For reading the cluster without joining it (`list-nodes`).
    pub async fn connect_read_only(config: Config) -> Result<Self> {
        Self::open(config, Arc::new(Keys::default()), |url, region| {
            NodeRecord {
                id: String::new(),
                url,
                region,
                state: NodeState::ShuttingDown,
                num_cpus: 0,
                stats: NodeStats::default(),
            }
        })
        .await
    }

    /// Connects to Redis; `me` makes this node's record from its URL and
    /// region once connected.
    async fn open(
        config: Config,
        keys: Arc<Keys>,
        me: impl FnOnce(String, String) -> NodeRecord,
    ) -> Result<Self> {
        let client = redis::Client::open(redis_url(&config.redis)).context("redis address")?;
        let redis = ConnectionManager::new(client)
            .await
            .with_context(|| format!("connecting to Redis at {}", config.redis.address))?;
        Ok(Cluster(Arc::new(Inner {
            keys,
            me: Mutex::new(me(config.url, config.region)),
            redis,
            selector: config.selector,
            claimed: Mutex::default(),
            http: hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build_http(),
        })))
    }

    /// A short-lived token naming this node, signed with an API key.
    pub fn node_token(&self) -> Option<String> {
        let (key, secret) = self.0.keys.any()?;
        let grants = ClaimGrants {
            identity: format!("{NODE_IDENTITY}{}", self.id()),
            video: Some(VideoGrant {
                agent: true,
                room_admin: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        Some(token::sign(
            key,
            secret,
            &grants,
            Duration::from_secs(60),
            SystemTime::now(),
        ))
    }

    /// Whether a request was passed on by another node (`LOCAL_HEADER`
    /// holds a valid node token).
    pub fn forwarded(&self, headers: &HeaderMap) -> bool {
        headers
            .get(LOCAL_HEADER)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|t| {
                token::verify(t, &self.0.keys, SystemTime::now()).is_ok_and(|(_, g)| {
                    g.identity.starts_with(NODE_IDENTITY)
                        && g.video.is_some_and(|v| v.agent && v.room_admin)
                })
            })
    }

    pub fn id(&self) -> String {
        self.0.me.lock().unwrap().id.clone()
    }

    fn redis(&self) -> ConnectionManager {
        self.0.redis.clone()
    }

    async fn register(&self) -> Result<()> {
        let me = self.0.me.lock().unwrap().clone();
        let json = serde_json::to_string(&me)?;
        let _: () = self.redis().hset(NODES, &me.id, json).await?;
        Ok(())
    }

    /// LiveKit's `RemoveDeadNodes`.
    async fn remove_dead_nodes(&self) {
        let now = unix_now();
        for n in self.nodes().await.unwrap_or_default() {
            if !n.available(now) {
                info!(node = %n.id, "removing dead node");
                let _: Result<(), _> = self.redis().hdel(NODES, &n.id).await;
            }
        }
    }

    pub async fn nodes(&self) -> Result<Vec<NodeRecord>> {
        let values: Vec<String> = self.redis().hvals(NODES).await?;
        Ok(values
            .iter()
            .filter_map(|v| serde_json::from_str(v).ok())
            .collect())
    }

    pub async fn node(&self, id: &str) -> Result<Option<NodeRecord>> {
        let value: Option<String> = self.redis().hget(NODES, id).await?;
        Ok(value.and_then(|v| serde_json::from_str(&v).ok()))
    }

    /// Where `room` is, placing it if it is nowhere (or on a dead node) and
    /// `create`.
    pub async fn route(&self, room: &str, create: bool) -> Result<Route> {
        let me = self.id();
        let owner: Option<String> = self.redis().hget(ROOMS, room).await?;
        let now = unix_now();
        if let Some(id) = &owner {
            if *id == me {
                self.claim(room);
                return Ok(Route::Local);
            }
            if let Some(n) = self.node(id).await?
                && n.available(now)
            {
                return Ok(Route::Remote(n));
            }
        }
        if !create {
            return Ok(Route::Missing);
        }
        let nodes = self.nodes().await?;
        let chosen = select(
            &self.0.selector,
            &self.0.me.lock().unwrap().region,
            nodes.clone(),
            now,
        )
        .map(|n| n.id)
        .unwrap_or_else(|| me.clone());
        let winner: String = redis::Script::new(PLACE)
            .key(ROOMS)
            .arg(room)
            .arg(owner.as_deref().unwrap_or(""))
            .arg(&chosen)
            .invoke_async(&mut self.redis())
            .await?;
        if winner == me {
            info!(room, "room placed on this node");
            self.claim(room);
            return Ok(Route::Local);
        }
        match nodes.into_iter().find(|n| n.id == winner) {
            Some(n) => {
                debug!(room, node = %n.id, "room placed on another node");
                Ok(Route::Remote(n))
            }
            None => match self.node(&winner).await? {
                Some(n) => Ok(Route::Remote(n)),
                None => Ok(Route::Missing),
            },
        }
    }

    fn claim(&self, room: &str) {
        self.0
            .claimed
            .lock()
            .unwrap()
            .insert(room.to_owned(), Instant::now());
    }

    /// Every live node, this one first.
    pub async fn live_nodes(&self) -> Result<Vec<NodeRecord>> {
        let now = unix_now();
        let me = self.id();
        let mut nodes: Vec<NodeRecord> = self
            .nodes()
            .await?
            .into_iter()
            .filter(|n| n.id == me || n.available(now))
            .collect();
        nodes.sort_by_key(|n| n.id != me);
        Ok(nodes)
    }

    /// Records which node a WHIP session (by its participant sid) is on.
    pub async fn set_whip(&self, sid: &str) {
        let me = self.id();
        let r: Result<(), _> = self
            .redis()
            .set_ex(format!("{WHIP}{sid}"), me, 24 * 60 * 60)
            .await;
        if let Err(e) = r {
            warn!("recording WHIP session in Redis: {e}");
        }
    }

    pub async fn whip_node(&self, sid: &str) -> Option<NodeRecord> {
        let id: Option<String> = self.redis().get(format!("{WHIP}{sid}")).await.ok()?;
        let n = self.node(&id?).await.ok()??;
        (n.id != self.id()).then_some(n)
    }

    /// Updates this node's stats every `STATS_EVERY`, and gives back the
    /// rooms it no longer has.
    pub fn start(&self, placement: Arc<Placement>, metrics: Arc<Metrics>, agents: Agents) {
        // The agent workers here, for the other nodes' dispatch: on every
        // change, at most twice a second, and with each heartbeat.
        let cluster = self.clone();
        let workers = agents.clone();
        tokio::spawn(async move {
            loop {
                cluster.publish_workers(&workers).await;
                tokio::select! {
                    _ = workers.changed() => {}
                    _ = tokio::time::sleep(STATS_EVERY) => {}
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
        let cluster = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(STATS_EVERY);
            let mut cpu = CpuSample::read();
            let mut bytes = (0u64, 0u64, Instant::now());
            loop {
                tick.tick().await;
                use std::sync::atomic::Ordering::Relaxed;
                let lk = &metrics.lk;
                let now_bytes = (
                    lk.bytes_in.load(Relaxed),
                    lk.bytes_out.load(Relaxed) + lk.bytes_rtx.load(Relaxed),
                    Instant::now(),
                );
                let secs = now_bytes.2.duration_since(bytes.2).as_secs_f64().max(0.001);
                let next = CpuSample::read();
                {
                    let mut me = cluster.0.me.lock().unwrap();
                    let s = &mut me.stats;
                    s.updated_at = unix_now();
                    s.num_rooms = metrics.rooms.load(Relaxed);
                    s.num_clients = metrics.peers.load(Relaxed);
                    s.num_tracks_in = lk
                        .published
                        .iter()
                        .map(|v| v.load(Relaxed).max(0) as u64)
                        .sum();
                    s.num_tracks_out = lk
                        .subscribed
                        .iter()
                        .map(|v| v.load(Relaxed).max(0) as u64)
                        .sum();
                    s.cpu_load = match (&cpu, &next) {
                        (Some(a), Some(b)) => b.load_since(a),
                        _ => 0.0,
                    };
                    s.load_avg_last1min = load_avg();
                    s.bytes_in_per_sec = now_bytes.0.saturating_sub(bytes.0) as f64 / secs;
                    s.bytes_out_per_sec = now_bytes.1.saturating_sub(bytes.1) as f64 / secs;
                }
                cpu = next;
                bytes = now_bytes;
                if let Err(e) = cluster.register().await {
                    warn!("updating node in Redis: {e}");
                }
                cluster.reconcile(&placement).await;
            }
        });
    }

    /// Rooms this node claimed that it no longer has are freed for others;
    /// rooms it has are claimed again if their entry went (Redis lost it).
    async fn reconcile(&self, placement: &Placement) {
        let me = self.id();
        let here: HashSet<String> = placement.rooms().into_iter().collect();
        let gone: Vec<String> = {
            let mut claimed = self.0.claimed.lock().unwrap();
            for room in &here {
                claimed.entry(room.clone()).or_insert_with(Instant::now);
            }
            let gone = claimed
                .iter()
                .filter(|(room, at)| !here.contains(*room) && at.elapsed() > PLACED_GRACE)
                .map(|(room, _)| room.clone())
                .collect();
            for room in &gone {
                claimed.remove(room);
            }
            gone
        };
        for room in gone {
            let r: Result<i64, _> = redis::Script::new(RELEASE)
                .key(ROOMS)
                .arg(&room)
                .arg(&me)
                .invoke_async(&mut self.redis())
                .await;
            if let Err(e) = r {
                warn!(room, "releasing room in Redis: {e}");
            }
        }
        for room in here {
            let r: Result<String, _> = redis::Script::new(PLACE)
                .key(ROOMS)
                .arg(&room)
                .arg("")
                .arg(&me)
                .invoke_async(&mut self.redis())
                .await;
            if let Ok(owner) = r
                && owner != me
            {
                warn!(room, other = %owner, "room is on this node, but Redis has it elsewhere");
            }
        }
    }

    async fn publish_workers(&self, agents: &Agents) {
        let summary = serde_json::to_string(&agents.summary()).unwrap_or_default();
        let r: Result<(), _> = self.redis().hset(AGENT_WORKERS, self.id(), summary).await;
        if let Err(e) = r {
            warn!("publishing agent workers: {e}");
        }
    }

    /// The other live nodes' agent workers.
    pub async fn remote_workers(&self) -> Vec<(NodeRecord, Vec<WorkerSummary>)> {
        let all: HashMap<String, String> = match self.redis().hgetall(AGENT_WORKERS).await {
            Ok(m) => m,
            Err(e) => {
                warn!("reading agent workers: {e}");
                return vec![];
            }
        };
        let nodes = self.live_nodes().await.unwrap_or_default();
        let me = self.id();
        nodes
            .into_iter()
            .filter(|n| n.id != me)
            .filter_map(|n| {
                let workers = serde_json::from_str(all.get(&n.id)?).ok()?;
                Some((n, workers))
            })
            .collect()
    }

    /// POSTs `body` to another node's internal endpoint, with a node token
    /// (`forward`'s `LOCAL_HEADER`); `left` marks a termination because the
    /// agent left. The response body on success.
    pub async fn call(
        &self,
        node: &NodeRecord,
        path: &str,
        body: Vec<u8>,
        left: bool,
    ) -> Option<axum::body::Bytes> {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/protobuf"),
        );
        if left {
            headers.insert("x-natter-agent-left", HeaderValue::from_static("1"));
        }
        let resp = self
            .forward(node, Method::POST, path, &headers, body.into())
            .await;
        if !resp.status().is_success() {
            return None;
        }
        axum::body::to_bytes(resp.into_body(), 16 << 20).await.ok()
    }

    /// Stops taking new rooms (LiveKit's `Drain`).
    pub async fn drain(&self) {
        self.0.me.lock().unwrap().state = NodeState::ShuttingDown;
        if let Err(e) = self.register().await {
            warn!("marking node draining: {e}");
        }
    }

    /// Leaves the cluster: the node and its rooms' entries go.
    pub async fn stop(&self) {
        let me = self.id();
        let rooms: Vec<String> = self.0.claimed.lock().unwrap().keys().cloned().collect();
        for room in rooms {
            let _: Result<i64, _> = redis::Script::new(RELEASE)
                .key(ROOMS)
                .arg(&room)
                .arg(&me)
                .invoke_async(&mut self.redis())
                .await;
        }
        let _: Result<(), _> = self.redis().hdel(NODES, &me).await;
        let _: Result<(), _> = self.redis().hdel(AGENT_WORKERS, &me).await;
        info!("left the cluster");
    }

    /// Passes an HTTP request to `node` as it came, marked as passed on.
    pub async fn forward(
        &self,
        node: &NodeRecord,
        method: Method,
        path_and_query: &str,
        headers: &HeaderMap,
        body: axum::body::Bytes,
    ) -> Response {
        let mut req = Request::builder()
            .method(method)
            .uri(format!("{}{path_and_query}", node.url));
        for (name, value) in headers {
            if !matches!(
                *name,
                header::HOST | header::CONNECTION | header::TRANSFER_ENCODING
            ) {
                req = req.header(name, value);
            }
        }
        let req = match req
            .header(LOCAL_HEADER, self.node_token().unwrap_or_default())
            .body(Body::from(body))
        {
            Ok(r) => r,
            Err(e) => return (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
        };
        match tokio::time::timeout(FORWARD_TIMEOUT, self.0.http.request(req)).await {
            Ok(Ok(resp)) => {
                let (parts, body) = resp.into_parts();
                match body.collect().await {
                    Ok(b) => Response::from_parts(parts, Body::from(b.to_bytes())),
                    Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
                }
            }
            Ok(Err(e)) => {
                warn!(node = %node.id, "forwarding to node: {e}");
                (
                    StatusCode::BAD_GATEWAY,
                    format!("node {} unreachable", node.id),
                )
                    .into_response()
            }
            Err(_) => (StatusCode::GATEWAY_TIMEOUT, "node did not answer").into_response(),
        }
    }
}

/// Relays a client's WebSocket to the same path on `node` (the room's),
/// with its headers: the node verifies the token itself.
pub async fn proxy_ws(
    client: WebSocket,
    cluster: Cluster,
    node: NodeRecord,
    path_and_query: String,
    headers: HeaderMap,
) {
    let url = format!("{}{path_and_query}", node.url.replacen("http", "ws", 1));
    let mut request =
        match tungstenite::client::IntoClientRequest::into_client_request(url.as_str()) {
            Ok(r) => r,
            Err(e) => {
                warn!("proxy request to {url}: {e}");
                return;
            }
        };
    for name in [header::AUTHORIZATION, header::USER_AGENT] {
        if let Some(v) = headers.get(&name) {
            request.headers_mut().insert(name, v.clone());
        }
    }
    if let Some(v) = cluster
        .node_token()
        .and_then(|t| HeaderValue::from_str(&t).ok())
    {
        request.headers_mut().insert(LOCAL_HEADER, v);
    }
    let config = tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(2 << 20))
        .max_frame_size(Some(2 << 20));
    let upstream =
        match tokio_tungstenite::connect_async_with_config(request, Some(config), true).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                warn!(node = %node.id, "signal relay to node failed: {e}");
                let mut client = client;
                let _ = client.close().await;
                return;
            }
        };
    debug!(node = %node.id, "relaying signalling to the room's node");
    let (mut up_tx, mut up_rx) = upstream.split();
    let (mut down_tx, mut down_rx) = client.split();
    let to_node = async {
        while let Some(Ok(msg)) = down_rx.next().await {
            let out = match msg {
                Message::Text(t) => tungstenite::Message::Text(t.as_str().into()),
                Message::Binary(b) => tungstenite::Message::Binary(b),
                Message::Ping(p) => tungstenite::Message::Ping(p),
                Message::Pong(p) => tungstenite::Message::Pong(p),
                Message::Close(_) => break,
            };
            if up_tx.send(out).await.is_err() {
                break;
            }
        }
        let _ = up_tx.close().await;
    };
    let to_client = async {
        while let Some(Ok(msg)) = up_rx.next().await {
            let out = match msg {
                tungstenite::Message::Text(t) => Message::Text(t.as_str().into()),
                tungstenite::Message::Binary(b) => Message::Binary(b),
                tungstenite::Message::Ping(p) => Message::Ping(p),
                tungstenite::Message::Pong(p) => Message::Pong(p),
                tungstenite::Message::Close(_) => break,
                tungstenite::Message::Frame(_) => continue,
            };
            if down_tx.send(out).await.is_err() {
                break;
            }
        }
        let _ = down_tx.close().await;
    };
    // Either side closing ends the relay, as a closed connection would.
    tokio::select! {
        _ = to_node => {}
        _ = to_client => {}
    }
}

/// LiveKit's `list-nodes`: a table of the nodes.
pub fn node_table(nodes: &[NodeRecord]) -> String {
    let now = unix_now();
    let mut out = format!(
        "{:<16} {:<28} {:<10} {:<14} {:>5} {:>7} {:>6} {:>8} {:>9}\n",
        "ID", "URL", "REGION", "STATE", "CPUS", "ROOMS", "CLIENTS", "CPU", "UPDATED"
    );
    for n in nodes {
        let _ = std::fmt::Write::write_fmt(
            &mut out,
            format_args!(
                "{:<16} {:<28} {:<10} {:<14} {:>5} {:>7} {:>6} {:>7.0}% {:>8}s\n",
                n.id,
                n.url,
                n.region,
                format!("{:?}", n.state),
                n.num_cpus,
                n.stats.num_rooms,
                n.stats.num_clients,
                n.stats.cpu_load * 100.0,
                now - n.stats.updated_at
            ),
        );
    }
    out
}

/// LiveKit's node selectors (`routing/selector`): which node a new room
/// goes to. `None` if no node is available.
pub fn select(
    c: &SelectorConfig,
    my_region: &str,
    nodes: Vec<NodeRecord>,
    now: i64,
) -> Option<NodeRecord> {
    let available: Vec<NodeRecord> = nodes
        .into_iter()
        .filter(|n| n.available(now) && n.state == NodeState::Serving)
        .collect();
    if available.is_empty() {
        return None;
    }
    // Filters keep every node when none passes, as LiveKit's.
    let filter = |nodes: Vec<NodeRecord>, limit: f32, f: fn(&NodeRecord) -> f32| {
        let kept: Vec<NodeRecord> = nodes.iter().filter(|n| f(n) < limit).cloned().collect();
        if kept.is_empty() { nodes } else { kept }
    };
    let mut nodes = match c.kind.as_str() {
        "cpuload" => filter(available, c.cpu_load_limit, |n| n.stats.cpu_load),
        "sysload" | "regionaware" => filter(available, c.sysload_limit, NodeRecord::sysload),
        _ => available,
    };
    if c.kind == "regionaware"
        && let Some(&(_, lat, lon)) = c.regions.iter().find(|(name, ..)| name == my_region)
    {
        let distance = |region: &str| {
            c.regions
                .iter()
                .find(|(name, ..)| name == region)
                .map(|&(_, la, lo)| haversine(lat, lon, la, lo))
        };
        let nearest = nodes
            .iter()
            .filter_map(|n| distance(&n.region))
            .fold(f64::INFINITY, f64::min);
        let near: Vec<NodeRecord> = nodes
            .iter()
            .filter(|n| distance(&n.region) == Some(nearest))
            .cloned()
            .collect();
        if !near.is_empty() {
            nodes = near;
        }
    }
    let key = |n: &NodeRecord| -> f64 {
        match c.sort_by.as_str() {
            "sysload" => f64::from(n.sysload()),
            "cpuload" => f64::from(n.stats.cpu_load),
            "rooms" => n.stats.num_rooms as f64,
            "clients" => n.stats.num_clients as f64,
            "tracks" => (n.stats.num_tracks_in + n.stats.num_tracks_out) as f64,
            "bytespersec" => n.stats.bytes_in_per_sec + n.stats.bytes_out_per_sec,
            _ => 0.0,
        }
    };
    let random = |n: usize| -> usize {
        (u64::from_le_bytes(crate::util::random_bytes()) % n as u64) as usize
    };
    if c.sort_by == "random" || c.sort_by.is_empty() {
        return Some(nodes.swap_remove(random(nodes.len())));
    }
    if c.algorithm == "twochoice" && nodes.len() > 2 {
        let a = nodes.swap_remove(random(nodes.len()));
        let b = nodes.swap_remove(random(nodes.len()));
        return Some(if key(&b) < key(&a) { b } else { a });
    }
    nodes.into_iter().min_by(|a, b| key(a).total_cmp(&key(b)))
}

fn haversine(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
    let (dp, dl) = ((lat2 - lat1).to_radians(), (lon2 - lon1).to_radians());
    let a = (dp / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dl / 2.0).sin().powi(2);
    2.0 * 6371.0 * a.sqrt().asin()
}

/// Aggregate CPU time from /proc/stat, for the load between two samples.
struct CpuSample {
    busy: u64,
    total: u64,
}

impl CpuSample {
    fn read() -> Option<Self> {
        let stat = std::fs::read_to_string("/proc/stat").ok()?;
        let line = stat.lines().next()?.strip_prefix("cpu ")?;
        let v: Vec<u64> = line
            .split_whitespace()
            .filter_map(|x| x.parse().ok())
            .collect();
        let total: u64 = v.iter().sum();
        // idle and iowait
        let idle = v.get(3).copied().unwrap_or(0) + v.get(4).copied().unwrap_or(0);
        Some(CpuSample {
            busy: total - idle,
            total,
        })
    }

    fn load_since(&self, before: &CpuSample) -> f32 {
        let total = self.total.saturating_sub(before.total);
        if total == 0 {
            return 0.0;
        }
        self.busy.saturating_sub(before.busy) as f32 / total as f32
    }
}

fn load_avg() -> f32 {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| s.split_whitespace().next()?.parse().ok())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, rooms: u64, updated: i64, state: NodeState) -> NodeRecord {
        NodeRecord {
            id: id.into(),
            url: String::new(),
            region: String::new(),
            state,
            num_cpus: 4,
            stats: NodeStats {
                updated_at: updated,
                num_rooms: rooms,
                ..Default::default()
            },
        }
    }

    #[test]
    fn selects_as_livekit() {
        let now = 100;
        let c = SelectorConfig {
            sort_by: "rooms".into(),
            ..Default::default()
        };
        let nodes = vec![
            node("busy", 10, now, NodeState::Serving),
            node("idle-but-dead", 0, now - 6, NodeState::Serving),
            node("draining", 0, now, NodeState::ShuttingDown),
            node("quiet", 2, now, NodeState::Serving),
        ];
        assert_eq!(select(&c, "", nodes.clone(), now).unwrap().id, "quiet");
        let only_dead = vec![node("dead", 0, now - 10, NodeState::Serving)];
        assert!(select(&c, "", only_dead, now).is_none());
        let random = SelectorConfig::default();
        for _ in 0..20 {
            let id = select(&random, "", nodes.clone(), now).unwrap().id;
            assert!(id == "busy" || id == "quiet");
        }
    }

    #[test]
    fn region_aware_prefers_the_nearest() {
        let now = 100;
        let c = SelectorConfig {
            kind: "regionaware".into(),
            sort_by: "rooms".into(),
            regions: vec![
                ("us-west".into(), 37.6, -122.4),
                ("us-east".into(), 40.7, -74.0),
                ("eu".into(), 50.1, 8.7),
            ],
            ..Default::default()
        };
        let mut a = node("a", 0, now, NodeState::Serving);
        a.region = "eu".into();
        let mut b = node("b", 5, now, NodeState::Serving);
        b.region = "us-east".into();
        assert_eq!(select(&c, "us-west", vec![a, b], now).unwrap().id, "b");
    }

    #[test]
    fn redis_urls() {
        let c = RedisConfig {
            address: "r:6379".into(),
            password: "p@ss".into(),
            db: 2,
            ..Default::default()
        };
        assert_eq!(redis_url(&c), "redis://:p%40ss@r:6379/2");
    }
}
