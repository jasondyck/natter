//! natter's settings. A LiveKit config file and LiveKit's command-line
//! flags and environment variables work as they do for `livekit-server`
//! (`docs/livekit/protocol.md` §7), so a LiveKit deployment's config moves
//! over unchanged; natter's own `NATTER_*` variables (see README) override
//! them.
//!
//! The YAML (`--config FILE`, `--config-body`/`LIVEKIT_CONFIG`) is checked
//! against LiveKit 1.13.10's schema: a key LiveKit doesn't know is an error,
//! as in LiveKit's strict mode (`--disable-strict-config` turns it into a
//! warning). Keys LiveKit knows but natter doesn't act on are accepted and
//! listed in one warning. LiveKit's flags (`--keys`, `--node-ip`,
//! `--udp-port`, `--bind`, `--dev`…) and its generated ones (`--rtc.tcp_port`
//! or `LIVEKIT_RTC_TCP_PORT`, for the keys natter uses) override the file.
//!
//! Without any LiveKit config, natter's own defaults apply (HTTP on
//! 127.0.0.1:8080); with one, LiveKit's (HTTP on 7880, ICE/TCP on 7881).

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs, UdpSocket};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::Value;
use url::Url;

use crate::livekit;

/// LiveKit's schema, as dotted paths: what natter uses, and what it
/// accepts but ignores (a subtree is ignored whole). From LiveKit 1.13.10's
/// `pkg/config/config.go` and the types it embeds.
const SCHEMA: &[(&str, Use)] = {
    use Use::{Ignored as I, Used as U};
    &[
        ("port", U),
        ("bind_addresses", U),
        ("prometheus_port", U),
        ("prometheus.port", U),
        ("prometheus.username", U),
        ("prometheus.password", U),
        ("debug_handler", I),
        ("webtransport", I),
        // rtc (with mediatransportutil's rtcconfig inlined)
        ("rtc.udp_port", U),
        ("rtc.tcp_port", U),
        ("rtc.port_range_start", U),
        ("rtc.port_range_end", U),
        ("rtc.use_stun_port_as_ice", I),
        ("rtc.node_ip", U),
        ("rtc.stun_servers", U),
        ("rtc.use_external_ip", U),
        ("rtc.require_ipv4", I),
        ("rtc.skip_external_ip_validation", I),
        ("rtc.advertise_internal_ip", I),
        ("rtc.use_ice_lite", I),
        ("rtc.interfaces", I),
        ("rtc.ips", I),
        ("rtc.enable_loopback_candidate", I),
        ("rtc.use_mdns", I),
        ("rtc.external_ip_only", I),
        ("rtc.batch_io", I),
        ("rtc.sctp_min_cwnd", I),
        ("rtc.sctp_fast_rtx_wnd", I),
        ("rtc.sctp_cwnd_ca_step", I),
        ("rtc.host_acceptance_min_wait", I),
        ("rtc.srflx_acceptance_min_wait", I),
        ("rtc.prflx_acceptance_min_wait", I),
        ("rtc.relay_acceptance_min_wait", I),
        ("rtc.force_tcp", I),
        ("rtc.turn_servers", U),
        ("rtc.enable_warp", I),
        ("rtc.strict_acks", I),
        ("rtc.packet_buffer_size", I),
        ("rtc.packet_buffer_size_video", I),
        ("rtc.packet_buffer_size_audio", I),
        ("rtc.pli_throttle", I),
        ("rtc.congestion_control.enabled", U),
        ("rtc.congestion_control.allow_pause", U),
        ("rtc.congestion_control.use_send_side_bwe", U),
        ("rtc.congestion_control.stream_allocator", I),
        ("rtc.congestion_control.remote_bwe", I),
        ("rtc.congestion_control.use_send_side_bwe_interceptor", I),
        ("rtc.congestion_control.send_side_bwe_pacer", I),
        ("rtc.congestion_control.send_side_bwe", I),
        ("rtc.allow_tcp_fallback", U),
        ("rtc.tcp_fallback_rtt_threshold", U),
        ("rtc.allow_udp_unstable_fallback", U),
        ("rtc.reconnect_on_publication_error", I),
        ("rtc.reconnect_on_subscription_error", I),
        ("rtc.reconnect_on_data_channel_error", I),
        ("rtc.data_channel_max_buffered_amount", I),
        ("rtc.datachannel_slow_threshold", I),
        ("rtc.datachannel_lossy_target_latency", I),
        ("rtc.datachannel_data_track_target_latency", I),
        ("rtc.forward_stats", I),
        ("rtc.enable_rtp_stream_restart_detection", I),
        ("redis.address", U),
        ("redis.username", U),
        ("redis.password", U),
        ("redis.db", U),
        ("redis.use_tls", U),
        ("redis.tls", I),
        ("redis.sentinel_master_name", I),
        ("redis.sentinel_username", I),
        ("redis.sentinel_password", I),
        ("redis.sentinel_addresses", I),
        ("redis.cluster_addresses", I),
        ("redis.dial_timeout", I),
        ("redis.read_timeout", I),
        ("redis.write_timeout", I),
        ("redis.max_redirects", I),
        ("redis.pool_timeout", I),
        ("redis.pool_size", I),
        ("audio", I),
        ("video", I),
        ("room.auto_create", U),
        ("room.enabled_codecs", I),
        ("room.max_participants", U),
        ("room.empty_timeout", U),
        ("room.departure_timeout", U),
        ("room.enable_remote_unmute", U),
        ("room.playout_delay", I),
        ("room.sync_streams", I),
        ("room.create_room_timeout", I),
        ("room.create_room_attempts", I),
        ("room.enable_full_room_in_webhooks", I),
        ("room.update_batch_target_size", I),
        ("room.max_metadata_size", I),
        ("room.max_room_name_length", I),
        ("room.max_participant_identity_length", I),
        ("room.room_configurations", I),
        ("turn.enabled", U),
        ("turn.domain", U),
        ("turn.cert_file", U),
        ("turn.key_file", U),
        ("turn.tls_port", U),
        ("turn.udp_port", U),
        ("turn.relay_range_start", I),
        ("turn.relay_range_end", I),
        ("turn.external_tls", U),
        ("turn.bind_addresses", I),
        ("turn.proxy_protocol", I),
        ("turn.proxy_protocol_trusted_cidrs", I),
        ("turn.per_user_relay_allocation_limit", I),
        ("turn.ttl_seconds", I),
        ("turn.allow_restricted_peer_cidrs", I),
        ("turn.deny_peer_cidrs", I),
        ("ingress", I),
        ("sip", I),
        ("webhook.urls", U),
        ("webhook.api_key", U),
        ("webhook.url_notifier", I),
        ("webhook.resource_url_notifier", I),
        ("webhook.filter_params.include_events", U),
        ("webhook.filter_params.exclude_events", U),
        ("node_selector.kind", U),
        ("node_selector.sort_by", U),
        ("node_selector.algorithm", U),
        ("node_selector.cpu_load_limit", U),
        ("node_selector.sysload_limit", U),
        ("node_selector.regions", U),
        ("key_file", U),
        ("keys", U),
        ("region", U),
        ("signal_relay", I),
        ("psrpc", I),
        ("log_level", U),
        ("logging.level", U),
        ("logging.json", I),
        ("logging.sample", I),
        ("logging.component_levels", I),
        ("logging.sample_initial", I),
        ("logging.sample_interval", I),
        ("logging.item_sample_seconds", I),
        ("logging.item_sample_initial", I),
        ("logging.item_sample_interval", I),
        ("logging.pion_level", I),
        ("limit", I),
        ("agents", I),
        ("development", U),
        ("metric", I),
        ("trace", I),
        ("node_stats", I),
        ("enable_data_tracks", I),
        ("enable_participant_data_blob", I),
        ("api", I),
    ]
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Use {
    Used,
    Ignored,
}

/// Used keys that are lists or maps: LiveKit generates no flag for them.
const NOT_SCALAR: &[&str] = &[
    "bind_addresses",
    "keys",
    "rtc.stun_servers",
    "rtc.turn_servers",
    "webhook.urls",
    "webhook.filter_params.include_events",
    "webhook.filter_params.exclude_events",
    "node_selector.regions",
];

/// The keys with generated flags (`--rtc.tcp_port`) and variables
/// (`LIVEKIT_RTC_TCP_PORT`) natter reads.
fn generated() -> impl Iterator<Item = &'static str> {
    SCHEMA
        .iter()
        .filter(|(p, u)| *u == Use::Used && !NOT_SCALAR.contains(p))
        .map(|(p, _)| *p)
}

/// LiveKit's default STUN servers (`rtcconfig.DefaultStunServers`), for
/// finding the external IP.
const DEFAULT_STUN: &[&str] = &["stun.l.google.com:19302", "stun1.l.google.com:19302"];
/// LiveKit's TTL for external TURN credentials from a shared secret.
const EXTERNAL_TURN_TTL: u64 = 14400;
const TURN_MAX_TTL: u64 = 24 * 60 * 60;

/// LiveKit's config, as far as natter uses it. Unknown keys were dealt
/// with by `check`, so nothing here rejects any.
#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct LkConfig {
    port: Option<u16>,
    bind_addresses: Option<Vec<String>>,
    prometheus_port: Option<u16>,
    prometheus: LkPrometheus,
    rtc: LkRtc,
    room: LkRoom,
    turn: LkTurn,
    webhook: LkWebhook,
    key_file: Option<String>,
    keys: Option<BTreeMap<String, String>>,
    region: Option<String>,
    log_level: Option<String>,
    logging: LkLogging,
    development: bool,
    redis: LkRedis,
    node_selector: LkSelector,
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct LkRedis {
    address: String,
    username: String,
    password: String,
    db: i64,
    use_tls: bool,
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct LkSelector {
    kind: Option<String>,
    sort_by: Option<String>,
    algorithm: Option<String>,
    cpu_load_limit: Option<f32>,
    sysload_limit: Option<f32>,
    regions: Vec<LkRegion>,
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct LkRegion {
    name: String,
    lat: f64,
    lon: f64,
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct LkPrometheus {
    port: Option<u16>,
    username: Option<String>,
    password: Option<String>,
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct LkRtc {
    udp_port: Option<PortRange>,
    tcp_port: Option<u16>,
    port_range_start: Option<u16>,
    port_range_end: Option<u16>,
    node_ip: Option<String>,
    stun_servers: Option<Vec<String>>,
    use_external_ip: bool,
    turn_servers: Vec<LkTurnServer>,
    congestion_control: LkCongestion,
    allow_tcp_fallback: Option<bool>,
    tcp_fallback_rtt_threshold: Option<u64>,
    allow_udp_unstable_fallback: Option<bool>,
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct LkCongestion {
    enabled: Option<bool>,
    allow_pause: Option<bool>,
    use_send_side_bwe: Option<bool>,
}

#[derive(Deserialize, Default, Debug, Clone)]
#[serde(default)]
struct LkTurnServer {
    host: String,
    port: u16,
    protocol: String,
    username: String,
    credential: String,
    secret: String,
    secret_file: String,
    ttl: i64,
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct LkRoom {
    auto_create: Option<bool>,
    max_participants: Option<u32>,
    empty_timeout: Option<u32>,
    departure_timeout: Option<u32>,
    enable_remote_unmute: Option<bool>,
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct LkTurn {
    enabled: bool,
    domain: String,
    cert_file: String,
    key_file: String,
    tls_port: u16,
    udp_port: u16,
    external_tls: bool,
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct LkWebhook {
    urls: Vec<String>,
    api_key: String,
    filter_params: LkFilter,
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct LkFilter {
    include_events: Vec<String>,
    exclude_events: Vec<String>,
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct LkLogging {
    level: Option<String>,
}

/// `udp_port`: one port, or `start-end` (LiveKit's `PortRange`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PortRange {
    start: u16,
    end: Option<u16>,
}

impl PortRange {
    fn parse(s: &str) -> Result<Self, String> {
        let port = |p: &str| {
            p.trim()
                .parse::<u16>()
                .map_err(|e| format!("invalid port {p:?}: {e}"))
        };
        match s.split_once('-') {
            Some((a, b)) => {
                let (start, end) = (port(a)?, port(b)?);
                if end <= start {
                    return Err(format!(
                        "end port {end} must be greater than start port {start}"
                    ));
                }
                Ok(PortRange {
                    start,
                    end: Some(end),
                })
            }
            None => Ok(PortRange {
                start: port(s)?,
                end: None,
            }),
        }
    }
}

impl<'de> Deserialize<'de> for PortRange {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Value::deserialize(d)?;
        let s = match v {
            Value::Number(n) => n.to_string(),
            Value::String(s) => s,
            other => {
                return Err(serde::de::Error::custom(format!(
                    "invalid port range {other}"
                )));
            }
        };
        PortRange::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// The command line: LiveKit's flags, or a command.
#[derive(Debug, Default)]
pub struct Cli {
    flags: Vec<(String, Option<String>)>,
    pub command: Vec<String>,
}

/// LiveKit's flags that take no value.
const BOOL_FLAGS: &[&str] = &["dev", "disable-strict-config"];
/// LiveKit's base flags, and the environment variable each also reads.
const BASE_FLAGS: &[(&str, Option<&str>)] = &[
    ("bind", None),
    ("config", None),
    ("config-body", Some("LIVEKIT_CONFIG")),
    ("key-file", None),
    ("keys", Some("LIVEKIT_KEYS")),
    ("region", Some("LIVEKIT_REGION")),
    ("node-ip", Some("NODE_IP")),
    ("udp-port", Some("UDP_PORT")),
    ("redis-host", Some("REDIS_HOST")),
    ("redis-password", Some("REDIS_PASSWORD")),
    ("turn-cert", Some("LIVEKIT_TURN_CERT")),
    ("turn-key", Some("LIVEKIT_TURN_KEY")),
    ("dev", None),
    ("disable-strict-config", None),
];

impl Cli {
    pub fn parse(args: &[String]) -> Result<Self> {
        let mut cli = Cli::default();
        let mut i = 0;
        while i < args.len() {
            let arg = &args[i];
            i += 1;
            let Some(flag) = arg.strip_prefix("--").or_else(|| arg.strip_prefix('-')) else {
                cli.command.push(arg.clone());
                continue;
            };
            if flag.is_empty() || !cli.command.is_empty() {
                cli.command.push(arg.clone());
                continue;
            }
            let (name, value) = match flag.split_once('=') {
                Some((n, v)) => (n.to_owned(), Some(v.to_owned())),
                None => (flag.to_owned(), None),
            };
            let known =
                BASE_FLAGS.iter().any(|(f, _)| *f == name) || generated().any(|p| p == name);
            if !known {
                bail!("flag provided but not defined: -{name}");
            }
            let value = if BOOL_FLAGS.contains(&name.as_str()) {
                value.or(Some("true".into()))
            } else if value.is_some() {
                value
            } else {
                let v = args
                    .get(i)
                    .with_context(|| format!("flag needs an argument: -{name}"))?;
                i += 1;
                Some(v.clone())
            };
            cli.flags.push((name, value));
        }
        Ok(cli)
    }

    /// The last value of `name`, or its environment variable.
    fn get(&self, name: &str) -> Option<String> {
        self.flags
            .iter()
            .rev()
            .find(|(n, _)| n == name)
            .and_then(|(_, v)| v.clone())
            .or_else(|| {
                BASE_FLAGS
                    .iter()
                    .find(|(f, _)| *f == name)
                    .and_then(|(_, env)| *env)
                    .and_then(|e| std::env::var(e).ok())
                    .filter(|v| !v.is_empty())
            })
    }

    fn all(&self, name: &str) -> Vec<String> {
        self.flags
            .iter()
            .filter(|(n, _)| n == name)
            .filter_map(|(_, v)| v.clone())
            .collect()
    }

    fn flag(&self, name: &str) -> bool {
        self.get(name).is_some_and(|v| v == "true" || v == "1")
    }
}

/// External ICE servers handed to clients besides natter's own TURN
/// (LiveKit's `rtc.turn_servers` and `rtc.stun_servers`).
#[derive(Clone, Debug, Default)]
pub struct IceServers {
    turn: Vec<(String, TurnAuth)>,
    stun: Vec<String>,
}

#[derive(Clone, Debug)]
enum TurnAuth {
    Static {
        username: String,
        credential: String,
    },
    /// A shared secret: TTL credentials as coturn's `static-auth-secret`.
    Secret { secret: String, ttl: u64 },
}

impl IceServers {
    /// For one participant (`id` goes into secret-based usernames), as
    /// LiveKit's `iceServersForParticipant`.
    pub fn for_participant(&self, id: &str) -> Vec<livekit_protocol::IceServer> {
        let mut out: Vec<_> = self
            .turn
            .iter()
            .map(|(url, auth)| {
                let (username, credential) = match auth {
                    TurnAuth::Static {
                        username,
                        credential,
                    } => (username.clone(), credential.clone()),
                    TurnAuth::Secret { secret, ttl } => {
                        crate::turn::rest_credential(secret.as_bytes(), id, *ttl)
                    }
                };
                livekit_protocol::IceServer {
                    urls: vec![url.clone()],
                    username,
                    credential,
                }
            })
            .collect();
        if !self.stun.is_empty() {
            out.push(livekit_protocol::IceServer {
                urls: self.stun.iter().map(|s| format!("stun:{s}")).collect(),
                ..Default::default()
            });
        }
        out
    }
}

/// Room defaults from `room:`, for rooms whose creator set none.
#[derive(Clone, Copy, Debug)]
pub struct RoomDefaults {
    pub empty_timeout: u32,
    pub departure_timeout: u32,
    pub max_participants: u32,
    /// Joins may create rooms (`room.auto_create`).
    pub auto_create: bool,
}

impl Default for RoomDefaults {
    fn default() -> Self {
        RoomDefaults {
            empty_timeout: 300,
            departure_timeout: 20,
            max_participants: 0,
            auto_create: true,
        }
    }
}

/// The Prometheus listener (`prometheus:`), with basic auth if both a
/// username and password are set.
#[derive(Clone, Debug)]
pub struct Prometheus {
    pub port: u16,
    pub auth: Option<(String, String)>,
}

/// Settings from the config file, the command line and the environment.
/// See README for what each one is for.
pub struct Settings {
    pub http_addrs: Vec<SocketAddr>,
    pub udp_port: u16,
    /// Media engines (threads), each on its own UDP port from `udp_port`.
    pub shards: usize,
    /// ICE/TCP: the one TCP port every engine shares, or None.
    pub tcp_port: Option<u16>,
    /// The embedded TURN server's UDP and TCP port, if on.
    pub turn_port: Option<u16>,
    pub turn_tls: Option<crate::turn::TlsConfig>,
    pub public_ip: IpAddr,
    pub udp_bind: IpAddr,
    pub static_dir: PathBuf,
    pub origin: Url,
    pub data_dir: PathBuf,
    /// Where track egress writes its files.
    pub egress_dir: PathBuf,
    pub max_peers: usize,
    /// RoomService may unmute tracks (LiveKit `room.enable_remote_unmute`).
    pub remote_unmute: bool,
    /// Participants an engine takes before new rooms go to the next.
    pub shard_fill: usize,
    /// Each engine kept on a CPU of its own (when there are enough).
    pub pin_engines: bool,
    pub dtls_crypto: livekit::rtc_config::DtlsCrypto,
    pub media: livekit::rtc_config::MediaConfig,
    /// Moving clients off UDP when it fails or keeps losing (LiveKit's
    /// `rtc.allow_tcp_fallback`, `rtc.tcp_fallback_rtt_threshold`,
    /// `rtc.allow_udp_unstable_fallback`).
    pub fallback: livekit::fallback::Config,
    /// API keys for LiveKit access tokens (`key: secret`).
    pub keys: livekit::token::Keys,
    pub region: String,
    pub rooms: RoomDefaults,
    pub ice_servers: IceServers,
    pub prometheus: Option<Prometheus>,
    pub webhook: Option<livekit::webhook::Config>,
    /// The log filter, unless RUST_LOG sets one.
    pub log_filter: String,
    /// What to log once logging is up: ignored keys and the like.
    pub notes: Vec<String>,
    /// Multi-node, with Redis (`redis.address`).
    pub cluster: Option<crate::livekit::cluster::Config>,
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn env_parse<T>(key: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    env(key)
        .map(|v| v.parse().with_context(|| format!("{key}={v}")))
        .transpose()
}

/// A port from `key`; `Some(None)` if it is "off".
fn env_port(key: &str) -> Result<Option<Option<u16>>> {
    env(key)
        .map(|v| match v.as_str() {
            "off" => Ok(None),
            p => p.parse().map(Some).with_context(|| format!("{key}={p}")),
        })
        .transpose()
}

/// Checks the YAML tree against LiveKit's schema. Returns the keys LiveKit
/// doesn't know and those natter ignores (set, and not null).
fn check(tree: &Value) -> (Vec<String>, Vec<String>) {
    let (mut unknown, mut ignored) = (vec![], vec![]);
    fn walk(v: &Value, prefix: &str, unknown: &mut Vec<String>, ignored: &mut Vec<String>) {
        let Value::Object(map) = v else { return };
        for (k, child) in map {
            let path = if prefix.is_empty() {
                k.clone()
            } else {
                format!("{prefix}.{k}")
            };
            if let Some((_, u)) = SCHEMA.iter().find(|(p, _)| *p == path) {
                if *u == Use::Ignored && !child.is_null() {
                    ignored.push(path);
                }
            } else if SCHEMA.iter().any(|(p, _)| {
                p.strip_prefix(path.as_str())
                    .is_some_and(|r| r.starts_with('.'))
            }) {
                walk(child, &path, unknown, ignored);
            } else {
                unknown.push(path);
            }
        }
    }
    walk(tree, "", &mut unknown, &mut ignored);
    (unknown, ignored)
}

/// Sets `path` (dotted) in the tree.
fn set(tree: &mut Value, path: &str, value: Value) {
    let mut node = tree;
    let mut parts = path.split('.').peekable();
    while let Some(part) = parts.next() {
        if !node.is_object() {
            *node = Value::Object(Default::default());
        }
        let map = node.as_object_mut().expect("just made an object");
        if parts.peek().is_none() {
            map.insert(part.to_owned(), value);
            return;
        }
        node = map.entry(part).or_insert(Value::Null);
    }
}

/// A flag's or variable's value as YAML would read it: numbers and
/// booleans typed, anything else a string.
fn scalar(s: &str) -> Value {
    serde_saphyr::from_str::<Value>(s)
        .ok()
        .filter(|v| !v.is_object() && !v.is_array() && !v.is_null())
        .unwrap_or_else(|| Value::String(s.to_owned()))
}

/// `key: secret` lines (or YAML) into a map.
fn parse_keys(text: &str) -> Result<BTreeMap<String, String>> {
    let keys = livekit::token::Keys::parse(text).map_err(anyhow::Error::msg)?;
    Ok(keys
        .iter()
        .map(|(k, s)| (k.to_owned(), s.to_owned()))
        .collect())
}

/// The LiveKit config tree: the YAML, with flags and environment applied.
/// `None` if no LiveKit config was given.
fn livekit_tree(cli: &Cli) -> Result<Option<Value>> {
    let text = match (cli.get("config"), cli.get("config-body")) {
        (Some(path), _) => {
            Some(std::fs::read_to_string(&path).with_context(|| format!("reading {path}"))?)
        }
        (None, Some(body)) => Some(body),
        (None, None) => None,
    };
    let mut in_use = text.is_some() || cli.flag("dev");
    let mut tree = match &text {
        Some(t) => serde_saphyr::from_str::<Value>(t)
            .map_err(|e| anyhow::anyhow!("could not parse config: {e}"))?,
        None => Value::Object(Default::default()),
    };
    if tree.is_null() {
        tree = Value::Object(Default::default());
    }

    // LiveKit's generated flags and variables, for the keys natter uses.
    for path in generated() {
        let var = format!("LIVEKIT_{}", path.replace('.', "_").to_ascii_uppercase());
        let value = cli
            .flags
            .iter()
            .rev()
            .find(|(n, _)| n == path)
            .and_then(|(_, v)| v.clone())
            .or_else(|| env(&var).filter(|v| !v.is_empty()));
        if let Some(v) = value {
            set(&mut tree, path, scalar(&v));
            in_use = true;
        }
    }
    // Its base flags (and their variables).
    for (flag, path) in [
        ("key-file", "key_file"),
        ("region", "region"),
        ("node-ip", "rtc.node_ip"),
        ("udp-port", "rtc.udp_port"),
        ("turn-cert", "turn.cert_file"),
        ("turn-key", "turn.key_file"),
        ("redis-host", "redis.address"),
        ("redis-password", "redis.password"),
    ] {
        if let Some(v) = cli.get(flag) {
            set(&mut tree, path, Value::String(v));
            in_use = true;
        }
    }
    let binds = cli.all("bind");
    if !binds.is_empty() {
        set(
            &mut tree,
            "bind_addresses",
            Value::Array(binds.into_iter().map(Value::String).collect()),
        );
        in_use = true;
    }
    if cli.flag("dev") {
        set(&mut tree, "development", Value::Bool(true));
    }
    if let Some(keys) = cli.get("keys") {
        let keys = parse_keys(&keys).context(
            "Could not parse keys, it needs to be exactly, \"key: secret\", including the space",
        )?;
        set(&mut tree, "keys", serde_json::to_value(keys)?);
    }
    Ok(in_use.then_some(tree))
}

impl Settings {
    pub fn load(cli: &Cli) -> Result<Settings> {
        let mut notes = vec![];
        let tree = livekit_tree(cli)?;
        let lk_mode = tree.is_some();
        let lk: LkConfig = match &tree {
            Some(tree) => {
                let (unknown, ignored) = check(tree);
                if !unknown.is_empty() {
                    let msg = format!(
                        "could not parse config: keys LiveKit doesn't have: {}",
                        unknown.join(", ")
                    );
                    if cli.flag("disable-strict-config") {
                        notes.push(msg);
                    } else {
                        bail!("{msg} (--disable-strict-config to ignore them)");
                    }
                }
                if !ignored.is_empty() {
                    notes.push(format!(
                        "LiveKit config keys natter does not act on, ignored: {}",
                        ignored.join(", ")
                    ));
                }
                serde_json::from_value(tree.clone())
                    .map_err(|e| anyhow::anyhow!("could not parse config: {e}"))?
            }
            None => LkConfig::default(),
        };
        let dev = lk.development;

        // API keys: LIVEKIT_KEYS, `keys`, or `key_file`; devkey in dev mode.
        let mut keys = lk.keys.clone().unwrap_or_default();
        if keys.is_empty()
            && let Some(file) = &lk.key_file
        {
            let text = std::fs::read_to_string(file).with_context(|| format!("reading {file}"))?;
            keys = parse_keys(&text).with_context(|| format!("parsing {file}"))?;
        }
        if keys.is_empty() && !lk_mode {
            keys = parse_keys(&env("LIVEKIT_KEYS").unwrap_or_default()).context("LIVEKIT_KEYS")?;
        }
        let mut binds = lk.bind_addresses.clone();
        // Where HTTP listens when nothing more specific says: the image sets
        // 0.0.0.0, keeping the port the config (or the default) gives.
        let http_bind: Option<IpAddr> = env_parse("NATTER_HTTP_BIND")?;
        if dev && keys.is_empty() {
            notes.push(
                "no keys provided, using placeholder keys: API Key devkey, API Secret secret"
                    .into(),
            );
            keys.insert("devkey".into(), "secret".into());
            if http_bind.is_none() {
                binds.get_or_insert_with(|| vec!["127.0.0.1".into(), "::1".into()]);
            }
        }
        let keys_text: String = keys.iter().map(|(k, s)| format!("{k}: {s}\n")).collect();
        let keys = livekit::token::Keys::parse(&keys_text).map_err(anyhow::Error::msg)?;

        // HTTP.
        let http_addrs = match env_parse("NATTER_HTTP_ADDR")? {
            Some(a) => vec![a],
            None if lk_mode => {
                let port = lk.port.unwrap_or(7880);
                match binds.filter(|b| !b.is_empty()) {
                    Some(b) => b
                        .iter()
                        .map(|ip| {
                            ip.parse::<IpAddr>()
                                .map(|ip| SocketAddr::new(ip, port))
                                .with_context(|| format!("bind address {ip:?}"))
                        })
                        .collect::<Result<_>>()?,
                    None => vec![SocketAddr::new(
                        http_bind.unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
                        port,
                    )],
                }
            }
            None => vec![SocketAddr::new(
                http_bind.unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST)),
                8080,
            )],
        };

        // Media ports: one engine per port of the range.
        let range =
            lk.rtc.udp_port.or(lk
                .rtc
                .port_range_start
                .filter(|&s| s > 0)
                .map(|start| PortRange {
                    start,
                    end: lk.rtc.port_range_end.filter(|&e| e > start),
                }));
        // LiveKit's defaults: one port (7882) in dev mode, else 50000-60000.
        let range = range.unwrap_or(match (lk_mode, dev) {
            (true, true) => PortRange {
                start: 7882,
                end: None,
            },
            (true, false) => PortRange {
                start: 50000,
                end: Some(60000),
            },
            (false, _) => PortRange {
                start: 50000,
                end: None,
            },
        });
        let udp_port = env_parse("NATTER_UDP_PORT")?.unwrap_or(range.start);
        let cores = std::thread::available_parallelism().map_or(1, usize::from);
        let shards = match env_parse("NATTER_SHARDS")? {
            Some(n) => n,
            None if env("NATTER_UDP_PORT").is_some() || !lk_mode => cores,
            // One engine per port of the range; LiveKit's single port is
            // one port for everything.
            None => match range.end {
                Some(end) => cores.min(usize::from(end - range.start) + 1),
                None => 1,
            },
        };
        let tcp_port = match env_port("NATTER_TCP_PORT")? {
            Some(p) => p,
            None => match lk.rtc.tcp_port {
                Some(0) => None,
                Some(p) => Some(p),
                None if lk_mode => Some(7881),
                // The UDP ports' numbers, over TCP.
                None => Some(udp_port),
            },
        };

        // The advertised address, and where media binds.
        // `explicit`: the operator named the address, so it must be bound.
        let (public_ip, explicit) = match env_parse("NATTER_PUBLIC_IP")? {
            Some(ip) => (ip, true),
            None => match (&lk.rtc.node_ip, lk.rtc.use_external_ip) {
                (Some(ip), false) => (node_ip(ip)?, true),
                (_, true) => {
                    let servers: Vec<String> = match &lk.rtc.stun_servers {
                        Some(s) if !s.is_empty() => s.clone(),
                        _ => DEFAULT_STUN.iter().map(|s| s.to_string()).collect(),
                    };
                    let ip =
                        external_ip(&servers).context("could not determine the external IP")?;
                    (ip, false)
                }
                (None, false) => (default_route_ip()?, false),
            },
        };
        // Binding the advertised address, not 0.0.0.0, makes replies
        // leave from it. Otherwise a multi-homed host can answer from
        // another interface's address and browsers drop the packets. An
        // address not on this host (behind NAT) can't be bound: use the
        // local one on the default route. Not for a configured address:
        // one missing at boot (a VPN not up yet) would otherwise bind
        // another interface for good, so fail and let the service retry.
        let udp_bind = match env_parse("NATTER_UDP_BIND")? {
            Some(ip) => ip,
            None if UdpSocket::bind((public_ip, 0)).is_ok() => public_ip,
            None if explicit => bail!(
                "{public_ip} is not on this host (yet?); set NATTER_UDP_BIND=0.0.0.0 if it is a NAT address"
            ),
            None => default_route_ip()?,
        };

        // TURN.
        let turn_port = match env_port("NATTER_TURN_PORT")? {
            Some(p) => p,
            None if lk.turn.enabled && lk.turn.udp_port > 0 => Some(lk.turn.udp_port),
            None if lk.turn.enabled => {
                bail!("natter's TURN needs turn.udp_port (it serves UDP and TCP there)")
            }
            None => None,
        };
        let turn_tls = match env("NATTER_TURN_TLS") {
            Some(v) => {
                let (domain, port) = v
                    .rsplit_once(':')
                    .context("NATTER_TURN_TLS is domain:port")?;
                let port = port.parse().context("NATTER_TURN_TLS port")?;
                let cert_key = match (env("NATTER_TURN_CERT"), env("NATTER_TURN_KEY")) {
                    (Some(c), Some(k)) => Some((c.into(), k.into())),
                    (None, None) => None,
                    _ => bail!("NATTER_TURN_CERT and NATTER_TURN_KEY go together"),
                };
                Some(crate::turn::TlsConfig {
                    domain: domain.to_owned(),
                    port,
                    advertised_port: port,
                    cert_key,
                    plain: false,
                })
            }
            // As LiveKit: TLS on `tls_port`, advertised on 443 (a load
            // balancer in front, typically); with `external_tls`, plain
            // TCP there, TLS terminated in front.
            None if lk.turn.enabled && lk.turn.tls_port > 0 => {
                if lk.turn.domain.is_empty() {
                    bail!("turn.domain is required with turn.tls_port");
                }
                let cert_key = if lk.turn.external_tls {
                    None
                } else if lk.turn.cert_file.is_empty() || lk.turn.key_file.is_empty() {
                    bail!("TURN TLS needs turn.cert_file and turn.key_file, or turn.external_tls");
                } else {
                    Some((
                        lk.turn.cert_file.clone().into(),
                        lk.turn.key_file.clone().into(),
                    ))
                };
                Some(crate::turn::TlsConfig {
                    domain: lk.turn.domain.clone(),
                    port: lk.turn.tls_port,
                    advertised_port: 443,
                    cert_key,
                    plain: lk.turn.external_tls,
                })
            }
            None => None,
        };

        // External ICE servers.
        let mut ice_servers = IceServers::default();
        for s in &lk.rtc.turn_servers {
            let (scheme, transport) = match s.protocol.as_str() {
                "tls" => ("turns", "tcp"),
                "udp" => ("turn", "udp"),
                _ => ("turn", "tcp"),
            };
            let url = format!("{scheme}:{}:{}?transport={transport}", s.host, s.port);
            let mut secret = s.secret.trim().to_owned();
            if secret.is_empty() && !s.secret_file.is_empty() {
                secret = std::fs::read_to_string(&s.secret_file)
                    .with_context(|| format!("reading turn secret file {:?}", s.secret_file))?
                    .trim()
                    .to_owned();
                if secret.is_empty() {
                    bail!(
                        "turn server {:?} secret file {:?} is empty",
                        s.host,
                        s.secret_file
                    );
                }
            }
            let auth = if !secret.is_empty() {
                let ttl = match s.ttl {
                    ..=0 => EXTERNAL_TURN_TTL,
                    t => (t as u64).min(TURN_MAX_TTL),
                };
                TurnAuth::Secret { secret, ttl }
            } else if !s.username.is_empty() && !s.credential.is_empty() {
                TurnAuth::Static {
                    username: s.username.clone(),
                    credential: s.credential.clone(),
                }
            } else {
                bail!(
                    "turn server {:?}: no credentials (secret, secret_file, or username and credential)",
                    s.host
                );
            };
            ice_servers.turn.push((url, auth));
        }
        ice_servers.stun = lk.rtc.stun_servers.clone().unwrap_or_default();

        // Rooms.
        let defaults = RoomDefaults::default();
        let rooms = RoomDefaults {
            empty_timeout: lk
                .room
                .empty_timeout
                .filter(|&t| t > 0)
                .unwrap_or(defaults.empty_timeout),
            departure_timeout: lk
                .room
                .departure_timeout
                .filter(|&t| t > 0)
                .unwrap_or(defaults.departure_timeout),
            max_participants: lk.room.max_participants.unwrap_or(0),
            auto_create: lk.room.auto_create.unwrap_or(true),
        };

        // Congestion control: off, or natter's send-side (TWCC) estimate
        // whichever estimator LiveKit was told to use.
        let cc = &lk.rtc.congestion_control;
        let congestion = match env("NATTER_CONGESTION_CONTROL") {
            Some(v) => v
                .parse()
                .map_err(anyhow::Error::msg)
                .context("NATTER_CONGESTION_CONTROL")?,
            None if cc.enabled == Some(false) => livekit::rtc_config::Congestion::Off,
            None => "twcc".parse().map_err(anyhow::Error::msg)?,
        };
        if cc.use_send_side_bwe == Some(false) && lk_mode && cc.enabled != Some(false) {
            notes.push("rtc.congestion_control.use_send_side_bwe: natter estimates send-side (TWCC) regardless; NATTER_CONGESTION_CONTROL=remb for REMB".into());
        }

        // Webhooks, signed with one of the API keys (LiveKit's
        // `webhook.api_key`).
        let list = |k: &str| -> Option<Vec<String>> {
            env(k).map(|v| {
                v.split(',')
                    .map(|s| s.trim().to_owned())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
        };
        let urls = list("NATTER_WEBHOOK_URLS").unwrap_or_else(|| lk.webhook.urls.clone());
        let webhook = if urls.is_empty() {
            None
        } else {
            let key = match env("NATTER_WEBHOOK_API_KEY")
                .or_else(|| Some(lk.webhook.api_key.clone()).filter(|k| !k.is_empty()))
            {
                Some(k) => k,
                None if keys.iter().count() == 1 => keys
                    .iter()
                    .next()
                    .map(|(k, _)| k.to_owned())
                    .unwrap_or_default(),
                None => bail!(
                    "webhooks need an API key (webhook.api_key or NATTER_WEBHOOK_API_KEY): one of the API keys"
                ),
            };
            let Some(secret) = keys.secret(&key) else {
                bail!("webhook API key {key:?} is not one of the API keys");
            };
            Some(livekit::webhook::Config {
                urls,
                api_key: key.clone(),
                api_secret: secret.to_owned(),
                include: list("NATTER_WEBHOOK_INCLUDE")
                    .unwrap_or_else(|| lk.webhook.filter_params.include_events.clone()),
                exclude: list("NATTER_WEBHOOK_EXCLUDE")
                    .unwrap_or_else(|| lk.webhook.filter_params.exclude_events.clone()),
                track_mutes: env("NATTER_WEBHOOK_TRACK_MUTES")
                    .is_some_and(|v| v == "true" || v == "1"),
            })
        };

        // Prometheus.
        let prometheus = lk
            .prometheus
            .port
            .or(lk.prometheus_port)
            .filter(|&p| p > 0)
            .map(|port| Prometheus {
                port,
                auth: match (&lk.prometheus.username, &lk.prometheus.password) {
                    (Some(u), Some(p)) if !u.is_empty() && !p.is_empty() => {
                        Some((u.clone(), p.clone()))
                    }
                    _ => None,
                },
            });

        // Logging: LiveKit's level for natter's own logs.
        let level = lk
            .log_level
            .clone()
            .or(lk.logging.level.clone())
            .or_else(|| dev.then(|| "debug".into()));
        let log_filter = match level.as_deref() {
            Some(l) => {
                let l = match l.to_ascii_lowercase().as_str() {
                    "warning" => "warn".to_owned(),
                    "panic" | "fatal" | "dpanic" => "error".to_owned(),
                    other => other.to_owned(),
                };
                format!("natter_server={l},str0m=warn")
            }
            None => "natter_server=info,str0m=warn".into(),
        };
        // Multi-node: Redis, the node selector, and where other nodes reach
        // this one (NATTER_INTERNAL_URL, else this host on the HTTP port).
        let cluster = if lk.redis.address.is_empty() {
            None
        } else {
            use crate::livekit::cluster;
            let defaults = cluster::SelectorConfig::default();
            let s = &lk.node_selector;
            let kind = s.kind.clone().unwrap_or(defaults.kind);
            if !matches!(
                kind.as_str(),
                "any" | "cpuload" | "sysload" | "regionaware" | "random"
            ) {
                bail!("unsupported node selector: {kind}");
            }
            if kind == "regionaware" && lk.region.as_deref().unwrap_or("").is_empty() {
                bail!("current region cannot be blank");
            }
            let url = match env("NATTER_INTERNAL_URL") {
                Some(u) => u,
                None => {
                    let a = http_addrs[0];
                    let ip = if a.ip().is_unspecified() {
                        default_route_ip()?
                    } else {
                        a.ip()
                    };
                    format!("http://{}", SocketAddr::new(ip, a.port()))
                }
            };
            Some(cluster::Config {
                redis: cluster::RedisConfig {
                    address: lk.redis.address.clone(),
                    username: lk.redis.username.clone(),
                    password: lk.redis.password.clone(),
                    db: lk.redis.db,
                    use_tls: lk.redis.use_tls,
                },
                selector: cluster::SelectorConfig {
                    kind,
                    sort_by: s.sort_by.clone().unwrap_or(defaults.sort_by),
                    algorithm: s.algorithm.clone().unwrap_or(defaults.algorithm),
                    cpu_load_limit: s.cpu_load_limit.unwrap_or(defaults.cpu_load_limit),
                    sysload_limit: s.sysload_limit.unwrap_or(defaults.sysload_limit),
                    regions: s
                        .regions
                        .iter()
                        .map(|r| (r.name.clone(), r.lat, r.lon))
                        .collect(),
                },
                url,
                region: lk.region.clone().unwrap_or_default(),
            })
        };
        if lk.rtc.use_external_ip && lk.rtc.node_ip.is_some() {
            notes.push("rtc.node_ip is ignored with rtc.use_external_ip, as in LiveKit".into());
        }

        let data_dir = PathBuf::from(env("NATTER_DATA_DIR").unwrap_or_else(|| "data".into()));
        Ok(Settings {
            http_addrs,
            udp_port,
            shards,
            tcp_port,
            turn_port,
            turn_tls,
            public_ip,
            udp_bind,
            static_dir: env("NATTER_STATIC_DIR")
                .unwrap_or_else(|| "crates/client/dist".into())
                .into(),
            origin: env_parse("NATTER_ORIGIN")?
                .unwrap_or_else(|| "http://localhost:8080".parse().expect("valid")),
            data_dir: data_dir.clone(),
            egress_dir: env("NATTER_EGRESS_DIR")
                .map_or_else(|| data_dir.join("egress"), PathBuf::from),
            max_peers: env_parse("NATTER_MAX_PEERS")?.unwrap_or(200),
            remote_unmute: env_parse("NATTER_REMOTE_UNMUTE")?
                .unwrap_or(lk.room.enable_remote_unmute.unwrap_or(false)),
            shard_fill: env_parse("NATTER_SHARD_FILL")?.unwrap_or(120),
            pin_engines: env("NATTER_PIN_ENGINES").is_none_or(|v| v != "off"),
            // A build with aws-lc was made to use it.
            dtls_crypto: env("NATTER_DTLS_CRYPTO")
                .unwrap_or_else(|| {
                    if cfg!(feature = "aws-lc") {
                        "aws-lc"
                    } else {
                        "rust"
                    }
                    .into()
                })
                .parse()
                .map_err(anyhow::Error::msg)
                .context("NATTER_DTLS_CRYPTO")?,
            media: livekit::rtc_config::MediaConfig {
                nack: env_parse("NATTER_AUDIO_NACK")?.unwrap_or(true),
                red: env_parse("NATTER_AUDIO_RED")?.unwrap_or(true),
                stats: env_parse("NATTER_STATS")?.unwrap_or(true),
                congestion,
                allow_pause: env_parse("NATTER_ALLOW_PAUSE")?
                    .unwrap_or(cc.allow_pause.unwrap_or(false)),
            },
            fallback: livekit::fallback::Config {
                enabled: lk.rtc.allow_tcp_fallback.unwrap_or(true),
                rtt_threshold: lk
                    .rtc
                    .tcp_fallback_rtt_threshold
                    .filter(|&ms| ms > 0)
                    .map(Duration::from_millis),
                udp_unstable: lk.rtc.allow_udp_unstable_fallback.unwrap_or(false),
            },
            keys,
            region: lk.region.clone().unwrap_or_default(),
            rooms,
            ice_servers,
            prometheus,
            webhook,
            log_filter,
            notes,
            cluster,
        })
    }

    /// LiveKit's `ports` command.
    pub fn ports(&self) -> String {
        let mut tcp = vec![];
        let mut udp = vec![];
        for a in &self.http_addrs {
            tcp.push(format!("{} - HTTP service", a.port()));
        }
        tcp.dedup();
        let last = |base: u16| base + self.shards as u16 - 1;
        if let Some(p) = self.tcp_port {
            tcp.push(format!("{p} - ICE/TCP"));
        }
        udp.push(match self.shards {
            1 => format!("{} - ICE/UDP", self.udp_port),
            _ => format!("{}-{} - ICE/UDP", self.udp_port, last(self.udp_port)),
        });
        if let Some(t) = &self.turn_tls
            && (t.cert_key.is_some() || t.plain)
        {
            tcp.push(format!("{} - TURN/TLS", t.port));
        }
        if let Some(p) = self.turn_port {
            tcp.push(format!("{p} - TURN/TCP"));
            udp.push(format!("{p} - TURN/UDP"));
        }
        if let Some(p) = &self.prometheus {
            tcp.push(format!("{} - Prometheus", p.port));
        }
        format!(
            "TCP Ports\n{}\nUDP Ports\n{}",
            tcp.join("\n"),
            udp.join("\n")
        )
    }
}

/// `node_ip`: an IPv4 and/or IPv6 address, comma separated; IPv4 wins.
fn node_ip(s: &str) -> Result<IpAddr> {
    let ips: Vec<IpAddr> = s
        .split(',')
        .map(|p| p.trim().parse().with_context(|| format!("invalid ip {p}")))
        .collect::<Result<_>>()?;
    ips.iter()
        .find(|ip| ip.is_ipv4())
        .or(ips.first())
        .copied()
        .context("empty rtc.node_ip")
}

/// The local address the OS would use to reach the internet. Connecting a
/// UDP socket only picks a route; nothing is sent.
pub fn default_route_ip() -> Result<IpAddr> {
    let probe = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
    probe.connect((Ipv4Addr::new(192, 0, 2, 1), 9))?;
    Ok(probe.local_addr()?.ip())
}

/// The address the internet sees this host at, from a STUN binding
/// request (LiveKit's `use_external_ip`). The first server to answer wins.
fn external_ip(servers: &[String]) -> Result<IpAddr> {
    use crate::turn::stun;
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
    socket.set_read_timeout(Some(Duration::from_secs(2)))?;
    for server in servers {
        let Some(addr) = server
            .trim_start_matches("stun:")
            .to_socket_addrs()
            .ok()
            .and_then(|mut a| a.find(SocketAddr::is_ipv4))
        else {
            continue;
        };
        let mut txid = [0u8; 12];
        getrandom::fill(&mut txid)?;
        let request =
            stun::Builder::new(stun::method::BINDING, stun::Class::Request, txid).finish(None);
        for _ in 0..3 {
            if socket.send_to(&request, addr).is_err() {
                break;
            }
            let mut buf = [0u8; 1500];
            let Ok((n, from)) = socket.recv_from(&mut buf) else {
                continue;
            };
            if from != addr {
                continue;
            }
            if let Some(m) = stun::Message::parse(&buf[..n])
                && m.txid == txid
                && let Some(mapped) = m.xor_address(stun::attr::XOR_MAPPED_ADDRESS)
            {
                return Ok(mapped.ip());
            }
        }
    }
    bail!("no STUN server answered ({})", servers.join(", "))
}

/// LiveKit's `generate-keys`.
pub fn generate_keys() -> String {
    let key = livekit::ids::new("API");
    use base64::Engine as _;
    let secret =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(crate::util::random_bytes::<32>());
    format!("API Key:  {key}\nAPI Secret:  {secret}")
}

#[cfg(test)]
fn tree_of(yaml: &str) -> Value {
    serde_saphyr::from_str(yaml).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_and_ignored_keys() {
        let tree = tree_of(
            "port: 7880\nrtc:\n  udp_port: 7882\n  use_ice_lite: true\n  bogus: 1\nredis:\n  address: x\nroom:\n  empty_timeout: 10\n  nope: 2\nlogging:\n  level: debug\n  pion_level: error\nkeys:\n  a: b\nwhat: 3\n",
        );
        let (unknown, ignored) = check(&tree);
        assert_eq!(unknown, ["room.nope", "rtc.bogus", "what"]);
        assert_eq!(ignored, ["logging.pion_level", "rtc.use_ice_lite"]);
    }

    #[test]
    fn port_ranges() {
        assert_eq!(
            PortRange::parse("7882").unwrap(),
            PortRange {
                start: 7882,
                end: None
            }
        );
        assert_eq!(
            PortRange::parse("50000 - 50010").unwrap(),
            PortRange {
                start: 50000,
                end: Some(50010)
            }
        );
        assert!(PortRange::parse("5-5").is_err());
        let c: LkConfig = serde_json::from_value(tree_of("rtc:\n  udp_port: 7882\n")).unwrap();
        assert_eq!(
            c.rtc.udp_port,
            Some(PortRange {
                start: 7882,
                end: None
            })
        );
        let c: LkConfig = serde_json::from_value(tree_of("rtc:\n  udp_port: 7882-7890\n")).unwrap();
        assert_eq!(c.rtc.udp_port.unwrap().end, Some(7890));
    }

    #[test]
    fn flags() {
        let args: Vec<String> = [
            "--dev",
            "--bind",
            "0.0.0.0",
            "--rtc.tcp_port=9000",
            "--keys",
            "k: s",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let cli = Cli::parse(&args).unwrap();
        assert!(cli.flag("dev"));
        assert_eq!(cli.all("bind"), ["0.0.0.0"]);
        let tree = livekit_tree(&cli).unwrap().unwrap();
        assert_eq!(tree["rtc"]["tcp_port"], 9000);
        assert_eq!(tree["keys"]["k"], "s");
        assert_eq!(tree["development"], true);
        assert!(Cli::parse(&["--nope".into()]).is_err());
        let cmd = Cli::parse(&["invite".into()]).unwrap();
        assert_eq!(cmd.command, ["invite"]);
    }

    #[test]
    fn node_ips() {
        assert_eq!(
            node_ip("::1, 10.0.0.1").unwrap(),
            "10.0.0.1".parse::<IpAddr>().unwrap()
        );
        assert_eq!(node_ip("::1").unwrap(), "::1".parse::<IpAddr>().unwrap());
        assert!(node_ip("x").is_err());
    }

    #[test]
    fn external_turn_credentials() {
        let ice = IceServers {
            turn: vec![
                (
                    "turn:t:3478?transport=udp".into(),
                    TurnAuth::Secret {
                        secret: "s".into(),
                        ttl: 60,
                    },
                ),
                (
                    "turns:t:443?transport=tcp".into(),
                    TurnAuth::Static {
                        username: "u".into(),
                        credential: "c".into(),
                    },
                ),
            ],
            stun: vec!["stun.example:3478".into()],
        };
        let out = ice.for_participant("PA_x");
        assert_eq!(out.len(), 3);
        assert!(out[0].username.ends_with(":PA_x"));
        assert_eq!(out[0].credential.len(), 28, "base64 of an HMAC-SHA1");
        assert_eq!(
            (out[1].username.as_str(), out[1].credential.as_str()),
            ("u", "c")
        );
        assert_eq!(out[2].urls, ["stun:stun.example:3478"]);
    }
}
