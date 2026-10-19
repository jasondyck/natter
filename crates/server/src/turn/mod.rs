//! Embedded TURN server (RFC 8656), for clients that can reach natter only
//! through a relay: UDP blocked and ICE/TCP not enough (a proxy that allows
//! only TLS, say). As LiveKit's (`docs/livekit/media.md` E.2), it relays
//! only to natter's own media engines, so it is no use as an open relay;
//! UDP and TCP on one port, and TLS either natively (the `tls` feature) or
//! terminated in front of natter and forwarded to the TCP port.
//!
//! Credentials are the TURN REST API's (draft-uberti-behave-turn-rest):
//! the username is `expiry:id` and the password an HMAC of it under a
//! secret only this process knows, so nothing is stored per client.
//!
//! Each allocation relays through a UDP socket of its own beside the
//! engines, so to an engine a relayed client is just another address.
//! One thread runs it all: relaying is for the few clients that need it.

pub(crate) mod stun;
#[cfg(feature = "tls")]
mod tls;

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use hmac::{KeyInit, Mac};
use livekit_protocol as lk;
use mio::net::{TcpListener, TcpStream, UdpSocket};
use mio::{Events, Interest, Poll, Token};
use tracing::{debug, info, warn};

use crate::netio;
use stun::{Builder, Class, Message, attr, method};

const REALM: &str = "natter";
/// How long credentials handed out at join can start allocations (chosen:
/// a resume hands out new ones, and they reach only natter's own ports).
const CREDENTIAL_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// A nonce is good this long, then the client is asked to take a new one
/// (438 Stale Nonce).
const NONCE_TTL: Duration = Duration::from_secs(60 * 60);
/// Allocation lifetimes (RFC 8656 §3.2: 10 minutes by default, at most an
/// hour), permissions (§9: 5 minutes) and channel bindings (§12: 10).
const DEFAULT_LIFETIME: u32 = 600;
const MAX_LIFETIME: u32 = 3600;
const PERMISSION: Duration = Duration::from_secs(300);
const CHANNEL: Duration = Duration::from_secs(600);
/// Allocations per username (LiveKit `per_user_relay_allocation_limit`)
/// and in all (chosen).
const PER_USER: usize = 12;
const MAX_ALLOCATIONS: usize = 4096;
/// TCP and TLS connections (chosen), how long one may sit without an
/// allocation or traffic, and how far behind it may fall before it is
/// dropped (as `livekit::tcp`).
const MAX_CONNECTIONS: usize = 4096;
const IDLE: Duration = Duration::from_secs(30);
const MAX_QUEUED: usize = 4 << 20;

const UDP: Token = Token(0);
const TCP: Token = Token(1);
const TLS: Token = Token(2);

/// TURN over TLS, as advertised: `turns:domain:port`.
#[derive(Clone, Debug)]
pub struct TlsConfig {
    pub domain: String,
    /// Where TLS is served, if natter serves it.
    pub port: u16,
    /// The port in the `turns:` URL clients get (LiveKit always says 443,
    /// for a load balancer in front).
    pub advertised_port: u16,
    /// PEM certificate chain and key, for natter to terminate TLS itself.
    /// Without them TLS is terminated in front of natter and forwarded to
    /// the TURN TCP port, or to `port` if `plain`.
    pub cert_key: Option<(std::path::PathBuf, std::path::PathBuf)>,
    /// Plain TURN over TCP on `port`, TLS terminated in front (LiveKit's
    /// `external_tls`).
    pub plain: bool,
}

pub struct Config {
    /// UDP and TCP.
    pub port: u16,
    pub tls: Option<TlsConfig>,
    /// Where to listen (the engines' bind address).
    pub bind: IpAddr,
    pub public_ip: IpAddr,
    /// The engines' advertised UDP addresses (what clients name as peers),
    /// each with the address it is reached at from this host.
    pub peers: HashMap<SocketAddr, SocketAddr>,
}

/// What signalling hands out: the servers and credentials for a client.
#[derive(Clone)]
pub struct Turn {
    secret: Arc<[u8; 32]>,
    urls: Arc<Vec<String>>,
}

impl Turn {
    /// TURN servers and credentials for a client known by `id`.
    pub fn ice_server(&self, id: &str) -> lk::IceServer {
        let (username, credential) = rest_credential(&*self.secret, id, CREDENTIAL_TTL.as_secs());
        lk::IceServer {
            urls: self.urls.to_vec(),
            username,
            credential,
        }
    }
}

/// A TURN REST credential (coturn's `static-auth-secret`) valid for `ttl`
/// seconds: username `expiry:id`, password its HMAC.
pub fn rest_credential(secret: &[u8], id: &str, ttl: u64) -> (String, String) {
    let username = format!("{}:{id}", unix_now() + ttl);
    let credential = password(secret, &username);
    (username, credential)
}

use crate::util::unix_secs as unix_now;

fn hmac_sha1(secret: &[u8], data: &[u8]) -> [u8; 20] {
    let mut mac = hmac::Hmac::<sha1::Sha1>::new_from_slice(secret).expect("any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn password(secret: &[u8], username: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(hmac_sha1(secret, username.as_bytes()))
}

/// Starts the TURN thread. Returns what signalling advertises.
pub fn start(config: Config) -> anyhow::Result<Turn> {
    let mut secret = [0u8; 32];
    getrandom::fill(&mut secret)?;
    let secret = Arc::new(secret);

    let poll = Poll::new()?;
    let mut udp = UdpSocket::bind(SocketAddr::new(config.bind, config.port))?;
    {
        let s = socket2::SockRef::from(&udp);
        let _ = s.set_recv_buffer_size(4 << 20);
        let _ = s.set_send_buffer_size(4 << 20);
    }
    poll.registry()
        .register(&mut udp, UDP, Interest::READABLE)?;
    let mut tcp = TcpListener::bind(SocketAddr::new(config.bind, config.port))?;
    poll.registry()
        .register(&mut tcp, TCP, Interest::READABLE)?;

    let host = config.public_ip;
    let mut urls = vec![
        format!("turn:{}?transport=udp", SocketAddr::new(host, config.port)),
        format!("turn:{}?transport=tcp", SocketAddr::new(host, config.port)),
    ];
    let mut tls_listener = None;
    let mut tls_config = None;
    if let Some(tls) = &config.tls {
        urls.push(format!(
            "turns:{}:{}?transport=tcp",
            tls.domain, tls.advertised_port
        ));
        if let Some((cert, key)) = &tls.cert_key {
            tls_config = Some(load_tls(cert, key)?);
        }
        if tls.cert_key.is_some() || tls.plain {
            let mut l = TcpListener::bind(SocketAddr::new(config.bind, tls.port))?;
            poll.registry().register(&mut l, TLS, Interest::READABLE)?;
            tls_listener = Some(l);
        }
    }
    let _ = &tls_config;

    // Relays send from the engines' address, or loopback when they listen
    // on every address.
    let relay_ip = match config.bind {
        ip if ip.is_unspecified() => match ip {
            IpAddr::V4(_) => IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            IpAddr::V6(_) => IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
        },
        ip => ip,
    };
    let local_peers = config.peers.iter().map(|(a, l)| (*l, *a)).collect();
    let server = Server {
        poll,
        udp,
        tcp,
        tls: tls_listener,
        tls_plain: config.tls.as_ref().is_some_and(|t| t.plain),
        #[cfg(feature = "tls")]
        tls_config,
        secret: Arc::clone(&secret),
        peer_ips: config.peers.keys().map(SocketAddr::ip).collect(),
        peers: config.peers,
        local_peers,
        relay_ip,
        next: 16,
        conns: HashMap::new(),
        allocations: HashMap::new(),
        by_client: HashMap::new(),
        per_user: HashMap::new(),
    };
    std::thread::Builder::new()
        .name("turn".into())
        .spawn(move || server.run())?;
    info!(urls = ?urls, "TURN listening");
    Ok(Turn {
        secret,
        urls: Arc::new(urls),
    })
}

#[cfg(feature = "tls")]
type TlsConfigLoaded = Arc<rustls::ServerConfig>;
#[cfg(not(feature = "tls"))]
type TlsConfigLoaded = ();

#[cfg(feature = "tls")]
fn load_tls(cert: &std::path::Path, key: &std::path::Path) -> anyhow::Result<TlsConfigLoaded> {
    tls::config(cert, key)
}

#[cfg(not(feature = "tls"))]
fn load_tls(_: &std::path::Path, _: &std::path::Path) -> anyhow::Result<TlsConfigLoaded> {
    anyhow::bail!(
        "natter was built without TLS: build with `--features tls`, or terminate TLS in front of it and forward to the TURN TCP port"
    )
}

#[cfg(feature = "tls")]
type TlsSession = rustls::ServerConnection;
#[cfg(not(feature = "tls"))]
type TlsSession = std::convert::Infallible;

/// Where a client's messages come from: a UDP address or a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Client {
    Udp(SocketAddr),
    Stream(Token),
}

struct Conn {
    stream: TcpStream,
    peer: SocketAddr,
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    tls: Option<TlsSession>,
    /// Plaintext received, not yet framed.
    read: Vec<u8>,
    /// Bytes for the socket (TLS records, when TLS).
    write: VecDeque<u8>,
    last_active: Instant,
}

struct Allocation {
    client: Client,
    client_addr: SocketAddr,
    relay: UdpSocket,
    relayed: SocketAddr,
    username: String,
    expires: Instant,
    permissions: HashMap<IpAddr, Instant>,
    /// Channel → peer (as the client names it), and back.
    channels: HashMap<u16, (SocketAddr, Instant)>,
    peer_channels: HashMap<SocketAddr, u16>,
}

struct Server {
    poll: Poll,
    udp: UdpSocket,
    tcp: TcpListener,
    tls: Option<TcpListener>,
    /// The TLS listener takes plain TURN (TLS terminated in front).
    tls_plain: bool,
    #[cfg(feature = "tls")]
    tls_config: Option<TlsConfigLoaded>,
    secret: Arc<[u8; 32]>,
    /// Advertised engine address → where to send to it, and back.
    peers: HashMap<SocketAddr, SocketAddr>,
    local_peers: HashMap<SocketAddr, SocketAddr>,
    peer_ips: HashSet<IpAddr>,
    relay_ip: IpAddr,
    next: usize,
    conns: HashMap<Token, Conn>,
    /// By relay socket token.
    allocations: HashMap<Token, Allocation>,
    by_client: HashMap<Client, Token>,
    per_user: HashMap<String, usize>,
}

/// What came of a request: a response for the client.
type Reply = Vec<u8>;

impl Server {
    fn run(mut self) {
        let mut events = Events::with_capacity(256);
        let mut buf = vec![0u8; 64 * 1024];
        let mut last_sweep = Instant::now();
        loop {
            if let Err(e) = self.poll.poll(&mut events, Some(Duration::from_secs(1))) {
                if e.kind() != ErrorKind::Interrupted {
                    warn!("TURN poll failed: {e}");
                }
                continue;
            }
            let mut closed = vec![];
            for event in &events {
                match event.token() {
                    UDP => self.read_udp(&mut buf),
                    TCP => self.accept(false),
                    TLS => self.accept(true),
                    token if self.allocations.contains_key(&token) => {
                        self.read_relay(token, &mut buf);
                    }
                    token => {
                        if event.is_readable() && self.read_conn(token, &mut buf).is_err() {
                            closed.push(token);
                        }
                        if event.is_writable()
                            && let Some(c) = self.conns.get_mut(&token)
                            && netio::flush(&mut c.stream, &mut c.write).is_err()
                        {
                            closed.push(token);
                        }
                    }
                }
            }
            closed.extend(self.flush_all());
            if last_sweep.elapsed() >= Duration::from_secs(1) {
                last_sweep = Instant::now();
                closed.extend(self.sweep());
            }
            for token in closed {
                self.close(token);
            }
        }
    }

    fn read_udp(&mut self, buf: &mut [u8]) {
        loop {
            match self.udp.recv_from(buf) {
                Ok((n, from)) => {
                    let data = buf[..n].to_vec();
                    self.on_message(Client::Udp(from), from, &data);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => {
                    debug!("TURN UDP receive failed: {e}");
                    break;
                }
            }
        }
    }

    fn accept(&mut self, tls: bool) {
        let listener = if tls {
            self.tls.as_ref()
        } else {
            Some(&self.tcp)
        };
        let Some(listener) = listener else { return };
        loop {
            match listener.accept() {
                Ok((mut stream, peer)) => {
                    if self.conns.len() >= MAX_CONNECTIONS {
                        continue;
                    }
                    let tls = tls && !self.tls_plain;
                    let session = if tls { self.tls_session() } else { None };
                    if tls && session.is_none() {
                        continue;
                    }
                    let _ = stream.set_nodelay(true);
                    let token = Token(self.next);
                    self.next += 1;
                    if self
                        .poll
                        .registry()
                        .register(&mut stream, token, Interest::READABLE)
                        .is_ok()
                    {
                        debug!(%peer, tls, "TURN connection");
                        self.conns.insert(
                            token,
                            Conn {
                                stream,
                                peer,
                                tls: session,
                                read: vec![],
                                write: VecDeque::new(),
                                last_active: Instant::now(),
                            },
                        );
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => {
                    debug!("TURN accept failed: {e}");
                    break;
                }
            }
        }
    }

    #[cfg(feature = "tls")]
    fn tls_session(&self) -> Option<TlsSession> {
        rustls::ServerConnection::new(Arc::clone(self.tls_config.as_ref()?)).ok()
    }

    #[cfg(not(feature = "tls"))]
    fn tls_session(&self) -> Option<TlsSession> {
        None
    }

    /// Reads a connection and handles each whole message. Err: closed.
    fn read_conn(&mut self, token: Token, buf: &mut [u8]) -> Result<(), ()> {
        let Some(c) = self.conns.get_mut(&token) else {
            return Ok(());
        };
        read_stream(c, buf)?;
        let peer = c.peer;
        loop {
            let Some(c) = self.conns.get_mut(&token) else {
                return Ok(());
            };
            let Some(len) = stun::frame_len(&c.read)? else {
                return Ok(());
            };
            if c.read.len() < len {
                return Ok(());
            }
            let frame: Vec<u8> = c.read.drain(..len).collect();
            self.on_message(Client::Stream(token), peer, &frame);
        }
    }

    /// A STUN message or ChannelData from a client.
    fn on_message(&mut self, client: Client, from: SocketAddr, data: &[u8]) {
        match stun::kind(data) {
            stun::Kind::Channel => {
                if let Some((channel, payload)) = stun::parse_channel_data(data) {
                    self.relay_channel(client, channel, payload);
                }
            }
            stun::Kind::Stun => {
                let Some(m) = Message::parse(data) else {
                    return;
                };
                if let Some(reply) = self.on_stun(client, from, &m) {
                    self.send_client(client, &reply);
                }
            }
            stun::Kind::Other => {}
        }
    }

    fn on_stun(&mut self, client: Client, from: SocketAddr, m: &Message) -> Option<Reply> {
        match (m.method, m.class) {
            (method::BINDING, Class::Request) => Some(
                Builder::new(method::BINDING, Class::Success, m.txid)
                    .xor_address(attr::XOR_MAPPED_ADDRESS, from)
                    .finish(None),
            ),
            (method::SEND, Class::Indication) => {
                self.relay_send(client, m);
                None
            }
            (
                method::ALLOCATE
                | method::REFRESH
                | method::CREATE_PERMISSION
                | method::CHANNEL_BIND,
                Class::Request,
            ) => Some(match self.authenticate(client, m) {
                Err(reply) => reply,
                Ok((username, key)) => match m.method {
                    method::ALLOCATE => self.allocate(client, from, m, username, key),
                    method::REFRESH => self.refresh(client, m, &key),
                    method::CREATE_PERMISSION => self.create_permission(client, m, &key),
                    _ => self.channel_bind(client, m, &key),
                },
            }),
            _ => None,
        }
    }

    fn nonce(&self) -> String {
        let now = unix_now();
        let mac = hmac_sha1(&*self.secret, &now.to_be_bytes());
        format!("{now:016x}{}", hex::encode(&mac[..8]))
    }

    fn nonce_valid(&self, nonce: &str) -> bool {
        let (Some(at), Some(mac)) = (nonce.get(..16), nonce.get(16..)) else {
            return false;
        };
        let Ok(at) = u64::from_str_radix(at, 16) else {
            return false;
        };
        let expected = hmac_sha1(&*self.secret, &at.to_be_bytes());
        hex::encode(&expected[..8]) == mac && unix_now().saturating_sub(at) < NONCE_TTL.as_secs()
    }

    fn challenge(&self, m: &Message, code: u16, reason: &str) -> Reply {
        Builder::new(m.method, Class::Error, m.txid)
            .error(code, reason)
            .attr(attr::REALM, REALM.as_bytes())
            .attr(attr::NONCE, self.nonce().as_bytes())
            .finish(None)
    }

    /// The username and key of a request with valid long-term credentials,
    /// or the error to answer with.
    fn authenticate(&self, client: Client, m: &Message) -> Result<(String, [u8; 16]), Reply> {
        let (Some(username), Some(nonce), Some(_)) = (
            m.str(attr::USERNAME),
            m.str(attr::NONCE),
            m.get(attr::MESSAGE_INTEGRITY),
        ) else {
            return Err(self.challenge(m, 401, "Unauthorized"));
        };
        if !self.nonce_valid(nonce) {
            return Err(self.challenge(m, 438, "Stale Nonce"));
        }
        let key = stun::long_term_key(username, REALM, &password(&*self.secret, username));
        if !m.verify(&key) {
            return Err(self.challenge(m, 401, "Unauthorized"));
        }
        // An existing allocation is only for the username that made it.
        if let Some(a) = self
            .by_client
            .get(&client)
            .and_then(|t| self.allocations.get(t))
            && a.username != username
        {
            return Err(error(m, 441, "Wrong Credentials", None));
        }
        Ok((username.to_owned(), key))
    }

    fn allocate(
        &mut self,
        client: Client,
        from: SocketAddr,
        m: &Message,
        username: String,
        key: [u8; 16],
    ) -> Reply {
        if let Some(a) = self
            .by_client
            .get(&client)
            .and_then(|t| self.allocations.get(t))
        {
            // A retransmit: the same answer.
            let lifetime = a
                .expires
                .saturating_duration_since(Instant::now())
                .as_secs() as u32;
            return Builder::new(method::ALLOCATE, Class::Success, m.txid)
                .xor_address(attr::XOR_RELAYED_ADDRESS, a.relayed)
                .u32(attr::LIFETIME, lifetime)
                .xor_address(attr::XOR_MAPPED_ADDRESS, from)
                .finish(Some(&key));
        }
        let expiry = username
            .split_once(':')
            .and_then(|(e, _)| e.parse::<u64>().ok());
        if expiry.is_none_or(|e| e < unix_now()) {
            return self.challenge(m, 401, "Unauthorized");
        }
        // UDP relaying only (17), as LiveKit.
        if m.get(attr::REQUESTED_TRANSPORT).and_then(|v| v.first()) != Some(&17) {
            return error(m, 442, "Unsupported Transport Protocol", Some(&key));
        }
        let family = match self.relay_ip {
            IpAddr::V4(_) => 1,
            IpAddr::V6(_) => 2,
        };
        if m.get(attr::REQUESTED_ADDRESS_FAMILY)
            .and_then(|v| v.first())
            .is_some_and(|f| *f != family)
        {
            return error(m, 440, "Address Family not Supported", Some(&key));
        }
        if self.allocations.len() >= MAX_ALLOCATIONS
            || self.per_user.get(&username).copied().unwrap_or(0) >= PER_USER
        {
            return error(m, 486, "Allocation Quota Reached", Some(&key));
        }
        let Ok(mut relay) = UdpSocket::bind(SocketAddr::new(self.relay_ip, 0)) else {
            return error(m, 508, "Insufficient Capacity", Some(&key));
        };
        let token = Token(self.next);
        self.next += 1;
        let (Ok(relayed), Ok(())) = (
            relay.local_addr(),
            self.poll
                .registry()
                .register(&mut relay, token, Interest::READABLE),
        ) else {
            return error(m, 508, "Insufficient Capacity", Some(&key));
        };
        let lifetime = lifetime(m);
        debug!(%from, %relayed, %username, "TURN allocation");
        *self.per_user.entry(username.clone()).or_default() += 1;
        self.allocations.insert(
            token,
            Allocation {
                client,
                client_addr: from,
                relay,
                relayed,
                username,
                expires: Instant::now() + Duration::from_secs(lifetime.into()),
                permissions: HashMap::new(),
                channels: HashMap::new(),
                peer_channels: HashMap::new(),
            },
        );
        self.by_client.insert(client, token);
        Builder::new(method::ALLOCATE, Class::Success, m.txid)
            .xor_address(attr::XOR_RELAYED_ADDRESS, relayed)
            .u32(attr::LIFETIME, lifetime)
            .xor_address(attr::XOR_MAPPED_ADDRESS, from)
            .finish(Some(&key))
    }

    fn refresh(&mut self, client: Client, m: &Message, key: &[u8]) -> Reply {
        let Some(&token) = self.by_client.get(&client) else {
            return error(m, 437, "Allocation Mismatch", Some(key));
        };
        let requested = m.u32(attr::LIFETIME);
        let lifetime = if requested == Some(0) { 0 } else { lifetime(m) };
        if lifetime == 0 {
            self.remove_allocation(token);
        } else if let Some(a) = self.allocations.get_mut(&token) {
            a.expires = Instant::now() + Duration::from_secs(lifetime.into());
        }
        Builder::new(method::REFRESH, Class::Success, m.txid)
            .u32(attr::LIFETIME, lifetime)
            .finish(Some(key))
    }

    fn create_permission(&mut self, client: Client, m: &Message, key: &[u8]) -> Reply {
        let Some(a) = self
            .by_client
            .get(&client)
            .and_then(|t| self.allocations.get_mut(t))
        else {
            return error(m, 437, "Allocation Mismatch", Some(key));
        };
        let peers: Option<Vec<SocketAddr>> = m.xor_addresses(attr::XOR_PEER_ADDRESS).collect();
        let Some(peers) = peers.filter(|p| !p.is_empty()) else {
            return error(m, 400, "Bad Request", Some(key));
        };
        // Only natter's own engines.
        if peers.iter().any(|p| !self.peer_ips.contains(&p.ip())) {
            return error(m, 403, "Forbidden", Some(key));
        }
        let until = Instant::now() + PERMISSION;
        for p in peers {
            a.permissions.insert(p.ip(), until);
        }
        Builder::new(method::CREATE_PERMISSION, Class::Success, m.txid).finish(Some(key))
    }

    fn channel_bind(&mut self, client: Client, m: &Message, key: &[u8]) -> Reply {
        let Some(a) = self
            .by_client
            .get(&client)
            .and_then(|t| self.allocations.get_mut(t))
        else {
            return error(m, 437, "Allocation Mismatch", Some(key));
        };
        let channel = m
            .get(attr::CHANNEL_NUMBER)
            .and_then(|v| Some(u16::from_be_bytes(v.get(..2)?.try_into().ok()?)));
        let (Some(channel), Some(peer)) = (channel, m.xor_address(attr::XOR_PEER_ADDRESS)) else {
            return error(m, 400, "Bad Request", Some(key));
        };
        if !(0x4000..=0x7FFF).contains(&channel)
            || a.channels.get(&channel).is_some_and(|(p, _)| *p != peer)
            || a.peer_channels.get(&peer).is_some_and(|c| *c != channel)
        {
            return error(m, 400, "Bad Request", Some(key));
        }
        if !self.peer_ips.contains(&peer.ip()) {
            return error(m, 403, "Forbidden", Some(key));
        }
        let now = Instant::now();
        a.channels.insert(channel, (peer, now + CHANNEL));
        a.peer_channels.insert(peer, channel);
        a.permissions.insert(peer.ip(), now + PERMISSION);
        Builder::new(method::CHANNEL_BIND, Class::Success, m.txid).finish(Some(key))
    }

    /// Sends a client's data to a peer it has permission for.
    fn relay_to_peer(&self, token: Token, peer: SocketAddr, data: &[u8]) {
        let Some(a) = self.allocations.get(&token) else {
            return;
        };
        let Some(local) = self.peers.get(&peer) else {
            return;
        };
        if a.permissions.contains_key(&peer.ip()) {
            let _ = a.relay.send_to(data, *local);
        }
    }

    fn relay_send(&mut self, client: Client, m: &Message) {
        let Some(&token) = self.by_client.get(&client) else {
            return;
        };
        if let (Some(peer), Some(data)) = (m.xor_address(attr::XOR_PEER_ADDRESS), m.get(attr::DATA))
        {
            self.relay_to_peer(token, peer, data);
        }
    }

    fn relay_channel(&mut self, client: Client, channel: u16, data: &[u8]) {
        let Some(&token) = self.by_client.get(&client) else {
            return;
        };
        let peer = self
            .allocations
            .get(&token)
            .and_then(|a| a.channels.get(&channel))
            .map(|(p, _)| *p);
        if let Some(peer) = peer {
            self.relay_to_peer(token, peer, data);
        }
    }

    /// What an engine sent to a relayed client.
    fn read_relay(&mut self, token: Token, buf: &mut [u8]) {
        loop {
            let Some(a) = self.allocations.get(&token) else {
                return;
            };
            let (n, from) = match a.relay.recv_from(buf) {
                Ok(r) => r,
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(_) => return,
            };
            // As the client knows the engine: its advertised address.
            let Some(&peer) = self.local_peers.get(&from) else {
                continue;
            };
            if !a.permissions.contains_key(&peer.ip()) {
                continue;
            }
            let client = a.client;
            let out = match a.peer_channels.get(&peer) {
                Some(&channel) => {
                    stun::channel_data(channel, &buf[..n], matches!(client, Client::Stream(_)))
                }
                None => {
                    let mut txid = [0u8; 12];
                    let _ = getrandom::fill(&mut txid);
                    Builder::new(method::DATA, Class::Indication, txid)
                        .xor_address(attr::XOR_PEER_ADDRESS, peer)
                        .attr(attr::DATA, &buf[..n])
                        .finish(None)
                }
            };
            self.send_client(client, &out);
        }
    }

    fn send_client(&mut self, client: Client, data: &[u8]) {
        match client {
            Client::Udp(addr) => {
                let _ = self.udp.send_to(data, addr);
            }
            Client::Stream(token) => {
                if let Some(c) = self.conns.get_mut(&token) {
                    write_stream(c, data);
                }
            }
        }
    }

    /// Writes what each connection has queued. Returns those to close.
    fn flush_all(&mut self) -> Vec<Token> {
        let mut closed = vec![];
        for (token, c) in self.conns.iter_mut() {
            if c.write.is_empty() {
                continue;
            }
            if c.write.len() > MAX_QUEUED {
                closed.push(*token);
                continue;
            }
            let interest = match netio::flush(&mut c.stream, &mut c.write) {
                Ok(true) => Interest::READABLE,
                Ok(false) => Interest::READABLE | Interest::WRITABLE,
                Err(()) => {
                    closed.push(*token);
                    continue;
                }
            };
            let _ = self
                .poll
                .registry()
                .reregister(&mut c.stream, *token, interest);
        }
        closed
    }

    /// Expires allocations, permissions, channels and idle connections.
    fn sweep(&mut self) -> Vec<Token> {
        let now = Instant::now();
        let expired: Vec<Token> = self
            .allocations
            .iter()
            .filter(|(_, a)| a.expires <= now)
            .map(|(t, _)| *t)
            .collect();
        for t in expired {
            self.remove_allocation(t);
        }
        for a in self.allocations.values_mut() {
            a.permissions.retain(|_, until| *until > now);
            a.channels.retain(|_, (_, until)| *until > now);
            let channels = &a.channels;
            a.peer_channels.retain(|_, c| channels.contains_key(c));
        }
        self.conns
            .iter()
            .filter(|(t, c)| {
                !self.by_client.contains_key(&Client::Stream(**t))
                    && now.duration_since(c.last_active) > IDLE
            })
            .map(|(t, _)| *t)
            .collect()
    }

    fn remove_allocation(&mut self, token: Token) {
        if let Some(mut a) = self.allocations.remove(&token) {
            let _ = self.poll.registry().deregister(&mut a.relay);
            self.by_client.remove(&a.client);
            if let Some(n) = self.per_user.get_mut(&a.username) {
                *n -= 1;
                if *n == 0 {
                    self.per_user.remove(&a.username);
                }
            }
            debug!(client = %a.client_addr, relayed = %a.relayed, "TURN allocation closed");
        }
    }

    /// A connection closed: its allocation goes with it (RFC 6062 §5.2).
    fn close(&mut self, token: Token) {
        if let Some(mut c) = self.conns.remove(&token) {
            let _ = self.poll.registry().deregister(&mut c.stream);
            debug!(peer = %c.peer, "TURN connection closed");
        }
        if let Some(t) = self.by_client.get(&Client::Stream(token)).copied() {
            self.remove_allocation(t);
        }
    }
}

fn lifetime(m: &Message) -> u32 {
    m.u32(attr::LIFETIME)
        .unwrap_or(DEFAULT_LIFETIME)
        .clamp(DEFAULT_LIFETIME, MAX_LIFETIME)
}

fn error(m: &Message, code: u16, reason: &str, key: Option<&[u8]>) -> Reply {
    Builder::new(m.method, Class::Error, m.txid)
        .error(code, reason)
        .finish(key)
}

/// Reads what is available into `c.read`, decrypted if TLS. Err: closed.
fn read_stream(c: &mut Conn, buf: &mut [u8]) -> Result<(), ()> {
    c.last_active = Instant::now();
    #[cfg(feature = "tls")]
    if let Some(tls) = &mut c.tls {
        return tls::read(tls, &mut c.stream, &mut c.read, &mut c.write, buf);
    }
    netio::read_into(&mut c.stream, buf, &mut c.read).map(drop)
}

/// Queues `data` for the socket, encrypted if TLS.
fn write_stream(c: &mut Conn, data: &[u8]) {
    #[cfg(feature = "tls")]
    if let Some(tls) = &mut c.tls {
        tls::write(tls, &mut c.write, data);
        return;
    }
    c.write.extend(data);
}

#[cfg(test)]
mod tests;
