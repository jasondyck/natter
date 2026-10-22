//! Media workers. Each thread owns a set of client peer connections and
//! sends and receives their media; the signalling tasks hand connections
//! over once negotiated and reach them later through [`Worker`] commands.
//!
//! Every connection has its own UDP socket, as a real client does: SFUs
//! serve all clients from one port, so packets can only be told apart by
//! the local port they arrive on.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use mio::net::UdpSocket;
use mio::{Events, Interest, Poll, Token, Waker};
use str0m::change::{SdpAnswer, SdpOffer};
use str0m::channel::ChannelId;
use str0m::format::{Codec, CodecConfig};
use str0m::media::{Frequency, KeyframeRequestKind, MediaData, MediaKind, MediaTime, Mid, Pt, Rid};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, Input, Output, Rtc};
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use tracing::{debug, warn};

use crate::stamp::{self, Stamp};
use crate::stats::{Receipts, Streams};

/// One Opus frame per packet.
pub const FRAME: Duration = Duration::from_millis(20);
const SAMPLES_PER_FRAME: u64 = 960;
/// Longest the loop sleeps without anything due.
const MAX_WAIT: Duration = Duration::from_millis(50);
/// Resend buffer, as a browser's: enough to answer NACKs for 4 seconds.
const AUDIO_PACKET_BUFFER: usize = 200;
/// Video's, as natter's server: a second or more at a few Mbit/s.
const VIDEO_PACKET_BUFFER: usize = 500;
/// Fewest between two keyframe requests for one stream, as libwebrtc's
/// receiver.
const PLI_INTERVAL: Duration = Duration::from_millis(500);
/// NACKs per missing packet, as libwebrtc (str0m's default is 5). An SFU
/// that resends a packet at most once per round trip (LiveKit: 100 ms when
/// it measures none) only answers a NACK sent that much after the first;
/// str0m's five, ~33 ms apart, miss that.
const NACK_RETRIES: u8 = 10;
/// Longest an incomplete video frame waits for its missing packets: the
/// NACK retries above, plus a margin. str0m's default, 2 s, would hold every
/// later frame that long; a browser asks for a keyframe sooner.
const VIDEO_HOLD: Duration = Duration::from_millis(500);

pub type ConnId = u64;

/// State all workers read: the clock stamps are relative to, and the phase.
pub struct Shared {
    pub epoch: Instant,
    /// Publishers mark packets as measured, and receivers count them.
    pub measuring: AtomicBool,
    /// Loss is injected only once everyone is connected.
    pub lossy: AtomicBool,
    /// Shaped downlinks' capacity now, bits/s (`--down-step` changes it).
    pub link_bps: AtomicU64,
}

impl Shared {
    pub fn micros(&self, at: Instant) -> u64 {
        at.saturating_duration_since(self.epoch).as_micros() as u64
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub payload: usize,
    /// Share of RTP/RTCP packets dropped on arrival (server to client).
    pub loss_down: f64,
    /// Share dropped on sending (client to server).
    pub loss_up: f64,
    pub video: VideoOptions,
    pub downlink: Option<LinkOptions>,
}

/// What video publishers send: VP8 frames of a fixed size at a fixed rate,
/// a bigger keyframe first and whenever one is asked for. With simulcast,
/// the same frames on three layers, `q`, `h` and `f`, at a tenth, a third
/// and all of the size.
#[derive(Debug, Clone, Copy)]
pub struct VideoOptions {
    pub fps: u32,
    pub frame_bytes: usize,
    pub keyframe_bytes: usize,
}

/// Simulcast layers lowest first, as LiveKit's clients name them, and each
/// one's share of the top layer's bitrate.
pub const LAYERS: [(&str, f64); 3] = [("q", 0.1), ("h", 1.0 / 3.0), ("f", 1.0)];

/// A bottleneck in front of a client: packets queue at the capacity in
/// `Shared::link_bps` and are dropped when the queue would hold more than
/// `max_delay` (a router's tail drop). Past the bottleneck each packet takes
/// `delay` plus up to `jitter` more, in order, as on a real path; `delay`
/// stands for the whole round trip, since nothing delays the other way.
#[derive(Debug, Clone, Copy)]
pub struct LinkOptions {
    pub max_delay: Duration,
    pub delay: Duration,
    pub jitter: Duration,
}

/// A client's downlink: when it is next free, what is in flight, and when
/// the last packet in flight arrives (arrivals keep their order).
struct Link {
    options: LinkOptions,
    busy_until: Instant,
    last_arrival: Instant,
    queue: VecDeque<(Instant, SocketAddr, Vec<u8>)>,
}

/// Hands a datagram from the server to a connection's `Rtc`.
fn feed(
    rtc: &mut Rtc,
    id: ConnId,
    now: Instant,
    source: SocketAddr,
    destination: SocketAddr,
    data: &[u8],
) {
    let Ok(contents) = data.try_into() else {
        return;
    };
    let receive = Receive {
        proto: Protocol::Udp,
        source,
        destination,
        contents,
    };
    if let Err(e) = rtc.handle_input(Input::Receive(now, receive)) {
        debug!("conn {id}: bad input: {e:?}");
    }
}

/// Which client a connection belongs to, for the readiness check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientKey {
    pub room: u32,
    pub index: u32,
}

/// What a connection publishes: one audio or video track.
#[derive(Debug)]
pub struct Publish {
    pub mid: Mid,
    pub publisher: u32,
    pub kind: MediaKind,
    pt: Option<Pt>,
    seq: u32,
    /// Video, per layer (one without simulcast): the next frame is a
    /// keyframe.
    keyframe: [bool; 3],
    simulcast: bool,
}

impl Publish {
    pub fn new(mid: Mid, publisher: u32, kind: MediaKind, simulcast: bool) -> Self {
        Publish {
            mid,
            publisher,
            kind,
            pt: None,
            seq: 0,
            keyframe: [true; 3],
            simulcast,
        }
    }

    fn interval(&self, options: &Options) -> Duration {
        match self.kind {
            MediaKind::Audio => FRAME,
            MediaKind::Video => Duration::from_secs(1) / options.video.fps.max(1),
        }
    }
}

/// A received video stream's decoder state, as a receiver would track it.
#[derive(Debug, Default)]
struct VideoRx {
    /// A frame went missing: nothing decodes until the next keyframe.
    broken: bool,
    last_pli: Option<Instant>,
}

/// Things a connection tells its signalling task.
#[derive(Debug)]
pub enum ConnEvent {
    Connected,
    ChannelOpen,
    ChannelData(Vec<u8>),
    Closed,
}

pub struct Conn {
    pub id: ConnId,
    pub client: ClientKey,
    pub rtc: Rtc,
    pub socket: UdpSocket,
    pub publish: Option<Publish>,
    pub events: UnboundedSender<ConnEvent>,
    created: Instant,
    connected_after: Option<Duration>,
    channel: Option<ChannelId>,
    deadline: Instant,
    streams: Streams,
    video: HashMap<Mid, VideoRx>,
    link: Option<Link>,
}

impl Conn {
    /// `created` is when signalling for it began, for setup times.
    pub fn new(
        client: ClientKey,
        rtc: Rtc,
        socket: std::net::UdpSocket,
        publish: Option<Publish>,
        events: UnboundedSender<ConnEvent>,
        created: Instant,
    ) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Conn {
            id: NEXT.fetch_add(1, Relaxed),
            client,
            rtc,
            socket: UdpSocket::from_std(socket),
            publish,
            events,
            created,
            connected_after: None,
            channel: None,
            deadline: created,
            streams: Streams::new(),
            video: HashMap::new(),
            link: None,
        }
    }
}

/// A non-blocking UDP socket on `ip`, any port.
pub fn bind(ip: IpAddr) -> anyhow::Result<std::net::UdpSocket> {
    let socket = std::net::UdpSocket::bind((ip, 0)).context("binding a client UDP socket")?;
    socket.set_nonblocking(true)?;
    Ok(socket)
}

/// A client peer connection as a browser would set one up: Opus with
/// in-band FEC and NACK, no RED (so the same bytes cross every server), VP8
/// with RTX, and its socket's address as the only candidate.
pub fn client_rtc(local: SocketAddr) -> Rtc {
    let mut codecs = CodecConfig::empty();
    codecs.enable_opus(true, false);
    codecs.enable_vp8(true);
    let mut params = codecs.params().to_vec();
    for p in &mut params {
        if p.spec().codec == Codec::Opus {
            p.set_fb_nack(true);
        }
    }
    // One certificate for all of them: generating one per connection would
    // add its cost to every setup time measured.
    static CERT: std::sync::OnceLock<str0m::config::DtlsCert> = std::sync::OnceLock::new();
    let cert = CERT.get_or_init(|| {
        str0m::crypto::from_feature_flags()
            .dtls_provider
            .generate_certificate()
            .expect("generating a DTLS certificate")
    });
    let mut config = Rtc::builder()
        .set_send_buffer_audio(AUDIO_PACKET_BUFFER)
        .set_send_buffer_video(VIDEO_PACKET_BUFFER)
        .set_dtls_cert(cert.clone())
        .set_nack_retries(NACK_RETRIES)
        .set_reordering_timeout_video(Some(VIDEO_HOLD));
    *config.codec_config() = CodecConfig::new_from_payload_params(params);
    let mut rtc = config.build(Instant::now());
    rtc.add_local_candidate(Candidate::host(local, "udp").expect("a host candidate"));
    rtc
}

pub enum Cmd {
    Add(Box<Conn>),
    AcceptOffer {
        conn: ConnId,
        offer: SdpOffer,
        reply: oneshot::Sender<Result<SdpAnswer, String>>,
    },
    AddCandidate {
        conn: ConnId,
        candidate: Candidate,
    },
    /// Text on the connection's first data channel.
    SendText {
        conn: ConnId,
        text: String,
    },
    Report(oneshot::Sender<Report>),
}

#[derive(Debug, Clone)]
pub struct ConnStatus {
    pub client: ClientKey,
    pub connected_after: Option<Duration>,
    pub publishing: bool,
    /// Publishers heard from at any time.
    pub publishers_heard: usize,
}

/// A worker's counts so far.
#[derive(Default)]
pub struct Report {
    pub receipts: Receipts,
    /// Measured packets sent, by publisher id.
    pub sent: Vec<(u32, u64)>,
    pub conns: Vec<ConnStatus>,
    pub dropped_down: u64,
    pub dropped_up: u64,
    pub send_errors: u64,
    pub video: VideoCounts,
}

/// Video events while measuring.
#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct VideoCounts {
    /// Keyframes publishers sent.
    pub keyframes: u64,
    /// Keyframe requests (PLI/FIR) publishers received.
    pub keyframe_requests: u64,
    /// PLIs subscribers sent after losing a frame for good.
    pub plis_sent: u64,
    /// Frames that arrived while their stream waited for a keyframe: a
    /// decoder could not show them.
    pub undecodable: u64,
    /// Frames received on each simulcast layer (single-layer video: 0).
    pub layer_frames: [u64; 3],
    /// Packets the shaped downlink dropped (queue full), all the run.
    pub link_drops: u64,
}

impl VideoCounts {
    pub fn merge(&mut self, o: &VideoCounts) {
        self.keyframes += o.keyframes;
        self.keyframe_requests += o.keyframe_requests;
        self.plis_sent += o.plis_sent;
        self.undecodable += o.undecodable;
        for (a, b) in self.layer_frames.iter_mut().zip(o.layer_frames) {
            *a += b;
        }
        self.link_drops += o.link_drops;
    }
}

#[derive(Clone)]
pub struct Worker {
    commands: mpsc::Sender<Cmd>,
    waker: Arc<Waker>,
}

impl Worker {
    pub fn spawn(index: usize, shared: Arc<Shared>, options: Options) -> anyhow::Result<Worker> {
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKE)?);
        let (commands, rx) = mpsc::channel();
        thread::Builder::new()
            .name(format!("media-{index}"))
            .spawn(move || Loop::new(poll, rx, shared, options, index as u64).run())?;
        Ok(Worker { commands, waker })
    }

    fn send(&self, cmd: Cmd) {
        if self.commands.send(cmd).is_ok() {
            let _ = self.waker.wake();
        }
    }

    pub fn add(&self, conn: Conn) {
        self.send(Cmd::Add(Box::new(conn)));
    }

    pub async fn accept_offer(&self, conn: ConnId, offer: SdpOffer) -> anyhow::Result<SdpAnswer> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::AcceptOffer { conn, offer, reply });
        rx.await
            .context("connection gone")?
            .map_err(anyhow::Error::msg)
    }

    pub fn add_candidate(&self, conn: ConnId, candidate: Candidate) {
        self.send(Cmd::AddCandidate { conn, candidate });
    }

    pub fn send_text(&self, conn: ConnId, text: String) {
        self.send(Cmd::SendText { conn, text });
    }

    pub async fn report(&self) -> Report {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::Report(reply));
        rx.await.unwrap_or_default()
    }
}

const WAKE: Token = Token(usize::MAX);

/// RTP and RTCP, per RFC 7983's first-byte ranges; the only packets loss
/// is injected into, so connection setup and consent checks are spared.
fn is_rtp(first: u8) -> bool {
    (128..=191).contains(&first)
}

/// xorshift64*: loss decisions only.
struct Rng(u64);

impl Rng {
    /// Uniform in [0, 1).
    fn unit(&mut self) -> f64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        let x = self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11;
        x as f64 / (1u64 << 53) as f64
    }

    fn chance(&mut self, p: f64) -> bool {
        p > 0.0 && self.unit() < p
    }
}

/// Everything a connection's polling touches besides the connection.
struct Net {
    shared: Arc<Shared>,
    options: Options,
    rng: Rng,
    receipts: Receipts,
    sent: HashMap<u32, u64>,
    dropped_down: u64,
    dropped_up: u64,
    send_errors: u64,
    video: VideoCounts,
}

struct Loop {
    poll: Poll,
    commands: mpsc::Receiver<Cmd>,
    conns: HashMap<ConnId, Conn>,
    /// Each connection's next str0m deadline; stale entries are skipped.
    timers: BinaryHeap<Reverse<(Instant, ConnId)>>,
    /// When each publisher sends its next frame.
    sends: BinaryHeap<Reverse<(Instant, ConnId)>>,
    /// When a shaped downlink delivers its next packet.
    releases: BinaryHeap<Reverse<(Instant, ConnId)>>,
    touched: HashSet<ConnId>,
    net: Net,
}

impl Loop {
    fn new(
        poll: Poll,
        commands: mpsc::Receiver<Cmd>,
        shared: Arc<Shared>,
        options: Options,
        seed: u64,
    ) -> Self {
        Loop {
            poll,
            commands,
            conns: HashMap::new(),
            timers: BinaryHeap::new(),
            sends: BinaryHeap::new(),
            releases: BinaryHeap::new(),
            touched: HashSet::new(),
            net: Net {
                shared,
                options,
                rng: Rng(0x9E37_79B9_7F4A_7C15 ^ (seed + 1).wrapping_mul(0xD1B5_4A32_D192_ED03)),
                receipts: Receipts::default(),
                sent: HashMap::new(),
                dropped_down: 0,
                dropped_up: 0,
                send_errors: 0,
                video: VideoCounts::default(),
            },
        }
    }

    fn run(mut self) {
        let mut events = Events::with_capacity(1024);
        let mut buf = vec![0u8; 2000];
        loop {
            loop {
                match self.commands.try_recv() {
                    Ok(cmd) => self.command(cmd),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => return,
                }
            }

            let now = Instant::now();
            while let Some(&Reverse((at, id))) = self.sends.peek() {
                if at > now {
                    break;
                }
                self.sends.pop();
                let Some(conn) = self.conns.get_mut(&id) else {
                    continue;
                };
                send_frame(conn, &mut self.net, now);
                // Keep the cadence; after a stall, start afresh.
                let interval = conn
                    .publish
                    .as_ref()
                    .map_or(FRAME, |p| p.interval(&self.net.options));
                let next = if now - at > 5 * interval {
                    now + interval
                } else {
                    at + interval
                };
                self.sends.push(Reverse((next, id)));
                self.touched.insert(id);
            }
            while let Some(&Reverse((at, id))) = self.releases.peek() {
                if at > now {
                    break;
                }
                self.releases.pop();
                self.deliver(id, now);
            }
            for id in std::mem::take(&mut self.touched) {
                self.poll_conn(id, now);
            }
            while let Some(&Reverse((at, id))) = self.timers.peek() {
                if at > now {
                    break;
                }
                self.timers.pop();
                if self.conns.get(&id).is_some_and(|c| c.deadline == at) {
                    self.poll_conn(id, now);
                }
            }

            let next = [self.sends.peek(), self.timers.peek(), self.releases.peek()]
                .into_iter()
                .flatten()
                .map(|Reverse((at, _))| *at)
                .min()
                .unwrap_or(now + MAX_WAIT)
                .min(now + MAX_WAIT);
            // Round up: mio's millisecond timeouts would wake early and spin.
            let wait = next.saturating_duration_since(Instant::now());
            let wait = Duration::from_millis(wait.as_micros().div_ceil(1000) as u64);
            if let Err(e) = self.poll.poll(&mut events, Some(wait)) {
                if e.kind() != ErrorKind::Interrupted {
                    warn!("poll failed: {e}");
                }
                continue;
            }
            for event in &events {
                if event.token() != WAKE {
                    self.receive(event.token().0 as ConnId, &mut buf);
                }
            }
        }
    }

    fn command(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Add(conn) => {
                let mut conn = *conn;
                let id = conn.id;
                if let Err(e) = self.poll.registry().register(
                    &mut conn.socket,
                    Token(id as usize),
                    Interest::READABLE,
                ) {
                    warn!("registering a socket: {e}");
                    return;
                }
                conn.link = self.net.options.downlink.map(|options| Link {
                    options,
                    busy_until: Instant::now(),
                    last_arrival: Instant::now(),
                    queue: VecDeque::new(),
                });
                if let Some(p) = &conn.publish {
                    // Spread publishers over the frame interval.
                    let offset = p.interval(&self.net.options).mul_f64(self.net.rng.unit());
                    self.sends.push(Reverse((Instant::now() + offset, id)));
                }
                self.conns.insert(id, conn);
                self.touched.insert(id);
            }
            Cmd::AcceptOffer { conn, offer, reply } => {
                let result = match self.conns.get_mut(&conn) {
                    Some(c) => c
                        .rtc
                        .sdp_api()
                        .accept_offer(offer)
                        .map_err(|e| e.to_string()),
                    None => Err("no such connection".into()),
                };
                let _ = reply.send(result);
                self.touched.insert(conn);
            }
            Cmd::AddCandidate { conn, candidate } => {
                if let Some(c) = self.conns.get_mut(&conn) {
                    c.rtc.add_remote_candidate(candidate);
                    self.touched.insert(conn);
                }
            }
            Cmd::SendText { conn, text } => {
                if let Some(c) = self.conns.get_mut(&conn) {
                    let sent = c
                        .channel
                        .and_then(|id| c.rtc.channel(id))
                        .map(|mut ch| ch.write(false, text.as_bytes()));
                    if !matches!(sent, Some(Ok(_))) {
                        warn!("conn {conn}: data channel write failed");
                    }
                    self.touched.insert(conn);
                }
            }
            Cmd::Report(reply) => {
                let _ = reply.send(self.report());
            }
        }
    }

    fn report(&self) -> Report {
        let mut receipts = Receipts::default();
        receipts.merge(&self.net.receipts);
        Report {
            receipts,
            sent: self.net.sent.iter().map(|(&p, &n)| (p, n)).collect(),
            conns: self
                .conns
                .values()
                .map(|c| ConnStatus {
                    client: c.client,
                    connected_after: c.connected_after,
                    publishing: c.publish.is_some(),
                    publishers_heard: c.streams.len(),
                })
                .collect(),
            dropped_down: self.net.dropped_down,
            dropped_up: self.net.dropped_up,
            send_errors: self.net.send_errors,
            video: self.net.video,
        }
    }

    fn receive(&mut self, id: ConnId, buf: &mut [u8]) {
        let Some(conn) = self.conns.get_mut(&id) else {
            return;
        };
        let Ok(destination) = conn.socket.local_addr() else {
            return;
        };
        loop {
            let (n, source) = match conn.socket.recv_from(buf) {
                Ok(r) => r,
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => {
                    debug!("conn {id}: recv failed: {e}");
                    break;
                }
            };
            if n > 0
                && is_rtp(buf[0])
                && self.net.shared.lossy.load(Relaxed)
                && self.net.rng.chance(self.net.options.loss_down)
            {
                self.net.dropped_down += 1;
                continue;
            }
            if let Some(link) = &mut conn.link {
                let now = Instant::now();
                let start = link.busy_until.max(now);
                if start.duration_since(now) > link.options.max_delay {
                    self.net.video.link_drops += 1;
                    continue;
                }
                // Serialization at the link's rate, with UDP/IP overhead.
                let bits = (n as u64 + 28) * 8;
                let bps = self.net.shared.link_bps.load(Relaxed).max(1);
                link.busy_until = start + Duration::from_nanos(bits * 1_000_000_000 / bps);
                let jitter = link.options.jitter.mul_f64(self.net.rng.unit());
                let arrival =
                    (link.busy_until + link.options.delay + jitter).max(link.last_arrival);
                link.last_arrival = arrival;
                if link.queue.is_empty() {
                    self.releases.push(Reverse((arrival, id)));
                }
                link.queue.push_back((arrival, source, buf[..n].to_vec()));
                continue;
            }
            feed(
                &mut conn.rtc,
                id,
                Instant::now(),
                source,
                destination,
                &buf[..n],
            );
        }
        self.touched.insert(id);
    }

    /// Hands a shaped downlink's due packets to the connection.
    fn deliver(&mut self, id: ConnId, now: Instant) {
        let Some(conn) = self.conns.get_mut(&id) else {
            return;
        };
        let Ok(destination) = conn.socket.local_addr() else {
            return;
        };
        let Some(link) = &mut conn.link else {
            return;
        };
        while let Some((at, ..)) = link.queue.front() {
            if *at > now {
                self.releases.push(Reverse((*at, id)));
                break;
            }
            let (_, source, data) = link.queue.pop_front().expect("peeked");
            feed(&mut conn.rtc, id, now, source, destination, &data);
        }
        self.touched.insert(id);
    }

    /// Feeds the time and drains output until str0m's next deadline.
    fn poll_conn(&mut self, id: ConnId, now: Instant) {
        let Some(conn) = self.conns.get_mut(&id) else {
            return;
        };
        if !conn.rtc.is_alive() {
            let _ = conn.events.send(ConnEvent::Closed);
            let mut conn = self.conns.remove(&id).expect("present");
            let _ = self.poll.registry().deregister(&mut conn.socket);
            return;
        }
        if let Err(e) = conn.rtc.handle_input(Input::Timeout(now)) {
            warn!("conn {id}: {e:?}");
            conn.rtc.disconnect();
        }
        let deadline = loop {
            match conn.rtc.poll_output() {
                Ok(Output::Timeout(t)) => break t,
                Ok(Output::Transmit(t)) => {
                    let net = &mut self.net;
                    if t.contents.first().is_some_and(|&b| is_rtp(b))
                        && net.shared.lossy.load(Relaxed)
                        && net.rng.chance(net.options.loss_up)
                    {
                        net.dropped_up += 1;
                        continue;
                    }
                    if conn.socket.send_to(&t.contents, t.destination).is_err() {
                        net.send_errors += 1;
                    }
                }
                Ok(Output::Event(e)) => event(conn, e, &mut self.net),
                Err(e) => {
                    warn!("conn {id}: {e:?}");
                    conn.rtc.disconnect();
                    break now + MAX_WAIT;
                }
            }
        };
        if deadline != conn.deadline {
            conn.deadline = deadline;
            self.timers.push(Reverse((deadline, id)));
        }
    }
}

fn event(conn: &mut Conn, event: Event, net: &mut Net) {
    match event {
        Event::Connected => {
            conn.connected_after = Some(conn.created.elapsed());
            debug!(
                "conn {} ({:?}, publishing {}): connected after {:?}",
                conn.id,
                conn.client,
                conn.publish.is_some(),
                conn.created.elapsed()
            );
            let _ = conn.events.send(ConnEvent::Connected);
        }
        Event::IceConnectionStateChange(state) => {
            debug!(
                "conn {}: ICE {state:?} after {:?}",
                conn.id,
                conn.created.elapsed()
            );
        }
        Event::ChannelOpen(id, _) => {
            conn.channel.get_or_insert(id);
            let _ = conn.events.send(ConnEvent::ChannelOpen);
        }
        Event::ChannelData(d) => {
            let _ = conn.events.send(ConnEvent::ChannelData(d.data));
        }
        Event::MediaData(d) if d.params.spec().codec == Codec::Vp8 => video_frame(conn, d, net),
        Event::MediaData(d) => {
            let Some(stamp) = Stamp::parse(&d.data) else {
                return;
            };
            let stream = conn.streams.entry(stamp.publisher).or_default();
            let fresh = stream.insert(stamp.seq);
            if stamp.measured() {
                let now_us = net.shared.micros(Instant::now());
                net.receipts.record(fresh, &stamp, now_us);
            }
        }
        Event::KeyframeRequest(req) => {
            if let Some(p) = &mut conn.publish {
                match req
                    .rid
                    .and_then(|r| LAYERS.iter().position(|(n, _)| **n == *r))
                {
                    Some(l) => p.keyframe[l] = true,
                    None => p.keyframe = [true; 3],
                }
                if net.shared.measuring.load(Relaxed) {
                    net.video.keyframe_requests += 1;
                }
            }
        }
        _ => {}
    }
}

/// A complete frame, counted as audio packets are; plus what a decoder
/// would make of it. str0m resends NACKed packets itself and hands over a
/// frame only once it is whole, so a frame not `contiguous` with the last
/// one means one in between was lost for good: the stream is broken until a
/// keyframe, which is asked for with a PLI.
fn video_frame(conn: &mut Conn, d: MediaData, net: &mut Net) {
    let Some((stamp, keyframe)) = Stamp::parse_vp8(&d.data) else {
        return;
    };
    let now = Instant::now();
    let rx = conn.video.entry(d.mid).or_default();
    if keyframe {
        rx.broken = false;
    } else if !d.contiguous {
        rx.broken = true;
    }
    let measured = stamp.measured();
    if rx.broken {
        if measured {
            net.video.undecodable += 1;
        }
        if rx.last_pli.is_none_or(|t| now - t >= PLI_INTERVAL) {
            rx.last_pli = Some(now);
            let asked = conn
                .rtc
                .writer(d.mid)
                .map(|mut w| w.request_keyframe(d.rid, KeyframeRequestKind::Pli));
            if matches!(asked, Some(Ok(()))) && measured {
                net.video.plis_sent += 1;
            }
        }
    }
    let fresh = conn
        .streams
        .entry(stamp.publisher)
        .or_default()
        .insert(stamp.seq);
    if measured && fresh {
        net.video.layer_frames[usize::from(stamp.layer().min(2))] += 1;
    }
    if measured {
        net.receipts.record(fresh, &stamp, net.shared.micros(now));
    }
}

fn send_frame(conn: &mut Conn, net: &mut Net, now: Instant) {
    let Some(publish) = &mut conn.publish else {
        return;
    };
    if !conn.rtc.is_connected() {
        return;
    }
    let pt = match publish.pt {
        Some(pt) => pt,
        None => {
            let codec = match publish.kind {
                MediaKind::Audio => Codec::Opus,
                MediaKind::Video => Codec::Vp8,
            };
            let Some(pt) = conn.rtc.writer(publish.mid).and_then(|w| {
                w.payload_params()
                    .find(|p| p.spec().codec == codec)
                    .map(|p| p.pt())
            }) else {
                return;
            };
            publish.pt = Some(pt);
            pt
        }
    };
    let measured = net.shared.measuring.load(Relaxed);
    let stamp = Stamp {
        flags: if measured { stamp::MEASURED } else { 0 },
        publisher: publish.publisher,
        seq: publish.seq,
        sent_us: net.shared.micros(now),
    };
    match publish.kind {
        MediaKind::Audio => {
            let Some(writer) = conn.rtc.writer(publish.mid) else {
                return;
            };
            let time = MediaTime::new(
                u64::from(publish.seq) * SAMPLES_PER_FRAME,
                Frequency::FORTY_EIGHT_KHZ,
            );
            // −30 dBov with voice: a talking participant, for servers that
            // track who is speaking.
            let written = writer.audio_level(-30, true).write(
                pt,
                now,
                time,
                stamp.payload(net.options.payload),
            );
            if let Err(e) = written {
                debug!("conn {}: write failed: {e:?}", conn.id);
                return;
            }
        }
        MediaKind::Video => {
            let video = net.options.video;
            let time = MediaTime::new(
                u64::from(publish.seq) * 90_000 / u64::from(video.fps.max(1)),
                Frequency::NINETY_KHZ,
            );
            // The same frame on every layer, each at its share of the size.
            let layers: &[(&str, f64)] = if publish.simulcast {
                &LAYERS
            } else {
                &[("", 1.0)]
            };
            let mut any = false;
            for (l, &(rid, share)) in layers.iter().enumerate() {
                let Some(mut writer) = conn.rtc.writer(publish.mid) else {
                    return;
                };
                if publish.simulcast {
                    writer = writer.rid(Rid::from(rid));
                }
                let key = std::mem::take(&mut publish.keyframe[l]);
                let full = if key {
                    video.keyframe_bytes
                } else {
                    video.frame_bytes
                };
                let size = (full as f64 * share) as usize;
                let frame = stamp.with_layer(l as u8).vp8_frame(key, size);
                if let Err(e) = writer.write(pt, now, time, frame) {
                    debug!("conn {}: write failed: {e:?}", conn.id);
                    publish.keyframe[l] |= key;
                    continue;
                }
                any = true;
                if key && measured {
                    net.video.keyframes += 1;
                }
            }
            if !any {
                return;
            }
        }
    }
    publish.seq = publish.seq.wrapping_add(1);
    if measured {
        *net.sent.entry(publish.publisher).or_default() += 1;
    }
}
