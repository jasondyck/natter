//! The media engine behind `/rtc`: rooms, participants and forwarding, on
//! one thread and one UDP socket, speaking LiveKit's model.
//!
//! Each participant has two peer connections, as LiveKit's dual-PC mode:
//! the **publisher** (the client offers; we only receive) and the
//! **subscriber** (we offer; we only send). Both run str0m in RTP mode:
//! every received packet is forwarded at once to each subscribed
//! participant, sharing one payload, with per-subscriber sequence number
//! and timestamp offsets (LiveKit's DownTrack munger).
//!
//! The signalling side (`livekit::signal`) builds both `Rtc`s off this
//! thread and hands them over in [`Command::Join`]; it then relays every
//! `SignalRequest` here and every `SignalResponse` back.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{AcqRel, Relaxed, Release};
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use livekit_protocol as lk;
use lk::{
    data_packet, data_stream, leave_request, participant_info, request_response, signal_request,
    signal_response,
};
use prost::Message as _;
use str0m::bwe::{Bitrate, BweKind};
use str0m::change::{SdpAnswer, SdpOffer, SdpPendingOffer};
use str0m::channel::{ChannelConfig, ChannelId, Reliability};
use str0m::format::{Codec, PayloadParams};
use str0m::media::{Direction, KeyframeRequestKind, MediaKind, Mid, Pt, Rid, Rids};
use str0m::net::{Protocol, Receive};
use str0m::rtp::{ExtensionValues, RtpPacket, RtpWrite, SeqNo, Ssrc};
use str0m::{Candidate, CandidateKind, Event, IceConnectionState, Input, Output, Rtc};
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TrySendError;
use tracing::{debug, info, warn};

use super::agent::Agents;
use super::allocator;
use super::audio_level::{self, AudioLevel};
use super::cc_detect;
use super::dd;
use super::fallback::{self, Fallback, Path, Pref};
use super::ids;
use super::placement::Placement;
use super::quality::{self, Scorer};
use super::red::{ToPrimary, Unpacked};
use super::rtc_config::Congestion;
use super::sdp::msid_tracks;
use super::tcp;
use super::token::VideoGrant;
use super::video::{self, Layer};
use super::webhook::{self, Webhooks};
use crate::config::RoomDefaults;
use crate::metrics::Metrics;

mod admin;
mod agents;
mod data_track;
pub use admin::{AdminError, AdminOp, AdminReply};

/// The LiveKit release whose behaviour natter matches. Clients gate
/// features on the version and protocol they are told.
pub const LIVEKIT_VERSION: &str = "1.13.10";
pub const PROTOCOL: i32 = 17;
const PING_INTERVAL: i32 = 5;
const PING_TIMEOUT: i32 = 15;

/// How long a new subscriber's video waits on the lowest layer for its
/// first bandwidth estimate before congestion control is taken as absent.
/// Chosen, not derived.
const ESTIMATE_GRACE: Duration = Duration::from_secs(5);

/// Rooms smaller than this regress a track to its backup codec once the
/// backup is published, instead of sending both (LiveKit's
/// `video.codec_regression_threshold`).
const CODEC_REGRESSION_THRESHOLD: usize = 5;

/// Longest the loop blocks on the socket, so commands are seen promptly.
const MAX_WAIT: Duration = Duration::from_millis(20);
/// Longest a transport goes unpolled.
const CHECK_INTERVAL: Duration = Duration::from_millis(250);
/// Subscriber renegotiation debounce, as LiveKit (`transport.go:61-64`).
const NEGOTIATION_DEBOUNCE: Duration = Duration::from_millis(150);
const FAST_NEGOTIATION: Duration = Duration::from_millis(10);
/// How long a client gets to answer a subscriber offer.
const OFFER_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a participant whose media path failed has to resume, as
/// LiveKit's `disconnectCleanupDuration`.
const RESUME_AFTER_MEDIA_LOSS: Duration = Duration::from_secs(5);
/// How long one whose WebSocket dropped has to resume.
const RESUME_AFTER_SIGNAL_LOSS: Duration = Duration::from_secs(15);
/// How often rooms check connection quality, as LiveKit.
const QUALITY_INTERVAL: Duration = Duration::from_secs(5);
/// How often a room's participant counts go out at most (LiveKit's
/// `roomUpdateInterval`).
const ROOM_UPDATE_INTERVAL: Duration = Duration::from_secs(5);
/// How many user-packet nonces a room remembers (LiveKit's deduper size).
const USER_PACKET_NONCES: usize = 100;
/// How long, and how much, reliable data a room keeps for joiners
/// (LiveKit's `dataMessageCacheTTL`, `dataMessageCacheSize` in bytes).
const RELIABLE_CACHE_TTL: Duration = Duration::from_secs(2);
const RELIABLE_CACHE_BYTES: usize = 100_000;
/// Unsequenced reliable data held for a joiner (server-sent: RoomService
/// SendData), at most (LiveKit's `cMaxJoiningUnsequencedReliableBytes`).
const JOINING_HELD_BYTES: usize = 100_000;

/// A reliable data packet as relayed, by its sender and sequence number.
struct CachedData {
    at: Instant,
    sender: String,
    seq: u32,
    data: Vec<u8>,
    destinations: Vec<String>,
}

pub type ConnId = u64;

/// The PC that sends a participant what it subscribes to: its subscriber
/// PC, or in single-PC mode its only one. A macro so the borrow stays on
/// the fields and other fields of the participant remain usable.
macro_rules! sub {
    ($p:expr) => {
        match &mut $p.subscriber {
            Some(t) => t,
            None => &mut $p.publisher,
        }
    };
}

/// The engines of a server, one per shard, and where rooms live on them.
#[derive(Clone)]
pub struct Handle {
    pub shards: Vec<Shard>,
    pub placement: Arc<Placement>,
    pub media: super::rtc_config::MediaConfig,
    /// The embedded TURN server, if on.
    pub turn: Option<crate::turn::Turn>,
}

impl Handle {
    /// Closes every room on every shard (SERVER_SHUTDOWN), waiting up to
    /// `wait` for them to go.
    pub async fn shutdown(&self, wait: Duration) {
        let mut replies = vec![];
        for shard in &self.shards {
            let (reply, rx) = tokio::sync::oneshot::channel();
            let op = Box::new(admin::AdminOp::Shutdown);
            if shard.commands.send(Command::Admin { op, reply }).is_ok() {
                replies.push(rx);
            }
        }
        let all = async {
            for rx in replies {
                let _ = rx.await;
            }
        };
        if tokio::time::timeout(wait, all).await.is_err() {
            warn!("stopping with rooms still open");
        }
    }
}

#[derive(Clone)]
pub struct Shard {
    pub commands: Commands,
    /// Where clients send this shard's media; its own UDP port.
    pub advertised: SocketAddr,
    /// Its ICE/TCP address, if TCP is on.
    pub tcp: Option<SocketAddr>,
}

/// A shard's command queue. The engine blocks reading its UDP socket, so
/// a send also wakes it with a datagram to that socket; without one a
/// command waited for the next packet or the read timeout (up to
/// `MAX_WAIT`, ~9 ms of every join on an idle engine). One datagram per
/// batch: the engine clears `pending` before it drains the queue.
#[derive(Clone)]
pub struct Commands {
    tx: std::sync::mpsc::Sender<Command>,
    wake: Arc<Wake>,
}

pub struct Wake {
    socket: UdpSocket,
    engine: SocketAddr,
    /// The wake datagrams' source, for the engine to tell them apart.
    from: SocketAddr,
    pending: AtomicBool,
}

impl Commands {
    /// A queue for the engine reading UDP on `engine_udp`, and the engine's
    /// side of its wake-ups.
    pub fn new(
        tx: std::sync::mpsc::Sender<Command>,
        engine_udp: SocketAddr,
    ) -> std::io::Result<(Self, Arc<Wake>)> {
        // From the engine's own IP, so it reaches the socket however that
        // is bound; loopback when that is every address.
        let ip = if engine_udp.ip().is_unspecified() {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        } else {
            engine_udp.ip()
        };
        let socket = UdpSocket::bind((ip, 0))?;
        let wake = Arc::new(Wake {
            from: socket.local_addr()?,
            socket,
            engine: SocketAddr::new(ip, engine_udp.port()),
            pending: AtomicBool::new(false),
        });
        Ok((
            Self {
                tx,
                wake: Arc::clone(&wake),
            },
            wake,
        ))
    }

    pub fn send(&self, cmd: Command) -> Result<(), std::sync::mpsc::SendError<Command>> {
        self.tx.send(cmd)?;
        if !self.wake.pending.swap(true, AcqRel) {
            let _ = self.wake.socket.send_to(&[0], self.wake.engine);
        }
        Ok(())
    }
}

pub enum Command {
    Join(Box<Join>),
    Signal {
        conn: ConnId,
        msg: Box<signal_request::Message>,
    },
    /// The WebSocket closed. Without a `leave` first, the participant
    /// stays a while so it can resume.
    Close {
        conn: ConnId,
    },
    /// A client reconnecting to its session (`reconnect=1`).
    Resume(Box<Resume>),
    /// LiveKit's RoomService, for a room on this shard (or, to list them,
    /// for all of its rooms).
    Admin {
        op: Box<AdminOp>,
        reply: admin::AdminTx,
    },
    /// An agent worker took a job of one of the room's dispatches.
    AgentJob {
        room: String,
        dispatch_id: String,
        job: Box<lk::Job>,
    },
}

/// A verified resume: the same identity, room and participant sid.
pub struct Resume {
    pub conn: ConnId,
    pub room: String,
    pub identity: String,
    pub sid: String,
    pub tx: Sender<lk::SignalResponse>,
    /// TURN servers for the client, with fresh credentials.
    pub ice_servers: Vec<lk::IceServer>,
    /// Why the client resumes (`ReconnectReason`): a failed peer
    /// connection counts towards the transport fallback.
    pub reason: i32,
}

/// A verified join: the token's grants, and both peer connections built.
pub struct Join {
    pub conn: ConnId,
    pub room: String,
    pub identity: String,
    pub name: String,
    pub metadata: String,
    pub attributes: HashMap<String, String>,
    pub kind: participant_info::Kind,
    pub grant: VideoGrant,
    pub room_config: Option<lk::RoomConfiguration>,
    pub auto_subscribe: bool,
    pub auto_subscribe_data: bool,
    /// The client sizes video to what it shows (adaptive stream): its video
    /// subscriptions start at the lowest layer until it says otherwise.
    pub adaptive_stream: bool,
    /// How the bandwidth to this client is estimated, and whether its
    /// video may be paused when even the lowest layer doesn't fit.
    pub congestion: Congestion,
    pub allow_pause: bool,
    pub protocol: i32,
    pub client: lk::ClientInfo,
    pub publisher: Box<Rtc>,
    /// `None` for single-PC (`/rtc/v1`): the one PC both publishes and
    /// subscribes, and the client makes every offer.
    pub subscriber: Option<Box<Rtc>>,
    pub tx: Sender<lk::SignalResponse>,
    /// TURN servers for the client, with fresh credentials.
    pub ice_servers: Vec<lk::IceServer>,
    /// The client can do ICE/TCP (every SDK but Go's).
    pub ice_tcp: bool,
    /// WHIP: no signalling after the answer (LiveKit's one-shot mode).
    pub one_shot: bool,
    /// When its signalling connection was made (LiveKit's session start).
    pub started: Instant,
}

/// A one-shot (WHIP) session that hasn't connected by then is dropped
/// (chosen: LiveKit fails ICE after 15 s and DTLS 10-20 s after that).
const ONE_SHOT_CONNECT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Side {
    Publisher,
    Subscriber,
}

struct Transport {
    rtc: Box<Rtc>,
    /// When str0m next wants a timeout fed in.
    deadline: Instant,
    /// Handed input since last polled.
    dirty: bool,
    connected: bool,
    /// When ICE last connected, since the last (re)start: how long-lived
    /// the connection was if it fails.
    connected_at: Option<Instant>,
    /// Where the client's packets last came from, and whether over TCP.
    last_from: Option<(SocketAddr, bool)>,
    /// The client's relayed candidates: packets from these came through
    /// TURN.
    relays: Vec<SocketAddr>,
}

impl Transport {
    fn new(rtc: Box<Rtc>) -> Self {
        Transport {
            rtc,
            deadline: Instant::now(),
            dirty: true,
            connected: false,
            connected_at: None,
            last_from: None,
            relays: vec![],
        }
    }

    /// What the connection runs over, as far as the server can tell.
    fn path(&self) -> Option<Path> {
        let (from, tcp) = self.last_from?;
        Some(if tcp {
            Path::Tcp
        } else if self.relays.contains(&from) {
            Path::Relay
        } else {
            Path::Udp
        })
    }
}

/// A track a participant publishes.
struct Published {
    info: lk::TrackInfo,
    kind: MediaKind,
    mid: Option<Mid>,
    /// Video: the publisher's simulcast layers, or its one stream (rid
    /// None), as they arrive.
    layers: Vec<Layer>,
    /// Payload types seen from the publisher, with what they carry.
    pts: Vec<(Pt, PayloadParams)>,
    bound_at: Instant,
    /// Video: a keyframe has arrived; until then one is asked for every
    /// `video::SEED_EVERY` (LiveKit's key-frame seeder).
    keyframe_seen: bool,
    seeded_at: Option<Instant>,
    /// Layer sizes come from the video itself (WHIP: an SDP has none), as
    /// LiveKit's `UpdateTrackInfoByVideoSizeChange`.
    size_from_media: bool,
    /// The RED payload type around Opus, if the publisher negotiated it.
    red_pt: Option<Pt>,
    level: AudioLevel,
    to_primary: ToPrimary,
    /// Who has a DownTrack for this track.
    subscribers: Vec<ConnId>,
    /// The publisher has been told someone subscribed.
    ever_subscribed: bool,
    scorer: Scorer,
    /// Cumulative stream counters at the last stats event, per layer.
    stats_last: Vec<(Option<Rid>, StreamCounts)>,
    /// Video SVC (VP9/AV1 in one stream): the dependency structure, the
    /// decode targets the publisher has active, and each spatial layer's
    /// rate.
    svc: Option<Svc>,
    /// Video: this is the backup codec's stream of the track with this sid
    /// (LiveKit's simulcast codecs): not a track of its own, it serves the
    /// primary's subscribers that can't decode the primary codec, or all of
    /// them once the track has regressed to it.
    backup_of: Option<String>,
    /// Primary video: its subscribers have moved to the backup codec and
    /// the publisher has been told to stop the primary (LiveKit's codec
    /// regression, in rooms smaller than `CODEC_REGRESSION_THRESHOLD`).
    regressed: bool,
    /// Video: the highest quality the publisher was last told anyone
    /// receives, per codec (primary, backup; None: nobody), and a lower
    /// one waiting out the debounce.
    dynacast: [Option<lk::VideoQuality>; 2],
    dynacast_lower: Option<([Option<lk::VideoQuality>; 2], Instant)>,
}

struct Svc {
    structure: Option<dd::Structure>,
    active: u32,
    spatial: [Layer; 3],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DownState {
    ToOpen,
    Negotiating(Mid),
    Open(Mid),
    ToStop(Mid),
    NegotiatingStop(Mid),
}

impl DownState {
    fn mid(self) -> Option<Mid> {
        match self {
            DownState::ToOpen => None,
            DownState::Negotiating(m)
            | DownState::Open(m)
            | DownState::ToStop(m)
            | DownState::NegotiatingStop(m) => Some(m),
        }
    }
}

/// LiveKit's stream allocator `StreamState`, per video downtrack.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamState {
    Inactive,
    Active,
    Paused,
}

/// A track forwarded to a subscriber.
struct DownTrack {
    track_sid: String,
    publisher_sid: String,
    publisher: ConnId,
    kind: MediaKind,
    state: DownState,
    /// Payload types the subscriber negotiated.
    opus_pt: Option<Pt>,
    red_pt: Option<Pt>,
    /// Added to the publisher's extended sequence number and timestamp.
    seq_offset: Option<u64>,
    ts_offset: u32,
    /// Video: the publisher's payload types and ours for the same codec
    /// (None when this subscriber can't receive it).
    video_pts: Vec<(Pt, Option<Pt>)>,
    /// Video: the layer forwarded (None until its first keyframe), and the
    /// last sequence number and timestamp sent, for switching layers.
    layer: Option<Option<Rid>>,
    /// Video: the highest spatial layer the subscriber wants (its track
    /// settings), None when it disabled the track.
    max_layer: Option<u8>,
    /// Video: the layer congestion control holds it to (`applied` None
    /// until first allocated; Some(None) while paused for bandwidth).
    hold: allocator::Hold,
    /// Video: what the subscriber was last told by `stream_state_update`,
    /// as LiveKit's stream allocator `Track`: ACTIVE when forwarding starts
    /// from anything else (its `OnResume`), PAUSED when congestion control
    /// pauses it; muting or disabling makes it inactive without a message.
    stream_state: StreamState,
    /// Video SVC: the decode target it receives.
    svc: video::SvcDown,
    /// Video: the subscriber has no payload type for the track's primary
    /// codec, so it takes the backup codec's stream; and which stream it
    /// was last forwarded from.
    cannot_primary: bool,
    on_backup: bool,
    last_out: Option<(u64, u32, Instant)>,
    told_publisher: bool,
    /// The subscriber has been told it may have the track.
    told_allowed: bool,
    /// When the subscription began, for LiveKit's subscribe times.
    created_at: Instant,
    scorer: Scorer,
    stats_last: StreamCounts,
    /// The published track's source, for LiveKit's metrics.
    source: lk::TrackSource,
}

struct Participant {
    conn: ConnId,
    sid: String,
    identity: String,
    name: String,
    metadata: String,
    attributes: HashMap<String, String>,
    kind: participant_info::Kind,
    grant: VideoGrant,
    state: participant_info::State,
    joined_at: SystemTime,
    version: u32,
    room: Arc<str>,
    auto_subscribe: bool,
    auto_subscribe_data: bool,
    adaptive_stream: bool,
    /// The latest `UpdateTrackSettings` for each track, also for tracks not
    /// subscribed yet.
    track_settings: HashMap<String, lk::UpdateTrackSettings>,
    congestion: Congestion,
    allow_pause: bool,
    /// Congestion control's state for what we send it.
    cc: Cc,
    protocol: i32,
    /// What the client said of itself, for its `ParticipantInfo`.
    client: lk::ClientInfo,
    /// OBS (by its browser name): its RTCP sender reports are incomplete.
    obs: bool,
    tx: Sender<lk::SignalResponse>,
    /// The client stopped reading signalling; drop it.
    backlogged: bool,
    closing: bool,
    publisher: Transport,
    subscriber: Option<Transport>,
    /// Single-PC: the client's receive-only m-lines, which forwarded
    /// tracks are assigned to, and whether we asked for a re-offer.
    slots: Vec<(Mid, MediaKind)>,
    reoffer_requested: bool,
    /// `add_track` requests not yet bound to an m-line, with their sids.
    pending_tracks: Vec<(String, lk::AddTrackRequest)>,
    /// When each track it asked to publish was asked for, by sid.
    publish_asked: HashMap<String, Instant>,
    /// m-line → client track id, from the publisher's latest offer.
    offered_tracks: HashMap<String, String>,
    published: Vec<Published>,
    /// Publisher SSRC → (published track, simulcast rid).
    ssrc_track: Vec<(Ssrc, usize, Option<Rid>)>,
    downtracks: Vec<DownTrack>,
    sub_pending: Option<SdpPendingOffer>,
    sub_pending_since: Option<Instant>,
    sub_offer_id: u32,
    /// When the next subscriber renegotiation may go out (debounce).
    negotiate_after: Option<Instant>,
    /// Whether the subscriber side has been negotiated once.
    negotiated: bool,
    /// Data channels by label, on each side.
    pub_channels: Vec<(ChannelId, String)>,
    sub_channels: Vec<(ChannelId, String)>,
    /// Quality levels last sent to this participant, by participant sid.
    quality_sent: HashMap<String, lk::ConnectionQuality>,
    /// Closed unless it resumes by then.
    cleanup_at: Option<Instant>,
    /// Restart ICE on the subscriber PC with the next offer (resume).
    restart_ice: bool,
    /// Which candidates the client is held to, after failures.
    fallback: Fallback,
    ice_tcp: bool,
    /// The TURN servers it was given (all of them, for when the fallback
    /// narrows them to TLS).
    ice_servers: Vec<lk::IceServer>,
    /// The signalling round trip the client reports in its pings.
    signal_rtt: Option<Duration>,
    /// Held to ICE/TCP: if ICE hasn't connected by then, TCP failed too.
    tcp_check_by: Option<Instant>,
    /// WHIP: nothing can be told to it, so it is closed, not resumed.
    one_shot: bool,
    /// When its signalling connected, and when it became active.
    started: Instant,
    active_at: Option<Instant>,
    /// The last sequence number and timestamp sent on each of its m-lines:
    /// a track that takes over an m-line carries on from them (str0m keeps
    /// the stream, and its SSRC; a fresh random start below the last would
    /// be dropped as replayed or old).
    mid_sent: Vec<(Mid, u64, u32)>,
    /// The last reliable data packet's sequence number from this client:
    /// after a resume it resends what came later (`last_message_seq`), and
    /// anything not newer is a duplicate.
    last_reliable_seq: u32,
    /// Who may subscribe to its tracks (`SubscriptionPermission`), by
    /// subscriber identity; None: everyone (LiveKit's default).
    sub_permission: Option<HashMap<String, lk::TrackPermission>>,
    /// Tracks it wants (auto-subscribe or asked for) that their
    /// publisher's permissions refuse, with the publisher: they are
    /// subscribed once allowed, as LiveKit's subscription manager keeps
    /// them desired.
    refused: Vec<(String, ConnId)>,
    /// Data tracks it publishes, and those it wants.
    data_tracks: Vec<data_track::DataTrack>,
    data_wants: Vec<data_track::DataWant>,
    /// The last handle it was given for a data track it receives.
    next_data_handle: u16,
    /// Reliable data can be written to it: its reliable channel has opened.
    /// Until then, the first sequence number missed from each sender (to
    /// replay from the room's cache), and server-sent data held whole
    /// (LiveKit's `reliableDataInfo`).
    reliable_ready: bool,
    joining_first: HashMap<String, u32>,
    joining_written: HashMap<String, u32>,
    joining_held: Vec<Vec<u8>>,
    joining_held_bytes: usize,
    /// How fast its `_data_track` channel drains, to drop what would wait.
    data_rate: data_track::DrainRate,
}

/// A subscriber's congestion control state (`Engine::allocate`).
#[derive(Default)]
struct Cc {
    /// The latest bandwidth estimate for what we send it, in bits/s.
    estimate: Option<u64>,
    /// When its video was last allocated.
    allocated_at: Option<Instant>,
    /// Audio bytes sent to it (with packet overhead), and the count at the
    /// last allocation: congestion control budgets audio at what it uses.
    audio_bytes_out: u64,
    audio_sample: Option<(u64, Instant)>,
    /// When congestion control last raised one of its tracks a layer.
    upgraded_at: Option<Instant>,
    /// When congestion control first had video to allocate for it.
    video_since: Option<Instant>,
    /// How much longer than the base hold its next upgrade waits, after
    /// upgrades that brought congestion (`allocator::BACKOFF_FACTOR`).
    backoff: f64,
    failed_at: Option<Instant>,
    /// Whether the estimator was last asked for more than is being sent.
    probing: bool,
    /// The highest estimate since the last congestion, and when
    /// (`allocator::PEAK_WINDOW`).
    peak: Option<(u64, Instant)>,
    /// Queueing, loss and delivered rate from TWCC feedback on what we
    /// send it.
    detector: cc_detect::Detector,
    /// Whether any TWCC feedback has come (else the round trip stands in).
    twcc_seen: bool,
    /// NACK reports it has sent on its video so far, and when the count
    /// last rose: loss tells congestion from a dip in the estimate.
    nacks_seen: u64,
    loss_at: Option<Instant>,
    /// The least round trip seen to it: queueing shows as the excess.
    min_rtt: Option<Duration>,
}

impl Participant {
    /// The TURN servers to offer: only TURN/TLS once the fallback has
    /// come to it (all of them if there is no TLS).
    fn ice_servers_now(&self) -> Vec<lk::IceServer> {
        if self.fallback.pref != Pref::Tls {
            return self.ice_servers.clone();
        }
        let tls: Vec<lk::IceServer> = self
            .ice_servers
            .iter()
            .map(|s| lk::IceServer {
                urls: s
                    .urls
                    .iter()
                    .filter(|u| u.starts_with("turns:"))
                    .cloned()
                    .collect(),
                ..s.clone()
            })
            .filter(|s| !s.urls.is_empty())
            .collect();
        if tls.is_empty() {
            self.ice_servers.clone()
        } else {
            tls
        }
    }

    /// Held to ICE/TCP, ICE gets a while to connect before the client
    /// counts as unable to (LiveKit: 8× the signalling RTT, 5-12 s; none
    /// past a second's RTT).
    fn arm_tcp_check(&mut self, now: Instant) {
        self.tcp_check_by = None;
        let rtt = self.signal_rtt.unwrap_or_default();
        if self.fallback.pref == Pref::Tcp && rtt < Duration::from_secs(1) {
            let wait = (rtt * 8).clamp(Duration::from_secs(5), Duration::from_secs(12));
            self.tcp_check_by = Some(now + wait);
        }
    }

    /// `force_relay` once the fallback has come to TURN/TLS.
    fn client_configuration(&self) -> Option<lk::ClientConfiguration> {
        (self.fallback.pref == Pref::Tls).then(|| lk::ClientConfiguration {
            force_relay: lk::ClientConfigSetting::Enabled as i32,
            ..Default::default()
        })
    }

    fn reply(&mut self, request_id: u32, reason: request_response::Reason, message: &str) {
        self.send(signal_response::Message::RequestResponse(
            lk::RequestResponse {
                request_id,
                reason: reason as i32,
                message: message.into(),
                ..Default::default()
            },
        ));
    }

    fn send(&mut self, message: signal_response::Message) {
        debug!(identity = %self.identity, "signal out: {}", variant_name(&message));
        match self.tx.try_send(response(message)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => self.backlogged = true,
            Err(TrySendError::Closed(_)) => {}
        }
    }

    fn transport(&mut self, side: Side) -> &mut Transport {
        match (side, &mut self.subscriber) {
            (Side::Subscriber, Some(t)) => t,
            _ => &mut self.publisher,
        }
    }

    fn single_pc(&self) -> bool {
        self.subscriber.is_none()
    }

    fn is_alive(&self) -> bool {
        !self.closing
            && self.publisher.rtc.is_alive()
            && self.subscriber.as_ref().is_none_or(|t| t.rtc.is_alive())
    }

    fn hidden(&self) -> bool {
        self.grant.hidden
    }

    /// Recorders and egress skip publishers' subscription permissions
    /// (LiveKit's `IsParticipantExemptFromTrackPermissionsRestrictions`).
    fn is_recorder(&self) -> bool {
        self.grant.recorder || self.kind == participant_info::Kind::Egress
    }

    /// Agents and recorders don't count as participants of the room: not
    /// in its numbers, its limit, or keeping it open (LiveKit's
    /// `IsDependent`).
    /// Whether it takes packet trailers (`CAP_PACKET_TRAILER`).
    fn takes_trailers(&self) -> bool {
        self.client
            .capabilities
            .contains(&(lk::client_info::Capability::CapPacketTrailer as i32))
    }

    fn is_agent(&self) -> bool {
        self.grant.agent || self.kind == participant_info::Kind::Agent
    }

    fn is_dependent(&self) -> bool {
        self.grant.agent
            || self.grant.recorder
            || matches!(
                self.kind,
                participant_info::Kind::Agent | participant_info::Kind::Egress
            )
    }

    /// Whether `subscriber` may subscribe to its track `track_sid`
    /// (LiveKit's `hasPermissionLocked`).
    fn permits(&self, track_sid: &str, subscriber: &Participant) -> bool {
        subscriber.is_recorder()
            || self.sub_permission.as_ref().is_none_or(|m| {
                m.get(&subscriber.identity)
                    .is_some_and(|t| t.all_tracks || t.track_sids.iter().any(|s| s == track_sid))
            })
    }

    fn permission(&self) -> lk::ParticipantPermission {
        let g = &self.grant;
        lk::ParticipantPermission {
            can_subscribe: g.can_subscribe(),
            can_publish: g.can_publish(),
            can_publish_data: g.can_publish_data(),
            can_publish_sources: g
                .can_publish_sources
                .iter()
                .filter_map(|s| lk::TrackSource::from_str_name(&s.to_ascii_uppercase()))
                .map(|s| s as i32)
                .collect(),
            hidden: g.hidden,
            recorder: g.recorder,
            can_update_metadata: g.can_update_own_metadata(),
            agent: g.agent,
            can_subscribe_metrics: g.can_subscribe_metrics.unwrap_or(false),
            can_manage_agent_session: g.can_manage_agent_session.unwrap_or(false),
        }
    }

    /// The side its media goes out on.
    fn sending_side(&self) -> Side {
        if self.subscriber.is_some() {
            Side::Subscriber
        } else {
            Side::Publisher
        }
    }

    /// Just who it is, for events about its tracks.
    fn who(&self) -> lk::ParticipantInfo {
        lk::ParticipantInfo {
            sid: self.sid.clone(),
            identity: self.identity.clone(),
            ..Default::default()
        }
    }

    fn info(&self) -> lk::ParticipantInfo {
        let joined = self
            .joined_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        lk::ParticipantInfo {
            sid: self.sid.clone(),
            identity: self.identity.clone(),
            state: self.state as i32,
            tracks: self
                .published
                .iter()
                .filter(|t| t.mid.is_some() && t.backup_of.is_none())
                .map(|t| t.info.clone())
                .collect(),
            metadata: self.metadata.clone(),
            joined_at: joined.as_secs() as i64,
            joined_at_ms: joined.as_millis() as i64,
            name: self.name.clone(),
            version: self.version,
            permission: Some(self.permission()),
            is_publisher: self.published.iter().any(|t| t.mid.is_some())
                || !self.data_tracks.is_empty(),
            data_tracks: self.data_tracks.iter().map(|t| t.info.clone()).collect(),
            kind: self.kind as i32,
            client_protocol: self.client.client_protocol,
            capabilities: self.client.capabilities.clone(),
            attributes: self.attributes.clone(),
            ..Default::default()
        }
    }

    /// The participant's speaking level: the loudest active microphone.
    fn speaker_level(&mut self, now: Instant) -> Option<f64> {
        self.published
            .iter_mut()
            .filter(|t| t.info.source == lk::TrackSource::Microphone as i32 && t.mid.is_some())
            .filter_map(|t| match t.level.level(now) {
                (level, true) => Some(level),
                _ => None,
            })
            .max_by(f64::total_cmp)
    }

    /// The lowest of the qualities of everything it sends and receives.
    fn quality(&self) -> (lk::ConnectionQuality, f32) {
        let lowest = self
            .published
            .iter()
            .filter(|t| t.mid.is_some())
            .map(|t| &t.scorer)
            .chain(
                self.downtracks
                    .iter()
                    .filter(|d| matches!(d.state, DownState::Open(_)))
                    .map(|d| &d.scorer),
            )
            .min_by(|a, b| a.score().total_cmp(&b.score()));
        match lowest {
            Some(s) => (s.quality(), s.mos()),
            None => (lk::ConnectionQuality::Excellent, quality::mos(100.0)),
        }
    }
}

struct Room {
    sid: String,
    name: Arc<str>,
    created: SystemTime,
    empty_timeout: u32,
    departure_timeout: u32,
    max_participants: u32,
    metadata: String,
    members: Vec<usize>,
    created_at: Instant,
    /// When a participant last left.
    left_at: Option<Instant>,
    /// A participant other than an agent or recorder has joined.
    had_participants: bool,
    /// Idle with only agents or recorders left: they have been sent away
    /// (ROOM_CLOSED), and it goes once they have.
    idle_closed: bool,
    /// Speaker levels last broadcast, by participant sid.
    speakers: HashMap<String, f32>,
    last_counts: (u32, u32, bool),
    /// When counts last went out, and whether a change is waiting for
    /// `ROOM_UPDATE_INTERVAL` to pass.
    counts_sent_at: Option<Instant>,
    counts_pending: bool,
    /// Deleted by an admin: gone once its participants are.
    deleted: bool,
    /// The latest user packets' nonces, to drop duplicates (LiveKit's
    /// `UserPacketDeduper`).
    nonces: VecDeque<[u8; 16]>,
    /// Reliable data relayed in the last `RELIABLE_CACHE_TTL`, for those
    /// still joining (LiveKit's `dataMessageCache`).
    reliable: VecDeque<CachedData>,
    reliable_bytes: usize,
    /// What end-to-end encrypted clients recognise server-made frames by
    /// (`sif_trailer`): random, as LiveKit's `RandomSecret`.
    trailer: Vec<u8>,
    agents: agents::RoomAgents,
}

impl Room {
    /// A room as its creator configured it, the server's defaults
    /// (`room:` in the config) filling in the rest.
    fn new(
        name: Arc<str>,
        config: Option<&lk::RoomConfiguration>,
        defaults: &RoomDefaults,
        now: Instant,
    ) -> Self {
        let pick = |v: u32, default: u32| if v > 0 { v } else { default };
        Room {
            sid: ids::new(ids::ROOM),
            name,
            created: SystemTime::now(),
            empty_timeout: pick(
                config.map_or(0, |c| c.empty_timeout),
                defaults.empty_timeout,
            ),
            departure_timeout: pick(
                config.map_or(0, |c| c.departure_timeout),
                defaults.departure_timeout,
            ),
            max_participants: pick(
                config.map_or(0, |c| c.max_participants),
                defaults.max_participants,
            ),
            metadata: config.map(|c| c.metadata.clone()).unwrap_or_default(),
            members: vec![],
            created_at: now,
            left_at: None,
            had_participants: false,
            idle_closed: false,
            speakers: HashMap::new(),
            last_counts: (0, 0, false),
            // LiveKit's proxy takes the new room as its first refresh.
            counts_sent_at: Some(now),
            counts_pending: false,
            deleted: false,
            nonces: VecDeque::new(),
            reliable: VecDeque::new(),
            reliable_bytes: 0,
            trailer: (0..4)
                .flat_map(|_| ids::new("").into_bytes())
                .take(43)
                .collect(),
            agents: Default::default(),
        }
    }

    fn info(&self, participants: &[Participant]) -> lk::Room {
        let created = self.created.duration_since(UNIX_EPOCH).unwrap_or_default();
        let members = || self.members.iter().map(|&i| &participants[i]);
        // As LiveKit's `updateProto`: hidden participants count, agents and
        // recorders don't; every publisher does.
        lk::Room {
            sid: self.sid.clone(),
            name: self.name.to_string(),
            empty_timeout: self.empty_timeout,
            departure_timeout: self.departure_timeout,
            max_participants: self.max_participants,
            creation_time: created.as_secs() as i64,
            creation_time_ms: created.as_millis() as i64,
            enabled_codecs: enabled_codecs(),
            metadata: self.metadata.clone(),
            num_participants: members().filter(|p| !p.is_dependent()).count() as u32,
            num_publishers: members().filter(|p| p.info().is_publisher).count() as u32,
            active_recording: members().any(Participant::is_recorder),
            ..Default::default()
        }
    }

    /// Whether it has gone idle (LiveKit's `CloseIfEmpty`): nobody but
    /// agents and recorders in it, for `departure_timeout` since someone
    /// last left, or `empty_timeout` since it was created if nobody else
    /// ever came.
    fn idle(&self, participants: &[Participant], now: Instant) -> bool {
        if self
            .members
            .iter()
            .any(|&i| !participants[i].is_dependent())
        {
            return false;
        }
        let (since, timeout) = match self.left_at {
            Some(at) if self.had_participants => (at, self.departure_timeout),
            _ => (self.created_at, self.empty_timeout),
        };
        now.duration_since(since) >= Duration::from_secs(timeout.into())
    }

    /// Gone: deleted or idle, and its last participants have left.
    fn expired(&self, participants: &[Participant], now: Instant) -> bool {
        self.members.is_empty()
            && (self.deleted || self.idle_closed || self.idle(participants, now))
    }
}

pub fn server_info(node_id: &str, region: &str) -> lk::ServerInfo {
    lk::ServerInfo {
        edition: lk::server_info::Edition::Standard as i32,
        version: LIVEKIT_VERSION.into(),
        protocol: PROTOCOL,
        region: region.to_owned(),
        node_id: node_id.to_owned(),
        agent_protocol: 1,
        ..Default::default()
    }
}

/// What every engine is configured with.
#[derive(Clone)]
pub struct Options {
    /// Joins beyond this many participants on the server are refused.
    pub max_participants: usize,
    /// Admins may unmute (LiveKit `room.enable_remote_unmute`).
    pub remote_unmute: bool,
    pub room_defaults: RoomDefaults,
    /// This node's region (LiveKit `region`).
    pub region: String,
    /// This node's id, shared by its shards.
    pub node_id: String,
}

fn enabled_codecs() -> Vec<lk::Codec> {
    [
        "audio/opus",
        "audio/red",
        "video/VP8",
        "video/H264",
        "video/VP9",
        "video/AV1",
        "video/H265",
        "video/rtx",
    ]
    .into_iter()
    .map(|mime| lk::Codec {
        mime: mime.into(),
        fmtp_line: String::new(),
    })
    .collect()
}

pub struct Engine {
    /// This engine's index among the server's shards.
    shard: usize,
    placement: Arc<Placement>,
    /// Participants and rooms last added to the server-wide gauges.
    counted: (u64, u64),
    /// Published and subscribed tracks (audio, video) last added to
    /// LiveKit's gauges, and when they were last counted.
    tracks_counted: [i64; 4],
    tracks_counted_at: Option<Instant>,
    socket: UdpSocket,
    /// ICE/TCP, when on: frames to and from the TCP thread, and which
    /// transport each TCP peer's frames go to.
    tcp: Option<tcp::Link>,
    tcp_routes: HashMap<SocketAddr, (ConnId, Side)>,
    /// Transport preferences by room and identity, kept a while after
    /// their participant goes so a full reconnect keeps them.
    ice_prefs: HashMap<(String, String), (Pref, Instant)>,
    /// Joins beyond this many participants on the server are refused, so an
    /// overloaded server turns new calls away instead of degrading all.
    max_participants: usize,
    /// Where packets arrive, as str0m's ICE needs it: the host candidate,
    /// i.e. the advertised address. str0m discards checks to any other,
    /// so with NATTER_UDP_BIND not the public IP (a 1:1 NAT, say), the
    /// socket's own address would leave UDP dead.
    local: SocketAddr,
    metrics: Arc<Metrics>,
    node_id: String,
    participants: Vec<Participant>,
    by_conn: HashMap<ConnId, usize>,
    rooms: HashMap<Arc<str>, Room>,
    routes: HashMap<SocketAddr, (ConnId, Side)>,
    /// What polling transports produced besides packets to send.
    events: VecDeque<(usize, Side, Event)>,
    touched: Vec<(usize, Side)>,
    /// Every transport's next deadline, soonest first. Entries go stale when
    /// a transport is polled again; they are skipped when popped (the
    /// transport's own `deadline` no longer matches).
    timers: BinaryHeap<Reverse<(Instant, ConnId, Side)>>,
    /// Subscriber renegotiations held back by the debounce, by when due.
    negotiations: BinaryHeap<Reverse<(Instant, ConnId)>>,
    membership_changed: bool,
    /// Admins may unmute (LiveKit `room.enable_remote_unmute`).
    remote_unmute: bool,
    room_defaults: RoomDefaults,
    region: String,
    /// Server-initiated RPCs (RoomService PerformRpc) awaiting replies.
    rpcs: HashMap<String, admin::PendingRpc>,
    webhooks: Option<Webhooks>,
    agents: Agents,
    /// The server is stopping (`AdminOp::Shutdown`): every room closes
    /// (SERVER_SHUTDOWN), joins are refused, and these are answered once
    /// the last room has gone.
    shutdown: Option<Vec<admin::AdminTx>>,
    /// This engine's own command queue, for what comes back to it.
    back: Commands,
    wake: Arc<Wake>,
}

/// Runs forever on its own thread.
#[allow(clippy::too_many_arguments)]
pub fn run(
    shard: usize,
    placement: Arc<Placement>,
    socket: UdpSocket,
    advertised: SocketAddr,
    tcp: Option<tcp::Link>,
    opts: Options,
    webhooks: Option<Webhooks>,
    agents: Agents,
    back: Commands,
    wake: Arc<Wake>,
    commands: Receiver<Command>,
    metrics: Arc<Metrics>,
) {
    let local = advertised;
    let mut engine = Engine {
        shard,
        placement,
        counted: (0, 0),
        tracks_counted: [0; 4],
        tracks_counted_at: None,
        socket,
        tcp,
        tcp_routes: HashMap::new(),
        ice_prefs: HashMap::new(),
        max_participants: opts.max_participants,
        local,
        metrics,
        node_id: opts.node_id,
        participants: vec![],
        by_conn: HashMap::new(),
        rooms: HashMap::new(),
        routes: HashMap::new(),
        events: VecDeque::new(),
        touched: vec![],
        timers: BinaryHeap::new(),
        negotiations: BinaryHeap::new(),
        membership_changed: false,
        remote_unmute: opts.remote_unmute,
        room_defaults: opts.room_defaults,
        region: opts.region,
        rpcs: HashMap::new(),
        webhooks,
        agents,
        shutdown: None,
        back,
        wake,
    };
    let mut buf = vec![0u8; 2000];
    let mut read_timeout = None;
    let mut scan_all = true;
    let mut next_speakers = Instant::now();
    let mut next_quality = Instant::now() + QUALITY_INTERVAL;
    let mut next_housekeeping = Instant::now();

    loop {
        engine.metrics.engine_loops_total.fetch_add(1, Relaxed);
        // Before draining, so a command sent after the drain wakes us.
        engine.wake.pending.store(false, Release);
        loop {
            match commands.try_recv() {
                Ok(cmd) => {
                    scan_all = true;
                    engine.command(cmd);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return,
            }
        }

        let now = Instant::now();
        if now >= next_housekeeping {
            next_housekeeping = now + Duration::from_millis(100);
            engine.reap(now);
            engine.seed_keyframes(now);
            engine.dynacast(now);
            engine.allocate_due(now);
        }
        if engine.membership_changed {
            engine.membership_changed = false;
            engine.rebuild_index();
            scan_all = true;
        }
        if now >= next_speakers {
            next_speakers = now + audio_level::Config::default().update_interval;
            engine.speakers(now);
        }
        if now >= next_quality {
            next_quality = now + QUALITY_INTERVAL;
            engine.connection_quality();
        }

        // Only transports handed input or whose timer fired can have
        // output. Scanning them all per packet would cost O(transports) for
        // every forward, so due timers come off a heap; a full scan is only
        // for when membership or commands may have changed anything.
        if scan_all {
            scan_all = false;
            engine.touched.clear();
            engine.timers.clear();
            for i in 0..engine.participants.len() {
                for side in [Side::Publisher, Side::Subscriber] {
                    engine.poll_and_schedule(i, side, now, true);
                }
            }
        } else {
            while let Some(&Reverse((at, conn))) = engine.negotiations.peek() {
                if at > now {
                    break;
                }
                engine.negotiations.pop();
                if let Some(&i) = engine.by_conn.get(&conn) {
                    engine.touched.push((i, Side::Subscriber));
                }
            }
            let touched = std::mem::take(&mut engine.touched);
            for &(i, side) in &touched {
                if i < engine.participants.len() {
                    engine.poll_and_schedule(i, side, now, false);
                }
            }
            engine.touched = touched;
            engine.touched.clear();
            while let Some(&Reverse((at, conn, side))) = engine.timers.peek() {
                if at > now {
                    break;
                }
                engine.timers.pop();
                let Some(&i) = engine.by_conn.get(&conn) else {
                    continue;
                };
                if engine.participants[i].transport(side).deadline != at {
                    continue;
                }
                engine.poll_and_schedule(i, side, now, true);
            }
        }
        let earliest = engine
            .timers
            .peek()
            .map_or(now + MAX_WAIT, |Reverse((at, _, _))| *at)
            .min(now + MAX_WAIT);
        let earliest = engine
            .negotiations
            .peek()
            .map_or(earliest, |Reverse((at, _))| earliest.min(*at));

        if !engine.events.is_empty() {
            while let Some((i, side, event)) = engine.events.pop_front() {
                engine.event(i, side, event);
            }
            continue;
        }

        let wait = earliest
            .min(next_speakers)
            .min(next_housekeeping)
            .saturating_duration_since(Instant::now())
            .max(Duration::from_millis(1));
        let wait = Some(Duration::from_millis(wait.as_millis() as u64));
        if wait != read_timeout {
            engine
                .socket
                .set_read_timeout(wait)
                .expect("setting socket read timeout");
            read_timeout = wait;
        }
        engine.receive(&mut buf);
    }
}

impl Engine {
    /// Polls a transport and files its next deadline. `always` pushes even
    /// an unchanged deadline (its heap entry was just consumed).
    fn poll_and_schedule(&mut self, i: usize, side: Side, now: Instant, always: bool) {
        let before = self.participants[i].transport(side).deadline;
        let deadline = self.poll(i, side, now);
        if always || deadline != before {
            let conn = self.participants[i].conn;
            self.timers.push(Reverse((deadline, conn, side)));
        }
    }

    fn receive(&mut self, buf: &mut [u8]) {
        let (n, source) = match self.socket.recv_from(buf) {
            Ok(r) => r,
            Err(e) => {
                if !matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) {
                    warn!("UDP read failed: {e}");
                }
                return;
            }
        };
        if self.tcp.as_ref().is_some_and(|t| t.wake_from == source) {
            return self.receive_tcp();
        }
        if source == self.wake.from {
            return;
        }
        let destination = self.local;
        let media = buf[0] >= 20;
        let Ok(contents) = buf[..n].try_into() else {
            return;
        };
        let input = Input::Receive(
            Instant::now(),
            Receive {
                proto: Protocol::Udp,
                source,
                destination,
                contents,
            },
        );
        self.route(input, source, false, media);
    }

    /// Frames the TCP thread has passed on.
    fn receive_tcp(&mut self) {
        let Some(link) = &self.tcp else {
            return;
        };
        let destination = link.local;
        let frames: Vec<_> = link.drain().collect();
        for (source, frame) in frames {
            let media = frame.first().is_some_and(|b| *b >= 20);
            let Ok(contents) = frame.as_slice().try_into() else {
                continue;
            };
            let input = Input::Receive(
                Instant::now(),
                Receive {
                    proto: Protocol::Tcp,
                    source,
                    destination,
                    contents,
                },
            );
            // str0m in RTP mode holds one received packet until polled:
            // poll after each frame, as the UDP path does per datagram.
            if let Some((i, side)) = self.route(input, source, true, media) {
                self.poll_and_schedule(i, side, Instant::now(), false);
            }
        }
    }

    /// Hands input to the transport it belongs to: by its source, once
    /// known, else whichever accepts it (its STUN credentials).
    /// Returns the transport it went to.
    ///
    /// `media`: DTLS, RTP or RTCP rather than STUN (RFC 7983: STUN starts
    /// below 20). The path a connection runs over is where those come
    /// from; ICE checks arrive on every candidate pair.
    fn route(
        &mut self,
        input: Input,
        source: SocketAddr,
        tcp: bool,
        media: bool,
    ) -> Option<(usize, Side)> {
        let routes = if tcp { &self.tcp_routes } else { &self.routes };
        let routed = routes.get(&source).and_then(|&(conn, side)| {
            let i = *self.by_conn.get(&conn)?;
            let p = &mut self.participants[i];
            p.transport(side).rtc.accepts(&input).then_some((i, side))
        });
        let found = routed.or_else(|| {
            self.participants.iter_mut().enumerate().find_map(|(i, p)| {
                [Side::Publisher, Side::Subscriber]
                    .into_iter()
                    .find(|&s| p.transport(s).rtc.accepts(&input))
                    .map(|s| (i, s))
            })
        });
        let Some((i, side)) = found else {
            debug!(tcp, %source, "no transport accepts input");
            return None;
        };
        if routed.is_none() {
            let routes = if tcp {
                &mut self.tcp_routes
            } else {
                &mut self.routes
            };
            routes.insert(source, (self.participants[i].conn, side));
        }
        let t = self.participants[i].transport(side);
        t.dirty = true;
        if media {
            t.last_from = Some((source, tcp));
        }
        if let Err(e) = t.rtc.handle_input(input) {
            warn!("bad input: {e:?}");
            self.participants[i].transport(side).rtc.disconnect();
        }
        self.touched.push((i, side));
        Some((i, side))
    }

    /// Feeds a due timeout, then polls until str0m has nothing to do
    /// before its next one. Returns that deadline.
    fn poll(&mut self, i: usize, side: Side, now: Instant) -> Instant {
        self.metrics.transport_polls_total.fetch_add(1, Relaxed);
        if side == Side::Subscriber {
            let p = &mut self.participants[i];
            if let Some(due) = negotiate(p, now) {
                let conn = p.conn;
                self.negotiations.push(Reverse((due, conn)));
            }
        }
        let p = &mut self.participants[i];
        // Single-PC: the subscriber side is the publisher PC, polled as such.
        let side = if p.single_pc() { Side::Publisher } else { side };
        let t = p.transport(side);
        if t.deadline <= now {
            t.dirty = true;
            if t.rtc.is_alive() {
                let _ = t.rtc.handle_input(Input::Timeout(now));
            }
        }
        if !t.dirty {
            return t.deadline;
        }
        t.dirty = false;
        let mut fed = false;
        loop {
            if !t.rtc.is_alive() {
                t.deadline = now + CHECK_INTERVAL;
                return t.deadline;
            }
            match t.rtc.poll_output() {
                Ok(Output::Transmit(tx)) if tx.proto == Protocol::Tcp => {
                    if let Some(link) = &self.tcp {
                        link.send(tx.destination, &tx.contents);
                    }
                }
                Ok(Output::Transmit(tx)) => {
                    if let Err(e) = self.socket.send_to(&tx.contents, tx.destination) {
                        debug!("UDP send failed: {e}");
                        self.metrics.udp_send_errors_total.fetch_add(1, Relaxed);
                    }
                }
                // The pacer releases a written packet only after a timeout.
                Ok(Output::Timeout(at)) if !fed && at <= Instant::now() => {
                    fed = true;
                    let _ = t.rtc.handle_input(Input::Timeout(Instant::now()));
                }
                Ok(Output::Timeout(at)) => {
                    t.deadline = at.min(now + CHECK_INTERVAL);
                    return t.deadline;
                }
                Ok(Output::Event(e)) => self.events.push_back((i, side, e)),
                Err(e) => {
                    warn!("poll_output failed: {e:?}");
                    t.rtc.disconnect();
                }
            }
        }
    }

    fn rebuild_index(&mut self) {
        self.by_conn.clear();
        for room in self.rooms.values_mut() {
            room.members.clear();
        }
        for (i, p) in self.participants.iter().enumerate() {
            self.by_conn.insert(p.conn, i);
            if let Some(room) = self.rooms.get_mut(&p.room) {
                room.members.push(i);
            }
        }
        let by_conn = &self.by_conn;
        self.routes
            .retain(|_, (conn, _)| by_conn.contains_key(conn));
        self.tcp_routes
            .retain(|_, (conn, _)| by_conn.contains_key(conn));
        // The gauges are server-wide; add this shard's change.
        let now = (self.participants.len() as u64, self.rooms.len() as u64);
        self.metrics
            .peers
            .fetch_add(now.0.wrapping_sub(self.counted.0), Relaxed);
        self.metrics
            .rooms
            .fetch_add(now.1.wrapping_sub(self.counted.1), Relaxed);
        self.counted = now;
        self.placement.set_load(self.shard, self.participants.len());
    }

    /// LiveKit's published and subscribed track gauges, recounted once a
    /// second; this shard adds its change.
    fn count_tracks(&mut self, now: Instant) {
        if self
            .tracks_counted_at
            .is_some_and(|at| now.duration_since(at) < Duration::from_secs(1))
        {
            return;
        }
        self.tracks_counted_at = Some(now);
        let mut counts = [0i64; 4];
        for p in &self.participants {
            for t in p
                .published
                .iter()
                .filter(|t| t.mid.is_some() && t.backup_of.is_none())
            {
                counts[usize::from(t.kind == MediaKind::Video)] += 1;
            }
            for d in p
                .downtracks
                .iter()
                .filter(|d| matches!(d.state, DownState::Open(_)))
            {
                counts[2 + usize::from(d.kind == MediaKind::Video)] += 1;
            }
        }
        let lk = &self.metrics.lk;
        for k in 0..2 {
            lk.published[k].fetch_add(counts[k] - self.tracks_counted[k], Relaxed);
            lk.subscribed[k].fetch_add(counts[2 + k] - self.tracks_counted[2 + k], Relaxed);
        }
        self.tracks_counted = counts;
    }

    // ---- Commands -------------------------------------------------------

    fn command(&mut self, cmd: Command) {
        match cmd {
            Command::Join(join) => self.join(*join),
            Command::Signal { conn, msg } => {
                if let Some(&i) = self.by_conn.get(&conn) {
                    self.signal(i, *msg);
                }
            }
            Command::Close { conn } => {
                if let Some(&i) = self.by_conn.get(&conn) {
                    let p = &mut self.participants[i];
                    let deadline = Instant::now() + RESUME_AFTER_SIGNAL_LOSS;
                    p.cleanup_at = Some(p.cleanup_at.map_or(deadline, |at| at.min(deadline)));
                }
            }
            Command::Resume(resume) => self.resume(*resume),
            Command::Admin { op, reply } => self.admin(*op, reply),
            Command::AgentJob {
                room,
                dispatch_id,
                job,
            } => self.agent_job(&room, &dispatch_id, *job),
        }
    }

    /// Hands a reconnected client its session back: new signalling, an ICE
    /// restart, and the room's state again (LiveKit's resume).
    /// A peer connection lost its media path, or never found one.
    fn media_lost(&mut self, i: usize, side: Side, short: bool) {
        let p = &mut self.participants[i];
        // As LiveKit: no way to ask a one-shot client to resume.
        if p.one_shot {
            if !p.closing {
                info!(identity = %p.identity, "WHIP media path lost; closing");
                p.closing = true;
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

    fn resume(&mut self, r: Resume) {
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

    fn join(&mut self, join: Join) {
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
                p.sub_offer_id += 1;
                let id = p.sub_offer_id;
                p.send(signal_response::Message::Offer(lk::SessionDescription {
                    r#type: "offer".into(),
                    sdp: fallback::filter_sdp(&offer.to_sdp_string(), p.fallback.pref),
                    id,
                    ..Default::default()
                }));
                p.sub_pending = Some(pending);
                p.sub_pending_since = Some(now);
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

    fn signal(&mut self, i: usize, msg: signal_request::Message) {
        use signal_request::Message as M;
        debug!(identity = %self.participants[i].identity, "signal in: {}", variant_name(&msg));
        match msg {
            M::Offer(sd) => self.publisher_offer(i, sd),
            M::Answer(sd) => self.subscriber_answer(i, sd),
            M::Trickle(t) => self.trickle(i, t),
            M::AddTrack(req) => self.add_track(i, req),
            M::Mute(req) => self.mute(i, req),
            M::Subscription(req) => self.subscription(i, req),
            M::Leave(_) => self.disconnect(i, lk::DisconnectReason::ClientInitiated),
            M::UpdateMetadata(req) => self.update_metadata(i, req),
            // Video layer and adaptive-stream settings come with video.
            M::TrackSetting(req) => self.track_settings(i, req),
            M::PingReq(ping) => {
                self.participants[i].signal_rtt = Some(Duration::from_millis(ping.rtt as u64));
            }
            M::UpdateLayers(_) | M::UpdateVideoTrack(_) => {}
            M::SyncState(state) => self.sync_state(i, state),
            M::SubscriptionPermission(req) => self.subscription_permission(i, req),
            M::UpdateAudioTrack(_) => {}
            M::PublishDataTrackRequest(req) => self.publish_data_track(i, req),
            M::UnpublishDataTrackRequest(req) => self.unpublish_data_track(i, req),
            M::UpdateDataSubscription(req) => self.update_data_subscription(i, req),
            // Data blobs are off, as by default in LiveKit
            // (`enable_participant_data_blob`): nothing is ever stored.
            M::StoreDataBlobRequest(req) => {
                self.participants[i].reply(
                    req.request_id,
                    request_response::Reason::NotAllowed,
                    "data blob not enabled",
                );
            }
            M::GetDataBlobRequest(req) => {
                let (reason, message) = if req.key.is_none() {
                    (
                        request_response::Reason::InvalidRequest,
                        "data blob key is required",
                    )
                } else if !self.rooms[&self.participants[i].room]
                    .members
                    .iter()
                    .any(|&j| self.participants[j].identity == req.participant_identity)
                {
                    (request_response::Reason::NotFound, "participant not found")
                } else {
                    (request_response::Reason::NotFound, "data blob not found")
                };
                self.participants[i].reply(req.request_id, reason, message);
            }
            other => debug!(
                identity = %self.participants[i].identity,
                "unsupported signal request: {}",
                signal_name(&other)
            ),
        }
    }

    /// What a resuming client says it has (LiveKit's `onSyncState`): its
    /// publications must all be known here, or it starts over with a full
    /// reconnect; then its disabled tracks and subscriptions apply.
    fn sync_state(&mut self, i: usize, state: lk::SyncState) {
        let p = &self.participants[i];
        let known = |sid: &str| {
            p.published.iter().any(|t| t.info.sid == sid)
                || p.pending_tracks.iter().any(|(s, _)| s == sid)
        };
        let unknown = state
            .publish_tracks
            .iter()
            .map(|t| t.track.as_ref().map(|ti| ti.sid.as_str()))
            .find(|sid| sid.is_none_or(|s| !known(s)));
        let unknown_data = state
            .publish_data_tracks
            .iter()
            .map(|t| t.info.as_ref().map(|i| i.sid.as_str()))
            .find(|sid| sid.is_none_or(|s| !p.data_tracks.iter().any(|t| t.info.sid == s)))
            .map(|_| "data track");
        if let Some(what) = unknown
            .map(|s| s.unwrap_or("track without info"))
            .or(unknown_data)
        {
            warn!(identity = %p.identity, track = what, "unknown publication on resume; full reconnect");
            let p = &mut self.participants[i];
            p.send(leave(
                lk::DisconnectReason::StateMismatch,
                leave_request::Action::Reconnect,
            ));
            p.closing = true;
            return;
        }
        if !state.track_sids_disabled.is_empty() {
            self.track_settings(
                i,
                lk::UpdateTrackSettings {
                    track_sids: state.track_sids_disabled,
                    disabled: true,
                    ..Default::default()
                },
            );
        }
        if let Some(sub) = state.subscription {
            self.subscription(i, sub);
        }
    }

    fn publisher_offer(&mut self, i: usize, sd: lk::SessionDescription) {
        let p = &mut self.participants[i];
        let offer = match SdpOffer::from_sdp_string(&fallback::filter_sdp(&sd.sdp, p.fallback.pref))
        {
            Ok(o) => o,
            Err(e) => {
                warn!(identity = %p.identity, "bad publisher offer: {e}");
                return;
            }
        };
        p.offered_tracks = if sd.mid_to_track_id.is_empty() {
            msid_tracks(&sd.sdp)
        } else {
            sd.mid_to_track_id.clone()
        };
        match p.publisher.rtc.sdp_api().accept_offer(offer) {
            Ok(answer) => {
                p.publisher.dirty = true;
                // Tracks whose m-line the client no longer sends on: it
                // unpublished them.
                let stopped: Vec<String> = p
                    .published
                    .iter()
                    .filter(|t| t.backup_of.is_none())
                    .filter(|t| {
                        t.mid.is_some_and(|m| {
                            p.publisher.rtc.media(m).is_none_or(|media| {
                                !matches!(
                                    media.direction(),
                                    Direction::RecvOnly | Direction::SendRecv
                                )
                            })
                        })
                    })
                    .map(|t| t.info.sid.clone())
                    .collect();
                for sid in stopped {
                    self.unpublish(i, &sid, false);
                }
                let p = &mut self.participants[i];
                if p.single_pc() {
                    p.slots = recv_slots(&sd.sdp);
                    let pts = negotiated_pts(&p.publisher.rtc);
                    open_negotiated(&self.metrics.lk, p, pts);
                    p.reoffer_requested = false;
                    p.sub_pending_since = None;
                }
                let mut sdp = answer.to_sdp_string();
                // Each video track in the codec its add_track asked for.
                for (mid, cid) in &p.offered_tracks {
                    let asked = p
                        .pending_tracks
                        .iter()
                        .find(|(_, r)| &r.cid == cid && r.r#type() == lk::TrackType::Video)
                        .and_then(|(_, r)| r.simulcast_codecs.first())
                        .map(|c| c.codec.clone())
                        .or_else(|| {
                            p.published
                                .iter()
                                .find(|t| t.kind == MediaKind::Video && t.info.mid == *mid)
                                .map(|t| t.info.mime_type.clone())
                        });
                    if let Some(codec) = asked.filter(|c| !c.is_empty()) {
                        let name = codec.rsplit('/').next().unwrap_or(&codec);
                        sdp = super::sdp::prefer_codec(&sdp, mid, name);
                    }
                }
                if p.single_pc() {
                    let used: HashSet<Mid> = p
                        .downtracks
                        .iter()
                        .filter_map(|d| match d.state {
                            DownState::Open(m) | DownState::ToStop(m) => Some(m),
                            _ => None,
                        })
                        .collect();
                    let free: Vec<String> = p
                        .slots
                        .iter()
                        .filter(|(m, _)| !used.contains(m))
                        .map(|(m, _)| m.to_string())
                        .collect();
                    sdp = super::sdp::inactive_slots(&sdp, &free);
                }
                let sdp = fallback::filter_sdp(&sdp, p.fallback.pref);
                debug!(identity = %p.identity, %sdp, "publisher answer");
                p.send(signal_response::Message::Answer(lk::SessionDescription {
                    r#type: "answer".into(),
                    sdp,
                    id: sd.id,
                    ..Default::default()
                }));
                self.touched.push((i, Side::Publisher));
            }
            Err(e) => warn!(identity = %p.identity, "publisher offer rejected: {e}"),
        }
    }

    fn subscriber_answer(&mut self, i: usize, sd: lk::SessionDescription) {
        let p = &mut self.participants[i];
        let Some(pending) = p.sub_pending.take() else {
            warn!(identity = %p.identity, "subscriber answer with no offer pending");
            return;
        };
        p.sub_pending_since = None;
        let answer =
            match SdpAnswer::from_sdp_string(&fallback::filter_sdp(&sd.sdp, p.fallback.pref)) {
                Ok(a) => a,
                Err(e) => {
                    warn!(identity = %p.identity, "bad subscriber answer: {e}");
                    sub!(p).rtc.disconnect();
                    return;
                }
            };
        if let Err(e) = sub!(p).rtc.sdp_api().accept_answer(pending, answer) {
            warn!(identity = %p.identity, "subscriber answer rejected: {e}");
            sub!(p).rtc.disconnect();
            return;
        }
        sub!(p).dirty = true;
        let pts = negotiated_pts(&sub!(p).rtc);
        open_negotiated(&self.metrics.lk, p, pts);
        self.touched.push((i, Side::Subscriber));
    }

    fn trickle(&mut self, i: usize, t: lk::TrickleRequest) {
        #[derive(serde::Deserialize)]
        struct Init {
            #[serde(default)]
            candidate: String,
        }
        let Ok(init) = serde_json::from_str::<Init>(&t.candidate_init) else {
            return;
        };
        if init.candidate.is_empty() {
            return;
        }
        let Ok(candidate) = Candidate::from_sdp_string(&init.candidate) else {
            debug!("unparsable candidate {}", init.candidate);
            return;
        };
        let side = if t.target == lk::SignalTarget::Subscriber as i32 {
            Side::Subscriber
        } else {
            Side::Publisher
        };
        let p = &mut self.participants[i];
        if p.fallback.pref == Pref::Tcp && !fallback::is_tcp(&init.candidate) {
            return;
        }
        let tr = p.transport(side);
        if candidate.kind() == CandidateKind::Relayed {
            tr.relays.push(candidate.addr());
        }
        tr.rtc.add_remote_candidate(candidate);
        tr.dirty = true;
        self.touched.push((i, side));
    }

    fn add_track(&mut self, i: usize, req: lk::AddTrackRequest) {
        if let Ok(kind @ (lk::TrackType::Audio | lk::TrackType::Video)) =
            lk::TrackType::try_from(req.r#type)
        {
            self.metrics
                .lk
                .publish_counter
                .inc(&[kind.as_str_name(), "attempt"]);
        }
        let p = &mut self.participants[i];
        let source = req.source();
        if !p.grant.can_publish_source(source) {
            p.reply(
                0,
                request_response::Reason::NotAllowed,
                "not allowed to publish this source",
            );
            return;
        }
        let kind = req.r#type();
        if kind == lk::TrackType::Data {
            p.reply(
                0,
                request_response::Reason::UnsupportedType,
                "data tracks are not supported yet",
            );
            return;
        }
        // A backup codec for a published track (the client publishes it
        // when dynacast asks for that codec): answered with the track as it
        // is, and bound to its m-line as that track's backup.
        if let Some(t) = p
            .published
            .iter()
            .find(|t| !req.sid.is_empty() && t.info.sid == req.sid && t.backup_of.is_none())
        {
            let mut req = req;
            if let Some(backup) = t.info.codecs.get(1) {
                req.simulcast_codecs = vec![lk::SimulcastCodec {
                    codec: backup.mime_type.clone(),
                    cid: req.cid.clone(),
                    ..Default::default()
                }];
            }
            p.send(signal_response::Message::TrackPublished(
                lk::TrackPublishedResponse {
                    cid: req.cid.clone(),
                    track: Some(t.info.clone()),
                },
            ));
            p.pending_tracks.push((req.sid.clone(), req));
            return;
        }
        let sid = if req.sid.is_empty() {
            ids::track(kind, source)
        } else {
            req.sid.clone()
        };
        let info = track_info(&sid, &req, None, None, p.obs);
        p.send(signal_response::Message::TrackPublished(
            lk::TrackPublishedResponse {
                cid: req.cid.clone(),
                track: Some(info),
            },
        ));
        p.publish_asked.insert(sid.clone(), Instant::now());
        p.pending_tracks.push((sid, req));
    }

    fn mute(&mut self, i: usize, req: lk::MuteTrackRequest) {
        let p = &mut self.participants[i];
        // The backup codec's stream mutes with its track.
        for b in p
            .published
            .iter_mut()
            .filter(|t| t.backup_of.as_ref() == Some(&req.sid))
        {
            b.info.muted = req.muted;
        }
        let Some(t) = p
            .published
            .iter_mut()
            .find(|t| t.info.sid == req.sid && t.backup_of.is_none())
        else {
            return;
        };
        if t.info.muted == req.muted {
            return;
        }
        t.info.muted = req.muted;
        t.scorer.set_muted(req.muted);
        let (sid, subscribers) = (t.info.sid.clone(), t.subscribers.clone());
        // Unmuted video: subscribers start again at a keyframe, and the
        // publisher hears again which qualities are wanted (LiveKit's
        // ForceUpdate), since it may have restarted every encoding.
        if !req.muted && t.kind == MediaKind::Video {
            let update = video::subscribed_quality(&sid, &dynacast_codecs(t));
            t.dynacast_lower = None;
            p.send(signal_response::Message::SubscribedQualityUpdate(update));
            for conn in subscribers {
                if let Some(&j) = self.by_conn.get(&conn)
                    && let Some(d) = self.participants[j]
                        .downtracks
                        .iter_mut()
                        .find(|d| d.track_sid == sid)
                {
                    d.layer = None;
                    d.stream_state = StreamState::Inactive;
                }
            }
        }
        let p = &mut self.participants[i];
        p.version += 1;
        let room = p.room.clone();
        self.participant_update(&room, i);
    }

    /// Ends a publication: its subscribers lose it and the room hears.
    /// With `tell`, the publisher gets `TrackUnpublished` (an admin
    /// revoked it; a client that unpublished knows).
    fn unpublish(&mut self, i: usize, sid: &str, tell: bool) {
        let p = &mut self.participants[i];
        let mut subscribers = vec![];
        let mut removed = None;
        // The track and its backup codec's stream.
        p.published.retain(|t| {
            let ours = t.info.sid == sid;
            if ours {
                subscribers.extend(t.subscribers.iter().copied());
                if t.backup_of.is_none() {
                    removed = Some(t.info.clone());
                }
            }
            !ours
        });
        let Some(removed) = removed else {
            return;
        };
        let who = p.who();
        info!(identity = %p.identity, track = %sid, tell, "unpublished");
        if tell {
            p.send(signal_response::Message::TrackUnpublished(
                lk::TrackUnpublishedResponse {
                    track_sid: sid.to_owned(),
                },
            ));
        }
        p.version += 1;
        let room = p.room.clone();
        subscribers.sort_unstable();
        subscribers.dedup();
        for conn in subscribers {
            if let Some(&j) = self.by_conn.get(&conn) {
                stop_downtrack(&mut self.participants[j], sid);
                self.touched.push((j, Side::Subscriber));
            }
        }
        let publisher = self.participants[i].conn;
        for &j in &self.rooms[&room].members {
            self.participants[j]
                .refused
                .retain(|(t, c)| !(t == sid && *c == publisher));
        }
        self.participant_update(&room, i);
        self.room_update(&room);
        self.hook(webhook::TRACK_UNPUBLISHED, &room, Some(who), Some(removed));
    }

    /// A subscriber's settings for tracks (adaptive stream: shown or not,
    /// and at what size), kept for tracks it subscribes to later.
    fn track_settings(&mut self, i: usize, req: lk::UpdateTrackSettings) {
        let p = &mut self.participants[i];
        for sid in &req.track_sids {
            p.track_settings.insert(sid.clone(), req.clone());
        }
        let wanted: Vec<(usize, ConnId, String)> = p
            .downtracks
            .iter()
            .enumerate()
            .filter(|(_, d)| d.kind == MediaKind::Video && req.track_sids.contains(&d.track_sid))
            .map(|(k, d)| (k, d.publisher, d.track_sid.clone()))
            .collect();
        for (k, publisher, sid) in wanted {
            let Some(&pi) = self.by_conn.get(&publisher) else {
                continue;
            };
            let Some(t) = self.participants[pi]
                .published
                .iter()
                .find(|t| t.info.sid == sid)
            else {
                continue;
            };
            let max = video::max_layer(&t.info.layers, &req);
            let d = &mut self.participants[i].downtracks[k];
            if d.max_layer != max {
                debug!(track = %sid, ?max, "subscriber settings");
                d.max_layer = max;
            }
        }
    }

    /// Allocates every subscriber not allocated for `allocator::INTERVAL`.
    fn allocate_due(&mut self, now: Instant) {
        for i in 0..self.participants.len() {
            if self.participants[i]
                .cc
                .allocated_at
                .is_none_or(|at| now.duration_since(at) >= allocator::INTERVAL)
            {
                self.allocate(i, now);
            }
        }
    }

    /// Congestion control for one subscriber: gives each video track it
    /// receives the layers its bandwidth estimate allows (`allocator`),
    /// pausing tracks nothing fits for if it allows that (with
    /// `stream_state_update`), and tells the estimator how much it would
    /// like to send, which it probes towards.
    fn allocate(&mut self, i: usize, now: Instant) {
        let p = &self.participants[i];
        let Some(estimate) = p.cc.estimate.filter(|_| p.congestion != Congestion::Off) else {
            return;
        };
        let mut keys = vec![];
        let mut tracks = vec![];
        for (k, d) in p.downtracks.iter().enumerate() {
            if d.kind == MediaKind::Audio
                || !matches!(d.state, DownState::Open(_) | DownState::Negotiating(_))
            {
                continue;
            }
            let Some(max) = d.max_layer else {
                continue;
            };
            let Some(t) = self.by_conn.get(&d.publisher).and_then(|&pi| {
                let published = &self.participants[pi].published;
                source(published, &d.track_sid, d.cannot_primary).map(|k| &published[k])
            }) else {
                continue;
            };
            if t.info.muted {
                continue;
            }
            let mut rates = [None; 3];
            // SVC: a spatial layer costs itself and every layer below.
            if let Some(svc) = &t.svc {
                let mut sum = 0u32;
                for l in &svc.spatial {
                    if !l.live(now) {
                        break;
                    }
                    sum += l.bitrate;
                    rates[usize::from(l.spatial)] = Some(sum.max(1));
                }
            }
            for l in t.layers.iter().filter(|l| l.live(now) && t.svc.is_none()) {
                let s = l.spatial.min(2);
                let announced = t
                    .info
                    .layers
                    .iter()
                    .find(|x| x.spatial_layer == i32::from(s))
                    .map_or(0, |x| x.bitrate);
                rates[usize::from(s)] = Some(allocator::layer_rate(l.bitrate, announced, s));
            }
            if rates.iter().all(Option::is_none) {
                continue;
            }
            keys.push(k);
            tracks.push(allocator::Track {
                screen: t.info.source == lk::TrackSource::ScreenShare as i32,
                max,
                rates,
            });
        }
        let p = &mut self.participants[i];
        let audio_bps = match p.cc.audio_sample.replace((p.cc.audio_bytes_out, now)) {
            Some((bytes, at)) if now > at => {
                let elapsed = now.duration_since(at).as_secs_f64();
                ((p.cc.audio_bytes_out - bytes) as f64 * 8.0 / elapsed) as u64
            }
            _ => 0,
        };
        if tracks.is_empty() {
            p.cc.allocated_at = Some(now);
            return;
        }
        let budget = estimate.saturating_sub(audio_bps);
        // Loss: NACKs from this subscriber on any of its video.
        let mids: Vec<Mid> = keys
            .iter()
            .filter_map(|&k| p.downtracks[k].state.mid())
            .collect();
        let t = sub!(p);
        let mut api = t.rtc.direct_api();
        // The round trip: the least of its streams' latest. A queue at the
        // bottleneck delays them all; one stream's report caught behind a
        // keyframe, or jitter, delays only that one.
        let (mut nacks, mut rtt) = (0u64, None::<Duration>);
        for &mid in &mids {
            if let Some(s) = api.stream_tx_by_mid(mid, None) {
                nacks += s.nacks_received();
                if let Some(r) = s.rtt() {
                    rtt = Some(rtt.map_or(r, |m: Duration| m.min(r)));
                }
            }
        }
        if nacks > p.cc.nacks_seen {
            p.cc.loss_at = Some(now);
        }
        p.cc.nacks_seen = nacks;
        if let Some(r) = rtt {
            p.cc.min_rtt = Some(p.cc.min_rtt.map_or(r, |m| m.min(r)));
        }
        // From TWCC feedback when there is any; else NACKs and the
        // receiver-report round trip.
        let evidence = p.cc.detector.evidence(now);
        let lossy = evidence.loss
            || p.cc
                .loss_at
                .is_some_and(|at| now.duration_since(at) < allocator::LOSS_WINDOW);
        let queueing = if p.cc.twcc_seen {
            evidence.queueing
        } else {
            rtt.zip(p.cc.min_rtt)
                .is_some_and(|(r, m)| r > m + allocator::QUEUEING)
        };
        let congestion = if lossy {
            allocator::Congestion::Loss
        } else if queueing {
            allocator::Congestion::Queueing
        } else {
            allocator::Congestion::None
        };
        // Upgrades wait for the last change to show in receiver reports.
        let hold = allocator::REPORT_INTERVAL + rtt.unwrap_or_default();
        // Congested: what the link delivers is its capacity; send less than
        // that so the queue drains.
        let budget = match evidence.delivered_bps {
            Some(d) if congestion != allocator::Congestion::None => {
                budget.min(((d as f64 * cc_detect::BACKOFF) as u64).saturating_sub(audio_bps))
            }
            _ => budget,
        };
        let alloc = allocator::allocate(budget, &tracks);
        // Upgrades: the peak estimate since the last congestion.
        if congestion != allocator::Congestion::None
            || p.cc.peak.is_none_or(|(b, at)| {
                estimate >= b || now.duration_since(at) > allocator::PEAK_WINDOW
            })
        {
            p.cc.peak = Some((estimate, now));
        }
        let peak = p.cc.peak.map_or(estimate, |(b, _)| b);
        let alloc_up = allocator::allocate(peak.saturating_sub(audio_bps), &tracks);
        // A step down with congestion before an upgrade has held for
        // `BACKOFF_RESET`: that upgrade failed (a small overload can take
        // seconds to fill a queue), so back off. One that has held that
        // long clears the backoff.
        let since_up = p.cc.upgraded_at.map(|at| now.duration_since(at));
        let stepping_down = congestion != allocator::Congestion::None
            && keys.iter().zip(&alloc).any(|(&k, a)| {
                p.downtracks[k]
                    .hold
                    .applied
                    .is_some_and(|applied| *a < applied)
            });
        // Congestion while probing past what is sent: the probe found the
        // link's limit, which counts as a failure too (LiveKit's
        // "congesting" probe).
        let failed_probe = p.cc.probing && congestion != allocator::Congestion::None;
        if failed_probe || stepping_down && since_up.is_some_and(|e| e < allocator::BACKOFF_RESET) {
            p.cc.backoff *= allocator::BACKOFF_FACTOR;
            p.cc.failed_at = Some(now);
            p.cc.upgraded_at = None;
            debug!(identity = %p.identity, backoff = p.cc.backoff, "upgrade backed off");
        } else if since_up.is_some_and(|e| e > allocator::BACKOFF_RESET) {
            p.cc.backoff = 1.0;
        }
        let wait = allocator::upgrade_wait(hold, p.cc.backoff);

        let p = &mut self.participants[i];
        let mut states = vec![];
        let mut may_upgrade = [p.cc.upgraded_at, p.cc.failed_at]
            .iter()
            .flatten()
            .all(|&at| now.duration_since(at) >= wait);
        for (((&k, t), a), a_up) in keys.iter().zip(&tracks).zip(alloc).zip(alloc_up) {
            // Without pausing, a track nothing fits for gets its lowest.
            let lowest = t.rates.iter().position(Option::is_some).map(|s| s as u8);
            let floor = |a: Option<u8>| a.or(if p.allow_pause { None } else { lowest });
            let d = &mut p.downtracks[k];
            let was_paused = d.hold.applied == Some(None);
            let change = d
                .hold
                .update(floor(a), floor(a_up), may_upgrade, congestion, wait, now);
            // The link is dropping packets: stop the old layer now rather
            // than flood it until the lower layer's keyframe; the picture
            // holds until then.
            if change == allocator::Change::Down && congestion == allocator::Congestion::Loss {
                d.layer = None;
            }
            if change == allocator::Change::Up {
                may_upgrade = false;
                p.cc.upgraded_at = Some(now);
            }
            if change != allocator::Change::None {
                let paused = d.hold.applied == Some(None);
                let layer = d.hold.applied.flatten();
                debug!(identity = %p.identity, track = %d.track_sid, ?layer, ?change, estimate, ?congestion, "allocated");
                // ACTIVE comes when video flows again (`forward_video`).
                if paused && !was_paused && d.stream_state != StreamState::Paused {
                    d.stream_state = StreamState::Paused;
                    states.push(lk::StreamStateInfo {
                        participant_sid: d.publisher_sid.clone(),
                        track_sid: d.track_sid.clone(),
                        state: lk::StreamState::Paused as i32,
                    });
                }
            }
        }
        if !states.is_empty() {
            p.send(signal_response::Message::StreamStateUpdate(
                lk::StreamStateUpdate {
                    stream_states: states,
                },
            ));
        }
        let caps = keys
            .iter()
            .map(|&k| p.downtracks[k].hold.applied.unwrap_or(None));
        let mut usage = audio_bps;
        let mut wanted = audio_bps;
        let mut step: Option<u64> = None;
        for (t, cap) in tracks.iter().zip(caps) {
            usage += allocator::usage_bps(t, cap);
            wanted += allocator::usage_bps(t, Some(t.max));
            if let Some(next) = allocator::next_step_bps(t, cap) {
                step = Some(step.map_or(next, |s| s.min(next)));
            }
        }
        let started = *p.cc.video_since.get_or_insert(now);
        // Backed off after a failed upgrade: no probing either until the
        // wait is over (LiveKit's probe regulator spaces the probes; on a
        // nearly full link each probe is a queueing spike).
        let backed_off =
            p.cc.failed_at
                .is_some_and(|at| now.duration_since(at) < wait);
        let goal = if backed_off {
            usage
        } else if now.duration_since(started) < allocator::STARTUP_PROBING {
            wanted.max(allocator::probe_goal(usage, step))
        } else {
            allocator::probe_goal(usage, step)
        };
        p.cc.probing = goal > usage;
        if p.congestion == Congestion::Twcc {
            let side = p.sending_side();
            let t = sub!(p);
            t.rtc.bwe().set_desired_bitrate(Bitrate::bps(goal));
            t.dirty = true;
            self.touched.push((i, side));
        }
        self.participants[i].cc.allocated_at = Some(now);
    }

    /// Dynacast: tells each video publisher the highest quality anyone
    /// receives, so it stops encoding layers nobody watches (all of them
    /// when nobody does). Higher at once; lower once it has held for
    /// `video::DYNACAST_DOWN_DELAY`. Not while the track is muted.
    fn dynacast(&mut self, now: Instant) {
        for i in 0..self.participants.len() {
            for t in 0..self.participants[i].published.len() {
                let track = &self.participants[i].published[t];
                if track.kind != MediaKind::Video
                    || track.mid.is_none()
                    || track.info.muted
                    || track.backup_of.is_some()
                {
                    continue;
                }
                // Per codec: the highest layer anyone taking it wants.
                let mut max: [Option<u8>; 2] = [None, None];
                for conn in &track.subscribers {
                    let Some(&j) = self.by_conn.get(conn) else {
                        continue;
                    };
                    let d = self.participants[j].downtracks.iter().find(|d| {
                        d.track_sid == track.info.sid
                            && !matches!(
                                d.state,
                                DownState::ToStop(_) | DownState::NegotiatingStop(_)
                            )
                    });
                    if let Some(d) = d {
                        let k = usize::from(d.cannot_primary || track.regressed);
                        max[k] = max[k].max(d.max_layer);
                    }
                }
                let want = max.map(|m| m.map(|m| video::quality_of(&track.info.layers, m)));
                let p = &mut self.participants[i];
                let track = &mut p.published[t];
                // No backup codec offered: everyone is on the primary.
                let want = if track.info.codecs.len() > 1 {
                    want
                } else {
                    [want[0].max(want[1]), None]
                };
                let up = want.iter().zip(&track.dynacast).any(|(w, c)| w > c);
                let send = if up {
                    true
                } else if want != track.dynacast {
                    match track.dynacast_lower {
                        Some((w, at)) if w == want => now >= at,
                        _ => {
                            track.dynacast_lower = Some((want, now + video::DYNACAST_DOWN_DELAY));
                            false
                        }
                    }
                } else {
                    track.dynacast_lower = None;
                    false
                };
                if send {
                    debug!(identity = %p.identity, track = %track.info.sid, ?want, "dynacast");
                    track.dynacast = want;
                    track.dynacast_lower = None;
                    let update =
                        video::subscribed_quality(&track.info.sid, &dynacast_codecs(track));
                    p.send(signal_response::Message::SubscribedQualityUpdate(update));
                }
            }
        }
    }

    fn subscription(&mut self, i: usize, req: lk::UpdateSubscription) {
        let mut sids: HashSet<String> = req.track_sids.into_iter().collect();
        for pt in req.participant_tracks {
            sids.extend(pt.track_sids);
        }
        let room = self.participants[i].room.clone();
        let conn = self.participants[i].conn;
        if req.subscribe && !self.participants[i].grant.can_subscribe() {
            return;
        }
        let members = self.rooms[&room].members.clone();
        let prefs = Prefs::of(&self.participants[i]);
        let mut add = vec![];
        let mut remove = vec![];
        let mut refused = vec![];
        for j in members {
            if j == i {
                continue;
            }
            let publisher_sid = self.participants[j].sid.clone();
            let publisher_conn = self.participants[j].conn;
            let (pubr, me) = (&self.participants[j], &self.participants[i]);
            let denied: Vec<String> = pubr
                .published
                .iter()
                .filter(|t| sids.contains(&t.info.sid) && t.mid.is_some() && t.backup_of.is_none())
                .filter(|t| !pubr.permits(&t.info.sid, me))
                .map(|t| t.info.sid.clone())
                .collect();
            for t in self.participants[j].published.iter_mut() {
                if !sids.contains(&t.info.sid) || t.mid.is_none() || t.backup_of.is_some() {
                    continue;
                }
                let has = t.subscribers.contains(&conn);
                if req.subscribe && !has && denied.contains(&t.info.sid) {
                    refused.push((t.info.sid.clone(), publisher_sid.clone(), publisher_conn));
                } else if req.subscribe && !has {
                    t.subscribers.push(conn);
                    add.push(new_downtrack(t, &publisher_sid, publisher_conn, &prefs));
                    self.metrics.lk.subscribe_counter.inc(&["attempt", ""]);
                } else if !req.subscribe && has {
                    t.subscribers.retain(|&c| c != conn);
                    remove.push(t.info.sid.clone());
                }
            }
        }
        let p = &mut self.participants[i];
        p.downtracks.extend(add);
        for sid in remove {
            stop_downtrack(p, &sid);
        }
        if !req.subscribe {
            p.refused.retain(|(t, _)| !sids.contains(t));
        }
        for (sid, publisher_sid, publisher_conn) in refused {
            refuse(p, sid, &publisher_sid, publisher_conn);
        }
        self.touched.push((i, Side::Subscriber));
    }

    /// A publisher's `SubscriptionPermission` (LiveKit's
    /// `UpdateSubscriptionPermission`): every update replaces the last.
    /// Subscribers it now refuses lose those tracks; refused ones it now
    /// allows get them.
    fn subscription_permission(&mut self, i: usize, req: lk::SubscriptionPermission) {
        let room = self.participants[i].room.clone();
        let members = self.rooms[&room].members.clone();
        let perms = if req.all_participants {
            None
        } else {
            let mut m = HashMap::new();
            for tp in req.track_permissions {
                let identity = if tp.participant_identity.is_empty() {
                    if tp.participant_sid.is_empty() {
                        // LiveKit refuses the whole update and keeps the
                        // permissions it had.
                        warn!(identity = %self.participants[i].identity, "subscription permission without identity or sid");
                        return;
                    }
                    let Some(&j) = members
                        .iter()
                        .find(|&&j| self.participants[j].sid == tp.participant_sid)
                    else {
                        debug!(sid = %tp.participant_sid, "subscription permission for unknown participant");
                        continue;
                    };
                    self.participants[j].identity.clone()
                } else {
                    tp.participant_identity.clone()
                };
                m.insert(identity, tp);
            }
            Some(m)
        };
        self.participants[i].sub_permission = perms;

        let publisher_sid = self.participants[i].sid.clone();
        let publisher_conn = self.participants[i].conn;
        for j in members {
            if j == i {
                continue;
            }
            let (pubr, sub) = (&self.participants[i], &self.participants[j]);
            let mut revoke = vec![];
            let mut grant = vec![];
            for t in pubr
                .published
                .iter()
                .filter(|t| t.mid.is_some() && t.backup_of.is_none())
            {
                let allowed = pubr.permits(&t.info.sid, sub);
                if !allowed && t.subscribers.contains(&sub.conn) {
                    revoke.push(t.info.sid.clone());
                } else if allowed
                    && sub
                        .refused
                        .iter()
                        .any(|(s, c)| s == &t.info.sid && *c == pubr.conn)
                {
                    grant.push(t.info.sid.clone());
                }
            }
            if revoke.is_empty() && grant.is_empty() {
                continue;
            }
            let conn = self.participants[j].conn;
            let prefs = Prefs::of(&self.participants[j]);
            let mut add = vec![];
            for t in self.participants[i].published.iter_mut() {
                if t.backup_of.is_some() {
                    continue;
                }
                if revoke.contains(&t.info.sid) {
                    t.subscribers.retain(|&c| c != conn);
                } else if grant.contains(&t.info.sid) {
                    t.subscribers.push(conn);
                    add.push(new_downtrack(t, &publisher_sid, publisher_conn, &prefs));
                    self.metrics.lk.subscribe_counter.inc(&["attempt", ""]);
                }
            }
            let s = &mut self.participants[j];
            info!(identity = %s.identity, publisher = %publisher_sid, revoked = revoke.len(), granted = grant.len(), "subscription permission changed");
            for sid in revoke {
                stop_downtrack(s, &sid);
                refuse(s, sid, &publisher_sid, publisher_conn);
            }
            s.refused.retain(|(t, _)| !grant.contains(t));
            s.downtracks.extend(add);
            self.touched.push((j, Side::Subscriber));
        }
        // Its data tracks too (LiveKit's `maybeRevokeDataTrackSubscriptions`
        // and the reconcile that follows).
        for j in self.rooms[&room].members.clone() {
            if j != i && !self.participants[j].data_wants.is_empty() {
                self.reconcile_data(j);
            }
        }
    }

    fn update_metadata(&mut self, i: usize, req: lk::UpdateParticipantMetadata) {
        let p = &mut self.participants[i];
        if !p.grant.can_update_own_metadata() {
            p.reply(
                req.request_id,
                request_response::Reason::NotAllowed,
                "does not have permission to update own metadata",
            );
            return;
        }
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
        p.version += 1;
        p.reply(req.request_id, request_response::Reason::Ok, "");
        let room = p.room.clone();
        self.participant_update(&room, i);
    }

    // ---- Transport events -----------------------------------------------

    fn event(&mut self, i: usize, side: Side, event: Event) {
        if i >= self.participants.len() {
            return;
        }
        match event {
            Event::RtpPacket(pkt) if side == Side::Publisher => self.forward(i, pkt),
            // A subscriber lost the picture (PLI or FIR): ask the publisher
            // of that track, on the layer it gets, for a keyframe.
            Event::KeyframeRequest(req) => {
                let p = &self.participants[i];
                let Some(d) = p.downtracks.iter().find(|d| d.state.mid() == Some(req.mid)) else {
                    return;
                };
                let Some(layer) = d.layer else {
                    return;
                };
                let (conn, sid) = (d.publisher, d.track_sid.clone());
                let Some(&pi) = self.by_conn.get(&conn) else {
                    return;
                };
                let cannot = self.participants[i]
                    .downtracks
                    .iter()
                    .find(|d| d.track_sid == sid)
                    .is_some_and(|d| d.cannot_primary);
                if let Some(t) = source(&self.participants[pi].published, &sid, cannot) {
                    self.request_keyframe(pi, t, layer, Instant::now());
                }
            }
            Event::Connected => {
                let p = &mut self.participants[i];
                p.transport(side).connected = true;
                if p.one_shot {
                    p.cleanup_at = None;
                }
                info!(identity = %p.identity, ?side, "media connected");
                self.metrics.media_connected_total.fetch_add(1, Relaxed);
                let transport = match side {
                    Side::Publisher => "PUBLISHER",
                    Side::Subscriber => "SUBSCRIBER",
                };
                for state in ["ice_connected", "connected", "fully_established"] {
                    self.metrics.lk.peer_connection.inc(&[transport, state]);
                }
                self.metrics
                    .lk
                    .service_operation
                    .inc(&["peer_connection", "success", ""]);
                // Active once the primary transport is up: the subscriber
                // PC, or the one PC of a single-PC client.
                let primary = side == Side::Subscriber || p.single_pc();
                if primary && p.state != participant_info::State::Active {
                    p.state = participant_info::State::Active;
                    let now = Instant::now();
                    p.active_at = Some(now);
                    let (sdk, protocol) = (sdk_name(&p.client), p.protocol.to_string());
                    let lk = &self.metrics.lk;
                    lk.participant_join.inc(&["rtc_active", "false", ""]);
                    lk.participant_join.inc(&["rtc_success", "false", sdk]);
                    lk.start_time.observe(
                        &[&protocol, "false", sdk],
                        now.duration_since(p.started).as_millis() as f64,
                    );
                    p.version += 1;
                    let room = p.room.clone();
                    self.participant_update(&room, i);
                    let info = self.participants[i].info();
                    self.hook(webhook::PARTICIPANT_JOINED, &room, Some(info), None);
                }
            }
            Event::IceConnectionStateChange(
                IceConnectionState::Connected | IceConnectionState::Completed,
            ) => {
                let t = self.participants[i].transport(side);
                t.connected_at.get_or_insert_with(Instant::now);
            }
            Event::IceConnectionStateChange(IceConnectionState::Disconnected) => {
                let short = self.participants[i]
                    .transport(side)
                    .connected_at
                    .is_some_and(|at| at.elapsed() < fallback::SHORT_CONNECTION);
                self.media_lost(i, side, short);
            }
            Event::MediaAdded(added)
                if side == Side::Publisher
                    && matches!(added.direction, Direction::RecvOnly | Direction::SendRecv) =>
            {
                self.bind_track(i, added.mid, added.kind);
            }
            Event::ChannelOpen(id, label) => {
                let p = &mut self.participants[i];
                // Data reaches it on the subscriber PC, or in single-PC mode
                // on its own channels.
                let primary = (side == Side::Publisher) == p.single_pc();
                let reliable = label == "_reliable";
                match side {
                    Side::Publisher => p.pub_channels.push((id, label)),
                    Side::Subscriber => p.sub_channels.push((id, label)),
                }
                if primary && reliable {
                    self.replay_joining(i);
                }
            }
            Event::ChannelClose(id) => {
                let p = &mut self.participants[i];
                p.pub_channels.retain(|(c, _)| *c != id);
                p.sub_channels.retain(|(c, _)| *c != id);
            }
            Event::ChannelData(data) => {
                let p = &self.participants[i];
                let channels = match side {
                    Side::Publisher => &p.pub_channels,
                    Side::Subscriber => &p.sub_channels,
                };
                let label = channels
                    .iter()
                    .find(|(c, _)| *c == data.id)
                    .map(|(_, l)| l.clone())
                    .unwrap_or_default();
                self.data(i, &label, &data.data);
            }
            Event::EgressBitrateEstimate(kind) => {
                let p = &mut self.participants[i];
                let sending = p.sending_side();
                let bps = match kind {
                    BweKind::Twcc { estimate, .. } if p.congestion == Congestion::Twcc => {
                        estimate.as_u64()
                    }
                    BweKind::Remb { estimate, .. } if p.congestion == Congestion::Remb => {
                        estimate.as_u64()
                    }
                    _ => return,
                };
                if side != sending {
                    return;
                }
                let old = p.cc.estimate.replace(bps);
                // Act at once on a drop or a rise of a tenth; otherwise at
                // the next periodic allocation.
                if old.is_none_or(|o| bps < o || bps > o + o / 10) {
                    self.allocate(i, Instant::now());
                }
            }
            Event::TwccFeedback(packets) => {
                let now = Instant::now();
                let p = &mut self.participants[i];
                p.cc.detector.on_feedback(&packets, now);
                p.cc.twcc_seen = true;
                // Act on congestion as soon as it shows.
                let e = p.cc.detector.evidence(now);
                if e.queueing || e.loss {
                    self.allocate(i, now);
                }
            }
            // Per stream, every STATS_INTERVAL: the quality score, and
            // LiveKit's stream metrics (`telemetry/stats.go`).
            Event::MediaIngressStats(s) if side == Side::Publisher => {
                let p = &mut self.participants[i];
                if let Some(t) = p.published.iter_mut().find(|t| t.mid == Some(s.mid)) {
                    let last = match t.stats_last.iter_mut().find(|(r, _)| *r == s.rid) {
                        Some((_, last)) => last,
                        None => {
                            t.stats_last.push((s.rid, StreamCounts::default()));
                            &mut t.stats_last.last_mut().expect("just pushed").1
                        }
                    };
                    let d = counts_delta(last, [s.packets, s.bytes, 0, 0, s.nacks, s.plis, s.firs]);
                    let window = window(d[0], s.loss);
                    t.scorer.update(&window, Instant::now());
                    let lk = &self.metrics.lk;
                    lk.packets_in.fetch_add(d[0], Relaxed);
                    lk.bytes_in.fetch_add(d[1], Relaxed);
                    lk.stream(
                        "incoming",
                        t.info.source().as_str_name(),
                        type_name(t.kind),
                        (d[4], d[5], d[6]),
                        window.lost,
                        0,
                        window.packets,
                        Some(jitter_us(s.jitter, t.kind)),
                        s.rtt,
                    );
                }
            }
            Event::MediaEgressStats(s) if side == Side::Subscriber => {
                let p = &mut self.participants[i];
                if let Some(d) = p
                    .downtracks
                    .iter_mut()
                    .find(|d| d.state.mid() == Some(s.mid))
                {
                    let c = counts_delta(
                        &mut d.stats_last,
                        [
                            s.packets,
                            s.bytes,
                            s.packets_resent,
                            s.bytes_resent,
                            s.nacks,
                            s.plis,
                            s.firs,
                        ],
                    );
                    let window = window(c[0], s.loss);
                    d.scorer.update(&window, Instant::now());
                    let lk = &self.metrics.lk;
                    lk.packets_out.fetch_add(c[0] - c[2].min(c[0]), Relaxed);
                    lk.bytes_out.fetch_add(c[1] - c[3].min(c[1]), Relaxed);
                    lk.packets_rtx.fetch_add(c[2], Relaxed);
                    lk.bytes_rtx.fetch_add(c[3], Relaxed);
                    lk.stream(
                        "outgoing",
                        d.source.as_str_name(),
                        type_name(d.kind),
                        (c[4], c[5], c[6]),
                        window.lost,
                        0,
                        window.packets,
                        s.remote.as_ref().map(|r| jitter_us(r.jitter, d.kind)),
                        s.rtt,
                    );
                }
            }
            _ => {}
        }
    }

    /// Ties an m-line the publisher opened to the `add_track` it belongs
    /// to, and starts forwarding it.
    fn bind_track(&mut self, i: usize, mid: Mid, kind: MediaKind) {
        let p = &mut self.participants[i];
        if p.published.iter().any(|t| t.mid == Some(mid)) {
            return;
        }
        let cid = p.offered_tracks.get(&mid.to_string()).cloned();
        let want = match kind {
            MediaKind::Audio => lk::TrackType::Audio,
            MediaKind::Video => lk::TrackType::Video,
        };
        let pos = cid
            .as_ref()
            .and_then(|cid| p.pending_tracks.iter().position(|(_, r)| &r.cid == cid))
            .or_else(|| {
                p.pending_tracks
                    .iter()
                    .position(|(_, r)| r.r#type() == want)
            });
        let Some(pos) = pos else {
            debug!(identity = %p.identity, %mid, "m-line with no add_track; ignoring");
            return;
        };
        let (sid, req) = p.pending_tracks.remove(pos);
        let (red_pt, mime) = match kind {
            MediaKind::Audio => {
                let (_, red_pt) = negotiated_pts(&p.publisher.rtc);
                let mime = if red_pt.is_some() {
                    "audio/red"
                } else {
                    "audio/opus"
                };
                (red_pt, mime)
            }
            // The codec add_track asked for (our answer put it first, so
            // that is what the client sends), else the client's first.
            MediaKind::Video => {
                let rtc = &p.publisher.rtc;
                let negotiated: Vec<Codec> = rtc
                    .media(mid)
                    .map(|m| m.remote_pts().to_vec())
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|pt| rtc.codec_config().params().iter().find(|x| x.pt() == pt))
                    .map(|x| x.spec().codec)
                    .collect();
                let asked = req.simulcast_codecs.first().map(|c| c.codec.as_str());
                let asked = asked.and_then(video::mime_for);
                let codec = negotiated
                    .iter()
                    .copied()
                    .find(|&c| asked.is_some() && video::mime(c) == asked)
                    .or(negotiated.first().copied());
                (None, codec.and_then(video::mime).unwrap_or("video/VP8"))
            }
        };
        let mut info = track_info(&sid, &req, Some(&mid.to_string()), Some(mime), p.obs);
        match p.publisher.rtc.media(mid).map(|m| m.rids_rx()) {
            Some(Rids::Specific(rids)) => info.simulcast = rids.len() > 1,
            // VP9/AV1 without rids: SVC, every layer in one stream, as
            // LiveKit marks it (not simulcast; layers without rids).
            _ if matches!(mime, "video/VP9" | "video/AV1") => {
                info.simulcast = false;
                let layers = info
                    .layers
                    .iter_mut()
                    .chain(info.codecs.iter_mut().flat_map(|c| c.layers.iter_mut()));
                for l in layers {
                    l.rid.clear();
                }
            }
            _ => {}
        }
        let fec = true;
        let backup_of = p
            .published
            .iter()
            .any(|t| t.info.sid == sid && t.backup_of.is_none())
            .then(|| sid.clone());
        p.published.push(Published {
            info,
            kind,
            mid: Some(mid),
            layers: vec![],
            pts: vec![],
            bound_at: Instant::now(),
            keyframe_seen: false,
            size_from_media: p.one_shot && kind == MediaKind::Video,
            seeded_at: None,
            red_pt,
            level: AudioLevel::new(audio_level::Config::default(), 48_000),
            to_primary: ToPrimary::default(),
            subscribers: vec![],
            ever_subscribed: false,
            scorer: Scorer::new(quality::loss_weight(mime, fec)),
            stats_last: vec![],
            svc: None,
            backup_of: backup_of.clone(),
            regressed: false,
            // Publishers start sending every layer of the primary codec,
            // and no backup until asked.
            dynacast: [Some(lk::VideoQuality::High), None],
            dynacast_lower: None,
        });
        p.version += 1;
        let t_index = p.published.len() - 1;
        let room = p.room.clone();
        // A backup codec's stream: the track now lists its m-line; it isn't
        // a track of its own, so nobody subscribes to it as one.
        if backup_of.is_some() {
            if let Some(t) = p
                .published
                .iter_mut()
                .find(|t| t.info.sid == sid && t.backup_of.is_none())
                && let Some(c) = t.info.codecs.iter_mut().find(|c| c.mime_type == mime)
            {
                c.mid = mid.to_string();
            }
            info!(identity = %p.identity, track = %sid, %mime, "backup codec published");
            self.participant_update(&room, i);
            return;
        }
        info!(identity = %p.identity, track = %sid, %mime, "published");
        let kind = type_name(p.published[t_index].kind);
        self.metrics.lk.publish_counter.inc(&[kind, "success"]);
        if let Some(at) = p.publish_asked.remove(&sid) {
            self.metrics.lk.pubsub(
                "publish",
                p.published[t_index].info.source().as_str_name(),
                kind,
                at.elapsed(),
                sdk_name(&p.client),
                p.kind.as_str_name(),
            );
        }
        let (who, info) = (p.who(), p.published[t_index].info.clone());
        self.hook(webhook::TRACK_PUBLISHED, &room, Some(who), Some(info));
        self.agents_on_publish(i);

        // Auto-subscribe everyone else who wants it.
        let publisher_sid = self.participants[i].sid.clone();
        let publisher_conn = self.participants[i].conn;
        let members = self.rooms[&room].members.clone();
        for j in members {
            if j == i {
                continue;
            }
            let (auto, can, conn) = {
                let s = &self.participants[j];
                (s.auto_subscribe, s.grant.can_subscribe(), s.conn)
            };
            if !auto || !can {
                continue;
            }
            if !self.participants[i].permits(&sid, &self.participants[j]) {
                refuse(
                    &mut self.participants[j],
                    sid.clone(),
                    &publisher_sid,
                    publisher_conn,
                );
                continue;
            }
            let prefs = Prefs::of(&self.participants[j]);
            let t = &mut self.participants[i].published[t_index];
            t.subscribers.push(conn);
            let d = new_downtrack(t, &publisher_sid, publisher_conn, &prefs);
            self.metrics.lk.subscribe_counter.inc(&["attempt", ""]);
            self.participants[j].downtracks.push(d);
            // Negotiate it now rather than at that transport's next timer.
            self.touched.push((j, Side::Subscriber));
        }
        if let Some(r) = self.rooms.get_mut(&room) {
            r.speakers.remove(&publisher_sid);
        }
        self.participant_update(&room, i);
        self.room_update(&room);
    }

    /// Forwards one packet from a publisher to everyone subscribed.
    fn forward(&mut self, i: usize, pkt: RtpPacket) {
        let now = Instant::now();
        let p = &mut self.participants[i];
        let ssrc = pkt.header.ssrc;
        let (t_index, rid) = match p.ssrc_track.iter().find(|(s, _, _)| *s == ssrc) {
            Some(&(_, t, rid)) => (t, rid),
            None => {
                let Some((mid, rid)) = p
                    .publisher
                    .rtc
                    .direct_api()
                    .stream_rx(&ssrc)
                    .map(|s| (s.mid(), s.rid()))
                else {
                    return;
                };
                let Some(t) = p.published.iter().position(|t| t.mid == Some(mid)) else {
                    return;
                };
                p.ssrc_track.push((ssrc, t, rid));
                fill_layer_ssrcs(p);
                (t, rid)
            }
        };
        self.metrics.packets_received_total.fetch_add(1, Relaxed);
        self.metrics
            .bytes_received_total
            .fetch_add(pkt.payload.len() as u64, Relaxed);
        if p.published[t_index].kind == MediaKind::Video {
            return self.forward_video(i, t_index, rid, pkt, now);
        }
        let track = &mut p.published[t_index];
        if let Some(level) = pkt.header.ext_vals.audio_level {
            self.metrics.audio_levels_total.fetch_add(1, Relaxed);
            track
                .level
                .observe(level.unsigned_abs().min(127), pkt.header.timestamp, now);
        }
        if track.subscribers.is_empty() {
            return;
        }

        let is_red = track.red_pt == Some(pkt.header.payload_type);
        let seq: u64 = *pkt.seq_no;
        let ts = pkt.header.timestamp;
        let abs_capture = pkt.header.ext_vals.abs_capture_time;
        let track_sid = track.info.sid.clone();
        let subscribers = track.subscribers.clone();
        let has_trailer = !track.info.packet_trailer_features.is_empty();

        // RED → Opus once for every subscriber that can't take RED.
        let needs_primary = is_red
            && subscribers.iter().any(|c| {
                self.by_conn.get(c).is_some_and(|&j| {
                    self.participants[j]
                        .downtracks
                        .iter()
                        .any(|d| d.track_sid == track_sid && d.red_pt.is_none())
                })
            });
        let p = &mut self.participants[i];
        let track = &mut p.published[t_index];
        let primaries: Vec<Unpacked> = if needs_primary {
            track.to_primary.unpack(seq as u16, &pkt.payload)
        } else {
            vec![]
        };

        for conn in subscribers {
            let Some(&j) = self.by_conn.get(&conn) else {
                continue;
            };
            let s = &mut self.participants[j];
            let strip_trailer = has_trailer && !s.takes_trailers();
            let Some(d) = s.downtracks.iter_mut().find(|d| d.track_sid == track_sid) else {
                continue;
            };
            let DownState::Open(mid) = d.state else {
                continue;
            };
            let ext = ExtensionValues {
                abs_capture_time: abs_capture,
                ..Default::default()
            };
            let mut written = 0u64;
            let mut bytes = 0u64;
            if is_red && d.red_pt.is_none() {
                let Some(opus) = d.opus_pt else { continue };
                for u in &primaries {
                    let seq = seq.wrapping_sub(u64::from(u.seq_back));
                    let ts = ts.wrapping_sub(u.ts_back);
                    let sent = write(
                        &mut sub!(s).rtc,
                        d,
                        &mut s.mid_sent,
                        mid,
                        opus,
                        seq,
                        ts,
                        &u.payload,
                        false,
                        ext.clone(),
                        now,
                    );
                    if sent.is_some() {
                        written += 1;
                        bytes += u.payload.len() as u64;
                    }
                }
            } else {
                let pt = if is_red { d.red_pt } else { d.opus_pt };
                let Some(pt) = pt else { continue };
                let stripped = strip_trailer
                    .then(|| without_trailer(&pkt.payload, pkt.header.marker))
                    .flatten();
                let payload = stripped.as_ref().unwrap_or(&pkt.payload);
                let sent = write(
                    &mut sub!(s).rtc,
                    d,
                    &mut s.mid_sent,
                    mid,
                    pt,
                    seq,
                    ts,
                    payload,
                    pkt.header.marker,
                    ext,
                    now,
                );
                if sent.is_some() {
                    written += 1;
                    bytes += pkt.payload.len() as u64;
                }
            }
            if written == 0 {
                continue;
            }
            s.cc.audio_bytes_out += bytes + written * allocator::PACKET_OVERHEAD;
            let first = !std::mem::replace(&mut d.told_publisher, true);
            self.forwarded(j, written, bytes, first.then_some(&track_sid));
        }
    }

    /// Counts packets forwarded to subscriber `j`, which then has output
    /// to send; `first` names the track on its first packet.
    #[inline]
    fn forwarded(&mut self, j: usize, packets: u64, bytes: u64, first: Option<&str>) {
        sub!(self.participants[j]).dirty = true;
        self.touched.push((j, Side::Subscriber));
        self.metrics
            .packets_forwarded_total
            .fetch_add(packets, Relaxed);
        self.metrics.bytes_forwarded_total.fetch_add(bytes, Relaxed);
        if let Some(track_sid) = first {
            self.stream_started(j, track_sid);
        }
    }

    #[cold]
    fn stream_started(&mut self, j: usize, track_sid: &str) {
        let s = &self.participants[j];
        let Some(d) = s.downtracks.iter().find(|d| d.track_sid == track_sid) else {
            return;
        };
        self.metrics.lk.pubsub(
            "subscribe-stream-start",
            d.source.as_str_name(),
            type_name(d.kind),
            d.created_at.elapsed(),
            sdk_name(&s.client),
            s.kind.as_str_name(),
        );
        let publisher_conn = d.publisher;
        self.tell_subscribed(j, publisher_conn, track_sid);
    }

    /// The publisher learns its track has been subscribed, once per track
    /// and not by a hidden subscriber (LiveKit's `OnTrackEverSubscribed`).
    fn tell_subscribed(&mut self, j: usize, publisher: ConnId, track_sid: &str) {
        if self.participants[j].hidden() {
            return;
        }
        let Some(&pi) = self.by_conn.get(&publisher) else {
            return;
        };
        let p = &mut self.participants[pi];
        let Some(t) = p
            .published
            .iter_mut()
            .find(|t| t.info.sid == track_sid && t.backup_of.is_none())
        else {
            return;
        };
        if t.ever_subscribed {
            return;
        }
        t.ever_subscribed = true;
        p.send(signal_response::Message::TrackSubscribed(
            lk::TrackSubscribed {
                track_sid: track_sid.to_owned(),
            },
        ));
    }

    /// Forwards one video packet. Each subscriber follows one of the
    /// publisher's layers (`video::target_layer`: the highest live one its
    /// settings allow) and starts, or switches, only where that layer
    /// starts a keyframe, asking for one meanwhile. Nothing is forwarded
    /// while the publisher has the track muted.
    fn forward_video(
        &mut self,
        i: usize,
        t_index: usize,
        rid: Option<Rid>,
        pkt: RtpPacket,
        now: Instant,
    ) {
        let room_size = self
            .rooms
            .get(&self.participants[i].room)
            .map_or(0, |r| r.members.len());
        let p = &mut self.participants[i];
        let pub_pt = pkt.header.payload_type;
        let params = match p.published[t_index]
            .pts
            .iter()
            .find(|(pt, _)| *pt == pub_pt)
        {
            Some(&(_, params)) => params,
            None => {
                let rtc = &p.publisher.rtc;
                let Some(&params) = rtc
                    .codec_config()
                    .params()
                    .iter()
                    .find(|x| x.pt() == pub_pt)
                else {
                    return;
                };
                p.published[t_index].pts.push((pub_pt, params));
                params
            }
        };
        let codec = params.spec().codec;
        let keyframe = video::starts_keyframe(codec, &pkt.payload);
        let track = &mut p.published[t_index];
        let len = pkt.payload.len();
        match track.layers.iter_mut().find(|l| l.rid == rid) {
            Some(l) => l.count(len, now),
            None => {
                let spatial = video::spatial(&track.info.layers, rid);
                let mut layer = Layer::new(rid, spatial, now);
                layer.count(len, now);
                track.layers.push(layer);
            }
        }
        track.keyframe_seen |= keyframe;
        let resized = track.size_from_media
            && keyframe
            && video::frame_size(codec, &pkt.payload).is_some_and(|(w, h)| {
                let spatial = video::spatial(&track.info.layers, rid);
                set_layer_size(&mut track.info, spatial, w, h)
            });
        // SVC: which layers this packet's frame belongs to.
        let mut svc_frame = None;
        if video::is_svc(codec) && rid.is_none() {
            let svc = track.svc.get_or_insert_with(|| Svc {
                structure: None,
                active: 0,
                spatial: [0, 1, 2].map(|s| Layer::new(None, s, now)),
            });
            let dd = pkt
                .header
                .ext_vals
                .user_values
                .get::<video::DependencyDescriptor>();
            if let Some(Ok(f)) = dd.map(|dd| dd::parse(&dd.0, svc.structure.as_ref())) {
                if let Some(s) = &f.structure {
                    svc.structure = Some(s.clone());
                    svc.active = (1u32 << s.dt_cnt().min(31)) - 1;
                }
                if let Some(a) = f.active {
                    svc.active = a;
                }
                svc.spatial[usize::from(f.spatial.min(2))].count(len, now);
                svc_frame = Some(f);
            }
        }
        if resized {
            let room = p.room.clone();
            p.version += 1;
            info!(identity = %p.identity, track = %track.info.sid, "video size from media");
            self.participant_update(&room, i);
        }
        let p = &mut self.participants[i];
        let track = &mut p.published[t_index];
        if track.info.muted {
            return;
        }
        // A backup codec's stream serves its primary's subscribers: those
        // that can't decode the primary, or all once the track regressed.
        // Its first packets regress the track in a small room, unless the
        // publisher asked for simulcast codecs.
        let backup_of = track.backup_of.clone();
        let (subscribers, regressed) = match &backup_of {
            None => (track.subscribers.clone(), track.regressed),
            Some(sid) => {
                let Some(primary) = p
                    .published
                    .iter_mut()
                    .find(|t| &t.info.sid == sid && t.backup_of.is_none())
                else {
                    return;
                };
                let policy = primary.info.backup_codec_policy;
                if !primary.regressed
                    && policy != lk::BackupCodecPolicy::Simulcast as i32
                    && room_size < CODEC_REGRESSION_THRESHOLD
                {
                    info!(track = %sid, "codec regression: everyone on the backup codec");
                    primary.regressed = true;
                }
                (primary.subscribers.clone(), primary.regressed)
            }
        };
        if subscribers.is_empty() {
            return;
        }
        let track = &mut p.published[t_index];
        let has_backup_codec = track.info.codecs.len() > 1;
        let has_trailer = !track.info.packet_trailer_features.is_empty();
        let track_sid = track.info.sid.clone();
        let describe = svc_frame.is_some();
        let svc = svc_frame.zip(
            track
                .svc
                .as_ref()
                .and_then(|s| s.structure.clone().map(|st| (st, s.active))),
        );
        // The layer to follow for each highest layer a subscriber may want.
        let targets: [Option<Option<Rid>>; 4] =
            [0, 1, 2, video::ANY_LAYER].map(|max| video::target_layer(&track.layers, max, now));
        let seq: u64 = *pkt.seq_no;
        let ts = pkt.header.timestamp;
        let mut ext = ExtensionValues {
            abs_capture_time: pkt.header.ext_vals.abs_capture_time,
            ..Default::default()
        };
        // The dependency descriptor only for SVC, where every subscriber
        // takes frames of the one stream it describes. Across simulcast
        // layers (separate encoders, each its own structure and frame
        // numbers) it would be wrong after a switch, and a receiver that
        // missed the structure (Chrome rebuilds its receive stream when
        // an answer relabels the m-line) drops every packet it can't parse.
        // LiveKit sends none for simulcast either.
        if describe
            && let Some(dd) = pkt
                .header
                .ext_vals
                .user_values
                .get::<video::DependencyDescriptor>()
        {
            ext.user_values.set(dd.clone());
        }
        let mut want_keyframe: Vec<Option<Rid>> = vec![];

        for conn in subscribers {
            let Some(&j) = self.by_conn.get(&conn) else {
                continue;
            };
            let s = &mut self.participants[j];
            let strip_trailer = has_trailer && !s.takes_trailers();
            let Some(d) = s.downtracks.iter_mut().find(|d| d.track_sid == track_sid) else {
                continue;
            };
            let DownState::Open(mid) = d.state else {
                continue;
            };
            // Only the stream of the codec it takes; moving to the other
            // starts again at a keyframe.
            if (d.cannot_primary || regressed) != backup_of.is_some() {
                continue;
            }
            if d.on_backup != backup_of.is_some() {
                d.on_backup = backup_of.is_some();
                d.layer = None;
                d.svc = video::SvcDown::default();
            }
            // Disabled, or paused for bandwidth: nothing, and a keyframe
            // to start again. Not allocated yet: the lowest layer while
            // congestion control is on (the top layers of every track at
            // once can overrun the link before the first estimate is used).
            // A client that never yields an estimate (no TWCC) gets what it
            // asks for once it has had time to.
            let estimating = s.congestion != Congestion::Off
                && (s.cc.estimate.is_some()
                    || s.joined_at.elapsed().is_ok_and(|e| e < ESTIMATE_GRACE));
            let unallocated = if estimating { 0 } else { video::ANY_LAYER };
            let (Some(max), Some(cap)) = (d.max_layer, d.hold.applied.unwrap_or(Some(unallocated)))
            else {
                if d.max_layer.is_none() {
                    d.stream_state = StreamState::Inactive;
                }
                d.layer = None;
                d.svc = video::SvcDown::default();
                continue;
            };
            let target = targets[usize::from(max.min(cap).min(3))];
            // Forwarding starts here from nothing (LiveKit's `IsResuming`).
            let mut layer_started = false;
            if d.layer != target {
                if let Some(target) = target
                    && !want_keyframe.contains(&target)
                {
                    want_keyframe.push(target);
                }
                if target == Some(rid) && keyframe {
                    debug!(identity = %s.identity, track = %track_sid, ?rid, "following layer");
                    layer_started = d.layer.is_none();
                    d.layer = Some(rid);
                    // Carry on the outgoing stream from the old layer's
                    // last packet, as if this one came next.
                    if let Some((out_seq, out_ts, at)) = d.last_out {
                        let ticks = now.saturating_duration_since(at).as_micros() * 90 / 1000;
                        d.seq_offset = Some(out_seq.wrapping_add(1).wrapping_sub(seq));
                        d.ts_offset = out_ts.wrapping_add(ticks.max(1) as u32).wrapping_sub(ts);
                    }
                }
            }
            if d.layer != Some(rid) {
                d.svc = video::SvcDown::default();
                continue;
            }
            // SVC: only the frames of its decode target, the marker at the
            // end of its top spatial layer, and the sequence numbers kept
            // contiguous over what is dropped.
            let mut marker = pkt.header.marker;
            if let Some((f, (st, active))) = &svc {
                let limit = (max.min(cap).min(2), 2);
                let before = d.svc.dt;
                let c = video::svc_select(
                    st,
                    *active,
                    f,
                    keyframe,
                    pkt.header.marker,
                    limit,
                    &mut d.svc,
                );
                if d.svc.dt != before {
                    debug!(identity = %s.identity, track = %track_sid, from = ?before.map(|dt| st.dt_layers[dt]), to = ?d.svc.dt.map(|dt| st.dt_layers[dt]), frame = f.number, keyframe, dti = ?d.svc.dt.and_then(|dt| f.dtis.get(dt)), active = format!("{active:b}"), "svc switch");
                }
                if c.want_keyframe && !want_keyframe.contains(&None) {
                    want_keyframe.push(None);
                }
                match c.forward {
                    Some(m) => marker = m,
                    None => {
                        if let Some(o) = d.seq_offset {
                            d.seq_offset = Some(o.wrapping_sub(1));
                        }
                        continue;
                    }
                }
            }
            let sub_pt = match d.video_pts.iter().find(|(pt, _)| *pt == pub_pt) {
                Some(&(_, pt)) => pt,
                None => {
                    let rtc = &sub!(s).rtc;
                    let pt = rtc.media(mid).and_then(|m| {
                        m.remote_pts().iter().copied().find(|&pt| {
                            rtc.codec_config()
                                .params()
                                .iter()
                                .any(|x| x.pt() == pt && video::compatible(&params, x))
                        })
                    });
                    if pt.is_none() {
                        // The primary codec: take the backup instead, which
                        // dynacast now asks the publisher for.
                        if backup_of.is_none() && has_backup_codec {
                            info!(identity = %s.identity, track = %track_sid, ?codec, "subscriber takes the backup codec");
                            d.cannot_primary = true;
                            d.layer = None;
                            continue;
                        }
                        warn!(identity = %s.identity, track = %track_sid, ?codec, "subscriber can't receive this codec");
                    }
                    d.video_pts.push((pub_pt, pt));
                    pt
                }
            };
            let Some(sub_pt) = sub_pt else {
                continue;
            };
            // The trailer ends a frame: the marker, or the end of a layer's
            // frame in SVC (LiveKit's `isEndOfLayerFrame`).
            let end_of_frame = pkt.header.marker || svc.as_ref().is_some_and(|(f, _)| f.end);
            let stripped = strip_trailer
                .then(|| without_trailer(&pkt.payload, end_of_frame))
                .flatten();
            let payload = stripped.as_ref().unwrap_or(&pkt.payload);
            let sent = write(
                &mut sub!(s).rtc,
                d,
                &mut s.mid_sent,
                mid,
                sub_pt,
                seq,
                ts,
                payload,
                marker,
                ext.clone(),
                now,
            );
            let Some((out_seq, out_ts)) = sent else {
                debug!(identity = %s.identity, %mid, "no stream to send video on");
                continue;
            };
            d.last_out = Some((out_seq, out_ts, now));
            let resumed = (layer_started && d.stream_state != StreamState::Active).then(|| {
                d.stream_state = StreamState::Active;
                lk::StreamStateInfo {
                    participant_sid: d.publisher_sid.clone(),
                    track_sid: d.track_sid.clone(),
                    state: lk::StreamState::Active as i32,
                }
            });
            let first = !std::mem::replace(&mut d.told_publisher, true);
            if let Some(info) = resumed {
                s.send(signal_response::Message::StreamStateUpdate(
                    lk::StreamStateUpdate {
                        stream_states: vec![info],
                    },
                ));
            }
            self.forwarded(j, 1, pkt.payload.len() as u64, first.then_some(&track_sid));
        }
        for layer in want_keyframe {
            self.request_keyframe(i, t_index, layer, now);
        }
    }

    /// Asks a publisher for a keyframe (PLI) on one layer of a track, at
    /// most every `video::PLI_THROTTLE`, as LiveKit only ever sends PLI
    /// upstream.
    fn request_keyframe(&mut self, i: usize, t_index: usize, rid: Option<Rid>, now: Instant) {
        let p = &mut self.participants[i];
        let Some(track) = p.published.get_mut(t_index) else {
            return;
        };
        let Some(mid) = track.mid else {
            return;
        };
        if let Some(layer) = track.layers.iter_mut().find(|l| l.rid == rid) {
            if layer
                .last_pli
                .is_some_and(|at| now.duration_since(at) < video::PLI_THROTTLE)
            {
                return;
            }
            layer.last_pli = Some(now);
        }
        let mut api = p.publisher.rtc.direct_api();
        let Some(stream) = api.stream_rx_by_mid(mid, rid) else {
            return;
        };
        stream.request_keyframe(KeyframeRequestKind::Pli);
        p.publisher.dirty = true;
        self.touched.push((i, Side::Publisher));
    }

    /// Until a new video track's first keyframe, asks for one every
    /// `video::SEED_EVERY` for up to `video::SEED_FOR`.
    fn seed_keyframes(&mut self, now: Instant) {
        let mut asks = vec![];
        for (i, p) in self.participants.iter_mut().enumerate() {
            for (t, track) in p.published.iter_mut().enumerate() {
                if track.kind != MediaKind::Video
                    || track.keyframe_seen
                    || now.duration_since(track.bound_at) > video::SEED_FOR
                    || track
                        .seeded_at
                        .is_some_and(|at| now.duration_since(at) < video::SEED_EVERY)
                {
                    continue;
                }
                track.seeded_at = Some(now);
                if track.layers.is_empty() {
                    asks.push((i, t, None));
                }
                asks.extend(track.layers.iter().map(|l| (i, t, l.rid)));
            }
        }
        for (i, t, rid) in asks {
            self.request_keyframe(i, t, rid, now);
        }
    }

    /// Relays a `DataPacket` to the room, stamped with who sent it.
    fn data(&mut self, i: usize, label: &str, bytes: &[u8]) {
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
        let destinations: HashSet<&str> = packet
            .destination_identities
            .iter()
            .map(String::as_str)
            .collect();
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
        let members = self.rooms[&room].members.clone();
        for j in members {
            if j == i {
                continue;
            }
            let o = &self.participants[j];
            if (!destinations.is_empty() || !destination_sids.is_empty())
                && !destinations.contains(o.identity.as_str())
                && !destination_sids.contains(&o.sid)
            {
                continue;
            }
            self.deliver(j, kind, &encoded, origin);
        }
    }

    /// A data packet to participant `j`, on its channel for `kind`. Reliable
    /// data for someone still joining waits (LiveKit's `SendDataMessage`):
    /// from a sender (`origin`: its sid and sequence number) it is replayed
    /// from the room's cache, else held.
    fn deliver(
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
    fn replay_joining(&mut self, j: usize) {
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
    fn deliver_on(&mut self, j: usize, label: &str, encoded: &[u8]) {
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

    /// Sends a participant's new state to everyone in the room, itself
    /// included.
    fn participant_update(&mut self, room: &Arc<str>, i: usize) {
        let p = &self.participants[i];
        let info = p.info();
        let hidden = p.hidden();
        let Some(r) = self.rooms.get(room) else {
            return;
        };
        for &j in &r.members.clone() {
            if hidden && j != i {
                continue;
            }
            self.participants[j].send(signal_response::Message::Update(lk::ParticipantUpdate {
                participants: vec![info.clone()],
            }));
        }
    }

    /// A webhook event about `room` (and a participant or track in it).
    fn hook(
        &self,
        event: &str,
        room: &str,
        participant: Option<lk::ParticipantInfo>,
        track: Option<lk::TrackInfo>,
    ) {
        let Some(w) = &self.webhooks else {
            return;
        };
        w.notify(lk::WebhookEvent {
            event: event.into(),
            room: self.rooms.get(room).map(|r| r.info(&self.participants)),
            participant,
            track,
            ..Default::default()
        });
    }

    /// The room's info to everyone in it, changed or not (metadata).
    fn room_update_forced(&mut self, room: &Arc<str>) {
        let Some(r) = self.rooms.get_mut(room) else {
            return;
        };
        let info = r.info(&self.participants);
        r.last_counts = (
            info.num_participants,
            info.num_publishers,
            info.active_recording,
        );
        for &j in &r.members.clone() {
            self.participants[j].send(signal_response::Message::RoomUpdate(lk::RoomUpdate {
                room: Some(info.clone()),
            }));
        }
    }

    /// The room's new counts to everyone in it, at most every
    /// `ROOM_UPDATE_INTERVAL` (LiveKit's room `ProtoProxy`); a change in
    /// recording goes out at once.
    fn room_update(&mut self, room: &Arc<str>) {
        let now = Instant::now();
        let Some(r) = self.rooms.get_mut(room) else {
            return;
        };
        let info = r.info(&self.participants);
        let counts = (
            info.num_participants,
            info.num_publishers,
            info.active_recording,
        );
        if counts == r.last_counts {
            r.counts_pending = false;
            return;
        }
        let recording_changed = counts.2 != r.last_counts.2;
        if !recording_changed
            && r.counts_sent_at
                .is_some_and(|at| now.duration_since(at) < ROOM_UPDATE_INTERVAL)
        {
            r.counts_pending = true;
            return;
        }
        r.last_counts = counts;
        r.counts_sent_at = Some(now);
        r.counts_pending = false;
        for &j in &r.members.clone() {
            self.participants[j].send(signal_response::Message::RoomUpdate(lk::RoomUpdate {
                room: Some(info.clone()),
            }));
        }
    }

    /// Every update interval, each room's speaker changes go to everyone
    /// subscribed to the speaker, and to the speaker.
    fn speakers(&mut self, now: Instant) {
        let rooms: Vec<Arc<str>> = self.rooms.keys().cloned().collect();
        for name in rooms {
            let members = self.rooms[&name].members.clone();
            let mut levels: Vec<(usize, String, f32)> = vec![];
            for &i in &members {
                let p = &mut self.participants[i];
                if let Some(level) = p.speaker_level(now) {
                    levels.push((i, p.sid.clone(), audio_level::quantize(level)));
                }
            }
            levels.sort_by(|a, b| b.2.total_cmp(&a.2));
            let room = self.rooms.get_mut(&name).expect("listed");
            let mut changes: Vec<(usize, lk::SpeakerInfo)> = vec![];
            for (i, sid, level) in &levels {
                if room.speakers.get(sid) != Some(level) {
                    changes.push((
                        *i,
                        lk::SpeakerInfo {
                            sid: sid.clone(),
                            level: *level,
                            active: true,
                        },
                    ));
                }
            }
            let speaking: HashSet<&String> = levels.iter().map(|(_, s, _)| s).collect();
            for (sid, _) in room.speakers.iter().filter(|(s, _)| !speaking.contains(s)) {
                if let Some(&i) = members.iter().find(|&&i| &self.participants[i].sid == sid) {
                    changes.push((
                        i,
                        lk::SpeakerInfo {
                            sid: sid.clone(),
                            level: 0.0,
                            active: false,
                        },
                    ));
                }
            }
            room.speakers = levels.iter().map(|(_, s, l)| (s.clone(), *l)).collect();
            if changes.is_empty() {
                continue;
            }
            for &j in &members {
                let r = &self.participants[j];
                let speakers: Vec<lk::SpeakerInfo> = changes
                    .iter()
                    .filter(|(i, info)| {
                        *i == j || r.downtracks.iter().any(|d| d.publisher_sid == info.sid)
                    })
                    .map(|(_, info)| info.clone())
                    .collect();
                if !speakers.is_empty() {
                    self.participants[j].send(signal_response::Message::SpeakersChanged(
                        lk::SpeakersChanged { speakers },
                    ));
                }
            }
        }
    }

    /// Each participant hears its own quality and that of those it is
    /// subscribed to, when the level changes.
    fn connection_quality(&mut self) {
        // RTX streams are paired once repairs flow; pick them up here.
        for p in &mut self.participants {
            fill_layer_ssrcs(p);
        }
        // Wanted data tracks not found yet are given up after a while.
        for j in 0..self.participants.len() {
            if !self.participants[j].data_wants.is_empty() {
                self.reconcile_data(j);
            }
        }
        let qualities: Vec<(String, lk::ConnectionQuality, f32)> = self
            .participants
            .iter()
            .map(|p| {
                let (q, mos) = p.quality();
                (p.sid.clone(), q, mos)
            })
            .collect();
        // LiveKit records each participant's own quality as it updates it.
        for (p, (_, q, mos)) in self.participants.iter().zip(&qualities) {
            if p.state == participant_info::State::Active {
                self.metrics
                    .lk
                    .quality_rating
                    .observe(&[], *q as i32 as f64);
                self.metrics.lk.quality_score.observe(&[], f64::from(*mos));
            }
        }
        for j in 0..self.participants.len() {
            let r = &self.participants[j];
            let lost_ok = r.protocol > 10;
            let mut updates = vec![];
            for (k, (sid, q, mos)) in qualities.iter().enumerate() {
                let relevant = k == j || r.downtracks.iter().any(|d| &d.publisher_sid == sid);
                if !relevant || self.participants[k].room != r.room {
                    continue;
                }
                let q = if *q == lk::ConnectionQuality::Lost && !lost_ok {
                    lk::ConnectionQuality::Poor
                } else {
                    *q
                };
                if r.quality_sent.get(sid) != Some(&q) {
                    updates.push(lk::ConnectionQualityInfo {
                        participant_sid: sid.clone(),
                        quality: q as i32,
                        score: *mos,
                    });
                }
            }
            if updates.is_empty() {
                continue;
            }
            let r = &mut self.participants[j];
            for u in &updates {
                r.quality_sent.insert(
                    u.participant_sid.clone(),
                    lk::ConnectionQuality::try_from(u.quality).unwrap_or_default(),
                );
            }
            r.send(signal_response::Message::ConnectionQuality(
                lk::ConnectionQualityUpdate { updates },
            ));
        }
    }

    /// Drops participants who left or broke, tells their rooms, and closes
    /// rooms that stayed empty long enough.
    fn reap(&mut self, now: Instant) {
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

/// Renegotiates the subscriber side when tracks open or close, debounced
/// as LiveKit's transport (`transport.go`): the first offer and ICE
/// restarts go out after 10 ms, later changes are gathered for 150 ms into
/// one offer. Returns when to come back if it is holding one back.
fn negotiate(p: &mut Participant, now: Instant) -> Option<Instant> {
    let has_work = p.restart_ice
        || p.downtracks
            .iter()
            .any(|d| matches!(d.state, DownState::ToOpen | DownState::ToStop(_)));
    if !has_work || p.closing {
        p.negotiate_after = None;
        return None;
    }
    // LiveKit tells a subscriber it may have a track when it subscribes
    // (the first permission check), ahead of the offer.
    let allowed: Vec<(String, String)> = p
        .downtracks
        .iter_mut()
        .filter(|d| d.state == DownState::ToOpen && !d.told_allowed)
        .map(|d| {
            d.told_allowed = true;
            (d.publisher_sid.clone(), d.track_sid.clone())
        })
        .collect();
    for (participant_sid, track_sid) in allowed {
        p.send(signal_response::Message::SubscriptionPermissionUpdate(
            lk::SubscriptionPermissionUpdate {
                participant_sid,
                track_sid,
                allowed: true,
            },
        ));
    }
    let due = match p.negotiate_after {
        Some(due) => due,
        None => {
            let delay = if p.restart_ice || !p.negotiated {
                FAST_NEGOTIATION
            } else {
                NEGOTIATION_DEBOUNCE
            };
            p.negotiate_after = Some(now + delay);
            return Some(now + delay);
        }
    };
    if now < due {
        return None;
    }
    let before = (p.sub_offer_id, p.reoffer_requested);
    if p.single_pc() {
        negotiate_single(p);
    } else {
        negotiate_dual(p);
    }
    // Still waiting on an answer: stays due, so it goes out right after.
    if (p.sub_offer_id, p.reoffer_requested) != before {
        p.negotiate_after = None;
        p.negotiated = true;
    }
    None
}

/// Offers new and stopped m-lines on the subscriber PC.
fn negotiate_dual(p: &mut Participant) {
    if p.sub_pending.is_some() || p.closing {
        return;
    }
    let has_work = p.restart_ice
        || p.downtracks
            .iter()
            .any(|d| matches!(d.state, DownState::ToOpen | DownState::ToStop(_)));
    if !has_work {
        return;
    }
    // str0m checks a new mid only against negotiated ones, so several
    // added at once can collide; rebuild with fresh mids if so.
    let (change, opened) = loop {
        let mut change = sub!(p).rtc.sdp_api();
        if p.restart_ice {
            change.ice_restart(true);
        }
        let mut opened: Vec<(usize, Mid)> = vec![];
        for (k, d) in p.downtracks.iter().enumerate() {
            match d.state {
                DownState::ToOpen => {
                    let stream = format!("{}|{}", d.publisher_sid, d.track_sid);
                    let mid = change.add_media(
                        d.kind,
                        Direction::SendOnly,
                        Some(stream),
                        Some(d.track_sid.clone()),
                        None,
                    );
                    opened.push((k, mid));
                }
                DownState::ToStop(mid) => change.stop_media(mid),
                _ => {}
            }
        }
        let unique: HashSet<Mid> = opened.iter().map(|&(_, m)| m).collect();
        if unique.len() == opened.len() {
            break (change, opened);
        }
    };
    if !change.has_changes() {
        return;
    }
    let Some((offer, pending)) = change.apply() else {
        return;
    };
    let mut mid_to_track_id = HashMap::new();
    for (k, mid) in opened {
        p.downtracks[k].state = DownState::Negotiating(mid);
        mid_to_track_id.insert(mid.to_string(), p.downtracks[k].track_sid.clone());
    }
    for d in &mut p.downtracks {
        if let DownState::ToStop(m) = d.state {
            d.state = DownState::NegotiatingStop(m);
        }
    }
    p.restart_ice = false;
    p.sub_offer_id += 1;
    let id = p.sub_offer_id;
    p.send(signal_response::Message::Offer(lk::SessionDescription {
        r#type: "offer".into(),
        sdp: fallback::filter_sdp(&offer.to_sdp_string(), p.fallback.pref),
        id,
        mid_to_track_id,
    }));
    p.sub_pending = Some(pending);
    p.sub_pending_since = Some(Instant::now());
}

/// Records a layer's picture size; the track's own for the top layer.
/// Whether it changed.
/// Each video layer's SSRC, and its RTX SSRC once str0m has paired one,
/// in the track's info (LiveKit's `SetLayerSsrcsForRid` and
/// `setLayerRtxInfo`; neither is announced). Set once, never overridden.
fn fill_layer_ssrcs(p: &mut Participant) {
    for &(ssrc, t, rid) in &p.ssrc_track {
        let Some(track) = p.published.get_mut(t) else {
            continue;
        };
        if track.kind != MediaKind::Video || track.backup_of.is_some() {
            continue;
        }
        let repair = p
            .publisher
            .rtc
            .direct_api()
            .stream_rx(&ssrc)
            .and_then(|s| s.rtx())
            .map_or(0, |r| *r);
        let spatial = i32::from(video::spatial(&track.info.layers, rid));
        let info = &mut track.info;
        let layers = info.layers.iter_mut().chain(
            info.codecs
                .iter_mut()
                .take(1)
                .flat_map(|c| c.layers.iter_mut()),
        );
        for l in layers.filter(|l| l.spatial_layer == spatial) {
            if l.ssrc == 0 {
                l.ssrc = *ssrc;
            }
            if l.ssrc == *ssrc && l.repair_ssrc == 0 {
                l.repair_ssrc = repair;
            }
        }
    }
}

fn set_layer_size(info: &mut lk::TrackInfo, spatial: u8, width: u32, height: u32) -> bool {
    let mut changed = false;
    let layers = info
        .layers
        .iter_mut()
        .chain(info.codecs.iter_mut().flat_map(|c| c.layers.iter_mut()));
    for l in layers.filter(|l| l.spatial_layer == i32::from(spatial)) {
        if (l.width, l.height) != (width, height) {
            (l.width, l.height) = (width, height);
            changed = true;
        }
    }
    let top = info
        .layers
        .iter()
        .map(|l| l.spatial_layer)
        .max()
        .unwrap_or(0);
    if i32::from(spatial) == top && (info.width, info.height) != (width, height) {
        (info.width, info.height) = (width, height);
        changed = true;
    }
    changed
}

/// Single-PC: the client makes every offer, so forwarded tracks go out on
/// its receive-only m-lines. Assign each new track a free one, label it
/// with the track's msid for the next answer, and ask the client to offer
/// again, with more m-lines if there weren't enough (LiveKit's
/// `media_sections_requirement`).
fn negotiate_single(p: &mut Participant) {
    if p.reoffer_requested || p.closing {
        return;
    }
    let mut changed = false;
    let (mut audios, mut videos) = (0u32, 0u32);
    for k in 0..p.downtracks.len() {
        match p.downtracks[k].state {
            DownState::ToOpen => {
                let kind = p.downtracks[k].kind;
                let used: HashSet<Mid> =
                    p.downtracks.iter().filter_map(|d| d.state.mid()).collect();
                let Some(&(mid, _)) = p
                    .slots
                    .iter()
                    .find(|(m, kd)| *kd == kind && !used.contains(m))
                else {
                    match kind {
                        MediaKind::Audio => audios += 1,
                        MediaKind::Video => videos += 1,
                    }
                    continue;
                };
                let d = &mut p.downtracks[k];
                let stream = format!("{}|{}", d.publisher_sid, d.track_sid);
                p.publisher.rtc.set_msid(mid, stream, d.track_sid.clone());
                d.state = DownState::Negotiating(mid);
                changed = true;
            }
            DownState::ToStop(mid) => {
                // An empty slot again, free for the next track. Its own
                // track id: Chrome refuses an SDP where two m-lines share
                // one msid, so several slots freed at once can't all be
                // `- -`.
                p.publisher.rtc.set_msid(mid, "-".into(), ids::new("-"));
                p.downtracks[k].state = DownState::NegotiatingStop(mid);
                changed = true;
            }
            _ => {}
        }
    }
    if !changed && audios == 0 && videos == 0 {
        return;
    }
    p.send(signal_response::Message::MediaSectionsRequirement(
        lk::MediaSectionsRequirement {
            num_audios: audios,
            num_videos: videos,
        },
    ));
    p.reoffer_requested = true;
    p.sub_pending_since = Some(Instant::now());
}

fn new_downtrack(
    t: &Published,
    publisher_sid: &str,
    publisher: ConnId,
    prefs: &Prefs,
) -> DownTrack {
    let mime = &t.info.mime_type;
    DownTrack {
        track_sid: t.info.sid.clone(),
        publisher_sid: publisher_sid.to_owned(),
        publisher,
        kind: if t.info.r#type == lk::TrackType::Video as i32 {
            MediaKind::Video
        } else {
            MediaKind::Audio
        },
        state: DownState::ToOpen,
        opus_pt: None,
        red_pt: None,
        seq_offset: None,
        ts_offset: 0,
        video_pts: vec![],
        layer: None,
        max_layer: prefs.max_layer(t),
        hold: allocator::Hold::default(),
        stream_state: StreamState::Inactive,
        svc: video::SvcDown::default(),
        cannot_primary: false,
        on_backup: false,
        last_out: None,
        told_publisher: false,
        created_at: Instant::now(),
        told_allowed: false,
        scorer: Scorer::new(quality::loss_weight(mime, true)),
        stats_last: StreamCounts::default(),
        source: t.info.source(),
    }
}

/// The published entry a subscriber of track `sid` takes packets from: the
/// backup codec's stream when it can't decode the primary or the track has
/// regressed (and a backup is being published), else the primary.
fn source(published: &[Published], sid: &str, cannot_primary: bool) -> Option<usize> {
    let primary = published
        .iter()
        .position(|t| t.info.sid == sid && t.backup_of.is_none())?;
    if (cannot_primary || published[primary].regressed)
        && let Some(b) = published
            .iter()
            .position(|t| t.backup_of.as_deref() == Some(sid))
    {
        return Some(b);
    }
    Some(primary)
}

/// A track's codecs with the qualities last committed for each, for a
/// dynacast update: the primary, then its backup if it has one.
fn dynacast_codecs(t: &Published) -> Vec<(&str, Option<lk::VideoQuality>)> {
    let mut codecs = vec![(t.info.mime_type.as_str(), t.dynacast[0])];
    if let Some(backup) = t.info.codecs.get(1) {
        codecs.push((backup.mime_type.as_str(), t.dynacast[1]));
    }
    codecs
}

/// What a subscriber wants of video it is about to receive.
struct Prefs {
    adaptive_stream: bool,
    settings: HashMap<String, lk::UpdateTrackSettings>,
}

impl Prefs {
    fn of(p: &Participant) -> Self {
        Prefs {
            adaptive_stream: p.adaptive_stream,
            settings: p.track_settings.clone(),
        }
    }

    /// Its settings for the track; else, as LiveKit, the lowest layer for
    /// an adaptive-stream client until it reports a size, the highest
    /// otherwise.
    fn max_layer(&self, t: &Published) -> Option<u8> {
        match self.settings.get(&t.info.sid) {
            Some(s) => video::max_layer(&t.info.layers, s),
            None if self.adaptive_stream => Some(0),
            None => Some(video::ANY_LAYER),
        }
    }
}

/// Keeps a subscription its publisher refuses, to start once allowed, and
/// tells the subscriber (LiveKit sends `allowed: false` when a desired
/// subscription's permission changes).
fn refuse(p: &mut Participant, track_sid: String, publisher_sid: &str, publisher: ConnId) {
    if p.refused
        .iter()
        .any(|(t, c)| *t == track_sid && *c == publisher)
    {
        return;
    }
    p.send(signal_response::Message::SubscriptionPermissionUpdate(
        lk::SubscriptionPermissionUpdate {
            participant_sid: publisher_sid.to_owned(),
            track_sid: track_sid.clone(),
            allowed: false,
        },
    ));
    p.refused.push((track_sid, publisher));
}

fn stop_downtrack(p: &mut Participant, track_sid: &str) {
    p.downtracks.retain_mut(|d| {
        if d.track_sid != track_sid {
            return true;
        }
        match d.state {
            DownState::ToOpen => false,
            DownState::Open(m) | DownState::Negotiating(m) => {
                d.state = DownState::ToStop(m);
                true
            }
            _ => true,
        }
    });
}

/// LiveKit's packet trailer (`packettrailer.StripTrailer`): at the end of
/// a frame, `…trailer, len ^ 0xFF, "LKTS"`, where len covers the whole
/// trailer. The payload without it, for subscribers that can't take it
/// (None: there is none to strip).
fn without_trailer(payload: &[u8], end_of_frame: bool) -> Option<Arc<[u8]>> {
    const ENVELOPE: usize = 5;
    if !end_of_frame || payload.len() < ENVELOPE || !payload.ends_with(b"LKTS") {
        return None;
    }
    let len = usize::from(payload[payload.len() - 5] ^ 0xFF);
    if len < ENVELOPE || len > payload.len() {
        return None;
    }
    Some(payload[..payload.len() - len].into())
}

/// RTP timestamp step from the last packet on an m-line to the first of the
/// track that takes it over (chosen: a frame at 30 fps, 90 kHz; 62 ms of
/// 48 kHz audio).
const TS_GAP: u32 = 3000;

/// Writes one packet to a subscriber with that DownTrack's offsets, and
/// returns the sequence number and timestamp it went out with. The first
/// packet picks random starting points, as a fresh RTP stream.
#[allow(clippy::too_many_arguments)]
fn write(
    rtc: &mut Rtc,
    d: &mut DownTrack,
    mid_sent: &mut Vec<(Mid, u64, u32)>,
    mid: Mid,
    pt: Pt,
    seq: u64,
    ts: u32,
    payload: &Arc<[u8]>,
    marker: bool,
    ext: ExtensionValues,
    now: Instant,
) -> Option<(u64, u32)> {
    let slot = mid_sent.iter().position(|(m, _, _)| *m == mid);
    let offset = *d.seq_offset.get_or_insert_with(|| {
        if let Some(k) = slot {
            // Carry on the m-line's stream: next sequence number, and a
            // timestamp a frame or so on (TS_GAP).
            let (_, last_seq, last_ts) = mid_sent[k];
            d.ts_offset = last_ts.wrapping_add(TS_GAP).wrapping_sub(ts);
            return last_seq.wrapping_add(1).wrapping_sub(seq);
        }
        let mut r = [0u8; 6];
        let _ = getrandom::fill(&mut r);
        d.ts_offset = u32::from_le_bytes([r[2], r[3], r[4], r[5]]).wrapping_sub(ts);
        // Start below 2^15 so the first extended number has no rollover.
        let start = u64::from(u16::from_le_bytes([r[0], r[1]]) & 0x7fff) + 1;
        start.wrapping_sub(seq)
    });
    let out_seq = seq.wrapping_add(offset);
    let out_ts = ts.wrapping_add(d.ts_offset);
    match slot {
        Some(k) => {
            // Resent packets go out with older numbers: keep the highest.
            if out_seq > mid_sent[k].1 {
                mid_sent[k].1 = out_seq;
                mid_sent[k].2 = out_ts;
            }
        }
        None => mid_sent.push((mid, out_seq, out_ts)),
    }
    let mut api = rtc.direct_api();
    let stream = api.stream_tx_by_mid(mid, None)?;
    stream.write_rtp(
        RtpWrite::new(pt, SeqNo::from(out_seq), out_ts, now, payload.clone())
            .marker(marker)
            .ext_vals(ext)
            .nackable(true),
    );
    Some((out_seq, out_ts))
}

/// The Opus payload type and RED around it, as negotiated on this PC.
fn negotiated_pts(rtc: &Rtc) -> (Option<Pt>, Option<Pt>) {
    rtc.codec_config()
        .params()
        .iter()
        .find(|p| p.spec().codec == Codec::Opus)
        .map_or((None, None), |p| (Some(p.pt()), p.red()))
}

fn track_info(
    sid: &str,
    req: &lk::AddTrackRequest,
    mid: Option<&str>,
    mime: Option<&str>,
    obs: bool,
) -> lk::TrackInfo {
    use lk::video_layer::Mode;
    let one_per_stream = if obs {
        Mode::OneSpatialLayerPerStreamIncompleteRtcpSr
    } else {
        Mode::OneSpatialLayerPerStream
    };
    // As LiveKit's addTrack: a client that lists no codecs gets one, one
    // layer per stream; listed video codecs get what they asked, else
    // several layers per stream for SVC codecs.
    let layer_mode = |asked: Option<&lk::SimulcastCodec>, mime: Option<&str>| {
        let Some(c) = asked else {
            return one_per_stream as i32;
        };
        if req.r#type != lk::TrackType::Video as i32 || c.video_layer_mode != Mode::Unused as i32 {
            return c.video_layer_mode;
        }
        match mime {
            Some("video/VP9" | "video/AV1") => Mode::MultipleSpatialLayersPerStream as i32,
            _ => one_per_stream as i32,
        }
    };
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64;
    let layers = if req.r#type == lk::TrackType::Video as i32 {
        video::ladder(req)
    } else {
        vec![]
    };
    lk::TrackInfo {
        sid: sid.to_owned(),
        name: req.name.clone(),
        r#type: req.r#type,
        source: req.source,
        muted: req.muted,
        width: req.width,
        height: req.height,
        simulcast: layers.len() > 1,
        disable_dtx: req.disable_dtx,
        stereo: req.stereo,
        disable_red: req.disable_red,
        encryption: req.encryption,
        stream: if req.stream.is_empty() {
            "camera".into()
        } else {
            req.stream.clone()
        },
        mime_type: mime.unwrap_or_default().to_owned(),
        mid: mid.unwrap_or_default().to_owned(),
        // The primary codec, then backups the client offered (LiveKit's
        // simulcast codecs; published only when a subscriber needs them).
        codecs: std::iter::once(lk::SimulcastCodecInfo {
            mime_type: mime.unwrap_or_default().to_owned(),
            mid: mid.unwrap_or_default().to_owned(),
            cid: req.cid.clone(),
            layers: layers.clone(),
            video_layer_mode: layer_mode(req.simulcast_codecs.first(), mime),
            ..Default::default()
        })
        .chain(
            req.simulcast_codecs
                .iter()
                .skip(1)
                .filter_map(|c| Some((c, video::mime_for(&c.codec)?)))
                .filter(|&(_, m)| Some(m) != mime)
                .map(|(c, m)| lk::SimulcastCodecInfo {
                    mime_type: m.to_owned(),
                    layers: layers.clone(),
                    video_layer_mode: layer_mode(Some(c), Some(m)),
                    ..Default::default()
                }),
        )
        .collect(),
        layers,
        version: Some(lk::TimedVersion {
            unix_micro: micros,
            ticks: 0,
        }),
        audio_features: req.audio_features.clone(),
        packet_trailer_features: {
            let mut f = req.packet_trailer_features.clone();
            f.sort_unstable();
            f.dedup();
            f
        },
        // Audio is always simulcast (LiveKit forces it); video as asked,
        // PREFER_REGRESSION by default.
        backup_codec_policy: if req.r#type == lk::TrackType::Video as i32 {
            req.backup_codec_policy
        } else {
            lk::BackupCodecPolicy::Simulcast as i32
        },
    }
}

/// A scoring window from cumulative packet counts and the loss fraction of
/// the latest receiver report.
/// A stream's cumulative counters from str0m's stats: packets, bytes,
/// resent packets and bytes, NACKs, PLIs, FIRs.
type StreamCounts = [u64; 7];

/// What changed since the last stats event, which becomes `last`.
fn counts_delta(last: &mut StreamCounts, now: StreamCounts) -> StreamCounts {
    let d = std::array::from_fn(|k| now[k].saturating_sub(last[k]));
    *last = now;
    d
}

/// A subscription is up: LiveKit's subscribe counter and time.
/// The subscriber side's answer is in: what it negotiated opens, what it
/// stopped goes.
fn open_negotiated(
    lk: &super::prom::LkMetrics,
    p: &mut Participant,
    (opus_pt, red_pt): (Option<Pt>, Option<Pt>),
) {
    let (sdk, pkind) = (sdk_name(&p.client), p.kind.as_str_name());
    for d in &mut p.downtracks {
        if let DownState::Negotiating(m) = d.state {
            d.state = DownState::Open(m);
            subscribed(lk, d, sdk, pkind);
            // str0m keeps RED on the Opus params only if both sides
            // negotiated it.
            d.opus_pt = opus_pt;
            d.red_pt = red_pt.filter(|_| d.kind == MediaKind::Audio);
        }
    }
    p.downtracks
        .retain(|d| !matches!(d.state, DownState::NegotiatingStop(_)));
}

fn subscribed(lk: &super::prom::LkMetrics, d: &DownTrack, sdk: &str, kind: &str) {
    lk.subscribe_counter.inc(&["success", ""]);
    lk.pubsub(
        "subscribe",
        d.source.as_str_name(),
        type_name(d.kind),
        d.created_at.elapsed(),
        sdk,
        kind,
    );
}

/// The client's SDK as LiveKit names it in its metrics (`JS`, `GO`…).
fn sdk_name(client: &lk::ClientInfo) -> &'static str {
    lk::client_info::Sdk::try_from(client.sdk)
        .unwrap_or_default()
        .as_str_name()
}

/// LiveKit's names for a track's type and source, in its metrics.
fn type_name(kind: MediaKind) -> &'static str {
    match kind {
        MediaKind::Audio => "AUDIO",
        MediaKind::Video => "VIDEO",
    }
}

/// Jitter in RTP timestamp units, in microseconds.
fn jitter_us(jitter: u32, kind: MediaKind) -> f64 {
    let rate = match kind {
        MediaKind::Audio => 48_000.0,
        MediaKind::Video => 90_000.0,
    };
    f64::from(jitter) / rate * 1e6
}

fn window(delta: u64, loss: Option<f32>) -> quality::Window {
    let delta = delta as u32;
    let loss = f64::from(loss.unwrap_or(0.0)).clamp(0.0, 0.99);
    let lost = (f64::from(delta) * loss / (1.0 - loss)).round() as u32;
    quality::Window {
        duration: super::rtc_config::STATS_INTERVAL,
        packets: delta,
        lost,
        ..Default::default()
    }
}

/// The m-lines a client offered as receive-only, by mid and kind.
fn recv_slots(sdp: &str) -> Vec<(Mid, MediaKind)> {
    let mut out = vec![];
    let mut kind = None;
    let mut mid = None;
    let mut recvonly = false;
    let mut flush = |kind: Option<MediaKind>, mid: Option<Mid>, recvonly: bool| {
        if let (Some(k), Some(m), true) = (kind, mid, recvonly) {
            out.push((m, k));
        }
    };
    for line in sdp.lines().map(str::trim_end) {
        if let Some(rest) = line.strip_prefix("m=") {
            flush(kind, mid, recvonly);
            kind = match rest.split(' ').next() {
                Some("audio") => Some(MediaKind::Audio),
                Some("video") => Some(MediaKind::Video),
                _ => None,
            };
            mid = None;
            recvonly = false;
        } else if let Some(m) = line.strip_prefix("a=mid:") {
            mid = Some(Mid::from(m));
        } else if line == "a=recvonly" {
            recvonly = true;
        }
    }
    flush(kind, mid, recvonly);
    out
}

/// A Leave asking for `action`; one asking to reconnect says it can.
fn leave(reason: lk::DisconnectReason, action: leave_request::Action) -> signal_response::Message {
    signal_response::Message::Leave(lk::LeaveRequest {
        reason: reason as i32,
        action: action as i32,
        can_reconnect: action == leave_request::Action::Reconnect,
        ..Default::default()
    })
}

fn response(message: signal_response::Message) -> lk::SignalResponse {
    lk::SignalResponse {
        message: Some(message),
    }
}

/// A signal message's variant name, for logs.
fn variant_name(msg: &impl std::fmt::Debug) -> String {
    let debug = format!("{msg:?}");
    debug
        .split(['(', ' '])
        .next()
        .unwrap_or_default()
        .to_owned()
}

fn signal_name(msg: &signal_request::Message) -> &'static str {
    use signal_request::Message as M;
    match msg {
        M::Simulate(_) => "simulate",
        M::Ping(_) | M::PingReq(_) => "ping",
        M::PublishDataTrackRequest(_)
        | M::UnpublishDataTrackRequest(_)
        | M::UpdateDataSubscription(_) => "data_track",
        M::StoreDataBlobRequest(_) | M::GetDataBlobRequest(_) => "data_blob",
        _ => "other",
    }
}
