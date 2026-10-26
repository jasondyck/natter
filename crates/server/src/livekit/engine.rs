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
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{AcqRel, Relaxed, Release};
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use livekit_protocol as lk;
use lk::{leave_request, participant_info, request_response, signal_request, signal_response};
use str0m::change::{SdpOffer, SdpPendingOffer};
use str0m::channel::ChannelId;
use str0m::format::{Codec, PayloadParams};
use str0m::media::{MediaKind, Mid, Pt, Rid};
use str0m::net::{Protocol, Receive};
use str0m::rtp::{ExtensionValues, RtpWrite, SeqNo, Ssrc};
use str0m::{Event, Input, Output, Rtc};
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TrySendError;
use tracing::{debug, warn};

use super::agent::Agents;
use super::allocator;
use super::audio_level::{self, AudioLevel};
use super::cc_detect;
use super::dd;
use super::fallback::{self, Fallback, Path, Pref};
use super::ids;
use super::placement::Placement;
use super::quality::{self, Scorer};
use super::red::ToPrimary;
use super::rtc_config::Congestion;
use super::tcp;
use super::token::VideoGrant;
use super::video::{self, Layer};
use super::webhook::{self, Webhooks};
use crate::config::RoomDefaults;
use crate::metrics::Metrics;
use crate::netio::{RecvBatch, SendBatch};

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

mod admin;
mod agents;
mod allocate;
mod data;
mod data_track;
mod egress;
mod events;
mod forward;
mod negotiate;
mod requests;
mod session;
mod updates;
pub use admin::{AdminError, AdminOp, AdminReply};
use negotiate::negotiate;

/// The LiveKit release whose behaviour natter matches. Clients gate
/// features on the version and protocol they are told.
pub const LIVEKIT_VERSION: &str = "1.13.10";
pub const PROTOCOL: i32 = 17;

/// Rooms smaller than this regress a track to its backup codec once the
/// backup is published, instead of sending both (LiveKit's
/// `video.codec_regression_threshold`).
const CODEC_REGRESSION_THRESHOLD: usize = 5;

/// Longest the loop blocks on the socket, so commands are seen promptly.
const MAX_WAIT: Duration = Duration::from_millis(20);
/// Longest a transport goes unpolled.
const CHECK_INTERVAL: Duration = Duration::from_millis(250);
/// How long one whose WebSocket dropped has to resume.
const RESUME_AFTER_SIGNAL_LOSS: Duration = Duration::from_secs(15);
/// How often rooms check connection quality, as LiveKit.
const QUALITY_INTERVAL: Duration = Duration::from_secs(5);
/// How often a room's participant counts go out at most (LiveKit's
/// `roomUpdateInterval`).
const ROOM_UPDATE_INTERVAL: Duration = Duration::from_secs(5);
/// How long, and how much, reliable data a room keeps for joiners
/// (LiveKit's `dataMessageCacheTTL`, `dataMessageCacheSize` in bytes).
const RELIABLE_CACHE_TTL: Duration = Duration::from_secs(2);
const RELIABLE_CACHE_BYTES: usize = 100_000;

/// A reliable data packet as relayed, by its sender and sequence number.
struct CachedData {
    at: Instant,
    sender: String,
    seq: u32,
    data: Vec<u8>,
    destinations: Vec<String>,
}

pub type ConnId = u64;

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
    /// WHIP and WHEP: no signalling after the answer (LiveKit's one-shot
    /// mode).
    pub one_shot: bool,
    /// WHEP: auto-subscribe only to this identity's tracks.
    pub watch: Option<String>,
    /// When its signalling connection was made (LiveKit's session start).
    pub started: Instant,
}

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
    /// Written packets to send since last polled: str0m's clock is brought
    /// up to now first, which releases them without a full timeout.
    wrote: bool,
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
            wrote: false,
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
    /// Track egress recording it.
    recorders: Vec<egress::Recorder>,
}

struct Svc {
    dd: dd::Stream,
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
    /// Video: the frame rate the subscriber asks for (0: any), served by
    /// the lowest temporal layer that has it (`video::temporal_for_fps`).
    fps: u32,
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
    /// Video VP8: its temporal layer and picture numbering.
    vp8: video::Vp8Down,
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
    /// WHIP and WHEP: nothing can be told to it, so it is closed, not
    /// resumed, and (WHEP) the tracks it receives fill the m-lines of its
    /// one offer, waiting for a free one.
    one_shot: bool,
    /// WHEP: auto-subscribes only to this identity's tracks.
    watch: Option<String>,
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

    /// Takes a name, metadata and attributes update, from the participant
    /// or an admin: empty leaves the name or metadata as they are, and
    /// removes an attribute.
    fn apply_info(
        &mut self,
        name: String,
        metadata: String,
        attributes: impl IntoIterator<Item = (String, String)>,
    ) {
        if !name.is_empty() {
            self.name = name;
        }
        if !metadata.is_empty() {
            self.metadata = metadata;
        }
        for (k, v) in attributes {
            if v.is_empty() {
                self.attributes.remove(&k);
            } else {
                self.attributes.insert(k, v);
            }
        }
    }

    /// Sends the subscriber PC's offer (filtered to the transport it is
    /// held to) and waits for the answer.
    fn send_sub_offer(
        &mut self,
        offer: SdpOffer,
        pending: SdpPendingOffer,
        mid_to_track_id: HashMap<String, String>,
        now: Instant,
    ) {
        self.sub_offer_id += 1;
        self.send(signal_response::Message::Offer(lk::SessionDescription {
            r#type: "offer".into(),
            sdp: fallback::filter_sdp(&offer.to_sdp_string(), self.fallback.pref),
            id: self.sub_offer_id,
            mid_to_track_id,
        }));
        self.sub_pending = Some(pending);
        self.sub_pending_since = Some(now);
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
    /// A track egress is recording one of its tracks.
    recording: bool,
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
            recording: false,
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
            active_recording: self.recording || members().any(Participant::is_recorder),
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
    pub recordings: Arc<crate::egress::Recordings>,
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
    /// Datagrams to send at the end of this pass.
    sends: SendBatch,
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
    /// Someone was marked closing: reap on the next pass rather than at
    /// the next housekeeping (up to 100 ms), so the room hears at once.
    reap_soon: bool,
    /// Admins may unmute (LiveKit `room.enable_remote_unmute`).
    remote_unmute: bool,
    room_defaults: RoomDefaults,
    region: String,
    /// Server-initiated RPCs (RoomService PerformRpc) awaiting replies.
    rpcs: HashMap<String, admin::PendingRpc>,
    webhooks: Option<Webhooks>,
    recordings: Arc<crate::egress::Recordings>,
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
        sends: Default::default(),
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
        reap_soon: false,
        remote_unmute: opts.remote_unmute,
        room_defaults: opts.room_defaults,
        region: opts.region,
        rpcs: HashMap::new(),
        webhooks,
        recordings: opts.recordings,
        agents,
        shutdown: None,
        back,
        wake,
    };
    let mut batch = RecvBatch::default();
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
            engine.reap_soon = false;
            engine.reap(now);
            engine.seed_keyframes(now);
            engine.dynacast(now);
            engine.check_recording();
            engine.allocate_due(now);
        }
        if engine.reap_soon {
            engine.reap_soon = false;
            engine.reap(now);
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
        engine.flush_sends();
        engine.receive(&mut batch);
    }
}

impl Engine {
    /// Sends the datagrams queued this pass.
    fn flush_sends(&mut self) {
        let failed = self.sends.flush(&self.socket);
        self.metrics
            .udp_send_errors_total
            .fetch_add(failed, Relaxed);
    }

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

    /// Reads what has arrived (waiting for the first datagram up to the
    /// socket's read timeout) and hands each datagram to its transport.
    fn receive(&mut self, batch: &mut RecvBatch) {
        let n = match batch.recv(&self.socket) {
            Ok(n) => n,
            Err(e) => {
                if !matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) {
                    warn!("UDP read failed: {e}");
                }
                return;
            }
        };
        let destination = self.local;
        for k in 0..n {
            let Some((data, source)) = batch.get(k) else {
                continue;
            };
            if self.tcp.as_ref().is_some_and(|t| t.wake_from == source) {
                self.receive_tcp();
                continue;
            }
            if source == self.wake.from {
                continue;
            }
            // str0m in RTP mode holds one received packet until polled:
            // poll before the next datagram, which may be for it too.
            if let Some((i, side)) = self.route(data, source, destination, false)
                && k + 1 < n
            {
                self.poll_and_schedule(i, side, Instant::now(), false);
            }
        }
    }

    /// Frames the TCP thread has passed on.
    fn receive_tcp(&mut self) {
        let Some(link) = &self.tcp else {
            return;
        };
        let destination = link.local;
        let frames: Vec<_> = link.drain().collect();
        for (source, frame) in frames {
            // str0m in RTP mode holds one received packet until polled:
            // poll after each frame, as the UDP path does per datagram.
            if let Some((i, side)) = self.route(&frame, source, destination, true) {
                self.poll_and_schedule(i, side, Instant::now(), false);
            }
        }
    }

    /// Hands a datagram (or TCP frame) to the transport it belongs to: by
    /// its source, once known, else whichever accepts it (its STUN
    /// credentials). Returns the transport it went to.
    fn route(
        &mut self,
        data: &[u8],
        source: SocketAddr,
        destination: SocketAddr,
        tcp: bool,
    ) -> Option<(usize, Side)> {
        // DTLS, RTP or RTCP rather than STUN (RFC 7983: STUN starts below
        // 20). The path a connection runs over is where those come from;
        // ICE checks arrive on every candidate pair.
        let media = data.first().is_some_and(|b| *b >= 20);
        let contents = data.try_into().ok()?;
        let proto = if tcp { Protocol::Tcp } else { Protocol::Udp };
        let receive = Receive {
            proto,
            source,
            destination,
            contents,
        };
        let input = Input::Receive(Instant::now(), receive);
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
            if p.one_shot
                && p.downtracks
                    .iter()
                    .any(|d| matches!(d.state, DownState::Negotiating(_)))
            {
                let pts = negotiated_pts(&p.publisher.rtc);
                requests::open_negotiated(&self.metrics.lk, p, pts);
            }
        }
        let p = &mut self.participants[i];
        // Single-PC: the subscriber side is the publisher PC, polled as such.
        let side = if p.single_pc() { Side::Publisher } else { side };
        let t = p.transport(side);
        if t.deadline <= now || t.wrote {
            t.dirty = true;
            t.wrote = false;
            if t.rtc.is_alive() {
                // Mostly due only to release written packets: str0m then
                // skips the rest of its timeout handling.
                let _ = t.rtc.handle_send_timeout(now);
            }
        }
        if !t.dirty {
            return t.deadline;
        }
        t.dirty = false;
        // Fed a timeout, and nothing has come out since.
        let mut fed = false;
        loop {
            if !t.rtc.is_alive() {
                t.deadline = now + CHECK_INTERVAL;
                return t.deadline;
            }
            match t.rtc.poll_output() {
                Ok(Output::Transmit(tx)) if tx.proto == Protocol::Tcp => {
                    fed = false;
                    if let Some(link) = &self.tcp {
                        link.send(tx.destination, &tx.contents);
                    }
                }
                Ok(Output::Transmit(tx)) => {
                    fed = false;
                    if self.sends.push(tx.destination, tx.contents.into()) {
                        let failed = self.sends.flush(&self.socket);
                        self.metrics
                            .udp_send_errors_total
                            .fetch_add(failed, Relaxed);
                    }
                }
                // The pacer releases a written packet only after a timeout:
                // feed it again while that releases something, as packets
                // read in one batch are written at once (waiting for the
                // next pass left them for the socket's read timeout, 1 ms+).
                Ok(Output::Timeout(at)) if !fed && at <= Instant::now() => {
                    fed = true;
                    let _ = t.rtc.handle_send_timeout(Instant::now());
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
        fps: prefs.settings.get(&t.info.sid).map_or(0, |s| s.fps),
        hold: allocator::Hold::default(),
        stream_state: StreamState::Inactive,
        svc: video::SvcDown::default(),
        vp8: video::Vp8Down::default(),
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
