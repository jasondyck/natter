//! How each participant's two str0m peer connections are set up, matching
//! LiveKit's defaults (`pkg/rtc/config.go`, `pkg/rtc/mediaengine.go`,
//! `protocol/codecs`; see `docs/livekit/media.md` A):
//!
//! - Opus (PT 111, `minptime=10;useinbandfec=1`, `usedtx=1` asked of the
//!   publisher as LiveKit munges into its answer) with NACK, and RED (PT 63).
//! - Video at LiveKit's payload types, each with RTX at PT+1: VP8 96, VP9
//!   profile 0 98 and profile 1 100, H.264 42e01f in both packetization
//!   modes (125, 108) and High 640032 (123; not offered on the subscriber
//!   PC, where we are the offerer), AV1 35, H.265 116.
//! - Video feedback per direction: the publisher PC takes TWCC, FIR, NACK
//!   and PLI (plus REMB in single-PC mode, which also carries downstream
//!   media); the subscriber PC takes FIR, NACK, PLI and, by congestion
//!   control mode, TWCC (with the transport-wide sequence extension) or
//!   REMB (LiveKit's default).
//! - With TWCC congestion control, the PC that sends media (subscriber, or
//!   the single PC) runs str0m's bandwidth estimator, which probes.
//! - Publisher extensions: mid, rid, repaired-rid, audio-level,
//!   abs-capture-time, TWCC and Dependency Descriptor (video only;
//!   abs-send-time too in single-PC mode). Subscriber: abs-capture-time,
//!   abs-send-time, Dependency Descriptor.
//! - RTP mode on both: packets are forwarded as they arrive, one shared
//!   payload per packet, with our own sequence/timestamp rewriting.
//! - Resend buffers of 200 audio and 500 video packets, RTCP reports every
//!   second, stats every 5s for connection quality.

use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use str0m::bwe::Bitrate;
use str0m::config::DtlsCert;
use str0m::format::{Codec, CodecConfig, FormatParams, PayloadParams};
use str0m::media::{Frequency, Pt};
use str0m::rtp::Extension;
use str0m::{Candidate, Rtc, RtcConfig};

const AUDIO_PACKET_BUFFER: usize = 200;
const VIDEO_PACKET_BUFFER: usize = 500;
const RTCP_INTERVAL: Duration = Duration::from_secs(1);
pub const STATS_INTERVAL: Duration = Duration::from_secs(5);

/// Which peer connection a configuration is for: video feedback and
/// codecs differ per direction, as LiveKit's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pc {
    /// Dual-PC publisher: the client offers what it sends.
    Publisher,
    /// Single-PC (`/rtc/v1`): one PC, client-offered, both directions.
    Single,
    /// Dual-PC subscriber: we offer what we send.
    Subscriber,
}

fn base(media: MediaConfig, pc: Pc) -> RtcConfig {
    let mut config = Rtc::builder()
        .set_rtp_mode(true)
        .clear_extension_map()
        .set_send_buffer_audio(AUDIO_PACKET_BUFFER)
        .set_send_buffer_video(VIDEO_PACKET_BUFFER)
        .set_rtcp_report_interval_audio(RTCP_INTERVAL)
        .set_rtcp_report_interval_video(RTCP_INTERVAL)
        .set_stats_interval(media.stats.then_some(STATS_INTERVAL))
        .set_dtls_cert(certificate());
    if media.congestion == Congestion::Twcc && pc != Pc::Publisher {
        let initial = Bitrate::bps(super::allocator::INITIAL_ESTIMATE_BPS);
        config = config
            .enable_bwe(Some(initial))
            .enable_twcc_feedback_events(true);
    }
    let mut params = vec![opus_params(media)];
    params.extend(video_params(pc, media.congestion));
    *config.codec_config() = CodecConfig::new_from_payload_params(params);
    config
}

/// The DTLS certificate every peer connection presents, made once per
/// process, as mediasoup and Janus do. A certificate per connection would
/// cost each join two P-256 key generations and signatures; clients only
/// check that the certificate matches the fingerprint in our SDP.
fn certificate() -> DtlsCert {
    static CERT: OnceLock<DtlsCert> = OnceLock::new();
    CERT.get_or_init(|| {
        str0m::crypto::from_feature_flags()
            .dtls_provider
            .generate_certificate()
            .expect("generating the DTLS certificate")
    })
    .clone()
}

/// Receives what the participant publishes. The client offers. In single-PC
/// mode (`single`) it also carries everything the participant receives.
pub fn publisher(
    advertised: SocketAddr,
    tcp: Option<SocketAddr>,
    media: MediaConfig,
    single: bool,
) -> Box<Rtc> {
    let pc = if single { Pc::Single } else { Pc::Publisher };
    let mut config = base(media, pc)
        .set_extension(1, Extension::AudioLevel)
        .set_extension(3, Extension::TransportSequenceNumber)
        .set_extension(4, Extension::RtpMid)
        .set_extension(10, Extension::RtpStreamId)
        .set_extension(11, Extension::RepairedRtpStreamId)
        .set_extension(5, Extension::AbsoluteCaptureTime)
        .set_extension(12, super::video::dependency_descriptor());
    if single {
        config = config.set_extension(2, Extension::AbsoluteSendTime);
    }
    finish(config, advertised, tcp)
}

/// Sends the participant what it subscribes to. The server offers.
pub fn subscriber(advertised: SocketAddr, tcp: Option<SocketAddr>, media: MediaConfig) -> Box<Rtc> {
    let mut config = base(media, Pc::Subscriber)
        .set_extension(2, Extension::AbsoluteCaptureTime)
        .set_extension(3, Extension::AbsoluteSendTime)
        .set_extension(12, super::video::dependency_descriptor());
    if media.congestion == Congestion::Twcc {
        config = config.set_extension(7, Extension::TransportSequenceNumber);
    }
    finish(config, advertised, tcp)
}

/// LiveKit's video codec table, in its order of preference, each with RTX
/// at the next payload type.
pub fn video_params(pc: Pc, congestion: Congestion) -> Vec<PayloadParams> {
    let twcc_down = congestion == Congestion::Twcc;
    let mut codecs = CodecConfig::empty();
    let hz = Frequency::NINETY_KHZ;
    let vp9 = |profile| FormatParams {
        profile_id: Some(profile),
        ..Default::default()
    };
    let pt = Pt::from;
    codecs.add_config(
        pt(96),
        Some(pt(97)),
        Codec::Vp8,
        hz,
        None,
        FormatParams::default(),
    );
    codecs.add_config(pt(98), Some(pt(99)), Codec::Vp9, hz, None, vp9(0));
    codecs.add_config(pt(100), Some(pt(101)), Codec::Vp9, hz, None, vp9(1));
    codecs.add_h264(pt(125), Some(pt(126)), false, 0x42e01f);
    codecs.add_h264(pt(108), Some(pt(109)), true, 0x42e01f);
    if pc != Pc::Subscriber {
        codecs.add_h264(pt(123), Some(pt(124)), true, 0x640032);
    }
    // Baseline (not Constrained Baseline), as ffmpeg's WHIP offers it:
    // LiveKit's pion accepts it against its Constrained Baseline, str0m
    // only with an entry of its own (LiveKit's payload types). Subscribers
    // are offered it too, so it is forwarded unchanged (browsers decode
    // Baseline).
    codecs.add_h264(pt(102), Some(pt(103)), true, 0x42001f);
    codecs.add_h264(pt(104), Some(pt(105)), false, 0x42001f);
    codecs.add_config(
        pt(35),
        Some(pt(36)),
        Codec::Av1,
        hz,
        None,
        FormatParams::default(),
    );
    codecs.add_config(
        pt(116),
        Some(pt(117)),
        Codec::H265,
        hz,
        None,
        FormatParams::default(),
    );
    codecs
        .params()
        .iter()
        .map(|p| {
            let mut p = *p;
            p.set_fb_transport_cc(pc != Pc::Subscriber || twcc_down);
            p.set_fb_fir(true);
            p.set_fb_nack(true);
            p.set_fb_pli(true);
            p.set_fb_remb(match pc {
                Pc::Publisher => false,
                Pc::Single => true,
                Pc::Subscriber => !twcc_down,
            });
            p
        })
        .collect()
}

/// The connection, with a UDP host candidate and, when ICE/TCP is on, a
/// passive TCP one (RFC 6544): clients connect to it when UDP is blocked.
/// ICE prefers UDP, so it is used only then.
fn finish(mut config: RtcConfig, advertised: SocketAddr, tcp: Option<SocketAddr>) -> Box<Rtc> {
    if tcp.is_some() {
        // The engines share one ICE/TCP port, which tells them apart by this.
        config = config.set_ice_ufrag_prefix(super::tcp::ufrag_prefix(advertised.port()));
    }
    let mut rtc = Box::new(config.build(Instant::now()));
    rtc.add_local_candidate(Candidate::host(advertised, "udp").expect("a host candidate"));
    if let Some(tcp) = tcp {
        let passive = Candidate::builder()
            .tcp()
            .host(tcp)
            .tcptype(str0m::net::TcpType::Passive)
            .build()
            .expect("a passive TCP candidate");
        rtc.add_local_candidate(passive);
    }
    rtc
}

/// Optional parts of the media path, each of which costs CPU per packet.
/// The defaults match what LiveKit turns on for audio, so the two can be
/// compared like for like.
#[derive(Debug, Clone, Copy)]
pub struct MediaConfig {
    /// Retransmit lost audio on request (RTCP NACK), both from browsers to
    /// the server and from the server to browsers.
    pub nack: bool,
    /// Offer RFC 2198 redundant audio: each packet also carries the frame
    /// before it, so a single loss is filled without waiting for a resend.
    pub red: bool,
    /// Collect RTP stats and send each room its connection quality.
    pub stats: bool,
    /// How the bandwidth to each subscriber is estimated, which decides
    /// the simulcast layers it gets.
    pub congestion: Congestion,
    /// Whether congestion control may pause a subscriber's video entirely
    /// when even its lowest layer doesn't fit (LiveKit's `allow_pause`,
    /// off by default); a client can ask with `subscriber_allow_pause`.
    pub allow_pause: bool,
}

/// Congestion control for what the server sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Congestion {
    /// Send-side estimation from the client's TWCC feedback (str0m's GCC),
    /// probing with padding for more (LiveKit's `use_send_side_bwe`).
    Twcc,
    /// The client's own estimate (REMB), LiveKit's default; no probing.
    Remb,
    /// No estimate: every subscriber gets what its settings ask for.
    Off,
}

impl std::str::FromStr for Congestion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "twcc" => Ok(Congestion::Twcc),
            "remb" => Ok(Congestion::Remb),
            "off" => Ok(Congestion::Off),
            _ => Err(format!("expected `twcc`, `remb` or `off`, got {s:?}")),
        }
    }
}

impl Default for MediaConfig {
    fn default() -> Self {
        MediaConfig {
            nack: true,
            red: true,
            stats: true,
            congestion: Congestion::Twcc,
            allow_pause: false,
        }
    }
}

/// Opus as str0m's `enable_opus` sets it up, plus DTX: in the answer,
/// `usedtx=1` asks the browser to send only a packet every 400ms or so while
/// its user is silent, instead of 50 a second. Most of a big room is silent
/// at any moment.
pub fn opus_params(media: MediaConfig) -> PayloadParams {
    let mut codecs = CodecConfig::empty();
    // Only `enable_opus` can attach RED: the field is private to str0m.
    codecs.enable_opus(true, media.red);
    let mut opus = codecs.params()[0];
    opus.set_fb_nack(media.nack);
    // Nor can the format be changed once added, except through its serde
    // form. Tests check the answer really says `usedtx=1`.
    let mut value = serde_json::to_value(opus).expect("PayloadParams serialises");
    value["spec"]["format"]["use_dtx"] = true.into();
    serde_json::from_value(value).expect("PayloadParams deserialises")
}

/// Who does the DTLS handshake's crypto, and SRTP's (`NATTER_DTLS_CRYPTO`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DtlsCrypto {
    /// RustCrypto, with P-256 signatures on p256 0.14 (`dtls_crypto`).
    Rust,
    /// aws-lc (C and assembly), with the `aws-lc` cargo feature.
    /// Handshakes cost about a third, which shortens setup when hundreds of
    /// clients join one engine at once; see the README's "Load testing".
    AwsLc,
}

impl std::str::FromStr for DtlsCrypto {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "rust" => Ok(DtlsCrypto::Rust),
            "aws-lc" if cfg!(feature = "aws-lc") => Ok(DtlsCrypto::AwsLc),
            "aws-lc" => Err("this build has no aws-lc (cargo feature `aws-lc`)".into()),
            _ => Err(format!("expected `rust` or `aws-lc`, got {s:?}")),
        }
    }
}

impl std::fmt::Display for DtlsCrypto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DtlsCrypto::Rust => "rust",
            DtlsCrypto::AwsLc => "aws-lc",
        })
    }
}

/// Installs the process-wide crypto providers. Call once before any `Rtc`.
///
/// str0m's `rust-crypto` feature only covers SRTP and certificates (pure
/// Rust in the vendored `str0m-rust-crypto`). Its DTLS library, dimpl, picks
/// its own provider: an installed default, else aws-lc if compiled in. By
/// default the RustCrypto provider keeps the handshake itself in Rust.
pub fn install_crypto(dtls: DtlsCrypto) {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        #[allow(unused_mut)]
        let mut provider = str0m::crypto::from_feature_flags();
        // Per packet, SRTP is the larger cost: aws-lc's AES-GCM is a
        // fraction of RustCrypto's (GHASH alone was ~10% of forwarding).
        #[cfg(feature = "aws-lc")]
        if dtls == DtlsCrypto::AwsLc {
            static SRTP: super::srtp_aws_lc::AwsLcRsSrtpProvider =
                super::srtp_aws_lc::AwsLcRsSrtpProvider;
            provider.srtp_provider = &SRTP;
        }
        provider.install_process_default();
        dimpl::crypto::CryptoProvider::install_default(match dtls {
            DtlsCrypto::Rust => super::dtls_crypto::provider(),
            #[cfg(feature = "aws-lc")]
            DtlsCrypto::AwsLc => dimpl::crypto::aws_lc_rs::default_provider(),
            #[cfg(not(feature = "aws-lc"))]
            DtlsCrypto::AwsLc => unreachable!("rejected when parsed"),
        });
    });
}
