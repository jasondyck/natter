//! Video helpers for forwarding: where a subscriber may start decoding,
//! which of a subscriber's payload types carries a publisher's codec, and
//! which simulcast layer each subscriber follows.
//!
//! Simulcast as LiveKit (`docs/livekit/media.md` C.1, C.2, C.5): a track's
//! layers are its `add_track` layers sorted low to high, spatial layer 0, 1,
//! 2, named `q`, `h`, `f`. Each subscriber has a highest layer it wants (its
//! track settings) and follows the highest live layer up to that, switching
//! only at a keyframe of the new layer. Temporal layers are all forwarded.

use std::sync::Arc;
use std::time::{Duration, Instant};

use livekit_protocol as lk;
use lk::VideoQuality;
use str0m::format::{Codec, PayloadParams};
use str0m::media::Rid;
use str0m::rtp::{Extension, ExtensionSerializer, ExtensionValues};

/// Least time between keyframe requests to one layer, as LiveKit's
/// `pli_throttle` for its low layer and its buffers' default.
pub const PLI_THROTTLE: Duration = Duration::from_millis(500);
/// Until a new track's first keyframe, ask for one this often, for at most
/// `SEED_FOR` (LiveKit's key-frame seeder).
pub const SEED_EVERY: Duration = Duration::from_secs(1);
pub const SEED_FOR: Duration = Duration::from_secs(30);
/// Dynacast tells publishers about a lower quality only after it has held
/// this long (LiveKit's `dynacast_pause_delay`); higher ones at once.
pub const DYNACAST_DOWN_DELAY: Duration = Duration::from_secs(5);

pub fn mime(codec: Codec) -> Option<&'static str> {
    Some(match codec {
        Codec::Vp8 => "video/VP8",
        Codec::Vp9 => "video/VP9",
        Codec::H264 => "video/H264",
        Codec::H265 => "video/H265",
        Codec::Av1 => "video/AV1",
        _ => return None,
    })
}

/// Whether a decoder can start at this packet: the first packet of a
/// keyframe, or for H.264/H.265 of the parameter sets sent just before one.
pub fn starts_keyframe(codec: Codec, payload: &[u8]) -> bool {
    let Some(&b0) = payload.first() else {
        return false;
    };
    match codec {
        Codec::Vp8 => str0m::format::detect_vp8_keyframe(payload),
        // P=0 (independently decodable) and B=1 (start of a frame).
        Codec::Vp9 => b0 & 0x40 == 0 && b0 & 0x08 != 0,
        // N=1: the first packet of a new coded video sequence.
        Codec::Av1 => b0 & 0x08 != 0,
        Codec::H264 => h264_starts(payload),
        Codec::H265 => h265_starts(payload),
        _ => false,
    }
}

/// SPS (7) or IDR (5), alone, aggregated (STAP-A) or as the first fragment
/// (FU-A).
fn h264_starts(p: &[u8]) -> bool {
    let key = |t: u8| t == 5 || t == 7;
    match p[0] & 0x1f {
        t @ 1..=23 => key(t),
        24 => nal_units(&p[1..]).any(|n| key(n[0] & 0x1f)),
        28 => p.get(1).is_some_and(|&h| h & 0x80 != 0 && key(h & 0x1f)),
        _ => false,
    }
}

/// VPS (32), SPS (33) or an IRAP picture (16-21), alone, aggregated (48) or
/// as the first fragment (49).
fn h265_starts(p: &[u8]) -> bool {
    if p.len() < 2 {
        return false;
    }
    let key = |t: u8| (16..=21).contains(&t) || t == 32 || t == 33;
    let kind = |b: u8| (b >> 1) & 0x3f;
    match kind(p[0]) {
        48 => nal_units(&p[2..]).any(|n| key(kind(n[0]))),
        49 => p.get(2).is_some_and(|&h| h & 0x80 != 0 && key(h & 0x3f)),
        t => key(t),
    }
}

/// A keyframe's picture size, from its first packet (VP8: the frame
/// header) or any packet carrying an SPS (H.264). For tracks published
/// without layer sizes (WHIP), as LiveKit's `UpdateTrackInfoByVideoSizeChange`.
pub fn frame_size(codec: Codec, payload: &[u8]) -> Option<(u32, u32)> {
    match codec {
        Codec::Vp8 => vp8_size(payload),
        Codec::H264 => {
            let sps = match payload.first()? & 0x1f {
                7 => Some(payload),
                24 => nal_units(payload.get(1..)?).find(|n| n[0] & 0x1f == 7),
                _ => None,
            }?;
            h264_sps_size(sps)
        }
        _ => None,
    }
}

/// RFC 7741 payload descriptor, then RFC 6386 §9.1: a key frame's 3-byte
/// tag, start code 9d 01 2a, 14-bit width and height.
fn vp8_size(p: &[u8]) -> Option<(u32, u32)> {
    let b0 = *p.first()?;
    // S=1 and partition 0: the frame starts here.
    if b0 & 0x10 == 0 || b0 & 0x07 != 0 {
        return None;
    }
    let at = vp8_header_len(p)?;
    let f = p.get(at..at + 10)?;
    if f[0] & 0x01 != 0 || f[3..6] != [0x9d, 0x01, 0x2a] {
        return None;
    }
    let w = u32::from(u16::from_le_bytes([f[6], f[7]]) & 0x3fff);
    let h = u32::from(u16::from_le_bytes([f[8], f[9]]) & 0x3fff);
    (w > 0 && h > 0).then_some((w, h))
}

/// The length of a VP8 payload descriptor (RFC 7741 §4.2): where the VP8
/// data starts.
pub fn vp8_header_len(p: &[u8]) -> Option<usize> {
    let b0 = *p.first()?;
    let mut at = 1;
    if b0 & 0x80 != 0 {
        let x = *p.get(at)?;
        at += 1;
        if x & 0x80 != 0 {
            // PictureID, 7 or 15 bits.
            at += if p.get(at)? & 0x80 != 0 { 2 } else { 1 };
        }
        if x & 0x40 != 0 {
            at += 1;
        }
        if x & 0x30 != 0 {
            at += 1;
        }
    }
    (at <= p.len()).then_some(at)
}

/// The picture size an H.264 SPS describes (ITU-T H.264 §7.3.2.1.1).
fn h264_sps_size(nal: &[u8]) -> Option<(u32, u32)> {
    // Without emulation prevention bytes (00 00 03).
    let mut rbsp = Vec::with_capacity(nal.len());
    let mut zeros = 0;
    for &b in nal.get(1..)? {
        if zeros >= 2 && b == 3 {
            zeros = 0;
            continue;
        }
        zeros = if b == 0 { zeros + 1 } else { 0 };
        rbsp.push(b);
    }
    let mut r = Bits { data: &rbsp, at: 0 };
    let profile = r.bits(8)?;
    r.bits(16)?; // constraint flags, level
    r.ue()?; // seq_parameter_set_id
    let mut chroma = 1;
    if [100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135].contains(&profile) {
        chroma = r.ue()?;
        if chroma == 3 {
            r.bits(1)?;
        }
        r.ue()?;
        r.ue()?;
        r.bits(1)?;
        if r.bits(1)? == 1 {
            for i in 0..if chroma == 3 { 12 } else { 8 } {
                if r.bits(1)? == 1 {
                    // A scaling list: delta-coded, skipped.
                    let (mut last, mut next) = (8i64, 8i64);
                    for _ in 0..if i < 6 { 16 } else { 64 } {
                        if next != 0 {
                            next = (last + r.se()? + 256) % 256;
                        }
                        if next != 0 {
                            last = next;
                        }
                    }
                }
            }
        }
    }
    r.ue()?; // log2_max_frame_num_minus4
    match r.ue()? {
        0 => {
            r.ue()?;
        }
        1 => {
            r.bits(1)?;
            r.se()?;
            r.se()?;
            for _ in 0..r.ue()? {
                r.se()?;
            }
        }
        _ => {}
    }
    r.ue()?; // max_num_ref_frames
    r.bits(1)?;
    let w_mbs = r.ue()? + 1;
    let h_units = r.ue()? + 1;
    let frame_mbs_only = r.bits(1)?;
    if frame_mbs_only == 0 {
        r.bits(1)?;
    }
    r.bits(1)?;
    let (mut cl, mut cr, mut ct, mut cb) = (0, 0, 0, 0);
    if r.bits(1)? == 1 {
        (cl, cr, ct, cb) = (r.ue()?, r.ue()?, r.ue()?, r.ue()?);
    }
    let (sub_w, sub_h) = match chroma {
        1 => (2, 2),
        2 => (2, 1),
        _ => (1, 1),
    };
    let crop_x = if chroma == 0 { 1 } else { sub_w };
    let crop_y = (2 - frame_mbs_only) * if chroma == 0 { 1 } else { sub_h };
    let w = (w_mbs * 16).checked_sub((cl + cr) * crop_x)?;
    let h = ((2 - frame_mbs_only) * h_units * 16).checked_sub((ct + cb) * crop_y)?;
    (w > 0 && h > 0).then_some((w, h))
}

/// An RBSP bit reader with Exp-Golomb codes.
struct Bits<'a> {
    data: &'a [u8],
    at: usize,
}

impl Bits<'_> {
    fn bits(&mut self, n: u32) -> Option<u32> {
        let mut v = 0;
        for _ in 0..n {
            let byte = *self.data.get(self.at / 8)?;
            v = (v << 1) | u32::from((byte >> (7 - self.at % 8)) & 1);
            self.at += 1;
        }
        Some(v)
    }

    fn ue(&mut self) -> Option<u32> {
        let mut zeros = 0;
        while self.bits(1)? == 0 {
            zeros += 1;
            if zeros > 31 {
                return None;
            }
        }
        Some((1u32 << zeros) - 1 + self.bits(zeros)?)
    }

    fn se(&mut self) -> Option<i64> {
        let k = i64::from(self.ue()?);
        Some(if k % 2 == 1 { (k + 1) / 2 } else { -(k / 2) })
    }
}

/// The NAL units of an aggregation packet's body (16-bit size, unit).
fn nal_units(mut body: &[u8]) -> impl Iterator<Item = &[u8]> {
    std::iter::from_fn(move || {
        let size = usize::from(u16::from_be_bytes([*body.first()?, *body.get(1)?]));
        let unit = body.get(2..2 + size).filter(|u| !u.is_empty())?;
        body = &body[2 + size..];
        Some(unit)
    })
}

/// Whether a subscriber's payload type can carry a publisher's stream
/// unchanged: same codec, and for H.264 the same packetization mode and
/// profile, for VP9 the same profile.
pub fn compatible(publisher: &PayloadParams, subscriber: &PayloadParams) -> bool {
    let (a, b) = (publisher.spec(), subscriber.spec());
    if a.codec != b.codec {
        return false;
    }
    match a.codec {
        Codec::H264 => {
            let profile = |p: Option<u32>| h264_profile(p.unwrap_or(0x42e01f));
            a.format.packetization_mode.unwrap_or(0) == b.format.packetization_mode.unwrap_or(0)
                && profile(a.format.profile_level_id) == profile(b.format.profile_level_id)
        }
        Codec::Vp9 => a.format.profile_id.unwrap_or(0) == b.format.profile_id.unwrap_or(0),
        _ => true,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum H264Profile {
    ConstrainedBaseline,
    Baseline,
    Main,
    ConstrainedHigh,
    High,
    PredictiveHigh444,
    Other(u32),
}

/// The profile a `profile-level-id` names (its first two bytes; the level
/// may differ), as libwebrtc classifies it (`h264_profile_level_id.cc`):
/// several encodings mean Constrained Baseline, for one.
fn h264_profile(profile_level_id: u32) -> H264Profile {
    use H264Profile::*;
    let idc = (profile_level_id >> 16) as u8;
    let iop = (profile_level_id >> 8) as u8;
    let fits = |pattern: &str| {
        pattern.bytes().enumerate().all(|(i, c)| {
            let bit = iop >> (7 - i) & 1;
            c == b'x' || bit == c - b'0'
        })
    };
    match idc {
        0x42 if fits("x1xx0000") => ConstrainedBaseline,
        0x4d if fits("1xxx0000") => ConstrainedBaseline,
        0x58 if fits("11xx0000") => ConstrainedBaseline,
        0x42 if fits("x0xx0000") => Baseline,
        0x58 if fits("10xx0000") => Baseline,
        0x4d if fits("0x0x0000") => Main,
        0x64 if iop == 0 => High,
        0x64 if iop == 0x0c => ConstrainedHigh,
        0xf4 if iop == 0 => PredictiveHigh444,
        _ => Other(profile_level_id >> 8),
    }
}

/// The AV1 RTP spec's Dependency Descriptor header extension. Chrome's AV1
/// (and VP9 SVC) receivers need it to assemble frames, and with it finds a
/// simulcast VP8 stream's references without PictureIDs; natter rewrites it
/// per subscriber (`DdOut`, `dd::rewrite`).
pub const DD_URI: &str =
    "https://aomediacodec.github.io/av1-rtp-spec/#dependency-descriptor-rtp-header-extension";

/// A packet's Dependency Descriptor, as received.
#[derive(Debug, Clone)]
pub struct DependencyDescriptor(pub Arc<[u8]>);

#[derive(Debug)]
struct DdSerializer;

impl ExtensionSerializer for DdSerializer {
    fn write_to(&self, buf: &mut [u8], ev: &ExtensionValues) -> usize {
        let Some(dd) = ev.user_values.get::<DependencyDescriptor>() else {
            return 0;
        };
        let Some(out) = buf.get_mut(..dd.0.len()) else {
            return 0;
        };
        out.copy_from_slice(&dd.0);
        dd.0.len()
    }

    fn parse_value(&self, buf: &[u8], ev: &mut ExtensionValues) -> bool {
        ev.user_values.set(DependencyDescriptor(buf.into()));
        true
    }

    fn is_video(&self) -> bool {
        true
    }

    fn is_audio(&self) -> bool {
        false
    }

    // Descriptors with a template structure run past the one-byte form's
    // 16 bytes.
    fn requires_two_byte_form(&self, ev: &ExtensionValues) -> bool {
        ev.user_values
            .get::<DependencyDescriptor>()
            .is_some_and(|dd| dd.0.len() > 16)
    }
}

pub fn dependency_descriptor() -> Extension {
    Extension::with_serializer(DD_URI, DdSerializer)
}

/// LiveKit's names for simulcast layers, lowest first.
const RIDS: [&str; 3] = ["q", "h", "f"];

/// A subscriber's highest layer when it asks for no limit.
pub const ANY_LAYER: u8 = u8::MAX;

/// Requested sizes and frame rates within this much of a layer's are
/// served by it (LiveKit's `layerSelectionTolerance`).
const LAYER_TOLERANCE: f64 = 0.9;

/// A track's layers as `TrackInfo` carries them: one per quality, lowest
/// first, with spatial index and rid. A track with no layers (a single
/// stream) gets one HIGH layer of the track's size.
pub fn ladder(req: &lk::AddTrackRequest) -> Vec<lk::VideoLayer> {
    let mut layers = req.layers.clone();
    if layers.is_empty() {
        layers.push(lk::VideoLayer {
            quality: VideoQuality::High as i32,
            width: req.width,
            height: req.height,
            ..Default::default()
        });
    }
    layers.retain(|l| l.quality != VideoQuality::Off as i32);
    layers.sort_by_key(|l| l.quality);
    layers.dedup_by_key(|l| l.quality);
    layers.truncate(RIDS.len());
    let simulcast = layers.len() > 1;
    for (i, l) in layers.iter_mut().enumerate() {
        l.spatial_layer = i as i32;
        if simulcast && l.rid.is_empty() {
            l.rid = RIDS[i].into();
        }
    }
    layers
}

/// The spatial layer a publisher's stream carries: by its rid in the
/// ladder, else by LiveKit's names, else 0.
pub fn spatial(ladder: &[lk::VideoLayer], rid: Option<Rid>) -> u8 {
    let Some(rid) = rid else {
        return 0;
    };
    if let Some(l) = ladder.iter().find(|l| l.rid == *rid) {
        return l.spatial_layer as u8;
    }
    RIDS.iter().position(|&r| r == &*rid).unwrap_or(0) as u8
}

/// The quality a subscriber limited to `max` receives at most.
pub fn quality_of(ladder: &[lk::VideoLayer], max: u8) -> VideoQuality {
    ladder
        .iter()
        .rev()
        .find(|l| l.spatial_layer as u8 <= max)
        .or(ladder.first())
        .and_then(|l| VideoQuality::try_from(l.quality).ok())
        .unwrap_or(VideoQuality::High)
}

/// The highest layer of at most `quality`, or the lowest.
pub fn spatial_for_quality(ladder: &[lk::VideoLayer], quality: i32) -> u8 {
    if quality == VideoQuality::High as i32 {
        return ANY_LAYER;
    }
    ladder
        .iter()
        .rev()
        .find(|l| l.quality <= quality)
        .map_or(0, |l| l.spatial_layer as u8)
}

/// The smallest layer at least 90% of the size a subscriber shows the
/// video at, comparing heights (widths for portrait video), as LiveKit;
/// the highest if none is that big.
pub fn spatial_for_size(ladder: &[lk::VideoLayer], width: u32, height: u32) -> u8 {
    let portrait = ladder.last().is_some_and(|l| l.width < l.height);
    let (wanted, size): (u32, fn(&lk::VideoLayer) -> u32) = if portrait {
        (width, |l| l.width)
    } else {
        (height, |l| l.height)
    };
    if ladder.iter().all(|l| size(l) == 0) {
        return ANY_LAYER;
    }
    ladder
        .iter()
        .find(|l| f64::from(size(l)) >= f64::from(wanted) * LAYER_TOLERANCE)
        .map_or(ANY_LAYER, |l| l.spatial_layer as u8)
}

/// What a subscriber's settings for a track allow: None when disabled,
/// else its highest layer.
pub fn max_layer(ladder: &[lk::VideoLayer], s: &lk::UpdateTrackSettings) -> Option<u8> {
    if s.disabled {
        None
    } else if s.width > 0 || s.height > 0 {
        Some(spatial_for_size(ladder, s.width, s.height))
    } else {
        Some(spatial_for_quality(ladder, s.quality))
    }
}

/// A publisher's simulcast layer (or its only stream, `rid` None).
#[derive(Debug, Clone)]
pub struct Layer {
    pub rid: Option<Rid>,
    pub spatial: u8,
    /// Whether the layer is live.
    pub tracker: Tracker,
    pub last_pli: Option<Instant>,
    /// Payload bytes since `window_start`, and the bitrate of the last
    /// whole window (0 until there is one).
    pub bytes: u64,
    pub window_start: Instant,
    pub bitrate: u32,
    /// Frames begun and bytes since `window_start` per temporal layer, and
    /// the frame rate and bitrate of the last whole window up to each
    /// temporal layer (0 for layers not seen).
    frames: [u32; TEMPORAL_LAYERS],
    temporal_bytes: [u64; TEMPORAL_LAYERS],
    pub fps: [f32; TEMPORAL_LAYERS],
    pub temporal_rates: [u32; TEMPORAL_LAYERS],
    /// A simulcast stream's dependency descriptors (its temporal layers).
    pub dd: super::dd::Stream,
}

/// How long a layer's bitrate and frame rates are averaged over.
const RATE_WINDOW: Duration = Duration::from_secs(1);

/// Temporal layers told apart, as LiveKit (`DefaultMaxLayerTemporal` 3).
pub const TEMPORAL_LAYERS: usize = 4;

impl Layer {
    pub fn new(rid: Option<Rid>, spatial: u8, screen: bool, now: Instant) -> Self {
        Layer {
            rid,
            spatial,
            tracker: Tracker::packets(spatial, screen, now),
            last_pli: None,
            bytes: 0,
            window_start: now,
            bitrate: 0,
            frames: [0; TEMPORAL_LAYERS],
            temporal_bytes: [0; TEMPORAL_LAYERS],
            fps: [0.0; TEMPORAL_LAYERS],
            temporal_rates: [0; TEMPORAL_LAYERS],
            dd: Default::default(),
        }
    }

    pub fn live(&self) -> bool {
        self.tracker.live()
    }

    /// Counts a packet of `len` payload bytes, and its headers, of
    /// temporal layer `temporal` (0 when the stream has none), which begins
    /// a frame if `starts_frame`.
    pub fn count(&mut self, len: usize, temporal: u8, starts_frame: bool, now: Instant) {
        let bytes = len as u64 + super::allocator::PACKET_OVERHEAD;
        self.bytes += bytes;
        let t = usize::from(temporal).min(TEMPORAL_LAYERS - 1);
        self.temporal_bytes[t] += bytes;
        if starts_frame {
            self.frames[t] += 1;
        }
        let elapsed = now.duration_since(self.window_start);
        if elapsed >= RATE_WINDOW {
            let ms = elapsed.as_millis().max(1) as u64;
            self.bitrate = (self.bytes * 8 * 1000 / ms) as u32;
            self.bytes = 0;
            let (mut frames, mut bytes) = (0, 0);
            for t in 0..TEMPORAL_LAYERS {
                frames += self.frames[t];
                bytes += self.temporal_bytes[t];
                self.fps[t] = if self.frames[t] == 0 {
                    0.0
                } else {
                    frames as f32 * 1000.0 / ms as f32
                };
                self.temporal_rates[t] = if self.temporal_bytes[t] == 0 {
                    0
                } else {
                    (bytes * 8 * 1000 / ms) as u32
                };
            }
            self.frames = [0; TEMPORAL_LAYERS];
            self.temporal_bytes = [0; TEMPORAL_LAYERS];
            self.window_start = now;
        }
    }
}

/// Whether a publisher's layer is live, as LiveKit's stream trackers. A
/// simulcast layer (or a single stream) by its packets (LiveKit's default
/// packet tracker): live from its first packet, stopped after a cycle with
/// too few, and live again only after `cycles` cycles in a row with enough
/// (2 s for the lowest layer, 10 s for the others; 2 s for a screen
/// share), so a layer the publisher's encoder keeps dropping and restarting
/// isn't switched to each time. A reset (unmute, or dynacast asking for the
/// layer again) makes its next packet live at once. An SVC spatial layer is
/// live while the decode targets the publisher has active include it
/// (LiveKit's dependency descriptor tracker). Muted, nothing is live.
#[derive(Debug, Clone, Copy)]
pub struct Tracker {
    rule: TrackerRule,
    paused: bool,
    active: bool,
    /// Packet tracker: seen a packet since the last reset; packets this
    /// cycle; cycles in a row with enough; when this cycle began.
    initialized: bool,
    count: u32,
    good: u32,
    cycle_start: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrackerRule {
    Packets {
        samples: u32,
        cycles: u32,
        cycle: Duration,
    },
    Decode,
}

impl Tracker {
    /// LiveKit's `DefaultStreamTrackerPacketConfigVideo` and
    /// `...Screenshare`.
    pub fn packets(spatial: u8, screen: bool, now: Instant) -> Self {
        let rule = match (screen, spatial) {
            (true, _) => TrackerRule::Packets {
                samples: 1,
                cycles: 1,
                cycle: Duration::from_secs(2),
            },
            (false, 0) => TrackerRule::Packets {
                samples: 1,
                cycles: 4,
                cycle: Duration::from_millis(500),
            },
            (false, _) => TrackerRule::Packets {
                samples: 5,
                cycles: 20,
                cycle: Duration::from_millis(500),
            },
        };
        Tracker {
            rule,
            paused: false,
            active: false,
            initialized: false,
            count: 0,
            good: 0,
            cycle_start: now,
        }
    }

    /// An SVC spatial layer's, set by `set_decodable`.
    pub fn decode_targets(now: Instant) -> Self {
        Tracker {
            rule: TrackerRule::Decode,
            ..Tracker::packets(0, false, now)
        }
    }

    pub fn live(&self) -> bool {
        self.active && !self.paused
    }

    /// A packet of the layer, its track muted or not.
    pub fn observe(&mut self, muted: bool, now: Instant) {
        self.tick(muted, now);
        if self.paused || self.rule == TrackerRule::Decode {
            return;
        }
        if !self.initialized {
            self.initialized = true;
            self.active = true;
            self.count = 1;
            self.good = 0;
            self.cycle_start = now;
            return;
        }
        self.count += 1;
    }

    /// SVC: whether the publisher's active decode targets include it.
    pub fn set_decodable(&mut self, decodable: bool) {
        if self.rule == TrackerRule::Decode {
            self.active = decodable;
        }
    }

    /// Starts over: stopped until its next packet, which makes it live.
    pub fn reset(&mut self, now: Instant) {
        if self.rule == TrackerRule::Decode {
            return;
        }
        self.active = false;
        self.initialized = false;
        self.count = 0;
        self.good = 0;
        self.cycle_start = now;
    }

    /// Ends the cycles that are over (at least every `cycle`; the engine
    /// calls it with its housekeeping), and follows the track's mute:
    /// unmuted, it starts over.
    pub fn tick(&mut self, muted: bool, now: Instant) {
        if muted != self.paused {
            self.paused = muted;
            if !muted {
                self.reset(now);
            }
        }
        let TrackerRule::Packets {
            samples,
            cycles,
            cycle,
        } = self.rule
        else {
            return;
        };
        if self.paused || !self.initialized {
            return;
        }
        let over =
            (now.saturating_duration_since(self.cycle_start).as_nanos() / cycle.as_nanos()) as u32;
        for k in 0..over.min(2) {
            // After the first, the cycles that ended are empty.
            let count = if k == 0 { self.count } else { 0 };
            self.good = if count >= samples { self.good + 1 } else { 0 };
            if self.good == 0 {
                self.active = false;
            } else if self.good >= cycles {
                self.active = true;
            }
            self.count = 0;
        }
        self.cycle_start += cycle * over;
    }
}

/// The temporal layer that serves a subscriber asking for `fps` frames a
/// second from a layer with these frame rates: the lowest whose rate is
/// within 90% of it, as LiveKit's `GetTemporalLayerForSpatialFps`; all of
/// them when none is, or before any were measured, or for no limit (0).
pub fn temporal_for_fps(rates: &[f32; TEMPORAL_LAYERS], fps: u32) -> u8 {
    let wanted = fps as f32 * LAYER_TOLERANCE as f32;
    if fps == 0 {
        return ANY_LAYER;
    }
    rates
        .iter()
        .position(|&r| r > 0.0 && wanted <= r)
        .map_or(ANY_LAYER, |t| t as u8)
}

/// The layer to follow for a subscriber wanting at most `max`: the
/// highest live layer up to it, else the lowest live layer.
pub fn target_layer(layers: &[Layer], max: u8) -> Option<Option<Rid>> {
    let live = || layers.iter().filter(|l| l.live());
    live()
        .filter(|l| l.spatial <= max)
        .max_by_key(|l| l.spatial)
        .or_else(|| live().min_by_key(|l| l.spatial))
        .map(|l| l.rid)
}

/// Whether a codec carries SVC (every layer in one stream) when published
/// without simulcast rids, as LiveKit's clients publish VP9 and AV1.
pub fn is_svc(codec: Codec) -> bool {
    matches!(codec, Codec::Vp9 | Codec::Av1)
}

/// A subscriber's place in an SVC stream: the decode target it receives,
/// and the frames it was sent lately (frame number + 1 by number modulo
/// 64; 0 for none), to tell whether a frame's references reached it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SvcDown {
    pub dt: Option<usize>,
    sent: [u32; 64],
}

impl Default for SvcDown {
    fn default() -> Self {
        SvcDown {
            dt: None,
            sent: [0; 64],
        }
    }
}

impl SvcDown {
    fn was_sent(&self, number: u16) -> bool {
        self.sent[usize::from(number % 64)] == u32::from(number) + 1
    }

    fn mark_sent(&mut self, number: u16) {
        self.sent[usize::from(number % 64)] = u32::from(number) + 1;
    }
}

/// The stream a subscriber's descriptors come from: the backup codec's or
/// the primary's, and its simulcast layer.
pub type DdSource = (bool, Option<Rid>);

/// The frame numbers of the descriptors a subscriber is sent, as LiveKit's
/// `DDForwarder`: they never go back, or Chrome drops a keyframe older than
/// the last structure it took and every frame after it. So a keyframe that
/// starts another stream (a simulcast layer, the backup codec, another
/// track taking over the m-line) continues from the last number sent, as
/// does one far from it (the publisher's encoder restarted, or forwarding
/// paused a long while); otherwise numbers keep their offset, so frame and
/// chain diffs stay valid.
#[derive(Debug, Clone, Copy, Default)]
pub struct DdOut {
    source: Option<DdSource>,
    offset: u64,
    /// The highest number sent (extended).
    pub last: Option<u64>,
}

/// How far, in frames, a keyframe's number may be from the last sent before
/// it counts as another stream (chosen: back by more than packets arrive
/// reordered, forward by about 30 s at 30 fps).
const DD_BACK: i64 = 64;
const DD_AHEAD: i64 = 1000;

impl DdOut {
    /// Carries on from another stream's last number (the m-line's).
    pub fn seed(&mut self, last: u64) {
        if self.last.is_none() {
            self.last = Some(last);
        }
    }

    /// The number to send `f` with; None while a new stream waits for its
    /// keyframe's structure (sent without a descriptor).
    pub fn number(&mut self, source: DdSource, f: &super::dd::Frame) -> Option<u16> {
        let far = |out: u64, last: u64| {
            let d = out.wrapping_sub(last) as i64;
            d <= -DD_BACK || d > DD_AHEAD
        };
        let restart = self.source != Some(source)
            || self
                .last
                .is_some_and(|l| far(f.ext_number.wrapping_add(self.offset), l));
        if restart {
            if !f.layout.has_structure {
                return None;
            }
            self.source = Some(source);
            self.offset = self
                .last
                .map_or(0, |l| l.wrapping_add(1).wrapping_sub(f.ext_number));
        }
        let out = f.ext_number.wrapping_add(self.offset);
        if self.last.is_none_or(|l| out > l) {
            self.last = Some(out);
        }
        Some(out as u16)
    }
}

/// What to do with one SVC packet for one subscriber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SvcChoice {
    /// Forward it, and with what marker bit; None: drop it.
    pub forward: Option<bool>,
    /// The subscriber waits on a keyframe (or a switch frame) of a higher
    /// layer: ask the publisher for one.
    pub want_keyframe: bool,
}

/// Picks, from the decode targets the publisher has active, the highest
/// within a subscriber's limits (else the lowest active one), and decides
/// one packet: forwarded if its frame is part of the subscriber's decode
/// target. A subscriber starts at a keyframe and moves to another target at
/// a keyframe, at a frame that is a switch point for it whose references
/// all reached the subscriber (a switch point only promises the rest of the
/// target decodes if this frame does), or, going down, at any frame start
/// when the lower target is contained in its current one; meanwhile a
/// keyframe is asked for. The marker bit, which the publisher sets at the end of a
/// temporal unit's top spatial layer, is set at the end of the highest
/// spatial layer the subscriber gets (as LiveKit's VP9 selector does).
pub fn svc_select(
    s: &super::dd::Structure,
    active: u32,
    f: &super::dd::Frame,
    keyframe: bool,
    marker: bool,
    max: (u8, u8),
    down: &mut SvcDown,
) -> SvcChoice {
    use super::dd::{NOT_PRESENT, SWITCH};
    let is_active = |dt: usize| dt < 32 && active & (1 << dt) != 0;
    let layers = |dt: usize| s.dt_layers[dt];
    let desired = (0..s.dt_cnt())
        .filter(|&dt| is_active(dt) && layers(dt).0 <= max.0 && layers(dt).1 <= max.1)
        .max_by_key(|&dt| layers(dt))
        .or_else(|| {
            (0..s.dt_cnt())
                .filter(|&dt| is_active(dt))
                .min_by_key(|&dt| layers(dt))
        });
    let mut want_keyframe = false;
    match (down.dt, desired) {
        (_, None) => {}
        (None, Some(want)) => {
            if keyframe && f.start {
                down.dt = Some(want);
            } else {
                want_keyframe = true;
            }
        }
        (Some(cur), Some(want)) if want != cur => {
            let refs_sent = f
                .fdiffs
                .iter()
                .all(|&d| down.was_sent(f.number.wrapping_sub(d)));
            let switch_point = keyframe || (f.dtis.get(want) == Some(&SWITCH) && refs_sent);
            let contained = layers(want) < layers(cur) && s.contained_in(want, cur);
            if f.start && (switch_point || contained) {
                down.dt = Some(want);
            } else if layers(want).0 != layers(cur).0 && !contained {
                want_keyframe = true;
            }
        }
        _ => {}
    }
    let forward = down.dt.and_then(|dt| {
        let present = f.dtis.get(dt).is_some_and(|&d| d != NOT_PRESENT);
        present.then(|| marker || (f.end && f.spatial == layers(dt).0))
    });
    if forward.is_some() {
        down.mark_sent(f.number);
    }
    SvcChoice {
        forward,
        want_keyframe,
    }
}

/// The fields of a VP8 payload descriptor (RFC 7741 §4.2) that temporal
/// layers need, and where they are, to rewrite them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Vp8Descriptor {
    /// S=1 in partition 0: the packet begins a frame.
    pub start: bool,
    /// PictureID, its offset in the payload and whether it is 15 bits.
    pub picture_id: Option<(u16, usize, bool)>,
    /// TL0PICIDX and its offset.
    pub tl0: Option<(u8, usize)>,
    /// The TID/Y/KEYIDX byte's offset, and its fields (TID only with T=1,
    /// KEYIDX only with K=1).
    pub tk: Option<usize>,
    pub tid: Option<u8>,
    pub layer_sync: bool,
    pub key_idx: Option<u8>,
}

pub fn vp8_descriptor(p: &[u8]) -> Option<Vp8Descriptor> {
    let b0 = *p.first()?;
    let mut d = Vp8Descriptor {
        start: b0 & 0x10 != 0 && b0 & 0x07 == 0,
        picture_id: None,
        tl0: None,
        tk: None,
        tid: None,
        layer_sync: false,
        key_idx: None,
    };
    if b0 & 0x80 == 0 {
        return Some(d);
    }
    let x = *p.get(1)?;
    let mut at = 2;
    if x & 0x80 != 0 {
        let m = *p.get(at)?;
        d.picture_id = Some(if m & 0x80 != 0 {
            let id = u16::from_be_bytes([m & 0x7f, *p.get(at + 1)?]);
            (id, at, true)
        } else {
            (u16::from(m), at, false)
        });
        at += if m & 0x80 != 0 { 2 } else { 1 };
    }
    if x & 0x40 != 0 {
        d.tl0 = Some((*p.get(at)?, at));
        at += 1;
    }
    if x & 0x30 != 0 {
        let b = *p.get(at)?;
        d.tk = Some(at);
        if x & 0x20 != 0 {
            d.tid = Some(b >> 6);
            d.layer_sync = b & 0x20 != 0;
        }
        if x & 0x10 != 0 {
            d.key_idx = Some(b & 0x1f);
        }
    }
    Some(d)
}

/// The payload with PictureID, TL0PICIDX and KEYIDX replaced (each kept
/// at its width), sharing nothing with the original.
pub fn vp8_rewrite(p: &[u8], d: &Vp8Descriptor, ids: Vp8Ids) -> Arc<[u8]> {
    let mut out = p.to_vec();
    if let Some((_, at, long)) = d.picture_id {
        if long {
            out[at] = 0x80 | (ids.picture_id >> 8) as u8 & 0x7f;
            out[at + 1] = ids.picture_id as u8;
        } else {
            out[at] = ids.picture_id as u8 & 0x7f;
        }
    }
    if let Some((_, at)) = d.tl0 {
        out[at] = ids.tl0;
    }
    if let (Some(at), Some(_)) = (d.tk, d.key_idx) {
        out[at] = out[at] & 0xe0 | ids.key_idx & 0x1f;
    }
    out.into()
}

/// The picture numbering a VP8 subscriber sees.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Vp8Ids {
    pub picture_id: u16,
    pub tl0: u8,
    pub key_idx: u8,
}

/// Pictures a VP8 subscriber remembers deciding, for their later packets.
const VP8_RECENT: usize = 32;

/// A VP8 subscriber's temporal layer and picture numbering, as LiveKit's
/// VP8 temporal layer selector and munger (`temporallayerselector/vp8.go`,
/// `codecmunger/vp8.go`). Pictures of temporal layers above its limit are
/// dropped and taken out of the PictureID sequence, so the decoder sees no
/// gap; across a simulcast switch PictureID, TL0PICIDX and KEYIDX carry on
/// from the last ones sent.
#[derive(Debug, Clone)]
pub struct Vp8Down {
    /// The temporal layer it gets now.
    current: u8,
    /// Subtracted from the publisher's numbers.
    offsets: Vp8Ids,
    /// The last numbers sent (as the subscriber sees them).
    last: Option<Vp8Ids>,
    /// The newest PictureID seen (the publisher's).
    newest: Option<u16>,
    /// Recent pictures: PictureID + 1 (0: none), and the PictureID offset
    /// they went out with (None: dropped), by PictureID modulo the size.
    recent: [(u16, Option<u16>); VP8_RECENT],
}

impl Default for Vp8Down {
    fn default() -> Self {
        Vp8Down {
            current: 0,
            offsets: Vp8Ids::default(),
            last: None,
            newest: None,
            recent: [(0, None); VP8_RECENT],
        }
    }
}

/// PictureIDs are 15 bits.
const PICTURE_ID_MASK: u16 = 0x7fff;

impl Vp8Down {
    /// Starts on a stream (a layer) at its keyframe `d`: carries on from
    /// the numbers last sent, if any.
    pub fn restart(&mut self, d: &Vp8Descriptor, max_temporal: u8) {
        let ids = d.ids();
        self.offsets = match self.last {
            Some(last) => Vp8Ids {
                picture_id: ids.picture_id.wrapping_sub(last.picture_id).wrapping_sub(1)
                    & PICTURE_ID_MASK,
                tl0: ids.tl0.wrapping_sub(last.tl0).wrapping_sub(1),
                key_idx: ids.key_idx.wrapping_sub(last.key_idx).wrapping_sub(1) & 0x1f,
            },
            None => Vp8Ids::default(),
        };
        self.current = max_temporal;
        self.newest = None;
        self.recent = [(0, None); VP8_RECENT];
    }

    /// Decides one packet for a subscriber limited to `max_temporal`: None
    /// to drop it, else the numbers it goes out with. A picture is decided
    /// at its first packet, and its other packets follow. The layer goes up
    /// at a keyframe, or one at a time at a layer sync frame of the next
    /// layer; down after the end of a frame.
    pub fn select(
        &mut self,
        d: &Vp8Descriptor,
        keyframe: bool,
        marker: bool,
        max_temporal: u8,
    ) -> Option<Vp8Ids> {
        let ids = d.ids();
        let pid = ids.picture_id;
        let slot = usize::from(pid) % VP8_RECENT;
        let offset = if self.recent[slot].0 == pid + 1 {
            self.recent[slot].1
        } else if self
            .newest
            .is_some_and(|n| n.wrapping_sub(pid) & PICTURE_ID_MASK < 0x4000)
        {
            // Older than any remembered: it can't be numbered.
            return None;
        } else {
            self.newest = Some(pid);
            let tid = d.tid.unwrap_or(0);
            if self.current < max_temporal {
                if keyframe && tid <= max_temporal {
                    self.current = max_temporal;
                } else if tid == self.current + 1 && d.start && d.layer_sync {
                    self.current = tid;
                }
            }
            let offset = if tid <= self.current {
                Some(self.offsets.picture_id)
            } else {
                self.offsets.picture_id = self.offsets.picture_id.wrapping_add(1);
                None
            };
            self.recent[slot] = (pid + 1, offset);
            offset
        };
        if marker && self.current > max_temporal {
            self.current = max_temporal;
        }
        let out = Vp8Ids {
            picture_id: pid.wrapping_sub(offset?) & PICTURE_ID_MASK,
            tl0: ids.tl0.wrapping_sub(self.offsets.tl0),
            key_idx: ids.key_idx.wrapping_sub(self.offsets.key_idx) & 0x1f,
        };
        if self.newest == Some(pid) {
            self.last = Some(out);
        }
        Some(out)
    }
}

impl Vp8Descriptor {
    /// The publisher's numbering.
    pub fn ids(&self) -> Vp8Ids {
        Vp8Ids {
            picture_id: self.picture_id.map_or(0, |(id, _, _)| id),
            tl0: self.tl0.map_or(0, |(i, _)| i),
            key_idx: self.key_idx.unwrap_or(0),
        }
    }
}

/// The mime type for a codec as clients name it in `simulcast_codecs`
/// (`vp8`, `video/VP8`, ...).
pub fn mime_for(codec: &str) -> Option<&'static str> {
    let name = codec.rsplit('/').next().unwrap_or(codec);
    [Codec::Vp8, Codec::H264, Codec::Vp9, Codec::Av1, Codec::H265]
        .into_iter()
        .filter_map(mime)
        .find(|m| m[6..].eq_ignore_ascii_case(name))
}

/// A dynacast update: per codec of the track (primary first), the highest
/// quality anyone receives (None for nobody). `video/VP8` is sent as `vp8`;
/// the legacy top-level qualities are the primary's.
pub fn subscribed_quality(
    track_sid: &str,
    codecs: &[(&str, Option<VideoQuality>)],
) -> lk::SubscribedQualityUpdate {
    let qualities = |max: Option<VideoQuality>| -> Vec<lk::SubscribedQuality> {
        [VideoQuality::Low, VideoQuality::Medium, VideoQuality::High]
            .into_iter()
            .map(|q| lk::SubscribedQuality {
                quality: q as i32,
                enabled: max.is_some_and(|m| q as i32 <= m as i32),
            })
            .collect()
    };
    let subscribed_codecs: Vec<lk::SubscribedCodec> = codecs
        .iter()
        .map(|&(mime, max)| lk::SubscribedCodec {
            codec: mime.rsplit('/').next().unwrap_or(mime).to_ascii_lowercase(),
            qualities: qualities(max),
        })
        .collect();
    lk::SubscribedQualityUpdate {
        track_sid: track_sid.to_owned(),
        subscribed_qualities: subscribed_codecs
            .first()
            .map(|c| c.qualities.clone())
            .unwrap_or_default(),
        subscribed_codecs,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn frame_sizes() {
        // SPSs x264 wrote, cropping included (1080 is 1088 coded).
        for (hex, size) in [
            (
                "6742c01fd9005005bb0110000003001000000303c0f1832480",
                (1280, 720),
            ),
            (
                "6764001eacd940a02ff970110000030001000003003c0f162d96",
                (640, 360),
            ),
            (
                "674d4028eca03c0113f2e022000003000200000300781e30632c",
                (1920, 1080),
            ),
        ] {
            let sps = hex::decode(hex).unwrap();
            assert_eq!(frame_size(Codec::H264, &sps), Some(size), "{hex}");
            // Aggregated with a PPS (STAP-A), as browsers send it.
            let mut stap = vec![24];
            stap.extend((sps.len() as u16).to_be_bytes());
            stap.extend(&sps);
            stap.extend([0, 4, 0x68, 0xce, 0x3c, 0x80]);
            assert_eq!(frame_size(Codec::H264, &stap), Some(size));
        }
        // VP8: descriptor with a 7-bit PictureID, a key frame's tag, start
        // code, 1280x720.
        let vp8 = [
            0x90, 0x80, 0x12, 0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a, 0x00, 0x05, 0xd0, 0x02,
        ];
        assert_eq!(frame_size(Codec::Vp8, &vp8), Some((1280, 720)));
        // An interframe: no size.
        let mut inter = vp8;
        inter[3] |= 1;
        assert_eq!(frame_size(Codec::Vp8, &inter), None);
    }

    use super::*;
    use str0m::format::{CodecConfig, FormatParams};
    use str0m::media::Frequency;

    #[test]
    fn finds_where_decoding_can_start() {
        // H.264: STAP-A with SPS and PPS; FU-A starting an IDR; a P slice.
        assert!(starts_keyframe(
            Codec::H264,
            &[24, 0, 2, 0x67, 1, 0, 2, 0x68, 1]
        ));
        assert!(starts_keyframe(Codec::H264, &[0x7c, 0x85, 0]));
        assert!(
            !starts_keyframe(Codec::H264, &[0x7c, 0x05, 0]),
            "not the first fragment"
        );
        assert!(!starts_keyframe(Codec::H264, &[0x41, 0]));
        // VP9: keyframe start, keyframe middle, inter frame.
        assert!(starts_keyframe(Codec::Vp9, &[0x08]));
        assert!(!starts_keyframe(Codec::Vp9, &[0x00]));
        assert!(!starts_keyframe(Codec::Vp9, &[0x48]));
        // VP8: S=1, PID=0, then P=0 in the payload header.
        assert!(starts_keyframe(Codec::Vp8, &[0x10, 0x00]));
        assert!(!starts_keyframe(Codec::Vp8, &[0x10, 0x01]));
        // H.265: VPS alone; FU starting an IDR (19).
        assert!(starts_keyframe(Codec::H265, &[32 << 1, 1, 0]));
        assert!(starts_keyframe(Codec::H265, &[49 << 1, 1, 0x80 | 19, 0]));
        assert!(!starts_keyframe(Codec::H265, &[1 << 1, 1, 0]));
        assert!(starts_keyframe(Codec::Av1, &[0x08]));
        assert!(!starts_keyframe(Codec::Vp8, &[]));
    }

    #[test]
    fn matches_h264_by_profile_and_mode() {
        let mut c = CodecConfig::empty();
        c.add_h264(102.into(), None, true, 0x42e01f);
        c.add_h264(108.into(), None, true, 0x42c01f);
        c.add_h264(125.into(), None, false, 0x42e01f);
        c.add_h264(123.into(), None, true, 0x640032);
        c.add_config(
            96.into(),
            None,
            Codec::Vp8,
            Frequency::NINETY_KHZ,
            None,
            FormatParams::default(),
        );
        let p = c.params();
        assert!(compatible(&p[0], &p[1]), "both constrained baseline");
        assert_eq!(h264_profile(0x4d001f), H264Profile::Main);
        assert_eq!(h264_profile(0x42001f), H264Profile::Baseline);
        assert_eq!(h264_profile(0x640c1f), H264Profile::ConstrainedHigh);
        assert!(!compatible(&p[0], &p[2]), "packetization mode");
        assert!(!compatible(&p[0], &p[3]), "profile");
        assert!(!compatible(&p[0], &p[4]), "codec");
    }

    #[test]
    fn follows_the_highest_live_layer_it_may() {
        let now = Instant::now();
        // A layer that sent a packet, or (`live` false) sent one and then
        // nothing for a cycle.
        let layer = |rid: &str, spatial: u8, live: bool| {
            let mut l = Layer::new(Some(rid.into()), spatial, false, now);
            l.tracker.observe(false, now);
            if !live {
                l.tracker.tick(false, now + Duration::from_millis(1100));
            }
            l
        };
        let all = [
            layer("q", 0, true),
            layer("f", 2, true),
            layer("h", 1, true),
        ];
        assert_eq!(target_layer(&all, ANY_LAYER), Some(Some("f".into())));
        assert_eq!(target_layer(&all, 1), Some(Some("h".into())));
        assert_eq!(target_layer(&all, 0), Some(Some("q".into())));
        let f_stopped = [layer("q", 0, true), layer("f", 2, false)];
        assert_eq!(target_layer(&f_stopped, ANY_LAYER), Some(Some("q".into())));
        let only_f = [layer("q", 0, false), layer("f", 2, true)];
        assert_eq!(
            target_layer(&only_f, 0),
            Some(Some("f".into())),
            "lowest live"
        );
        assert_eq!(target_layer(&[], 0), None);
    }

    /// Packets at `pps` from `from` for `secs`, ticking every 100 ms as the
    /// engine's housekeeping; returns the time it got to.
    fn feed(t: &mut Tracker, from: Instant, secs: f32, pps: u32) -> Instant {
        let steps = (secs * 10.0) as u32;
        let mut sent = 0;
        let mut now = from;
        for k in 1..=steps {
            now = from + Duration::from_millis(u64::from(k) * 100);
            while sent * 10 < k * pps {
                t.observe(false, now);
                sent += 1;
            }
            t.tick(false, now);
        }
        now
    }

    #[test]
    fn packet_tracker_as_livekits() {
        let t0 = Instant::now();
        // A high layer: live from its first packet, stopped after a cycle
        // with fewer than 5, live again only after 10 s with enough.
        let mut f = Tracker::packets(2, false, t0);
        assert!(!f.live());
        f.observe(false, t0);
        assert!(f.live());
        let t = feed(&mut f, t0, 2.0, 30);
        assert!(f.live());
        let t = feed(&mut f, t, 1.0, 0);
        assert!(!f.live(), "a silent cycle stops it");
        let t = feed(&mut f, t, 9.0, 30);
        assert!(!f.live(), "not back before 10 s");
        let t = feed(&mut f, t, 1.5, 30);
        assert!(f.live(), "back after 20 good cycles");
        // Too few packets a cycle (4 per 500 ms) keep it stopped.
        let t = feed(&mut f, t, 1.0, 0);
        let t = feed(&mut f, t, 12.0, 8);
        assert!(!f.live());
        // Reset (dynacast asked for it again): its next packet.
        f.reset(t);
        assert!(!f.live());
        f.observe(false, t);
        assert!(f.live());

        // The lowest layer needs one packet a cycle, and 2 s to return.
        let mut q = Tracker::packets(0, false, t0);
        q.observe(false, t0);
        let t = feed(&mut q, t0, 3.0, 4);
        assert!(q.live());
        let t = feed(&mut q, t, 1.0, 0);
        assert!(!q.live());
        let t = feed(&mut q, t, 1.5, 4);
        assert!(!q.live());
        feed(&mut q, t, 1.0, 4);
        assert!(q.live());

        // A screen share: a packet in 2 s keeps it, and brings it back.
        let mut s = Tracker::packets(2, true, t0);
        s.observe(false, t0);
        let t = feed(&mut s, t0, 4.0, 1);
        assert!(s.live());
        let t = feed(&mut s, t, 4.5, 0);
        assert!(!s.live());
        feed(&mut s, t, 4.0, 1);
        assert!(s.live());
    }

    #[test]
    fn muted_layers_are_not_live_and_start_over() {
        let t0 = Instant::now();
        let mut f = Tracker::packets(2, false, t0);
        f.observe(false, t0);
        let t = feed(&mut f, t0, 1.0, 30);
        f.tick(true, t);
        assert!(!f.live());
        // Packets while muted don't count; unmuted, the first is live.
        f.observe(true, t);
        assert!(!f.live());
        f.observe(false, t + Duration::from_secs(5));
        assert!(f.live());
        // SVC: by the decode targets, while not muted.
        let mut s = Tracker::decode_targets(t0);
        s.observe(false, t0);
        assert!(!s.live());
        s.set_decodable(true);
        assert!(s.live());
        s.tick(true, t0);
        assert!(!s.live());
    }

    fn req(layers: &[(VideoQuality, u32, u32)]) -> lk::AddTrackRequest {
        lk::AddTrackRequest {
            width: 1280,
            height: 720,
            layers: layers
                .iter()
                .map(|&(q, w, h)| lk::VideoLayer {
                    quality: q as i32,
                    width: w,
                    height: h,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn maps_layers_qualities_and_sizes() {
        use VideoQuality::*;
        let three = ladder(&req(&[
            (High, 1280, 720),
            (Low, 320, 180),
            (Medium, 640, 360),
        ]));
        let rids: Vec<_> = three
            .iter()
            .map(|l| (l.rid.as_str(), l.spatial_layer))
            .collect();
        assert_eq!(rids, [("q", 0), ("h", 1), ("f", 2)]);
        assert_eq!(spatial(&three, Some("h".into())), 1);
        assert_eq!(spatial(&three, None), 0);
        assert_eq!(spatial_for_quality(&three, Medium as i32), 1);
        assert_eq!(spatial_for_quality(&three, High as i32), ANY_LAYER);
        assert_eq!(spatial_for_size(&three, 400, 225), 1, "360 serves 225");
        assert_eq!(spatial_for_size(&three, 300, 170), 0);
        assert_eq!(spatial_for_size(&three, 1920, 1080), ANY_LAYER);
        assert_eq!(quality_of(&three, 1), Medium);
        assert_eq!(quality_of(&three, ANY_LAYER), High);

        // Two layers, as a client sends for small video: q and h.
        let two = ladder(&req(&[(Low, 320, 180), (Medium, 640, 360)]));
        assert_eq!(two[1].rid, "h");
        assert_eq!(spatial_for_quality(&two, High as i32), ANY_LAYER);
        assert_eq!(quality_of(&two, ANY_LAYER), Medium);

        let single = ladder(&req(&[]));
        assert_eq!((single.len(), single[0].rid.as_str()), (1, ""));
        assert_eq!(
            max_layer(
                &three,
                &lk::UpdateTrackSettings {
                    disabled: true,
                    ..Default::default()
                }
            ),
            None
        );
    }

    #[test]
    fn dynacast_updates_enable_up_to_the_max() {
        let u = subscribed_quality("TR_x", &[("video/VP8", Some(VideoQuality::Medium))]);
        let on: Vec<bool> = u.subscribed_qualities.iter().map(|q| q.enabled).collect();
        assert_eq!(on, [true, true, false]);
        assert_eq!(u.subscribed_codecs[0].codec, "vp8");
        let off = subscribed_quality("TR_x", &[("video/H264", None)]);
        assert!(
            off.subscribed_codecs[0]
                .qualities
                .iter()
                .all(|q| !q.enabled)
        );
        // A primary nobody can decode, and its backup some want.
        let both = subscribed_quality(
            "TR_x",
            &[("video/AV1", None), ("video/VP8", Some(VideoQuality::High))],
        );
        assert_eq!(both.subscribed_codecs.len(), 2);
        assert!(both.subscribed_qualities.iter().all(|q| !q.enabled));
        assert!(
            both.subscribed_codecs[1]
                .qualities
                .iter()
                .all(|q| q.enabled)
        );
        assert_eq!(mime_for("vp8"), Some("video/VP8"));
        assert_eq!(mime_for("video/AV1"), Some("video/AV1"));
        assert_eq!(mime_for("opus"), None);
    }

    #[test]
    fn svc_selects_by_decode_target() {
        use super::super::dd;
        let hex = |s: &str| -> Vec<u8> {
            (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
                .collect()
        };
        let key = "c00001c0081485214eaaaaa8000600004000100002aa80a8000600004000100002a000a800060000400016d549241b5524906d54923157e001974ca864330e222396eca8655304224390eca87753013f00b3027f016704ff02cf0380";
        let kf = dd::parse(&hex(key), None).unwrap();
        let s = kf.structure.clone().unwrap();
        let all = (1u32 << s.dt_cnt()) - 1;
        // A subscriber limited to the base spatial layer starts at the keyframe.
        let mut low = SvcDown::default();
        let c = svc_select(&s, all, &kf, true, false, (0, 2), &mut low);
        assert_eq!(s.dt_layers[low.dt.unwrap()], (0, 2));
        assert_eq!(c.forward, Some(true), "end of the base layer: marker");
        // Before any keyframe nothing is forwarded, and one is asked for.
        let mut late = SvcDown::default();
        let f = dd::parse(&hex("850004"), Some(&s)).unwrap();
        let c = svc_select(&s, all, &f, false, false, (2, 2), &mut late);
        assert_eq!((c.forward, c.want_keyframe), (None, true));
        // Frames of higher spatial layers aren't forwarded to the base-only
        // subscriber; its own layer's are.
        for d in ["850004", "8a0005", "c4003a", "890068", "0b0090", "8800da"] {
            let f = dd::parse(&hex(d), Some(&s)).unwrap();
            let c = svc_select(&s, all, &f, false, false, (0, 2), &mut low);
            assert_eq!(c.forward.is_some(), f.spatial == 0, "{d}");
        }
    }

    /// A VP8 packet with PictureID (15 bits), TL0PICIDX, TID/Y and KEYIDX.
    fn vp8(pid: u16, tl0: u8, tid: u8, sync: bool, start: bool) -> Vec<u8> {
        let s = if start { 0x10 } else { 0 };
        let tk = tid << 6 | u8::from(sync) << 5 | 3;
        vec![
            0x80 | s,
            0xf0,
            0x80 | (pid >> 8) as u8,
            pid as u8,
            tl0,
            tk,
            0xaa,
        ]
    }

    #[test]
    fn vp8_descriptor_rewritten() {
        let p = vp8(0x1234, 9, 2, true, true);
        let d = vp8_descriptor(&p).unwrap();
        assert!(d.start && d.layer_sync);
        assert_eq!((d.tid, d.key_idx), (Some(2), Some(3)));
        assert_eq!(
            d.ids(),
            Vp8Ids {
                picture_id: 0x1234,
                tl0: 9,
                key_idx: 3
            }
        );
        let ids = Vp8Ids {
            picture_id: 0x0102,
            tl0: 200,
            key_idx: 31,
        };
        let out = vp8_rewrite(&p, &d, ids);
        let e = vp8_descriptor(&out).unwrap();
        assert_eq!(e.ids(), ids);
        assert_eq!((e.tid, e.layer_sync, out[6]), (Some(2), true, 0xaa));
        // Without the X byte there is nothing to number.
        assert_eq!(vp8_descriptor(&[0x10, 0x00]).unwrap().picture_id, None);
    }

    /// L1T3: TIDs 0 2 1 2 per four pictures; TL0PICIDX counts TID 0.
    fn l1t3(first: u16, n: u16) -> Vec<(Vec<u8>, bool)> {
        (0..n)
            .map(|k| {
                let tid = [0, 2, 1, 2][usize::from(k % 4)];
                let tl0 = (k / 4) as u8;
                (vp8(first + k, tl0, tid, tid > 0, true), k == 0)
            })
            .collect()
    }

    fn run(down: &mut Vp8Down, pkts: &[(Vec<u8>, bool)], max: u8) -> Vec<(u16, u8)> {
        pkts.iter()
            .filter_map(|(p, key)| {
                let d = vp8_descriptor(p).unwrap();
                down.select(&d, *key, true, max)
                    .map(|o| (o.picture_id, o.tl0))
            })
            .collect()
    }

    #[test]
    fn vp8_temporal_layers_dropped_and_renumbered() {
        let pkts = l1t3(100, 8);
        for (max, want) in [
            (0, vec![(100, 0), (101, 1)]),
            (1, vec![(100, 0), (101, 0), (102, 1), (103, 1)]),
            (
                ANY_LAYER,
                (0..8).map(|k| (100 + k, (k / 4) as u8)).collect(),
            ),
        ] {
            let mut down = Vp8Down::default();
            let d = vp8_descriptor(&pkts[0].0).unwrap();
            down.restart(&d, max);
            assert_eq!(run(&mut down, &pkts, max), want, "max {max}");
        }
    }

    #[test]
    fn vp8_temporal_switches() {
        let pkts = l1t3(0, 16);
        let mut down = Vp8Down::default();
        down.restart(&vp8_descriptor(&pkts[0].0).unwrap(), 0);
        assert_eq!(run(&mut down, &pkts[..4], 0).len(), 1);
        // Up: at the next TID 1 sync frame (picture 6), then TID 2 (7).
        let up: Vec<u16> = run(&mut down, &pkts[4..12], ANY_LAYER)
            .iter()
            .map(|o| o.0)
            .collect();
        assert_eq!(up, [1, 2, 3, 4, 5, 6, 7]);
        // Down: a dropped picture's later packets go too, and are counted
        // once.
        let p = vp8(12 + 1, 3, 2, true, false);
        let d = vp8_descriptor(&p).unwrap();
        assert_eq!(
            down.select(&vp8_descriptor(&pkts[12].0).unwrap(), false, true, 0)
                .map(|o| o.picture_id),
            Some(8)
        );
        assert_eq!(
            down.select(&vp8_descriptor(&pkts[13].0).unwrap(), false, false, 0),
            None
        );
        assert_eq!(down.select(&d, false, true, 0), None);
        let last = run(&mut down, &pkts[14..], 0);
        assert_eq!(last, []);
        let next = l1t3(16, 1);
        assert_eq!(run(&mut down, &next, 0)[0].0, 9);
    }

    #[test]
    fn vp8_numbering_carries_over_a_layer_switch() {
        let mut down = Vp8Down::default();
        let a = l1t3(100, 4);
        down.restart(&vp8_descriptor(&a[0].0).unwrap(), ANY_LAYER);
        assert_eq!(run(&mut down, &a, ANY_LAYER).last(), Some(&(103, 0)));
        // Another layer's keyframe, with its own numbers.
        let b = l1t3(5000, 2);
        let k = vp8(5000, 77, 0, false, true);
        down.restart(&vp8_descriptor(&k).unwrap(), ANY_LAYER);
        let mut b = b;
        b[0].0 = k;
        b[1].0[4] = 77;
        assert_eq!(run(&mut down, &b, ANY_LAYER), [(104, 1), (105, 1)]);
    }

    #[test]
    fn temporal_layer_for_fps() {
        let rates = [7.5, 15.0, 30.0, 0.0];
        for (fps, want) in [
            (8, 0),
            (10, 1),
            (15, 1),
            (30, 2),
            (60, ANY_LAYER),
            (0, ANY_LAYER),
        ] {
            assert_eq!(temporal_for_fps(&rates, fps), want, "{fps} fps");
        }
        assert_eq!(temporal_for_fps(&[0.0; 4], 5), ANY_LAYER);
    }

    #[test]
    fn layer_frame_rates() {
        let t0 = Instant::now();
        let mut l = Layer::new(None, 0, false, t0);
        // 30 frames a second in L1T3, two packets each.
        for k in 0..32u32 {
            let at = t0 + Duration::from_micros(u64::from(k) * 33_334);
            let tid = [0, 2, 1, 2][k as usize % 4];
            l.count(1000, tid, true, at);
            l.count(1000, tid, false, at);
        }
        let [a, b, c, d] = l.fps;
        assert!(
            (7.0..8.5).contains(&a) && (14.0..16.5).contains(&b),
            "{:?}",
            l.fps
        );
        assert!((29.0..32.0).contains(&c) && d == 0.0, "{:?}", l.fps);
        // Bitrates likewise: a quarter, half and all of the bytes.
        let [a, b, c, d] = l.temporal_rates;
        assert!(
            d == 0 && a > 0 && (b as f32 / a as f32 - 2.0).abs() < 0.2,
            "{:?}",
            l.temporal_rates
        );
        assert!(
            (c as f32 / a as f32 - 4.0).abs() < 0.4 && c == l.bitrate,
            "{:?}",
            l.temporal_rates
        );
    }

    #[test]
    fn dd_numbers_never_go_back() {
        use super::super::dd;
        use super::*;
        let hex = |s: &str| -> Vec<u8> {
            (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
                .collect()
        };
        let key = |n: u16| {
            let mut d = hex(
                "c00001c0081485214eaaaaa8000600004000100002aa80a8000600004000100002a000a800060000400016d549241b5524906d54923157e001974ca864330e222396eca8655304224390eca87753013f00b3027f016704ff02cf0380",
            );
            d[1..3].copy_from_slice(&n.to_be_bytes());
            d
        };
        let delta = |n: u16| {
            let mut d = hex("850000");
            d[1..3].copy_from_slice(&n.to_be_bytes());
            d
        };
        let (q, h) = ((false, Some(Rid::from("q"))), (false, Some(Rid::from("h"))));
        let mut out = DdOut::default();
        let mut sq = dd::Stream::default();
        let mut sh = dd::Stream::default();
        // Layer q from its keyframe at 500: numbers as they come.
        assert_eq!(out.number(q, &sq.parse(&key(500)).unwrap()), Some(500));
        assert_eq!(out.number(q, &sq.parse(&delta(502)).unwrap()), Some(502));
        // A late packet: its own number, no rebase.
        assert_eq!(out.number(q, &sq.parse(&delta(501)).unwrap()), Some(501));
        // Layer h's frames before its keyframe: nothing to describe.
        sh.parse(&key(90));
        assert_eq!(out.number(h, &sh.parse(&delta(95)).unwrap()), None);
        // Its keyframe, numbered below: carries on after 502.
        assert_eq!(out.number(h, &sh.parse(&key(100)).unwrap()), Some(503));
        assert_eq!(out.number(h, &sh.parse(&delta(101)).unwrap()), Some(504));
        // The encoder restarts at 3: a keyframe far back is a new stream.
        assert_eq!(out.number(h, &sh.parse(&key(3)).unwrap()), Some(505));
        // A keyframe far ahead (paused a while) too, past the wrap.
        assert_eq!(out.number(h, &sh.parse(&key(9000)).unwrap()), Some(506));
        // A track taking over the m-line carries on from its last number.
        let mut next = DdOut::default();
        next.seed(out.last.unwrap());
        let mut s2 = dd::Stream::default();
        assert_eq!(next.number(q, &s2.parse(&key(40000)).unwrap()), Some(507));
    }
}
