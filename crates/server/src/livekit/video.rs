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

/// A layer not heard from in this long is treated as stopped.
pub const LAYER_TIMEOUT: Duration = Duration::from_secs(1);
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
    let f = p.get(at..at + 10)?;
    if f[0] & 0x01 != 0 || f[3..6] != [0x9d, 0x01, 0x2a] {
        return None;
    }
    let w = u32::from(u16::from_le_bytes([f[6], f[7]]) & 0x3fff);
    let h = u32::from(u16::from_le_bytes([f[8], f[9]]) & 0x3fff);
    (w > 0 && h > 0).then_some((w, h))
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
/// (and VP9 SVC) receivers need it to assemble frames; natter passes it
/// through unchanged (rewriting it per subscriber comes with SVC, PARITY
/// phase 6).
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

/// Requested sizes within this much of a layer's are served by it
/// (LiveKit's `layerSelectionTolerance`).
const SIZE_TOLERANCE: f64 = 0.9;

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
        .find(|l| f64::from(size(l)) >= f64::from(wanted) * SIZE_TOLERANCE)
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
    pub last_seen: Instant,
    pub last_pli: Option<Instant>,
    /// Payload bytes since `window_start`, and the bitrate of the last
    /// whole window (0 until there is one).
    pub bytes: u64,
    pub window_start: Instant,
    pub bitrate: u32,
}

/// How long a layer's bitrate is averaged over.
const RATE_WINDOW: Duration = Duration::from_secs(1);

impl Layer {
    pub fn new(rid: Option<Rid>, spatial: u8, now: Instant) -> Self {
        Layer {
            rid,
            spatial,
            last_seen: now,
            last_pli: None,
            bytes: 0,
            window_start: now,
            bitrate: 0,
        }
    }

    pub fn live(&self, now: Instant) -> bool {
        now.duration_since(self.last_seen) < LAYER_TIMEOUT
    }

    /// Counts a packet of `len` payload bytes, and its headers.
    pub fn count(&mut self, len: usize, now: Instant) {
        self.last_seen = now;
        self.bytes += len as u64 + super::allocator::PACKET_OVERHEAD;
        let elapsed = now.duration_since(self.window_start);
        if elapsed >= RATE_WINDOW {
            self.bitrate = (self.bytes * 8 * 1000 / elapsed.as_millis().max(1) as u64) as u32;
            self.bytes = 0;
            self.window_start = now;
        }
    }
}

/// The layer to follow for a subscriber wanting at most `max`: the
/// highest live layer up to it, else the lowest live layer.
pub fn target_layer(layers: &[Layer], max: u8, now: Instant) -> Option<Option<Rid>> {
    let live = || layers.iter().filter(|l| l.live(now));
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
        let layer = |rid: &str, spatial: u8, ago: u64| Layer {
            last_seen: now - Duration::from_millis(ago),
            ..Layer::new(Some(rid.into()), spatial, now)
        };
        let all = [layer("q", 0, 0), layer("f", 2, 10), layer("h", 1, 0)];
        assert_eq!(target_layer(&all, ANY_LAYER, now), Some(Some("f".into())));
        assert_eq!(target_layer(&all, 1, now), Some(Some("h".into())));
        assert_eq!(target_layer(&all, 0, now), Some(Some("q".into())));
        let f_stopped = [layer("q", 0, 0), layer("f", 2, 2000)];
        assert_eq!(
            target_layer(&f_stopped, ANY_LAYER, now),
            Some(Some("q".into()))
        );
        let only_f = [layer("q", 0, 2000), layer("f", 2, 0)];
        assert_eq!(
            target_layer(&only_f, 0, now),
            Some(Some("f".into())),
            "lowest live"
        );
        assert_eq!(target_layer(&[], 0, now), None);
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
}
