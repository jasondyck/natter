//! The AV1 RTP Dependency Descriptor (AV1 RTP specification, Appendix A):
//! which spatial and temporal layer a frame belongs to and which decode
//! targets need it. Chrome sends it with VP9 and AV1 SVC (one stream
//! carrying every layer) and with each VP8 simulcast stream (its temporal
//! layers), and an SFU uses it to forward each subscriber only the layers
//! it can show.
//!
//! The template structure comes with keyframes; other descriptors (usually
//! 3 bytes) name a template in it, so the parser keeps the latest structure
//! per stream.

use std::sync::Arc;

/// Decode target indications: how a frame matters to a decode target
/// (0 not present, 1 discardable, 2 switch point, 3 required).
pub const NOT_PRESENT: u8 = 0;
pub const SWITCH: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Template {
    pub spatial: u8,
    pub temporal: u8,
    /// Per decode target.
    pub dtis: Vec<u8>,
    pub fdiffs: Vec<u16>,
    pub chain_fdiffs: Vec<u8>,
}

/// The template dependency structure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Structure {
    pub template_id_offset: u8,
    pub templates: Vec<Template>,
    /// Each decode target's highest spatial and temporal layer.
    pub dt_layers: Vec<(u8, u8)>,
    pub chain_cnt: u8,
    /// Each decode target's protecting chain (when `chain_cnt` > 0).
    pub dt_protected_by: Vec<u8>,
    /// Width and height per spatial layer, if given.
    pub resolutions: Vec<(u16, u16)>,
}

impl Structure {
    pub fn dt_cnt(&self) -> usize {
        self.dt_layers.len()
    }

    /// Whether every frame decode target `a` needs is also part of `b`, so
    /// a receiver of `b` already has all of `a`'s references and can move
    /// to `a` at any frame. Not so between spatial layers that depend on
    /// each other only at keyframes (`L3T3_KEY`).
    /// Decode target `dt` and those at or below both its layers, as a
    /// bitmask: what a receiver of `dt` is sent (LiveKit's
    /// `GetActiveDecodeTargetBitmask`).
    pub fn at_or_below(&self, dt: usize) -> u32 {
        let Some(&(s, t)) = self.dt_layers.get(dt) else {
            return 0;
        };
        self.dt_layers
            .iter()
            .enumerate()
            .filter(|&(k, &(ls, lt))| k < 32 && ls <= s && lt <= t)
            .fold(0, |m, (k, _)| m | 1 << k)
    }

    pub fn contained_in(&self, a: usize, b: usize) -> bool {
        self.templates
            .iter()
            .all(|t| t.dtis.get(a) == Some(&NOT_PRESENT) || t.dtis.get(b) != Some(&NOT_PRESENT))
    }
}

/// One packet's descriptor, resolved against the structure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub start: bool,
    pub end: bool,
    pub number: u16,
    pub spatial: u8,
    pub temporal: u8,
    /// Per decode target.
    pub dtis: Vec<u8>,
    pub fdiffs: Vec<u16>,
    /// Decode targets the sender says it is producing, if it said.
    pub active: Option<u32>,
    /// A structure this descriptor carried (on keyframes).
    pub structure: Option<Structure>,
    /// The frame number extended past 16 bits, by the stream it came on
    /// (`Stream::parse`; 0 from `parse`).
    pub ext_number: u64,
    pub layout: Layout,
}

/// Where a descriptor's optional parts lie, in bits, for rewriting it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Layout {
    /// Past the mandatory 3 bytes: the extended fields' flags follow.
    pub extended: bool,
    pub has_structure: bool,
    /// Where the active decode targets bitmask goes (after the structure),
    /// where it ends (the same when it has none), and the descriptor's end.
    pub mask_at: usize,
    pub mask_end: usize,
    pub end: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Short,
    /// A template, or the structure itself, missing.
    NoStructure,
    Invalid,
}

struct Bits<'a> {
    data: &'a [u8],
    pos: usize,
}

impl Bits<'_> {
    fn f(&mut self, n: u32) -> Result<u32, Error> {
        let mut v = 0u32;
        for _ in 0..n {
            let byte = *self.data.get(self.pos / 8).ok_or(Error::Short)?;
            let bit = (byte >> (7 - self.pos % 8)) & 1;
            v = (v << 1) | u32::from(bit);
            self.pos += 1;
        }
        Ok(v)
    }

    fn flag(&mut self) -> Result<bool, Error> {
        Ok(self.f(1)? == 1)
    }

    /// Non-symmetric unsigned value in 0..n.
    fn ns(&mut self, n: u32) -> Result<u32, Error> {
        let w = 32 - n.leading_zeros();
        let m = (1u32 << w) - n;
        let v = self.f(w.saturating_sub(1))?;
        if v < m {
            return Ok(v);
        }
        let extra = self.f(1)?;
        Ok((v << 1) - m + extra)
    }
}

/// Parses a descriptor, against `known`, the stream's latest structure.
/// What one stream's descriptors said lately: its structure, and the
/// decode targets the sender has active.
#[derive(Debug, Clone, Default)]
pub struct Stream {
    pub structure: Option<Arc<Structure>>,
    pub active: u32,
    /// The highest extended frame number seen.
    last_ext: Option<u64>,
}

impl Stream {
    /// Parses a descriptor of this stream, keeping a structure it carries
    /// and the active decode targets it names.
    pub fn parse(&mut self, data: &[u8]) -> Option<Frame> {
        let mut f = parse(data, self.structure.as_deref()).ok()?;
        if let Some(s) = f.structure.take() {
            self.active = (1u32 << s.dt_cnt().min(31)) - 1;
            self.structure = Some(Arc::new(s));
        }
        if let Some(a) = f.active {
            self.active = a;
        }
        // Extended numbers start at 2^32, so reordered frames before the
        // first never go below zero.
        f.ext_number = match self.last_ext {
            None => (1 << 32) + u64::from(f.number),
            Some(l) => l.wrapping_add_signed(i64::from(f.number.wrapping_sub(l as u16) as i16)),
        };
        if self.last_ext.is_none_or(|l| f.ext_number > l) {
            self.last_ext = Some(f.ext_number);
        }
        Some(f)
    }
}

pub fn parse(data: &[u8], known: Option<&Structure>) -> Result<Frame, Error> {
    let mut b = Bits { data, pos: 0 };
    let start = b.flag()?;
    let end = b.flag()?;
    let template_id = b.f(6)? as u8;
    let number = b.f(16)? as u16;

    let (mut custom_dtis, mut custom_fdiffs, mut custom_chains) = (false, false, false);
    let mut structure = None;
    let mut active = None;
    let mut layout = Layout {
        extended: data.len() > 3,
        mask_at: 24,
        mask_end: 24,
        ..Default::default()
    };
    if layout.extended {
        let structure_present = b.flag()?;
        let active_present = b.flag()?;
        custom_dtis = b.flag()?;
        custom_fdiffs = b.flag()?;
        custom_chains = b.flag()?;
        if structure_present {
            let s = template_dependency_structure(&mut b)?;
            active = Some(if s.dt_cnt() >= 32 {
                u32::MAX
            } else {
                (1u32 << s.dt_cnt()) - 1
            });
            structure = Some(s);
        }
        layout.has_structure = structure_present;
        layout.mask_at = b.pos;
        if active_present {
            let dt_cnt = structure
                .as_ref()
                .or(known)
                .ok_or(Error::NoStructure)?
                .dt_cnt();
            active = Some(b.f(dt_cnt as u32)?);
        }
        layout.mask_end = b.pos;
    }
    let s = structure.as_ref().or(known).ok_or(Error::NoStructure)?;
    let index = (usize::from(template_id) + 64 - usize::from(s.template_id_offset)) % 64;
    let t = s.templates.get(index).ok_or(Error::NoStructure)?;
    let dtis = if custom_dtis {
        (0..s.dt_cnt())
            .map(|_| b.f(2).map(|v| v as u8))
            .collect::<Result<_, _>>()?
    } else {
        t.dtis.clone()
    };
    let fdiffs = if custom_fdiffs {
        let mut v = vec![];
        loop {
            let size = b.f(2)?;
            if size == 0 {
                break;
            }
            v.push(b.f(4 * size)? as u16 + 1);
        }
        v
    } else {
        t.fdiffs.clone()
    };
    if custom_chains {
        for _ in 0..s.chain_cnt {
            b.f(8)?;
        }
    }
    layout.end = b.pos;
    Ok(Frame {
        start,
        end,
        number,
        spatial: t.spatial,
        temporal: t.temporal,
        dtis,
        fdiffs,
        active,
        structure,
        ext_number: 0,
        layout,
    })
}

/// A descriptor as sent on: `data` (parsed as `f`) with frame number
/// `number` and the active decode targets bitmask `active` (of `dt_cnt`
/// decode targets), which a 3-byte descriptor gains extended fields for.
pub fn rewrite(data: &[u8], f: &Frame, dt_cnt: usize, number: u16, active: u32) -> Vec<u8> {
    let l = &f.layout;
    let mut w = Writer::default();
    w.copy(data, 0, 8);
    w.put(u32::from(number), 16);
    if l.extended {
        w.copy(data, 24, 25);
        w.put(1, 1);
        w.copy(data, 26, l.mask_at);
    } else {
        // No structure, the bitmask, no custom DTIs, fdiffs or chains.
        w.put(0b01000, 5);
    }
    w.put(active, dt_cnt);
    if l.extended {
        w.copy(data, l.mask_end, l.end);
    }
    w.bytes
}

#[derive(Default)]
struct Writer {
    bytes: Vec<u8>,
    bits: usize,
}

impl Writer {
    fn bit(&mut self, v: bool) {
        if self.bits.is_multiple_of(8) {
            self.bytes.push(0);
        }
        if v {
            *self.bytes.last_mut().expect("pushed") |= 0x80 >> (self.bits % 8);
        }
        self.bits += 1;
    }

    fn put(&mut self, v: u32, n: usize) {
        for k in (0..n).rev() {
            self.bit((v >> k) & 1 == 1);
        }
    }

    /// Bits `from..to` of `data`.
    fn copy(&mut self, data: &[u8], from: usize, to: usize) {
        for pos in from..to {
            self.bit((data[pos / 8] >> (7 - pos % 8)) & 1 == 1);
        }
    }
}

fn template_dependency_structure(b: &mut Bits) -> Result<Structure, Error> {
    let template_id_offset = b.f(6)? as u8;
    let dt_cnt = b.f(5)? as usize + 1;

    // Layers: each template's spatial and temporal id.
    let mut layers = vec![];
    let (mut spatial, mut temporal) = (0u8, 0u8);
    loop {
        layers.push((spatial, temporal));
        if layers.len() > 64 {
            return Err(Error::Invalid);
        }
        match b.f(2)? {
            0 => {}
            1 => temporal += 1,
            2 => {
                temporal = 0;
                spatial += 1;
            }
            _ => break,
        }
    }
    let mut templates: Vec<Template> = layers
        .iter()
        .map(|&(spatial, temporal)| Template {
            spatial,
            temporal,
            dtis: vec![],
            fdiffs: vec![],
            chain_fdiffs: vec![],
        })
        .collect();
    for t in &mut templates {
        t.dtis = (0..dt_cnt)
            .map(|_| b.f(2).map(|v| v as u8))
            .collect::<Result<_, _>>()?;
    }
    for t in &mut templates {
        while b.flag()? {
            t.fdiffs.push(b.f(4)? as u16 + 1);
        }
    }
    let chain_cnt = b.ns(dt_cnt as u32 + 1)? as u8;
    let mut dt_protected_by = vec![];
    if chain_cnt > 0 {
        dt_protected_by = (0..dt_cnt)
            .map(|_| b.ns(u32::from(chain_cnt)).map(|v| v as u8))
            .collect::<Result<_, _>>()?;
        for t in &mut templates {
            t.chain_fdiffs = (0..chain_cnt)
                .map(|_| b.f(4).map(|v| v as u8))
                .collect::<Result<_, _>>()?;
        }
    }
    let dt_layers = (0..dt_cnt)
        .map(|dt| {
            templates
                .iter()
                .filter(|t| t.dtis[dt] != NOT_PRESENT)
                .fold((0, 0), |(s, tl), t| (s.max(t.spatial), tl.max(t.temporal)))
        })
        .collect();
    let mut resolutions = vec![];
    if b.flag()? {
        let max_spatial = templates.iter().map(|t| t.spatial).max().unwrap_or(0);
        for _ in 0..=max_spatial {
            let w = b.f(16)? as u16 + 1;
            let h = b.f(16)? as u16 + 1;
            resolutions.push((w, h));
        }
    }
    Ok(Structure {
        template_id_offset,
        templates,
        dt_layers,
        chain_cnt,
        dt_protected_by,
        resolutions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// Chrome's L3T3_KEY keyframe descriptor (VP9 through LiveKit's JS SDK).
    const KEY: &str = "c00001c0081485214eaaaaa8000600004000100002aa80a8000600004000100002a000a800060000400016d549241b5524906d54923157e001974ca864330e222396eca8655304224390eca87753013f00b3027f016704ff02cf0380";

    #[test]
    fn parses_chromes_l3t3_key_structure() {
        let f = parse(&hex(KEY), None).unwrap();
        // 0xc0: the base layer's keyframe fits one packet.
        assert!(f.start && f.end);
        assert_eq!(f.number, 1);
        assert_eq!((f.spatial, f.temporal), (0, 0));
        let s = f.structure.expect("a keyframe carries the structure");
        // 3 spatial × 3 temporal decode targets.
        assert_eq!(s.dt_cnt(), 9);
        let mut layers = s.dt_layers.clone();
        layers.sort();
        layers.dedup();
        assert_eq!(layers.len(), 9);
        assert_eq!(layers.iter().map(|l| l.0).max(), Some(2));
        assert_eq!(layers.iter().map(|l| l.1).max(), Some(2));
        assert_eq!(s.resolutions, [(320, 180), (640, 360), (1280, 720)]);
        // Chrome starts with the lowest spatial layer's three decode
        // targets active, adding layers as its encoder ramps up.
        assert_eq!(f.active, Some(0b111));
        let active_layers: Vec<(u8, u8)> = (0..9)
            .filter(|dt| f.active.unwrap() & (1 << dt) != 0)
            .map(|dt| s.dt_layers[dt])
            .collect();
        assert!(active_layers.iter().all(|l| l.0 == 0), "{active_layers:?}");
        // The keyframe's base layer matters to every decode target.
        assert!(f.dtis.iter().all(|&d| d != NOT_PRESENT));
    }

    #[test]
    fn resolves_short_descriptors_against_the_structure() {
        let s = parse(&hex(KEY), None).unwrap().structure.unwrap();
        // start, template 5, frame 4; end, template 11, frame 0xb4.
        let a = parse(&hex("850004"), Some(&s)).unwrap();
        assert!(a.start && !a.end);
        assert_eq!(a.number, 4);
        let b = parse(&hex("4b00b4"), Some(&s)).unwrap();
        assert!(!b.start && b.end);
        assert_eq!(b.number, 0xb4);
        // Every frame belongs to a layer within the structure, and to the
        // decode target of its own layers.
        for d in ["850004", "8a0005", "c4003a", "890068", "0b0090", "8800da"] {
            let f = parse(&hex(d), Some(&s)).unwrap();
            assert!(f.spatial <= 2 && f.temporal <= 2, "{d}");
            let own = s
                .dt_layers
                .iter()
                .position(|&l| l == (f.spatial, f.temporal))
                .unwrap();
            assert_ne!(f.dtis[own], NOT_PRESENT, "{d}");
        }
        assert_eq!(parse(&hex("850004"), None), Err(Error::NoStructure));
    }

    #[test]
    fn key_mode_spatial_layers_are_not_nested() {
        let s = parse(&hex(KEY), None).unwrap().structure.unwrap();
        let dt = |l: (u8, u8)| s.dt_layers.iter().position(|&x| x == l).unwrap();
        // A lower temporal layer of the same spatial layer: nested.
        assert!(s.contained_in(dt((2, 1)), dt((2, 2))));
        assert!(s.contained_in(dt((0, 0)), dt((0, 2))));
        // A lower spatial layer: L3T3_KEY predicts across them only at
        // keyframes, so the top layer's receiver lacks its references.
        assert!(!s.contained_in(dt((1, 2)), dt((2, 2))));
    }

    /// What a rewrite must keep: everything but the number and bitmask.
    fn same_but(a: &Frame, b: &Frame) {
        assert_eq!((a.start, a.end), (b.start, b.end));
        assert_eq!((a.spatial, a.temporal), (b.spatial, b.temporal));
        assert_eq!((&a.dtis, &a.fdiffs), (&b.dtis, &b.fdiffs));
        assert_eq!(a.structure, b.structure);
    }

    #[test]
    fn rewrites_number_and_active_targets() {
        let kf = parse(&hex(KEY), None).unwrap();
        let s = kf.structure.clone().unwrap();
        // The keyframe: its structure kept, the bitmask replaced after it.
        let out = rewrite(&hex(KEY), &kf, s.dt_cnt(), 0x1234, 0b11);
        let r = parse(&out, None).unwrap();
        same_but(&kf, &r);
        assert_eq!((r.number, r.active), (0x1234, Some(0b11)));
        assert_eq!(r.structure.as_ref(), Some(&s));
        // A 3-byte descriptor gains the extended fields for its bitmask.
        let short = hex("850004");
        let f = parse(&short, Some(&s)).unwrap();
        assert!(!f.layout.extended);
        let out = rewrite(&short, &f, s.dt_cnt(), 7, 0b1_1111_1111);
        let r = parse(&out, Some(&s)).unwrap();
        same_but(&f, &r);
        assert_eq!((r.number, r.active), (7, Some(0b1_1111_1111)));
        // One that already had a bitmask (and custom fdiffs): replaced, the
        // rest kept. Built from the short one, then rewritten again.
        let with = rewrite(&short, &f, s.dt_cnt(), 8, 0b111);
        let f2 = parse(&with, Some(&s)).unwrap();
        assert!(f2.layout.mask_end > f2.layout.mask_at);
        let r = parse(&rewrite(&with, &f2, s.dt_cnt(), 9, 0b1), Some(&s)).unwrap();
        same_but(&f, &r);
        assert_eq!((r.number, r.active), (9, Some(0b1)));
    }

    #[test]
    fn rewrite_keeps_custom_fields() {
        let s = parse(&hex(KEY), None).unwrap().structure.unwrap();
        // Extended, no structure or bitmask, custom fdiffs: 2 and 5 (each
        // a 2-bit size of 1 and 4 bits minus one), then the end.
        let mut w = Writer::default();
        w.put(0b10_000101, 8);
        w.put(0x0042, 16);
        w.put(0b00010, 5);
        w.put(0b01, 2);
        w.put(1, 4);
        w.put(0b01, 2);
        w.put(4, 4);
        w.put(0, 2);
        let data = w.bytes;
        let f = parse(&data, Some(&s)).unwrap();
        assert_eq!(f.fdiffs, [2, 5]);
        let r = parse(&rewrite(&data, &f, s.dt_cnt(), 0x43, 0b111), Some(&s)).unwrap();
        same_but(&f, &r);
        assert_eq!((r.number, r.active), (0x43, Some(0b111)));
    }

    #[test]
    fn decode_targets_at_or_below() {
        let s = parse(&hex(KEY), None).unwrap().structure.unwrap();
        let dt = |l: (u8, u8)| s.dt_layers.iter().position(|&x| x == l).unwrap();
        let mask = |ls: &[(u8, u8)]| ls.iter().fold(0, |m, &l| m | 1 << dt(l));
        assert_eq!(s.at_or_below(dt((0, 0))), mask(&[(0, 0)]));
        assert_eq!(
            s.at_or_below(dt((1, 1))),
            mask(&[(0, 0), (0, 1), (1, 0), (1, 1)])
        );
        assert_eq!(s.at_or_below(dt((2, 2))), (1 << 9) - 1);
    }

    #[test]
    fn extends_frame_numbers_across_wraps() {
        let key = hex(KEY);
        let mut st = Stream::default();
        let first = st.parse(&key).unwrap().ext_number;
        // 0xffff then 0x0001: forward 2 past the wrap; 0xfffe late.
        let at = |n: u16| {
            let mut d = hex("850000");
            d[1..3].copy_from_slice(&n.to_be_bytes());
            d
        };
        let a = st.parse(&at(0xffff)).unwrap().ext_number;
        let b = st.parse(&at(0x0001)).unwrap().ext_number;
        let c = st.parse(&at(0xfffe)).unwrap().ext_number;
        assert_eq!(a, first - 2);
        assert_eq!(b, a + 2);
        assert_eq!(c, a - 1);
    }

    #[test]
    fn non_symmetric_values() {
        // ns(5): 0..2 in 2 bits, 3 and 4 in 3: 00 01 110 111.
        let mut b = Bits {
            data: &[0b0001_1101, 0b1100_0000],
            pos: 0,
        };
        assert_eq!(b.ns(5), Ok(0));
        assert_eq!(b.ns(5), Ok(1));
        assert_eq!(b.ns(5), Ok(3));
        assert_eq!(b.ns(5), Ok(4));
    }
}
