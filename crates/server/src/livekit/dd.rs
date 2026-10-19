//! The AV1 RTP Dependency Descriptor (AV1 RTP specification, Appendix A):
//! which spatial and temporal layer a frame belongs to and which decode
//! targets need it. Chrome sends it with VP9 and AV1 SVC (one stream
//! carrying every layer), and an SFU uses it to forward each subscriber
//! only the layers it can show.
//!
//! The template structure comes with keyframes; other descriptors (usually
//! 3 bytes) name a template in it, so the parser keeps the latest structure
//! per stream.

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
pub fn parse(data: &[u8], known: Option<&Structure>) -> Result<Frame, Error> {
    let mut b = Bits { data, pos: 0 };
    let start = b.flag()?;
    let end = b.flag()?;
    let template_id = b.f(6)? as u8;
    let number = b.f(16)? as u16;

    let (mut custom_dtis, mut custom_fdiffs, mut custom_chains) = (false, false, false);
    let mut structure = None;
    let mut active = None;
    if data.len() > 3 {
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
        if active_present {
            let dt_cnt = structure
                .as_ref()
                .or(known)
                .ok_or(Error::NoStructure)?
                .dt_cnt();
            active = Some(b.f(dt_cnt as u32)?);
        }
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
    })
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
