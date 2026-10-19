//! What each fake Opus packet carries, so a receiver can tell which
//! publisher sent it, whether it is a duplicate, and how long it took.
//! Servers forward the payload untouched, so the stamp survives any
//! sequence number or timestamp rewriting on the way.
//!
//! Layout: an Opus TOC byte (CELT fullband 20ms, mono, one frame), then
//! `NL`, flags, publisher id (u32), sequence (u32), send time (u64 µs since
//! the run started), all big-endian, then zero padding.
//!
//! A video frame is a VP8 frame header instead of the TOC byte: the 3-byte
//! frame tag, and for a keyframe the start code and 1280×720, so servers
//! that look into VP8 see a keyframe where there is one. The stamp follows,
//! and the sequence counts frames.

/// The TOC byte of a 20ms fullband CELT frame, so the packet parses as Opus.
const TOC: u8 = 0xF8;
const MAGIC: [u8; 2] = *b"NL";
pub const LEN: usize = 20;
/// A VP8 interframe's header: the frame tag.
const VP8_HEADER: usize = 3;
/// A keyframe's: the tag, start code, width and height.
const VP8_KEY_HEADER: usize = 10;

/// Sent while the run is being measured; only these packets are counted.
pub const MEASURED: u8 = 1;
/// Video: the simulcast layer (0 to 2) a frame was sent on, in bits 1-2.
const LAYER_SHIFT: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    pub flags: u8,
    pub publisher: u32,
    pub seq: u32,
    pub sent_us: u64,
}

impl Stamp {
    /// A payload of `size` bytes (at least [`LEN`]) carrying this stamp.
    pub fn payload(&self, size: usize) -> Vec<u8> {
        let mut p = Vec::with_capacity(size.max(LEN));
        p.push(TOC);
        self.body(&mut p);
        p.resize(size.max(LEN), 0);
        p
    }

    pub fn parse(payload: &[u8]) -> Option<Stamp> {
        if payload.first() != Some(&TOC) {
            return None;
        }
        Self::parse_body(&payload[1..])
    }

    /// A VP8 frame of `size` bytes (at least the header and the stamp).
    pub fn vp8_frame(&self, keyframe: bool, size: usize) -> Vec<u8> {
        let header = if keyframe { VP8_KEY_HEADER } else { VP8_HEADER };
        let size = size.max(header + LEN - 1);
        let mut f = Vec::with_capacity(size);
        // Frame tag: P (0 for a keyframe), version 0, shown, and the first
        // partition's size: the whole frame after the header.
        let tag = u32::from(!keyframe) | 1 << 4 | ((size - header) as u32 & 0x7_FFFF) << 5;
        f.extend_from_slice(&tag.to_le_bytes()[..3]);
        if keyframe {
            f.extend_from_slice(&[0x9d, 0x01, 0x2a]);
            f.extend_from_slice(&1280u16.to_le_bytes());
            f.extend_from_slice(&720u16.to_le_bytes());
        }
        self.body(&mut f);
        f.resize(size, 0);
        f
    }

    /// The stamp in a frame from [`Stamp::vp8_frame`], and whether it is a
    /// keyframe.
    pub fn parse_vp8(frame: &[u8]) -> Option<(Stamp, bool)> {
        let keyframe = frame.first()? & 1 == 0;
        let header = if keyframe { VP8_KEY_HEADER } else { VP8_HEADER };
        Some((Self::parse_body(frame.get(header..)?)?, keyframe))
    }

    fn body(&self, p: &mut Vec<u8>) {
        p.extend_from_slice(&MAGIC);
        p.push(self.flags);
        p.extend_from_slice(&self.publisher.to_be_bytes());
        p.extend_from_slice(&self.seq.to_be_bytes());
        p.extend_from_slice(&self.sent_us.to_be_bytes());
    }

    /// Everything after the codec header: `LEN - 1` bytes.
    fn parse_body(b: &[u8]) -> Option<Stamp> {
        if b.len() < LEN - 1 || b[..2] != MAGIC {
            return None;
        }
        let u32_at = |i: usize| u32::from_be_bytes(b[i..i + 4].try_into().unwrap());
        Some(Stamp {
            flags: b[2],
            publisher: u32_at(3),
            seq: u32_at(7),
            sent_us: u64::from_be_bytes(b[11..19].try_into().unwrap()),
        })
    }

    pub fn measured(&self) -> bool {
        self.flags & MEASURED != 0
    }

    pub fn with_layer(mut self, layer: u8) -> Self {
        self.flags |= (layer & 3) << LAYER_SHIFT;
        self
    }

    pub fn layer(&self) -> u8 {
        (self.flags >> LAYER_SHIFT) & 3
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_pads() {
        let s = Stamp {
            flags: MEASURED,
            publisher: 7,
            seq: 1 << 20,
            sent_us: 123_456_789,
        };
        let p = s.payload(100);
        assert_eq!(p.len(), 100);
        assert_eq!(Stamp::parse(&p), Some(s));
        assert!(Stamp::parse(&p[..10]).is_none());
        assert!(Stamp::parse(&[0u8; 40]).is_none());
    }

    #[test]
    fn vp8_frames() {
        let s = Stamp {
            flags: 0,
            publisher: 3,
            seq: 99,
            sent_us: 42,
        };
        let key = s.vp8_frame(true, 4000);
        assert_eq!(key.len(), 4000);
        assert_eq!(key[0] & 1, 0, "P bit clear");
        assert_eq!(&key[3..6], &[0x9d, 0x01, 0x2a]);
        assert_eq!(Stamp::parse_vp8(&key), Some((s, true)));
        let inter = s.vp8_frame(false, 10);
        assert_eq!(inter.len(), 3 + LEN - 1);
        assert_eq!(Stamp::parse_vp8(&inter), Some((s, false)));
        assert!(Stamp::parse(&inter).is_none());
    }
}
