//! RED (RFC 2198) to plain Opus, for subscribers that didn't negotiate RED,
//! ported from LiveKit's `pkg/sfu/redprimaryreceiver.go`: forward each
//! packet's primary block, and when the sequence numbers show a recent
//! packet missing, also forward it from the redundant blocks.

use std::sync::Arc;

/// Redundant blocks LiveKit looks at per packet.
const MAX_RED_COUNT: u16 = 2;

/// One Opus packet out of a RED packet. Sequence number and timestamp are
/// offsets back from the RED packet's own (0 for the primary).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unpacked {
    pub seq_back: u16,
    pub ts_back: u32,
    pub pt: u8,
    pub payload: Arc<[u8]>,
}

struct Block {
    pt: u8,
    ts_offset: u32,
    len: usize,
}

/// Per published track: which of the last 8 sequence numbers arrived.
#[derive(Debug, Default)]
pub struct ToPrimary {
    last_seq: Option<u16>,
    history: u8,
}

impl ToPrimary {
    /// Unpacks one RED packet; recovered packets come first, oldest first,
    /// then the primary. Malformed payloads give nothing.
    pub fn unpack(&mut self, seq: u16, payload: &[u8]) -> Vec<Unpacked> {
        let mut need_recover = false;
        match self.last_seq {
            None => {
                self.last_seq = Some(seq);
                self.history = 0;
            }
            Some(last) => {
                let diff = seq.wrapping_sub(last);
                if diff == 0 {
                    // Duplicate.
                } else if diff > 0x8000 {
                    // Late: mark it in history if recent enough.
                    let back = 65535 - diff;
                    if back < 8 {
                        self.history |= 1 << back;
                        need_recover = true;
                    }
                } else if diff > 8 {
                    self.last_seq = Some(seq);
                    self.history = 0;
                    need_recover = true;
                } else {
                    self.last_seq = Some(seq);
                    self.history = ((u16::from(self.history) << diff) as u8) | (1 << (diff - 1));
                    need_recover = true;
                }
            }
        }

        let mut recover_bits = 0u8;
        if need_recover {
            let first = self.last_seq.unwrap_or(seq).wrapping_sub(seq);
            for (i, bit) in (first..).take(MAX_RED_COUNT.into()).enumerate() {
                if bit > 7 {
                    break;
                }
                if self.history & (1 << bit) == 0 {
                    recover_bits |= 1 << i;
                }
            }
        }
        extract(payload, recover_bits).unwrap_or_default()
    }
}

fn extract(mut payload: &[u8], recover_bits: u8) -> Option<Vec<Unpacked>> {
    let mut blocks = vec![];
    let mut total = 0;
    let primary_pt = loop {
        let first = *payload.first()?;
        if first & 0x80 == 0 {
            payload = &payload[1..];
            break first & 0x7f;
        }
        let head = u32::from_be_bytes(payload.get(..4)?.try_into().ok()?);
        let block = Block {
            len: (head & 0x3ff) as usize,
            ts_offset: (head >> 10) & 0x3fff,
            pt: ((head >> 24) & 0x7f) as u8,
        };
        total += block.len;
        blocks.push(block);
        payload = &payload[4..];
    };
    if payload.len() < total {
        return None;
    }

    let mut out = vec![];
    let n = blocks.len();
    for (i, b) in blocks.iter().enumerate() {
        let recover_index = n - i;
        if recover_bits & (1 << (recover_index - 1)) != 0 {
            out.push(Unpacked {
                seq_back: recover_index as u16,
                ts_back: b.ts_offset,
                pt: b.pt,
                payload: payload[..b.len].into(),
            });
        }
        payload = &payload[b.len..];
    }
    out.push(Unpacked {
        seq_back: 0,
        ts_back: 0,
        pt: primary_pt,
        payload: payload.into(),
    });
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A RED payload with one redundant block (the previous frame) and the
    /// primary, as Chrome sends.
    fn red(prev: &[u8], primary: &[u8]) -> Vec<u8> {
        let head = (1u32 << 31) | (111 << 24) | (960 << 10) | prev.len() as u32;
        let mut out = head.to_be_bytes().to_vec();
        out.push(111);
        out.extend_from_slice(prev);
        out.extend_from_slice(primary);
        out
    }

    #[test]
    fn forwards_primaries_and_recovers_one_loss() {
        let mut r = ToPrimary::default();
        let first = r.unpack(10, &red(b"p9", b"p10"));
        assert_eq!(first.len(), 1);
        assert_eq!(&*first[0].payload, b"p10");
        assert_eq!(first[0].pt, 111);

        // 11 is lost; 12 carries it as redundancy.
        let out = r.unpack(12, &red(b"p11", b"p12"));
        assert_eq!(out.len(), 2);
        assert_eq!(
            (&*out[0].payload, out[0].seq_back, out[0].ts_back),
            (&b"p11"[..], 1, 960)
        );
        assert_eq!(&*out[1].payload, b"p12");

        // In order again: nothing to recover.
        assert_eq!(r.unpack(13, &red(b"p12", b"p13")).len(), 1);
        // Late 11 after recovery is just its primary.
        assert_eq!(r.unpack(11, &red(b"p10", b"p11")).len(), 1);
    }

    #[test]
    fn rejects_truncated_payloads() {
        let mut r = ToPrimary::default();
        let mut bad = red(b"previous", b"x");
        bad.truncate(7);
        assert!(r.unpack(1, &bad).is_empty());
        assert!(r.unpack(2, &[]).is_empty());
    }
}
