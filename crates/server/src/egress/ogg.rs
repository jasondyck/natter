//! Ogg Opus files (RFC 7845, framing from RFC 3533): an Opus track as
//! LiveKit's track egress writes it, without transcoding.
//!
//! Packets come in RTP order with their RTP timestamps. Gaps (DTX, a
//! muted track, loss) are filled with silent frames, so the file's
//! timeline matches the call's.

use std::io::{self, Write};

const SAMPLE_RATE: u32 = 48_000;
/// libopus' encoder lookahead at 48 kHz, the usual pre-skip.
const PRE_SKIP: u16 = 312;
/// A page is flushed at this many lacing values (of 255).
const PAGE_SEGMENTS: usize = 240;
/// Or once it holds this much audio.
const PAGE_SAMPLES: u64 = SAMPLE_RATE as u64;
/// Gaps longer than this are a new start (a broken clock), not silence.
const MAX_GAP: u64 = 3600 * SAMPLE_RATE as u64;

/// Silent CELT frames by duration in samples: TOC (fullband CELT, mono,
/// one frame) and an all-zero range-coded body, as Chrome's DTX sends.
const SILENCE: [(u64, [u8; 3]); 4] = [
    (960, [0xf8, 0xff, 0xfe]),
    (480, [0xf0, 0xff, 0xfe]),
    (240, [0xe8, 0xff, 0xfe]),
    (120, [0xe0, 0xff, 0xfe]),
];

pub struct OggOpus<W: Write> {
    out: W,
    serial: u32,
    page_seq: u32,
    /// The current page: lacing values and packet bytes.
    lacing: Vec<u8>,
    body: Vec<u8>,
    page_start: u64,
    /// Samples written, the granule position after the last packet.
    granule: u64,
    /// The last packet's RTP timestamp, unwrapped, and where it ended.
    last: Option<(u32, u64, u64)>,
    headers: bool,
}

impl<W: Write> OggOpus<W> {
    pub fn new(out: W, serial: u32) -> Self {
        OggOpus {
            out,
            serial,
            page_seq: 0,
            lacing: vec![],
            body: vec![],
            page_start: 0,
            granule: 0,
            last: None,
            headers: false,
        }
    }

    /// Writes one Opus packet with RTP timestamp `rtp_ts`; packets not
    /// after the last one (late or repeated) are dropped.
    pub fn push(&mut self, rtp_ts: u32, packet: &[u8]) -> io::Result<()> {
        let Some(samples) = packet_samples(packet) else {
            return Ok(());
        };
        if !self.headers {
            self.write_headers(packet[0] & 0x04 != 0)?;
        }
        let ts = match self.last {
            None => u64::from(rtp_ts),
            Some((last_rtp, last_ts, next)) => {
                let ahead = rtp_ts.wrapping_sub(last_rtp);
                if ahead == 0 || ahead > u32::MAX / 2 {
                    return Ok(());
                }
                let ts = last_ts + u64::from(ahead);
                let gap = ts.saturating_sub(next);
                if gap < MAX_GAP {
                    self.silence(gap)?;
                }
                ts
            }
        };
        self.packet(packet, samples)?;
        self.last = Some((rtp_ts, ts, ts + samples));
        Ok(())
    }

    /// Duration written so far.
    pub fn duration(&self) -> std::time::Duration {
        std::time::Duration::from_micros(self.granule * 1_000_000 / u64::from(SAMPLE_RATE))
    }

    /// Ends the stream (the last page marked end of stream).
    pub fn finish(mut self) -> io::Result<W> {
        if !self.headers {
            self.write_headers(false)?;
        }
        self.flush_page(0x04)?;
        self.out.flush()?;
        Ok(self.out)
    }

    fn silence(&mut self, mut samples: u64) -> io::Result<()> {
        for (n, frame) in SILENCE {
            while samples >= n {
                self.packet(&frame, n)?;
                samples -= n;
            }
        }
        Ok(())
    }

    fn packet(&mut self, packet: &[u8], samples: u64) -> io::Result<()> {
        let segments = packet.len() / 255 + 1;
        if self.lacing.len() + segments > 255 {
            self.flush_page(0)?;
        }
        self.lacing
            .extend(std::iter::repeat_n(255, packet.len() / 255));
        self.lacing.push((packet.len() % 255) as u8);
        self.body.extend_from_slice(packet);
        self.granule += samples;
        if self.lacing.len() >= PAGE_SEGMENTS || self.granule - self.page_start >= PAGE_SAMPLES {
            self.flush_page(0)?;
        }
        Ok(())
    }

    fn write_headers(&mut self, stereo: bool) -> io::Result<()> {
        self.headers = true;
        let mut head = b"OpusHead".to_vec();
        head.push(1);
        head.push(if stereo { 2 } else { 1 });
        head.extend_from_slice(&PRE_SKIP.to_le_bytes());
        head.extend_from_slice(&SAMPLE_RATE.to_le_bytes());
        head.extend_from_slice(&0i16.to_le_bytes());
        head.push(0);
        self.page(0x02, 0, &head)?;
        let vendor = b"natter";
        let mut tags = b"OpusTags".to_vec();
        tags.extend_from_slice(&(vendor.len() as u32).to_le_bytes());
        tags.extend_from_slice(vendor);
        tags.extend_from_slice(&0u32.to_le_bytes());
        self.page(0, 0, &tags)
    }

    /// A page holding one whole header packet.
    fn page(&mut self, flags: u8, granule: u64, packet: &[u8]) -> io::Result<()> {
        let mut lacing = vec![255; packet.len() / 255];
        lacing.push((packet.len() % 255) as u8);
        self.write_page(flags, granule, &lacing, packet)
    }

    fn flush_page(&mut self, flags: u8) -> io::Result<()> {
        if self.lacing.is_empty() && flags == 0 {
            return Ok(());
        }
        let lacing = std::mem::take(&mut self.lacing);
        let body = std::mem::take(&mut self.body);
        self.write_page(flags, self.granule + u64::from(PRE_SKIP), &lacing, &body)?;
        self.page_start = self.granule;
        Ok(())
    }

    fn write_page(
        &mut self,
        flags: u8,
        granule: u64,
        lacing: &[u8],
        body: &[u8],
    ) -> io::Result<()> {
        let mut page = Vec::with_capacity(27 + lacing.len() + body.len());
        page.extend_from_slice(b"OggS");
        page.push(0);
        page.push(flags);
        page.extend_from_slice(&granule.to_le_bytes());
        page.extend_from_slice(&self.serial.to_le_bytes());
        page.extend_from_slice(&self.page_seq.to_le_bytes());
        page.extend_from_slice(&[0; 4]);
        page.push(lacing.len() as u8);
        page.extend_from_slice(lacing);
        page.extend_from_slice(body);
        let crc = crc32(&page);
        page[22..26].copy_from_slice(&crc.to_le_bytes());
        self.page_seq += 1;
        self.out.write_all(&page)
    }
}

/// Samples (at 48 kHz) in an Opus packet, from its TOC (RFC 6716 §3.1).
pub fn packet_samples(p: &[u8]) -> Option<u64> {
    let toc = *p.first()?;
    let config = toc >> 3;
    let frame = match config {
        0..=11 => [480, 960, 1920, 2880][usize::from(config % 4)],
        12..=15 => [480, 960][usize::from(config % 2)],
        _ => [120, 240, 480, 960][usize::from(config % 4)],
    };
    let frames = match toc & 0x03 {
        0 => 1,
        1 | 2 => 2,
        _ => u64::from(p.get(1)? & 0x3f),
    };
    (frames > 0).then_some(frame * frames)
}

/// Ogg's CRC-32: polynomial 0x04c11db7, not reflected, initial 0.
fn crc32(data: &[u8]) -> u32 {
    static TABLE: std::sync::LazyLock<[u32; 256]> = std::sync::LazyLock::new(|| {
        let mut t = [0u32; 256];
        for (i, v) in t.iter_mut().enumerate() {
            let mut r = (i as u32) << 24;
            for _ in 0..8 {
                r = if r & 0x8000_0000 != 0 {
                    (r << 1) ^ 0x04c1_1db7
                } else {
                    r << 1
                };
            }
            *v = r;
        }
        t
    });
    data.iter().fold(0, |crc, &b| {
        (crc << 8) ^ TABLE[usize::from((crc >> 24) as u8 ^ b)]
    })
}
