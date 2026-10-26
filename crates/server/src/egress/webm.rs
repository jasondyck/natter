//! WebM files (Matroska, EBML): VP8/VP9 video and Opus audio as they came,
//! as LiveKit's track egress writes VP8 and VP9.
//!
//! Written front to back, then finished in place: the segment and cluster
//! sizes, the duration, and the Cues (one per cluster that starts with a
//! keyframe), which the SeekHead points at, so players can seek.

use std::io::{self, Seek, SeekFrom, Write};

const EBML: u32 = 0x1a45_dfa3;
const SEGMENT: u32 = 0x1853_8067;
const SEEK_HEAD: u32 = 0x114d_9b74;
const SEEK: u32 = 0x4dbb;
const SEEK_ID: u32 = 0x53ab;
const SEEK_POSITION: u32 = 0x53ac;
const INFO: u32 = 0x1549_a966;
const TIMESTAMP_SCALE: u32 = 0x2ad7b1;
const DURATION: u32 = 0x4489;
const MUXING_APP: u32 = 0x4d80;
const WRITING_APP: u32 = 0x5741;
const TRACKS: u32 = 0x1654_ae6b;
const TRACK_ENTRY: u32 = 0xae;
const TRACK_NUMBER: u32 = 0xd7;
const TRACK_UID: u32 = 0x73c5;
const TRACK_TYPE: u32 = 0x83;
const FLAG_LACING: u32 = 0x9c;
const CODEC_ID: u32 = 0x86;
const CODEC_PRIVATE: u32 = 0x63a2;
const CODEC_DELAY: u32 = 0x56aa;
const SEEK_PRE_ROLL: u32 = 0x56bb;
const VIDEO: u32 = 0xe0;
const PIXEL_WIDTH: u32 = 0xb0;
const PIXEL_HEIGHT: u32 = 0xba;
const AUDIO: u32 = 0xe1;
const SAMPLING_FREQUENCY: u32 = 0xb5;
const CHANNELS: u32 = 0x9f;
const CLUSTER: u32 = 0x1f43_b675;
const TIMESTAMP: u32 = 0xe7;
const SIMPLE_BLOCK: u32 = 0xa3;
const CUES: u32 = 0x1c53_bb6b;
const CUE_POINT: u32 = 0xbb;
const CUE_TIME: u32 = 0xb3;
const CUE_TRACK_POSITIONS: u32 = 0xb7;
const CUE_TRACK: u32 = 0xf7;
const CUE_CLUSTER_POSITION: u32 = 0xf1;

/// Size field of an element whose size is filled in later: 8 bytes.
const UNKNOWN_SIZE: [u8; 8] = [0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
/// A block's timestamp is an i16 of milliseconds from its cluster's.
const CLUSTER_SPAN_MS: u64 = 30_000;
/// New clusters at least this often, keyframe or not.
const CLUSTER_MAX_MS: u64 = 5_000;
/// Opus: 80 ms to decode before a seek point (RFC 7845 §4.6).
const OPUS_PRE_ROLL_NS: u64 = 80_000_000;
const OPUS_PRE_SKIP: u16 = 312;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Track {
    Vp8 { width: u32, height: u32 },
    Vp9 { width: u32, height: u32 },
    Opus { channels: u8 },
}

impl Track {
    fn is_video(self) -> bool {
        !matches!(self, Track::Opus { .. })
    }
}

pub struct Webm<W: Write + Seek> {
    out: W,
    tracks: Vec<Track>,
    /// Where the segment's data starts; positions inside are from here.
    segment_data: u64,
    /// Where to write the Cues' position, and the duration.
    cues_seek_at: u64,
    duration_at: u64,
    /// The open cluster: its start, size field and timestamp.
    cluster: Option<(u64, u64)>,
    cues: Vec<(u64, u8, u64)>,
    last_ms: u64,
}

impl<W: Write + Seek> Webm<W> {
    /// Starts a file with these tracks, numbered from 1 in this order.
    pub fn new(mut out: W, tracks: Vec<Track>) -> io::Result<Self> {
        let mut head = vec![];
        element(&mut head, EBML, |b| {
            uint(b, 0x4286, 1);
            uint(b, 0x42f7, 1);
            uint(b, 0x42f2, 4);
            uint(b, 0x42f3, 8);
            string(b, 0x4282, "webm");
            uint(b, 0x4287, 4);
            uint(b, 0x4285, 2);
        });
        id(&mut head, SEGMENT);
        head.extend_from_slice(&UNKNOWN_SIZE);
        let segment_data = head.len() as u64;

        let mut info_body = vec![];
        uint(&mut info_body, TIMESTAMP_SCALE, 1_000_000);
        id(&mut info_body, DURATION);
        size(&mut info_body, 8);
        let duration_in_body = info_body.len();
        float_bytes(&mut info_body, 0.0);
        string(&mut info_body, MUXING_APP, "natter");
        string(&mut info_body, WRITING_APP, "natter");
        let mut info = vec![];
        binary(&mut info, INFO, &info_body);
        let duration_in_info = info.len() - info_body.len() + duration_in_body;

        let mut track_bytes = vec![];
        element(&mut track_bytes, TRACKS, |b| {
            for (n, t) in tracks.iter().enumerate() {
                track_entry(b, n as u64 + 1, *t);
            }
        });

        // SeekHead: Info, Tracks, then Cues, whose position (its last 8
        // bytes) is filled in at the end. Positions are always 8 bytes, so
        // its length doesn't depend on them.
        let seek_head = |info_pos: u64, tracks_pos: u64| {
            let mut b = vec![];
            element(&mut b, SEEK_HEAD, |b| {
                seek_entry(b, INFO, info_pos);
                seek_entry(b, TRACKS, tracks_pos);
                seek_entry(b, CUES, 0);
            });
            b
        };
        let info_pos = seek_head(0, 0).len() as u64;
        let tracks_pos = info_pos + info.len() as u64;
        let seek = seek_head(info_pos, tracks_pos);
        let cues_seek_at = segment_data + seek.len() as u64 - 8;
        let duration_at = segment_data + info_pos + duration_in_info as u64;

        out.write_all(&head)?;
        out.write_all(&seek)?;
        out.write_all(&info)?;
        out.write_all(&track_bytes)?;
        Ok(Webm {
            out,
            tracks,
            segment_data,
            cues_seek_at,
            duration_at,
            cluster: None,
            cues: vec![],
            last_ms: 0,
        })
    }

    /// Writes a frame of track `track` (from 1) at `ms` milliseconds.
    pub fn frame(&mut self, track: u8, ms: u64, keyframe: bool, data: &[u8]) -> io::Result<()> {
        let video = self.tracks[usize::from(track - 1)].is_video();
        let new_cluster = match self.cluster {
            None => true,
            Some((_, start)) => {
                ms < start
                    || ms - start >= CLUSTER_SPAN_MS
                    || (video && keyframe)
                    || ms - start >= CLUSTER_MAX_MS
            }
        };
        if new_cluster {
            self.close_cluster()?;
            let pos = self.out.stream_position()?;
            let mut b = vec![];
            id(&mut b, CLUSTER);
            b.extend_from_slice(&UNKNOWN_SIZE);
            uint(&mut b, TIMESTAMP, ms);
            self.out.write_all(&b)?;
            self.cluster = Some((pos, ms));
            if video && keyframe {
                self.cues.push((ms, track, pos - self.segment_data));
            }
        }
        let (_, start) = self.cluster.expect("opened");
        let rel = (ms - start) as i16;
        let mut b = vec![];
        id(&mut b, SIMPLE_BLOCK);
        size(&mut b, 4 + data.len() as u64);
        b.push(0x80 | track);
        b.extend_from_slice(&rel.to_be_bytes());
        b.push(if keyframe { 0x80 } else { 0 });
        self.out.write_all(&b)?;
        self.out.write_all(data)?;
        self.last_ms = self.last_ms.max(ms);
        Ok(())
    }

    /// Writes the Cues and fills in the sizes, positions and `duration_ms`.
    pub fn finish(mut self, duration_ms: u64) -> io::Result<W> {
        self.close_cluster()?;
        let cues_pos = self.out.stream_position()?;
        if !self.cues.is_empty() {
            let mut b = vec![];
            element(&mut b, CUES, |b| {
                for &(ms, track, pos) in &self.cues {
                    element(b, CUE_POINT, |b| {
                        uint(b, CUE_TIME, ms);
                        element(b, CUE_TRACK_POSITIONS, |b| {
                            uint(b, CUE_TRACK, u64::from(track));
                            uint(b, CUE_CLUSTER_POSITION, pos);
                        });
                    });
                }
            });
            self.out.write_all(&b)?;
            self.out.seek(SeekFrom::Start(self.cues_seek_at))?;
            self.out
                .write_all(&(cues_pos - self.segment_data).to_be_bytes())?;
        }
        let end = self.out.seek(SeekFrom::End(0))?;
        self.out.seek(SeekFrom::Start(self.segment_data - 8))?;
        self.out.write_all(&size8(end - self.segment_data))?;
        self.out.seek(SeekFrom::Start(self.duration_at))?;
        self.out.write_all(&(duration_ms as f64).to_be_bytes())?;
        self.out.seek(SeekFrom::End(0))?;
        self.out.flush()?;
        Ok(self.out)
    }

    fn close_cluster(&mut self) -> io::Result<()> {
        let Some((pos, _)) = self.cluster.take() else {
            return Ok(());
        };
        let end = self.out.stream_position()?;
        // The size field follows the 4-byte id.
        let data = pos + 4 + 8;
        self.out.seek(SeekFrom::Start(pos + 4))?;
        self.out.write_all(&size8(end - data))?;
        self.out.seek(SeekFrom::Start(end))?;
        Ok(())
    }
}

fn track_entry(b: &mut Vec<u8>, number: u64, t: Track) {
    element(b, TRACK_ENTRY, |b| {
        uint(b, TRACK_NUMBER, number);
        uint(b, TRACK_UID, number);
        uint(b, FLAG_LACING, 0);
        match t {
            Track::Vp8 { width, height } | Track::Vp9 { width, height } => {
                uint(b, TRACK_TYPE, 1);
                let codec = if matches!(t, Track::Vp8 { .. }) {
                    "V_VP8"
                } else {
                    "V_VP9"
                };
                string(b, CODEC_ID, codec);
                element(b, VIDEO, |b| {
                    uint(b, PIXEL_WIDTH, u64::from(width));
                    uint(b, PIXEL_HEIGHT, u64::from(height));
                });
            }
            Track::Opus { channels } => {
                uint(b, TRACK_TYPE, 2);
                string(b, CODEC_ID, "A_OPUS");
                binary(b, CODEC_PRIVATE, &opus_head(channels));
                uint(
                    b,
                    CODEC_DELAY,
                    u64::from(OPUS_PRE_SKIP) * 1_000_000_000 / 48_000,
                );
                uint(b, SEEK_PRE_ROLL, OPUS_PRE_ROLL_NS);
                element(b, AUDIO, |b| {
                    float(b, SAMPLING_FREQUENCY, 48_000.0);
                    uint(b, CHANNELS, u64::from(channels));
                });
            }
        }
    });
}

fn seek_entry(b: &mut Vec<u8>, target: u32, pos: u64) {
    element(b, SEEK, |b| {
        let mut target_id = vec![];
        id(&mut target_id, target);
        binary(b, SEEK_ID, &target_id);
        id(b, SEEK_POSITION);
        size(b, 8);
        b.extend_from_slice(&pos.to_be_bytes());
    });
}

/// RFC 7845 §5.1, mapping family 0.
fn opus_head(channels: u8) -> Vec<u8> {
    let mut h = b"OpusHead".to_vec();
    h.push(1);
    h.push(channels);
    h.extend_from_slice(&OPUS_PRE_SKIP.to_le_bytes());
    h.extend_from_slice(&48_000u32.to_le_bytes());
    h.extend_from_slice(&0i16.to_le_bytes());
    h.push(0);
    h
}

/// An element id, as written (its length marker included).
fn id(b: &mut Vec<u8>, id: u32) {
    let bytes = id.to_be_bytes();
    let skip = bytes.iter().take_while(|&&x| x == 0).count();
    b.extend_from_slice(&bytes[skip..]);
}

/// A size as the shortest EBML variable-length integer.
fn size(b: &mut Vec<u8>, n: u64) {
    let len = size_len(n as usize);
    let marked = n | 1 << (7 * len);
    b.extend_from_slice(&marked.to_be_bytes()[8 - len..]);
}

fn size_len(n: usize) -> usize {
    // All ones is reserved (unknown size), so one value less per length.
    (1..=8)
        .find(|&l| (n as u64) < (1u64 << (7 * l)) - 1)
        .unwrap_or(8)
}

fn size8(n: u64) -> [u8; 8] {
    (n | 1 << 56).to_be_bytes()
}

fn element(b: &mut Vec<u8>, element_id: u32, body: impl FnOnce(&mut Vec<u8>)) {
    let mut inner = vec![];
    body(&mut inner);
    id(b, element_id);
    size(b, inner.len() as u64);
    b.extend_from_slice(&inner);
}

fn uint(b: &mut Vec<u8>, element_id: u32, v: u64) {
    let bytes = v.to_be_bytes();
    let skip = bytes.iter().take_while(|&&x| x == 0).count().min(7);
    binary(b, element_id, &bytes[skip..]);
}

fn float(b: &mut Vec<u8>, element_id: u32, v: f64) {
    binary(b, element_id, &v.to_be_bytes());
}

fn float_bytes(b: &mut Vec<u8>, v: f64) {
    b.extend_from_slice(&v.to_be_bytes());
}

fn string(b: &mut Vec<u8>, element_id: u32, s: &str) {
    binary(b, element_id, s.as_bytes());
}

fn binary(b: &mut Vec<u8>, element_id: u32, data: &[u8]) {
    id(b, element_id);
    size(b, data.len() as u64);
    b.extend_from_slice(data);
}
