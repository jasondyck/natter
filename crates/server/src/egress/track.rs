//! One track's recording: a thread that takes the track's RTP from the
//! engine, puts it back in order, and writes it out (Opus to Ogg; VP8
//! frames, rebuilt from their packets, to WebM). The engine never waits
//! on it: packets come over a bounded channel, and a full channel loses
//! them like the network would.
//!
//! The egress ends when the engine drops its end of the channel (the
//! track or room has gone, or StopEgress): the file is finished and the
//! `EgressInfo` completed.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufWriter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use livekit_protocol as lk;
use lk::EgressStatus;
use tracing::{info, warn};

use super::ogg::OggOpus;
use super::webm::{self, Webm};
use super::{END_SOURCE_CLOSED, Recordings, STOPPED_BEFORE_STARTED, unix_nanos};

/// Packets in flight to the writer.
const QUEUE: usize = 2048;
/// How long a missing packet is waited for (a NACKed resend) before it is
/// counted lost. Chosen: a few round trips.
const REORDER_WAIT: Duration = Duration::from_millis(300);
/// Or how many packets may wait behind it.
const REORDER_MAX: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    Opus,
    Vp8,
}

impl Codec {
    pub fn extension(self) -> &'static str {
        match self {
            Codec::Opus => ".ogg",
            Codec::Vp8 => ".webm",
        }
    }
}

/// One RTP packet: its sequence number and timestamp as the engine keeps
/// them continuous across simulcast layer switches.
pub struct Packet {
    pub seq: u64,
    pub ts: u32,
    pub marker: bool,
    pub payload: Arc<[u8]>,
}

pub enum Msg {
    Packet(Packet),
    /// Why it ends, just before the engine lets go.
    End(&'static str),
}

/// The engine's end of a recording.
pub struct Tap {
    pub egress_id: String,
    tx: SyncSender<Msg>,
    /// The writer lost video and needs a keyframe to go on.
    pub want_keyframe: Arc<AtomicBool>,
}

impl Tap {
    /// False once the writer has gone (it failed): drop the tap.
    pub fn send(&self, p: Packet) -> bool {
        !matches!(
            self.tx.try_send(Msg::Packet(p)),
            Err(TrySendError::Disconnected(_))
        )
    }

    pub fn end(self, reason: &'static str) {
        let _ = self.tx.try_send(Msg::End(reason));
    }
}

/// Starts recording into `path`; `info` is the egress as created
/// (STARTING).
pub fn start(
    recordings: Arc<Recordings>,
    info: lk::EgressInfo,
    codec: Codec,
    path: std::path::PathBuf,
) -> io::Result<Tap> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let file = File::create(&path)?;
    let (tx, rx) = mpsc::sync_channel(QUEUE);
    let want_keyframe = Arc::new(AtomicBool::new(codec != Codec::Opus));
    let tap = Tap {
        egress_id: info.egress_id.clone(),
        tx,
        want_keyframe: Arc::clone(&want_keyframe),
    };
    std::thread::Builder::new()
        .name(format!("egress-{}", info.egress_id))
        .spawn(move || {
            let mut w = Writer {
                recordings,
                info,
                path,
                out: Some(Out::new(codec, file)),
                want_keyframe,
                reorder: BTreeMap::new(),
                next_seq: None,
            };
            w.run(rx);
        })?;
    Ok(tap)
}

enum Out {
    Ogg(OggOpus<BufWriter<File>>),
    /// The file, until the first keyframe gives the picture size.
    Vp8 {
        file: Option<BufWriter<File>>,
        webm: Option<Webm<BufWriter<File>>>,
        frame: Frame,
        /// Frames can be decoded: nothing lost since the last keyframe.
        intact: bool,
        first_ts: Option<u64>,
        last: Option<(u32, u64)>,
        last_ms: u64,
        frames: u64,
    },
}

#[derive(Default)]
struct Frame {
    ts: Option<u32>,
    data: Vec<u8>,
    keyframe: bool,
    size: Option<(u32, u32)>,
    /// It began at a partition head and nothing is missing.
    whole: bool,
}

impl Out {
    fn new(codec: Codec, file: File) -> Self {
        let file = BufWriter::new(file);
        match codec {
            Codec::Opus => Out::Ogg(OggOpus::new(
                file,
                u32::from_le_bytes(crate::util::random_bytes()),
            )),
            Codec::Vp8 => Out::Vp8 {
                file: Some(file),
                webm: None,
                frame: Frame::default(),
                intact: false,
                first_ts: None,
                last: None,
                last_ms: 0,
                frames: 0,
            },
        }
    }

    /// Whether anything has been written yet.
    fn started(&self) -> bool {
        match self {
            Out::Ogg(o) => o.duration() > Duration::ZERO,
            Out::Vp8 { webm, .. } => webm.is_some(),
        }
    }

    fn duration(&self) -> Duration {
        match self {
            Out::Ogg(o) => o.duration(),
            Out::Vp8 {
                last_ms, frames, ..
            } => {
                // The last frame lasts as long as the average one.
                let each = if *frames > 1 {
                    last_ms / (frames - 1)
                } else {
                    0
                };
                Duration::from_millis(last_ms + each)
            }
        }
    }

    fn finish(self) -> io::Result<()> {
        let duration = self.duration();
        match self {
            Out::Ogg(o) => {
                o.finish()?.into_inner().map_err(|e| e.into_error())?;
            }
            Out::Vp8 { webm: Some(w), .. } => {
                w.finish(duration.as_millis() as u64)?
                    .into_inner()
                    .map_err(|e| e.into_error())?;
            }
            Out::Vp8 { .. } => {}
        }
        Ok(())
    }
}

struct Writer {
    recordings: Arc<Recordings>,
    info: lk::EgressInfo,
    path: std::path::PathBuf,
    out: Option<Out>,
    want_keyframe: Arc<AtomicBool>,
    /// Packets waiting for an earlier one, with when they came.
    reorder: BTreeMap<u64, (Packet, Instant)>,
    next_seq: Option<u64>,
}

impl Writer {
    fn run(&mut self, rx: Receiver<Msg>) {
        let mut reason = END_SOURCE_CLOSED;
        let result = loop {
            match rx.recv_timeout(REORDER_WAIT / 3) {
                Ok(Msg::Packet(p)) => self.queue(p),
                Ok(Msg::End(r)) => reason = r,
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break Ok(()),
            }
            if let Err(e) = self.release(false) {
                break Err(e);
            }
        };
        let result = result.and_then(|()| self.release(true));
        self.end(reason, result);
    }

    fn queue(&mut self, p: Packet) {
        if self.next_seq.is_some_and(|n| p.seq < n) {
            return;
        }
        self.reorder.entry(p.seq).or_insert((p, Instant::now()));
    }

    /// Writes what is in order; skips a missing packet once it has been
    /// waited for long enough (or `all`, at the end).
    fn release(&mut self, all: bool) -> io::Result<()> {
        let now = Instant::now();
        loop {
            let full = self.reorder.len() > REORDER_MAX;
            let Some(entry) = self.reorder.first_entry() else {
                break;
            };
            let seq = *entry.key();
            let in_order = self.next_seq.is_none_or(|n| n == seq);
            let waited = now - entry.get().1 >= REORDER_WAIT;
            if !(in_order || all || waited || full) {
                break;
            }
            let lost = !in_order;
            let (p, _) = entry.remove();
            self.next_seq = Some(seq + 1);
            self.write(p, lost)?;
        }
        Ok(())
    }

    fn write(&mut self, p: Packet, lost_before: bool) -> io::Result<()> {
        let was_started = self.out.as_ref().is_some_and(Out::started);
        match self.out.as_mut().expect("writing") {
            Out::Ogg(o) => o.push(p.ts, &p.payload)?,
            Out::Vp8 {
                file,
                webm,
                frame,
                intact,
                first_ts,
                last,
                last_ms,
                frames,
            } => {
                if lost_before {
                    frame.whole = false;
                    *intact = false;
                }
                // A new timestamp: the last frame never got its marker.
                if frame.ts.is_some_and(|ts| ts != p.ts) {
                    *frame = Frame::default();
                }
                let Some(desc) = crate::livekit::video::vp8_descriptor(&p.payload) else {
                    return Ok(());
                };
                let Some(header) = crate::livekit::video::vp8_header_len(&p.payload) else {
                    return Ok(());
                };
                if frame.ts.is_none() {
                    frame.ts = Some(p.ts);
                    frame.whole = desc.start;
                    frame.keyframe = desc.start && str0m::format::detect_vp8_keyframe(&p.payload);
                    frame.size =
                        crate::livekit::video::frame_size(str0m::format::Codec::Vp8, &p.payload);
                }
                frame.data.extend_from_slice(&p.payload[header..]);
                if !p.marker {
                    return Ok(());
                }
                let done = std::mem::take(frame);
                if !done.whole {
                    *intact = false;
                } else if done.keyframe {
                    *intact = true;
                }
                if !*intact {
                    self.want_keyframe.store(true, Ordering::Relaxed);
                    return Ok(());
                }
                let ts = match *last {
                    None => u64::from(p.ts),
                    Some((rtp, at)) => at + u64::from(p.ts.wrapping_sub(rtp)),
                };
                *last = Some((p.ts, ts));
                let first = *first_ts.get_or_insert(ts);
                let ms = (ts - first) / 90;
                if webm.is_none() {
                    let (width, height) = done.size.unwrap_or((0, 0));
                    let track = webm::Track::Vp8 { width, height };
                    let f = file.take().expect("file until started");
                    *webm = Some(Webm::new(f, vec![track])?);
                }
                webm.as_mut()
                    .expect("started")
                    .frame(1, ms, done.keyframe, &done.data)?;
                *last_ms = ms;
                *frames += 1;
            }
        }
        if !was_started && self.out.as_ref().is_some_and(Out::started) {
            // Media is going into the file: ACTIVE, as LiveKit's egress
            // once its pipeline plays.
            let now = unix_nanos();
            for f in &mut self.info.file_results {
                f.started_at = now;
            }
            self.set_status(EgressStatus::EgressActive);
        }
        Ok(())
    }

    fn set_status(&mut self, status: EgressStatus) {
        self.info.status = status as i32;
        self.info.updated_at = unix_nanos();
        self.sync_file_result();
        self.recordings.update(&self.info);
    }

    /// As LiveKit's controller ending a pipeline: ENDING, then COMPLETE
    /// (or ABORTED if nothing was written, FAILED on an error).
    fn end(&mut self, reason: &'static str, result: io::Result<()>) {
        let out = self.out.take().expect("ends once");
        let started = out.started();
        let duration = out.duration();
        self.info.details = format!("End reason: {reason}");
        if started {
            self.set_status(EgressStatus::EgressEnding);
        }
        let result = result.and_then(|()| out.finish());
        let now = unix_nanos();
        self.info.ended_at = now;
        self.info.updated_at = now;
        match result {
            Err(e) => {
                warn!(egress = %self.info.egress_id, "recording failed: {e}");
                self.info.status = EgressStatus::EgressFailed as i32;
                self.info.error = e.to_string();
                self.info.details = "End reason: Failure".into();
            }
            Ok(()) if !started => {
                self.info.status = EgressStatus::EgressAborted as i32;
                self.info.error = STOPPED_BEFORE_STARTED.into();
                // http.StatusPreconditionFailed
                self.info.error_code = 412;
                let _ = std::fs::remove_file(&self.path);
            }
            Ok(()) => {
                self.info.status = EgressStatus::EgressComplete as i32;
                for f in &mut self.info.file_results {
                    f.ended_at = now;
                    f.duration = duration.as_nanos() as i64;
                }
            }
        }
        self.sync_file_result();
        info!(
            egress = %self.info.egress_id,
            status = ?EgressStatus::try_from(self.info.status),
            file = %self.path.display(),
            seconds = duration.as_secs_f32(),
            "recording ended"
        );
        self.recordings.update(&self.info);
    }

    /// Size on disk, and the deprecated `result` field LiveKit still fills.
    fn sync_file_result(&mut self) {
        let size = std::fs::metadata(&self.path).map_or(0, |m| m.len() as i64);
        for f in &mut self.info.file_results {
            f.size = size;
        }
        if let Some(f) = self.info.file_results.first() {
            #[allow(deprecated)]
            let result = lk::egress_info::Result::File(f.clone());
            self.info.result = Some(result);
        }
    }
}
