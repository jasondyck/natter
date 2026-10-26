//! Track egress on the engine's side: a recorder per recorded track that
//! hands its packets to the writer thread (`crate::egress::track`). Video
//! follows the best simulcast layer from a keyframe, keeping one
//! continuous stream across switches as a DownTrack does; audio sent as
//! RED is recorded as its primary Opus.

use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

use livekit_protocol as lk;
use str0m::format::Codec;
use str0m::media::{MediaKind, Rid};
use str0m::rtp::RtpPacket;
use tracing::info;

use super::Engine;
use super::admin::{AdminError, AdminReply, AdminResult};
use crate::egress::END_API;
use crate::egress::track::{self, Packet, Tap};
use crate::livekit::video;

pub(super) struct Recorder {
    tap: Tap,
    /// Video: the layer followed (None until a keyframe of one).
    layer: Option<Option<Rid>>,
    seq_offset: u64,
    ts_offset: u32,
    last_out: Option<(u64, u32, Instant)>,
    /// The writer has gone (it failed).
    closed: bool,
}

impl Engine {
    /// StartTrackEgress, for a track in a room on this shard.
    pub(super) fn start_track_egress(&mut self, mut info: lk::EgressInfo) -> AdminResult {
        let Some(lk::egress_info::Request::Track(req)) = info.request.clone() else {
            return Err(AdminError::new("invalid_argument", "not a track egress"));
        };
        let room = self
            .rooms
            .get(req.room_name.as_str())
            .ok_or_else(|| AdminError::new("not_found", "requested room does not exist"))?;
        info.room_id = room.sid.clone();
        let (i, t) = room
            .members
            .iter()
            .find_map(|&i| {
                let p = &self.participants[i];
                p.published
                    .iter()
                    .position(|t| t.info.sid == req.track_id && t.backup_of.is_none())
                    .map(|t| (i, t))
            })
            .ok_or_else(|| AdminError::new("not_found", "track not found"))?;
        let p = &self.participants[i];
        let track = &p.published[t];
        let mime = track.info.mime_type.to_ascii_lowercase();
        let codec = match (track.kind, mime.as_str()) {
            (MediaKind::Audio, "audio/opus" | "audio/red") => track::Codec::Opus,
            (MediaKind::Video, "video/vp8") => track::Codec::Vp8,
            _ => {
                return Err(AdminError::new(
                    "unimplemented",
                    format!("natter can't record {mime} tracks yet"),
                ));
            }
        };
        let Some(lk::track_egress_request::Output::File(file)) = &req.output else {
            return Err(AdminError::new(
                "unimplemented",
                "natter records track egress to files only",
            ));
        };
        if file.output.is_some() {
            return Err(AdminError::new(
                "unimplemented",
                "natter records to its own disk only (no cloud upload)",
            ));
        }
        let kind = if track.kind == MediaKind::Audio {
            "audio"
        } else {
            "video"
        };
        let source = track.info.source().as_str_name().to_ascii_lowercase();
        let replacements = [
            ("{room_name}", req.room_name.as_str()),
            ("{room_id}", info.room_id.as_str()),
            ("{track_id}", req.track_id.as_str()),
            ("{track_type}", kind),
            ("{track_source}", source.as_str()),
            ("{publisher_identity}", p.identity.as_str()),
        ];
        let recordings = self.recordings.clone();
        let (path, filename) = recordings
            .file_path(
                &file.filepath,
                &req.track_id,
                &replacements,
                codec.extension(),
            )
            .map_err(|e| AdminError::new("invalid_argument", e))?;
        info.file_results = vec![lk::FileInfo {
            filename,
            location: path.display().to_string(),
            ..Default::default()
        }];
        let tap = track::start(recordings.clone(), info.clone(), codec, path)
            .map_err(|e| AdminError::new("internal", format!("could not create the file: {e}")))?;
        recordings.create(&info);
        info!(egress = %info.egress_id, track = %req.track_id, identity = %p.identity, "recording");
        let room_name = p.room.clone();
        self.participants[i].published[t].recorders.push(Recorder {
            tap,
            layer: None,
            seq_offset: 0,
            ts_offset: 0,
            last_out: None,
            closed: false,
        });
        self.recording_changed(&room_name);
        Ok(AdminReply::Egress(Box::new(info)))
    }

    /// StopEgress: lets go of the recorder (the writer then finishes).
    pub(super) fn stop_egress(&mut self, id: &str) -> AdminResult {
        for i in 0..self.participants.len() {
            for t in &mut self.participants[i].published {
                if let Some(k) = t.recorders.iter().position(|r| r.tap.egress_id == id) {
                    t.recorders.remove(k).tap.end(END_API);
                    let room = self.participants[i].room.clone();
                    self.recording_changed(&room);
                    return Ok(AdminReply::Done);
                }
            }
        }
        Err(AdminError::new("not_found", "egress not found"))
    }

    /// Whether any track in the room is being recorded, or a recorder has
    /// joined; tells everyone at once when that changes.
    pub(super) fn recording_changed(&mut self, room: &str) {
        let Some(r) = self.rooms.get(room) else {
            return;
        };
        let recording = r.members.iter().any(|&i| {
            let p = &self.participants[i];
            p.is_recorder() || p.published.iter().any(|t| !t.recorders.is_empty())
        });
        if r.recording != recording {
            let name = r.name.clone();
            self.rooms.get_mut(room).expect("checked").recording = recording;
            self.room_update_forced(&name);
        }
    }

    /// Rooms still marked recording whose recorded tracks have gone (with
    /// their publisher, or unpublished).
    pub(super) fn check_recording(&mut self) {
        let rooms: Vec<_> = self
            .rooms
            .values()
            .filter(|r| r.recording)
            .map(|r| r.name.clone())
            .collect();
        for room in rooms {
            self.recording_changed(&room);
        }
    }

    /// Audio to a track's recorders, RED as its primary.
    pub(super) fn record_audio(&mut self, i: usize, t: usize, pkt: &RtpPacket) {
        let track = &mut self.participants[i].published[t];
        let is_red = track.red_pt == Some(pkt.header.payload_type);
        let payload = if is_red {
            match crate::livekit::red::primary(&pkt.payload) {
                Some(p) => p.into(),
                None => return,
            }
        } else {
            std::sync::Arc::from(&pkt.payload[..])
        };
        let before = track.recorders.len();
        track.recorders.retain(|r| {
            r.tap.send(Packet {
                seq: *pkt.seq_no,
                ts: pkt.header.timestamp,
                marker: pkt.header.marker,
                payload: std::sync::Arc::clone(&payload),
            })
        });
        if track.recorders.len() != before {
            let room = self.participants[i].room.clone();
            self.recording_changed(&room);
        }
    }

    /// Video to a track's recorders: the best layer there is, switched to
    /// at its keyframes.
    pub(super) fn record_video(
        &mut self,
        i: usize,
        t: usize,
        rid: Option<Rid>,
        codec: Codec,
        pkt: &RtpPacket,
        now: Instant,
    ) {
        let track = &mut self.participants[i].published[t];
        let target = video::target_layer(&track.layers, video::ANY_LAYER, now);
        let keyframe = video::starts_keyframe(codec, &pkt.payload);
        let seq: u64 = *pkt.seq_no;
        let ts = pkt.header.timestamp;
        let mut want: Vec<Option<Rid>> = vec![];
        let mut gone = false;
        for r in &mut track.recorders {
            if r.layer != target {
                if target == Some(rid) && keyframe {
                    if let Some((out_seq, out_ts, at)) = r.last_out {
                        let ticks = now.saturating_duration_since(at).as_micros() * 90 / 1000;
                        r.seq_offset = out_seq.wrapping_add(1).wrapping_sub(seq);
                        r.ts_offset = out_ts.wrapping_add(ticks.max(1) as u32).wrapping_sub(ts);
                    }
                    r.layer = target;
                } else if let Some(target) = target
                    && !want.contains(&target)
                {
                    want.push(target);
                }
            }
            if r.layer != Some(rid) {
                continue;
            }
            if r.tap.want_keyframe.swap(false, Relaxed) && !want.contains(&rid) {
                want.push(rid);
            }
            let out_seq = seq.wrapping_add(r.seq_offset);
            let out_ts = ts.wrapping_add(r.ts_offset);
            r.last_out = Some((out_seq, out_ts, now));
            r.closed = !r.tap.send(Packet {
                seq: out_seq,
                ts: out_ts,
                marker: pkt.header.marker,
                payload: std::sync::Arc::from(&pkt.payload[..]),
            });
            gone |= r.closed;
        }
        if gone {
            track.recorders.retain(|r| !r.closed);
            let room = self.participants[i].room.clone();
            self.recording_changed(&room);
        }
        for layer in want {
            self.request_keyframe(i, t, layer, now);
        }
    }
}
