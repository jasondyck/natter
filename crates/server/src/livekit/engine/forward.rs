//! Forwarding RTP to subscribers: rewriting sequence numbers and
//! timestamps (LiveKit's DownTrack munger), simulcast and SVC layer
//! switching, and keyframe requests.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use livekit_protocol as lk;
use lk::signal_response;
use str0m::format::Codec;
use str0m::media::{KeyframeRequestKind, MediaKind, Rid};
use str0m::rtp::{ExtensionValues, RtpPacket};
use tracing::{debug, info, warn};

use super::{
    CODEC_REGRESSION_THRESHOLD, ConnId, DownState, Engine, Side, StreamState, Svc,
    fill_layer_ssrcs, sdk_name, set_layer_size, type_name, write,
};
use crate::livekit::allocator;
use crate::livekit::dd;
use crate::livekit::red::Unpacked;
use crate::livekit::rtc_config::Congestion;
use crate::livekit::video::{self, Layer};

/// How long a new subscriber's video waits on the lowest layer for its
/// first bandwidth estimate before congestion control is taken as absent.
/// Chosen, not derived.
const ESTIMATE_GRACE: Duration = Duration::from_secs(5);

impl Engine {
    /// Forwards one packet from a publisher to everyone subscribed.
    pub(super) fn forward(&mut self, i: usize, pkt: RtpPacket) {
        let now = Instant::now();
        let p = &mut self.participants[i];
        let ssrc = pkt.header.ssrc;
        let (t_index, rid) = match p.ssrc_track.iter().find(|(s, _, _)| *s == ssrc) {
            Some(&(_, t, rid)) => (t, rid),
            None => {
                let Some((mid, rid)) = p
                    .publisher
                    .rtc
                    .direct_api()
                    .stream_rx(&ssrc)
                    .map(|s| (s.mid(), s.rid()))
                else {
                    return;
                };
                let Some(t) = p.published.iter().position(|t| t.mid == Some(mid)) else {
                    return;
                };
                p.ssrc_track.push((ssrc, t, rid));
                fill_layer_ssrcs(p);
                (t, rid)
            }
        };
        self.metrics.packets_received_total.fetch_add(1, Relaxed);
        self.metrics
            .bytes_received_total
            .fetch_add(pkt.payload.len() as u64, Relaxed);
        if p.published[t_index].kind == MediaKind::Video {
            return self.forward_video(i, t_index, rid, pkt, now);
        }
        let track = &mut p.published[t_index];
        if let Some(level) = pkt.header.ext_vals.audio_level {
            self.metrics.audio_levels_total.fetch_add(1, Relaxed);
            track
                .level
                .observe(level.unsigned_abs().min(127), pkt.header.timestamp, now);
        }
        if !track.recorders.is_empty() {
            self.record_audio(i, t_index, &pkt);
        }
        let track = &mut self.participants[i].published[t_index];
        if track.subscribers.is_empty() {
            return;
        }

        let is_red = track.red_pt == Some(pkt.header.payload_type);
        let seq: u64 = *pkt.seq_no;
        let ts = pkt.header.timestamp;
        let abs_capture = pkt.header.ext_vals.abs_capture_time;
        let track_sid = track.info.sid.clone();
        let subscribers = track.subscribers.clone();
        let has_trailer = !track.info.packet_trailer_features.is_empty();

        // RED → Opus once for every subscriber that can't take RED.
        let needs_primary = is_red
            && subscribers.iter().any(|c| {
                self.by_conn.get(c).is_some_and(|&j| {
                    self.participants[j]
                        .downtracks
                        .iter()
                        .any(|d| d.track_sid == track_sid && d.red_pt.is_none())
                })
            });
        let p = &mut self.participants[i];
        let track = &mut p.published[t_index];
        let primaries: Vec<Unpacked> = if needs_primary {
            track.to_primary.unpack(seq as u16, &pkt.payload)
        } else {
            vec![]
        };

        for conn in subscribers {
            let Some(&j) = self.by_conn.get(&conn) else {
                continue;
            };
            let s = &mut self.participants[j];
            let strip_trailer = has_trailer && !s.takes_trailers();
            let Some(d) = s.downtracks.iter_mut().find(|d| d.track_sid == track_sid) else {
                continue;
            };
            let DownState::Open(mid) = d.state else {
                continue;
            };
            let ext = ExtensionValues {
                abs_capture_time: abs_capture,
                ..Default::default()
            };
            let mut written = 0u64;
            let mut bytes = 0u64;
            if is_red && d.red_pt.is_none() {
                let Some(opus) = d.opus_pt else { continue };
                for u in &primaries {
                    let seq = seq.wrapping_sub(u64::from(u.seq_back));
                    let ts = ts.wrapping_sub(u.ts_back);
                    let sent = write(
                        &mut sub!(s).rtc,
                        d,
                        &mut s.mid_sent,
                        mid,
                        opus,
                        seq,
                        ts,
                        &u.payload,
                        false,
                        ext.clone(),
                        now,
                    );
                    if sent.is_some() {
                        written += 1;
                        bytes += u.payload.len() as u64;
                    }
                }
            } else {
                let pt = if is_red { d.red_pt } else { d.opus_pt };
                let Some(pt) = pt else { continue };
                let stripped = strip_trailer
                    .then(|| without_trailer(&pkt.payload, pkt.header.marker))
                    .flatten();
                let payload = stripped.as_ref().unwrap_or(&pkt.payload);
                let sent = write(
                    &mut sub!(s).rtc,
                    d,
                    &mut s.mid_sent,
                    mid,
                    pt,
                    seq,
                    ts,
                    payload,
                    pkt.header.marker,
                    ext,
                    now,
                );
                if sent.is_some() {
                    written += 1;
                    bytes += pkt.payload.len() as u64;
                }
            }
            if written == 0 {
                continue;
            }
            s.cc.audio_bytes_out += bytes + written * allocator::PACKET_OVERHEAD;
            let first = !std::mem::replace(&mut d.told_publisher, true);
            self.forwarded(j, written, bytes, first.then_some(&track_sid));
        }
    }

    /// Counts packets forwarded to subscriber `j`, which then has output
    /// to send; `first` names the track on its first packet.
    #[inline]
    fn forwarded(&mut self, j: usize, packets: u64, bytes: u64, first: Option<&str>) {
        let t = sub!(self.participants[j]);
        t.dirty = true;
        t.wrote = true;
        self.touched.push((j, Side::Subscriber));
        self.metrics
            .packets_forwarded_total
            .fetch_add(packets, Relaxed);
        self.metrics.bytes_forwarded_total.fetch_add(bytes, Relaxed);
        if let Some(track_sid) = first {
            self.stream_started(j, track_sid);
        }
    }

    #[cold]
    fn stream_started(&mut self, j: usize, track_sid: &str) {
        let s = &self.participants[j];
        let Some(d) = s.downtracks.iter().find(|d| d.track_sid == track_sid) else {
            return;
        };
        self.metrics.lk.pubsub(
            "subscribe-stream-start",
            d.source.as_str_name(),
            type_name(d.kind),
            d.created_at.elapsed(),
            sdk_name(&s.client),
            s.kind.as_str_name(),
        );
        let publisher_conn = d.publisher;
        self.tell_subscribed(j, publisher_conn, track_sid);
    }

    /// The publisher learns its track has been subscribed, once per track
    /// and not by a hidden subscriber (LiveKit's `OnTrackEverSubscribed`).
    fn tell_subscribed(&mut self, j: usize, publisher: ConnId, track_sid: &str) {
        if self.participants[j].hidden() {
            return;
        }
        let Some(&pi) = self.by_conn.get(&publisher) else {
            return;
        };
        let p = &mut self.participants[pi];
        let Some(t) = p
            .published
            .iter_mut()
            .find(|t| t.info.sid == track_sid && t.backup_of.is_none())
        else {
            return;
        };
        if t.ever_subscribed {
            return;
        }
        t.ever_subscribed = true;
        p.send(signal_response::Message::TrackSubscribed(
            lk::TrackSubscribed {
                track_sid: track_sid.to_owned(),
            },
        ));
    }

    /// Forwards one video packet. Each subscriber follows one of the
    /// publisher's layers (`video::target_layer`: the highest live one its
    /// settings allow) and starts, or switches, only where that layer
    /// starts a keyframe, asking for one meanwhile. Nothing is forwarded
    /// while the publisher has the track muted.
    fn forward_video(
        &mut self,
        i: usize,
        t_index: usize,
        rid: Option<Rid>,
        pkt: RtpPacket,
        now: Instant,
    ) {
        let room_size = self
            .rooms
            .get(&self.participants[i].room)
            .map_or(0, |r| r.members.len());
        let p = &mut self.participants[i];
        let pub_pt = pkt.header.payload_type;
        let params = match p.published[t_index]
            .pts
            .iter()
            .find(|(pt, _)| *pt == pub_pt)
        {
            Some(&(_, params)) => params,
            None => {
                let rtc = &p.publisher.rtc;
                let Some(&params) = rtc
                    .codec_config()
                    .params()
                    .iter()
                    .find(|x| x.pt() == pub_pt)
                else {
                    return;
                };
                p.published[t_index].pts.push((pub_pt, params));
                params
            }
        };
        let codec = params.spec().codec;
        let keyframe = video::starts_keyframe(codec, &pkt.payload);
        let dd = pkt
            .header
            .ext_vals
            .user_values
            .get::<video::DependencyDescriptor>();
        let track = &mut p.published[t_index];
        let len = pkt.payload.len();
        // SVC: which layers this packet's frame belongs to, by the
        // dependency descriptor of the one stream.
        let svc_mode = video::is_svc(codec) && rid.is_none();
        let mut svc_frame = None;
        if svc_mode {
            let svc = track.svc.get_or_insert_with(|| Svc {
                dd: dd::Stream::default(),
                spatial: [0, 1, 2].map(|s| Layer::new(None, s, now)),
            });
            if let Some(f) = dd.and_then(|dd| svc.dd.parse(&dd.0)) {
                svc.spatial[usize::from(f.spatial.min(2))].count(len, f.temporal, f.start, now);
                svc_frame = svc.dd.structure.clone().map(|st| (f, st, svc.dd.active));
            }
        }
        let layer = match track.layers.iter().position(|l| l.rid == rid) {
            Some(k) => &mut track.layers[k],
            None => {
                let spatial = video::spatial(&track.info.layers, rid);
                track.layers.push(Layer::new(rid, spatial, now));
                track.layers.last_mut().expect("pushed")
            }
        };
        // A simulcast stream's temporal layers: by its dependency
        // descriptor (Chrome sends VP8 with one and no PictureID), else by
        // the VP8 payload descriptor, whose picture numbering subscribers
        // then see rewritten.
        let mut layer_frame = None;
        let mut vp8 = None;
        let (temporal, starts_frame) = if svc_mode {
            (0, false)
        } else if let Some(f) = dd.and_then(|dd| layer.dd.parse(&dd.0)) {
            let at = (f.temporal, f.start);
            layer_frame = layer
                .dd
                .structure
                .clone()
                .map(|st| (f, st, layer.dd.active));
            at
        } else {
            vp8 = (codec == Codec::Vp8)
                .then(|| video::vp8_descriptor(&pkt.payload))
                .flatten()
                .filter(|d| d.picture_id.is_some());
            vp8.map_or((0, false), |d| (d.tid.unwrap_or(0), d.start))
        };
        layer.count(len, temporal, starts_frame, now);
        let rates = layer.fps;
        let layer_spatial = layer.spatial;
        track.keyframe_seen |= keyframe;
        let resized = track.size_from_media
            && keyframe
            && video::frame_size(codec, &pkt.payload).is_some_and(|(w, h)| {
                let spatial = video::spatial(&track.info.layers, rid);
                set_layer_size(&mut track.info, spatial, w, h)
            });
        if resized {
            let room = p.room.clone();
            p.version += 1;
            info!(identity = %p.identity, track = %track.info.sid, "video size from media");
            self.participant_update(&room, i);
        }
        if !self.participants[i].published[t_index].recorders.is_empty() {
            self.record_video(i, t_index, rid, codec, &pkt, now);
        }
        let p = &mut self.participants[i];
        let track = &mut p.published[t_index];
        if track.info.muted {
            return;
        }
        // A backup codec's stream serves its primary's subscribers: those
        // that can't decode the primary, or all once the track regressed.
        // Its first packets regress the track in a small room, unless the
        // publisher asked for simulcast codecs.
        let backup_of = track.backup_of.clone();
        let (subscribers, regressed) = match &backup_of {
            None => (track.subscribers.clone(), track.regressed),
            Some(sid) => {
                let Some(primary) = p
                    .published
                    .iter_mut()
                    .find(|t| &t.info.sid == sid && t.backup_of.is_none())
                else {
                    return;
                };
                let policy = primary.info.backup_codec_policy;
                if !primary.regressed
                    && policy != lk::BackupCodecPolicy::Simulcast as i32
                    && room_size < CODEC_REGRESSION_THRESHOLD
                {
                    info!(track = %sid, "codec regression: everyone on the backup codec");
                    primary.regressed = true;
                }
                (primary.subscribers.clone(), primary.regressed)
            }
        };
        if subscribers.is_empty() {
            return;
        }
        let track = &mut p.published[t_index];
        let has_backup_codec = track.info.codecs.len() > 1;
        let has_trailer = !track.info.packet_trailer_features.is_empty();
        let track_sid = track.info.sid.clone();
        let svc_rates = track
            .svc
            .as_ref()
            .map(|s| s.spatial.each_ref().map(|l| l.fps));
        // The layer to follow for each highest layer a subscriber may want.
        let targets: [Option<Option<Rid>>; 4] =
            [0, 1, 2, video::ANY_LAYER].map(|max| video::target_layer(&track.layers, max, now));
        let seq: u64 = *pkt.seq_no;
        let ts = pkt.header.timestamp;
        let ext = ExtensionValues {
            abs_capture_time: pkt.header.ext_vals.abs_capture_time,
            ..Default::default()
        };
        // The dependency descriptor, rewritten per subscriber (its decode
        // targets and frame numbers), shared by those it comes out the same
        // for.
        let dd_in = dd.cloned();
        let dd_source: video::DdSource = (backup_of.is_some(), rid);
        let mut described: Vec<(u16, u32, Arc<[u8]>)> = vec![];
        let mut want_keyframe: Vec<Option<Rid>> = vec![];
        // VP8 payloads renumbered, shared by subscribers that see the same
        // numbers (and trailer or not).
        let mut renumbered: Vec<(video::Vp8Ids, bool, Arc<[u8]>)> = vec![];

        for conn in subscribers {
            let Some(&j) = self.by_conn.get(&conn) else {
                continue;
            };
            let s = &mut self.participants[j];
            let strip_trailer = has_trailer && !s.takes_trailers();
            let Some(d) = s.downtracks.iter_mut().find(|d| d.track_sid == track_sid) else {
                continue;
            };
            let DownState::Open(mid) = d.state else {
                continue;
            };
            // Only the stream of the codec it takes; moving to the other
            // starts again at a keyframe.
            if (d.cannot_primary || regressed) != backup_of.is_some() {
                continue;
            }
            if d.on_backup != backup_of.is_some() {
                d.on_backup = backup_of.is_some();
                d.layer = None;
                d.svc = video::SvcDown::default();
            }
            // Disabled, or paused for bandwidth: nothing, and a keyframe
            // to start again. Not allocated yet: the lowest layer while
            // congestion control is on (the top layers of every track at
            // once can overrun the link before the first estimate is used).
            // A client that never yields an estimate (no TWCC) gets what it
            // asks for once it has had time to.
            let estimating = s.congestion != Congestion::Off
                && (s.cc.estimate.is_some()
                    || s.joined_at.elapsed().is_ok_and(|e| e < ESTIMATE_GRACE));
            let unallocated = if estimating {
                allocator::Pick::new(0, video::ANY_LAYER)
            } else {
                allocator::Pick::new(video::ANY_LAYER, video::ANY_LAYER)
            };
            let (Some(max), Some(cap)) = (d.max_layer, d.hold.applied.unwrap_or(Some(unallocated)))
            else {
                if d.max_layer.is_none() {
                    d.stream_state = StreamState::Inactive;
                }
                d.layer = None;
                d.svc = video::SvcDown::default();
                continue;
            };
            let spatial_cap = max.min(cap.spatial);
            let target = targets[usize::from(spatial_cap.min(3))];
            // Temporal layers: what its frame rate setting allows, and
            // congestion control's cap where it forwards the capped spatial
            // layer (a lower one gets all of its own).
            let temporal_cap = |spatial: u8| {
                if spatial < cap.spatial {
                    video::ANY_LAYER
                } else {
                    cap.temporal
                }
            };
            let max_temporal =
                video::temporal_for_fps(&rates, d.fps).min(temporal_cap(layer_spatial));
            // Forwarding starts here from nothing (LiveKit's `IsResuming`).
            let mut layer_started = false;
            if d.layer != target {
                if let Some(target) = target
                    && !want_keyframe.contains(&target)
                {
                    want_keyframe.push(target);
                }
                if target == Some(rid) && keyframe {
                    debug!(identity = %s.identity, track = %track_sid, ?rid, temporal = if layer_frame.is_some() { "dd" } else if vp8.is_some() { "vp8" } else { "-" }, "following layer");
                    layer_started = d.layer.is_none();
                    d.layer = Some(rid);
                    d.svc = video::SvcDown::default();
                    if let Some(v) = &vp8 {
                        d.vp8.restart(v, max_temporal);
                    }
                    // Carry on the outgoing stream from the old layer's
                    // last packet, as if this one came next.
                    if let Some((out_seq, out_ts, at)) = d.last_out {
                        let ticks = now.saturating_duration_since(at).as_micros() * 90 / 1000;
                        d.seq_offset = Some(out_seq.wrapping_add(1).wrapping_sub(seq));
                        d.ts_offset = out_ts.wrapping_add(ticks.max(1) as u32).wrapping_sub(ts);
                    }
                }
            }
            if d.layer != Some(rid) {
                continue;
            }
            // SVC: only the frames of its decode target, the marker at the
            // end of its top spatial layer, and the sequence numbers kept
            // contiguous over what is dropped.
            let mut marker = pkt.header.marker;
            let mut dd_ext = None;
            if let Some((f, st, active)) = svc_frame.as_ref().or(layer_frame.as_ref()) {
                // A simulcast stream's are its temporal layers only.
                let limit = match svc_rates {
                    Some(r) if svc_frame.is_some() => {
                        let spatial = spatial_cap.min(2);
                        let rates = &r[usize::from(spatial)];
                        let fps = video::temporal_for_fps(rates, d.fps);
                        (spatial, fps.min(temporal_cap(spatial_cap)))
                    }
                    _ => (video::ANY_LAYER, max_temporal),
                };
                let before = d.svc.dt;
                let c = video::svc_select(
                    st,
                    *active,
                    f,
                    keyframe,
                    pkt.header.marker,
                    limit,
                    &mut d.svc,
                );
                if d.svc.dt != before {
                    debug!(identity = %s.identity, track = %track_sid, from = ?before.map(|dt| st.dt_layers[dt]), to = ?d.svc.dt.map(|dt| st.dt_layers[dt]), frame = f.number, keyframe, dti = ?d.svc.dt.and_then(|dt| f.dtis.get(dt)), active = format!("{active:b}"), "svc switch");
                }
                if c.want_keyframe && !want_keyframe.contains(&rid) {
                    want_keyframe.push(rid);
                }
                match c.forward {
                    Some(m) => marker = m,
                    None => {
                        if let Some(o) = d.seq_offset {
                            d.seq_offset = Some(o.wrapping_sub(1));
                        }
                        continue;
                    }
                }
                // Its decode target and those below it, of what the
                // publisher has active (LiveKit's `DDForwarder`).
                if let Some(data) = &dd_in {
                    if let Some(&(_, last)) = s.mid_frames.iter().find(|(m, _)| *m == mid) {
                        d.dd_out.seed(last);
                    }
                    let mask = d.svc.dt.map_or(*active, |dt| st.at_or_below(dt) & *active);
                    if let Some(number) = d.dd_out.number(dd_source, f) {
                        let k = match described.iter().position(|x| x.0 == number && x.1 == mask) {
                            Some(k) => k,
                            None => {
                                let out = dd::rewrite(&data.0, f, st.dt_cnt(), number, mask);
                                described.push((number, mask, out.into()));
                                described.len() - 1
                            }
                        };
                        dd_ext = Some(video::DependencyDescriptor(described[k].2.clone()));
                        if let Some(last) = d.dd_out.last {
                            match s.mid_frames.iter_mut().find(|(m, _)| *m == mid) {
                                Some(x) => x.1 = x.1.max(last),
                                None => s.mid_frames.push((mid, last)),
                            }
                        }
                    }
                }
            }
            // VP8: only the pictures of its temporal layers, renumbered
            // without the others, and the sequence numbers kept contiguous.
            let mut ids = None;
            if let Some(v) = &vp8 {
                match d.vp8.select(v, keyframe, pkt.header.marker, max_temporal) {
                    Some(out) => ids = (out != v.ids()).then_some(out),
                    None => {
                        if let Some(o) = d.seq_offset {
                            d.seq_offset = Some(o.wrapping_sub(1));
                        }
                        continue;
                    }
                }
            }
            let sub_pt = match d.video_pts.iter().find(|(pt, _)| *pt == pub_pt) {
                Some(&(_, pt)) => pt,
                None => {
                    let rtc = &sub!(s).rtc;
                    let pt = rtc.media(mid).and_then(|m| {
                        m.remote_pts().iter().copied().find(|&pt| {
                            rtc.codec_config()
                                .params()
                                .iter()
                                .any(|x| x.pt() == pt && video::compatible(&params, x))
                        })
                    });
                    if pt.is_none() {
                        // The primary codec: take the backup instead, which
                        // dynacast now asks the publisher for.
                        if backup_of.is_none() && has_backup_codec {
                            info!(identity = %s.identity, track = %track_sid, ?codec, "subscriber takes the backup codec");
                            d.cannot_primary = true;
                            d.layer = None;
                            continue;
                        }
                        warn!(identity = %s.identity, track = %track_sid, ?codec, "subscriber can't receive this codec");
                    }
                    d.video_pts.push((pub_pt, pt));
                    pt
                }
            };
            let Some(sub_pt) = sub_pt else {
                continue;
            };
            // The trailer ends a frame: the marker, or the end of a layer's
            // frame in SVC (LiveKit's `isEndOfLayerFrame`).
            let end_of_frame = pkt.header.marker || svc_frame.as_ref().is_some_and(|(f, ..)| f.end);
            let stripped = strip_trailer
                .then(|| without_trailer(&pkt.payload, end_of_frame))
                .flatten();
            let mut payload = stripped.as_ref().unwrap_or(&pkt.payload);
            if let (Some(ids), Some(v)) = (ids, &vp8) {
                let trailer = stripped.is_some();
                let k = match renumbered.iter().position(|r| r.0 == ids && r.1 == trailer) {
                    Some(k) => k,
                    None => {
                        renumbered.push((ids, trailer, video::vp8_rewrite(payload, v, ids)));
                        renumbered.len() - 1
                    }
                };
                payload = &renumbered[k].2;
            }
            let sent = write(
                &mut sub!(s).rtc,
                d,
                &mut s.mid_sent,
                mid,
                sub_pt,
                seq,
                ts,
                payload,
                marker,
                {
                    let mut ext = ext.clone();
                    if let Some(dd) = dd_ext {
                        ext.user_values.set(dd);
                    }
                    ext
                },
                now,
            );
            let Some((out_seq, out_ts)) = sent else {
                debug!(identity = %s.identity, %mid, "no stream to send video on");
                continue;
            };
            d.last_out = Some((out_seq, out_ts, now));
            let resumed = (layer_started && d.stream_state != StreamState::Active).then(|| {
                d.stream_state = StreamState::Active;
                lk::StreamStateInfo {
                    participant_sid: d.publisher_sid.clone(),
                    track_sid: d.track_sid.clone(),
                    state: lk::StreamState::Active as i32,
                }
            });
            let first = !std::mem::replace(&mut d.told_publisher, true);
            if let Some(info) = resumed {
                s.send(signal_response::Message::StreamStateUpdate(
                    lk::StreamStateUpdate {
                        stream_states: vec![info],
                    },
                ));
            }
            self.forwarded(j, 1, pkt.payload.len() as u64, first.then_some(&track_sid));
        }
        for layer in want_keyframe {
            self.request_keyframe(i, t_index, layer, now);
        }
    }

    /// Asks a publisher for a keyframe (PLI) on one layer of a track, at
    /// most every `video::PLI_THROTTLE`, as LiveKit only ever sends PLI
    /// upstream.
    pub(super) fn request_keyframe(
        &mut self,
        i: usize,
        t_index: usize,
        rid: Option<Rid>,
        now: Instant,
    ) {
        let p = &mut self.participants[i];
        let Some(track) = p.published.get_mut(t_index) else {
            return;
        };
        let Some(mid) = track.mid else {
            return;
        };
        if let Some(layer) = track.layers.iter_mut().find(|l| l.rid == rid) {
            if layer
                .last_pli
                .is_some_and(|at| now.duration_since(at) < video::PLI_THROTTLE)
            {
                return;
            }
            layer.last_pli = Some(now);
        }
        let mut api = p.publisher.rtc.direct_api();
        let Some(stream) = api.stream_rx_by_mid(mid, rid) else {
            return;
        };
        stream.request_keyframe(KeyframeRequestKind::Pli);
        p.publisher.dirty = true;
        self.touched.push((i, Side::Publisher));
    }

    /// Until a new video track's first keyframe, asks for one every
    /// `video::SEED_EVERY` for up to `video::SEED_FOR`.
    pub(super) fn seed_keyframes(&mut self, now: Instant) {
        let mut asks = vec![];
        for (i, p) in self.participants.iter_mut().enumerate() {
            for (t, track) in p.published.iter_mut().enumerate() {
                if track.kind != MediaKind::Video
                    || track.keyframe_seen
                    || now.duration_since(track.bound_at) > video::SEED_FOR
                    || track
                        .seeded_at
                        .is_some_and(|at| now.duration_since(at) < video::SEED_EVERY)
                {
                    continue;
                }
                track.seeded_at = Some(now);
                if track.layers.is_empty() {
                    asks.push((i, t, None));
                }
                asks.extend(track.layers.iter().map(|l| (i, t, l.rid)));
            }
        }
        for (i, t, rid) in asks {
            self.request_keyframe(i, t, rid, now);
        }
    }
}

/// LiveKit's packet trailer (`packettrailer.StripTrailer`): at the end of
/// a frame, `…trailer, len ^ 0xFF, "LKTS"`, where len covers the whole
/// trailer. The payload without it, for subscribers that can't take it
/// (None: there is none to strip).
fn without_trailer(payload: &[u8], end_of_frame: bool) -> Option<Arc<[u8]>> {
    const ENVELOPE: usize = 5;
    if !end_of_frame || payload.len() < ENVELOPE || !payload.ends_with(b"LKTS") {
        return None;
    }
    let len = usize::from(payload[payload.len() - 5] ^ 0xFF);
    if len < ENVELOPE || len > payload.len() {
        return None;
    }
    Some(payload[..payload.len() - len].into())
}
