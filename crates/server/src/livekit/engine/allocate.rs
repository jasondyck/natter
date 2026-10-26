//! Which layer each subscriber gets (`allocator`), and dynacast: telling
//! publishers which layers anyone still wants.

use std::time::{Duration, Instant};

use livekit_protocol as lk;
use lk::signal_response;
use str0m::bwe::Bitrate;
use str0m::media::{MediaKind, Mid};
use tracing::debug;

use super::{DownState, Engine, StreamState, dynacast_codecs, source};
use crate::livekit::allocator;
use crate::livekit::cc_detect;
use crate::livekit::rtc_config::Congestion;
use crate::livekit::video;

/// A layer's bitrate up to each temporal layer it sends, `rate` in all
/// when its temporal layers aren't known (or measured yet).
fn temporal_rates(l: &video::Layer, rate: u32) -> [Option<u32>; allocator::TEMPORAL] {
    let mut rates = own_rates(l);
    if rates.iter().all(Option::is_none) {
        rates[0] = Some(rate.max(1));
    }
    rates
}

fn own_rates(l: &video::Layer) -> [Option<u32>; allocator::TEMPORAL] {
    l.temporal_rates.map(|r| (r > 0).then_some(r))
}

/// The rate up to the highest temporal layer below `tl` that is sent.
fn own_below(rates: &[Option<u32>; allocator::TEMPORAL], tl: usize) -> Option<u32> {
    rates[..tl].iter().rev().find_map(|r| *r)
}

impl Engine {
    /// Allocates every subscriber not allocated for `allocator::INTERVAL`.
    pub(super) fn allocate_due(&mut self, now: Instant) {
        for i in 0..self.participants.len() {
            if self.participants[i]
                .cc
                .allocated_at
                .is_none_or(|at| now.duration_since(at) >= allocator::INTERVAL)
            {
                self.allocate(i, now);
            }
        }
    }

    /// Congestion control for one subscriber: gives each video track it
    /// receives the layers its bandwidth estimate allows (`allocator`),
    /// pausing tracks nothing fits for if it allows that (with
    /// `stream_state_update`), and tells the estimator how much it would
    /// like to send, which it probes towards.
    pub(super) fn allocate(&mut self, i: usize, now: Instant) {
        let p = &self.participants[i];
        let Some(estimate) = p.cc.estimate.filter(|_| p.congestion != Congestion::Off) else {
            return;
        };
        let mut keys = vec![];
        let mut tracks = vec![];
        for (k, d) in p.downtracks.iter().enumerate() {
            if d.kind == MediaKind::Audio
                || !matches!(d.state, DownState::Open(_) | DownState::Negotiating(_))
            {
                continue;
            }
            let Some(max) = d.max_layer else {
                continue;
            };
            let Some(t) = self.by_conn.get(&d.publisher).and_then(|&pi| {
                let published = &self.participants[pi].published;
                source(published, &d.track_sid, d.cannot_primary).map(|k| &published[k])
            }) else {
                continue;
            };
            if t.info.muted {
                continue;
            }
            let mut rates = [[None; allocator::TEMPORAL]; allocator::SPATIAL];
            let mut max_temporal = [video::ANY_LAYER; allocator::SPATIAL];
            let mut fps = [[0.0; allocator::TEMPORAL]; allocator::SPATIAL];
            // SVC: a spatial layer costs itself and every layer below, each
            // up to the same temporal layer.
            if let Some(svc) = &t.svc {
                let mut sum = [0u32; allocator::TEMPORAL];
                for l in &svc.spatial {
                    if !l.live(now) {
                        break;
                    }
                    let s = usize::from(l.spatial);
                    let own = temporal_rates(l, l.bitrate);
                    for (tl, own) in own.iter().enumerate() {
                        // A layer below without this temporal layer: its
                        // highest below it.
                        let r = own.or_else(|| own_below(&own_rates(l), tl));
                        sum[tl] += r.unwrap_or(0);
                        rates[s][tl] = own.map(|_| sum[tl].max(1));
                    }
                    max_temporal[s] = video::temporal_for_fps(&l.fps, d.fps);
                    fps[s] = l.fps;
                }
            }
            for l in t.layers.iter().filter(|l| l.live(now) && t.svc.is_none()) {
                let s = l.spatial.min(2);
                let announced = t
                    .info
                    .layers
                    .iter()
                    .find(|x| x.spatial_layer == i32::from(s))
                    .map_or(0, |x| x.bitrate);
                let rate = allocator::layer_rate(l.bitrate, announced, s);
                rates[usize::from(s)] = temporal_rates(l, rate);
                max_temporal[usize::from(s)] = video::temporal_for_fps(&l.fps, d.fps);
                fps[usize::from(s)] = l.fps;
            }
            if rates.iter().flatten().all(Option::is_none) {
                continue;
            }
            let mut track = allocator::Track {
                screen: t.info.source == lk::TrackSource::ScreenShare as i32,
                max,
                max_temporal,
                rates,
            };
            track.shape(&fps, p.allow_pause);
            keys.push(k);
            tracks.push(track);
        }
        let p = &mut self.participants[i];
        let audio_bps = match p.cc.audio_sample.replace((p.cc.audio_bytes_out, now)) {
            Some((bytes, at)) if now > at => {
                let elapsed = now.duration_since(at).as_secs_f64();
                ((p.cc.audio_bytes_out - bytes) as f64 * 8.0 / elapsed) as u64
            }
            _ => 0,
        };
        if tracks.is_empty() {
            p.cc.allocated_at = Some(now);
            return;
        }
        let budget = estimate.saturating_sub(audio_bps);
        // Loss: NACKs from this subscriber on any of its video.
        let mids: Vec<Mid> = keys
            .iter()
            .filter_map(|&k| p.downtracks[k].state.mid())
            .collect();
        let t = sub!(p);
        let mut api = t.rtc.direct_api();
        // The round trip: the least of its streams' latest. A queue at the
        // bottleneck delays them all; one stream's report caught behind a
        // keyframe, or jitter, delays only that one.
        let (mut nacks, mut rtt) = (0u64, None::<Duration>);
        for &mid in &mids {
            if let Some(s) = api.stream_tx_by_mid(mid, None) {
                nacks += s.nacks_received();
                if let Some(r) = s.rtt() {
                    rtt = Some(rtt.map_or(r, |m: Duration| m.min(r)));
                }
            }
        }
        if nacks > p.cc.nacks_seen {
            p.cc.loss_at = Some(now);
        }
        p.cc.nacks_seen = nacks;
        if let Some(r) = rtt {
            p.cc.min_rtt = Some(p.cc.min_rtt.map_or(r, |m| m.min(r)));
        }
        // From TWCC feedback when there is any; else NACKs and the
        // receiver-report round trip.
        let evidence = p.cc.detector.evidence(now);
        let lossy = evidence.loss
            || p.cc
                .loss_at
                .is_some_and(|at| now.duration_since(at) < allocator::LOSS_WINDOW);
        let queueing = if p.cc.twcc_seen {
            evidence.queueing
        } else {
            rtt.zip(p.cc.min_rtt)
                .is_some_and(|(r, m)| r > m + allocator::QUEUEING)
        };
        let congestion = if lossy {
            allocator::Congestion::Loss
        } else if queueing {
            allocator::Congestion::Queueing
        } else {
            allocator::Congestion::None
        };
        // Upgrades wait for the last change to show in receiver reports.
        let hold = allocator::REPORT_INTERVAL + rtt.unwrap_or_default();
        // Congested: what the link delivers is its capacity; send less than
        // that so the queue drains.
        let budget = match evidence.delivered_bps {
            Some(d) if congestion != allocator::Congestion::None => {
                budget.min(((d as f64 * cc_detect::BACKOFF) as u64).saturating_sub(audio_bps))
            }
            _ => budget,
        };
        let alloc = allocator::allocate(budget, &tracks);
        // Upgrades: the peak estimate since the last congestion.
        if congestion != allocator::Congestion::None
            || p.cc.peak.is_none_or(|(b, at)| {
                estimate >= b || now.duration_since(at) > allocator::PEAK_WINDOW
            })
        {
            p.cc.peak = Some((estimate, now));
        }
        let peak = p.cc.peak.map_or(estimate, |(b, _)| b);
        let usage = audio_bps
            + keys
                .iter()
                .zip(&tracks)
                .map(|(&k, t)| {
                    allocator::usage_bps(t, p.downtracks[k].hold.applied.unwrap_or(None))
                })
                .sum::<u64>();
        // GCC goes below what is sent only on an overuse signal (while the
        // sender is application-limited it stays above): no upgrade then,
        // whatever the peak.
        let up = if estimate < usage {
            usage
        } else {
            allocator::upgrade_budget(usage, peak)
        };
        let alloc_up = allocator::allocate(up.saturating_sub(audio_bps), &tracks);
        // A step down with congestion before an upgrade has held for
        // `BACKOFF_RESET`: that upgrade failed (a small overload can take
        // seconds to fill a queue), so back off. One that has held that
        // long clears the backoff.
        let since_up = p.cc.upgraded_at.map(|at| now.duration_since(at));
        let stepping_down = congestion != allocator::Congestion::None
            && keys.iter().zip(&alloc).any(|(&k, a)| {
                p.downtracks[k]
                    .hold
                    .applied
                    .is_some_and(|applied| *a < applied)
            });
        // Congestion while probing past what is sent: the probe found the
        // link's limit, which counts as a failure too (LiveKit's
        // "congesting" probe).
        let failed_probe = p.cc.probing && congestion != allocator::Congestion::None;
        if failed_probe || stepping_down && since_up.is_some_and(|e| e < allocator::BACKOFF_RESET) {
            p.cc.backoff *= allocator::BACKOFF_FACTOR;
            p.cc.failed_at = Some(now);
            p.cc.upgraded_at = None;
            debug!(identity = %p.identity, backoff = p.cc.backoff, "upgrade backed off");
        } else if since_up.is_some_and(|e| e > allocator::BACKOFF_RESET) {
            p.cc.backoff = 1.0;
        }
        let wait = allocator::upgrade_wait(hold, p.cc.backoff);

        let p = &mut self.participants[i];
        let mut states = vec![];
        let mut may_upgrade = [p.cc.upgraded_at, p.cc.failed_at]
            .iter()
            .flatten()
            .all(|&at| now.duration_since(at) >= wait);
        for (((&k, t), a), a_up) in keys.iter().zip(&tracks).zip(alloc).zip(alloc_up) {
            // Without pausing, a track nothing fits for gets its lowest.
            let lowest = t.lowest();
            let floor =
                |a: Option<allocator::Pick>| a.or(if p.allow_pause { None } else { lowest });
            let d = &mut p.downtracks[k];
            let before = d.hold.applied.flatten();
            let was_paused = d.hold.applied == Some(None);
            let next = allocator::next_step(t, before).map(|(p, _)| p);
            let change = d.hold.update(
                floor(a),
                floor(a_up),
                next,
                may_upgrade,
                congestion,
                wait,
                now,
            );
            // The link is dropping packets: stop the old spatial layer now
            // rather than flood it until the lower one's keyframe; the
            // picture holds until then. (Fewer temporal layers take effect
            // at the next frame.)
            let spatial = |p: Option<allocator::Pick>| p.map(|p| p.spatial);
            if change == allocator::Change::Down
                && congestion == allocator::Congestion::Loss
                && spatial(d.hold.applied.flatten()) < spatial(before)
            {
                d.layer = None;
            }
            if change == allocator::Change::Up {
                may_upgrade = false;
                p.cc.upgraded_at = Some(now);
            }
            if change != allocator::Change::None {
                let paused = d.hold.applied == Some(None);
                let layer = d.hold.applied.flatten();
                debug!(identity = %p.identity, track = %d.track_sid, ?layer, ?change, estimate, ?congestion, "allocated");
                // ACTIVE comes when video flows again (`forward_video`).
                if paused && !was_paused && d.stream_state != StreamState::Paused {
                    d.stream_state = StreamState::Paused;
                    states.push(lk::StreamStateInfo {
                        participant_sid: d.publisher_sid.clone(),
                        track_sid: d.track_sid.clone(),
                        state: lk::StreamState::Paused as i32,
                    });
                }
            }
        }
        if !states.is_empty() {
            p.send(signal_response::Message::StreamStateUpdate(
                lk::StreamStateUpdate {
                    stream_states: states,
                },
            ));
        }
        let caps = keys
            .iter()
            .map(|&k| p.downtracks[k].hold.applied.unwrap_or(None));
        let mut usage = audio_bps;
        let mut wanted = audio_bps;
        let mut step: Option<u64> = None;
        for (t, cap) in tracks.iter().zip(caps) {
            usage += allocator::usage_bps(t, cap);
            wanted += allocator::wanted_bps(t);
            if let Some(next) = allocator::next_step_bps(t, cap) {
                step = Some(step.map_or(next, |s| s.min(next)));
            }
        }
        let started = *p.cc.video_since.get_or_insert(now);
        // Backed off after a failed upgrade: no probing either until the
        // wait is over (LiveKit's probe regulator spaces the probes; on a
        // nearly full link each probe is a queueing spike).
        let backed_off =
            p.cc.failed_at
                .is_some_and(|at| now.duration_since(at) < wait);
        let goal = if backed_off {
            usage
        } else if now.duration_since(started) < allocator::STARTUP_PROBING {
            wanted.max(allocator::probe_goal(usage, step))
        } else {
            allocator::probe_goal(usage, step)
        };
        p.cc.probing = goal > usage;
        if p.congestion == Congestion::Twcc {
            let side = p.sending_side();
            let t = sub!(p);
            t.rtc.bwe().set_desired_bitrate(Bitrate::bps(goal));
            t.dirty = true;
            self.touched.push((i, side));
        }
        self.participants[i].cc.allocated_at = Some(now);
    }

    /// Dynacast: tells each video publisher the highest quality anyone
    /// receives, so it stops encoding layers nobody watches (all of them
    /// when nobody does). Higher at once; lower once it has held for
    /// `video::DYNACAST_DOWN_DELAY`. Not while the track is muted.
    pub(super) fn dynacast(&mut self, now: Instant) {
        for i in 0..self.participants.len() {
            for t in 0..self.participants[i].published.len() {
                let track = &self.participants[i].published[t];
                if track.kind != MediaKind::Video
                    || track.mid.is_none()
                    || track.info.muted
                    || track.backup_of.is_some()
                {
                    continue;
                }
                // Per codec: the highest layer anyone taking it wants.
                let mut max: [Option<u8>; 2] = [None, None];
                for conn in &track.subscribers {
                    let Some(&j) = self.by_conn.get(conn) else {
                        continue;
                    };
                    let d = self.participants[j].downtracks.iter().find(|d| {
                        d.track_sid == track.info.sid
                            && !matches!(
                                d.state,
                                DownState::ToStop(_) | DownState::NegotiatingStop(_)
                            )
                    });
                    if let Some(d) = d {
                        let k = usize::from(d.cannot_primary || track.regressed);
                        max[k] = max[k].max(d.max_layer);
                    }
                }
                // A recording takes the best there is.
                if !track.recorders.is_empty() {
                    max[0] = max[0].max(Some(2));
                }
                let want = max.map(|m| m.map(|m| video::quality_of(&track.info.layers, m)));
                let p = &mut self.participants[i];
                let track = &mut p.published[t];
                // No backup codec offered: everyone is on the primary.
                let want = if track.info.codecs.len() > 1 {
                    want
                } else {
                    [want[0].max(want[1]), None]
                };
                let up = want.iter().zip(&track.dynacast).any(|(w, c)| w > c);
                let send = if up {
                    true
                } else if want != track.dynacast {
                    match track.dynacast_lower {
                        Some((w, at)) if w == want => now >= at,
                        _ => {
                            track.dynacast_lower = Some((want, now + video::DYNACAST_DOWN_DELAY));
                            false
                        }
                    }
                } else {
                    track.dynacast_lower = None;
                    false
                };
                if send {
                    debug!(identity = %p.identity, track = %track.info.sid, ?want, "dynacast");
                    track.dynacast = want;
                    track.dynacast_lower = None;
                    let update =
                        video::subscribed_quality(&track.info.sid, &dynacast_codecs(track));
                    p.send(signal_response::Message::SubscribedQualityUpdate(update));
                }
            }
        }
    }
}
