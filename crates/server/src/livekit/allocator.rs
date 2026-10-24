//! Congestion control's allocation step: which spatial and temporal layer
//! of each video track a subscriber gets, given the bandwidth estimate for
//! its link.
//!
//! As LiveKit's stream allocator allocates all tracks
//! (`docs/livekit/media.md` B.1): layer by layer, spatial then temporal
//! (every track's lowest spatial layer at each frame rate before anyone's
//! next spatial layer), screen shares first, then tracks wanting more
//! before tracks wanting less. A track nothing fits for gets no layer: it
//! is paused if the subscriber allows that, else it gets its lowest layer
//! anyway.

use std::time::{Duration, Instant};

/// Bandwidth estimators run until they have an estimate on this (LiveKit's
/// pion GCC starts at 1 Mbit/s).
pub const INITIAL_ESTIMATE_BPS: u64 = 1_000_000;
/// Allocations are recomputed at least this often: every housekeeping
/// pass, so loss (a NACK) is acted on within 100 ms of arriving; estimate
/// drops also trigger one at once.
pub const INTERVAL: Duration = Duration::from_millis(100);
/// The RTCP receiver-report interval (`rtc_config::RTCP_INTERVAL`): how
/// often loss and round trip are reported. A higher layer is given only
/// once the estimate has allowed it for this plus a round trip, the time
/// the last change takes to show up in those reports; and one subscriber's
/// tracks go up one layer per such hold (LiveKit's boost), since a switch
/// costs a keyframe and switching every track at once is a burst.
pub const REPORT_INTERVAL: Duration = Duration::from_secs(1);
/// For this long after a subscriber's first video, the estimator is asked
/// for everything its tracks want, so its start-of-call probing (3× and 6×
/// the initial estimate, as libwebrtc's) isn't capped at the next step.
/// Chosen, not derived.
pub const STARTUP_PROBING: Duration = Duration::from_secs(5);
/// Bytes each packet carries beyond its payload on the wire: RTP header (12),
/// header extensions (~16), SRTP tag (10), UDP and IPv4 headers (28). Layer
/// and audio rates count it, as the link carries it: without it the
/// allocator saw 6% less than was sent and took steps that didn't fit.
pub const PACKET_OVERHEAD: u64 = 66;

/// After an upgrade that brought congestion (a step down within one hold of
/// it), the next upgrade waits this much longer, up to `BACKOFF_MAX`; an
/// upgrade that has held for `BACKOFF_RESET` clears it. LiveKit's probe
/// regulator: `backoff_factor` 1.5, `max_interval` 2 min.
pub const BACKOFF_FACTOR: f64 = 1.5;
pub const BACKOFF_MAX: Duration = Duration::from_secs(120);
/// Chosen, not derived: ten report intervals.
pub const BACKOFF_RESET: Duration = Duration::from_secs(10);

/// A retry after a failed upgrade or probe waits at least this, times the
/// backoff (LiveKit's probe regulator `base_interval`).
pub const BACKOFF_BASE: Duration = Duration::from_secs(3);

/// The wait before a subscriber's next upgrade: `base` (report interval
/// plus round trip) while nothing has failed; after failures,
/// `BACKOFF_BASE` (or `base`, if longer) times the backoff, within
/// `BACKOFF_MAX`.
pub fn upgrade_wait(base: Duration, backoff: f64) -> Duration {
    if backoff <= 1.0 {
        return base;
    }
    base.max(BACKOFF_BASE).mul_f64(backoff).min(BACKOFF_MAX)
}

/// Upgrades are judged against the highest estimate within this window
/// since the last congestion, not the current one: GCC, as libwebrtc's,
/// keeps its estimate within 1.5 × acknowledged throughput while the sender
/// is application-limited, so after a probe shows more room the estimate
/// sinks back below the next step before any hold is over. LiveKit commits
/// a successful probe's result as capacity likewise. `BACKOFF_RESET`.
pub const PEAK_WINDOW: Duration = BACKOFF_RESET;

/// Loss (a NACK) reported this recently counts as current: one report
/// interval. Chosen, not derived.
pub const LOSS_WINDOW: Duration = Duration::from_secs(1);
/// A round trip this far above the least seen means a queue is building
/// (LiveKit's send-side detector: `jqr_min_delay` 50 ms).
pub const QUEUEING: Duration = Duration::from_millis(50);
/// Probes reach this far past the next step's cost before it is taken, and
/// at least `PROBE_MIN_BPS` (LiveKit: `probe_overage_pct` 120,
/// `probe_min_bps` 200k).
pub const PROBE_OVERAGE: f64 = 1.2;
pub const PROBE_MIN_BPS: u64 = 200_000;
/// A layer's bitrate before it has been measured or announced, by spatial
/// index (LiveKit's camera presets at 720p).
const DEFAULT_LAYER_BPS: [u32; 3] = [150_000, 500_000, 1_700_000];

/// A spatial and temporal layer, ordered spatial first: every frame rate
/// of a spatial layer comes below the next spatial layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pick {
    pub spatial: u8,
    pub temporal: u8,
}

impl Pick {
    pub const fn new(spatial: u8, temporal: u8) -> Self {
        Pick { spatial, temporal }
    }
}

/// A camera's higher spatial layers keep temporal layers with at least
/// half the frame rate of all of them (within LiveKit's 90% tolerance).
const HALF_FRAME_RATE: f32 = 0.5 * 0.9;

/// Spatial and temporal layers told apart.
pub const SPATIAL: usize = 3;
pub const TEMPORAL: usize = crate::livekit::video::TEMPORAL_LAYERS;

/// One video track as the allocator sees it.
#[derive(Debug, Clone)]
pub struct Track {
    /// Screen shares are allocated before cameras.
    pub screen: bool,
    /// The highest spatial layer the subscriber's settings allow, and per
    /// spatial layer the highest temporal layer (its frame rate setting).
    pub max: u8,
    pub max_temporal: [u8; SPATIAL],
    /// What the publisher sends now: each spatial layer up to each
    /// temporal layer (cumulative), None for layers it doesn't send. A
    /// stream without temporal layers has only temporal layer 0.
    pub rates: [[Option<u32>; TEMPORAL]; SPATIAL],
}

impl Track {
    /// Its layers within its settings, lowest first.
    fn layers(&self) -> impl Iterator<Item = (Pick, u32)> + '_ {
        (0..SPATIAL as u8).flat_map(move |s| {
            (0..TEMPORAL as u8).filter_map(move |t| {
                let rate = self.rates[usize::from(s)][usize::from(t)]?;
                Some((Pick::new(s, t), rate))
            })
        })
    }

    fn allowed(&self, p: Pick) -> bool {
        p.spatial <= self.max && p.temporal <= self.max_temporal[usize::from(p.spatial)]
    }

    /// Its lowest layer.
    pub fn lowest(&self) -> Option<Pick> {
        self.layers().next().map(|(p, _)| p)
    }

    /// Takes out the layers it isn't to be given, by how people judge video
    /// on a call (`docs/livekit/PARITY.md`, "Status"), from each layer's
    /// frame rates up to each temporal layer (0: not measured):
    /// - Without pausing, the lowest spatial layer keeps all the frames it
    ///   may have. Fewer are worth it only where the alternative is a
    ///   pause; and sending less leaves GCC to recover from a loss as
    ///   slowly as the little acknowledged allows (str0m probes only while
    ///   application-limited): it held everyone at 180p and 5 fps.
    /// - A camera's higher spatial layers keep at least half their frame
    ///   rate (unless its frame rate setting asks for fewer): faces stay
    ///   acceptable at 10-15 fps when sharper, but below about 10 lips
    ///   and fingerspelling stop reading.
    /// - A screen share's lower spatial layers come only at their fewest
    ///   frames, so it gains resolution before frames (text needs it; as
    ///   LiveKit's SDKs, which keep a screen share's resolution).
    pub fn shape(&mut self, fps: &[[f32; TEMPORAL]; SPATIAL], allow_pause: bool) {
        let sent = |r: &[Option<u32>; TEMPORAL]| r.iter().any(Option::is_some);
        let Some(lowest) = self.rates.iter().position(sent) else {
            return;
        };
        let highest = self.rates.iter().rposition(sent).unwrap_or(lowest);
        for (s, f) in fps.iter().enumerate().take(highest + 1).skip(lowest) {
            let rates = self.rates[s];
            let Some(top) = rates.iter().rposition(Option::is_some) else {
                continue;
            };
            let present = |t: usize| rates[t].is_some();
            let fewest = (0..=top).find(|&t| present(t)).unwrap_or(top);
            // The most it may have: its frame rate setting's layer.
            let wanted = (0..=top.min(usize::from(self.max_temporal[s])))
                .rev()
                .find(|&t| present(t))
                .unwrap_or(fewest);
            let keep = |t: usize| -> bool {
                if s == lowest && !allow_pause {
                    t == wanted
                } else if self.screen {
                    s == highest || t == fewest
                } else if s == lowest {
                    true
                } else {
                    // Unmeasured: the top two.
                    let measured = f[top] > 0.0;
                    let half = (0..=top)
                        .find(|&t| {
                            present(t)
                                && if measured {
                                    f[t] >= f[top] * HALF_FRAME_RATE
                                } else {
                                    t + 1 >= top
                                }
                        })
                        .unwrap_or(top);
                    t >= half.min(wanted)
                }
            };
            for (t, r) in self.rates[s].iter_mut().enumerate() {
                if !keep(t) {
                    *r = None;
                }
            }
        }
    }

    fn rate(&self, p: Pick) -> Option<u32> {
        self.rates
            .get(usize::from(p.spatial))?
            .get(usize::from(p.temporal))
            .copied()
            .flatten()
    }
}

/// The bitrate to assume for a layer: measured, else what the publisher
/// announced, else LiveKit's preset.
pub fn layer_rate(measured: u32, announced: u32, spatial: u8) -> u32 {
    if measured > 0 {
        measured
    } else if announced > 0 {
        announced
    } else {
        DEFAULT_LAYER_BPS[usize::from(spatial.min(2))]
    }
}

/// The highest layer each track gets within `budget_bps`, or None when not
/// even its lowest fits. Each step up costs more than the one before.
pub fn allocate(budget_bps: u64, tracks: &[Track]) -> Vec<Option<Pick>> {
    let mut order: Vec<usize> = (0..tracks.len()).collect();
    order.sort_by_key(|&k| (!tracks[k].screen, std::cmp::Reverse(tracks[k].max)));
    let mut alloc: Vec<Option<Pick>> = vec![None; tracks.len()];
    let mut left = budget_bps;
    for spatial in 0..SPATIAL as u8 {
        for temporal in 0..TEMPORAL as u8 {
            let pick = Pick::new(spatial, temporal);
            for &k in &order {
                let t = &tracks[k];
                let Some(rate) = t.rate(pick) else {
                    continue;
                };
                if !t.allowed(pick) {
                    continue;
                }
                // Only steps that cost more: a higher spatial layer at a
                // lower frame rate can cost less than a lower one at all of
                // them, and isn't better for it (LiveKit takes it).
                let held = alloc[k].and_then(|a| t.rate(a));
                if held.is_some_and(|h| rate <= h) {
                    continue;
                }
                let cost = u64::from(rate) - u64::from(held.unwrap_or(0));
                if cost <= left {
                    left -= cost;
                    alloc[k] = Some(pick);
                }
            }
        }
    }
    alloc
}

/// The layer a track is forwarded at a cap: its highest layer up to the cap
/// (a higher spatial layer's frame rates come above every lower one's)
/// within its frame rate setting, else its lowest (the forwarder's choice).
pub fn forwarded(t: &Track, cap: Pick) -> Option<(Pick, u32)> {
    t.layers()
        .filter(|&(p, _)| p <= cap && p.temporal <= t.max_temporal[usize::from(p.spatial)])
        .last()
        .or_else(|| t.layers().next())
}

/// What a track sends at the most its settings allow.
pub fn wanted_bps(t: &Track) -> u64 {
    t.layers()
        .filter(|&(p, _)| t.allowed(p))
        .last()
        .map_or(0, |(_, r)| u64::from(r))
}

/// What a track sends at a cap (None: paused).
pub fn usage_bps(t: &Track, cap: Option<Pick>) -> u64 {
    cap.and_then(|c| forwarded(t, c))
        .map_or(0, |(_, rate)| u64::from(rate))
}

/// A track's next layer up from a cap, within its settings, and what it
/// costs on top.
pub fn next_step(t: &Track, cap: Option<Pick>) -> Option<(Pick, u64)> {
    let now = usage_bps(t, cap);
    let (at, _) = cap
        .and_then(|c| forwarded(t, c))
        .map_or((None, 0), |(p, r)| (Some(p), r));
    t.layers()
        .filter(|&(p, _)| t.allowed(p) && at.is_none_or(|a| p > a) && cap.is_none_or(|c| p > c))
        .find(|&(_, r)| u64::from(r) > now)
        .map(|(p, r)| (p, u64::from(r) - now))
}

/// The cost of a track's next step up, if it is below what it wants.
pub fn next_step_bps(t: &Track, cap: Option<Pick>) -> Option<u64> {
    next_step(t, cap).map(|(_, d)| d)
}

/// What to ask the bandwidth estimator for, which it probes towards with
/// padding: what is sent now, plus, if a track is below what it wants,
/// the cheapest next step with LiveKit's overage. Asking for everything
/// wanted instead probes far past a thin link, and the probes' bursts cost
/// more loss than they find. Since the probe goes past the step, the step
/// itself is taken once the estimate covers it, with no further margin.
pub fn probe_goal(usage: u64, cheapest_step: Option<u64>) -> u64 {
    usage
        + cheapest_step.map_or(0, |d| {
            ((d as f64 * PROBE_OVERAGE) as u64).max(PROBE_MIN_BPS)
        })
}

/// The budget upgrades are allocated against: what is sent now plus the
/// largest step the estimate covers with the probe's overage, so a step is
/// taken only once the estimate has reached what probing for it aims at
/// (LiveKit's probe goal: a probe succeeds when the estimate reaches
/// usage + overage). Taking a step as soon as the estimate covers its bare
/// cost left no margin: on a 1 Mbit/s link with a 150 ms round trip GCC's
/// estimate creeps to 1.08 Mbit/s, which covered two `h` tracks, and the
/// queue that built until the detector saw it cost 600 ms of latency.
pub fn upgrade_budget(usage: u64, peak: u64) -> u64 {
    let headroom = peak.saturating_sub(usage);
    if headroom < PROBE_MIN_BPS {
        return usage;
    }
    usage + (headroom as f64 / PROBE_OVERAGE) as u64
}

/// Evidence that the link to a subscriber is congested now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Congestion {
    /// Neither: a drop in the estimate is GCC's reaction to a burst (a
    /// keyframe, a switch up), not congestion. GCC cuts its estimate to 85%
    /// of recently acknowledged throughput on any overuse signal, which
    /// right after a switch up (an SFU's rate jumps; an encoder's ramps) is
    /// far below what is sent, and climbs back over seconds. LiveKit's
    /// detector likewise commits a lower estimate only on sustained
    /// congestion.
    None,
    /// The round trip is `QUEUEING` above its least.
    Queueing,
    /// The subscriber NACKed within `LOSS_WINDOW`: the link is dropping.
    Loss,
}

/// The layer a subscriber is held to for one track: lower at once when the
/// estimate calls for it and there is evidence of congestion, higher one
/// layer at a time once the estimate has allowed it for a hold.
#[derive(Debug, Clone, Copy, Default)]
pub struct Hold {
    /// The applied cap: None while nothing fits (paused, if allowed).
    pub applied: Option<Option<Pick>>,
    higher_since: Option<Instant>,
}

/// What an update did to a track's cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    None,
    Down,
    Up,
}

impl Hold {
    /// Feeds a fresh allocation: `fits` from the current estimate (for
    /// going down), `fits_up` from the peak estimate since the last
    /// congestion, within `upgrade_budget` (for going up; see
    /// `PEAK_WINDOW`); `next` is the track's next layer up from the applied
    /// cap. `may_upgrade` is false while another of the subscriber's tracks
    /// went up within `hold`.
    #[allow(clippy::too_many_arguments)]
    pub fn update(
        &mut self,
        fits: Option<Pick>,
        fits_up: Option<Pick>,
        next: Option<Pick>,
        may_upgrade: bool,
        congestion: Congestion,
        hold: Duration,
        now: Instant,
    ) -> Change {
        let Some(applied) = self.applied else {
            self.applied = Some(fits);
            return Change::Down;
        };
        if fits < applied {
            self.higher_since = None;
            if congestion == Congestion::None {
                return Change::None;
            }
            self.applied = Some(fits);
            return Change::Down;
        }
        if fits_up <= applied {
            self.higher_since = None;
            return Change::None;
        }
        let since = *self.higher_since.get_or_insert(now);
        if !may_upgrade || now.duration_since(since) < hold {
            return Change::None;
        }
        let Some(step) = next else {
            return Change::None;
        };
        self.applied = Some(Some(step).min(fits_up));
        self.higher_since = None;
        Change::Up
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ANY: u8 = u8::MAX;

    fn p(spatial: u8, temporal: u8) -> Option<Pick> {
        Some(Pick::new(spatial, temporal))
    }

    /// Three spatial layers, no temporal layers.
    fn camera(max: u8) -> Track {
        let mut rates = [[None; TEMPORAL]; SPATIAL];
        for (s, r) in [150_000, 500_000, 1_500_000].into_iter().enumerate() {
            rates[s][0] = Some(r);
        }
        Track {
            screen: false,
            max,
            max_temporal: [ANY; SPATIAL],
            rates,
        }
    }

    /// Chrome's VP8 simulcast: three spatial layers in L1T3, each
    /// temporal layer half the frames (and here half the bytes) of the
    /// whole.
    fn l1t3(max: u8) -> Track {
        let mut t = camera(max);
        for (s, r) in [150_000u32, 500_000, 1_500_000].into_iter().enumerate() {
            t.rates[s] = [Some(r / 4), Some(r / 2), Some(r), None];
        }
        t
    }

    #[test]
    fn lowest_layers_first_then_up() {
        let tracks = [camera(2), camera(2), camera(2)];
        assert_eq!(allocate(10_000_000, &tracks), [p(2, 0), p(2, 0), p(2, 0)]);
        // 3 × 150k, then 3 × 350k more for h, then 1M more each for f.
        assert_eq!(allocate(2_000_000, &tracks), [p(1, 0), p(1, 0), p(1, 0)]);
        assert_eq!(allocate(2_600_000, &tracks), [p(2, 0), p(1, 0), p(1, 0)]);
        assert_eq!(allocate(400_000, &tracks), [p(0, 0), p(0, 0), None]);
    }

    #[test]
    fn temporal_layers_within_each_spatial_layer() {
        let tracks = [l1t3(2), l1t3(2)];
        // Each spatial layer at every frame rate before the next one.
        assert_eq!(allocate(80_000, &tracks), [p(0, 0), p(0, 0)]);
        assert_eq!(allocate(160_000, &tracks), [p(0, 1), p(0, 1)]);
        assert_eq!(allocate(300_000, &tracks), [p(0, 2), p(0, 2)]);
        assert_eq!(allocate(600_000, &tracks), [p(1, 1), p(1, 1)]);
        // 720p at a quarter of the frames costs less than 360p at all of
        // them: not a step up.
        assert_eq!(allocate(1_000_000, &tracks), [p(1, 2), p(1, 2)]);
        assert_eq!(allocate(1_800_000, &tracks), [p(2, 1), p(2, 1)]);
        // A frame rate setting caps the temporal layer of every spatial one.
        let mut capped = l1t3(2);
        capped.max_temporal = [1; SPATIAL];
        assert_eq!(allocate(10_000_000, &[capped]), [p(2, 1)]);
    }

    /// Chrome's L1T3 frame rates from a 20 fps camera.
    const FPS: [[f32; TEMPORAL]; SPATIAL] = [[5.0, 10.0, 20.0, 0.0]; SPATIAL];

    fn temporal_layers(t: &Track) -> Vec<Vec<usize>> {
        t.rates
            .iter()
            .map(|r| (0..TEMPORAL).filter(|&l| r[l].is_some()).collect())
            .collect()
    }

    #[test]
    fn shaped_by_how_people_judge_video() {
        // A camera: all the frames of the lowest layer, at least half of
        // the others'.
        let mut t = l1t3(2);
        t.shape(&FPS, false);
        assert_eq!(temporal_layers(&t), [vec![2], vec![1, 2], vec![1, 2]]);
        assert_eq!(
            allocate(1_000_000, &[t.clone(), t.clone()]),
            [p(1, 2), p(1, 2)]
        );
        // Fewer at the lowest only where the alternative is a pause.
        let mut t = l1t3(2);
        t.shape(&FPS, true);
        assert_eq!(temporal_layers(&t), [vec![0, 1, 2], vec![1, 2], vec![1, 2]]);
        // Asking for 5 fps still gets them.
        let mut t = l1t3(2);
        t.max_temporal = [0; SPATIAL];
        t.shape(&FPS, false);
        assert_eq!(temporal_layers(&t), [vec![0], vec![0, 1, 2], vec![0, 1, 2]]);
        assert_eq!(allocate(10_000_000, &[t]), [p(2, 0)]);
        // Not measured yet: the top two temporal layers.
        let mut t = l1t3(2);
        t.shape(&[[0.0; TEMPORAL]; SPATIAL], true);
        assert_eq!(temporal_layers(&t), [vec![0, 1, 2], vec![1, 2], vec![1, 2]]);
        // A screen share: resolution before frames.
        let mut t = l1t3(2);
        t.screen = true;
        t.shape(&FPS, true);
        assert_eq!(temporal_layers(&t), [vec![0], vec![0], vec![0, 1, 2]]);
        let one = std::slice::from_ref(&t);
        assert_eq!(allocate(300_000, one), [p(1, 0)]);
        assert_eq!(allocate(500_000, one), [p(2, 0)]);
        assert_eq!(allocate(800_000, one), [p(2, 1)]);
    }

    #[test]
    fn screen_shares_and_bigger_wants_go_first() {
        let mut screen = camera(2);
        screen.screen = true;
        let tracks = [camera(0), camera(2), screen];
        assert_eq!(allocate(300_000, &tracks), [None, p(0, 0), p(0, 0)]);
        assert_eq!(allocate(1_800_000, &tracks), [p(0, 0), p(1, 0), p(1, 0)]);
        assert_eq!(allocate(2_900_000, &tracks), [p(0, 0), p(1, 0), p(2, 0)]);
    }

    #[test]
    fn skips_layers_not_sent() {
        let mut only_f = camera(2);
        only_f.rates[0][0] = None;
        only_f.rates[1][0] = None;
        let one = std::slice::from_ref(&only_f);
        assert_eq!(allocate(1_000_000, one), [None]);
        assert_eq!(allocate(2_000_000, one), [p(2, 0)]);
        assert_eq!(usage_bps(&only_f, p(0, 0)), 1_500_000, "the lowest live");
        assert_eq!(usage_bps(&camera(1), p(1, 0)), 500_000);
        // A cap above the temporal layers sent is the top one.
        assert_eq!(usage_bps(&l1t3(2), p(1, ANY)), 500_000);
    }

    #[test]
    fn probes_for_the_next_step_only() {
        let t = camera(2);
        assert_eq!(usage_bps(&t, p(0, 0)), 150_000);
        assert_eq!(usage_bps(&t, None), 0);
        assert_eq!(next_step_bps(&t, p(0, 0)), Some(350_000));
        assert_eq!(next_step_bps(&t, p(2, 0)), None, "at what it wants");
        assert_eq!(next_step_bps(&camera(0), p(0, 0)), None);
        assert_eq!(next_step(&t, None), Some((Pick::new(0, 0), 150_000)));
        // With temporal layers the next step is the next frame rate, and
        // from the top one the next spatial layer's lowest.
        let t = l1t3(2);
        // (360p at a quarter of the frames costs less than 180p at all of
        // them, so it is no step up.)
        assert_eq!(next_step(&t, p(0, 2)), Some((Pick::new(1, 1), 100_000)));
        assert_eq!(next_step(&t, p(1, 0)), Some((Pick::new(1, 1), 125_000)));
        assert_eq!(probe_goal(300_000, Some(350_000)), 720_000);
        assert_eq!(probe_goal(300_000, Some(100_000)), 500_000, "at least 200k");
        assert_eq!(probe_goal(300_000, None), 300_000);
    }

    #[test]
    fn upgrades_need_the_probe_goal() {
        // Each step is taken exactly when the estimate reaches its probe goal.
        for (usage, step) in [
            (300_000, 350_000),
            (300_000, 100_000),
            (1_000_000, 1_000_000),
        ] {
            let goal = probe_goal(usage, Some(step));
            assert!(
                upgrade_budget(usage, goal) >= usage + step,
                "{usage} {step}"
            );
            assert!(
                upgrade_budget(usage, goal - 2_000) < usage + step,
                "{usage} {step}"
            );
        }
    }

    #[test]
    fn down_only_on_evidence_and_at_once() {
        let t0 = Instant::now();
        let hold = Duration::from_secs(1);
        let mut h = Hold::default();
        let up = p(2, 0);
        assert_eq!(
            h.update(up, up, None, true, Congestion::None, hold, t0),
            Change::Down,
            "first"
        );
        // A dip without evidence: nothing, however long.
        let later = t0 + Duration::from_secs(5);
        let (h1, h0) = (p(1, 0), p(1, 0));
        assert_eq!(
            h.update(h1, h1, up, true, Congestion::None, hold, later),
            Change::None
        );
        // Queueing or loss: down at once.
        assert_eq!(
            h.update(h0, h0, up, true, Congestion::Queueing, hold, later),
            Change::Down
        );
        assert_eq!(
            h.update(None, None, up, true, Congestion::Loss, hold, later),
            Change::Down
        );
        assert_eq!(h.applied, Some(None));
    }

    #[test]
    fn up_one_layer_per_hold() {
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let hold = Duration::from_millis(1100);
        let mut h = Hold::default();
        let t = l1t3(2);
        h.update(p(0, 2), p(0, 2), None, true, Congestion::None, hold, t0);
        let up = |h: &mut Hold, may: bool, ms: u64| {
            let next = next_step(&t, h.applied.flatten()).map(|(p, _)| p);
            h.update(p(2, 2), p(2, 2), next, may, Congestion::None, hold, at(ms))
        };
        assert_eq!(up(&mut h, true, 0), Change::None);
        assert_eq!(up(&mut h, true, 1000), Change::None);
        assert_eq!(up(&mut h, false, 1100), Change::None, "another went up");
        // The next layer that costs more: 360p at half the frames.
        assert_eq!(up(&mut h, true, 1200), Change::Up);
        assert_eq!(h.applied, Some(p(1, 1)));
        assert_eq!(up(&mut h, true, 1300), Change::None);
        assert_eq!(up(&mut h, true, 2400), Change::Up);
        assert_eq!(h.applied, Some(p(1, 2)));
    }

    #[test]
    fn up_by_the_peak_down_by_the_current() {
        let t0 = Instant::now();
        let hold = Duration::from_secs(1);
        let mut h = Hold::default();
        h.update(p(1, 0), p(1, 0), None, true, Congestion::None, hold, t0);
        // The current estimate fits only h, the peak f: up after the hold.
        let at = |s: u64| t0 + Duration::from_secs(s);
        let (h1, f) = (p(1, 0), p(2, 0));
        assert_eq!(
            h.update(h1, f, f, true, Congestion::None, hold, at(1)),
            Change::None
        );
        assert_eq!(
            h.update(h1, f, f, true, Congestion::None, hold, at(2)),
            Change::Up
        );
        assert_eq!(h.applied, Some(f));
        // Current below without evidence: stays.
        assert_eq!(
            h.update(h1, f, None, true, Congestion::None, hold, at(3)),
            Change::None
        );
    }

    #[test]
    fn backoff_lengthens_the_wait() {
        let base = Duration::from_secs(1);
        assert_eq!(upgrade_wait(base, 1.0), base);
        assert_eq!(
            upgrade_wait(base, BACKOFF_FACTOR),
            Duration::from_millis(4500)
        );
        assert_eq!(upgrade_wait(base, 1e6), BACKOFF_MAX);
    }
}
