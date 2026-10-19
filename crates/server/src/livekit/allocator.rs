//! Congestion control's allocation step: which simulcast layer of each video
//! track a subscriber gets, given the bandwidth estimate for its link.
//!
//! As LiveKit's stream allocator allocates all tracks
//! (`docs/livekit/media.md` B.1): layer by layer, every track's lowest layer
//! before anyone's next one, screen shares first, then tracks wanting more
//! before tracks wanting less. A track nothing fits for gets no layer: it is
//! paused if the subscriber allows that, else it gets its lowest layer
//! anyway. Temporal layers are not allocated (all are forwarded).

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

/// One video track as the allocator sees it.
#[derive(Debug, Clone)]
pub struct Track {
    /// Screen shares are allocated before cameras.
    pub screen: bool,
    /// The highest spatial layer the subscriber's settings allow.
    pub max: u8,
    /// Bitrate of each spatial layer the publisher is sending now.
    pub rates: [Option<u32>; 3],
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

/// The highest spatial layer each track gets within `budget_bps`, or None
/// when not even its lowest fits.
pub fn allocate(budget_bps: u64, tracks: &[Track]) -> Vec<Option<u8>> {
    let mut order: Vec<usize> = (0..tracks.len()).collect();
    order.sort_by_key(|&k| (!tracks[k].screen, std::cmp::Reverse(tracks[k].max)));
    let mut alloc: Vec<Option<u8>> = vec![None; tracks.len()];
    let mut left = budget_bps;
    for spatial in 0..3u8 {
        for &k in &order {
            let t = &tracks[k];
            let Some(rate) = t.rates[usize::from(spatial)] else {
                continue;
            };
            if spatial > t.max {
                continue;
            }
            let held = alloc[k].and_then(|s| t.rates[usize::from(s)]).unwrap_or(0);
            let cost = u64::from(rate).saturating_sub(u64::from(held));
            if cost <= left {
                left -= cost;
                alloc[k] = Some(spatial);
            }
        }
    }
    alloc
}

/// What a track sends at a cap (None: paused): its highest live layer up to
/// the cap, else its lowest (the forwarder's choice).
pub fn usage_bps(t: &Track, cap: Option<u8>) -> u64 {
    let Some(cap) = cap else {
        return 0;
    };
    let live = |s: u8| t.rates[usize::from(s)].map(u64::from);
    (0..=cap.min(2))
        .rev()
        .find_map(live)
        .or_else(|| (0..3).find_map(live))
        .unwrap_or(0)
}

/// The cost of a track's next step up, if it is below what it wants: the
/// next live layer above its cap, within its settings.
pub fn next_step_bps(t: &Track, cap: Option<u8>) -> Option<u64> {
    let now = usage_bps(t, cap);
    let from = cap.map_or(0, |c| c.saturating_add(1));
    (from..=t.max.min(2))
        .find_map(|s| t.rates[usize::from(s)])
        .map(|r| u64::from(r).saturating_sub(now))
        .filter(|&d| d > 0)
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
    pub applied: Option<Option<u8>>,
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
    /// congestion (for going up; see `PEAK_WINDOW`). `may_upgrade` is false
    /// while another of the subscriber's tracks went up within `hold`.
    pub fn update(
        &mut self,
        fits: Option<u8>,
        fits_up: Option<u8>,
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
        if fits_up.max(fits) <= applied {
            self.higher_since = None;
            return Change::None;
        }
        let since = *self.higher_since.get_or_insert(now);
        if !may_upgrade || now.duration_since(since) < hold {
            return Change::None;
        }
        let step = applied.map_or(0, |s| s.saturating_add(1));
        self.applied = Some(Some(step).min(fits_up.max(fits)));
        self.higher_since = None;
        Change::Up
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn camera(max: u8) -> Track {
        Track {
            screen: false,
            max,
            rates: [Some(150_000), Some(500_000), Some(1_500_000)],
        }
    }

    #[test]
    fn lowest_layers_first_then_up() {
        let tracks = [camera(2), camera(2), camera(2)];
        assert_eq!(allocate(10_000_000, &tracks), [Some(2), Some(2), Some(2)]);
        // 3 × 150k, then 3 × 350k more for h, then 1M more each for f.
        assert_eq!(allocate(2_000_000, &tracks), [Some(1), Some(1), Some(1)]);
        assert_eq!(allocate(2_600_000, &tracks), [Some(2), Some(1), Some(1)]);
        assert_eq!(allocate(400_000, &tracks), [Some(0), Some(0), None]);
    }

    #[test]
    fn screen_shares_and_bigger_wants_go_first() {
        let mut screen = camera(2);
        screen.screen = true;
        let tracks = [camera(0), camera(2), screen];
        assert_eq!(allocate(300_000, &tracks), [None, Some(0), Some(0)]);
        assert_eq!(allocate(1_800_000, &tracks), [Some(0), Some(1), Some(1)]);
        assert_eq!(allocate(2_900_000, &tracks), [Some(0), Some(1), Some(2)]);
    }

    #[test]
    fn skips_layers_not_sent() {
        let only_f = Track {
            rates: [None, None, Some(1_500_000)],
            ..camera(2)
        };
        let one = std::slice::from_ref(&only_f);
        assert_eq!(allocate(1_000_000, one), [None]);
        assert_eq!(allocate(2_000_000, one), [Some(2)]);
        assert_eq!(usage_bps(&only_f, Some(0)), 1_500_000, "the lowest live");
        assert_eq!(usage_bps(&camera(1), Some(1)), 500_000);
    }

    #[test]
    fn probes_for_the_next_step_only() {
        let t = camera(2);
        assert_eq!(usage_bps(&t, Some(0)), 150_000);
        assert_eq!(usage_bps(&t, None), 0);
        assert_eq!(next_step_bps(&t, Some(0)), Some(350_000));
        assert_eq!(next_step_bps(&t, Some(2)), None, "at what it wants");
        assert_eq!(next_step_bps(&camera(0), Some(0)), None);
        assert_eq!(probe_goal(300_000, Some(350_000)), 720_000);
        assert_eq!(probe_goal(300_000, Some(100_000)), 500_000, "at least 200k");
        assert_eq!(probe_goal(300_000, None), 300_000);
    }

    #[test]
    fn down_only_on_evidence_and_at_once() {
        let t0 = Instant::now();
        let hold = Duration::from_secs(1);
        let mut h = Hold::default();
        assert_eq!(
            h.update(Some(2), Some(2), true, Congestion::None, hold, t0),
            Change::Down,
            "first"
        );
        // A dip without evidence: nothing, however long.
        let later = t0 + Duration::from_secs(5);
        assert_eq!(
            h.update(Some(1), Some(1), true, Congestion::None, hold, later),
            Change::None
        );
        // Queueing or loss: down at once.
        assert_eq!(
            h.update(Some(1), Some(1), true, Congestion::Queueing, hold, later),
            Change::Down
        );
        assert_eq!(
            h.update(None, None, true, Congestion::Loss, hold, later),
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
        h.update(Some(0), Some(0), true, Congestion::None, hold, t0);
        let up = |h: &mut Hold, may: bool, ms: u64| {
            h.update(Some(2), Some(2), may, Congestion::None, hold, at(ms))
        };
        assert_eq!(up(&mut h, true, 0), Change::None);
        assert_eq!(up(&mut h, true, 1000), Change::None);
        assert_eq!(up(&mut h, false, 1100), Change::None, "another went up");
        assert_eq!(up(&mut h, true, 1200), Change::Up);
        assert_eq!(h.applied, Some(Some(1)));
        assert_eq!(up(&mut h, true, 1300), Change::None);
        assert_eq!(up(&mut h, true, 2400), Change::Up);
        assert_eq!(h.applied, Some(Some(2)));
    }

    #[test]
    fn up_by_the_peak_down_by_the_current() {
        let t0 = Instant::now();
        let hold = Duration::from_secs(1);
        let mut h = Hold::default();
        h.update(Some(1), Some(1), true, Congestion::None, hold, t0);
        // The current estimate fits only h, the peak f: up after the hold.
        let at = |s: u64| t0 + Duration::from_secs(s);
        assert_eq!(
            h.update(Some(1), Some(2), true, Congestion::None, hold, at(1)),
            Change::None
        );
        assert_eq!(
            h.update(Some(1), Some(2), true, Congestion::None, hold, at(2)),
            Change::Up
        );
        assert_eq!(h.applied, Some(Some(2)));
        // Current below without evidence: stays.
        assert_eq!(
            h.update(Some(1), Some(2), true, Congestion::None, hold, at(3)),
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
