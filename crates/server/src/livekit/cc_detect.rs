//! Congestion evidence from TWCC feedback: what the allocator steps down on.
//!
//! GCC's estimate (str0m's, as libwebrtc's) is a poor signal for an SFU:
//! it cuts to 85% of recently acknowledged throughput on any overuse
//! signal, which right after a simulcast switch up is far below what is
//! sent, and the receiver-report round trip it could be checked against
//! updates once a second. TWCC feedback comes every 50-100 ms with each
//! packet's send and arrival time, which gives directly:
//!
//! - queueing delay: a packet's one-way delay above the least seen lately
//!   (the clocks' offset cancels);
//! - loss: packets the receiver reports missing;
//! - the delivered rate: what actually arrived, which while a queue is
//!   building is the bottleneck's capacity.
//!
//! As LiveKit's own send-side detector (`docs/livekit/media.md` B.1),
//! queueing counts only when sustained: a keyframe's burst queues for tens
//! of milliseconds, a link that is too slow keeps queueing.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use str0m::bwe::TwccPacket;

/// The least one-way delay is taken over this long (chosen: long enough to
/// see an empty queue, short enough to follow a route change).
const BASE_WINDOW: Duration = Duration::from_secs(10);
/// A report's median queueing delay at least this is a queue building
/// (LiveKit `jqr_min_delay`).
pub const QUEUEING: Duration = Duration::from_millis(50);
/// ...and only once it has lasted this long (LiveKit's early warning:
/// 2 groups / 200 ms of queueing).
pub const SUSTAINED: Duration = Duration::from_millis(200);
/// Loss and delivered rate are measured over this window (chosen).
const RATE_WINDOW: Duration = Duration::from_millis(500);
/// This share lost within `RATE_WINDOW` is loss (chosen: a tail-dropping
/// queue loses runs of packets; a clean link loses none).
pub const LOSS: f64 = 0.02;
/// Under congestion, send this share of what was delivered, so the queue
/// drains (GCC's multiplicative decrease, β = 0.85).
pub const BACKOFF: f64 = 0.85;

/// Evidence of congestion now, per [`Detector`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Evidence {
    pub queueing: bool,
    pub loss: bool,
    /// What arrived over the last `RATE_WINDOW`, bits/s, when there is
    /// enough to tell.
    pub delivered_bps: Option<u64>,
}

#[derive(Debug, Default)]
pub struct Detector {
    /// Least one-way delay (ns, signed: the clocks' offset) per report,
    /// with when, for the windowed minimum.
    bases: VecDeque<(Instant, i128)>,
    /// When the median queueing delay first reached `QUEUEING`, while it
    /// has stayed there.
    queueing_since: Option<Instant>,
    /// Packets reported over the last `RATE_WINDOW`: when the report came,
    /// arrival (None: lost) and size.
    recent: VecDeque<(Instant, Option<Instant>, u16)>,
}

/// One-way delay in nanoseconds, signed (arrival is on the remote's clock).
fn owd(p: &TwccPacket) -> Option<i128> {
    let arrived = p.arrived?;
    Some(match arrived.checked_duration_since(p.sent) {
        Some(d) => d.as_nanos() as i128,
        None => -(p.sent.duration_since(arrived).as_nanos() as i128),
    })
}

impl Detector {
    /// Takes one feedback report's packets, in send order.
    pub fn on_feedback(&mut self, packets: &[TwccPacket], now: Instant) {
        for p in packets {
            self.recent.push_back((now, p.arrived, p.size));
        }
        while self
            .recent
            .front()
            .is_some_and(|(at, _, _)| now.duration_since(*at) > RATE_WINDOW)
        {
            self.recent.pop_front();
        }

        let mut delays: Vec<i128> = packets.iter().filter_map(owd).collect();
        let Some(&least) = delays.iter().min() else {
            return;
        };
        while self
            .bases
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > BASE_WINDOW)
        {
            self.bases.pop_front();
        }
        // Keep the deque increasing so its front is the window's minimum.
        while self.bases.back().is_some_and(|&(_, b)| b >= least) {
            self.bases.pop_back();
        }
        self.bases.push_back((now, least));
        let base = self.bases.front().map_or(least, |&(_, b)| b);

        delays.sort_unstable();
        let median = delays[delays.len() / 2] - base;
        if median >= QUEUEING.as_nanos() as i128 {
            self.queueing_since.get_or_insert(now);
        } else {
            self.queueing_since = None;
        }
    }

    pub fn evidence(&self, now: Instant) -> Evidence {
        let queueing = self
            .queueing_since
            .is_some_and(|since| now.duration_since(since) >= SUSTAINED);
        let total = self.recent.len();
        let lost = self.recent.iter().filter(|(_, a, _)| a.is_none()).count();
        let loss = total >= 10 && lost as f64 >= total as f64 * LOSS;
        // Delivered: bytes that arrived after the first arrival, over the
        // span from it to the last.
        let mut first: Option<(Instant, u16)> = None;
        let (mut last, mut bytes, mut n) = (None::<Instant>, 0u64, 0);
        for (a, s) in self
            .recent
            .iter()
            .filter_map(|(_, a, s)| a.map(|a| (a, *s)))
        {
            if first.is_none_or(|(f, _)| a < f) {
                first = Some((a, s));
            }
            last = Some(last.map_or(a, |l| l.max(a)));
            bytes += u64::from(s);
            n += 1;
        }
        let delivered_bps = match (first, last) {
            (Some((f, size)), Some(l))
                if n >= 10 && l.duration_since(f) >= Duration::from_millis(100) =>
            {
                let span = l.duration_since(f).as_secs_f64();
                Some(((bytes - u64::from(size)) as f64 * 8.0 / span) as u64)
            }
            _ => None,
        };
        Evidence {
            queueing,
            loss,
            delivered_bps,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A report of `n` packets of `size` bytes sent every `gap`, each
    /// arriving `delay(i)` after it was sent (None: lost).
    fn report(
        start: Instant,
        n: usize,
        gap: Duration,
        size: u16,
        delay: impl Fn(usize) -> Option<Duration>,
    ) -> Vec<TwccPacket> {
        (0..n)
            .map(|i| {
                let sent = start + gap * i as u32;
                TwccPacket {
                    sent,
                    arrived: delay(i).map(|d| sent + d),
                    size,
                }
            })
            .collect()
    }

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn a_short_burst_is_not_queueing_a_lasting_queue_is() {
        let t0 = Instant::now();
        let mut d = Detector::default();
        // Empty queue: 20 ms one way.
        d.on_feedback(
            &report(t0, 20, MS * 5, 1000, |_| Some(MS * 20)),
            t0 + MS * 100,
        );
        assert!(!d.evidence(t0 + MS * 100).queueing);
        // One report queued 80 ms (a keyframe's burst), then empty again.
        d.on_feedback(
            &report(t0 + MS * 100, 20, MS * 5, 1000, |_| Some(MS * 100)),
            t0 + MS * 200,
        );
        d.on_feedback(
            &report(t0 + MS * 200, 20, MS * 5, 1000, |_| Some(MS * 20)),
            t0 + MS * 300,
        );
        assert!(!d.evidence(t0 + MS * 300).queueing);
        // Queued 80 ms for 300 ms: queueing.
        for k in 0..4u32 {
            let at = t0 + MS * (400 + 100 * k);
            d.on_feedback(
                &report(at - MS * 100, 20, MS * 5, 1000, |_| Some(MS * 100)),
                at,
            );
        }
        assert!(d.evidence(t0 + MS * 700).queueing);
    }

    #[test]
    fn loss_and_delivered_rate() {
        let t0 = Instant::now();
        let mut d = Detector::default();
        // 100 packets of 1250 bytes every 10 ms: 1 Mbit/s delivered.
        d.on_feedback(
            &report(t0, 100, MS * 10, 1250, |_| Some(MS * 20)),
            t0 + MS * 100,
        );
        let e = d.evidence(t0 + MS * 100);
        assert!(!e.loss);
        let bps = e.delivered_bps.unwrap();
        assert!((990_000..=1_010_000).contains(&bps), "{bps}");
        // Every tenth lost: loss.
        d.on_feedback(
            &report(t0, 50, MS * 10, 1250, |i| (i % 10 != 0).then_some(MS * 20)),
            t0 + MS * 200,
        );
        assert!(d.evidence(t0 + MS * 200).loss);
    }

    #[test]
    fn the_clock_offset_cancels() {
        let t0 = Instant::now() + Duration::from_secs(10);
        let mut d = Detector::default();
        // The remote's clock reads 5 s behind ours: negative one-way delays.
        let behind = |q: u32| move |_| Some(MS * q);
        let shifted = |r: Vec<TwccPacket>| -> Vec<TwccPacket> {
            r.into_iter()
                .map(|p| TwccPacket {
                    arrived: p.arrived.map(|a| a - Duration::from_secs(5)),
                    ..p
                })
                .collect()
        };
        d.on_feedback(
            &shifted(report(t0, 20, MS * 5, 1000, behind(20))),
            t0 + MS * 100,
        );
        for k in 0..4u32 {
            let at = t0 + MS * (200 + 100 * k);
            d.on_feedback(
                &shifted(report(at - MS * 100, 20, MS * 5, 1000, behind(90))),
                at,
            );
        }
        assert!(d.evidence(t0 + MS * 500).queueing);
    }
}
