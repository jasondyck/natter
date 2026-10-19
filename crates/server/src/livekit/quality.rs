//! Connection quality, ported from LiveKit's
//! `pkg/sfu/connectionquality/scorer.go` as v1.13.10 runs it: only packet
//! loss counts (every caller leaves RTT, jitter and bitrate scoring off),
//! weighted per codec and scaled down for DTX-like low packet rates.

use std::time::{Duration, Instant};

use livekit_protocol::ConnectionQuality;

const MAX_SCORE: f64 = 100.0;
const MIN_SCORE: f64 = 30.0;
/// Scores rise slowly and fall fast.
const INCREASE: f64 = 0.4;
const DECREASE: f64 = 0.8;
const GOOD_BELOW: f64 = 80.0;
const POOR_BELOW: f64 = 40.0;
const LOST_BELOW: f64 = 20.0;

const PPS_BIN: f64 = 2.0;
const PPS_BINS: usize = 250;
const PPS_MIN_READINGS: u32 = 10;
/// Rates under 10 pps (DTX, static screens) never become the reference.
const PPS_MODE_MIN_BIN: usize = 5;
const MODE_INTERVAL: Duration = Duration::from_secs(120);

/// How much a percent of loss costs, by codec (`connectionstats.go`).
pub fn loss_weight(mime: &str, fec: bool) -> f64 {
    let mime = mime.to_ascii_lowercase();
    let (weight, fec) = if mime == "audio/red" {
        (4.0, true)
    } else if mime.starts_with("audio/") {
        (8.0, fec)
    } else {
        (10.0, false)
    };
    if fec { weight / 1.5 } else { weight }
}

/// One scoring window's counts.
#[derive(Debug, Clone, Copy, Default)]
pub struct Window {
    pub duration: Duration,
    pub packets: u32,
    pub padding: u32,
    pub lost: u32,
    /// Lost upstream, so not the subscriber's fault.
    pub missing: u32,
    pub out_of_order: u32,
    pub last_rtcp: Option<Instant>,
}

#[derive(Debug, Clone)]
pub struct Scorer {
    weight: f64,
    score: f64,
    muted: bool,
    histogram: Box<[u32; PPS_BINS]>,
    readings: u32,
    mode_at: Option<Instant>,
    mode: usize,
}

impl Scorer {
    pub fn new(weight: f64) -> Self {
        Scorer {
            weight,
            score: MAX_SCORE,
            muted: false,
            histogram: Box::new([0; PPS_BINS]),
            readings: 0,
            mode_at: None,
            mode: 0,
        }
    }

    /// Muting shows as excellent at once, unless already lost.
    pub fn set_muted(&mut self, muted: bool) {
        if muted && !self.muted && quality(self.score) != ConnectionQuality::Lost {
            self.score = MAX_SCORE;
        }
        self.muted = muted;
    }

    pub fn update(&mut self, w: &Window, now: Instant) {
        if self.muted {
            return;
        }
        let weight = self.adjusted_weight(w, now);
        let score = if w.packets + w.padding == 0 {
            if w.last_rtcp
                .is_some_and(|at| now.duration_since(at) > w.duration)
            {
                LOST_BELOW
            } else {
                POOR_BELOW
            }
        } else {
            let lost = w.lost.saturating_sub(w.missing + w.out_of_order);
            let loss = f64::from(lost) * 100.0 / f64::from(w.packets + w.padding) * weight;
            let packet = (MAX_SCORE - loss).max(0.0);
            let factor = if packet < self.score {
                DECREASE
            } else {
                INCREASE
            };
            (factor * packet + (1.0 - factor) * self.score).max(MIN_SCORE)
        };
        self.score = score;
    }

    fn adjusted_weight(&mut self, w: &Window, now: Instant) -> f64 {
        if w.duration.is_zero() {
            return self.weight;
        }
        let pps = f64::from(w.packets) / w.duration.as_secs_f64();
        let bin = ((pps / PPS_BIN + 0.5) as usize).min(PPS_BINS - 1);
        self.histogram[bin] += 1;
        self.readings += 1;
        if self.readings > PPS_MIN_READINGS
            && self
                .mode_at
                .is_none_or(|at| now.duration_since(at) > MODE_INTERVAL)
        {
            let (mut mode, mut max) = (0, 0);
            for (i, &count) in self.histogram.iter().enumerate().skip(PPS_MODE_MIN_BIN) {
                if count > max {
                    (mode, max) = (i, count);
                }
            }
            if max > 0 {
                self.mode = mode;
            }
            for count in self.histogram.iter_mut() {
                *count >>= 1;
            }
            self.mode_at = Some(now);
        }
        if self.mode == 0 || self.mode == PPS_BINS - 1 {
            return self.weight;
        }
        let ratio = (pps / (self.mode as f64 * PPS_BIN)).min(1.0);
        ratio.sqrt() * self.weight
    }

    pub fn score(&self) -> f64 {
        self.score
    }

    pub fn quality(&self) -> ConnectionQuality {
        quality(self.score)
    }

    /// What LiveKit reports as the score: the R factor as a MOS.
    pub fn mos(&self) -> f32 {
        mos(self.score)
    }
}

pub fn quality(score: f64) -> ConnectionQuality {
    if score > GOOD_BELOW {
        ConnectionQuality::Excellent
    } else if score > POOR_BELOW {
        ConnectionQuality::Good
    } else if score > LOST_BELOW {
        ConnectionQuality::Poor
    } else {
        ConnectionQuality::Lost
    }
}

pub fn mos(score: f64) -> f32 {
    if score <= 0.0 {
        1.0
    } else if score >= 100.0 {
        4.5
    } else {
        (1.0 + 0.035 * score + 0.000007 * score * (score - 60.0) * (100.0 - score)) as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(packets: u32, lost: u32) -> Window {
        Window {
            duration: Duration::from_secs(5),
            packets,
            lost,
            ..Window::default()
        }
    }

    #[test]
    fn opus_loss_thresholds_match_livekit() {
        let w = loss_weight("audio/opus", false);
        // Past 2.5% loss the score drops below 80, the GOOD boundary.
        let mut s = Scorer::new(w);
        let now = Instant::now();
        for _ in 0..20 {
            s.update(&window(250, 7), now);
        }
        assert_eq!(s.quality(), ConnectionQuality::Good);
        let mut clean = Scorer::new(w);
        clean.update(&window(250, 0), now);
        assert_eq!(
            (clean.quality(), clean.mos()),
            (ConnectionQuality::Excellent, 4.5)
        );
        assert!((loss_weight("audio/opus", true) - 8.0 / 1.5).abs() < 1e-12);
        assert_eq!(loss_weight("audio/red", false), 4.0 / 1.5);
        assert_eq!(loss_weight("video/VP8", true), 10.0);
    }

    #[test]
    fn falls_fast_rises_slow_and_floors() {
        let now = Instant::now();
        let mut s = Scorer::new(8.0);
        s.update(&window(250, 250), now);
        assert_eq!(s.score(), MIN_SCORE.max(0.8 * 0.0 + 0.2 * 100.0));
        s.update(&window(250, 0), now);
        assert!((s.score() - (0.4 * 100.0 + 0.6 * 30.0)).abs() < 1e-9);
    }

    #[test]
    fn silence_is_poor_or_lost() {
        let now = Instant::now();
        let mut s = Scorer::new(8.0);
        s.update(&window(0, 0), now);
        assert_eq!(s.quality(), ConnectionQuality::Poor);
        let stale = Window {
            last_rtcp: Some(now),
            ..window(0, 0)
        };
        s.update(&stale, now + Duration::from_secs(6));
        assert_eq!(s.quality(), ConnectionQuality::Lost);
        s.set_muted(true);
        assert_eq!(
            s.quality(),
            ConnectionQuality::Lost,
            "muting doesn't hide LOST"
        );
    }

    #[test]
    fn dtx_rates_weigh_loss_less_once_a_mode_is_known() {
        let now = Instant::now();
        let mut s = Scorer::new(8.0);
        for i in 0..12 {
            s.update(&window(250, 0), now + Duration::from_secs(i));
        }
        // Mode is 50 pps; 2.5 pps is 5% of it, so loss weighs sqrt(0.05).
        let w = s.adjusted_weight(&window(12, 0), now + Duration::from_secs(20));
        assert!((w - 8.0 * (2.4f64 / 50.0).sqrt()).abs() < 1e-9, "{w}");
    }

    #[test]
    fn mos_mapping() {
        assert_eq!(mos(100.0), 4.5);
        assert_eq!(mos(0.0), 1.0);
        assert!((mos(80.0) - 4.024).abs() < 1e-3);
    }
}
