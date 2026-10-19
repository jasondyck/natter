//! Active-speaker detection, ported from LiveKit's `pkg/sfu/audio/audiolevel.go`
//! with its defaults, so `speakers_changed` fires when LiveKit's would.
//!
//! Each packet's RFC 6464 level (0 loudest, 127 silent) is observed for the
//! RTP time it covers. Every `update_interval` of audio, a window that was
//! active (level ≤ `active_level`) for at least `min_percentile` of it
//! yields its loudest level, weighted by how much of the window was active,
//! smoothed with an EMA. Anything else resets the level to 0.

use std::time::{Duration, Instant};

const SILENT: u8 = 127;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// Loudest level (dBov below 0) that still counts as silence; −35 dBov.
    pub active_level: u8,
    /// Share of a window, in percent, that must be active.
    pub min_percentile: u8,
    /// Window length; also how often the room reports speakers.
    pub update_interval: Duration,
    /// EMA over this many windows; 0 disables smoothing.
    pub smooth_intervals: u32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            active_level: 35,
            min_percentile: 40,
            update_interval: Duration::from_millis(400),
            smooth_intervals: 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AudioLevel {
    config: Config,
    clock_rate: u32,
    min_active_ms: u32,
    smooth_factor: f64,
    active_threshold: f64,
    smoothed: f64,
    loudest: u8,
    active_ms: u32,
    observed_ms: u32,
    last_observed: Option<Instant>,
    highest_ts: Option<u32>,
}

/// Decibels below full scale to a linear level.
pub fn linear(level: f64) -> f64 {
    10f64.powf(-level / 20.0)
}

impl AudioLevel {
    pub fn new(config: Config, clock_rate: u32) -> Self {
        let interval_ms = config.update_interval.as_millis() as u32;
        AudioLevel {
            config,
            clock_rate,
            min_active_ms: u32::from(config.min_percentile) * interval_ms / 100,
            smooth_factor: if config.smooth_intervals > 0 {
                2.0 / f64::from(config.smooth_intervals + 1)
            } else {
                1.0
            },
            active_threshold: linear(f64::from(config.active_level)),
            smoothed: 0.0,
            loudest: SILENT,
            active_ms: 0,
            observed_ms: 0,
            last_observed: None,
            highest_ts: None,
        }
    }

    /// Observes a packet by its RTP timestamp. Out-of-order packets (older
    /// than the newest seen) are ignored, as LiveKit's.
    pub fn observe(&mut self, level: u8, ts: u32, now: Instant) {
        let highest = *self.highest_ts.get_or_insert(ts);
        let delta = ts.wrapping_sub(highest);
        if delta >= 1 << 31 {
            return;
        }
        let duration_ms = (u64::from(delta) * 1000 / u64::from(self.clock_rate)) as u32;
        self.observe_duration(level, duration_ms, now);
        self.highest_ts = Some(ts);
    }

    fn observe_duration(&mut self, level: u8, duration_ms: u32, now: Instant) {
        let interval_ms = self.config.update_interval.as_millis() as u32;
        if interval_ms == 0 {
            return;
        }
        self.last_observed = Some(now);
        self.observed_ms += duration_ms;
        if level <= self.config.active_level {
            self.active_ms += duration_ms;
            self.loudest = self.loudest.min(level);
        }
        if self.observed_ms >= interval_ms {
            let mut smoothed = 0.0;
            if self.active_ms >= self.min_active_ms {
                // 0 when active the whole window, negative when less.
                let activity = 20.0 * (f64::from(self.active_ms) / f64::from(interval_ms)).log10();
                let level = linear(f64::from(self.loudest) - activity);
                smoothed = self.smoothed + (level - self.smoothed) * self.smooth_factor;
            }
            self.reset(smoothed);
        }
    }

    /// The smoothed linear level, and whether that counts as speaking.
    /// Goes to 0 after two windows without packets (DTX silence, mute).
    pub fn level(&mut self, now: Instant) -> (f64, bool) {
        if self.config.update_interval.is_zero() {
            return (0.0, false);
        }
        if self
            .last_observed
            .is_none_or(|at| now.duration_since(at) >= 2 * self.config.update_interval)
        {
            self.reset(0.0);
        }
        (self.smoothed, self.smoothed >= self.active_threshold)
    }

    fn reset(&mut self, smoothed: f64) {
        self.smoothed = smoothed;
        self.loudest = SILENT;
        self.active_ms = 0;
        self.observed_ms = 0;
    }
}

/// LiveKit sends speaker levels rounded up to eighths, and only changes.
pub fn quantize(level: f64) -> f32 {
    ((level * 8.0).ceil() / 8.0) as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(al: &mut AudioLevel, level: u8, packets: u32, start_ts: u32, now: Instant) -> u32 {
        let mut ts = start_ts;
        for _ in 0..packets {
            al.observe(level, ts, now);
            ts = ts.wrapping_add(960);
        }
        ts
    }

    #[test]
    fn loud_speech_is_active_and_silence_is_not() {
        let now = Instant::now();
        let mut al = AudioLevel::new(Config::default(), 48_000);
        // 21 packets of 20ms: the first only anchors the timestamp.
        feed(&mut al, 20, 21, 0, now);
        let (level, active) = al.level(now);
        assert!(active, "{level}");
        // First window: EMA from 0 with factor 2/3 of linear(20).
        assert!((level - linear(20.0) * 2.0 / 3.0).abs() < 1e-9, "{level}");

        let mut quiet = AudioLevel::new(Config::default(), 48_000);
        feed(&mut quiet, 60, 21, 0, now);
        assert_eq!(quiet.level(now), (0.0, false));
    }

    #[test]
    fn short_bursts_are_weighted_down_and_stale_levels_reset() {
        let now = Instant::now();
        let mut al = AudioLevel::new(Config::default(), 48_000);
        // 40% of the window loud (8 of 20 packets), the rest silent.
        let ts = feed(&mut al, 10, 9, 0, now);
        feed(&mut al, 100, 12, ts, now);
        let (level, active) = al.level(now);
        let expected = linear(10.0 - 20.0 * 0.4f64.log10()) * 2.0 / 3.0;
        assert!((level - expected).abs() < 1e-9 && active);
        assert_eq!(al.level(now + Duration::from_millis(800)), (0.0, false));
    }

    #[test]
    fn quantizes_up_to_eighths() {
        assert_eq!(quantize(0.01), 0.125);
        assert_eq!(quantize(0.5), 0.5);
        assert_eq!(quantize(0.51), 0.625);
    }
}
