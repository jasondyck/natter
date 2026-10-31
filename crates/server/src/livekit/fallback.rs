//! Candidate-type fallback, as LiveKit's (`docs/livekit/media.md` E.5):
//! a client whose connection keeps failing over UDP is moved to ICE/TCP,
//! and from TCP to TURN/TLS. A connection that dies young (connected less
//! than 90 s) moves at once; otherwise two failures within a minute do.
//!
//! One step per resume: after moving, further failures count only once
//! the client has resumed with the new preference. Unlike LiveKit, a move
//! that a client's own failure report causes holds for the resume that
//! carries the report: LiveKit answers that resume, and restarts ICE, with
//! the preference from before it.

use std::time::{Duration, Instant};

/// A connection younger than this that fails counts as short-lived
/// (LiveKit `shortConnectionThreshold`).
pub const SHORT_CONNECTION: Duration = Duration::from_secs(90);
/// Failures that move a long-lived connection on, and the window they
/// must fall in (LiveKit `failureCountThreshold`,
/// `preferNextByFailureWindow`).
const FAILURES: u32 = 2;
const FAILURE_WINDOW: Duration = Duration::from_secs(60);
/// How long a room member's preference outlives them, for a full
/// reconnect (LiveKit's ICE config cache, `iceConfigTTLMin`).
pub const REMEMBER: Duration = Duration::from_secs(5 * 60);

/// LiveKit's fallback settings: `rtc.allow_tcp_fallback` (on by default),
/// `rtc.tcp_fallback_rtt_threshold` (ICE/TCP only for clients whose
/// signalling round trip is under it, TURN/TLS for the rest; none by
/// default), and `rtc.allow_udp_unstable_fallback` (off by default, and
/// only with the threshold set).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    pub enabled: bool,
    pub rtt_threshold: Option<Duration>,
    pub udp_unstable: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            enabled: true,
            rtt_threshold: None,
            udp_unstable: false,
        }
    }
}

impl Config {
    /// Whether ICE/TCP is worth trying for a client with this signalling
    /// round trip (LiveKit's `canUseICETCP`; unknown counts as under).
    pub fn tcp_by_rtt(&self, signal_rtt: Option<Duration>) -> bool {
        self.rtt_threshold
            .is_none_or(|t| signal_rtt.is_none_or(|r| r < t))
    }

    /// The UDP-unstable check runs (LiveKit needs the threshold for it).
    pub fn checks_unstable(&self) -> bool {
        self.enabled && self.udp_unstable && self.rtt_threshold.is_some()
    }
}

/// A receiver report fraction lost (of 256) this high is an unstable
/// report, and this many of the last 32 make the connection unstable
/// (LiveKit `udpLossFracUnstable` 25%, `udpLossUnstableCountThreshold`).
const UNSTABLE_LOSS: u8 = (255 * 25 / 100) as u8;
const UNSTABLE_REPORTS: u32 = 20;
/// Reports are taken as their highest loss once this often (LiveKit's
/// media loss proxy, `downLostUpdateDelta`).
const LOSS_EVERY: Duration = Duration::from_secs(1);

/// LiveKit's UDP-unstable fallback: whether what a client receives over
/// UDP keeps losing so much that it should move to ICE/TCP (or TURN/TLS)
/// though the connection hasn't failed. Its receiver reports, at most one
/// sample a second (the highest fraction lost since the last), each
/// unstable at 25% or more; 20 unstable of the last 32, while signalling
/// round-trips in under 1.3× the media round trip and under the
/// threshold, moves it.
#[derive(Debug, Default)]
pub struct Unstable {
    /// One bit per sample, the latest lowest.
    samples: u32,
    highest: Option<u8>,
    sampled_at: Option<Instant>,
    /// The media round trip, halfway to each new one (LiveKit's
    /// `UpdateMediaRTT`).
    media_rtt: Option<Duration>,
    /// Receiver reports counted, per subscriber stream (by m-line).
    seen: Vec<(str0m::media::Mid, u64)>,
}

impl Unstable {
    /// A stream's receiver reports so far (`StreamTx::reception`): counts
    /// its latest loss if it is new.
    pub fn reception(&mut self, mid: str0m::media::Mid, reports: u64, fraction_lost: u8) {
        let new = match self.seen.iter_mut().find(|(m, _)| *m == mid) {
            Some((_, n)) => std::mem::replace(n, reports) != reports,
            None => {
                self.seen.push((mid, reports));
                true
            }
        };
        if new {
            self.highest = Some(self.highest.unwrap_or(0).max(fraction_lost));
        }
    }

    /// A media round trip measured.
    pub fn media_rtt(&mut self, rtt: Duration) {
        self.media_rtt = Some(match self.media_rtt {
            None => rtt,
            Some(m) if rtt >= m => m + (rtt - m) / 2,
            Some(m) => m - (m - rtt) / 2,
        });
    }

    /// Takes a sample if one is due; true when the connection counts as
    /// unstable and should move (which starts the count over).
    pub fn check(&mut self, cfg: &Config, signal_rtt: Option<Duration>, now: Instant) -> bool {
        if self
            .sampled_at
            .is_some_and(|at| now.duration_since(at) <= LOSS_EVERY)
        {
            return false;
        }
        let Some(loss) = self.highest.take() else {
            return false;
        };
        self.sampled_at = Some(now);
        self.samples <<= 1;
        if loss < UNSTABLE_LOSS {
            return false;
        }
        self.samples |= 1;
        let (Some(threshold), Some(media), Some(signal)) =
            (cfg.rtt_threshold, self.media_rtt, signal_rtt)
        else {
            return false;
        };
        if self.samples.count_ones() >= UNSTABLE_REPORTS
            && signal < media.mul_f32(1.3)
            && signal < threshold
        {
            self.samples = 0;
            return true;
        }
        false
    }

    /// The client resumed with a new preference: count afresh.
    pub fn reset(&mut self) {
        self.samples = 0;
        self.highest = None;
    }
}

/// Which candidates a client is held to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Pref {
    #[default]
    Any,
    /// ICE/TCP only: UDP candidates are left out of the server's SDP and
    /// ignored from the client's.
    Tcp,
    /// TURN/TLS only: the client gets only `turns:` servers and
    /// `force_relay`.
    Tls,
}

/// What a failed connection ran over (LiveKit's ICE connection types, in
/// its order: the "lowest priority" of two is the later).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Path {
    Udp,
    Tcp,
    Relay,
}

#[derive(Debug, Default)]
pub struct Fallback {
    pub pref: Pref,
    failures: u32,
    last_failure: Option<Instant>,
    /// Moved since the last resume.
    moved: bool,
}

impl Fallback {
    pub fn new(pref: Pref) -> Self {
        Fallback {
            pref,
            ..Default::default()
        }
    }

    /// A connection that went over `path` failed. `tcp` and `tls`: whether
    /// the client can do ICE/TCP and whether TURN/TLS is on. Returns the
    /// new preference when it changes.
    pub fn on_failure(
        &mut self,
        path: Option<Path>,
        short: bool,
        tcp: bool,
        tls: bool,
        now: Instant,
    ) -> Option<Pref> {
        if self.moved {
            return None;
        }
        if !short {
            self.failures += 1;
            let last = self.last_failure.replace(now);
            if self.failures < FAILURES || last.is_none_or(|l| now - l > FAILURE_WINDOW) {
                return None;
            }
        }
        let next = match path {
            Some(Path::Udp) if self.pref == Pref::Any && tcp => Pref::Tcp,
            Some(Path::Udp) if self.pref == Pref::Any && tls => Pref::Tls,
            Some(Path::Tcp) if tls => Pref::Tls,
            // LiveKit falls back to any transport here, which undoes a
            // move when the client reports the failure the server already
            // moved on; staying put is the same otherwise.
            _ => self.pref,
        };
        if next == self.pref {
            return None;
        }
        self.pref = next;
        self.failures = 0;
        self.last_failure = None;
        self.moved = true;
        Some(next)
    }

    /// The client resumed (and restarts ICE) with the current preference.
    pub fn resumed(&mut self) {
        self.failures = 0;
        self.last_failure = None;
        self.moved = false;
    }
}

/// `sdp` without the candidates `pref` rules out.
pub fn filter_sdp(sdp: &str, pref: Pref) -> String {
    if pref != Pref::Tcp {
        return sdp.to_owned();
    }
    let mut out = String::with_capacity(sdp.len());
    for line in sdp.split_inclusive('\n') {
        if line.starts_with("a=candidate:") && !is_tcp(line) {
            continue;
        }
        out.push_str(line);
    }
    out
}

/// Whether a candidate (`a=candidate:…` or `candidate:…`) is TCP: its
/// third field, the transport.
pub fn is_tcp(candidate: &str) -> bool {
    candidate
        .split_whitespace()
        .nth(2)
        .is_some_and(|t| t.eq_ignore_ascii_case("tcp"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_short_udp_failure_moves_to_tcp_then_tls() {
        let t = Instant::now();
        let mut f = Fallback::default();
        assert_eq!(
            f.on_failure(Some(Path::Udp), true, true, true, t),
            Some(Pref::Tcp)
        );
        // Once per resume.
        assert_eq!(f.on_failure(Some(Path::Tcp), true, true, true, t), None);
        f.resumed();
        assert_eq!(
            f.on_failure(Some(Path::Tcp), true, true, true, t),
            Some(Pref::Tls)
        );
        f.resumed();
        // TLS is as far as it goes.
        assert_eq!(f.on_failure(Some(Path::Relay), true, true, true, t), None);
        assert_eq!(f.pref, Pref::Tls);
    }

    #[test]
    fn long_lived_connections_need_two_failures_in_a_minute() {
        let t = Instant::now();
        let mut f = Fallback::default();
        assert_eq!(f.on_failure(Some(Path::Udp), false, true, false, t), None);
        let later = t + Duration::from_secs(61);
        assert_eq!(
            f.on_failure(Some(Path::Udp), false, true, false, later),
            None
        );
        let soon = later + Duration::from_secs(30);
        assert_eq!(
            f.on_failure(Some(Path::Udp), false, true, false, soon),
            Some(Pref::Tcp)
        );
    }

    #[test]
    fn without_tcp_udp_goes_to_tls_and_without_tls_tcp_stays() {
        let t = Instant::now();
        let mut f = Fallback::default();
        assert_eq!(
            f.on_failure(Some(Path::Udp), true, false, true, t),
            Some(Pref::Tls)
        );
        let mut f = Fallback::new(Pref::Tcp);
        assert_eq!(f.on_failure(Some(Path::Tcp), true, true, false, t), None);
        assert_eq!(f.pref, Pref::Tcp);
    }

    #[test]
    fn unstable_after_20_lossy_seconds_of_32() {
        use str0m::media::Mid;
        let cfg = Config {
            rtt_threshold: Some(Duration::from_millis(500)),
            udp_unstable: true,
            ..Config::default()
        };
        let t0 = Instant::now();
        let mid = Mid::from("0");
        let mut u = Unstable::default();
        u.media_rtt(Duration::from_millis(100));
        let signal = Some(Duration::from_millis(110));
        let mut reports = 0;
        // One report a second at `loss`, sampled from the second on.
        let mut second = |u: &mut Unstable, k: u64, loss: u8| {
            reports += 1;
            u.reception(mid, reports, loss);
            u.check(&cfg, signal, t0 + Duration::from_millis(1001 * k))
        };
        // 19 lossy seconds, a clean one, then the 20th lossy one moves it.
        for k in 0..19 {
            assert!(!second(&mut u, k, 70));
        }
        assert!(!second(&mut u, 19, 10));
        assert!(second(&mut u, 20, 64));
        // And counts again from nothing.
        assert!(!second(&mut u, 21, 255));
        // Under 25% never counts.
        let mut v = Unstable::default();
        v.media_rtt(Duration::from_millis(100));
        for k in 0..40 {
            reports += 1;
            v.reception(mid, reports, 62);
            assert!(!v.check(&cfg, signal, t0 + Duration::from_millis(1001 * k)));
        }
    }

    #[test]
    fn unstable_needs_signalling_no_slower_than_media() {
        use str0m::media::Mid;
        let cfg = Config {
            rtt_threshold: Some(Duration::from_millis(500)),
            udp_unstable: true,
            ..Config::default()
        };
        let t0 = Instant::now();
        let run = |media: u64, signal: u64| {
            let mut u = Unstable::default();
            u.media_rtt(Duration::from_millis(media));
            (0..32).any(|k| {
                u.reception(Mid::from("0"), k + 1, 128);
                u.check(
                    &cfg,
                    Some(Duration::from_millis(signal)),
                    t0 + Duration::from_millis(1001 * k),
                )
            })
        };
        assert!(run(100, 120));
        assert!(!run(100, 140), "signalling over 1.3x media");
        assert!(!run(600, 520), "signalling over the threshold");
        // A report seen twice counts once: no new report, no sample.
        let mut u = Unstable::default();
        u.media_rtt(Duration::from_millis(100));
        u.reception(Mid::from("0"), 1, 128);
        for k in 0..40 {
            u.reception(Mid::from("0"), 1, 128);
            assert!(!u.check(
                &cfg,
                Some(Duration::from_millis(50)),
                t0 + Duration::from_millis(1001 * k)
            ));
        }
    }

    #[test]
    fn tcp_only_under_the_rtt_threshold() {
        let cfg = Config {
            rtt_threshold: Some(Duration::from_millis(300)),
            ..Config::default()
        };
        assert!(cfg.tcp_by_rtt(Some(Duration::from_millis(100))));
        assert!(!cfg.tcp_by_rtt(Some(Duration::from_millis(300))));
        assert!(cfg.tcp_by_rtt(None));
        assert!(Config::default().tcp_by_rtt(Some(Duration::from_secs(5))));
        assert!(
            !Config {
                udp_unstable: true,
                ..Config::default()
            }
            .checks_unstable()
        );
    }

    #[test]
    fn filters_udp_candidates_for_tcp() {
        let sdp = "v=0\r\na=candidate:1 1 udp 2130706431 1.2.3.4 50000 typ host\r\n\
                   a=candidate:2 1 tcp 1671430143 1.2.3.4 50000 typ host tcptype passive\r\n\
                   a=end-of-candidates\r\n";
        let tcp = filter_sdp(sdp, Pref::Tcp);
        assert!(!tcp.contains(" udp "));
        assert!(tcp.contains(" tcp ") && tcp.contains("end-of-candidates"));
        assert_eq!(filter_sdp(sdp, Pref::Any), sdp);
        assert!(is_tcp(
            "candidate:2 1 TCP 1 1.2.3.4 9 typ host tcptype active"
        ));
    }
}
