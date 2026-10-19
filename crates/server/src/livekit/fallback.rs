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
