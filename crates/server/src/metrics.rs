//! Counters shared between the SFU thread and the HTTP side, rendered at
//! `/metrics` in the Prometheus text exposition format.
//!
//! Everything is an `AtomicU64` because the SFU thread writes these on the
//! media path while axum reads them from a request handler. Relaxed ordering
//! is enough: they are statistics, not synchronisation.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// The `natter_…` metrics: field (the name, less `natter_`), type and help.
macro_rules! metrics {
    ($($(#[$doc:meta])* $field:ident: $kind:literal, $help:literal;)*) => {
        pub struct Metrics {
            /// The same under LiveKit's names, and LiveKit's other metrics.
            pub lk: crate::livekit::prom::LkMetrics,
            $($(#[$doc])* pub $field: AtomicU64,)*
        }

        impl Metrics {
            /// For the node `node_id` (LiveKit's metrics carry it).
            pub fn new(node_id: &str) -> Self {
                Metrics {
                    lk: crate::livekit::prom::LkMetrics::new(node_id),
                    $($field: AtomicU64::new(0),)*
                }
            }

            pub fn render(&self) -> String {
                let mut out = String::with_capacity(1024);
                let all = [$((stringify!($field), $kind, $help, &self.$field)),*];
                for (field, kind, help, value) in all {
                    let _ = writeln!(out, "# HELP natter_{field} {help}");
                    let _ = writeln!(out, "# TYPE natter_{field} {kind}");
                    let _ = writeln!(out, "natter_{field} {}", value.load(Relaxed));
                }
                self.lk
                    .render(&mut out, self.rooms.load(Relaxed), self.peers.load(Relaxed));
                out
            }
        }
    };
}

metrics! {
    /// Peers currently in a room.
    peers: "gauge", "Peers currently connected to a room.";
    /// Rooms with at least one peer.
    rooms: "gauge", "Rooms with at least one peer.";
    /// Media paths that reached DTLS-connected, over the process lifetime.
    media_connected_total: "counter", "Media paths that reached DTLS-connected.";
    /// RTP frames received from browsers.
    packets_received_total: "counter", "RTP frames received from browsers.";
    /// RTP frames written out again, counted once per recipient.
    packets_forwarded_total: "counter", "RTP frames written to other peers, counted per recipient.";
    /// Payload bytes received from browsers.
    bytes_received_total: "counter", "RTP payload bytes received from browsers.";
    /// Payload bytes written out again, counted once per recipient.
    bytes_forwarded_total: "counter",
        "RTP payload bytes written to other peers, counted per recipient.";
    /// Frames that carried an audio level, i.e. the extension is negotiated
    /// and being used.
    audio_levels_total: "counter", "Frames carrying an RTP audio level.";
    /// UDP sends that failed.
    udp_send_errors_total: "counter", "UDP sends that failed.";
    /// Joins turned away because the server was at `NATTER_MAX_PEERS`.
    joins_refused_total: "counter", "Joins turned away because the server was full.";
    /// Sessions handed back to a reconnecting client.
    resumes_total: "counter", "Sessions resumed by a reconnecting client.";
    /// Passes of the engine loop, and transports polled in them: their
    /// ratio to forwarded packets is the loop's own overhead.
    engine_loops_total: "counter", "Passes of the media engine's loop.";
    transport_polls_total: "counter", "Peer connections polled by the media engine.";
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics::new(&crate::livekit::ids::new(crate::livekit::ids::NODE))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_prometheus_text() {
        let metrics = Metrics::default();
        metrics.peers.store(2, Relaxed);
        metrics.packets_forwarded_total.store(41, Relaxed);

        let text = metrics.render();
        assert!(text.contains("# TYPE natter_peers gauge\nnatter_peers 2\n"));
        assert!(text.contains("# TYPE natter_packets_forwarded_total counter\n"));
        assert!(text.contains("natter_packets_forwarded_total 41\n"));
        // Every line of a Prometheus document must be a comment, a metric, or blank.
        for line in text.lines() {
            assert!(
                line.starts_with('#')
                    || ["natter_", "livekit_", "process_"]
                        .iter()
                        .any(|p| line.starts_with(p)),
                "unexpected line: {line:?}"
            );
        }
    }
}
