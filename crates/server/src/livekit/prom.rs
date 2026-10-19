//! LiveKit's Prometheus metrics, under its names (`livekit_…`, from
//! `pkg/telemetry/prometheus` in LiveKit 1.13.10) with its labels and
//! buckets, so its dashboards and alerts work on natter. Rendered after
//! natter's own `natter_…` metrics at `/metrics`.
//!
//! Every series carries LiveKit's constant labels: this node's id and
//! `node_type="SERVER"`. `country` is always empty, as in LiveKit without a
//! GeoIP database. Recorded where LiveKit records them; see each method.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

/// `prometheus.ExponentialBucketsRange`.
fn exponential_range(min: f64, max: f64, count: usize) -> Vec<f64> {
    let factor = (max / min).powf(1.0 / (count - 1) as f64);
    (0..count).map(|i| min * factor.powi(i as i32)).collect()
}

struct Hist {
    counts: Vec<u64>,
    sum: f64,
    count: u64,
}

/// `# HELP` and `# TYPE` of a family.
fn header(out: &mut String, name: &str, help: &str, kind: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

/// A histogram family: one histogram per label set.
pub struct HistVec {
    name: &'static str,
    help: &'static str,
    labels: &'static [&'static str],
    buckets: Vec<f64>,
    series: Mutex<BTreeMap<Vec<String>, Hist>>,
}

impl HistVec {
    fn new(
        name: &'static str,
        help: &'static str,
        labels: &'static [&'static str],
        buckets: Vec<f64>,
    ) -> Self {
        HistVec {
            name,
            help,
            labels,
            buckets,
            series: Mutex::default(),
        }
    }

    fn empty(&self) -> Hist {
        Hist {
            counts: vec![0; self.buckets.len()],
            sum: 0.0,
            count: 0,
        }
    }

    pub fn observe(&self, labels: &[&str], value: f64) {
        let mut series = self.series.lock().unwrap();
        let h = series
            .entry(labels.iter().map(|s| s.to_string()).collect())
            .or_insert_with(|| self.empty());
        for (b, c) in self.buckets.iter().zip(&mut h.counts) {
            if value <= *b {
                *c += 1;
            }
        }
        h.sum += value;
        h.count += 1;
    }

    fn render(&self, out: &mut String, constant: &str) {
        header(out, self.name, self.help, "histogram");
        // Without labels there is one series from the start, as in
        // client_golang.
        if self.labels.is_empty() {
            self.series
                .lock()
                .unwrap()
                .entry(vec![])
                .or_insert_with(|| self.empty());
        }
        for (values, h) in self.series.lock().unwrap().iter() {
            let labels = label_set(constant, self.labels, values);
            for (b, c) in self.buckets.iter().zip(&h.counts) {
                let _ = writeln!(
                    out,
                    "{}_bucket{{{labels},le=\"{}\"}} {c}",
                    self.name,
                    float(*b)
                );
            }
            let _ = writeln!(
                out,
                "{}_bucket{{{labels},le=\"+Inf\"}} {}",
                self.name, h.count
            );
            let _ = writeln!(out, "{}_sum{{{labels}}} {}", self.name, float(h.sum));
            let _ = writeln!(out, "{}_count{{{labels}}} {}", self.name, h.count);
        }
    }
}

/// A counter family: one value per label set.
pub struct Vec64 {
    name: &'static str,
    help: &'static str,
    labels: &'static [&'static str],
    series: Mutex<BTreeMap<Vec<String>, f64>>,
}

impl Vec64 {
    fn new(name: &'static str, help: &'static str, labels: &'static [&'static str]) -> Self {
        Vec64 {
            name,
            help,
            labels,
            series: Mutex::default(),
        }
    }

    pub fn add(&self, labels: &[&str], v: f64) {
        *self
            .series
            .lock()
            .unwrap()
            .entry(labels.iter().map(|s| s.to_string()).collect())
            .or_default() += v;
    }

    pub fn inc(&self, labels: &[&str]) {
        self.add(labels, 1.0);
    }

    fn render(&self, out: &mut String, constant: &str) {
        header(out, self.name, self.help, "counter");
        for (values, v) in self.series.lock().unwrap().iter() {
            let labels = label_set(constant, self.labels, values);
            let _ = writeln!(out, "{}{{{labels}}} {}", self.name, float(*v));
        }
    }
}

/// Labels in Prometheus' order: sorted by name, as client_golang writes.
fn label_set(constant: &str, names: &[&str], values: &[String]) -> String {
    let mut all: Vec<(&str, String)> = constant
        .split(',')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k, v.trim_matches('"').to_owned()))
        .chain(names.iter().copied().zip(values.iter().cloned()))
        .collect();
    all.sort_by(|a, b| a.0.cmp(b.0));
    all.iter()
        .map(|(k, v)| format!("{k}=\"{}\"", escape(v)))
        .collect::<Vec<_>>()
        .join(",")
}

fn escape(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// As Go's `strconv.FormatFloat(v, 'g', -1, 64)`, which client_golang
/// uses: integers without a point, large values with an exponent.
fn float(v: f64) -> String {
    if v.is_infinite() {
        return if v > 0.0 {
            "+Inf".into()
        } else {
            "-Inf".into()
        };
    }
    if v == v.trunc() && v.abs() < 1e21 {
        if v.abs() >= 1e6 {
            // 3.527882e+06
            let s = format!("{v:e}");
            let (m, e) = s.split_once('e').expect("exponent form");
            let e: i32 = e.parse().expect("exponent");
            return format!("{m}e{}{:02}", if e < 0 { '-' } else { '+' }, e.abs());
        }
        return format!("{}", v as i64);
    }
    format!("{v}")
}

const STREAM: &[&str] = &["direction", "source", "type", "country"];
const PERCENT: [f64; 10] = [0.0, 0.1, 0.3, 0.5, 0.7, 1.0, 5.0, 10.0, 40.0, 100.0];

/// LiveKit's metrics. Hot-path counts are atomics; the rest take a lock,
/// at most a few times a second per stream.
pub struct LkMetrics {
    constant: String,
    /// `livekit_packet_total` / `_bytes` by direction and transmission.
    pub packets_in: AtomicU64,
    pub bytes_in: AtomicU64,
    pub packets_out: AtomicU64,
    pub bytes_out: AtomicU64,
    pub packets_rtx: AtomicU64,
    pub bytes_rtx: AtomicU64,
    /// `livekit_track_published_total` / `subscribed_total`: audio, video.
    pub published: [AtomicI64; 2],
    pub subscribed: [AtomicI64; 2],
    pub room_duration: HistVec,
    pub publish_counter: Vec64,
    pub subscribe_counter: Vec64,
    pub join_latency: HistVec,
    pub start_time: HistVec,
    pub session_duration: HistVec,
    pub peer_connection: Vec64,
    pub participant_join: Vec64,
    pub nacks: Vec64,
    pub plis: Vec64,
    pub firs: Vec64,
    pub loss_total: Vec64,
    pub loss_percent: HistVec,
    pub ooo_total: Vec64,
    pub ooo_percent: HistVec,
    pub jitter: HistVec,
    pub rtt: HistVec,
    pub pubsub_time: HistVec,
    pub service_operation: Vec64,
    pub quality_rating: HistVec,
    pub quality_score: HistVec,
    pub messages: Vec64,
    pub twirp_status: Vec64,
    pub twirp_latency: HistVec,
}

impl LkMetrics {
    pub fn new(node_id: &str) -> Self {
        let h = HistVec::new;
        let v = Vec64::new;
        let m = LkMetrics {
            constant: format!("node_id=\"{node_id}\",node_type=\"SERVER\""),
            packets_in: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            packets_out: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            packets_rtx: AtomicU64::new(0),
            bytes_rtx: AtomicU64::new(0),
            published: Default::default(),
            subscribed: Default::default(),
            room_duration: h(
                "livekit_room_duration_seconds",
                "Duration of closed rooms.",
                &[],
                vec![
                    5.0, 10.0, 60.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0, 18000.0, 36000.0,
                ],
            ),
            publish_counter: v(
                "livekit_track_publish_counter",
                "Track publications by state.",
                &["kind", "state"],
            ),
            subscribe_counter: v(
                "livekit_track_subscribe_counter",
                "Track subscriptions by state.",
                &["state", "error"],
            ),
            join_latency: h(
                "livekit_session_join_latency_ms",
                "Time from signal connection to join response.",
                &["protocol_version"],
                exponential_range(10.0, 10000.0, 15),
            ),
            start_time: h(
                "livekit_session_start_time_ms",
                "Time from signal connection to the participant connecting.",
                &["protocol_version", "warp", "sdk"],
                exponential_range(100.0, 10000.0, 15),
            ),
            session_duration: h(
                "livekit_session_duration_ms",
                "Duration of participant sessions.",
                &["protocol_version"],
                exponential_range(100.0, 4.0 * 60.0 * 60.0 * 1000.0, 15),
            ),
            peer_connection: v(
                "livekit_peer_connection_state",
                "Peer connection state changes.",
                &["transport", "state"],
            ),
            participant_join: v(
                "livekit_participant_join_total",
                "Participant join stages.",
                &["state", "warp", "sdk"],
            ),
            nacks: v(
                "livekit_nack_total",
                "NACKs, per direction of the stream.",
                &["direction", "country"],
            ),
            plis: v(
                "livekit_pli_total",
                "PLIs, per direction of the stream.",
                &["direction", "country"],
            ),
            firs: v(
                "livekit_fir_total",
                "FIRs, per direction of the stream.",
                &["direction", "country"],
            ),
            loss_total: v("livekit_packet_loss_total", "Packets lost.", STREAM),
            loss_percent: h(
                "livekit_packet_loss_percent",
                "Packet loss per stats interval.",
                STREAM,
                PERCENT.to_vec(),
            ),
            ooo_total: v(
                "livekit_packet_out_of_order_total",
                "Packets out of order.",
                STREAM,
            ),
            ooo_percent: h(
                "livekit_packet_out_of_order_percent",
                "Packets out of order per stats interval.",
                STREAM,
                PERCENT.to_vec(),
            ),
            jitter: h(
                "livekit_jitter_us",
                "Interarrival jitter.",
                STREAM,
                vec![
                    1000.0, 10000.0, 30000.0, 50000.0, 70000.0, 100000.0, 300000.0, 600000.0,
                    1000000.0,
                ],
            ),
            rtt: h(
                "livekit_rtt_ms",
                "Round-trip time.",
                STREAM,
                vec![
                    50.0, 100.0, 150.0, 200.0, 250.0, 500.0, 750.0, 1000.0, 5000.0, 10000.0,
                ],
            ),
            pubsub_time: h(
                "livekit_pubsubtime_ms",
                "Time to publish, subscribe, and for a subscription's stream to start.",
                &[
                    "direction",
                    "source",
                    "type",
                    "country",
                    "sdk",
                    "kind",
                    "count",
                ],
                vec![100.0, 200.0, 500.0, 700.0, 1000.0, 5000.0, 10000.0],
            ),
            service_operation: v(
                "livekit_node_service_operation",
                "Service operations by outcome.",
                &["type", "status", "error_type"],
            ),
            quality_rating: h(
                "livekit_quality_rating",
                "Connection quality ratings.",
                &[],
                vec![0.0, 1.0, 2.0],
            ),
            quality_score: h(
                "livekit_quality_score",
                "Connection quality scores (MOS).",
                &[],
                vec![1.0, 2.0, 2.5, 3.0, 3.25, 3.5, 3.75, 4.0, 4.25, 4.5],
            ),
            messages: v(
                "livekit_node_messages",
                "Signal messages.",
                &["type", "status", "direction"],
            ),
            twirp_status: v(
                "livekit_node_twirp_request_status",
                "Twirp requests by status.",
                &["service", "method", "status", "code"],
            ),
            twirp_latency: h(
                "livekit_node_twirp_request_latency_ms",
                "Twirp request latency.",
                &["service", "method", "status"],
                vec![
                    5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
                    30000.0, 60000.0,
                ],
            ),
        };
        // LiveKit adds its cancellations, zero or not, as subscriptions go.
        m.subscribe_counter.add(&["cancel", ""], 0.0);
        m
    }

    /// Per-stream stats from one stats interval (LiveKit's
    /// `telemetry/stats.go`): RTCP counts, loss, reordering, jitter (µs)
    /// and RTT (ms).
    #[allow(clippy::too_many_arguments)]
    pub fn stream(
        &self,
        direction: &str,
        source: &str,
        kind: &str,
        rtcp: (u64, u64, u64),
        lost: u32,
        out_of_order: u32,
        packets: u32,
        jitter_us: Option<f64>,
        rtt: Option<Duration>,
    ) {
        let (nacks, plis, firs) = rtcp;
        let labels = [direction, source, kind, ""];
        for (v, family) in [(nacks, &self.nacks), (plis, &self.plis), (firs, &self.firs)] {
            if v > 0 {
                family.add(&[direction, ""], v as f64);
            }
        }
        if packets > 0 {
            self.loss_percent
                .observe(&labels, f64::from(lost) / f64::from(packets) * 100.0);
            self.ooo_percent.observe(
                &labels,
                f64::from(out_of_order) / f64::from(packets) * 100.0,
            );
        }
        if lost > 0 {
            self.loss_total.add(&labels, f64::from(lost));
        }
        if out_of_order > 0 {
            self.ooo_total.add(&labels, f64::from(out_of_order));
        }
        if let Some(j) = jitter_us.filter(|&j| j > 0.0) {
            self.jitter.observe(&labels, j.round());
        }
        // Whole milliseconds, rounded up as LiveKit's RTCP RTT.
        if let Some(ms) = rtt
            .map(|r| r.as_nanos().div_ceil(1_000_000) as f64)
            .filter(|&ms| ms > 0.0)
        {
            self.rtt.observe(&labels, ms);
        }
    }

    /// `livekit_pubsubtime_ms`: `publish` (asked to published), `subscribe`
    /// (subscription to bound) or `subscribe-stream-start` (to its first
    /// packet).
    pub fn pubsub(
        &self,
        direction: &str,
        source: &str,
        kind: &str,
        took: Duration,
        sdk: &str,
        participant_kind: &str,
    ) {
        self.pubsub_time.observe(
            &[direction, source, kind, "", sdk, participant_kind, "1"],
            took.as_millis() as f64,
        );
    }

    /// LiveKit's Twirp interceptors: latency and status per request.
    pub fn twirp(&self, service: &str, method: &str, status: u16, code: &str, took: Duration) {
        let family = match status {
            400..=499 => "4xx".to_owned(),
            500..=599 => "5xx".to_owned(),
            s => s.to_string(),
        };
        self.twirp_latency
            .observe(&[service, method, &family], took.as_millis() as f64);
        self.twirp_status.inc(&[service, method, &family, code]);
    }

    pub fn render(&self, out: &mut String, rooms: u64, participants: u64) {
        let c = &self.constant;
        let gauge = |out: &mut String, name: &str, help: &str, v: f64| {
            header(out, name, help, "gauge");
            let _ = writeln!(out, "{name}{{{c}}} {}", float(v));
        };
        gauge(
            out,
            "livekit_room_total",
            "Rooms on this node.",
            rooms as f64,
        );
        self.room_duration.render(out, c);
        gauge(
            out,
            "livekit_participant_total",
            "Participants on this node.",
            participants as f64,
        );
        for (name, help, values) in [
            (
                "livekit_track_published_total",
                "Published tracks.",
                &self.published,
            ),
            (
                "livekit_track_subscribed_total",
                "Subscribed tracks.",
                &self.subscribed,
            ),
        ] {
            header(out, name, help, "gauge");
            for (kind, v) in ["AUDIO", "VIDEO"].iter().zip(values) {
                let labels = label_set(c, &["kind"], &[kind.to_string()]);
                let _ = writeln!(out, "{name}{{{labels}}} {}", v.load(Relaxed));
            }
        }
        self.publish_counter.render(out, c);
        self.subscribe_counter.render(out, c);
        self.pubsub_time.render(out, c);
        self.service_operation.render(out, c);
        self.join_latency.render(out, c);
        self.start_time.render(out, c);
        self.session_duration.render(out, c);
        self.peer_connection.render(out, c);
        self.participant_join.render(out, c);
        for (name, help, values) in [
            (
                "livekit_packet_total",
                "RTP packets.",
                [&self.packets_in, &self.packets_out, &self.packets_rtx],
            ),
            (
                "livekit_packet_bytes",
                "RTP bytes.",
                [&self.bytes_in, &self.bytes_out, &self.bytes_rtx],
            ),
        ] {
            header(out, name, help, "counter");
            for ((direction, transmission), v) in [
                ("incoming", "initial"),
                ("outgoing", "initial"),
                ("outgoing", "retransmit"),
            ]
            .iter()
            .zip(values)
            {
                // A retransmit series appears with the first retransmit.
                if *transmission == "retransmit" && v.load(Relaxed) == 0 {
                    continue;
                }
                let labels = label_set(
                    c,
                    &["country", "direction", "transmission"],
                    &[
                        String::new(),
                        direction.to_string(),
                        transmission.to_string(),
                    ],
                );
                let _ = writeln!(out, "{name}{{{labels}}} {}", float(v.load(Relaxed) as f64));
            }
        }
        self.nacks.render(out, c);
        self.plis.render(out, c);
        self.firs.render(out, c);
        self.loss_total.render(out, c);
        self.loss_percent.render(out, c);
        self.ooo_total.render(out, c);
        self.ooo_percent.render(out, c);
        self.jitter.render(out, c);
        self.rtt.render(out, c);
        self.quality_rating.render(out, c);
        self.quality_score.render(out, c);
        self.messages.render(out, c);
        self.twirp_status.render(out, c);
        self.twirp_latency.render(out, c);
        process(out);
    }
}

/// client_golang's process collector, from /proc (Linux).
fn process(out: &mut String) {
    let page = 4096.0;
    let ticks = 100.0;
    let mut metric = |name: &str, kind: &str, help: &str, v: f64| {
        header(out, name, help, kind);
        let _ = writeln!(out, "{name} {}", float(v));
    };
    if let Ok(stat) = std::fs::read_to_string("/proc/self/stat")
        && let Some((_, rest)) = stat.rsplit_once(')')
    {
        // Fields after the command, starting at field 3 (state).
        let f: Vec<&str> = rest.split_whitespace().collect();
        let num = |i: usize| f.get(i - 3).and_then(|v| v.parse::<f64>().ok());
        if let (Some(utime), Some(stime)) = (num(14), num(15)) {
            metric(
                "process_cpu_seconds_total",
                "counter",
                "Total user and system CPU time spent in seconds.",
                (utime + stime) / ticks,
            );
        }
        if let Some(vsize) = num(23) {
            metric(
                "process_virtual_memory_bytes",
                "gauge",
                "Virtual memory size in bytes.",
                vsize,
            );
        }
        if let Some(rss) = num(24) {
            metric(
                "process_resident_memory_bytes",
                "gauge",
                "Resident memory size in bytes.",
                rss * page,
            );
        }
        let boot = std::fs::read_to_string("/proc/stat").ok().and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("btime "))
                .and_then(|v| v.trim().parse::<f64>().ok())
        });
        if let (Some(start), Some(boot)) = (num(22), boot) {
            metric(
                "process_start_time_seconds",
                "gauge",
                "Start time of the process since unix epoch in seconds.",
                boot + start / ticks,
            );
        }
    }
    if let Ok(fds) = std::fs::read_dir("/proc/self/fd") {
        metric(
            "process_open_fds",
            "gauge",
            "Number of open file descriptors.",
            fds.count() as f64,
        );
    }
    if let Some(max) = std::fs::read_to_string("/proc/self/limits")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("Max open files"))
                .and_then(|l| l.split_whitespace().nth(3))
                .and_then(|v| v.parse::<f64>().ok())
        })
    {
        metric(
            "process_max_fds",
            "gauge",
            "Maximum number of open file descriptors.",
            max,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_floats() {
        assert_eq!(float(0.0), "0");
        assert_eq!(float(42.0), "42");
        assert_eq!(float(3527882.0), "3.527882e+06");
        assert_eq!(float(49.5), "49.5");
        assert_eq!(float(f64::INFINITY), "+Inf");
    }

    #[test]
    fn buckets_as_client_golang() {
        let b = exponential_range(10.0, 10000.0, 15);
        assert_eq!(b.len(), 15);
        assert!((b[0] - 10.0).abs() < 1e-9 && (b[14] - 10000.0).abs() < 1e-6);
    }

    #[test]
    fn renders_like_livekit() {
        let m = LkMetrics::new("ND_x");
        m.quality_score.observe(&[], 4.5);
        m.stream(
            "incoming",
            "MICROPHONE",
            "AUDIO",
            (2, 0, 0),
            1,
            0,
            100,
            Some(675.0),
            Some(Duration::from_millis(3)),
        );
        let mut out = String::new();
        m.render(&mut out, 1, 2);
        for line in [
            r#"livekit_room_total{node_id="ND_x",node_type="SERVER"} 1"#,
            r#"livekit_participant_total{node_id="ND_x",node_type="SERVER"} 2"#,
            r#"livekit_nack_total{country="",direction="incoming",node_id="ND_x",node_type="SERVER"} 2"#,
            r#"livekit_packet_loss_percent_bucket{country="",direction="incoming",node_id="ND_x",node_type="SERVER",source="MICROPHONE",type="AUDIO",le="1"} 1"#,
            r#"livekit_packet_loss_percent_bucket{country="",direction="incoming",node_id="ND_x",node_type="SERVER",source="MICROPHONE",type="AUDIO",le="0.7"} 0"#,
            r#"livekit_jitter_us_sum{country="",direction="incoming",node_id="ND_x",node_type="SERVER",source="MICROPHONE",type="AUDIO"} 675"#,
            r#"livekit_quality_score_count{node_id="ND_x",node_type="SERVER"} 1"#,
            r#"livekit_packet_total{country="",direction="outgoing",node_id="ND_x",node_type="SERVER",transmission="initial"} 0"#,
        ] {
            assert!(out.contains(line), "missing {line}\n{out}");
        }
    }
}
