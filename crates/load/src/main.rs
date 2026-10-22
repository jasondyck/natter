//! natter-load: the same WebRTC clients against any SFU, so servers can be
//! compared on equal terms. See `docs/benchmarks/landscape.md`.
//!
//! Every room gets `--publishers` clients sending fake 20ms Opus packets (or
//! with `--video`, fake VP8 frames) and `--subscribers` clients receiving
//! them all. Each packet carries who sent
//! it, its sequence number and when (see `stamp`), so subscribers measure
//! loss, duplicates and one-way latency exactly; the server's CPU and memory
//! come from `/proc` or its container's cgroup over the same window.

mod media;
mod probe;
mod signal;
mod stamp;
mod stats;

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use clap::{Parser, ValueEnum};
use futures_util::StreamExt as _;
use hdrhistogram::Histogram;
use serde::Serialize;

use media::{ClientKey, Options, Shared, VideoCounts, VideoOptions, Worker};
use probe::{Probe, Sample};
use signal::{Client, Ctx};
use stats::Receipts;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Target {
    /// LiveKit's `/rtc` signalling: LiveKit and natter.
    Livekit,
    /// str0m's chat example, patched for rooms (bench/servers/str0m-chat).
    Str0mChat,
}

#[derive(Debug, Parser)]
#[command(version, about = "WebRTC load generator for comparing SFUs")]
struct Args {
    /// Signalling the server speaks.
    #[arg(long, value_enum)]
    target: Target,
    /// The server's HTTP base URL.
    #[arg(long, default_value = "http://127.0.0.1:7880")]
    url: String,
    #[arg(long, default_value_t = 1)]
    rooms: u32,
    /// Clients per room that send audio (and receive nothing).
    #[arg(long, default_value_t = 4)]
    publishers: u32,
    /// Clients per room that receive every publisher (and send nothing).
    #[arg(long, default_value_t = 4)]
    subscribers: u32,
    /// Time after everyone is connected before measuring starts.
    #[arg(long, default_value = "10s", value_parser = parse_duration)]
    warmup: Duration,
    /// How long to measure.
    #[arg(long, default_value = "30s", value_parser = parse_duration)]
    duration: Duration,
    /// Longest to wait for every client to connect and hear everyone.
    #[arg(long, default_value = "60s", value_parser = parse_duration)]
    ready_timeout: Duration,
    /// Media threads.
    #[arg(long, default_value_t = 4)]
    workers: usize,
    /// Address the clients' sockets bind to.
    #[arg(long, default_value = "127.0.0.1")]
    bind: IpAddr,
    /// Bytes per Opus packet (100 is about 40 kbit/s).
    #[arg(long, default_value_t = 100)]
    payload: usize,
    /// Publishers send one VP8 camera track instead of audio (LiveKit
    /// signalling only). Loss and latency are then counted in frames.
    #[arg(long)]
    video: bool,
    /// Video bitrate between keyframes.
    #[arg(long, default_value_t = 1500)]
    video_kbps: u32,
    #[arg(long, default_value_t = 30)]
    fps: u32,
    /// Keyframe size as a multiple of other frames'.
    #[arg(long, default_value_t = 4)]
    keyframe_ratio: usize,
    /// Video on three simulcast layers (q, h, f at a tenth, a third and
    /// all of --video-kbps).
    #[arg(long, requires = "video")]
    simulcast: bool,
    /// Subscribers ask for no more than this layer of each video track:
    /// q (320×180), h (640×360) or f.
    #[arg(long, value_parser = parse_quality, requires = "simulcast")]
    sub_quality: Option<u8>,
    /// Every client's downlink is a bottleneck of this many kbit/s, with a
    /// queue (--down-queue-ms) that drops packets when full.
    #[arg(long)]
    down_kbps: Option<u64>,
    #[arg(long, default_value = "200ms", value_parser = parse_ms)]
    down_queue_ms: Duration,
    /// Delay past the bottleneck, standing for the whole round trip.
    #[arg(long, default_value = "0ms", value_parser = parse_ms)]
    down_delay_ms: Duration,
    /// Up to this much more per packet, uniformly, keeping order.
    #[arg(long, default_value = "0ms", value_parser = parse_ms)]
    down_jitter_ms: Duration,
    /// Change the downlink capacity during the run: `KBPS@TIME`, TIME after
    /// measuring starts (e.g. `500@10s`).
    #[arg(long, requires = "down_kbps", value_parser = parse_step)]
    down_step: Option<(u64, Duration)>,
    /// Percent of RTP/RTCP from the server dropped on arrival, after connecting.
    #[arg(long, default_value_t = 0.0)]
    loss_down: f64,
    /// Percent of RTP/RTCP to the server dropped before sending.
    #[arg(long, default_value_t = 0.0)]
    loss_up: f64,
    /// Clients joining at once.
    #[arg(long, default_value_t = 16)]
    join_concurrency: usize,
    /// Server process to measure.
    #[arg(long, conflicts_with = "cgroup")]
    pid: Option<u32>,
    /// Server cgroup directory to measure (a container's, say).
    #[arg(long)]
    cgroup: Option<PathBuf>,
    #[arg(long, env = "LIVEKIT_API_KEY", default_value = "devkey")]
    api_key: String,
    #[arg(long, env = "LIVEKIT_API_SECRET", default_value = "secret")]
    api_secret: String,
    /// Prefix of room names; runs with different prefixes never share rooms.
    #[arg(long, default_value = "load")]
    room_prefix: String,
    /// Also write the results here as JSON.
    #[arg(long)]
    json: Option<PathBuf>,
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let (num, unit) = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .map_or((s, "s"), |i| s.split_at(i));
    let n: f64 = num.parse().map_err(|_| format!("bad duration {s:?}"))?;
    let secs = match unit {
        "ms" => n / 1000.0,
        "s" => n,
        "m" => n * 60.0,
        _ => return Err(format!("bad duration unit in {s:?}")),
    };
    Ok(Duration::from_secs_f64(secs))
}

/// For the `--*-ms` flags: a bare number is milliseconds, as the name says
/// (`--down-delay-ms 60` was a minute); with a unit, as `parse_duration`.
fn parse_ms(s: &str) -> Result<Duration, String> {
    match s.parse::<f64>() {
        Ok(ms) => Ok(Duration::from_secs_f64(ms / 1000.0)),
        Err(_) => parse_duration(s),
    }
}

fn parse_quality(s: &str) -> Result<u8, String> {
    match s {
        "q" => Ok(0),
        "h" => Ok(1),
        "f" => Ok(2),
        _ => Err(format!("expected q, h or f, not {s:?}")),
    }
}

fn parse_step(s: &str) -> Result<(u64, Duration), String> {
    let (kbps, at) = s.split_once('@').ok_or("expected KBPS@TIME")?;
    let kbps = kbps.parse().map_err(|_| format!("bad kbit/s in {s:?}"))?;
    Ok((kbps, parse_duration(at)?))
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "natter_load=info,warn,str0m=error".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    install_crypto();
    raise_fd_limit();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(args))
}

/// The same pure-Rust crypto as natter's server.
fn install_crypto() {
    str0m::crypto::from_feature_flags().install_process_default();
    dimpl::crypto::CryptoProvider::install_default(dimpl::crypto::rust_crypto::default_provider());
}

/// A socket per peer connection: hundreds of clients outgrow the usual
/// soft limit of 1024 descriptors.
fn raise_fd_limit() {
    // SAFETY: plain syscalls on a local struct.
    unsafe {
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 && lim.rlim_cur < lim.rlim_max {
            lim.rlim_cur = lim.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
        }
    }
}

async fn run(args: Args) -> anyhow::Result<()> {
    let probe = match (args.pid, &args.cgroup) {
        (Some(pid), _) => Some(Probe::Pid(pid)),
        (_, Some(dir)) => Some(Probe::Cgroup(dir.clone())),
        _ => None,
    };
    if let Some(p) = &probe {
        p.sample().context("probing the server")?;
    }
    let shared = Arc::new(Shared {
        epoch: Instant::now(),
        measuring: false.into(),
        lossy: false.into(),
        link_bps: args.down_kbps.unwrap_or(0).saturating_mul(1000).into(),
    });
    if args.video && args.target != Target::Livekit {
        bail!("--video needs --target livekit");
    }
    let frame_bytes = (args.video_kbps as usize * 1000 / 8) / args.fps.max(1) as usize;
    let options = Options {
        payload: args.payload,
        loss_down: args.loss_down / 100.0,
        loss_up: args.loss_up / 100.0,
        video: VideoOptions {
            fps: args.fps.max(1),
            frame_bytes,
            keyframe_bytes: frame_bytes * args.keyframe_ratio.max(1),
        },
        downlink: args.down_kbps.map(|_| media::LinkOptions {
            max_delay: args.down_queue_ms,
            delay: args.down_delay_ms,
            jitter: args.down_jitter_ms,
        }),
    };
    let kind = if args.video {
        str0m::media::MediaKind::Video
    } else {
        str0m::media::MediaKind::Audio
    };
    let workers = (0..args.workers.max(1))
        .map(|i| Worker::spawn(i, shared.clone(), options))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let ctx = Ctx::new(
        args.url.clone(),
        args.bind,
        kind,
        args.simulcast,
        args.video_kbps * 1000,
        args.sub_quality,
        workers.clone(),
    );

    // Publishers first, so subscribers find every track as they join.
    let per_room = args.publishers + args.subscribers;
    let mut clients = Vec::new();
    for publishing in [true, false] {
        for room in 0..args.rooms {
            let (first, count) = if publishing {
                (0, args.publishers)
            } else {
                (args.publishers, args.subscribers)
            };
            for index in first..first + count {
                clients.push(Client {
                    key: ClientKey { room, index },
                    room: format!("{}-{room}", args.room_prefix),
                    identity: format!("{}-{room}-{index}", args.room_prefix),
                    publisher: publishing.then_some(room * args.publishers + index),
                });
            }
        }
    }
    let total = clients.len();
    eprintln!(
        "joining {total} clients: {} rooms × ({} publishers + {} subscribers)",
        args.rooms, args.publishers, args.subscribers
    );

    let http = reqwest::Client::new();
    let keys = signal::livekit::Keys {
        key: args.api_key.clone(),
        secret: args.api_secret.clone(),
    };
    let target = args.target;
    let results: Vec<anyhow::Result<()>> = futures_util::stream::iter(clients)
        .map(|client| {
            let (ctx, http, keys) = (ctx.clone(), http.clone(), keys.clone());
            async move {
                let who = client.identity.clone();
                match target {
                    Target::Str0mChat => signal::chat::join(ctx, http, client).await,
                    Target::Livekit => signal::livekit::join(ctx, keys, client).await,
                }
                .with_context(|| who)
            }
        })
        .buffer_unordered(args.join_concurrency.max(1))
        .collect()
        .await;
    let failed: Vec<_> = results.into_iter().filter_map(Result::err).collect();
    if let Some(e) = failed.first() {
        eprintln!("{} of {total} joins failed; first: {e:#}", failed.len());
    }
    if failed.len() == total {
        bail!("no client joined");
    }

    // Wait until every publisher is connected and every subscriber hears
    // every publisher in its room.
    let deadline = Instant::now() + args.ready_timeout;
    let readiness = loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let r = readiness(&workers, args.publishers).await;
        let done = r.publishers == args.rooms * args.publishers
            && r.subscribers == args.rooms * args.subscribers;
        if done || Instant::now() >= deadline {
            break r;
        }
    };
    eprintln!(
        "ready: {}/{} publishers sending, {}/{} subscribers hearing all {} publishers",
        readiness.publishers,
        args.rooms * args.publishers,
        readiness.subscribers,
        args.rooms * args.subscribers,
        args.publishers,
    );

    shared.lossy.store(true, Relaxed);
    eprintln!("warming up for {:?}", args.warmup);
    tokio::time::sleep(args.warmup).await;

    eprintln!("measuring for {:?}", args.duration);
    let server_before = probe.as_ref().map(Probe::sample).transpose()?;
    let self_before = Probe::this_process()?;
    let started = Instant::now();
    shared.measuring.store(true, Relaxed);
    if let Some((kbps, at)) = args.down_step {
        let shared = shared.clone();
        tokio::spawn(async move {
            tokio::time::sleep(at).await;
            eprintln!("downlink now {kbps} kbit/s");
            shared.link_bps.store(kbps * 1000, Relaxed);
        });
    }
    tokio::time::sleep(args.duration).await;
    shared.measuring.store(false, Relaxed);
    let measured = started.elapsed();
    let server_after = probe.as_ref().map(Probe::sample).transpose()?;
    let self_after = Probe::this_process()?;
    // Packets sent at the very end, and their resends, still count.
    tokio::time::sleep(Duration::from_secs(2)).await;
    shared.lossy.store(false, Relaxed);

    let mut receipts = Receipts::default();
    let mut sent = HashMap::new();
    let (mut dropped_down, mut dropped_up, mut send_errors) = (0, 0, 0);
    let mut video = VideoCounts::default();
    for w in &workers {
        let r = w.report().await;
        receipts.merge(&r.receipts);
        for (publisher, n) in r.sent {
            *sent.entry(publisher).or_insert(0u64) += n;
        }
        dropped_down += r.dropped_down;
        dropped_up += r.dropped_up;
        send_errors += r.send_errors;
        video.merge(&r.video);
    }
    let sent_total: u64 = sent.values().sum();
    let expected = sent_total * u64::from(args.subscribers);
    let secs = measured.as_secs_f64();
    let server = match (server_before, server_after) {
        (Some(a), Some(b)) => Some(usage(a, b, measured, receipts.unique)),
        _ => None,
    };
    let load = usage(self_before, self_after, measured, receipts.unique);
    let q = |p: f64| receipts.latency.value_at_quantile(p) as f64 / 1000.0;

    let results = Results {
        target: format!("{:?}", args.target),
        media: if args.video { "video" } else { "audio" },
        video_kbps: args.video.then_some(args.video_kbps),
        fps: args.video.then_some(args.fps),
        video: args.video.then_some(video),
        undecodable_percent: args
            .video
            .then(|| 100.0 * video.undecodable as f64 / expected.max(1) as f64),
        url: args.url.clone(),
        rooms: args.rooms,
        publishers: args.publishers,
        subscribers: args.subscribers,
        clients: total,
        clients_per_room: per_room,
        join_failures: failed.len(),
        publishers_ready: readiness.publishers,
        subscribers_ready: readiness.subscribers,
        setup_ms_p50: readiness.setup.value_at_quantile(0.5),
        setup_ms_p99: readiness.setup.value_at_quantile(0.99),
        measured_s: secs,
        packets_sent: sent_total,
        receipts_expected: expected,
        receipts_unique: receipts.unique,
        loss_percent: if expected > 0 {
            100.0 * (1.0 - receipts.unique as f64 / expected as f64)
        } else {
            0.0
        },
        duplicates: receipts.duplicates,
        late: receipts.late,
        forwarded_per_s: receipts.unique as f64 / secs,
        latency_ms_p50: q(0.5),
        latency_ms_p90: q(0.9),
        latency_ms_p99: q(0.99),
        latency_ms_p999: q(0.999),
        latency_ms_max: receipts.latency.max() as f64 / 1000.0,
        loss_down_percent: args.loss_down,
        loss_up_percent: args.loss_up,
        dropped_down,
        dropped_up,
        send_errors,
        server,
        load_generator: load,
    };
    print(&results);
    if let Some(path) = &args.json {
        std::fs::write(path, serde_json::to_vec_pretty(&results)?)
            .with_context(|| format!("writing {}", path.display()))?;
    }
    // Clients' background tasks and media threads would run on.
    std::process::exit(0);
}

struct Readiness {
    publishers: u32,
    subscribers: u32,
    /// Milliseconds from starting to join until a client's last
    /// connection was up.
    setup: Histogram<u64>,
}

async fn readiness(workers: &[Worker], publishers: u32) -> Readiness {
    #[derive(Default)]
    struct State {
        publishing: bool,
        sending: bool,
        heard: usize,
        connected_after: Duration,
    }
    let mut clients: HashMap<ClientKey, State> = HashMap::new();
    for w in workers {
        for c in w.report().await.conns {
            let s = clients.entry(c.client).or_default();
            s.publishing |= c.publishing;
            s.sending |= c.publishing && c.connected_after.is_some();
            s.heard = s.heard.max(c.publishers_heard);
            if let Some(t) = c.connected_after {
                s.connected_after = s.connected_after.max(t);
            }
        }
    }
    let mut setup = Histogram::new(3).expect("valid");
    let mut r = (0, 0);
    for s in clients.values() {
        let ready = if s.publishing {
            r.0 += u32::from(s.sending);
            s.sending
        } else {
            let ok = s.heard >= publishers as usize;
            r.1 += u32::from(ok);
            ok
        };
        if ready {
            setup.saturating_record(s.connected_after.as_millis() as u64);
        }
    }
    Readiness {
        publishers: r.0,
        subscribers: r.1,
        setup,
    }
}

#[derive(Debug, Serialize)]
struct Usage {
    /// Percent of one core.
    cpu_percent: f64,
    /// CPU time per unique packet a subscriber received.
    cpu_us_per_forward: f64,
    memory_mib: f64,
}

fn usage(before: Sample, after: Sample, wall: Duration, forwards: u64) -> Usage {
    let cpu = after.cpu.saturating_sub(before.cpu);
    Usage {
        cpu_percent: 100.0 * cpu.as_secs_f64() / wall.as_secs_f64(),
        cpu_us_per_forward: cpu.as_secs_f64() * 1e6 / forwards.max(1) as f64,
        memory_mib: after.memory as f64 / (1 << 20) as f64,
    }
}

#[derive(Debug, Serialize)]
struct Results {
    target: String,
    /// What publishers sent; for video, packets below are frames.
    media: &'static str,
    video_kbps: Option<u32>,
    fps: Option<u32>,
    video: Option<VideoCounts>,
    /// Frames received but not decodable (waiting for a keyframe), as a
    /// share of those expected.
    undecodable_percent: Option<f64>,
    url: String,
    rooms: u32,
    publishers: u32,
    subscribers: u32,
    clients: usize,
    clients_per_room: u32,
    join_failures: usize,
    publishers_ready: u32,
    subscribers_ready: u32,
    setup_ms_p50: u64,
    setup_ms_p99: u64,
    measured_s: f64,
    packets_sent: u64,
    receipts_expected: u64,
    receipts_unique: u64,
    loss_percent: f64,
    duplicates: u64,
    late: u64,
    forwarded_per_s: f64,
    latency_ms_p50: f64,
    latency_ms_p90: f64,
    latency_ms_p99: f64,
    latency_ms_p999: f64,
    latency_ms_max: f64,
    loss_down_percent: f64,
    loss_up_percent: f64,
    dropped_down: u64,
    dropped_up: u64,
    send_errors: u64,
    server: Option<Usage>,
    load_generator: Usage,
}

fn print(r: &Results) {
    println!(
        "{} at {}: {} rooms × ({} + {}) {}, {:.1}s measured",
        r.target, r.url, r.rooms, r.publishers, r.subscribers, r.media, r.measured_s
    );
    println!(
        "  clients   {} joined of {}; ready: {} publishers, {} subscribers; setup p50 {} ms, p99 {} ms",
        r.clients - r.join_failures,
        r.clients,
        r.publishers_ready,
        r.subscribers_ready,
        r.setup_ms_p50,
        r.setup_ms_p99
    );
    println!(
        "  packets   {} sent, {} of {} receipts ({:.3}% lost), {} duplicates, {} later than {} ms",
        r.packets_sent,
        r.receipts_unique,
        r.receipts_expected,
        r.loss_percent,
        r.duplicates,
        r.late,
        stats::LATE_US / 1000
    );
    if let (Some(v), Some(pct)) = (&r.video, r.undecodable_percent) {
        println!(
            "  video     {} kbit/s at {} fps; {} undecodable ({:.3}%), {} keyframes, {} keyframe requests to publishers, {} PLIs from subscribers",
            r.video_kbps.unwrap_or_default(),
            r.fps.unwrap_or_default(),
            v.undecodable,
            pct,
            v.keyframes,
            v.keyframe_requests,
            v.plis_sent
        );
        let total: u64 = v.layer_frames.iter().sum::<u64>().max(1);
        let pct = |n: u64| 100.0 * n as f64 / total as f64;
        println!(
            "  layers    frames received on q {:.1}%, h {:.1}%, f {:.1}%",
            pct(v.layer_frames[0]),
            pct(v.layer_frames[1]),
            pct(v.layer_frames[2])
        );
    }
    if let Some(v) = &r.video
        && v.link_drops > 0
    {
        println!(
            "  downlink  {} packets dropped by the full queue",
            v.link_drops
        );
    }
    if r.loss_down_percent > 0.0 || r.loss_up_percent > 0.0 {
        println!(
            "  injected  {}% down ({} dropped), {}% up ({} dropped)",
            r.loss_down_percent, r.dropped_down, r.loss_up_percent, r.dropped_up
        );
    }
    println!(
        "  latency   p50 {:.2} ms, p90 {:.2}, p99 {:.2}, p99.9 {:.2}, max {:.2}",
        r.latency_ms_p50, r.latency_ms_p90, r.latency_ms_p99, r.latency_ms_p999, r.latency_ms_max
    );
    println!("  rate      {:.0} forwarded/s", r.forwarded_per_s);
    if let Some(s) = &r.server {
        println!(
            "  server    {:.1}% of a core, {:.1} µs per forward, {:.0} MiB",
            s.cpu_percent, s.cpu_us_per_forward, s.memory_mib
        );
    }
    println!(
        "  load gen  {:.1}% of a core, {:.0} MiB",
        r.load_generator.cpu_percent, r.load_generator.memory_mib
    );
    if r.send_errors > 0 {
        println!("  {} UDP sends failed", r.send_errors);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration("10"), Ok(Duration::from_secs(10)));
        assert_eq!(parse_duration("1.5s"), Ok(Duration::from_millis(1500)));
        assert_eq!(parse_duration("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert!(parse_duration("2h").is_err());
        assert_eq!(parse_ms("60"), Ok(Duration::from_millis(60)));
        assert_eq!(parse_ms("60ms"), Ok(Duration::from_millis(60)));
        assert_eq!(parse_ms("1s"), Ok(Duration::from_secs(1)));
    }
}
