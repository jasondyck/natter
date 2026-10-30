//! Starting the server: the engines and their sockets, TURN, the cluster,
//! the HTTP listeners, and the orderly stop on a signal.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tracing::{error, info, warn};

use crate::auth::Auth;
use crate::config::Settings;
use crate::http::{self, AppState};
use crate::livekit;
use crate::metrics::Metrics;
use crate::{first_invite, open_auth, turn, util};

/// Kernel buffer asked for on the media socket. The default (~208KB) holds
/// only a few milliseconds of a busy server's packets, so a slow pass of the
/// SFU loop drops audio instead of catching up. Joins are the slowest
/// passes (DTLS handshakes on the engine thread): with 1 MiB, 240 clients
/// joining one engine at once overflowed it, and the dropped ICE and DTLS
/// packets waited for retransmits (setup p99 1.2 s at 60 rooms; 4 MiB:
/// 0.6 s).
const UDP_BUFFER: usize = 4 << 20;

/// Runs the server until SIGTERM or Ctrl-C.
pub async fn serve(mut config: Settings) -> Result<()> {
    let auth = open_auth(&config)?;
    if auth.store.user_count()? == 0 {
        let token = first_invite(&auth)?;
        warn!(
            "no accounts yet; create the first one at {}",
            auth.invite_url(&token)
        );
    }

    livekit::rtc_config::install_crypto(config.dtls_crypto);

    // One node: its id in every shard's ServerInfo and in LiveKit's metrics.
    let node_id = livekit::ids::new(livekit::ids::NODE);
    let metrics = Arc::new(Metrics::new(&node_id));
    let shards = config.shards.max(1);
    let placement = Arc::new(livekit::placement::Placement::new(
        shards,
        config.shard_fill,
    ));

    let webhooks = match config.webhook.take() {
        Some(w) => Some(livekit::webhook::Webhooks::start(w)?),
        None => None,
    };
    let recordings = Arc::new(crate::egress::Recordings::new(
        config.egress_dir.clone(),
        webhooks.clone(),
    ));
    let keys = Arc::new(api_keys(&config)?);
    // Multi-node: this node joins the others in Redis.
    let cluster = match config.cluster.clone() {
        Some(c) => {
            let url = c.url.clone();
            let cluster = livekit::cluster::Cluster::connect(c, &node_id, Arc::clone(&keys))
                .await
                .context("joining the cluster")?;
            info!(node = %node_id, %url, "joined the cluster");
            Some(cluster)
        }
        None => None,
    };
    // Agent workers register here, whichever shard their rooms are on.
    let agents = livekit::agent::Agents::new(
        Arc::clone(&keys),
        livekit::engine::server_info(&node_id, &config.region),
        cluster.clone(),
    );
    if let Some(c) = &cluster {
        c.start(Arc::clone(&placement), Arc::clone(&metrics), agents.clone());
    }

    let sockets = bind_media(&config, shards)?;
    let mut tcp_links = match config.tcp_port {
        Some(port) => {
            let listener = std::net::TcpListener::bind((config.udp_bind, port))
                .with_context(|| format!("binding TCP {}:{port}", config.udp_bind))?;
            let engines = sockets
                .iter()
                .map(UdpSocket::local_addr)
                .collect::<std::io::Result<Vec<_>>>()?;
            livekit::tcp::start(listener, config.public_ip, &engines)?
                .into_iter()
                .map(Some)
                .collect()
        }
        None => vec![],
    };
    tcp_links.resize_with(shards, || None);
    // An engine per CPU, so the scheduler doesn't move them: an engine
    // sleeps and wakes per packet when quiet, and each move lands it on a
    // cold cache (about 15% of the CPU at 100 rooms). Not when there are
    // more engines than CPUs this process may use.
    let cpus = match util::allowed_cpus() {
        cpus if config.pin_engines && cpus.len() >= shards => cpus,
        _ => vec![],
    };
    if !cpus.is_empty() {
        info!(cpus = ?&cpus[..shards], "engines pinned");
    }
    let mut handles = vec![];
    for (shard, (socket, tcp)) in sockets.into_iter().zip(tcp_links).enumerate() {
        let advertised = SocketAddr::new(config.public_ip, socket.local_addr()?.port());
        let tcp_advertised = tcp.as_ref().map(|t| t.local);
        let (tx, rx) = std::sync::mpsc::channel();
        let (tx, wake) = livekit::engine::Commands::new(tx, socket.local_addr()?)?;
        let (engine_metrics, engine_placement) = (Arc::clone(&metrics), Arc::clone(&placement));
        let opts = livekit::engine::Options {
            max_participants: config.max_peers,
            remote_unmute: config.remote_unmute,
            room_defaults: config.rooms,
            region: config.region.clone(),
            node_id: node_id.clone(),
            recordings: Arc::clone(&recordings),
        };
        let webhooks = webhooks.clone();
        let (agents, back) = (agents.clone(), tx.clone());
        let cpu = cpus.get(shard).copied();
        std::thread::Builder::new()
            .name(format!("rtc-{shard}"))
            .spawn(move || {
                if let Some(cpu) = cpu {
                    util::pin_to_cpu(cpu);
                }
                let run = AssertUnwindSafe(|| {
                    livekit::engine::run(
                        shard,
                        engine_placement,
                        socket,
                        advertised,
                        tcp,
                        opts,
                        webhooks,
                        agents,
                        back,
                        wake,
                        rx,
                        engine_metrics,
                    )
                });
                if std::panic::catch_unwind(run).is_err() {
                    // Without its engine a shard's calls are dead while HTTP
                    // still answers, so exit and let the service manager
                    // start afresh.
                    error!(shard, "RTC engine panicked; exiting");
                    std::process::exit(1);
                }
            })?;
        handles.push(livekit::engine::Shard {
            commands: tx,
            advertised,
            tcp: tcp_advertised,
        });
    }
    let advertised = handles[0].advertised;
    let turn = start_turn(&config, &handles)?;

    purge_hourly(Arc::clone(&auth));
    trim_when_idle(Arc::clone(&metrics));

    let state = AppState {
        rtc: livekit::signal::RtcState {
            engine: livekit::engine::Handle {
                shards: handles,
                placement,
                media: config.media,
                turn,
            },
            keys,
            agents,
            ice_servers: Arc::new(config.ice_servers.clone()),
            auto_create: config.rooms.auto_create,
            metrics: Arc::clone(&metrics),
            next_conn: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            whip: Arc::default(),
            recordings,
            cluster: cluster.clone(),
        },
        auth,
        metrics: Arc::clone(&metrics),
    };
    let engine = state.rtc.engine.clone();
    let app = http::app(state, &config.static_dir);

    if let Some(p) = config.prometheus.clone() {
        http::start_prometheus(p, Arc::clone(&metrics)).await?;
    }

    let mut listeners = vec![];
    for addr in &config.http_addrs {
        listeners.push(
            tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("binding {addr}"))?,
        );
    }
    info!(
        http = ?config.http_addrs,
        origin = %config.origin,
        media = %advertised,
        shards,
        dtls_crypto = %config.dtls_crypto,
        static_dir = %config.static_dir.display(),
        "natter listening"
    );
    // Every address serves the same app; the first to stop (on a signal)
    // stops the server.
    let (stop_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let mut servers = tokio::task::JoinSet::new();
    for listener in listeners {
        let (app, mut stop) = (app.clone(), stop_tx.subscribe());
        servers.spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = stop.recv().await;
                })
                .await
        });
    }
    shutdown_signal().await;
    if let Some(c) = &cluster {
        c.drain().await;
    }
    // As LiveKit's `RoomManager.Stop`: every room closes (SERVER_SHUTDOWN),
    // and its last webhooks get a moment to go out (both waits chosen).
    engine.shutdown(Duration::from_secs(5)).await;
    if let Some(w) = &webhooks {
        w.flush(Duration::from_secs(5)).await;
    }
    if let Some(c) = &cluster {
        c.stop().await;
    }
    let _ = stop_tx.send(());
    while let Some(r) = servers.join_next().await {
        r??;
    }
    Ok(())
}

/// The configured API keys, or a fresh internal one with which natter
/// still mints tokens for its own web client.
fn api_keys(config: &Settings) -> Result<livekit::token::Keys> {
    for (key, secret) in config.keys.iter() {
        if secret.len() < livekit::token::MIN_SECRET_LEN {
            warn!(
                key,
                "API secret shorter than 32 characters; use a longer one"
            );
        }
    }
    if !config.keys.is_empty() {
        return Ok(config.keys.clone());
    }
    if config.cluster.is_some() {
        bail!("several nodes need the same API keys (keys, key_file or LIVEKIT_KEYS)");
    }
    let secret = hex::encode(util::random_bytes::<32>());
    let keys = livekit::token::Keys::parse(&format!("natter-internal: {secret}"))
        .map_err(anyhow::Error::msg)?;
    info!("LIVEKIT_KEYS not set; /rtc only accepts tokens natter mints itself");
    Ok(keys)
}

/// One socket per engine, each multiplexing its clients' media on its own
/// port: NATTER_UDP_PORT, +1, +2...
fn bind_media(config: &Settings, shards: usize) -> Result<Vec<UdpSocket>> {
    (0..shards)
        .map(|shard| {
            let port = config
                .udp_port
                .checked_add(shard as u16)
                .context("NATTER_UDP_PORT + NATTER_SHARDS overflows")?;
            let socket = UdpSocket::bind((config.udp_bind, port)).with_context(|| {
                format!(
                    "binding UDP {}:{port} (set NATTER_UDP_BIND=0.0.0.0 if the public IP is not on this host)",
                    config.udp_bind
                )
            })?;
            size_socket_buffers(&socket);
            Ok(socket)
        })
        .collect()
}

fn size_socket_buffers(socket: &UdpSocket) {
    let sock = socket2::SockRef::from(socket);
    let _ = sock.set_recv_buffer_size(UDP_BUFFER);
    let _ = sock.set_send_buffer_size(UDP_BUFFER);
    // Linux caps the request at net.core.rmem_max/wmem_max and reports
    // double what it grants, for its own bookkeeping.
    let recv = sock.recv_buffer_size().unwrap_or(0) / 2;
    let send = sock.send_buffer_size().unwrap_or(0) / 2;
    if recv < UDP_BUFFER || send < UDP_BUFFER {
        warn!(
            recv,
            send,
            wanted = UDP_BUFFER,
            "UDP buffers capped; raise net.core.rmem_max and net.core.wmem_max"
        );
    }
}

/// natter's own TURN server, if configured, relaying to the engines.
fn start_turn(config: &Settings, shards: &[livekit::engine::Shard]) -> Result<Option<turn::Turn>> {
    let Some(port) = config.turn_port else {
        if config.turn_tls.is_some() {
            bail!("NATTER_TURN_TLS needs NATTER_TURN_PORT");
        }
        return Ok(None);
    };
    // Relays reach the engines where they listen; loopback when that is
    // every address.
    let local_ip = if config.udp_bind.is_unspecified() {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    } else {
        config.udp_bind
    };
    let peers = shards
        .iter()
        .map(|s| (s.advertised, SocketAddr::new(local_ip, s.advertised.port())))
        .collect();
    let turn = turn::start(turn::Config {
        port,
        tls: config.turn_tls.clone(),
        bind: config.udp_bind,
        public_ip: config.public_ip,
        peers,
    })
    .context("starting TURN")?;
    Ok(Some(turn))
}

/// Deletes expired sessions and invites once an hour.
fn purge_hourly(auth: Arc<Auth>) {
    tokio::spawn(async move {
        let mut hourly = tokio::time::interval(Duration::from_secs(60 * 60));
        loop {
            hourly.tick().await;
            let auth = Arc::clone(&auth);
            match tokio::task::spawn_blocking(move || auth.store.purge_expired()).await {
                Ok(Ok((0, 0))) => {}
                Ok(Ok((sessions, invites))) => {
                    info!(sessions, invites, "purged expired");
                }
                Ok(Err(e)) => warn!("purging expired sessions: {e:#}"),
                Err(e) => warn!("purging expired sessions: {e}"),
            }
        }
    });
}

/// Hands freed heap back to the kernel once the last peer has gone. glibc
/// keeps what calls churned through in its per-thread arenas, so without
/// this the resident size of an idle server sits at its high-water mark
/// (≈115 MiB after a soak, with under 2 MiB of it live).
fn trim_when_idle(metrics: Arc<Metrics>) {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        let mut busy = false;
        loop {
            tick.tick().await;
            if metrics.peers.load(std::sync::atomic::Ordering::Relaxed) > 0 {
                busy = true;
                continue;
            }
            if !std::mem::take(&mut busy) {
                continue;
            }
            let trim = tokio::task::spawn_blocking(|| {
                let (before, start) = (util::resident_kib(), std::time::Instant::now());
                // SAFETY: no arguments to get wrong; glibc locks each arena.
                unsafe { libc::malloc_trim(0) };
                (before, util::resident_kib(), start.elapsed())
            });
            if let Ok((before, after, took)) = trim.await {
                info!(before_kib = before, after_kib = after, ?took, "idle: trimmed heap");
            }
        }
    });
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    let _ = metrics;
}

/// Ctrl-C, or SIGTERM from systemd or Kubernetes. As a container's PID 1
/// the process would otherwise ignore SIGTERM and be killed after the grace
/// period instead of closing the database cleanly.
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    info!("shutting down");
}
