mod auth;
mod config;
mod livekit;
mod metrics;
mod netio;
mod store;
mod turn;
mod util;

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use axum::Router;
use axum::extract::{FromRef, State};
use axum::http::{HeaderName, HeaderValue, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tower_http::services::ServeDir;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use auth::Auth;
use metrics::Metrics;
use store::Store;

#[derive(Clone, FromRef)]
struct AppState {
    rtc: livekit::signal::RtcState,
    auth: Arc<Auth>,
    metrics: Arc<Metrics>,
}

/// Kernel buffer asked for on the media socket. The default (~208KB) holds
/// only a few milliseconds of a busy server's packets, so a slow pass of the
/// SFU loop drops audio instead of catching up. Joins are the slowest
/// passes (DTLS handshakes on the engine thread): with 1 MiB, 240 clients
/// joining one engine at once overflowed it, and the dropped ICE and DTLS
/// packets waited for retransmits (setup p99 1.2 s at 60 rooms; 4 MiB:
/// 0.6 s).
const UDP_BUFFER: usize = 4 << 20;

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

/// Prometheus text format. Not authenticated: keep the HTTP listener private
/// (behind the TLS proxy) and scrape it from there.
async fn serve_metrics(State(metrics): State<Arc<Metrics>>) -> Response {
    let mut response = metrics.render().into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    response
}

/// A header added to every response.
fn always(name: HeaderName, value: &'static str) -> SetResponseHeaderLayer<HeaderValue> {
    SetResponseHeaderLayer::overriding(name, HeaderValue::from_static(value))
}

const USAGE: &str = "commands:
  generate-keys       print a new API key and secret (as LiveKit's)
  ports               print the ports the configuration uses (as LiveKit's)
run with the server stopped:
  invite              print a new invite link
  users               list accounts
  delete-user NAME    delete an account, its sessions and its invites
  revoke-invites      delete every unused invite";

/// Account administration from the command line. These open the database
/// directly, so they work when nobody can sign in.
fn admin(auth: &Auth, args: &[String]) -> Result<()> {
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["invite"] => println!("{}", auth.invite_url(&first_invite(auth)?)),
        ["users"] => {
            for user in auth.store.users()? {
                println!("{}\t{} passkey(s)", user.name, user.passkeys.len());
            }
        }
        ["delete-user", name] => {
            if !auth.store.delete_user(name)? {
                bail!("no account named {name:?}");
            }
            println!("deleted {name}");
        }
        ["revoke-invites"] => println!("revoked {} invite(s)", auth.store.revoke_invites()?),
        _ => bail!("{USAGE}"),
    }
    Ok(())
}

/// An invite made by the server rather than a user, so it has no cap.
fn first_invite(auth: &Auth) -> Result<String> {
    Ok(auth
        .store
        .create_invite(None, usize::MAX)?
        .expect("uncapped"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cli = config::Cli::parse(&args)?;
    if cli.command == ["generate-keys"] {
        println!("{}", config::generate_keys());
        return Ok(());
    }
    let mut config = config::Settings::load(&cli)?;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&config.log_filter)),
        )
        .init();
    for note in &config.notes {
        warn!("{note}");
    }
    if cli.command == ["ports"] {
        println!("{}", config.ports());
        return Ok(());
    }
    if cli.command == ["list-nodes"] {
        let Some(c) = config.cluster.clone() else {
            bail!("list-nodes needs redis configured");
        };
        let cluster = livekit::cluster::Cluster::connect_read_only(c).await?;
        println!("{}", livekit::cluster::node_table(&cluster.nodes().await?));
        return Ok(());
    }
    std::fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("creating {}", config.data_dir.display()))?;
    let db_path = config.data_dir.join("natter.redb");
    let store = Store::open(&db_path).with_context(|| {
        format!(
            "opening {} (only one process can, so stop the server to run `invite`)",
            db_path.display()
        )
    })?;
    let auth = Arc::new(Auth::new(config.origin.clone(), store)?);

    if !cli.command.is_empty() {
        return admin(&auth, &cli.command);
    }

    if auth.store.user_count()? == 0 {
        let token = first_invite(&auth)?;
        warn!(
            "no accounts yet; create the first one at {}",
            auth.invite_url(&token)
        );
    }

    livekit::rtc_config::install_crypto(config.dtls_crypto);

    // One engine per shard, each multiplexing its clients' media over one
    // socket on its own port: NATTER_UDP_PORT, +1, +2...
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
    let mut keys = config.keys.clone();
    for (key, secret) in keys.iter() {
        if secret.len() < livekit::token::MIN_SECRET_LEN {
            warn!(
                key,
                "API secret shorter than 32 characters; use a longer one"
            );
        }
    }
    if keys.is_empty() {
        // natter still mints tokens for its own web client.
        let secret = hex::encode(util::random_bytes::<32>());
        keys = livekit::token::Keys::parse(&format!("natter-internal: {secret}"))
            .map_err(anyhow::Error::msg)?;
        info!("LIVEKIT_KEYS not set; /rtc only accepts tokens natter mints itself");
    }

    let keys = Arc::new(keys);
    if config.cluster.is_some() && config.keys.is_empty() {
        bail!("several nodes need the same API keys (keys, key_file or LIVEKIT_KEYS)");
    }
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
    for shard in 0..shards {
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
        let advertised = SocketAddr::new(config.public_ip, socket.local_addr()?.port());
        size_socket_buffers(&socket);
        // ICE/TCP for clients whose network blocks UDP.
        let tcp = match config.tcp_port {
            Some(base) => {
                let port = base
                    .checked_add(shard as u16)
                    .context("NATTER_TCP_PORT + NATTER_SHARDS overflows")?;
                let listener = std::net::TcpListener::bind((config.udp_bind, port))
                    .with_context(|| format!("binding TCP {}:{port}", config.udp_bind))?;
                Some(livekit::tcp::start(
                    shard,
                    listener,
                    advertised,
                    socket.local_addr()?,
                )?)
            }
            None => None,
        };
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
    let media = config.media;

    let turn = match config.turn_port {
        Some(port) => {
            // Relays reach the engines where they listen; loopback when
            // that is every address.
            let local_ip = if config.udp_bind.is_unspecified() {
                IpAddr::V4(Ipv4Addr::LOCALHOST)
            } else {
                config.udp_bind
            };
            let peers = handles
                .iter()
                .map(|s| (s.advertised, SocketAddr::new(local_ip, s.advertised.port())))
                .collect();
            let tls = config.turn_tls.clone();
            Some(
                turn::start(turn::Config {
                    port,
                    tls,
                    bind: config.udp_bind,
                    public_ip: config.public_ip,
                    peers,
                })
                .context("starting TURN")?,
            )
        }
        None if config.turn_tls.is_some() => bail!("NATTER_TURN_TLS needs NATTER_TURN_PORT"),
        None => None,
    };

    let purge_auth = Arc::clone(&auth);
    tokio::spawn(async move {
        let mut hourly = tokio::time::interval(Duration::from_secs(60 * 60));
        loop {
            hourly.tick().await;
            let auth = Arc::clone(&purge_auth);
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

    let metrics_for_prometheus = Arc::clone(&metrics);
    let state = AppState {
        rtc: livekit::signal::RtcState {
            engine: livekit::engine::Handle {
                shards: handles,
                placement,
                media,
                turn,
            },
            keys,
            agents,
            ice_servers: Arc::new(config.ice_servers.clone()),
            auto_create: config.rooms.auto_create,
            metrics: Arc::clone(&metrics),
            next_conn: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            whip: Arc::default(),
            cluster: cluster.clone(),
        },
        auth,
        metrics,
    };
    let engine = state.rtc.engine.clone();
    let whip_limit = axum::extract::DefaultBodyLimit::max(livekit::whip::MAX_BODY);
    let app = Router::new()
        .route("/rtc", get(livekit::signal::rtc))
        .route("/rtc/validate", get(livekit::signal::validate::<false>))
        .route("/rtc/v1/validate", get(livekit::signal::validate::<true>))
        .route("/rtc/v1", get(livekit::signal::rtc_v1))
        .route("/agent", get(livekit::agent::agent_ws))
        // Between nodes, with node tokens.
        .route(
            "/internal/agent/job",
            axum::routing::post(livekit::agent::Agents::internal_job),
        )
        .route(
            "/internal/agent/terminate",
            axum::routing::post(livekit::agent::Agents::internal_terminate),
        )
        // LiveKit's server API (Twirp).
        .route(
            "/twirp/livekit.RoomService/{method}",
            axum::routing::post(livekit::twirp::room_service),
        )
        .route(
            "/twirp/livekit.AgentDispatchService/{method}",
            axum::routing::post(livekit::twirp::agent_dispatch_service),
        )
        // Native WHIP, as LiveKit's.
        .route(
            "/whip/v1",
            axum::routing::post(livekit::whip::create)
                .get(livekit::whip::get)
                .options(livekit::whip::options)
                .layer(whip_limit),
        )
        .route(
            "/whip/v1/{id}",
            axum::routing::patch(livekit::whip::patch)
                .delete(livekit::whip::delete)
                .get(livekit::whip::get)
                .options(livekit::whip::options)
                .layer(whip_limit),
        )
        .route(
            "/api/rtc/token",
            axum::routing::post(livekit::signal::token),
        )
        .route("/metrics", get(serve_metrics))
        .merge(auth::routes())
        .with_state(state)
        .fallback_service(ServeDir::new(&config.static_dir))
        // The CSP leaves scripts alone because trunk starts the WASM from an
        // inline script. It only rules out framing (so natter can't be
        // clickjacked), plugins, and `<base>` hijacking.
        .layer(always(
            header::CONTENT_SECURITY_POLICY,
            "frame-ancestors 'none'; object-src 'none'; base-uri 'none'",
        ))
        .layer(always(header::X_FRAME_OPTIONS, "DENY"))
        .layer(always(header::X_CONTENT_TYPE_OPTIONS, "nosniff"))
        .layer(always(header::REFERRER_POLICY, "no-referrer"))
        .layer(TraceLayer::new_for_http());

    // LiveKit's Prometheus listener, with basic auth if configured.
    if let Some(p) = config.prometheus.clone() {
        let metrics = Arc::clone(&metrics_for_prometheus);
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, p.port))
            .await
            .with_context(|| format!("binding Prometheus port {}", p.port))?;
        let auth = p.auth.map(|(u, pw)| {
            use base64::Engine as _;
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("{u}:{pw}"))
            )
        });
        let app = Router::new()
            .route("/metrics", get(serve_metrics))
            .route_layer(axum::middleware::from_fn(
                move |req: axum::extract::Request, next: axum::middleware::Next| {
                    let auth = auth.clone();
                    async move {
                        let ok = auth.as_deref().is_none_or(|want| {
                            req.headers()
                                .get(header::AUTHORIZATION)
                                .is_some_and(|got| got.as_bytes() == want.as_bytes())
                        });
                        if ok {
                            next.run(req).await
                        } else {
                            (
                                axum::http::StatusCode::UNAUTHORIZED,
                                [(header::WWW_AUTHENTICATE, "Basic realm=\"metrics\"")],
                            )
                                .into_response()
                        }
                    }
                },
            ))
            .with_state(metrics);
        info!(port = p.port, "Prometheus listening");
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                error!("Prometheus listener failed: {e}");
            }
        });
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
