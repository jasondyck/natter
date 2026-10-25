//! The HTTP side: every route on the main listeners, and LiveKit's separate
//! Prometheus listener.

use std::net::Ipv4Addr;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use axum::extract::{FromRef, State};
use axum::http::{HeaderName, HeaderValue, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tower_http::services::ServeDir;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

use crate::auth::{self, Auth};
use crate::config::Prometheus;
use crate::livekit;
use crate::metrics::Metrics;

#[derive(Clone, FromRef)]
pub struct AppState {
    pub rtc: livekit::signal::RtcState,
    pub auth: Arc<Auth>,
    pub metrics: Arc<Metrics>,
}

/// The app every HTTP address serves: LiveKit's endpoints, natter's
/// accounts, and the web client's files from `static_dir`.
pub fn app(state: AppState, static_dir: &Path) -> Router {
    let whip_limit = axum::extract::DefaultBodyLimit::max(livekit::whip::MAX_BODY);
    Router::new()
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
        // WHEP: the same sessions, watching.
        .route(
            "/whep/v1",
            axum::routing::post(livekit::whip::create_whep)
                .get(livekit::whip::get)
                .options(livekit::whip::options)
                .layer(whip_limit),
        )
        .route(
            "/whep/v1/{id}",
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
        .fallback_service(ServeDir::new(static_dir))
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
        .layer(TraceLayer::new_for_http())
}

/// LiveKit's Prometheus listener, with basic auth if configured. Runs until
/// the process exits.
pub async fn start_prometheus(p: Prometheus, metrics: Arc<Metrics>) -> Result<()> {
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
    Ok(())
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
