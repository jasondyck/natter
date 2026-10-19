//! Webhooks as LiveKit sends them (`docs/livekit/protocol.md` §5): a
//! signed JSON `WebhookEvent` POSTed to each configured URL for room,
//! participant and track events.
//!
//! As LiveKit's `ResourceURLNotifier`: events queue per resource (the
//! room, mostly), so one room's slow deliveries don't hold up another's
//! and a room's events arrive in order; a queue holds at most 200, and an
//! event that waited more than 30 s is dropped. Failed sends are retried
//! as Go's `retryablehttp` does by default: four more tries, backing off
//! from 1 s, on connection errors, 429 and 5xx other than 501.
//!
//! The body is protobuf JSON, its SHA-256 in the `sha256` claim of a
//! 5-minute token signed with the webhook API key, in `Authorization`
//! (no `Bearer`), so LiveKit's `webhook.Receive` verifies it.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use base64::Engine as _;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use livekit_protocol as lk;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::ids;
use super::token::{self, ClaimGrants};

/// Events a resource's queue holds (LiveKit `MaxDepth`).
const MAX_DEPTH: usize = 200;
/// Older events are dropped rather than sent (LiveKit `MaxAge`).
const MAX_AGE: Duration = Duration::from_secs(30);
/// Retries after a failed send, and the backoff (Go `retryablehttp`).
const RETRIES: u32 = 4;
const RETRY_WAIT_MIN: Duration = Duration::from_secs(1);
const RETRY_WAIT_MAX: Duration = Duration::from_secs(30);
/// One attempt's limit (chosen: LiveKit sets none, which lets one dead
/// endpoint hold a room's events until they age out).
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the token on each request is valid (LiveKit).
const TOKEN_TTL: Duration = Duration::from_secs(5 * 60);

pub const ROOM_STARTED: &str = "room_started";
pub const ROOM_FINISHED: &str = "room_finished";
pub const PARTICIPANT_JOINED: &str = "participant_joined";
pub const PARTICIPANT_LEFT: &str = "participant_left";
pub const PARTICIPANT_CONNECTION_ABORTED: &str = "participant_connection_aborted";
pub const TRACK_PUBLISHED: &str = "track_published";
pub const TRACK_UNPUBLISHED: &str = "track_unpublished";

pub struct Config {
    pub urls: Vec<String>,
    pub api_key: String,
    pub api_secret: String,
    /// LiveKit's `filter_params`: only these events, or all but these.
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

impl Config {
    fn allows(&self, event: &str) -> bool {
        if !self.include.is_empty() {
            return self.include.iter().any(|e| e == event);
        }
        !self.exclude.iter().any(|e| e == event)
    }
}

/// Where events go: cheap to clone, and callable from the engine threads.
#[derive(Clone)]
pub struct Webhooks {
    tx: mpsc::UnboundedSender<lk::WebhookEvent>,
    config: Arc<Config>,
    /// Deliveries (an event to one URL) queued and not yet done.
    pending: Arc<AtomicUsize>,
}

impl Webhooks {
    /// Starts delivery on the current Tokio runtime.
    pub fn start(config: Config) -> anyhow::Result<Self> {
        for url in &config.urls {
            let uri: hyper::Uri = url
                .parse()
                .map_err(|e| anyhow::anyhow!("webhook URL {url}: {e}"))?;
            if uri.scheme_str() == Some("https") && !cfg!(feature = "tls") {
                anyhow::bail!(
                    "webhook URL {url}: https needs a build with --features tls (or an http URL)"
                );
            }
        }
        let config = Arc::new(config);
        let pending = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(dispatch(rx, Arc::clone(&config), Arc::clone(&pending)));
        info!(urls = ?config.urls, "webhooks on");
        Ok(Webhooks {
            tx,
            config,
            pending,
        })
    }

    /// Waits up to `wait` for queued events to be delivered (or given up
    /// on), so the last ones (`room_finished`) go out before the process
    /// exits.
    pub async fn flush(&self, wait: Duration) {
        let deadline = Instant::now() + wait;
        while self.pending.load(Ordering::Acquire) > 0 {
            if Instant::now() >= deadline {
                warn!(
                    pending = self.pending.load(Ordering::Acquire),
                    "stopping with webhooks undelivered"
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Queues an event, stamped with its id and time.
    pub fn notify(&self, mut event: lk::WebhookEvent) {
        if !self.config.allows(&event.event) {
            return;
        }
        event.id = ids::new("EV_");
        event.created_at = crate::util::unix_secs() as i64;
        self.pending
            .fetch_add(self.config.urls.len(), Ordering::AcqRel);
        if self.tx.send(event).is_err() {
            self.pending
                .fetch_sub(self.config.urls.len(), Ordering::AcqRel);
        }
    }
}

/// LiveKit's `EventKey`: the resource an event is about.
fn event_key(e: &lk::WebhookEvent) -> String {
    if let Some(r) = &e.room {
        return r.name.clone();
    }
    if let Some(p) = &e.participant {
        return p.identity.clone();
    }
    if let Some(t) = &e.track {
        return t.sid.clone();
    }
    "default".into()
}

type Queues = Arc<Mutex<HashMap<(usize, String), VecDeque<(Instant, lk::WebhookEvent)>>>>;

async fn dispatch(
    mut rx: mpsc::UnboundedReceiver<lk::WebhookEvent>,
    config: Arc<Config>,
    pending: Arc<AtomicUsize>,
) {
    let client = Client::new();
    let queues: Queues = Arc::default();
    while let Some(event) = rx.recv().await {
        let key = event_key(&event);
        for url in 0..config.urls.len() {
            let mut q = queues.lock().expect("webhook queues");
            let entry = q.entry((url, key.clone()));
            let start = matches!(entry, std::collections::hash_map::Entry::Vacant(_));
            let queue = entry.or_default();
            if queue.len() >= MAX_DEPTH {
                info!(event = %event.event, url = %config.urls[url], "dropped webhook: queue is full");
                pending.fetch_sub(1, Ordering::AcqRel);
                continue;
            }
            queue.push_back((Instant::now(), event.clone()));
            drop(q);
            // A queue's worker runs while it has events, then goes.
            if start {
                tokio::spawn(worker(
                    Arc::clone(&queues),
                    (url, key.clone()),
                    client.clone(),
                    Arc::clone(&config),
                    Arc::clone(&pending),
                ));
            }
        }
    }
}

async fn worker(
    queues: Queues,
    key: (usize, String),
    client: Client,
    config: Arc<Config>,
    pending: Arc<AtomicUsize>,
) {
    let url = &config.urls[key.0];
    loop {
        let next = {
            let mut q = queues.lock().expect("webhook queues");
            match q.get_mut(&key).and_then(VecDeque::pop_front) {
                Some(item) => item,
                None => {
                    q.remove(&key);
                    return;
                }
            }
        };
        let (queued_at, event) = next;
        if queued_at.elapsed() > MAX_AGE {
            info!(event = %event.event, %url, "dropped webhook: too old");
        } else {
            match send(&client, url, &config, &event).await {
                Ok(status) => info!(event = %event.event, %url, %status, "sent webhook"),
                Err(e) => warn!(event = %event.event, %url, "failed to send webhook: {e}"),
            }
        }
        pending.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The signed body and its `Authorization` token.
pub fn sign(config: &Config, event: &lk::WebhookEvent) -> (Vec<u8>, String) {
    let body = serde_json::to_vec(event).expect("event serialises");
    let sum = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(&body));
    let grants = ClaimGrants {
        sha256: sum,
        ..Default::default()
    };
    let token = token::sign(
        &config.api_key,
        &config.api_secret,
        &grants,
        TOKEN_TTL,
        SystemTime::now(),
    );
    (body, token)
}

async fn send(
    client: &Client,
    url: &str,
    config: &Config,
    event: &lk::WebhookEvent,
) -> Result<u16, String> {
    let (body, token) = sign(config, event);
    let mut wait = RETRY_WAIT_MIN;
    let mut last = String::new();
    for attempt in 0..=RETRIES {
        if attempt > 0 {
            debug!(%url, attempt, ?wait, "retrying webhook");
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(RETRY_WAIT_MAX);
        }
        let req = hyper::Request::post(url)
            .header(hyper::header::AUTHORIZATION, &token)
            .header(hyper::header::CONTENT_TYPE, "application/webhook+json")
            .header(hyper::header::USER_AGENT, "LiveKit")
            .body(Full::new(Bytes::from(body.clone())))
            .map_err(|e| e.to_string())?;
        match tokio::time::timeout(SEND_TIMEOUT, client.request(req)).await {
            Ok(Ok(res)) => {
                let status = res.status().as_u16();
                let _ = res.into_body().collect().await;
                let retry = status == 429 || (status >= 500 && status != 501);
                if !retry {
                    return if status < 400 {
                        Ok(status)
                    } else {
                        Err(format!("status {status}"))
                    };
                }
                last = format!("status {status}");
            }
            Ok(Err(e)) => last = e.to_string(),
            Err(_) => last = "timed out".into(),
        }
    }
    Err(format!("giving up after {} attempts: {last}", RETRIES + 1))
}

#[cfg(not(feature = "tls"))]
type Connector = hyper_util::client::legacy::connect::HttpConnector;
#[cfg(feature = "tls")]
type Connector = hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>;

#[derive(Clone)]
struct Client(hyper_util::client::legacy::Client<Connector, Full<Bytes>>);

impl Client {
    fn new() -> Self {
        #[cfg(not(feature = "tls"))]
        let connector = hyper_util::client::legacy::connect::HttpConnector::new();
        #[cfg(feature = "tls")]
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        Client(
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build(connector),
        )
    }

    fn request(
        &self,
        req: hyper::Request<Full<Bytes>>,
    ) -> hyper_util::client::legacy::ResponseFuture {
        self.0.request(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_the_body_hash() {
        let config = Config {
            urls: vec![],
            api_key: "key".into(),
            api_secret: "secret-secret-secret-secret-secret".into(),
            include: vec![],
            exclude: vec![],
        };
        let event = lk::WebhookEvent {
            event: ROOM_STARTED.into(),
            room: Some(lk::Room {
                name: "r".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (body, jwt) = sign(&config, &event);
        let keys = token::Keys::parse("key: secret-secret-secret-secret-secret").unwrap();
        let (_, grants) = token::verify(&jwt, &keys, SystemTime::now()).unwrap();
        let sum = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(&body));
        assert_eq!(grants.sha256, sum);
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["event"], "room_started");
        assert_eq!(json["room"]["name"], "r");
    }

    #[test]
    fn filters_as_livekit() {
        let mut c = Config {
            urls: vec![],
            api_key: String::new(),
            api_secret: String::new(),
            include: vec![],
            exclude: vec!["track_published".into()],
        };
        assert!(c.allows(ROOM_STARTED) && !c.allows(TRACK_PUBLISHED));
        c.include = vec![ROOM_STARTED.into()];
        assert!(c.allows(ROOM_STARTED) && !c.allows(ROOM_FINISHED));
    }
}
