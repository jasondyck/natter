//! LiveKit's agent workers (`docs/livekit/protocol.md` §6, "Agents"):
//! `/agent`, the WebSocket agent frameworks (`livekit-agents`,
//! `agents-js`) register on, and the dispatch of jobs to them, as
//! LiveKit's `pkg/agent` and `service/agenthandler.go`.
//!
//! A worker registers for a job type (room, publisher or participant)
//! under an agent name (empty for automatic dispatch), namespace and
//! deployment. Rooms launch jobs for their dispatches (`engine::agents`);
//! each job goes to one matching worker, picked at random weighted by how
//! much load it has left. The worker is asked whether it takes the job
//! (10 s to answer); if it does, it gets the job and a token for its agent
//! participant, and the room learns which participant that is.
//!
//! Agent HTTP endpoints need a WebTransport data plane; over the WebSocket
//! LiveKit ignores them, and so does natter.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use axum::extract::ws::{
    Message, WebSocket, WebSocketUpgrade, rejection::WebSocketUpgradeRejection,
};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use livekit_protocol as lk;
use lk::{server_message, worker_message};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use super::cluster::{Cluster, NodeRecord};
use super::ids;
use super::signal::RtcState;
use super::token::{self, ClaimGrants, Keys, VideoGrant};
use super::wire;
use crate::util::{unix_ms, unix_ns};

/// The agent protocol natter speaks (LiveKit `CurrentProtocol`).
pub const PROTOCOL: i32 = 1;
/// A worker must register within this long of connecting.
const REGISTER_TIMEOUT: Duration = Duration::from_secs(10);
/// A worker must say whether it takes a job within this long.
const ASSIGN_TIMEOUT: Duration = Duration::from_secs(10);
/// LiveKit's `limit.agent_signal_message_size_limit`.
const MAX_MESSAGE: usize = 2 << 20;
/// WebSocket ping frames, as on `/rtc`.
const WS_PING_EVERY: Duration = Duration::from_secs(10);
/// How long an agent participant's token lasts.
const TOKEN_TTL: Duration = Duration::from_secs(60 * 60);
/// Set on every agent participant: the name of the agent it runs.
pub const AGENT_NAME_ATTRIBUTE: &str = "lk.agent_name";
/// LiveKit's `MaxDeploymentLength`.
const MAX_DEPLOYMENT: usize = 64;

/// LiveKit's `ValidateDeployment`: at most 64 bytes, no `_` (it separates
/// topic parts), whitespace or control bytes.
pub fn validate_deployment(deployment: &str) -> Result<(), String> {
    if deployment.len() > MAX_DEPLOYMENT {
        return Err(format!("deployment exceeds {MAX_DEPLOYMENT} bytes"));
    }
    for (i, c) in deployment.bytes().enumerate() {
        if c == b'_' {
            return Err("deployment contains reserved character '_'".into());
        }
        if c <= b' ' || c == 0x7f {
            return Err(format!(
                "deployment contains whitespace or control byte at position {i}"
            ));
        }
    }
    Ok(())
}

/// What a room asks for: a job of `job_type` for the agent `agent_name`
/// (LiveKit's `agent.JobRequest`).
#[derive(Debug, Clone)]
pub struct JobRequest {
    pub dispatch_id: String,
    pub job_type: lk::JobType,
    pub room: lk::Room,
    pub participant: Option<lk::ParticipantInfo>,
    pub metadata: String,
    pub agent_name: String,
    pub deployment: String,
    pub attributes: HashMap<String, String>,
}

/// Why a job is ended from the server's side (LiveKit's
/// `rpc.JobTerminateReason`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminateReason {
    /// Its dispatch was deleted.
    Requested,
    /// Its agent participant left the room.
    AgentLeftRoom,
}

/// Why a worker didn't take a job.
#[derive(Debug, PartialEq, Eq)]
enum AssignError {
    /// It said no: try another.
    NotAvailable,
    /// It went away: try another.
    Closed,
    Timeout,
    Duplicate,
    Token,
}

struct Worker {
    id: String,
    agent_name: String,
    namespace: String,
    job_type: lk::JobType,
    deployment: String,
    permissions: lk::ParticipantPermission,
    /// The key it connected with, which signs its agents' tokens.
    api_key: String,
    tx: mpsc::UnboundedSender<lk::ServerMessage>,
    state: Mutex<WorkerState>,
}

#[derive(Default)]
struct WorkerState {
    load: f32,
    status: lk::WorkerStatus,
    status_seq: u64,
    draining: bool,
    closed: bool,
    running: HashMap<String, lk::Job>,
    /// Jobs offered and waiting for the worker's answer.
    availability: HashMap<String, oneshot::Sender<lk::AvailabilityResponse>>,
}

impl Worker {
    fn send(&self, message: server_message::Message) -> Result<(), AssignError> {
        self.tx
            .send(lk::ServerMessage {
                message: Some(message),
            })
            .map_err(|_| AssignError::Closed)
    }

    fn available(&self) -> bool {
        let s = self.state.lock().unwrap();
        !s.closed && s.status == lk::WorkerStatus::WsAvailable
    }

    /// The worker's answer to a job it was offered.
    fn availability(&self, res: lk::AvailabilityResponse) {
        match self.state.lock().unwrap().availability.remove(&res.job_id) {
            Some(tx) => {
                let _ = tx.send(res);
            }
            None => warn!(job = %res.job_id, "availability response for unknown job"),
        }
    }

    /// LiveKit's `UpdateJobStatus`: the job's new status; ended jobs stop
    /// running.
    fn update_job(&self, update: &lk::UpdateJobStatus) -> Option<lk::JobState> {
        let mut s = self.state.lock().unwrap();
        let job = s.running.get_mut(&update.job_id)?;
        let state = job.state.get_or_insert_with(Default::default);
        let now = unix_ns();
        state.updated_at = now;
        if state.status() == lk::JobStatus::JsPending && update.status() != lk::JobStatus::JsPending
        {
            state.started_at = now;
        }
        state.status = update.status;
        state.error = update.error.clone();
        if !ended(update.status()) {
            return Some(state.clone());
        }
        state.ended_at = now;
        let state = state.clone();
        s.running.remove(&update.job_id);
        info!(worker = %self.id, job = %update.job_id, status = ?update.status(), error = %update.error, "job ended");
        Some(state)
    }

    fn update_worker(&self, update: &lk::UpdateWorkerStatus) {
        let mut s = self.state.lock().unwrap();
        if update.seq != 0 {
            if update.seq <= s.status_seq {
                return;
            }
            s.status_seq = update.seq;
        }
        if let Some(status) = update.status {
            s.status = lk::WorkerStatus::try_from(status).unwrap_or_default();
        }
        s.load = update.load;
        s.draining = update.draining;
    }
}

fn ended(status: lk::JobStatus) -> bool {
    matches!(status, lk::JobStatus::JsSuccess | lk::JobStatus::JsFailed)
}

/// The registered workers, shared by the `/agent` connections and the
/// engines. Cheap to clone.
#[derive(Clone)]
pub struct Agents(Arc<Inner>);

struct Inner {
    keys: Arc<Keys>,
    runtime: tokio::runtime::Handle,
    server_info: lk::ServerInfo,
    registry: Mutex<Registry>,
    /// The other nodes, whose workers take jobs too.
    cluster: Option<Cluster>,
    /// Workers came, went or changed status: tell the other nodes.
    changed: tokio::sync::Notify,
    /// Jobs running on another node's workers, by job id.
    remote_jobs: Mutex<HashMap<String, NodeRecord>>,
}

/// What other nodes know of a worker, to send it jobs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkerSummary {
    pub agent_name: String,
    pub namespace: String,
    pub job_type: i32,
    pub deployment: String,
    pub available: bool,
    pub load: f32,
}

/// LiveKit's `agents.target_load`: a node's affinity for a job is the
/// load its matching workers have left below this.
const TARGET_LOAD: f32 = 0.7;

/// How a job's end went (`Agents::terminate`).
#[derive(Debug)]
pub enum Termination {
    /// A worker here ran it; its final state.
    Ended(lk::JobState),
    /// A worker on another node runs it; it is told.
    Remote,
    /// No worker runs it.
    Unknown,
}

#[derive(Default)]
struct Registry {
    workers: Vec<Arc<Worker>>,
    jobs: HashMap<String, Arc<Worker>>,
}

impl Agents {
    /// On the current Tokio runtime, where jobs are dispatched from.
    pub fn new(keys: Arc<Keys>, server_info: lk::ServerInfo, cluster: Option<Cluster>) -> Self {
        Agents(Arc::new(Inner {
            keys,
            runtime: tokio::runtime::Handle::current(),
            server_info,
            registry: Mutex::default(),
            cluster,
            changed: tokio::sync::Notify::new(),
            remote_jobs: Mutex::default(),
        }))
    }

    /// The workers here, as other nodes see them.
    pub fn summary(&self) -> Vec<WorkerSummary> {
        self.0
            .registry
            .lock()
            .unwrap()
            .workers
            .iter()
            .map(|w| {
                let s = w.state.lock().unwrap();
                WorkerSummary {
                    agent_name: w.agent_name.clone(),
                    namespace: w.namespace.clone(),
                    job_type: w.job_type as i32,
                    deployment: w.deployment.clone(),
                    available: !s.closed && s.status == lk::WorkerStatus::WsAvailable,
                    load: s.load,
                }
            })
            .collect()
    }

    /// Resolves when the workers here have changed.
    pub async fn changed(&self) {
        self.0.changed.notified().await;
    }

    /// Whether a request came from another node (`Cluster::forwarded`).
    fn is_node(&self, headers: &HeaderMap) -> bool {
        self.0
            .cluster
            .as_ref()
            .is_some_and(|c| c.forwarded(headers))
    }

    fn register(&self, w: Arc<Worker>) {
        info!(
            worker = %w.id,
            agent_name = %w.agent_name,
            namespace = %w.namespace,
            job_type = ?w.job_type,
            deployment = %w.deployment,
            "worker registered"
        );
        self.0.registry.lock().unwrap().workers.push(w);
        self.0.changed.notify_one();
    }

    /// The worker has gone: no new jobs, and its running ones are no
    /// longer anyone's to end.
    fn deregister(&self, w: &Arc<Worker>) {
        let mut reg = self.0.registry.lock().unwrap();
        reg.workers.retain(|o| !Arc::ptr_eq(o, w));
        reg.jobs.retain(|_, o| !Arc::ptr_eq(o, w));
        let mut s = w.state.lock().unwrap();
        s.closed = true;
        // Offers waiting on it fail at once and go to another worker.
        s.availability.clear();
        info!(worker = %w.id, "worker deregistered");
        self.0.changed.notify_one();
    }

    fn matching(
        &self,
        agent_name: &str,
        job_type: lk::JobType,
        deployment: &str,
    ) -> Vec<Arc<Worker>> {
        self.0
            .registry
            .lock()
            .unwrap()
            .workers
            .iter()
            .filter(|w| {
                w.agent_name == agent_name && w.job_type == job_type && w.deployment == deployment
            })
            .cloned()
            .collect()
    }

    /// LiveKit's `LaunchJob`: a job per namespace that has workers for it
    /// (one, unless workers use the deprecated namespaces), each reported
    /// to `on_job` once a worker has taken it. Nothing if no worker is
    /// registered for it: jobs don't wait for workers.
    pub fn launch(&self, req: JobRequest, on_job: Arc<dyn Fn(lk::Job) + Send + Sync>) {
        let agents = self.clone();
        self.0.runtime.spawn(async move {
            // The nodes with matching workers, by namespace, with their
            // affinity (LiveKit's `JobRequestAffinity`); this node is `None`.
            let mut candidates: Vec<(String, Option<NodeRecord>, f32)> = agents
                .matching(&req.agent_name, req.job_type, &req.deployment)
                .iter()
                .map(|w| {
                    let s = w.state.lock().unwrap();
                    let free = !s.closed && s.status == lk::WorkerStatus::WsAvailable;
                    (
                        w.namespace.clone(),
                        None,
                        if free {
                            (TARGET_LOAD - s.load).max(0.0)
                        } else {
                            0.0
                        },
                    )
                })
                .collect();
            if let Some(c) = &agents.0.cluster {
                for (node, workers) in c.remote_workers().await {
                    for w in workers.iter().filter(|w| {
                        w.agent_name == req.agent_name
                            && w.job_type == req.job_type as i32
                            && w.deployment == req.deployment
                    }) {
                        let affinity = if w.available {
                            (TARGET_LOAD - w.load).max(0.0)
                        } else {
                            0.0
                        };
                        candidates.push((w.namespace.clone(), Some(node.clone()), affinity));
                    }
                }
            }
            let mut namespaces: Vec<String> = candidates.iter().map(|(n, ..)| n.clone()).collect();
            namespaces.sort();
            namespaces.dedup();
            if namespaces.is_empty() {
                info!(
                    agent_name = %req.agent_name,
                    job_type = ?req.job_type,
                    room = %req.room.name,
                    "not dispatching agent job since no worker is available"
                );
                return;
            }
            for namespace in namespaces {
                // Nodes by their total affinity, this one first among equals.
                let mut nodes: Vec<(Option<NodeRecord>, f32)> = vec![];
                for (_, node, a) in candidates.iter().filter(|(n, ..)| *n == namespace) {
                    match nodes
                        .iter_mut()
                        .find(|(o, _)| o.as_ref().map(|n| &n.id) == node.as_ref().map(|n| &n.id))
                    {
                        Some((_, sum)) => *sum += a,
                        None => nodes.push((node.clone(), *a)),
                    }
                }
                nodes.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.is_some().cmp(&b.0.is_some())));
                let job = lk::Job {
                    id: ids::new(ids::AGENT_JOB),
                    dispatch_id: req.dispatch_id.clone(),
                    r#type: req.job_type as i32,
                    room: Some(req.room.clone()),
                    participant: req.participant.clone(),
                    namespace,
                    agent_name: req.agent_name.clone(),
                    metadata: req.metadata.clone(),
                    deployment: req.deployment.clone(),
                    attributes: req.attributes.clone(),
                    ..Default::default()
                };
                let (agents, on_job) = (agents.clone(), Arc::clone(&on_job));
                tokio::spawn(async move {
                    for (node, _) in nodes {
                        let state = match &node {
                            None => agents.job_request(&job).await,
                            Some(n) => agents.remote_job(n, &job).await,
                        };
                        if let Some(state) = state {
                            if let Some(n) = node {
                                agents
                                    .0
                                    .remote_jobs
                                    .lock()
                                    .unwrap()
                                    .insert(job.id.clone(), n);
                            }
                            on_job(lk::Job {
                                state: Some(state),
                                ..job
                            });
                            return;
                        }
                    }
                });
            }
        });
    }

    /// Asks another node's workers to take `job`.
    async fn remote_job(&self, node: &NodeRecord, job: &lk::Job) -> Option<lk::JobState> {
        let c = self.0.cluster.as_ref()?;
        let resp = c
            .call(node, "/internal/agent/job", job.encode_to_vec(), false)
            .await?;
        lk::JobState::decode(resp.as_ref()).ok()
    }

    /// `POST /internal/agent/job`: another node's job for the workers here.
    pub async fn internal_job(
        State(state): State<RtcState>,
        headers: HeaderMap,
        body: axum::body::Bytes,
    ) -> Response {
        let agents = &state.agents;
        if !agents.is_node(&headers) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        let Ok(job) = lk::Job::decode(body.as_ref()) else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        match agents.job_request(&job).await {
            Some(s) => s.encode_to_vec().into_response(),
            None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        }
    }

    /// `POST /internal/agent/terminate`: another node ends a job here.
    pub async fn internal_terminate(
        State(state): State<RtcState>,
        headers: HeaderMap,
        body: axum::body::Bytes,
    ) -> Response {
        let agents = &state.agents;
        if !agents.is_node(&headers) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        let Ok(t) = lk::JobTermination::decode(body.as_ref()) else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        // The reason travels in the error field: "left" or "".
        let reason = if headers.contains_key("x-natter-agent-left") {
            TerminateReason::AgentLeftRoom
        } else {
            TerminateReason::Requested
        };
        match agents.terminate(&t.job_id, reason) {
            Termination::Ended(s) => s.encode_to_vec().into_response(),
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }

    /// LiveKit's `JobRequest`: offers the job to matching workers until one
    /// takes it, or none is left with capacity.
    async fn job_request(&self, job: &lk::Job) -> Option<lk::JobState> {
        let mut attempted: Vec<Arc<Worker>> = vec![];
        loop {
            let Some(w) = self.select(job, &attempted) else {
                warn!(job = %job.id, agent_name = %job.agent_name, "no worker available to handle job");
                return None;
            };
            attempted.push(Arc::clone(&w));
            match self.assign(&w, job.clone()).await {
                Ok(state) => {
                    if state.status() == lk::JobStatus::JsRunning {
                        info!(job = %job.id, worker = %w.id, "assigned job to worker");
                        let mut reg = self.0.registry.lock().unwrap();
                        let registered = reg.workers.iter().any(|o| Arc::ptr_eq(o, &w));
                        if registered && w.state.lock().unwrap().running.contains_key(&job.id) {
                            reg.jobs.insert(job.id.clone(), Arc::clone(&w));
                        }
                    }
                    return Some(state);
                }
                Err(e @ (AssignError::NotAvailable | AssignError::Closed)) => {
                    debug!(job = %job.id, worker = %w.id, "worker did not take job ({e:?}); trying another");
                }
                Err(e) => {
                    warn!(job = %job.id, worker = %w.id, "failed to assign job to worker: {e:?}");
                    return None;
                }
            }
        }
    }

    /// LiveKit's `selectWorkerWeightedByLoad`: a random available worker,
    /// weighted by the load it has left.
    fn select(&self, job: &lk::Job, attempted: &[Arc<Worker>]) -> Option<Arc<Worker>> {
        let candidates: Vec<(Arc<Worker>, f32)> = self
            .matching(&job.agent_name, job.r#type(), &job.deployment)
            .into_iter()
            .filter(|w| w.namespace == job.namespace)
            .filter(|w| !attempted.iter().any(|a| Arc::ptr_eq(a, w)))
            .filter(|w| w.available())
            .map(|w| {
                let left = (1.0 - w.state.lock().unwrap().load).max(0.0);
                (w, left)
            })
            .collect();
        pick_weighted(candidates, random_unit())
    }

    /// LiveKit's `Worker.AssignJob`: offers the job, and if the worker takes
    /// it, sends it with a token for the agent participant.
    async fn assign(&self, w: &Arc<Worker>, mut job: lk::Job) -> Result<lk::JobState, AssignError> {
        let (tx, rx) = oneshot::channel();
        {
            let mut s = w.state.lock().unwrap();
            if s.closed {
                return Err(AssignError::Closed);
            }
            if s.availability.contains_key(&job.id) {
                return Err(AssignError::Duplicate);
            }
            s.availability.insert(job.id.clone(), tx);
        }
        let now = unix_ns();
        let state = job.state.get_or_insert_with(Default::default);
        state.worker_id = w.id.clone();
        state.updated_at = now;
        state.started_at = now;
        state.status = lk::JobStatus::JsRunning as i32;
        let sent = w.send(server_message::Message::Availability(
            lk::AvailabilityRequest {
                job: Some(job.clone()),
                resuming: false,
            },
        ));
        let answer = match sent {
            Ok(()) => tokio::time::timeout(ASSIGN_TIMEOUT, rx).await,
            Err(e) => {
                w.state.lock().unwrap().availability.remove(&job.id);
                return Err(e);
            }
        };
        w.state.lock().unwrap().availability.remove(&job.id);
        let res = match answer {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => return Err(AssignError::Closed),
            Err(_) => return Err(AssignError::Timeout),
        };
        let mut state = job.state.clone().unwrap_or_default();
        if res.terminate {
            state.ended_at = now;
            state.status = lk::JobStatus::JsSuccess as i32;
            return Ok(state);
        }
        if !res.available {
            return Err(AssignError::NotAvailable);
        }
        state.participant_identity = res.participant_identity.clone();
        job.state = Some(state.clone());

        let mut attributes = res.participant_attributes;
        attributes.insert(AGENT_NAME_ATTRIBUTE.into(), w.agent_name.clone());
        let room = job
            .room
            .as_ref()
            .map(|r| r.name.clone())
            .unwrap_or_default();
        let token = self
            .agent_token(
                w,
                &room,
                &res.participant_identity,
                &res.participant_name,
                &res.participant_metadata,
                attributes,
            )
            .ok_or(AssignError::Token)?;
        w.state
            .lock()
            .unwrap()
            .running
            .insert(job.id.clone(), job.clone());
        let id = job.id.clone();
        if let Err(e) = w.send(server_message::Message::Assignment(lk::JobAssignment {
            job: Some(job),
            url: None,
            token,
        })) {
            w.state.lock().unwrap().running.remove(&id);
            return Err(e);
        }
        Ok(state)
    }

    /// LiveKit's `BuildAgentToken`: an hour's token for the agent
    /// participant, with the worker's permissions.
    fn agent_token(
        &self,
        w: &Worker,
        room: &str,
        identity: &str,
        name: &str,
        metadata: &str,
        attributes: HashMap<String, String>,
    ) -> Option<String> {
        let secret = self.0.keys.secret(&w.api_key)?;
        let p = &w.permissions;
        let grants = ClaimGrants {
            identity: identity.to_owned(),
            name: name.to_owned(),
            kind: "agent".into(),
            video: Some(VideoGrant {
                room_join: true,
                agent: true,
                room: room.to_owned(),
                can_subscribe: Some(p.can_subscribe),
                can_publish: Some(p.can_publish),
                can_publish_data: Some(p.can_publish_data),
                hidden: p.hidden,
                can_update_own_metadata: Some(p.can_update_metadata),
                can_subscribe_metrics: Some(p.can_subscribe_metrics),
                ..Default::default()
            }),
            metadata: metadata.to_owned(),
            attributes,
            ..Default::default()
        };
        Some(token::sign(
            &w.api_key,
            secret,
            &grants,
            TOKEN_TTL,
            SystemTime::now(),
        ))
    }

    /// LiveKit's `JobTerminate`: tells the job's worker to end it, and
    /// returns its final state; or passes it to the node whose worker runs
    /// it.
    pub fn terminate(&self, job_id: &str, reason: TerminateReason) -> Termination {
        let local = self.0.registry.lock().unwrap().jobs.remove(job_id);
        let Some(w) = local else {
            let remote = self.0.remote_jobs.lock().unwrap().remove(job_id);
            return match (remote, &self.0.cluster) {
                (Some(node), Some(c)) => {
                    let (c, job_id) = (c.clone(), job_id.to_owned());
                    self.0.runtime.spawn(async move {
                        let body = lk::JobTermination { job_id }.encode_to_vec();
                        let left = reason == TerminateReason::AgentLeftRoom;
                        if c.call(&node, "/internal/agent/terminate", body, left)
                            .await
                            .is_none()
                        {
                            debug!(node = %node.id, "remote job was no longer running");
                        }
                    });
                    Termination::Remote
                }
                _ => Termination::Unknown,
            };
        };
        if !w.state.lock().unwrap().running.contains_key(job_id) {
            return Termination::Unknown;
        }
        let _ = w.send(server_message::Message::Termination(lk::JobTermination {
            job_id: job_id.to_owned(),
        }));
        let (status, error) = match reason {
            TerminateReason::Requested => (lk::JobStatus::JsSuccess, ""),
            TerminateReason::AgentLeftRoom => {
                (lk::JobStatus::JsFailed, "agent worker left the room")
            }
        };
        match w.update_job(&lk::UpdateJobStatus {
            job_id: job_id.to_owned(),
            status: status as i32,
            error: error.into(),
        }) {
            Some(s) => Termination::Ended(s),
            None => Termination::Unknown,
        }
    }

    /// A job the worker reports ended is no longer anyone's to end.
    fn job_ended(&self, job_id: &str) {
        self.0.registry.lock().unwrap().jobs.remove(job_id);
    }
}

/// Picks among `(item, weight)` by `r` in `[0, 1)`; `None` if every weight
/// is zero.
fn pick_weighted<T>(candidates: Vec<(T, f32)>, r: f32) -> Option<T> {
    let sum: f32 = candidates.iter().map(|(_, w)| w).sum();
    if sum <= 0.0 {
        return None;
    }
    let mut left = r * sum;
    let mut last = None;
    for (item, weight) in candidates {
        if weight <= 0.0 {
            continue;
        }
        left -= weight;
        if left <= 0.0 {
            return Some(item);
        }
        last = Some(item);
    }
    last
}

fn random_unit() -> f32 {
    (u32::from_le_bytes(crate::util::random_bytes()) >> 8) as f32 / (1u32 << 24) as f32
}

/// `GET /agent`: an agent worker's WebSocket. The token needs the `agent`
/// video grant.
pub async fn agent_ws(
    upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
    State(state): State<RtcState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let Ok(upgrade) = upgrade else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(token) = super::signal::bearer(&headers, &query) else {
        return (StatusCode::UNAUTHORIZED, "permissions denied").into_response();
    };
    let (api_key, grants) = match token::verify(&token, &state.keys, SystemTime::now()) {
        Ok(v) => v,
        Err(e) => return (StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    if !grants.video.as_ref().is_some_and(|v| v.agent) {
        return (StatusCode::UNAUTHORIZED, "permissions denied").into_response();
    }
    let protocol = query
        .get("protocol")
        .and_then(|p| p.parse().ok())
        .unwrap_or(PROTOCOL);
    let agents = state.agents.clone();
    upgrade
        .max_message_size(MAX_MESSAGE)
        .max_frame_size(MAX_MESSAGE)
        .on_upgrade(move |ws| session(ws, agents, api_key, protocol))
}

async fn session(ws: WebSocket, agents: Agents, api_key: String, protocol: i32) {
    let (mut sink, mut stream) = ws.split();
    let mut format;

    // LiveKit's `HandshakeAgentWorker`: only pings until the worker has
    // registered, within REGISTER_TIMEOUT.
    let deadline = tokio::time::Instant::now() + REGISTER_TIMEOUT;
    let register = loop {
        let msg = match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(Ok(msg))) => msg,
            Ok(_) => return,
            Err(_) => {
                debug!("agent worker did not register in time");
                return;
            }
        };
        if matches!(msg, Message::Close(_)) {
            return;
        }
        let (msg, f) = match wire::decode::<lk::WorkerMessage>(&msg, "worker message") {
            Ok(Some(m)) => m,
            Ok(None) => continue,
            Err(e) => {
                warn!("{e}");
                return;
            }
        };
        format = f;
        match msg.message {
            Some(worker_message::Message::Register(req)) => break req,
            Some(worker_message::Message::Ping(ping)) => {
                let pong = pong(&ping);
                if sink.send(wire::encode(&pong, format)).await.is_err() {
                    return;
                }
            }
            other => {
                warn!("unable to handle worker signal before registering: {other:?}");
                return;
            }
        }
    };
    let job_type = match lk::JobType::try_from(register.r#type) {
        Ok(t) => t,
        Err(_) => {
            warn!(
                job_type = register.r#type,
                "worker registered an unknown job type"
            );
            return;
        }
    };
    if let Err(e) = validate_deployment(&register.deployment) {
        warn!("worker registered an invalid deployment: {e}");
        return;
    }
    if !register.endpoints.is_empty() {
        info!(agent_name = %register.agent_name, "endpoints ignored, connection has no data plane");
    }
    let permissions = register
        .allowed_permissions
        .clone()
        .unwrap_or(lk::ParticipantPermission {
            can_subscribe: true,
            can_publish: true,
            can_publish_data: true,
            can_update_metadata: true,
            ..Default::default()
        });
    let (tx, mut rx) = mpsc::unbounded_channel();
    let worker = Arc::new(Worker {
        id: ids::new(ids::AGENT_WORKER),
        agent_name: register.agent_name.clone(),
        namespace: register.namespace().to_owned(),
        job_type,
        deployment: register.deployment.clone(),
        permissions,
        api_key,
        tx,
        state: Mutex::default(),
    });
    let reply = lk::ServerMessage {
        message: Some(server_message::Message::Register(
            lk::RegisterWorkerResponse {
                worker_id: worker.id.clone(),
                server_info: Some(agents.0.server_info.clone()),
                ..Default::default()
            },
        )),
    };
    if sink.send(wire::encode(&reply, format)).await.is_err() {
        return;
    }
    debug!(worker = %worker.id, protocol, version = %register.version, "agent worker connected");
    agents.register(Arc::clone(&worker));

    let mut ws_ping = tokio::time::interval(WS_PING_EVERY);
    loop {
        tokio::select! {
            msg = stream.next() => {
                let Some(Ok(msg)) = msg else { break };
                if matches!(msg, Message::Close(_)) {
                    break;
                }
                let (msg, f) = match wire::decode::<lk::WorkerMessage>(&msg, "worker message") {
                    Ok(Some(m)) => m,
                    Ok(None) => continue,
                    Err(e) => {
                        warn!(worker = %worker.id, "{e}");
                        break;
                    }
                };
                format = f;
                match msg.message {
                    Some(worker_message::Message::Availability(res)) => worker.availability(res),
                    Some(worker_message::Message::UpdateJob(update)) => {
                        if worker.update_job(&update).is_none() {
                            debug!(worker = %worker.id, job = %update.job_id, "job update for unknown job");
                        }
                        if ended(update.status()) {
                            agents.job_ended(&update.job_id);
                        }
                    }
                    Some(worker_message::Message::SimulateJob(req)) => {
                        simulate(&agents, &worker, req);
                    }
                    Some(worker_message::Message::Ping(ping)) => {
                        if sink.send(wire::encode(&pong(&ping), format)).await.is_err() {
                            break;
                        }
                    }
                    Some(worker_message::Message::UpdateWorker(update)) => {
                        worker.update_worker(&update);
                        agents.0.changed.notify_one();
                    }
                    Some(worker_message::Message::MigrateJob(_)) => {}
                    other => {
                        warn!(worker = %worker.id, "unable to handle worker signal: {other:?}");
                        break;
                    }
                }
            }
            out = rx.recv() => {
                let Some(out) = out else { break };
                if sink.send(wire::encode(&out, format)).await.is_err() {
                    break;
                }
            }
            _ = ws_ping.tick() => {
                if sink.send(Message::Ping(Default::default())).await.is_err() {
                    break;
                }
            }
        }
    }
    agents.deregister(&worker);
    let _ = sink.close().await;
}

fn pong(ping: &lk::WorkerPing) -> lk::ServerMessage {
    lk::ServerMessage {
        message: Some(server_message::Message::Pong(lk::WorkerPong {
            last_timestamp: ping.timestamp,
            timestamp: unix_ms(),
        })),
    }
}

/// LiveKit's `HandleSimulateJob`: a job for this worker alone, for a room
/// (or participant) that need not exist, for testing agents.
fn simulate(agents: &Agents, w: &Arc<Worker>, req: lk::SimulateJobRequest) {
    let job_type = if req.participant.is_some() {
        lk::JobType::JtPublisher
    } else {
        lk::JobType::JtRoom
    };
    let job = lk::Job {
        id: ids::new(ids::AGENT_JOB),
        r#type: job_type as i32,
        room: req.room,
        participant: req.participant,
        namespace: w.namespace.clone(),
        agent_name: w.agent_name.clone(),
        ..Default::default()
    };
    let (agents, w) = (agents.clone(), Arc::clone(w));
    agents.0.runtime.clone().spawn(async move {
        if let Err(e) = agents.assign(&w, job).await {
            warn!(worker = %w.id, "unable to simulate job: {e:?}");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployments_as_livekit() {
        for ok in [
            "",
            "production",
            "prod-us.v2",
            "prod:us",
            "a/b",
            "prodüction",
            &"a".repeat(64),
        ] {
            assert!(validate_deployment(ok).is_ok(), "{ok:?}");
        }
        for bad in [
            "prod_us",
            "prod us",
            "prod\tus",
            "prod\nus",
            "prod\x7fus",
            "prod\0us",
            &"a".repeat(65),
        ] {
            assert!(validate_deployment(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn weighted_pick() {
        let c = || vec![("a", 0.0), ("b", 0.25), ("c", 0.75)];
        assert_eq!(pick_weighted(c(), 0.0), Some("b"));
        assert_eq!(pick_weighted(c(), 0.2), Some("b"));
        assert_eq!(pick_weighted(c(), 0.3), Some("c"));
        assert_eq!(pick_weighted(c(), 0.999), Some("c"));
        assert_eq!(pick_weighted(vec![("a", 0.0)], 0.5), None);
        assert_eq!(pick_weighted::<&str>(vec![], 0.5), None);
    }

    fn agents() -> Agents {
        let keys = Keys::parse("devkey: secretsecretsecretsecretsecretsecret").unwrap();
        Agents::new(Arc::new(keys), lk::ServerInfo::default(), None)
    }

    fn worker(
        agents: &Agents,
        name: &str,
    ) -> (Arc<Worker>, mpsc::UnboundedReceiver<lk::ServerMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let w = Arc::new(Worker {
            id: ids::new(ids::AGENT_WORKER),
            agent_name: name.into(),
            namespace: String::new(),
            job_type: lk::JobType::JtRoom,
            deployment: String::new(),
            permissions: lk::ParticipantPermission {
                can_subscribe: true,
                can_publish: true,
                can_publish_data: true,
                can_update_metadata: true,
                ..Default::default()
            },
            api_key: "devkey".into(),
            tx,
            state: Mutex::default(),
        });
        agents.register(Arc::clone(&w));
        (w, rx)
    }

    async fn offer(rx: &mut mpsc::UnboundedReceiver<lk::ServerMessage>) -> lk::Job {
        match rx.recv().await.and_then(|m| m.message) {
            Some(server_message::Message::Availability(a)) => a.job.unwrap(),
            other => panic!("expected an offer, got {other:?}"),
        }
    }

    fn request(agent_name: &str) -> JobRequest {
        JobRequest {
            dispatch_id: "AD_x".into(),
            job_type: lk::JobType::JtRoom,
            room: lk::Room {
                name: "r".into(),
                ..Default::default()
            },
            participant: None,
            metadata: "m".into(),
            agent_name: agent_name.into(),
            deployment: String::new(),
            attributes: HashMap::new(),
        }
    }

    /// A declined offer goes to the other worker; the one that takes it
    /// gets the job with a token for its agent, and the room hears of it.
    #[tokio::test]
    async fn declined_job_goes_to_another_worker() {
        let agents = agents();
        let (a, mut rx_a) = worker(&agents, "helper");
        let (b, mut rx_b) = worker(&agents, "helper");
        let (_other, mut rx_other) = worker(&agents, "other");
        let (done_tx, mut done) = mpsc::unbounded_channel();
        agents.launch(
            request("helper"),
            Arc::new(move |job| {
                let _ = done_tx.send(job);
            }),
        );

        // Whichever is asked first declines.
        let (first, second, rx_second) = tokio::select! {
            job = offer(&mut rx_a) => (job, &b, &mut rx_b),
            job = offer(&mut rx_b) => (job, &a, &mut rx_a),
        };
        assert_eq!(first.metadata, "m");
        assert_eq!(
            first.state.as_ref().unwrap().status(),
            lk::JobStatus::JsRunning
        );
        let decliner = if Arc::ptr_eq(second, &a) { &b } else { &a };
        decliner.availability(lk::AvailabilityResponse {
            job_id: first.id.clone(),
            available: false,
            ..Default::default()
        });
        let job = offer(rx_second).await;
        assert_eq!(job.id, first.id);
        second.availability(lk::AvailabilityResponse {
            job_id: job.id.clone(),
            available: true,
            participant_identity: "bot".into(),
            participant_name: "Bot".into(),
            participant_attributes: HashMap::from([("x".into(), "y".into())]),
            ..Default::default()
        });
        let assignment = match rx_second.recv().await.and_then(|m| m.message) {
            Some(server_message::Message::Assignment(a)) => a,
            other => panic!("expected the assignment, got {other:?}"),
        };
        let (key, claims) =
            token::verify(&assignment.token, &agents.0.keys, SystemTime::now()).unwrap();
        assert_eq!(key, "devkey");
        assert_eq!(claims.identity, "bot");
        assert_eq!(claims.name, "Bot");
        assert_eq!(claims.kind, "agent");
        assert_eq!(claims.attributes["x"], "y");
        assert_eq!(claims.attributes[AGENT_NAME_ATTRIBUTE], "helper");
        let video = claims.video.unwrap();
        assert!(video.room_join && video.agent && video.room == "r");
        assert_eq!(video.can_subscribe_metrics, Some(false));

        let reported = done.recv().await.unwrap();
        assert_eq!(reported.state.unwrap().participant_identity, "bot");
        assert!(
            rx_other.try_recv().is_err(),
            "another agent's worker was asked"
        );

        // Its agent leaves: the worker is told, the job has failed.
        let Termination::Ended(state) = agents.terminate(&job.id, TerminateReason::AgentLeftRoom)
        else {
            panic!("the job runs here");
        };
        assert_eq!(state.status(), lk::JobStatus::JsFailed);
        assert_eq!(state.error, "agent worker left the room");
        assert!(matches!(
            rx_second.recv().await.and_then(|m| m.message),
            Some(server_message::Message::Termination(t)) if t.job_id == job.id
        ));
        assert!(matches!(
            agents.terminate(&job.id, TerminateReason::Requested),
            Termination::Unknown
        ));
    }

    /// Full workers, and a worker that went away mid-offer, get no job.
    #[tokio::test]
    async fn no_job_without_capacity() {
        let agents = agents();
        let (a, mut rx_a) = worker(&agents, "");
        a.update_worker(&lk::UpdateWorkerStatus {
            status: Some(lk::WorkerStatus::WsFull as i32),
            load: 1.0,
            seq: 2,
            ..Default::default()
        });
        // Older than the last: ignored.
        a.update_worker(&lk::UpdateWorkerStatus {
            status: Some(lk::WorkerStatus::WsAvailable as i32),
            seq: 1,
            ..Default::default()
        });
        let (b, mut rx_b) = worker(&agents, "");
        let (done_tx, mut done) = mpsc::unbounded_channel();
        agents.launch(
            request(""),
            Arc::new(move |job| {
                let _ = done_tx.send(job);
            }),
        );
        offer(&mut rx_b).await;
        agents.deregister(&b);
        assert!(done.recv().await.is_none(), "a job was reported");
        assert!(rx_a.try_recv().is_err(), "the full worker was asked");
    }
}
