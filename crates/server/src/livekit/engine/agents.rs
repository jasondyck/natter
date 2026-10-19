//! The engine's side of agent dispatch, as LiveKit's `rtc.Room`: a room's
//! dispatches, the jobs they launch (for the room when it starts, for each
//! participant as it joins, for each publisher as it first publishes), and
//! the end of an agent's job when it leaves. AgentDispatchService's
//! requests land here too.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use livekit_protocol as lk;
use tracing::{debug, info, warn};

use super::admin::{AdminError, AdminReply, AdminResult};
use super::{Command, Engine};
use crate::livekit::agent::{JobRequest, TerminateReason, Termination};
use crate::livekit::ids;
use crate::util::unix_ns;

/// How long an agent whose dispatch was deleted has to leave on its own
/// before it is removed (LiveKit's `waitForParticipantLeaving`).
const LEAVE_WAIT: Duration = Duration::from_secs(3);

#[derive(Default)]
pub(super) struct RoomAgents {
    dispatches: Vec<lk::AgentDispatch>,
    /// The job each agent participant runs, by identity.
    jobs: HashMap<String, String>,
    /// Participants that have published, so publisher jobs launch once.
    published: HashSet<String>,
    /// Agents whose job was ended, removed if still here by then.
    removals: Vec<(Instant, String)>,
}

/// The agent to remove if it lingers after its job was ended: the one the
/// worker reported, or for a job on another node's worker (ended there,
/// in the background), the one it was assigned.
fn ended(t: Termination, known: &str) -> Option<String> {
    let identity = match t {
        Termination::Ended(state) => state.participant_identity,
        Termination::Remote => known.to_owned(),
        Termination::Unknown => return None,
    };
    (!identity.is_empty()).then_some(identity)
}

fn dispatch_not_found() -> AdminError {
    AdminError::new("not_found", "dispatch ID not found")
}

impl Engine {
    /// A new room's dispatches: those it was created with, or else one for
    /// any unnamed agent (LiveKit's `createAgentDispatchesFromRoomAgent`),
    /// whose room jobs launch at once.
    pub(super) fn start_dispatches(&mut self, room: &str, from: &[lk::RoomAgentDispatch]) {
        let default = [lk::RoomAgentDispatch::default()];
        let from = if from.is_empty() { &default[..] } else { from };
        let mut created = vec![];
        for rad in from {
            if let Err(e) = crate::livekit::agent::validate_deployment(&rad.deployment) {
                warn!(room, "failed storing room dispatch: {e}");
                continue;
            }
            created.push(lk::AgentDispatch {
                id: ids::new(ids::AGENT_DISPATCH),
                agent_name: rad.agent_name.clone(),
                room: room.to_owned(),
                metadata: rad.metadata.clone(),
                restart_policy: rad.restart_policy,
                deployment: rad.deployment.clone(),
                attributes: rad.attributes.clone(),
                state: Some(lk::AgentDispatchState {
                    created_at: unix_ns(),
                    ..Default::default()
                }),
            });
        }
        let Some(r) = self.rooms.get_mut(room) else {
            return;
        };
        r.agents.dispatches.extend(created.iter().cloned());
        for d in &created {
            self.launch_job(d, lk::JobType::JtRoom, None);
        }
    }

    /// Asks the workers for a job for `dispatch`; once one takes it, the
    /// job comes back here (`Command::AgentJob`).
    fn launch_job(&self, d: &lk::AgentDispatch, job_type: lk::JobType, participant: Option<usize>) {
        let Some(room) = self.rooms.get(d.room.as_str()) else {
            return;
        };
        let participant = participant.map(|i| self.participants[i].info());
        let back = self.back.clone();
        let (room_name, dispatch_id) = (d.room.clone(), d.id.clone());
        self.agents.launch(
            JobRequest {
                dispatch_id: d.id.clone(),
                job_type,
                room: room.info(&self.participants),
                participant,
                metadata: d.metadata.clone(),
                agent_name: d.agent_name.clone(),
                deployment: d.deployment.clone(),
                attributes: d.attributes.clone(),
            },
            Arc::new(move |job| {
                let _ = back.send(Command::AgentJob {
                    room: room_name.clone(),
                    dispatch_id: dispatch_id.clone(),
                    job: Box::new(job),
                });
            }),
        );
    }

    /// Jobs of `job_type` for participant `i`, from every dispatch of its
    /// room. Agents and recorders get none.
    fn launch_target_jobs(&self, i: usize, job_type: lk::JobType, only: Option<&str>) {
        let p = &self.participants[i];
        if p.is_dependent() {
            return;
        }
        let Some(room) = self.rooms.get(&p.room) else {
            return;
        };
        for d in &room.agents.dispatches {
            if only.is_none_or(|id| id == d.id) {
                self.launch_job(d, job_type, Some(i));
            }
        }
    }

    /// Participant `i` joined: participant jobs.
    pub(super) fn agents_on_join(&self, i: usize) {
        self.launch_target_jobs(i, lk::JobType::JtParticipant, None);
    }

    /// Participant `i` published a track: publisher jobs, the first time.
    pub(super) fn agents_on_publish(&mut self, i: usize) {
        let (room, identity) = (
            self.participants[i].room.clone(),
            self.participants[i].identity.clone(),
        );
        let Some(r) = self.rooms.get_mut(&room) else {
            return;
        };
        if r.agents.published.insert(identity) {
            self.launch_target_jobs(i, lk::JobType::JtPublisher, None);
        }
    }

    /// A worker took a job: the dispatch lists it, and its participant is
    /// known as that job's agent. A job whose dispatch was deleted
    /// meanwhile ends at once.
    pub(super) fn agent_job(&mut self, room: &str, dispatch_id: &str, job: lk::Job) {
        let now = Instant::now();
        let identity = job
            .state
            .as_ref()
            .map(|s| s.participant_identity.clone())
            .unwrap_or_default();
        let Some(r) = self.rooms.get_mut(room) else {
            return;
        };
        let Some(d) = r.agents.dispatches.iter_mut().find(|d| d.id == dispatch_id) else {
            if let Some(identity) = ended(
                self.agents.terminate(&job.id, TerminateReason::Requested),
                &identity,
            ) {
                r.agents.removals.push((now + LEAVE_WAIT, identity));
            }
            return;
        };
        if !identity.is_empty() {
            r.agents.jobs.insert(identity, job.id.clone());
        }
        d.state.get_or_insert_with(Default::default).jobs.push(job);
    }

    /// A participant left room `room` for good (no other session took its
    /// identity): an agent's job is over (LiveKit's `AGENT_LEFT_ROOM`).
    pub(super) fn agents_on_leave(&mut self, room: &str, identity: &str) {
        let Some(r) = self.rooms.get_mut(room) else {
            return;
        };
        r.agents.published.remove(identity);
        if let Some(job) = r.agents.jobs.remove(identity)
            && matches!(
                self.agents.terminate(&job, TerminateReason::AgentLeftRoom),
                Termination::Unknown
            )
        {
            debug!(job, identity, "agent left; its job was no longer running");
        }
    }

    /// Agents told to end their job that are still here are removed.
    pub(super) fn remove_lingering_agents(&mut self, now: Instant) {
        let mut due = vec![];
        for r in self.rooms.values_mut() {
            r.agents.removals.retain(|(at, identity)| {
                if now < *at {
                    return true;
                }
                due.push((r.members.clone(), identity.clone()));
                false
            });
        }
        for (members, identity) in due {
            if let Some(i) = members.into_iter().find(|&i| {
                self.participants[i].identity == identity && !self.participants[i].closing
            }) {
                info!(
                    identity,
                    "agent worker did not disconnect after 3s; removing"
                );
                self.disconnect(i, lk::DisconnectReason::ParticipantRemoved);
            }
        }
    }

    /// AgentDispatchService CreateDispatch: a new dispatch, whose room job
    /// launches, and target jobs for everyone already here.
    pub(super) fn create_dispatch(&mut self, mut d: lk::AgentDispatch) -> AdminResult {
        let room = self
            .rooms
            .get_mut(d.room.as_str())
            .filter(|r| !r.deleted)
            .ok_or_else(|| AdminError::new("not_found", "requested room does not exist"))?;
        d.state = Some(lk::AgentDispatchState {
            created_at: unix_ns(),
            ..Default::default()
        });
        room.agents.dispatches.push(d.clone());
        let members: Vec<usize> = room
            .members
            .iter()
            .copied()
            .filter(|&i| !self.participants[i].closing)
            .collect();
        self.launch_job(&d, lk::JobType::JtRoom, None);
        for i in members {
            if self.participants[i].info().is_publisher {
                self.launch_target_jobs(i, lk::JobType::JtPublisher, Some(&d.id));
            }
            self.launch_target_jobs(i, lk::JobType::JtParticipant, Some(&d.id));
        }
        info!(room = %d.room, dispatch = %d.id, agent_name = %d.agent_name, "agent dispatch created");
        Ok(AdminReply::Dispatch(d))
    }

    /// DeleteDispatch: its jobs end, and their agents get LEAVE_WAIT to go
    /// before they are removed.
    pub(super) fn delete_dispatch(&mut self, req: lk::DeleteAgentDispatchRequest) -> AdminResult {
        let now = Instant::now();
        self.room_index(&req.room)?;
        let r = self.rooms.get_mut(req.room.as_str()).expect("checked");
        let at = r
            .agents
            .dispatches
            .iter()
            .position(|d| d.id == req.dispatch_id)
            .ok_or_else(dispatch_not_found)?;
        let d = r.agents.dispatches.remove(at);
        for job in d.state.as_ref().map_or(&[][..], |s| &s.jobs[..]) {
            let known = job
                .state
                .as_ref()
                .map(|s| s.participant_identity.clone())
                .unwrap_or_default();
            if let Some(identity) = ended(
                self.agents.terminate(&job.id, TerminateReason::Requested),
                &known,
            ) {
                r.agents.removals.push((now + LEAVE_WAIT, identity));
            }
        }
        info!(room = %req.room, dispatch = %d.id, "agent dispatch deleted");
        Ok(AdminReply::Dispatch(d))
    }

    /// ListDispatch: all of the room's dispatches, or the one asked for.
    pub(super) fn list_dispatch(&self, req: &lk::ListAgentDispatchRequest) -> AdminResult {
        self.room_index(&req.room)?;
        let dispatches = self.rooms[req.room.as_str()]
            .agents
            .dispatches
            .iter()
            .filter(|d| req.dispatch_id.is_empty() || d.id == req.dispatch_id)
            .cloned()
            .collect();
        Ok(AdminReply::Dispatches(dispatches))
    }
}
