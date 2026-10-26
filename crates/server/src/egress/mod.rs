//! Recording (LiveKit's Egress): tracks to files without transcoding, as
//! LiveKit's track egress (`TrackEgressRequest`): Opus to Ogg, VP8 to
//! WebM. Unlike LiveKit, natter records in the server itself: the engine
//! hands a track's packets to a writer thread (`track`), with no extra
//! WebRTC connection or egress service.
//!
//! [`Recordings`] keeps every egress' `EgressInfo` (ended ones for 24 h,
//! as LiveKit's Redis store) and sends LiveKit's egress webhooks.

pub mod ogg;
pub mod track;
pub mod webm;

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use livekit_protocol as lk;
use lk::EgressStatus;

use crate::livekit::webhook::{self, Webhooks};

/// How long an ended egress stays listed (LiveKit's `egressWorker`).
const KEEP_ENDED: Duration = Duration::from_secs(24 * 3600);

/// LiveKit's end reasons (`protocol/livekit/livekit_egress.go`).
pub const END_API: &str = "StopEgress API";
pub const END_SOURCE_CLOSED: &str = "Source closed";
pub const STOPPED_BEFORE_STARTED: &str = "Stop called before pipeline could start";

pub struct Recordings {
    /// Files go under here.
    pub dir: PathBuf,
    webhooks: Option<Webhooks>,
    infos: Mutex<HashMap<String, (lk::EgressInfo, Option<Instant>)>>,
}

impl Recordings {
    pub fn new(dir: PathBuf, webhooks: Option<Webhooks>) -> Self {
        Recordings {
            dir,
            webhooks,
            infos: Mutex::default(),
        }
    }

    /// A new egress (`egress_started`).
    pub fn create(&self, info: &lk::EgressInfo) {
        self.store(info, webhook::EGRESS_STARTED);
    }

    /// Its status changed: `egress_updated` while it runs, `egress_ended`
    /// once it has ended, as LiveKit's `IOInfoService.UpdateEgress`.
    pub fn update(&self, info: &lk::EgressInfo) {
        let event = if ended(info) {
            webhook::EGRESS_ENDED
        } else {
            webhook::EGRESS_UPDATED
        };
        self.store(info, event);
    }

    fn store(&self, info: &lk::EgressInfo, event: &str) {
        let now = Instant::now();
        {
            let mut infos = self.infos.lock().expect("egress infos");
            infos.retain(|_, (_, at)| at.is_none_or(|at| now - at < KEEP_ENDED));
            infos.insert(
                info.egress_id.clone(),
                (info.clone(), ended(info).then_some(now)),
            );
        }
        if let Some(w) = &self.webhooks {
            w.notify(lk::WebhookEvent {
                event: event.into(),
                room: Some(lk::Room {
                    sid: info.room_id.clone(),
                    name: info.room_name.clone(),
                    ..Default::default()
                }),
                egress_info: Some(info.clone()),
                ..Default::default()
            });
        }
    }

    pub fn get(&self, id: &str) -> Option<lk::EgressInfo> {
        let infos = self.infos.lock().expect("egress infos");
        infos.get(id).map(|(i, _)| i.clone())
    }

    /// As LiveKit's `ListEgress`: one by id, else a room's (or all), only
    /// those still running if `active`.
    pub fn list(&self, req: &lk::ListEgressRequest) -> Vec<lk::EgressInfo> {
        if !req.egress_id.is_empty() {
            return self.get(&req.egress_id).into_iter().collect();
        }
        let infos = self.infos.lock().expect("egress infos");
        let mut list: Vec<_> = infos
            .values()
            .map(|(i, _)| i)
            .filter(|i| req.room_name.is_empty() || i.room_name == req.room_name)
            .filter(|i| !req.active || !ended(i))
            .cloned()
            .collect();
        list.sort_by_key(|i| i.started_at);
        list
    }

    /// Where a file goes: the request's path under `dir` (no absolute
    /// paths, no `..`), placeholders replaced, LiveKit's default name if it
    /// gives none or ends in `/`, and `ext` on the end. Returns the path
    /// and the name as the request put it (`FileInfo.filename`).
    pub fn file_path(
        &self,
        requested: &str,
        identifier: &str,
        replacements: &[(&str, &str)],
        ext: &str,
    ) -> Result<(PathBuf, String), String> {
        let mut name = replacements
            .iter()
            .fold(requested.to_owned(), |s, (k, v)| s.replace(k, v));
        if name.is_empty() || name.ends_with('/') {
            let time = time_stamp();
            name.push_str(&format!("{identifier}-{time}"));
        } else if let Some(dot) = name.rfind('.')
            && KNOWN_EXTENSIONS.contains(&&name[dot..])
        {
            name.truncate(dot);
        }
        name.push_str(ext);
        let rel = Path::new(&name);
        if !rel
            .components()
            .all(|c| matches!(c, Component::Normal(_) | Component::CurDir))
        {
            return Err(format!(
                "filepath {requested:?} must be relative, without .."
            ));
        }
        Ok((self.dir.join(rel), name))
    }
}

/// LiveKit's `FileExtensions`: replaced by the right one.
const KNOWN_EXTENSIONS: &[&str] = &[
    ".raw", ".ogg", ".mp3", ".m3u8", ".ts", ".m4s", ".mp4", ".ivf", ".webm", ".jpeg", ".txt",
];

pub fn ended(info: &lk::EgressInfo) -> bool {
    info.status >= EgressStatus::EgressComplete as i32
}

pub fn unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}

/// Go's `2006-01-02T150405`, UTC.
fn time_stamp() -> String {
    let secs = crate::util::unix_secs();
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (y, m, d) = civil_from_days(days as i64);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}{:02}{:02}",
        rem / 3600,
        rem / 60 % 60,
        rem % 60
    )
}

/// Days since 1970-01-01 to a date (Howard Hinnant's algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

#[cfg(test)]
mod tests;
