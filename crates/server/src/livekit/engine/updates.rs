//! What the room is told as it changes: participant and room updates,
//! webhooks, active speakers and connection quality.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use livekit_protocol as lk;
use lk::{participant_info, signal_response};

use super::{Engine, ROOM_UPDATE_INTERVAL, fill_layer_ssrcs};
use crate::livekit::audio_level;

impl Engine {
    /// Sends a participant's new state to everyone in the room, itself
    /// included.
    pub(super) fn participant_update(&mut self, room: &Arc<str>, i: usize) {
        let p = &self.participants[i];
        let info = p.info();
        let hidden = p.hidden();
        let Some(r) = self.rooms.get(room) else {
            return;
        };
        for &j in &r.members.clone() {
            if hidden && j != i {
                continue;
            }
            self.participants[j].send(signal_response::Message::Update(lk::ParticipantUpdate {
                participants: vec![info.clone()],
            }));
        }
    }

    /// A webhook event about `room` (and a participant or track in it).
    pub(super) fn hook(
        &self,
        event: &str,
        room: &str,
        participant: Option<lk::ParticipantInfo>,
        track: Option<lk::TrackInfo>,
    ) {
        let Some(w) = &self.webhooks else {
            return;
        };
        w.notify(lk::WebhookEvent {
            event: event.into(),
            room: self.rooms.get(room).map(|r| r.info(&self.participants)),
            participant,
            track,
            ..Default::default()
        });
    }

    /// The room's info to everyone in it, changed or not (metadata).
    pub(super) fn room_update_forced(&mut self, room: &Arc<str>) {
        let Some(r) = self.rooms.get_mut(room) else {
            return;
        };
        let info = r.info(&self.participants);
        r.last_counts = (
            info.num_participants,
            info.num_publishers,
            info.active_recording,
        );
        for &j in &r.members.clone() {
            self.participants[j].send(signal_response::Message::RoomUpdate(lk::RoomUpdate {
                room: Some(info.clone()),
            }));
        }
    }

    /// The room's new counts to everyone in it, at most every
    /// `ROOM_UPDATE_INTERVAL` (LiveKit's room `ProtoProxy`); a change in
    /// recording goes out at once.
    pub(super) fn room_update(&mut self, room: &Arc<str>) {
        let now = Instant::now();
        let Some(r) = self.rooms.get_mut(room) else {
            return;
        };
        let info = r.info(&self.participants);
        let counts = (
            info.num_participants,
            info.num_publishers,
            info.active_recording,
        );
        if counts == r.last_counts {
            r.counts_pending = false;
            return;
        }
        let recording_changed = counts.2 != r.last_counts.2;
        if !recording_changed
            && r.counts_sent_at
                .is_some_and(|at| now.duration_since(at) < ROOM_UPDATE_INTERVAL)
        {
            r.counts_pending = true;
            return;
        }
        r.last_counts = counts;
        r.counts_sent_at = Some(now);
        r.counts_pending = false;
        for &j in &r.members.clone() {
            self.participants[j].send(signal_response::Message::RoomUpdate(lk::RoomUpdate {
                room: Some(info.clone()),
            }));
        }
    }

    /// Every update interval, each room's speaker changes go to everyone
    /// subscribed to the speaker, and to the speaker.
    pub(super) fn speakers(&mut self, now: Instant) {
        let rooms: Vec<Arc<str>> = self.rooms.keys().cloned().collect();
        for name in rooms {
            let members = self.rooms[&name].members.clone();
            let mut levels: Vec<(usize, String, f32)> = vec![];
            for &i in &members {
                let p = &mut self.participants[i];
                if let Some(level) = p.speaker_level(now) {
                    levels.push((i, p.sid.clone(), audio_level::quantize(level)));
                }
            }
            levels.sort_by(|a, b| b.2.total_cmp(&a.2));
            let room = self.rooms.get_mut(&name).expect("listed");
            let mut changes: Vec<(usize, lk::SpeakerInfo)> = vec![];
            for (i, sid, level) in &levels {
                if room.speakers.get(sid) != Some(level) {
                    changes.push((
                        *i,
                        lk::SpeakerInfo {
                            sid: sid.clone(),
                            level: *level,
                            active: true,
                        },
                    ));
                }
            }
            let speaking: HashSet<&String> = levels.iter().map(|(_, s, _)| s).collect();
            for (sid, _) in room.speakers.iter().filter(|(s, _)| !speaking.contains(s)) {
                if let Some(&i) = members.iter().find(|&&i| &self.participants[i].sid == sid) {
                    changes.push((
                        i,
                        lk::SpeakerInfo {
                            sid: sid.clone(),
                            level: 0.0,
                            active: false,
                        },
                    ));
                }
            }
            room.speakers = levels.iter().map(|(_, s, l)| (s.clone(), *l)).collect();
            if changes.is_empty() {
                continue;
            }
            for &j in &members {
                let r = &self.participants[j];
                let speakers: Vec<lk::SpeakerInfo> = changes
                    .iter()
                    .filter(|(i, info)| {
                        *i == j || r.downtracks.iter().any(|d| d.publisher_sid == info.sid)
                    })
                    .map(|(_, info)| info.clone())
                    .collect();
                if !speakers.is_empty() {
                    self.participants[j].send(signal_response::Message::SpeakersChanged(
                        lk::SpeakersChanged { speakers },
                    ));
                }
            }
        }
    }

    /// Each participant hears its own quality and that of those it is
    /// subscribed to, when the level changes.
    pub(super) fn connection_quality(&mut self) {
        // RTX streams are paired once repairs flow; pick them up here.
        for p in &mut self.participants {
            fill_layer_ssrcs(p);
        }
        // Wanted data tracks not found yet are given up after a while.
        for j in 0..self.participants.len() {
            if !self.participants[j].data_wants.is_empty() {
                self.reconcile_data(j);
            }
        }
        let qualities: Vec<(String, lk::ConnectionQuality, f32)> = self
            .participants
            .iter()
            .map(|p| {
                let (q, mos) = p.quality();
                (p.sid.clone(), q, mos)
            })
            .collect();
        // LiveKit records each participant's own quality as it updates it.
        for (p, (_, q, mos)) in self.participants.iter().zip(&qualities) {
            if p.state == participant_info::State::Active {
                self.metrics
                    .lk
                    .quality_rating
                    .observe(&[], *q as i32 as f64);
                self.metrics.lk.quality_score.observe(&[], f64::from(*mos));
            }
        }
        for j in 0..self.participants.len() {
            let r = &self.participants[j];
            let lost_ok = r.protocol > 10;
            let mut updates = vec![];
            for (k, (sid, q, mos)) in qualities.iter().enumerate() {
                let relevant = k == j || r.downtracks.iter().any(|d| &d.publisher_sid == sid);
                if !relevant || self.participants[k].room != r.room {
                    continue;
                }
                let q = if *q == lk::ConnectionQuality::Lost && !lost_ok {
                    lk::ConnectionQuality::Poor
                } else {
                    *q
                };
                if r.quality_sent.get(sid) != Some(&q) {
                    updates.push(lk::ConnectionQualityInfo {
                        participant_sid: sid.clone(),
                        quality: q as i32,
                        score: *mos,
                    });
                }
            }
            if updates.is_empty() {
                continue;
            }
            let r = &mut self.participants[j];
            for u in &updates {
                r.quality_sent.insert(
                    u.participant_sid.clone(),
                    lk::ConnectionQuality::try_from(u.quality).unwrap_or_default(),
                );
            }
            r.send(signal_response::Message::ConnectionQuality(
                lk::ConnectionQualityUpdate { updates },
            ));
        }
    }
}
