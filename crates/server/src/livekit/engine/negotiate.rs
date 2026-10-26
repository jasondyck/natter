//! Subscriber renegotiation: when to offer after tracks open or close,
//! and the SDP changes for dual-PC and single-PC clients.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use livekit_protocol as lk;
use lk::signal_response;
use str0m::media::{Direction, MediaKind, Mid};

use super::{DownState, Participant};
use crate::livekit::ids;

/// Subscriber renegotiation debounce, as LiveKit (`transport.go:61-64`).
const NEGOTIATION_DEBOUNCE: Duration = Duration::from_millis(150);
const FAST_NEGOTIATION: Duration = Duration::from_millis(10);

/// Renegotiates the subscriber side when tracks open or close, debounced
/// as LiveKit's transport (`transport.go`): the first offer and ICE
/// restarts go out after 10 ms, later changes are gathered for 150 ms into
/// one offer. Returns when to come back if it is holding one back.
pub(super) fn negotiate(p: &mut Participant, now: Instant) -> Option<Instant> {
    let has_work = p.restart_ice
        || p.downtracks
            .iter()
            .any(|d| matches!(d.state, DownState::ToOpen | DownState::ToStop(_)));
    if !has_work || p.closing {
        p.negotiate_after = None;
        return None;
    }
    // LiveKit tells a subscriber it may have a track when it subscribes
    // (the first permission check), ahead of the offer.
    let allowed: Vec<(String, String)> = p
        .downtracks
        .iter_mut()
        .filter(|d| d.state == DownState::ToOpen && !d.told_allowed)
        .map(|d| {
            d.told_allowed = true;
            (d.publisher_sid.clone(), d.track_sid.clone())
        })
        .collect();
    for (participant_sid, track_sid) in allowed {
        p.send(signal_response::Message::SubscriptionPermissionUpdate(
            lk::SubscriptionPermissionUpdate {
                participant_sid,
                track_sid,
                allowed: true,
            },
        ));
    }
    let due = match p.negotiate_after {
        Some(due) => due,
        None => {
            let delay = if p.restart_ice || !p.negotiated {
                FAST_NEGOTIATION
            } else {
                NEGOTIATION_DEBOUNCE
            };
            p.negotiate_after = Some(now + delay);
            return Some(now + delay);
        }
    };
    if now < due {
        return None;
    }
    let before = (p.sub_offer_id, p.reoffer_requested);
    if p.one_shot {
        fill_slots(p);
    } else if p.single_pc() {
        negotiate_single(p);
    } else {
        negotiate_dual(p);
    }
    // Still waiting on an answer: stays due, so it goes out right after.
    if (p.sub_offer_id, p.reoffer_requested) != before {
        p.negotiate_after = None;
        p.negotiated = true;
    }
    None
}

/// Offers new and stopped m-lines on the subscriber PC.
fn negotiate_dual(p: &mut Participant) {
    if p.sub_pending.is_some() || p.closing {
        return;
    }
    let has_work = p.restart_ice
        || p.downtracks
            .iter()
            .any(|d| matches!(d.state, DownState::ToOpen | DownState::ToStop(_)));
    if !has_work {
        return;
    }
    // str0m checks a new mid only against negotiated ones, so several
    // added at once can collide; rebuild with fresh mids if so.
    let (change, opened) = loop {
        let mut change = sub!(p).rtc.sdp_api();
        if p.restart_ice {
            change.ice_restart(true);
        }
        let mut opened: Vec<(usize, Mid)> = vec![];
        for (k, d) in p.downtracks.iter().enumerate() {
            match d.state {
                DownState::ToOpen => {
                    let stream = format!("{}|{}", d.publisher_sid, d.track_sid);
                    let mid = change.add_media(
                        d.kind,
                        Direction::SendOnly,
                        Some(stream),
                        Some(d.track_sid.clone()),
                        None,
                    );
                    opened.push((k, mid));
                }
                DownState::ToStop(mid) => change.stop_media(mid),
                _ => {}
            }
        }
        let unique: HashSet<Mid> = opened.iter().map(|&(_, m)| m).collect();
        if unique.len() == opened.len() {
            break (change, opened);
        }
    };
    if !change.has_changes() {
        return;
    }
    let Some((offer, pending)) = change.apply() else {
        return;
    };
    let mut mid_to_track_id = HashMap::new();
    for (k, mid) in opened {
        p.downtracks[k].state = DownState::Negotiating(mid);
        mid_to_track_id.insert(mid.to_string(), p.downtracks[k].track_sid.clone());
    }
    for d in &mut p.downtracks {
        if let DownState::ToStop(m) = d.state {
            d.state = DownState::NegotiatingStop(m);
        }
    }
    p.restart_ice = false;
    p.send_sub_offer(offer, pending, mid_to_track_id, Instant::now());
}

/// One-shot (WHEP): the client's one offer is all there is, so its
/// receive-only m-lines are filled without renegotiating: a stopped track
/// frees its m-line, and a new one takes a free m-line of its kind or waits
/// for one. The engine then opens them (`open_negotiated`), carrying on
/// each m-line's stream (`mid_sent`).
fn fill_slots(p: &mut Participant) {
    p.downtracks
        .retain(|d| !matches!(d.state, DownState::ToStop(_)));
    for k in 0..p.downtracks.len() {
        if p.downtracks[k].state != DownState::ToOpen {
            continue;
        }
        let kind = p.downtracks[k].kind;
        let used: HashSet<Mid> = p.downtracks.iter().filter_map(|d| d.state.mid()).collect();
        if let Some(&(mid, _)) = p
            .slots
            .iter()
            .find(|(m, kd)| *kd == kind && !used.contains(m))
        {
            p.downtracks[k].state = DownState::Negotiating(mid);
        }
    }
}

/// Single-PC: the client makes every offer, so forwarded tracks go out on
/// its receive-only m-lines. Assign each new track a free one, label it
/// with the track's msid for the next answer, and ask the client to offer
/// again, with more m-lines if there weren't enough (LiveKit's
/// `media_sections_requirement`).
fn negotiate_single(p: &mut Participant) {
    if p.reoffer_requested || p.closing {
        return;
    }
    let mut changed = false;
    let (mut audios, mut videos) = (0u32, 0u32);
    for k in 0..p.downtracks.len() {
        match p.downtracks[k].state {
            DownState::ToOpen => {
                let kind = p.downtracks[k].kind;
                let used: HashSet<Mid> =
                    p.downtracks.iter().filter_map(|d| d.state.mid()).collect();
                let Some(&(mid, _)) = p
                    .slots
                    .iter()
                    .find(|(m, kd)| *kd == kind && !used.contains(m))
                else {
                    match kind {
                        MediaKind::Audio => audios += 1,
                        MediaKind::Video => videos += 1,
                    }
                    continue;
                };
                let d = &mut p.downtracks[k];
                let stream = format!("{}|{}", d.publisher_sid, d.track_sid);
                p.publisher.rtc.set_msid(mid, stream, d.track_sid.clone());
                d.state = DownState::Negotiating(mid);
                changed = true;
            }
            DownState::ToStop(mid) => {
                // An empty slot again, free for the next track. Its own
                // track id: Chrome refuses an SDP where two m-lines share
                // one msid, so several slots freed at once can't all be
                // `- -`.
                p.publisher.rtc.set_msid(mid, "-".into(), ids::new("-"));
                p.downtracks[k].state = DownState::NegotiatingStop(mid);
                changed = true;
            }
            _ => {}
        }
    }
    if !changed && audios == 0 && videos == 0 {
        return;
    }
    p.send(signal_response::Message::MediaSectionsRequirement(
        lk::MediaSectionsRequirement {
            num_audios: audios,
            num_videos: videos,
        },
    ));
    p.reoffer_requested = true;
    p.sub_pending_since = Some(Instant::now());
}
