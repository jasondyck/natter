//! LiveKit-style ids: a type prefix and 12 random base62 characters, as
//! `protocol/utils/guid`. Track ids also encode kind and source after the
//! prefix (`TR_AM…` is a microphone), as LiveKit's participant does.

use livekit_protocol::{TrackSource, TrackType};

const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const SIZE: usize = 12;

pub const ROOM: &str = "RM_";
pub const PARTICIPANT: &str = "PA_";
pub const NODE: &str = "ND_";
pub const AGENT_WORKER: &str = "AW_";
pub const AGENT_JOB: &str = "AJ_";
pub const AGENT_DISPATCH: &str = "AD_";
pub const EGRESS: &str = "EG_";

pub fn new(prefix: &str) -> String {
    let bytes = crate::util::random_bytes::<SIZE>();
    let mut id = String::with_capacity(prefix.len() + SIZE);
    id.push_str(prefix);
    // 256 % 62 skews the first few characters a little; ids only need to
    // be unique, as LiveKit's (which come from a seeded PRNG) are.
    id.extend(bytes.iter().map(|b| ALPHABET[usize::from(*b) % 62] as char));
    id
}

pub fn track(kind: TrackType, source: TrackSource) -> String {
    let mut prefix = String::from("TR_");
    match kind {
        TrackType::Audio => prefix.push('A'),
        TrackType::Video => prefix.push('V'),
        TrackType::Data => {}
    }
    match source {
        TrackSource::Camera => prefix.push('C'),
        TrackSource::Microphone => prefix.push('M'),
        TrackSource::ScreenShare => prefix.push('S'),
        TrackSource::ScreenShareAudio => prefix.push('s'),
        TrackSource::Unknown => {}
    }
    new(&prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_have_prefix_and_base62_body() {
        let id = new(PARTICIPANT);
        assert_eq!(id.len(), 15);
        assert!(id.starts_with("PA_"));
        assert!(id[3..].bytes().all(|b| b.is_ascii_alphanumeric()));
        assert_ne!(new(ROOM), new(ROOM));
        assert!(track(TrackType::Audio, TrackSource::Microphone).starts_with("TR_AM"));
        assert!(track(TrackType::Video, TrackSource::ScreenShare).starts_with("TR_VS"));
    }
}
