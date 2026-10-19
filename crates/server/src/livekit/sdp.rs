//! The little SDP work str0m doesn't expose: which m-line carries which
//! client track (LiveKit clients put the track's `cid` from `add_track` in
//! the m-line's `a=msid:<stream> <track>`), and putting the codec a client
//! asked to publish with first in our answer.

use std::collections::HashMap;

/// `sdp` cut before each m-line: the session part (if any), then each media
/// section, its m-line first, as they appear.
pub fn sections(sdp: &str) -> impl Iterator<Item = &str> {
    let mut rest = sdp;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        let end = rest.find("\nm=").map_or(rest.len(), |i| i + 1);
        let (section, next) = rest.split_at(end);
        rest = next;
        Some(section)
    })
}

/// `mid` → msid track id, for every m-line that has both.
pub fn msid_tracks(sdp: &str) -> HashMap<String, String> {
    sections(sdp)
        .filter_map(|section| {
            let (mut mid, mut track) = (None, None);
            for line in section.lines().map(str::trim_end) {
                if let Some(m) = line.strip_prefix("a=mid:") {
                    mid = Some(m);
                } else if let Some(msid) = line.strip_prefix("a=msid:") {
                    track = msid.split_whitespace().nth(1);
                } else if let Some(rest) = line.strip_prefix("a=ssrc:") {
                    // Plan B style `a=ssrc:<n> msid:<stream> <track>`, as Pion
                    // writes alongside the m-line attribute.
                    if track.is_none()
                        && let Some(msid) = rest
                            .split_once(' ')
                            .and_then(|(_, a)| a.strip_prefix("msid:"))
                    {
                        track = msid.split_whitespace().nth(1);
                    }
                }
            }
            Some((mid?.to_owned(), track?.to_owned()))
        })
        .collect()
}

/// Reorders the payload types of the m-line `mid` so those of `codec`
/// (an rtpmap encoding name, such as `H264`) come first, keeping the order
/// otherwise. The sender uses the first codec of the answer it gets, so
/// this is how a publisher ends up sending the codec it asked for in
/// `add_track` (LiveKit does the same to its publisher answers).
pub fn prefer_codec(sdp: &str, mid: &str, codec: &str) -> String {
    let lines: Vec<&str> = sdp.split("\r\n").collect();
    // Section bounds: each m-line up to the next.
    let starts: Vec<usize> = (0..lines.len())
        .filter(|&i| lines[i].starts_with("m="))
        .collect();
    let mut out: Vec<String> = lines.iter().map(|l| (*l).to_owned()).collect();
    for (k, &start) in starts.iter().enumerate() {
        let end = starts.get(k + 1).copied().unwrap_or(lines.len());
        let section = &lines[start..end];
        if !section
            .iter()
            .any(|l| l.strip_prefix("a=mid:") == Some(mid))
        {
            continue;
        }
        let wanted: Vec<&str> = section
            .iter()
            .filter_map(|l| l.strip_prefix("a=rtpmap:"))
            .filter_map(|r| r.split_once(' '))
            .filter(|(_, enc)| {
                enc.split('/')
                    .next()
                    .is_some_and(|name| name.eq_ignore_ascii_case(codec))
            })
            .map(|(pt, _)| pt)
            .collect();
        let mut fields: Vec<&str> = lines[start].split(' ').collect();
        if fields.len() <= 3 || wanted.is_empty() {
            break;
        }
        let pts = fields.split_off(3);
        let (mut first, rest): (Vec<&str>, Vec<&str>) =
            pts.into_iter().partition(|pt| wanted.contains(pt));
        first.extend(rest);
        fields.extend(first);
        out[start] = fields.join(" ");
        break;
    }
    out.join("\r\n")
}

/// Single-PC: the client's receive-only m-lines that carry nothing yet (or
/// any more), answered `inactive` without msid or SSRCs, as Pion answers a
/// transceiver with no track. Answered `sendonly` with a placeholder msid,
/// Chrome fires `track` for the slot, and again when a real track takes it.
pub fn inactive_slots(sdp: &str, free: &[String]) -> String {
    let mut out = String::with_capacity(sdp.len());
    for section in sections(sdp) {
        let free_slot = section.split_inclusive('\n').any(|l| {
            l.trim_end()
                .strip_prefix("a=mid:")
                .is_some_and(|m| free.iter().any(|f| f == m))
        });
        if !free_slot {
            out.push_str(section);
            continue;
        }
        for line in section.split_inclusive('\n') {
            let t = line.trim_end();
            if t.starts_with("a=msid:")
                || t.starts_with("a=ssrc:")
                || t.starts_with("a=ssrc-group:")
            {
                continue;
            }
            if matches!(t, "a=sendonly" | "a=sendrecv") {
                out.push_str(&line.replacen(t, "a=inactive", 1));
            } else {
                out.push_str(line);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_mids_to_track_ids() {
        let sdp = "v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:0\r\n\
                   m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:1\r\na=msid:stream TR_abc\r\n\
                   m=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:2\r\n\
                   a=ssrc:1 msid:s TR_vid\r\n";
        let map = msid_tracks(sdp);
        assert_eq!(map.len(), 2);
        assert_eq!(map["1"], "TR_abc");
        assert_eq!(map["2"], "TR_vid");
    }

    #[test]
    fn puts_the_asked_for_codec_first() {
        let sdp = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\na=rtpmap:111 opus/48000/2\r\n\
                   m=video 9 UDP/TLS/RTP/SAVPF 96 97 108 109\r\na=mid:1\r\n\
                   a=rtpmap:96 VP8/90000\r\na=rtpmap:97 rtx/90000\r\n\
                   a=rtpmap:108 H264/90000\r\na=rtpmap:109 rtx/90000\r\n";
        let out = prefer_codec(sdp, "1", "h264");
        assert!(
            out.contains("m=video 9 UDP/TLS/RTP/SAVPF 108 96 97 109\r\n"),
            "{out}"
        );
        assert!(out.contains("m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n"));
        assert_eq!(prefer_codec(sdp, "1", "AV1"), sdp, "not offered: unchanged");
        assert_eq!(prefer_codec(sdp, "7", "H264"), sdp);
    }

    #[test]
    fn free_slots_go_inactive() {
        let sdp = "v=0\r\na=msid-semantic: WMS a b\r\n\
                   m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\na=sendonly\r\na=msid:PA|TR TR\r\na=ssrc:1 cname:x\r\n\
                   m=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:1\r\na=sendonly\r\na=msid:a b\r\na=ssrc-group:FID 2 3\r\na=ssrc:2 cname:x\r\n";
        let out = inactive_slots(sdp, &["1".into()]);
        assert!(out.contains("a=mid:0\r\na=sendonly\r\na=msid:PA|TR TR\r\na=ssrc:1 cname:x\r\n"));
        assert!(out.ends_with("a=mid:1\r\na=inactive\r\n"));
    }
}
