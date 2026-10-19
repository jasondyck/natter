//! Native WHIP (RFC 9725) at `/whip/v1`, as LiveKit's: publish into a room
//! with one HTTP request, no WebSocket (`docs/livekit/protocol.md`).
//!
//! The session is an ordinary participant in "one-shot" mode: its tracks
//! come from the offer's m-lines (LiveKit's `synthesizeAddTrackRequests`),
//! nothing is ever offered to it, and when its transport fails it is
//! closed, as there is no one to tell. PATCH trickles candidates or, with
//! `If-Match: *`, restarts ICE; DELETE ends it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use livekit_protocol as lk;
use lk::{signal_request, signal_response};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

use super::engine::{self, Command, Join};
use super::rtc_config;
use super::signal::{RtcState, send_signals, verify};
use crate::util::content_type;

/// How long creating a session may take: joining, then answering.
const ANSWER_WAIT: Duration = Duration::from_secs(10);
/// Request bodies: an SDP offer or fragment (LiveKit:
/// `http.DefaultMaxHeaderBytes`, 1 MiB).
pub const MAX_BODY: usize = 1 << 20;

/// Live WHIP sessions, by participant sid.
#[derive(Default)]
pub struct Sessions(Mutex<HashMap<String, Session>>);

struct Session {
    conn: u64,
    shard: usize,
    room: String,
    identity: String,
    /// The offer, for ICE restarts: re-offered with the new credentials.
    offer: String,
    /// The server's ICE ufrag: the session's ETag.
    ufrag: String,
    /// Where the next answer goes (an ICE restart waiting for it).
    answer: Arc<Mutex<Option<oneshot::Sender<String>>>>,
}

fn error(status: StatusCode, msg: impl Into<String>) -> Response {
    let mut resp = (
        status,
        axum::Json(serde_json::json!({ "error": msg.into() })),
    )
        .into_response();
    cors(&mut resp);
    resp
}

/// WHIP clients run in browsers too: LiveKit allows any origin and exposes
/// every header (Location, Link, ETag).
fn cors(resp: &mut Response) {
    let h = resp.headers_mut();
    h.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    h.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("*"),
    );
}

/// `OPTIONS /whip/v1[/…]`: CORS preflight.
pub async fn options() -> Response {
    let mut resp = StatusCode::OK.into_response();
    cors(&mut resp);
    let h = resp.headers_mut();
    h.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("*"),
    );
    h.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("PATCH, OPTIONS, GET, POST, DELETE"),
    );
    resp
}

/// `GET /whip/v1[/{id}]`: nothing to say (LiveKit: 204).
pub async fn get() -> Response {
    let mut resp = StatusCode::NO_CONTENT.into_response();
    cors(&mut resp);
    resp
}

/// One m-line of an offer, as far as publishing needs it.
#[derive(Debug, PartialEq)]
struct Section {
    kind: lk::TrackType,
    track: Option<String>,
    rids: Vec<String>,
}

/// The audio and video m-lines a client sends on.
fn sections(sdp: &str) -> Vec<Section> {
    super::sdp::sections(sdp)
        .filter_map(|section| {
            let mut lines = section.lines().map(str::trim_end);
            let kind = match lines.next()?.strip_prefix("m=")?.split_whitespace().next() {
                Some("audio") => lk::TrackType::Audio,
                Some("video") => lk::TrackType::Video,
                _ => return None,
            };
            let mut s = Section {
                kind,
                track: None,
                rids: vec![],
            };
            for line in lines {
                if let Some(msid) = line.strip_prefix("a=msid:") {
                    s.track = msid.split_whitespace().nth(1).map(str::to_owned);
                } else if let Some(rid) = line.strip_prefix("a=rid:") {
                    let mut f = rid.split_whitespace();
                    if let (Some(id), Some("send")) = (f.next(), f.next()) {
                        s.rids.push(id.to_owned());
                    }
                } else if line == "a=recvonly" || line == "a=inactive" {
                    return None;
                }
            }
            Some(s)
        })
        .collect()
}

/// LiveKit's `synthesizeAddTrackRequests`: an `add_track` per m-line.
fn add_tracks(sdp: &str) -> Vec<lk::AddTrackRequest> {
    sections(sdp)
        .into_iter()
        .map(|s| {
            let audio = s.kind == lk::TrackType::Audio;
            let layers = if audio {
                vec![]
            } else if s.rids.is_empty() {
                // At least one layer, as LiveKit.
                vec![lk::VideoLayer::default()]
            } else {
                // The quality by order, and the rid (LiveKit fills it in
                // from the SDP later, `updateRidsFromSDP`); no sizes in an
                // SDP.
                s.rids
                    .iter()
                    .take(3)
                    .enumerate()
                    .map(|(q, rid)| lk::VideoLayer {
                        quality: q as i32,
                        rid: rid.clone(),
                        ..Default::default()
                    })
                    .collect()
            };
            lk::AddTrackRequest {
                cid: s.track.unwrap_or_else(|| super::ids::new("TR_")),
                name: if audio {
                    "synthesized-microphone"
                } else {
                    "synthesized-camera"
                }
                .into(),
                r#type: s.kind as i32,
                source: if audio {
                    lk::TrackSource::Microphone
                } else {
                    lk::TrackSource::Camera
                } as i32,
                disable_dtx: true,
                stream: "camera".into(),
                layers,
                ..Default::default()
            }
        })
        .collect()
}

fn ice_attr<'a>(sdp: &'a str, name: &str) -> Option<&'a str> {
    let prefix = format!("a={name}:");
    sdp.lines()
        .map(str::trim_end)
        .find_map(|l| l.strip_prefix(prefix.as_str()))
}

/// RFC 9725 §4.6: TURN servers as `Link: <url>; rel="ice-server"`.
fn link_headers(servers: &[lk::IceServer]) -> Vec<String> {
    servers
        .iter()
        .flat_map(|s| {
            s.urls.iter().map(move |u| {
                let mut link = format!("<{u}>; rel=\"ice-server\"");
                if !s.username.is_empty() {
                    link.push_str(&format!("; username=\"{}\"", s.username));
                }
                if !s.credential.is_empty() {
                    link.push_str(&format!(
                        "; credential=\"{}\"; credential-type=\"password\"",
                        s.credential
                    ));
                }
                link
            })
        })
        .collect()
}

/// `POST /whip/v1`: an SDP offer in, a session and its answer out.
pub async fn create(
    State(state): State<RtcState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    uri: axum::http::Uri,
    body: Bytes,
) -> Response {
    if content_type(&headers) != "application/sdp" {
        return error(
            StatusCode::BAD_REQUEST,
            format!("unsupported content-type: {}", content_type(&headers)),
        );
    }
    let v = match verify(&headers, &query, &state) {
        Ok(v) => v,
        Err((code, msg)) => return error(code, msg),
    };
    // The room may be on another node.
    if let Some(c) = &state.cluster
        && !c.forwarded(&headers)
    {
        let may_create =
            state.auto_create || v.grants.video.as_ref().is_some_and(|g| g.room_create);
        match c.route(&v.room, may_create).await {
            Ok(super::cluster::Route::Remote(node)) => {
                let path = uri.path_and_query().map_or("/whip/v1", |p| p.as_str());
                return c
                    .forward(&node, axum::http::Method::POST, path, &headers, body)
                    .await;
            }
            Ok(super::cluster::Route::Missing) => {
                return error(StatusCode::NOT_FOUND, "requested room does not exist");
            }
            Ok(super::cluster::Route::Local) => {}
            Err(e) => {
                tracing::warn!("routing room {:?}: {e}", v.room);
                return error(StatusCode::SERVICE_UNAVAILABLE, "no nodes available");
            }
        }
    }
    let Ok(offer) = String::from_utf8(body.to_vec()) else {
        return error(StatusCode::BAD_REQUEST, "body does not have SDP offer");
    };
    if offer.trim().is_empty() {
        return error(StatusCode::BAD_REQUEST, "body does not have SDP offer");
    }
    if let Err(e) = str0m::change::SdpOffer::from_sdp_string(&offer) {
        return error(StatusCode::BAD_REQUEST, format!("malformed SDP offer: {e}"));
    }

    debug!(room = %v.room, identity = %v.grants.identity, %offer, "WHIP offer");
    let shard = state.engine.placement.join(&v.room);
    let s = &state.engine.shards[shard];
    let (commands, advertised, tcp, media) =
        (s.commands.clone(), s.advertised, s.tcp, state.engine.media);
    let Ok(publisher) =
        tokio::task::spawn_blocking(move || rtc_config::publisher(advertised, tcp, media, true))
            .await
    else {
        state.engine.placement.leave(&v.room);
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not create the peer connection",
        );
    };
    let conn = state
        .next_conn
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (tx, mut rx) = mpsc::channel::<lk::SignalResponse>(super::signal::QUEUE);
    let ice_servers: Vec<lk::IceServer> = state.ice_servers_for(&conn.to_string()).collect();
    let g = &v.grants;
    let join = Join {
        conn,
        room: v.room.clone(),
        identity: g.identity.clone(),
        name: g.name.clone(),
        metadata: g.metadata.clone(),
        attributes: g.attributes.clone(),
        kind: lk::participant_info::Kind::from_str_name(&g.kind.to_ascii_uppercase())
            .unwrap_or(lk::participant_info::Kind::Standard),
        grant: g.video.clone().unwrap_or_default(),
        room_config: g.room_config.clone(),
        // Nothing can be offered to a one-shot participant.
        auto_subscribe: false,
        auto_subscribe_data: false,
        adaptive_stream: false,
        congestion: media.congestion,
        allow_pause: media.allow_pause,
        protocol: super::engine::PROTOCOL,
        // LiveKit's WHIP takes the client's info from the query too.
        client: super::signal::client_info(&query),
        publisher,
        subscriber: None,
        tx,
        ice_servers: ice_servers.clone(),
        ice_tcp: true,
        one_shot: true,
        started: std::time::Instant::now(),
    };
    if commands.send(Command::Join(Box::new(join))).is_err() {
        state.engine.placement.leave(&v.room);
        return error(StatusCode::SERVICE_UNAVAILABLE, "media engine unavailable");
    }
    let tracks = add_tracks(&offer)
        .into_iter()
        .map(signal_request::Message::AddTrack);
    let sd = signal_request::Message::Offer(lk::SessionDescription {
        r#type: "offer".into(),
        sdp: offer.clone(),
        ..Default::default()
    });
    send_signals(&commands, conn, tracks.chain([sd]));

    // The engine answers on the signal channel: the join, then the answer.
    let waited = tokio::time::timeout(ANSWER_WAIT, async {
        let mut sid = None;
        while let Some(resp) = rx.recv().await {
            match resp.message {
                Some(signal_response::Message::Join(j)) => {
                    sid = j.participant.map(|p| p.sid);
                }
                Some(signal_response::Message::Answer(a)) => return sid.map(|s| (s, a.sdp)),
                Some(signal_response::Message::Leave(_)) => return None,
                _ => {}
            }
        }
        None
    })
    .await;
    let Ok(Some((sid, answer))) = waited else {
        let leave = signal_request::Message::Leave(Default::default());
        send_signals(&commands, conn, [leave]);
        return error(StatusCode::SERVICE_UNAVAILABLE, "could not publish");
    };
    let ufrag = ice_attr(&answer, "ice-ufrag")
        .unwrap_or_default()
        .to_owned();
    let slot: Arc<Mutex<Option<oneshot::Sender<String>>>> = Arc::default();
    state.whip.0.lock().unwrap().insert(
        sid.clone(),
        Session {
            conn,
            shard,
            room: v.room.clone(),
            identity: g.identity.clone(),
            offer,
            ufrag: ufrag.clone(),
            answer: Arc::clone(&slot),
        },
    );
    // Until the participant goes: answers to ICE restarts, the rest
    // dropped (no one to tell).
    let (sessions, drained_sid) = (Arc::clone(&state.whip), sid.clone());
    tokio::spawn(async move {
        while let Some(resp) = rx.recv().await {
            if let Some(signal_response::Message::Answer(a)) = resp.message
                && let Some(waiting) = slot.lock().unwrap().take()
            {
                let _ = waiting.send(a.sdp);
            }
        }
        sessions.0.lock().unwrap().remove(&drained_sid);
        debug!(sid = %drained_sid, "WHIP session ended");
    });
    info!(room = %v.room, identity = %g.identity, %sid, "WHIP session");
    if let Some(c) = &state.cluster {
        c.set_whip(&sid).await;
    }

    let mut resp = (StatusCode::CREATED, answer).into_response();
    cors(&mut resp);
    let h = resp.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/sdp"),
    );
    if let Ok(v) = HeaderValue::from_str(&format!("/whip/v1/{sid}")) {
        h.insert(header::LOCATION, v);
    }
    if let Ok(v) = HeaderValue::from_str(&format!("\"{ufrag}\"")) {
        h.insert(header::ETAG, v);
    }
    for link in link_headers(&ice_servers) {
        if let Ok(v) = HeaderValue::from_str(&link) {
            h.append(header::LINK, v);
        }
    }
    resp
}

/// A request for a WHIP session on another node, passed to it as it came.
async fn on_other_node(
    state: &RtcState,
    sid: &str,
    method: axum::http::Method,
    uri: &axum::http::Uri,
    headers: &HeaderMap,
    body: Bytes,
) -> Option<Response> {
    let c = state.cluster.as_ref()?;
    if c.forwarded(headers) || state.whip.0.lock().unwrap().contains_key(sid) {
        return None;
    }
    let node = c.whip_node(sid).await?;
    let path = uri.path_and_query().map_or(uri.path(), |p| p.as_str());
    Some(c.forward(&node, method, path, headers, body).await)
}

/// The session `sid`, if it belongs to the token's room and identity.
fn session_for(
    state: &RtcState,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
    sid: &str,
) -> Result<(u64, usize), (StatusCode, String)> {
    let v = verify(headers, query, state)?;
    let sessions = state.whip.0.lock().unwrap();
    match sessions.get(sid) {
        Some(s) if s.room == v.room && s.identity == v.grants.identity => Ok((s.conn, s.shard)),
        _ => Err((StatusCode::NOT_FOUND, "participant not found".into())),
    }
}

/// `PATCH /whip/v1/{id}`: trickled candidates (`If-Match` the ETag), or an
/// ICE restart (`If-Match: *`), as `application/trickle-ice-sdpfrag`.
pub async fn patch(
    State(state): State<RtcState>,
    Path(sid): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    uri: axum::http::Uri,
    body: Bytes,
) -> Response {
    if let Some(resp) = on_other_node(
        &state,
        &sid,
        axum::http::Method::PATCH,
        &uri,
        &headers,
        body.clone(),
    )
    .await
    {
        return resp;
    }
    if content_type(&headers) != "application/trickle-ice-sdpfrag" {
        return error(
            StatusCode::BAD_REQUEST,
            format!("unsupported content-type: {}", content_type(&headers)),
        );
    }
    let Some(if_match) = headers.get(header::IF_MATCH).and_then(|v| v.to_str().ok()) else {
        return error(StatusCode::PRECONDITION_REQUIRED, "missing entity tag");
    };
    let if_match = if_match.trim().trim_matches('"').to_owned();
    let (conn, shard) = match session_for(&state, &query, &headers, &sid) {
        Ok(s) => s,
        Err((code, msg)) => return error(code, msg),
    };
    let Ok(frag) = String::from_utf8(body.to_vec()) else {
        return error(StatusCode::BAD_REQUEST, "body does not have SDP fragment");
    };
    let commands = state.engine.shards[shard].commands.clone();

    if if_match == "*" {
        return restart(&state, &sid, conn, &commands, &frag).await;
    }
    let ufrag = state
        .whip
        .0
        .lock()
        .unwrap()
        .get(&sid)
        .map(|s| s.ufrag.clone());
    if ufrag.as_deref() != Some(if_match.as_str()) {
        return error(StatusCode::PRECONDITION_FAILED, "ICE session mismatch");
    }
    let mut mid = None;
    for line in frag.lines().map(str::trim_end) {
        if let Some(m) = line.strip_prefix("a=mid:") {
            mid = Some(m.to_owned());
        } else if let Some(c) = line
            .strip_prefix("a=")
            .filter(|l| l.starts_with("candidate:"))
        {
            let init = serde_json::json!({ "candidate": c, "sdpMid": mid });
            let trickle = signal_request::Message::Trickle(lk::TrickleRequest {
                candidate_init: init.to_string(),
                target: lk::SignalTarget::Publisher as i32,
                ..Default::default()
            });
            send_signals(&commands, conn, [trickle]);
        }
    }
    let mut resp = StatusCode::NO_CONTENT.into_response();
    cors(&mut resp);
    resp
}

/// An ICE restart: the original offer again with the client's new
/// credentials (str0m restarts ICE when they change), answered with the
/// server's new ones and its candidates.
async fn restart(
    state: &RtcState,
    sid: &str,
    conn: u64,
    commands: &engine::Commands,
    frag: &str,
) -> Response {
    let (Some(ufrag), Some(pwd)) = (ice_attr(frag, "ice-ufrag"), ice_attr(frag, "ice-pwd")) else {
        return error(
            StatusCode::BAD_REQUEST,
            "ICE restart without new credentials",
        );
    };
    let (offer, slot) = {
        let sessions = state.whip.0.lock().unwrap();
        let Some(s) = sessions.get(sid) else {
            return error(StatusCode::NOT_FOUND, "participant not found");
        };
        (s.offer.clone(), Arc::clone(&s.answer))
    };
    let offer: String = offer
        .split_inclusive('\n')
        .map(|l| {
            if l.starts_with("a=ice-ufrag:") {
                format!("a=ice-ufrag:{ufrag}\r\n")
            } else if l.starts_with("a=ice-pwd:") {
                format!("a=ice-pwd:{pwd}\r\n")
            } else if l.starts_with("a=candidate:") || l.starts_with("a=end-of-candidates") {
                // Candidates of the old ICE session.
                String::new()
            } else {
                l.to_owned()
            }
        })
        .collect();
    let (tx, rx) = oneshot::channel();
    *slot.lock().unwrap() = Some(tx);
    let sd = signal_request::Message::Offer(lk::SessionDescription {
        r#type: "offer".into(),
        sdp: offer.clone(),
        ..Default::default()
    });
    send_signals(commands, conn, [sd]);
    let Ok(Ok(answer)) = tokio::time::timeout(ANSWER_WAIT, rx).await else {
        return error(StatusCode::INTERNAL_SERVER_ERROR, "ICE restart failed");
    };
    let (Some(new_ufrag), Some(new_pwd)) =
        (ice_attr(&answer, "ice-ufrag"), ice_attr(&answer, "ice-pwd"))
    else {
        return error(StatusCode::INTERNAL_SERVER_ERROR, "ICE restart failed");
    };
    if let Some(s) = state.whip.0.lock().unwrap().get_mut(sid) {
        s.offer = offer;
        s.ufrag = new_ufrag.to_owned();
    }
    // RFC 8840: the credentials, then the candidates under the first
    // m-line (bundled: one transport).
    let mut body = format!("a=ice-ufrag:{new_ufrag}\r\na=ice-pwd:{new_pwd}\r\n");
    let mid = ice_attr(&answer, "mid").unwrap_or("0");
    body.push_str(&format!("m=audio 9 UDP/TLS/RTP/SAVPF 0\r\na=mid:{mid}\r\n"));
    for line in answer.lines().map(str::trim_end) {
        if line.starts_with("a=candidate:") {
            body.push_str(line);
            body.push_str("\r\n");
        }
    }
    body.push_str("a=end-of-candidates\r\n");
    let mut resp = (StatusCode::OK, body).into_response();
    cors(&mut resp);
    let h = resp.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/trickle-ice-sdpfrag"),
    );
    if let Ok(v) = HeaderValue::from_str(&format!("\"{new_ufrag}\"")) {
        h.insert(header::ETAG, v);
    }
    resp
}

/// `DELETE /whip/v1/{id}`: end the session.
pub async fn delete(
    State(state): State<RtcState>,
    Path(sid): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    uri: axum::http::Uri,
) -> Response {
    if let Some(resp) = on_other_node(
        &state,
        &sid,
        axum::http::Method::DELETE,
        &uri,
        &headers,
        Bytes::new(),
    )
    .await
    {
        return resp;
    }
    let (conn, shard) = match session_for(&state, &query, &headers, &sid) {
        Ok(s) => s,
        Err((code, msg)) => return error(code, msg),
    };
    let leave = signal_request::Message::Leave(Default::default());
    send_signals(&state.engine.shards[shard].commands, conn, [leave]);
    state.whip.0.lock().unwrap().remove(&sid);
    let mut resp = StatusCode::OK.into_response();
    cors(&mut resp);
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    const OFFER: &str = "v=0\r\n\
        o=- 1 2 IN IP4 127.0.0.1\r\n\
        s=-\r\n\
        t=0 0\r\n\
        m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
        a=mid:0\r\n\
        a=sendonly\r\n\
        a=msid:s a1\r\n\
        m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
        a=mid:1\r\n\
        a=sendonly\r\n\
        a=rid:q send\r\n\
        a=rid:h send\r\n\
        a=rid:f send\r\n\
        a=simulcast:send q;h;f\r\n\
        m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
        a=mid:2\r\n\
        a=recvonly\r\n\
        m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
        a=mid:3\r\n";

    #[test]
    fn tracks_from_the_offer() {
        let t = add_tracks(OFFER);
        assert_eq!(t.len(), 2, "receive-only and data m-lines publish nothing");
        assert_eq!(t[0].r#type(), lk::TrackType::Audio);
        assert_eq!(t[0].cid, "a1");
        assert_eq!(t[0].source(), lk::TrackSource::Microphone);
        assert_eq!(t[1].r#type(), lk::TrackType::Video);
        assert!(t[1].cid.starts_with("TR_"));
        let q: Vec<(i32, &str)> = t[1]
            .layers
            .iter()
            .map(|l| (l.quality, l.rid.as_str()))
            .collect();
        assert_eq!(q, [(0, "q"), (1, "h"), (2, "f")]);
    }

    #[test]
    fn links_carry_credentials() {
        let l = link_headers(&[lk::IceServer {
            urls: vec!["turn:1.2.3.4:3478?transport=udp".into()],
            username: "u".into(),
            credential: "p".into(),
        }]);
        assert_eq!(
            l,
            [
                "<turn:1.2.3.4:3478?transport=udp>; rel=\"ice-server\"; username=\"u\"; credential=\"p\"; credential-type=\"password\""
            ]
        );
    }
}
