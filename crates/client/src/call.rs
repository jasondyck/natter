//! One voice call over LiveKit's signalling, as LiveKit's own JS SDK makes
//! it: a single `RTCPeerConnection` (`/rtc/v1`), the microphone published
//! in the join request itself, and everyone else's audio arriving on
//! receive-only transceivers the server fills.
//!
//! The browser does capture, Opus, echo cancellation and transport. This
//! module wires those up and speaks the protocol, in protobuf, through
//! `web-sys`.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use leptos::prelude::*;
use livekit_protocol as lk;
use lk::{signal_request, signal_response};
use natter_protocol::{RtcToken, RtcTokenRequest};
use prost::Message as _;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{JsFuture, spawn_local};
use web_sys::{
    BinaryType, HtmlAudioElement, MediaStream, MediaStreamConstraints, MediaStreamTrack,
    MessageEvent, RtcIceCandidateInit, RtcOfferOptions, RtcPeerConnection,
    RtcPeerConnectionIceEvent, RtcPeerConnectionState, RtcRtpTransceiver,
    RtcRtpTransceiverDirection, RtcRtpTransceiverInit, RtcSdpType, RtcSessionDescriptionInit,
    RtcTrackEvent, WebSocket,
};

use crate::api;

/// LiveKit's signalling protocol version this client speaks.
const PROTOCOL: i32 = 17;
/// Resume attempts after the connection drops, backing off from
/// `RESUME_FIRST_DELAY_MS`, before the call counts as failed.
const RESUME_ATTEMPTS: u32 = 5;
const RESUME_FIRST_DELAY_MS: i32 = 500;
/// Receive-only audio slots offered up front, as LiveKit's JS SDK does; the
/// server asks for more when a room outgrows them.
const AUDIO_SLOTS: u32 = 3;

#[derive(Debug, Clone, PartialEq)]
pub enum Phase {
    Idle,
    Connecting,
    Connected,
    Failed(String),
}

/// A participant's connection quality, from LiveKit's
/// `connection_quality` updates.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerQuality {
    pub quality: Quality,
    /// Mean opinion score, 1 (unusable) to 4.5.
    pub mos: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quality {
    Excellent,
    Good,
    Poor,
    /// The media path is down.
    Lost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    /// LiveKit participant sid (`PA_…`).
    pub sid: String,
    pub name: String,
}

/// What the UI renders. The call writes these; the UI only reads them.
#[derive(Clone, Copy)]
pub struct CallState {
    pub phase: RwSignal<Phase>,
    /// Everyone in the room, us included, in join order.
    pub peers: RwSignal<Vec<Peer>>,
    pub me: RwSignal<Option<String>>,
    /// Who is speaking now, from the server's `speakers_changed`.
    pub speaking: RwSignal<HashSet<String>>,
    pub quality: RwSignal<HashMap<String, PeerQuality>>,
}

impl CallState {
    pub fn new() -> Self {
        CallState {
            phase: RwSignal::new(Phase::Idle),
            peers: RwSignal::new(vec![]),
            me: RwSignal::new(None),
            speaking: RwSignal::new(HashSet::new()),
            quality: RwSignal::new(HashMap::new()),
        }
    }

    pub fn reset(&self) {
        self.phase.set(Phase::Idle);
        self.peers.set(vec![]);
        self.me.set(None);
        self.speaking.set(HashSet::new());
        self.quality.set(HashMap::new());
    }
}

pub struct Call {
    inner: Rc<Inner>,
    // JS holds these as event handlers, so they must live as long as the call.
    _on_track: Closure<dyn FnMut(RtcTrackEvent)>,
    _on_state: Closure<dyn FnMut()>,
    _on_ice: Closure<dyn FnMut(RtcPeerConnectionIceEvent)>,
    _ping: Closure<dyn FnMut()>,
    ping_timer: i32,
}

/// The WebSocket's handlers, kept to attach to the next socket on resume.
struct WsHandlers {
    on_message: Closure<dyn FnMut(MessageEvent)>,
    on_close: Closure<dyn FnMut()>,
}

struct Inner {
    pc: RtcPeerConnection,
    /// Replaced when the session resumes on a new connection.
    ws: RefCell<WebSocket>,
    ws_handlers: RefCell<Option<WsHandlers>>,
    /// The latest access token; the server sends fresh ones.
    token: RefCell<String>,
    /// A resume is under way, and how many have been tried in a row.
    resuming: Cell<bool>,
    resume_attempts: Cell<u32>,
    /// Hung up on purpose: a closing socket is not a reason to resume.
    leaving: Cell<bool>,
    mic: MediaStream,
    /// One `<audio>` element per remote track, keyed by stream id.
    audio: RefCell<HashMap<String, HtmlAudioElement>>,
    /// The microphone's track sid once the server has published it.
    mic_sid: RefCell<Option<String>>,
    muted: Cell<bool>,
    /// Offers are numbered so answers can be matched to them.
    offer_id: Cell<u32>,
    /// A re-offer is in flight; requests that arrive meanwhile wait.
    negotiating: Cell<bool>,
    pending_slots: Cell<Option<u32>>,
    /// Candidates gathered before the WebSocket opened.
    early_candidates: RefCell<Vec<String>>,
    last_error: RefCell<Option<String>>,
    state: CallState,
}

impl Call {
    pub async fn start(room: String, state: CallState) -> Result<Call, String> {
        let RtcToken { token, ice_servers } =
            api::post("/api/rtc/token", &RtcTokenRequest { room }).await?;
        let mic = microphone().await?;
        let (pc, join) = match offer(&mic, &ice_servers).await {
            Ok(r) => r,
            Err(e) => {
                stop_tracks(&mic);
                return Err(e);
            }
        };
        let ws = WebSocket::new(&ws_url(&token, &join)?).map_err(js_err)?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        let inner = Rc::new(Inner {
            pc,
            ws: RefCell::new(ws),
            ws_handlers: RefCell::default(),
            token: RefCell::new(token),
            resuming: Cell::new(false),
            resume_attempts: Cell::new(0),
            leaving: Cell::new(false),
            mic,
            audio: RefCell::default(),
            mic_sid: RefCell::default(),
            muted: Cell::new(false),
            offer_id: Cell::new(1),
            negotiating: Cell::new(false),
            pending_slots: Cell::new(None),
            early_candidates: RefCell::default(),
            last_error: RefCell::default(),
            state,
        });

        let on_message = {
            let inner = inner.clone();
            Closure::<dyn FnMut(MessageEvent)>::new(move |ev: MessageEvent| {
                let Ok(buf) = ev.data().dyn_into::<js_sys::ArrayBuffer>() else {
                    return;
                };
                let bytes = js_sys::Uint8Array::new(&buf).to_vec();
                match lk::SignalResponse::decode(bytes.as_slice()) {
                    Ok(resp) => {
                        if let Some(msg) = resp.message {
                            inner.handle(msg);
                        }
                    }
                    Err(e) => web_sys::console::warn_1(&format!("bad message: {e}").into()),
                }
            })
        };
        let on_close = {
            let inner = inner.clone();
            Closure::<dyn FnMut()>::new(move || {
                if inner.leaving.get() || state.phase.get_untracked() == Phase::Idle {
                    return;
                }
                if inner.last_error.borrow().is_some() {
                    // The server ended the call and said why.
                    let reason = inner.last_error.borrow_mut().take();
                    state.phase.set(Phase::Failed(reason.unwrap_or_default()));
                    return;
                }
                // Dropped, or a resume attempt that didn't connect: try
                // (again) to resume, as LiveKit's SDKs do.
                inner.resuming.set(false);
                inner.resume();
            })
        };
        let on_track = {
            let inner = inner.clone();
            Closure::<dyn FnMut(RtcTrackEvent)>::new(move |ev: RtcTrackEvent| {
                if let Some(stream) = ev.streams().get(0).dyn_ref::<MediaStream>() {
                    inner.play(stream);
                }
            })
        };
        let on_state = {
            let inner = inner.clone();
            Closure::<dyn FnMut()>::new(move || match inner.pc.connection_state() {
                RtcPeerConnectionState::Connected => inner.state.phase.set(Phase::Connected),
                // A network change: resume with an ICE restart.
                RtcPeerConnectionState::Failed => inner.resume(),
                _ => {}
            })
        };
        let on_ice = {
            let inner = inner.clone();
            Closure::<dyn FnMut(RtcPeerConnectionIceEvent)>::new(
                move |ev: RtcPeerConnectionIceEvent| {
                    let Some(c) = ev.candidate() else { return };
                    let Ok(json) = js_sys::JSON::stringify(&c.to_json()) else {
                        return;
                    };
                    let json = String::from(json);
                    if inner.ws.borrow().ready_state() == WebSocket::OPEN {
                        inner.trickle(json);
                    } else {
                        inner.early_candidates.borrow_mut().push(json);
                    }
                },
            )
        };
        let ping = {
            let inner = inner.clone();
            Closure::<dyn FnMut()>::new(move || {
                inner.send(signal_request::Message::PingReq(lk::Ping {
                    timestamp: js_sys::Date::now() as i64,
                    rtt: 0,
                }));
            })
        };

        *inner.ws_handlers.borrow_mut() = Some(WsHandlers {
            on_message,
            on_close,
        });
        inner.attach_ws();
        inner
            .pc
            .set_ontrack(Some(on_track.as_ref().unchecked_ref()));
        inner
            .pc
            .set_onconnectionstatechange(Some(on_state.as_ref().unchecked_ref()));
        inner
            .pc
            .set_onicecandidate(Some(on_ice.as_ref().unchecked_ref()));
        let ping_timer = web_sys::window()
            .ok_or("no window")?
            .set_interval_with_callback_and_timeout_and_arguments_0(
                ping.as_ref().unchecked_ref(),
                5000,
            )
            .map_err(js_err)?;

        Ok(Call {
            inner,
            _on_track: on_track,
            _on_state: on_state,
            _on_ice: on_ice,
            _ping: ping,
            ping_timer,
        })
    }

    pub fn set_muted(&self, muted: bool) {
        let inner = &self.inner;
        for track in inner.mic.get_audio_tracks() {
            track
                .unchecked_into::<MediaStreamTrack>()
                .set_enabled(!muted);
        }
        inner.muted.set(muted);
        if let Some(sid) = inner.mic_sid.borrow().clone() {
            inner.send(signal_request::Message::Mute(lk::MuteTrackRequest {
                sid,
                muted,
            }));
        }
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        let inner = &self.inner;
        inner.leaving.set(true);
        if let Some(w) = web_sys::window() {
            w.clear_interval_with_handle(self.ping_timer);
        }
        inner.send(signal_request::Message::Leave(lk::LeaveRequest {
            reason: lk::DisconnectReason::ClientInitiated as i32,
            action: lk::leave_request::Action::Disconnect as i32,
            ..Default::default()
        }));
        // Detach first so the closures are never called after they're freed.
        let ws = inner.ws.borrow();
        ws.set_onmessage(None);
        ws.set_onclose(None);
        // The handlers hold the call; dropping them breaks the cycle.
        inner.ws_handlers.borrow_mut().take();
        inner.pc.set_ontrack(None);
        inner.pc.set_onconnectionstatechange(None);
        inner.pc.set_onicecandidate(None);
        let _ = ws.close();
        drop(ws);
        inner.pc.close();
        stop_tracks(&inner.mic);
        for (_, el) in inner.audio.borrow_mut().drain() {
            el.remove();
        }
    }
}

impl Inner {
    fn attach_ws(&self) {
        let ws = self.ws.borrow();
        if let Some(h) = &*self.ws_handlers.borrow() {
            ws.set_onmessage(Some(h.on_message.as_ref().unchecked_ref()));
            ws.set_onclose(Some(h.on_close.as_ref().unchecked_ref()));
        }
    }

    /// Reconnects signalling to the same session and restarts ICE, as
    /// LiveKit's resume: a new WebSocket with `reconnect` and our sid, and
    /// an ICE-restart offer in the join request. Backs off between tries;
    /// after the last the call has failed.
    fn resume(self: &Rc<Self>) {
        if self.leaving.get() || self.resuming.get() {
            return;
        }
        let attempt = self.resume_attempts.get() + 1;
        if attempt > RESUME_ATTEMPTS {
            self.state
                .phase
                .set(Phase::Failed("lost connection to server".into()));
            return;
        }
        self.resuming.set(true);
        self.resume_attempts.set(attempt);
        self.state.phase.set(Phase::Connecting);
        let inner = self.clone();
        let delay = RESUME_FIRST_DELAY_MS << (attempt - 1);
        let go = Closure::once_into_js(move || {
            spawn_local(async move {
                if let Err(e) = inner.reconnect().await {
                    web_sys::console::warn_1(&format!("resume failed: {e}").into());
                    inner.resuming.set(false);
                    inner.resume();
                }
            });
        });
        if let Some(w) = web_sys::window() {
            let _ =
                w.set_timeout_with_callback_and_timeout_and_arguments_0(go.unchecked_ref(), delay);
        }
    }

    async fn reconnect(self: &Rc<Self>) -> Result<(), String> {
        let sid = self.state.me.get_untracked().ok_or("not joined yet")?;
        let options = RtcOfferOptions::new();
        options.set_ice_restart(true);
        let offer: RtcSessionDescriptionInit =
            JsFuture::from(self.pc.create_offer_with_rtc_offer_options(&options))
                .await
                .map_err(js_err)?
                .unchecked_into();
        JsFuture::from(self.pc.set_local_description(&offer))
            .await
            .map_err(js_err)?;
        let sdp = self
            .pc
            .local_description()
            .map(|d| d.sdp())
            .ok_or("no local description")?;
        let id = self.offer_id.get() + 1;
        self.offer_id.set(id);
        let join = lk::JoinRequest {
            client_info: Some(lk::ClientInfo {
                protocol: PROTOCOL,
                ..Default::default()
            }),
            connection_settings: Some(lk::ConnectionSettings {
                auto_subscribe: true,
                ..Default::default()
            }),
            reconnect: true,
            participant_sid: sid,
            publisher_offer: Some(lk::SessionDescription {
                r#type: "offer".into(),
                sdp,
                id,
                ..Default::default()
            }),
            ..Default::default()
        };
        let ws = WebSocket::new(&ws_url(&self.token.borrow(), &join)?).map_err(js_err)?;
        ws.set_binary_type(BinaryType::Arraybuffer);
        let old = self.ws.replace(ws);
        old.set_onmessage(None);
        old.set_onclose(None);
        let _ = old.close();
        self.negotiating.set(true);
        self.attach_ws();
        Ok(())
    }

    fn send(&self, msg: signal_request::Message) {
        let ws = self.ws.borrow();
        if ws.ready_state() != WebSocket::OPEN {
            return;
        }
        let bytes = lk::SignalRequest { message: Some(msg) }.encode_to_vec();
        if let Err(e) = ws.send_with_u8_array(&bytes) {
            web_sys::console::warn_1(&e);
        }
    }

    fn trickle(&self, candidate_init: String) {
        self.send(signal_request::Message::Trickle(lk::TrickleRequest {
            candidate_init,
            target: lk::SignalTarget::Publisher as i32,
            ..Default::default()
        }));
    }

    fn handle(self: &Rc<Self>, msg: signal_response::Message) {
        use signal_response::Message as M;
        match msg {
            M::Join(join) => {
                let me = join.participant.unwrap_or_default();
                self.state.me.set(Some(me.sid.clone()));
                let mut peers = vec![peer(&me)];
                peers.extend(join.other_participants.iter().map(peer));
                self.state.peers.set(peers);
                for c in self.early_candidates.take() {
                    self.trickle(c);
                }
            }
            M::Answer(sd) => {
                let inner = self.clone();
                spawn_local(async move {
                    if let Err(e) = inner.set_remote(RtcSdpType::Answer, &sd.sdp).await {
                        inner.state.phase.set(Phase::Failed(e));
                        return;
                    }
                    inner.negotiating.set(false);
                    if let Some(n) = inner.pending_slots.take() {
                        inner.reoffer(n);
                    }
                });
            }
            M::Trickle(t) => {
                let Ok(init) = js_sys::JSON::parse(&t.candidate_init) else {
                    return;
                };
                let init: RtcIceCandidateInit = init.unchecked_into();
                let _ = self
                    .pc
                    .add_ice_candidate_with_opt_rtc_ice_candidate_init(Some(&init));
            }
            M::TrackPublished(t) => {
                if let Some(track) = t.track {
                    *self.mic_sid.borrow_mut() = Some(track.sid.clone());
                    if self.muted.get() {
                        self.send(signal_request::Message::Mute(lk::MuteTrackRequest {
                            sid: track.sid,
                            muted: true,
                        }));
                    }
                }
            }
            M::Update(update) => {
                let me = self.state.me.get_untracked();
                self.state.peers.update(|peers| {
                    for p in update.participants {
                        let gone = p.state == lk::participant_info::State::Disconnected as i32;
                        let at = peers.iter().position(|x| x.sid == p.sid);
                        match (gone, at) {
                            (true, Some(i)) if Some(&p.sid) != me.as_ref() => {
                                peers.remove(i);
                            }
                            (false, Some(i)) => peers[i] = peer(&p),
                            (false, None) => peers.push(peer(&p)),
                            _ => {}
                        }
                    }
                });
                let live: HashSet<String> = self
                    .state
                    .peers
                    .get_untracked()
                    .into_iter()
                    .map(|p| p.sid)
                    .collect();
                self.audio.borrow_mut().retain(|stream, el| {
                    let keep = stream
                        .split('|')
                        .next()
                        .is_some_and(|sid| live.contains(sid));
                    if !keep {
                        el.remove();
                    }
                    keep
                });
            }
            M::SpeakersChanged(s) => {
                self.state.speaking.update(|speaking| {
                    for sp in s.speakers {
                        if sp.active {
                            speaking.insert(sp.sid);
                        } else {
                            speaking.remove(&sp.sid);
                        }
                    }
                });
            }
            M::ConnectionQuality(q) => {
                self.state.quality.update(|map| {
                    for u in q.updates {
                        let quality = match lk::ConnectionQuality::try_from(u.quality) {
                            Ok(lk::ConnectionQuality::Excellent) => Quality::Excellent,
                            Ok(lk::ConnectionQuality::Good) => Quality::Good,
                            Ok(lk::ConnectionQuality::Lost) => Quality::Lost,
                            _ => Quality::Poor,
                        };
                        map.insert(
                            u.participant_sid,
                            PeerQuality {
                                quality,
                                mos: u.score,
                            },
                        );
                    }
                });
            }
            M::MediaSectionsRequirement(req) => {
                if self.negotiating.get() {
                    let more = self.pending_slots.get().unwrap_or(0) + req.num_audios;
                    self.pending_slots.set(Some(more));
                } else {
                    self.reoffer(req.num_audios);
                }
            }
            M::Reconnect(_) => {
                self.resuming.set(false);
                self.resume_attempts.set(0);
                // If only signalling dropped, media never stopped and the
                // connection state won't change again to say so.
                if self.pc.connection_state() == RtcPeerConnectionState::Connected {
                    self.state.phase.set(Phase::Connected);
                }
                for c in self.early_candidates.take() {
                    self.trickle(c);
                }
            }
            M::RefreshToken(token) => *self.token.borrow_mut() = token,
            M::Leave(leave) if leave.action == lk::leave_request::Action::Resume as i32 => {
                // The server lost our media path and wants us back.
                self.resuming.set(false);
                self.resume();
            }
            M::Leave(leave) => {
                let reason = lk::DisconnectReason::try_from(leave.reason)
                    .map(|r| r.as_str_name().to_ascii_lowercase().replace('_', " "))
                    .unwrap_or_default();
                *self.last_error.borrow_mut() = Some(format!("disconnected: {reason}"));
                let _ = self.ws.borrow().close();
            }
            M::RequestResponse(r) if r.reason != lk::request_response::Reason::Ok as i32 => {
                web_sys::console::warn_1(&format!("server: {}", r.message).into());
            }
            _ => {}
        }
    }

    /// Answers `media_sections_requirement`: add the receive-only audio
    /// m-lines asked for, then offer again so the server can fill them.
    fn reoffer(self: &Rc<Self>, audios: u32) {
        self.negotiating.set(true);
        for _ in 0..audios {
            add_recv_slot(&self.pc);
        }
        let inner = self.clone();
        spawn_local(async move {
            match create_offer(&inner.pc).await {
                Ok(sdp) => {
                    let id = inner.offer_id.get() + 1;
                    inner.offer_id.set(id);
                    inner.send(signal_request::Message::Offer(lk::SessionDescription {
                        r#type: "offer".into(),
                        sdp,
                        id,
                        ..Default::default()
                    }));
                }
                Err(e) => inner.state.phase.set(Phase::Failed(e)),
            }
        });
    }

    async fn set_remote(&self, kind: RtcSdpType, sdp: &str) -> Result<(), String> {
        let desc = RtcSessionDescriptionInit::new(kind);
        desc.set_sdp(sdp);
        JsFuture::from(self.pc.set_remote_description(&desc))
            .await
            .map_err(js_err)?;
        Ok(())
    }

    /// Plays a remote track. Its stream id is LiveKit's `PA_…|TR_…`.
    fn play(&self, stream: &MediaStream) {
        let mut audio = self.audio.borrow_mut();
        if audio.contains_key(&stream.id()) {
            return;
        }
        let Some(el) = web_sys::window()
            .and_then(|w| w.document())
            .and_then(|d| d.create_element("audio").ok())
            .and_then(|el| el.dyn_into::<HtmlAudioElement>().ok())
        else {
            return;
        };
        el.set_autoplay(true);
        el.set_src_object(Some(stream));
        // Joining was a click, so autoplay policy allows this.
        let _ = el.play();
        audio.insert(stream.id(), el);
    }
}

fn peer(p: &lk::ParticipantInfo) -> Peer {
    Peer {
        sid: p.sid.clone(),
        name: if p.name.is_empty() {
            p.identity.clone()
        } else {
            p.name.clone()
        },
    }
}

async fn microphone() -> Result<MediaStream, String> {
    let devices = web_sys::window()
        .ok_or("no window")?
        .navigator()
        .media_devices()
        .map_err(|_| "microphone access needs HTTPS (or localhost)")?;
    let constraints = MediaStreamConstraints::new();
    // `true` keeps the browser defaults: echo cancellation, noise
    // suppression and gain control all on.
    constraints.set_audio(&JsValue::TRUE);
    let stream = JsFuture::from(
        devices
            .get_user_media_with_constraints(&constraints)
            .map_err(js_err)?,
    )
    .await
    .map_err(js_err)?;
    Ok(stream.unchecked_into())
}

/// The peer connection and the join request that publishes the mic: the
/// mic's send-only m-line plus empty receive-only ones, offered at once.
async fn offer(
    mic: &MediaStream,
    ice_servers: &[natter_protocol::IceServer],
) -> Result<(RtcPeerConnection, lk::JoinRequest), String> {
    let config = web_sys::RtcConfiguration::new();
    let servers = js_sys::Array::new();
    for s in ice_servers {
        let server = web_sys::RtcIceServer::new();
        let urls = js_sys::Array::new();
        for u in &s.urls {
            urls.push(&JsValue::from_str(u));
        }
        server.set_urls(&urls);
        server.set_username(&s.username);
        server.set_credential(&s.credential);
        servers.push(&server);
    }
    config.set_ice_servers(&servers);
    let pc = RtcPeerConnection::new_with_configuration(&config).map_err(js_err)?;
    let result = async {
        let init = RtcRtpTransceiverInit::new();
        init.set_direction(RtcRtpTransceiverDirection::Sendonly);
        let mut add_tracks = vec![];
        let mut mic_transceivers = vec![];
        for track in mic.get_audio_tracks() {
            let track: MediaStreamTrack = track.unchecked_into();
            let transceiver = pc.add_transceiver_with_media_stream_track_and_init(&track, &init);
            prefer_red(&transceiver);
            add_tracks.push(lk::AddTrackRequest {
                cid: track.id(),
                name: "microphone".into(),
                r#type: lk::TrackType::Audio as i32,
                source: lk::TrackSource::Microphone as i32,
                ..Default::default()
            });
            mic_transceivers.push((transceiver, track.id()));
        }
        for _ in 0..AUDIO_SLOTS {
            add_recv_slot(&pc);
        }
        let sdp = create_offer(&pc).await?;
        let mid_to_track_id = mic_transceivers
            .iter()
            .filter_map(|(t, cid)| t.mid().map(|m| (m, cid.clone())))
            .collect();
        Ok(lk::JoinRequest {
            client_info: Some(lk::ClientInfo {
                protocol: PROTOCOL,
                ..Default::default()
            }),
            connection_settings: Some(lk::ConnectionSettings {
                auto_subscribe: true,
                ..Default::default()
            }),
            add_track_requests: add_tracks,
            publisher_offer: Some(lk::SessionDescription {
                r#type: "offer".into(),
                sdp,
                id: 1,
                mid_to_track_id,
            }),
            ..Default::default()
        })
    }
    .await;
    match result {
        Ok(join) => Ok((pc, join)),
        Err(e) => {
            pc.close();
            Err(e)
        }
    }
}

fn add_recv_slot(pc: &RtcPeerConnection) {
    let init = RtcRtpTransceiverInit::new();
    init.set_direction(RtcRtpTransceiverDirection::Recvonly);
    pc.add_transceiver_with_str_and_init("audio", &init);
}

async fn create_offer(pc: &RtcPeerConnection) -> Result<String, String> {
    let offer: RtcSessionDescriptionInit = JsFuture::from(pc.create_offer())
        .await
        .map_err(js_err)?
        .unchecked_into();
    JsFuture::from(pc.set_local_description(&offer))
        .await
        .map_err(js_err)?;
    pc.local_description()
        .map(|d| d.sdp())
        .ok_or_else(|| "no local description".into())
}

/// Lists RED first among the mic's codecs, so the browser sends redundant
/// audio (each packet also carries the previous frame) when the server
/// accepts it, as LiveKit's SDK does. Browsers offer RED but don't prefer
/// it by default. Best effort: without RED the call is plain Opus.
fn prefer_red(transceiver: &RtcRtpTransceiver) {
    // web-sys has neither RTCRtpSender.getCapabilities nor
    // setCodecPreferences, so go through JS reflection.
    let result = (|| -> Result<(), JsValue> {
        let sender = js_sys::Reflect::get(&js_sys::global(), &"RTCRtpSender".into())?;
        let get_caps: js_sys::Function =
            js_sys::Reflect::get(&sender, &"getCapabilities".into())?.dyn_into()?;
        let caps = get_caps.call1(&sender, &"audio".into())?;
        let codecs: js_sys::Array = js_sys::Reflect::get(&caps, &"codecs".into())?.dyn_into()?;
        let is_red = |c: &JsValue| {
            js_sys::Reflect::get(c, &"mimeType".into())
                .ok()
                .and_then(|m| m.as_string())
                .is_some_and(|m| m.eq_ignore_ascii_case("audio/red"))
        };
        let ordered: js_sys::Array = codecs.iter().filter(|c| is_red(c)).collect();
        if ordered.length() == 0 {
            return Ok(());
        }
        for codec in codecs.iter().filter(|c| !is_red(c)) {
            ordered.push(&codec);
        }
        let set: js_sys::Function =
            js_sys::Reflect::get(transceiver, &"setCodecPreferences".into())?.dyn_into()?;
        set.call1(transceiver, &ordered)?;
        Ok(())
    })();
    if let Err(e) = result {
        web_sys::console::warn_1(&format!("can't prefer RED: {}", js_err(e)).into());
    }
}

fn stop_tracks(stream: &MediaStream) {
    for track in stream.get_tracks() {
        track.unchecked_into::<MediaStreamTrack>().stop();
    }
}

fn ws_url(token: &str, join: &lk::JoinRequest) -> Result<String, String> {
    let location = web_sys::window().ok_or("no window")?.location();
    let scheme = match location.protocol().map_err(js_err)?.as_str() {
        "https:" => "wss",
        _ => "ws",
    };
    let wrapped = lk::WrappedJoinRequest {
        compression: lk::wrapped_join_request::Compression::None as i32,
        join_request: join.encode_to_vec(),
    };
    let join = URL_SAFE_NO_PAD.encode(wrapped.encode_to_vec());
    let token = String::from(js_sys::encode_uri_component(token));
    Ok(format!(
        "{scheme}://{}/rtc/v1?access_token={token}&join_request={join}",
        location.host().map_err(js_err)?
    ))
}

pub fn js_err(e: JsValue) -> String {
    e.dyn_ref::<js_sys::Error>()
        .map(|e| String::from(e.message()))
        .or_else(|| e.as_string())
        .unwrap_or_else(|| format!("{e:?}"))
}
