//! LiveKit's `/rtc` (v0, two peer connections), as LiveKit's Go SDK and so
//! `lk load-test` speak it; natter and LiveKit both serve it.
//!
//! A publisher sends `addTrack`, waits for `trackPublished`, then offers its
//! publisher connection with the track. The server offers the subscriber
//! connection whenever subscriptions change and the client answers.
//! Candidates trickle from the server per connection. Publishers join with
//! `auto_subscribe=0`, as `lk load-test`'s publishers do.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, bail};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::{SinkExt as _, StreamExt as _};
use hmac::{Hmac, KeyInit, Mac};
use livekit_protocol as lk;
use lk::{signal_request, signal_response};
use prost::Message as _;
use sha2::Sha256;
use str0m::change::{SdpAnswer, SdpOffer, SdpPendingOffer};
use str0m::media::{Direction, MediaKind, Simulcast, SimulcastLayer};
use str0m::{Candidate, Rtc};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::{debug, warn};

use super::{Client, Ctx};
use crate::media::{self, Conn, ConnEvent, ConnId, Publish, Worker};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// The protocol version LiveKit's Go SDK sends.
const PROTOCOL: u32 = 16;

#[derive(Debug, Clone)]
pub struct Keys {
    pub key: String,
    pub secret: String,
}

/// An access token for `identity` in `room`, as `lk token create` mints.
pub fn token(keys: &Keys, identity: &str, room: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after 1970")
        .as_secs();
    let claims = serde_json::json!({
        "iss": keys.key,
        "sub": identity,
        "name": identity,
        "nbf": now,
        "exp": now + 6 * 3600,
        "video": {
            "room": room,
            "roomJoin": true,
            "canPublish": true,
            "canSubscribe": true,
        },
    });
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD.encode(claims.to_string());
    let signed = format!("{header}.{payload}");
    let mut mac =
        Hmac::<Sha256>::new_from_slice(keys.secret.as_bytes()).expect("HMAC takes any key");
    mac.update(signed.as_bytes());
    format!(
        "{signed}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}

/// Connects and waits for the join response; the rest of the client's
/// signalling then runs in the background.
pub async fn join(ctx: Arc<Ctx>, keys: Keys, client: Client) -> anyhow::Result<()> {
    let created = Instant::now();
    let base = ctx
        .url
        .replacen("http://", "ws://", 1)
        .replacen("https://", "wss://", 1);
    let url = format!(
        "{base}/rtc?access_token={}&auto_subscribe={}&protocol={PROTOCOL}&sdk=go&version=2.13.0",
        token(&keys, &client.identity, &client.room),
        u8::from(client.publisher.is_none()),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .context("connecting the signal WebSocket")?;
    let join = loop {
        match recv(&mut ws).await? {
            Some(signal_response::Message::Join(join)) => break join,
            Some(signal_response::Message::Leave(leave)) => bail!("refused: {leave:?}"),
            _ => {}
        }
    };
    let mut session = Session {
        worker: ctx.worker(),
        ctx,
        client,
        created,
        sid: join.participant.map(|p| p.sid).unwrap_or_default(),
        cid: String::new(),
        publishing: None,
        publisher: None,
        subscriber: None,
        candidates: Vec::new(),
        events: unbounded_channel().0,
        capped: HashSet::new(),
    };
    if let Some(cap) = session.cap_video(&join.other_participants) {
        send(&mut ws, cap).await?;
    }
    if session.client.publisher.is_some() {
        session.cid = format!("TR_load{}", session.client.identity);
        let add = match session.client.kind {
            MediaKind::Audio => lk::AddTrackRequest {
                cid: session.cid.clone(),
                name: "audio".into(),
                r#type: lk::TrackType::Audio as i32,
                source: lk::TrackSource::Microphone as i32,
                ..Default::default()
            },
            // VP8, one layer or three, as the Go SDK announces a camera.
            MediaKind::Video => lk::AddTrackRequest {
                cid: session.cid.clone(),
                name: "video".into(),
                r#type: lk::TrackType::Video as i32,
                source: lk::TrackSource::Camera as i32,
                width: 1280,
                height: 720,
                layers: video_layers(&session.ctx),
                simulcast_codecs: vec![lk::SimulcastCodec {
                    codec: "vp8".into(),
                    cid: session.cid.clone(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        };
        send(&mut ws, signal_request::Message::AddTrack(add)).await?;
    }
    let ping = Duration::from_secs(u64::from(join.ping_interval.max(1) as u32));
    tokio::spawn(async move {
        if let Err(e) = session.run(ws, ping).await {
            warn!("{}: {e:#}", session.client.identity);
        }
    });
    Ok(())
}

struct Session {
    ctx: Arc<Ctx>,
    worker: Worker,
    client: Client,
    created: Instant,
    sid: String,
    cid: String,
    /// The publisher connection between its offer and the answer.
    publishing: Option<(Rtc, std::net::UdpSocket, SdpPendingOffer, Publish)>,
    publisher: Option<ConnId>,
    subscriber: Option<ConnId>,
    /// Candidates for a connection not handed to the worker yet.
    candidates: Vec<(lk::SignalTarget, Candidate)>,
    /// Nobody listens: LiveKit signals everything over the WebSocket.
    events: UnboundedSender<ConnEvent>,
    /// Video tracks already asked to stay within `Ctx::sub_quality`.
    capped: HashSet<String>,
}

impl Session {
    async fn run(&mut self, mut ws: Ws, ping: Duration) -> anyhow::Result<()> {
        let mut pings = tokio::time::interval(ping);
        let mut leaving = self.ctx.leaving.subscribe();
        loop {
            tokio::select! {
                _ = leaving.changed() => {
                    let leave = lk::LeaveRequest {
                        reason: lk::DisconnectReason::ClientInitiated as i32,
                        ..Default::default()
                    };
                    send(&mut ws, signal_request::Message::Leave(leave)).await?;
                    let _ = ws.close(None).await;
                    return Ok(());
                }
                msg = recv(&mut ws) => {
                    let Some(msg) = msg? else { continue };
                    if let Some(reply) = self.handle(msg).await? {
                        send(&mut ws, reply).await?;
                    }
                }
                _ = pings.tick() => {
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("after 1970")
                        .as_millis() as i64;
                    send(&mut ws, signal_request::Message::PingReq(lk::Ping { timestamp, rtt: 0 }))
                        .await?;
                }
            }
        }
    }

    async fn handle(
        &mut self,
        msg: signal_response::Message,
    ) -> anyhow::Result<Option<signal_request::Message>> {
        use signal_response::Message as M;
        Ok(match msg {
            M::TrackPublished(published) if published.cid == self.cid => {
                let track = published.track.map(|t| t.sid).unwrap_or_default();
                Some(self.offer_publisher(&track)?)
            }
            M::Answer(sd) => {
                let Some((mut rtc, socket, pending, publish)) = self.publishing.take() else {
                    return Ok(None);
                };
                let answer = SdpAnswer::from_sdp_string(&sd.sdp).context("parsing the answer")?;
                rtc.sdp_api()
                    .accept_answer(pending, answer)
                    .context("accepting the answer")?;
                self.publisher = Some(self.hand_over(rtc, socket, Some(publish)));
                self.flush_candidates();
                None
            }
            M::Offer(sd) => {
                let offer = SdpOffer::from_sdp_string(&sd.sdp).context("parsing the offer")?;
                let answer = match self.subscriber {
                    Some(id) => self.worker.accept_offer(id, offer).await?,
                    None => {
                        let socket = media::bind(self.ctx.bind)?;
                        let mut rtc = media::client_rtc(socket.local_addr()?);
                        let answer = rtc.sdp_api().accept_offer(offer)?;
                        self.subscriber = Some(self.hand_over(rtc, socket, None));
                        self.flush_candidates();
                        answer
                    }
                };
                Some(signal_request::Message::Answer(lk::SessionDescription {
                    r#type: "answer".into(),
                    sdp: answer.to_sdp_string(),
                    id: sd.id,
                    ..Default::default()
                }))
            }
            M::Trickle(trickle) => {
                if let Some(candidate) = parse_candidate(&trickle.candidate_init) {
                    let target = trickle.target();
                    self.candidates.push((target, candidate));
                    self.flush_candidates();
                }
                None
            }
            M::Update(update) => self.cap_video(&update.participants),
            M::Leave(leave) => bail!("server asked to leave: {leave:?}"),
            _ => None,
        })
    }

    /// With `--sub-quality`: asks for the size of that layer on video
    /// tracks not asked about yet.
    fn cap_video(
        &mut self,
        participants: &[lk::ParticipantInfo],
    ) -> Option<signal_request::Message> {
        let quality = self.ctx.sub_quality?;
        let track_sids: Vec<String> = participants
            .iter()
            .flat_map(|p| &p.tracks)
            .filter(|t| {
                t.r#type == lk::TrackType::Video as i32 && self.capped.insert(t.sid.clone())
            })
            .map(|t| t.sid.clone())
            .collect();
        if track_sids.is_empty() {
            return None;
        }
        let (width, height) = LAYER_SIZES[usize::from(quality)];
        Some(signal_request::Message::TrackSetting(
            lk::UpdateTrackSettings {
                track_sids,
                width,
                height,
                ..Default::default()
            },
        ))
    }

    fn offer_publisher(&mut self, track: &str) -> anyhow::Result<signal_request::Message> {
        let publisher = self.client.publisher.context("not a publisher")?;
        let socket = media::bind(self.ctx.bind)?;
        let mut rtc = media::client_rtc(socket.local_addr()?);
        let mut api = rtc.sdp_api();
        let kind = self.client.kind;
        let simulcast = kind == MediaKind::Video && self.ctx.simulcast;
        let layers = simulcast.then(|| {
            let mut s = Simulcast::new();
            for (rid, _) in media::LAYERS {
                s.add_send_layer(SimulcastLayer::new(rid));
            }
            s
        });
        let mid = api.add_media(
            kind,
            Direction::SendOnly,
            Some(format!("{}|{track}", self.sid)),
            Some(self.cid.clone()),
            layers,
        );
        let (offer, pending) = api.apply().context("nothing to offer")?;
        let sdp = offer.to_sdp_string();
        self.publishing = Some((
            rtc,
            socket,
            pending,
            Publish::new(mid, publisher, kind, simulcast),
        ));
        Ok(signal_request::Message::Offer(lk::SessionDescription {
            r#type: "offer".into(),
            sdp,
            id: 1,
            ..Default::default()
        }))
    }

    fn hand_over(&self, rtc: Rtc, socket: std::net::UdpSocket, publish: Option<Publish>) -> ConnId {
        let conn = Conn::new(
            self.client.key,
            rtc,
            socket,
            publish,
            self.events.clone(),
            self.created,
        );
        let id = conn.id;
        self.worker.add(conn);
        id
    }

    fn flush_candidates(&mut self) {
        self.candidates.retain(|(target, candidate)| {
            let conn = match target {
                lk::SignalTarget::Publisher => self.publisher,
                lk::SignalTarget::Subscriber => self.subscriber,
            };
            match conn {
                Some(id) => {
                    self.worker.add_candidate(id, candidate.clone());
                    false
                }
                None => true,
            }
        });
    }
}

/// The layers a video publisher announces: 1280×720 alone, or with
/// simulcast 320×180, 640×360 and 1280×720 at their shares of the bitrate.
/// Simulcast layer sizes, q to f.
const LAYER_SIZES: [(u32, u32); 3] = [(320, 180), (640, 360), (1280, 720)];

fn video_layers(ctx: &Ctx) -> Vec<lk::VideoLayer> {
    let sizes = LAYER_SIZES;
    let qualities = [
        lk::VideoQuality::Low,
        lk::VideoQuality::Medium,
        lk::VideoQuality::High,
    ];
    let all: Vec<_> = (0..3)
        .map(|l| lk::VideoLayer {
            quality: qualities[l] as i32,
            width: sizes[l].0,
            height: sizes[l].1,
            bitrate: (f64::from(ctx.video_bps) * media::LAYERS[l].1) as u32,
            ..Default::default()
        })
        .collect();
    if ctx.simulcast {
        all
    } else {
        all[2..].to_vec()
    }
}

fn parse_candidate(init: &str) -> Option<Candidate> {
    #[derive(serde::Deserialize)]
    struct Init {
        #[serde(default)]
        candidate: String,
    }
    let init: Init = serde_json::from_str(init).ok()?;
    if init.candidate.is_empty() {
        return None;
    }
    Candidate::from_sdp_string(&init.candidate)
        .inspect_err(|e| debug!("unparsable candidate {}: {e}", init.candidate))
        .ok()
}

async fn send(ws: &mut Ws, msg: signal_request::Message) -> anyhow::Result<()> {
    let req = lk::SignalRequest { message: Some(msg) };
    ws.send(Message::Binary(req.encode_to_vec().into()))
        .await
        .context("signal WebSocket send")
}

/// The next signal message; `None` for frames that carry none.
async fn recv(ws: &mut Ws) -> anyhow::Result<Option<signal_response::Message>> {
    let frame = ws
        .next()
        .await
        .context("signal WebSocket closed")?
        .context("signal WebSocket")?;
    let resp = match frame {
        Message::Binary(bytes) => lk::SignalResponse::decode(bytes.as_ref())?,
        Message::Text(text) => serde_json::from_str(text.as_str())?,
        Message::Close(close) => bail!("signal WebSocket closed: {close:?}"),
        _ => return Ok(None),
    };
    Ok(resp.message)
}
