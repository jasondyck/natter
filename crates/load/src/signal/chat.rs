//! str0m's `chat` example (patched for rooms, `bench/servers/str0m-chat`):
//! the client POSTs its offer as JSON and gets the answer back; after that
//! the server renegotiates over the client's data channel, sending str0m's
//! JSON offers and taking JSON answers.

use std::sync::Arc;
use std::time::Instant;

use anyhow::Context as _;
use str0m::change::{SdpAnswer, SdpOffer};
use str0m::media::{Direction, MediaKind};
use tokio::sync::mpsc::unbounded_channel;
use tracing::warn;

use super::{Client, Ctx};
use crate::media::{self, Conn, ConnEvent, Publish};

pub async fn join(ctx: Arc<Ctx>, http: reqwest::Client, client: Client) -> anyhow::Result<()> {
    let created = Instant::now();
    let socket = media::bind(ctx.bind)?;
    let mut rtc = media::client_rtc(socket.local_addr()?);
    let mut api = rtc.sdp_api();
    api.add_channel("offer/answer".into());
    let publish = client.publisher.map(|id| {
        let mid = api.add_media(
            MediaKind::Audio,
            Direction::SendOnly,
            Some(client.identity.clone()),
            Some(format!("{}-mic", client.identity)),
            None,
        );
        Publish::new(mid, id, MediaKind::Audio, false)
    });
    let (offer, pending) = api.apply().context("nothing to offer")?;

    // A publisher takes nothing back, as in the other servers' scenarios.
    let receive = if publish.is_some() { 0 } else { 1 };
    let url = format!("{}/?room={}&recv={receive}", ctx.url, client.room);
    let answer: SdpAnswer = http
        .post(url)
        .json(&offer)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("reading the answer")?;
    rtc.sdp_api()
        .accept_answer(pending, answer)
        .context("accepting the answer")?;

    let (events, mut rx) = unbounded_channel();
    let worker = ctx.worker();
    let conn = Conn::new(client.key, rtc, socket, publish, events, created);
    let id = conn.id;
    worker.add(conn);

    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let ConnEvent::ChannelData(data) = event else {
                continue;
            };
            let Ok(offer) = serde_json::from_slice::<SdpOffer>(&data) else {
                continue;
            };
            match worker.accept_offer(id, offer).await {
                Ok(answer) => {
                    let text = serde_json::to_string(&answer).expect("answer serialises");
                    worker.send_text(id, text);
                }
                Err(e) => warn!("{}: renegotiation failed: {e}", client.identity),
            }
        }
    });
    Ok(())
}
