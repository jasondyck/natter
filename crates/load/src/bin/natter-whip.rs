//! natter-whip: publishes an Annex-B video stream from stdin over WHIP, for
//! the codecs no WHIP client at hand sends (ffmpeg's muxer is H.264 only;
//! Chrome on Linux has no H.265 encoder).
//!
//!     ffmpeg -re -f lavfi -i testsrc2=size=1280x720:rate=30 \
//!       -c:v libx265 -preset ultrafast -tune zerolatency -x265-params bframes=0:keyint=60 \
//!       -bsf:v hevc_metadata=aud=insert -f hevc - \
//!       | natter-whip --codec h265 --token "$TOKEN" http://localhost:7880/whip/v1
//!
//! Access units are split at access unit delimiters (the `aud=insert`
//! bitstream filter), each one a frame stamped `1/--fps` after the last.
//! The encoder can't be asked for a keyframe, so keyframe requests are only
//! logged: keep the keyframe interval short.

use std::io::Read as _;
use std::net::{SocketAddr, UdpSocket};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use clap::{Parser, ValueEnum};
use str0m::format::CodecConfig;
use str0m::media::{Direction, MediaKind, MediaTime, Mid};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, IceConnectionState, Input, Output, Rtc};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum VideoCodec {
    H264,
    H265,
}

#[derive(Parser)]
struct Args {
    /// The WHIP endpoint.
    url: String,
    /// Access token, sent as `Authorization: Bearer`.
    #[arg(long, env = "TOKEN")]
    token: String,
    #[arg(long, value_enum, default_value = "h265")]
    codec: VideoCodec,
    /// Frames a second, for the RTP timestamps.
    #[arg(long, default_value_t = 30)]
    fps: u32,
    /// Local address for the media socket.
    #[arg(long, default_value = "127.0.0.1")]
    ip: std::net::IpAddr,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    str0m::crypto::from_feature_flags().install_process_default();
    dimpl::crypto::CryptoProvider::install_default(dimpl::crypto::rust_crypto::default_provider());

    let socket = UdpSocket::bind((args.ip, 0)).context("binding the media socket")?;
    let local = socket.local_addr()?;
    let mut codecs = CodecConfig::empty();
    match args.codec {
        VideoCodec::H264 => codecs.enable_h264(true),
        VideoCodec::H265 => codecs.enable_h265(true),
    }
    let mut config = Rtc::builder();
    *config.codec_config() = codecs;
    let mut rtc = config.build(Instant::now());
    rtc.add_local_candidate(Candidate::host(local, "udp")?);
    let mut change = rtc.sdp_api();
    let mid = change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    let (offer, pending) = change.apply().context("no offer")?;

    let resp = reqwest::Client::new()
        .post(&args.url)
        .bearer_auth(&args.token)
        .header("Content-Type", "application/sdp")
        .body(offer.to_sdp_string())
        .send()
        .await?;
    let status = resp.status();
    let location = resp
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let body = resp.text().await?;
    if !status.is_success() {
        bail!("WHIP POST: {status}: {body}");
    }
    let answer = str0m::change::SdpAnswer::from_sdp_string(&body)?;
    rtc.sdp_api().accept_answer(pending, answer)?;
    eprintln!("session {}", location.as_deref().unwrap_or("?"));

    let (tx, frames) = mpsc::sync_channel(8);
    std::thread::spawn(move || read_access_units(args.codec, tx));
    run(&mut rtc, &socket, local, mid, args.fps, frames)
}

/// Sends frames from `frames` until stdin ends, then stops.
fn run(
    rtc: &mut Rtc,
    socket: &UdpSocket,
    local: SocketAddr,
    mid: Mid,
    fps: u32,
    frames: mpsc::Receiver<Vec<u8>>,
) -> anyhow::Result<()> {
    let mut buf = vec![0; 2000];
    let mut connected = false;
    let mut sent = 0u64;
    let started = Instant::now();
    loop {
        let timeout = loop {
            match rtc.poll_output()? {
                Output::Timeout(t) => break t,
                Output::Transmit(t) => {
                    socket.send_to(&t.contents, t.destination)?;
                }
                Output::Event(Event::IceConnectionStateChange(
                    IceConnectionState::Disconnected,
                )) => {
                    bail!("ICE disconnected");
                }
                Output::Event(Event::Connected) => {
                    connected = true;
                    eprintln!("connected after {:?}", started.elapsed());
                }
                Output::Event(Event::KeyframeRequest(r)) => {
                    eprintln!("keyframe request {:?}", r.kind)
                }
                Output::Event(_) => {}
            }
        };
        if connected {
            match frames.try_recv() {
                Ok(frame) => {
                    let writer = rtc.writer(mid).context("no writer")?;
                    let pt = writer
                        .payload_params()
                        .next()
                        .context("no payload type")?
                        .pt();
                    let time = MediaTime::new(
                        sent * 90_000 / fps as u64,
                        str0m::media::Frequency::NINETY_KHZ,
                    );
                    writer.write(pt, Instant::now(), time, frame)?;
                    sent += 1;
                    if sent.is_multiple_of(fps as u64 * 5) {
                        eprintln!("{sent} frames");
                    }
                    continue;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    eprintln!("stdin ended after {sent} frames");
                    return Ok(());
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
        }
        let wait = timeout
            .saturating_duration_since(Instant::now())
            .clamp(Duration::from_millis(1), Duration::from_millis(5));
        socket.set_read_timeout(Some(wait))?;
        let input = match socket.recv_from(&mut buf) {
            Ok((n, source)) => Input::Receive(
                Instant::now(),
                Receive::new(Protocol::Udp, source, local, &buf[..n])?,
            ),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Input::Timeout(Instant::now())
            }
            Err(e) => return Err(e.into()),
        };
        rtc.handle_input(input)?;
    }
}

/// Splits stdin into access units at access unit delimiters, AUD included.
fn read_access_units(codec: VideoCodec, tx: mpsc::SyncSender<Vec<u8>>) {
    let is_aud = |header: u8| match codec {
        VideoCodec::H264 => header & 0x1f == 9,
        VideoCodec::H265 => (header >> 1) & 0x3f == 35,
    };
    let mut stdin = std::io::stdin().lock();
    let mut data = Vec::new();
    let mut chunk = vec![0; 1 << 16];
    // Where the current access unit starts and how far the scan got.
    let (mut start, mut scan) = (None::<usize>, 0);
    loop {
        let n = stdin.read(&mut chunk).unwrap_or(0);
        if n == 0 {
            break;
        }
        data.extend_from_slice(&chunk[..n]);
        while scan + 4 < data.len() {
            if data[scan..scan + 3] == [0, 0, 1] && is_aud(data[scan + 3]) {
                // A 4-byte start code begins one byte earlier.
                let at = if scan > 0 && data[scan - 1] == 0 {
                    scan - 1
                } else {
                    scan
                };
                if let Some(s) = start
                    && tx.send(data[s..at].to_vec()).is_err()
                {
                    return;
                }
                start = Some(at);
            }
            scan += 1;
        }
        if let Some(s) = start {
            data.drain(..s);
            scan -= s;
            start = Some(0);
        }
    }
    if let Some(s) = start {
        let _ = tx.send(data[s..].to_vec());
    }
}
