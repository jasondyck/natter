# Feature parity

What natter supports today, what is planned, and how that compares with
LiveKit and other open-source media servers. natter's plan, phase by
phase, is [`livekit/PARITY.md`](livekit/PARITY.md); performance is in
[`benchmarks`](benchmarks/README.md).

Checked 2026-10-17 against each project's docs, source and changelogs:
LiveKit 1.13.10, mediasoup 3.30.1, Janus 1.4.3, Galène 1.2.1, the Jitsi Meet
stack (stable), MediaMTX 1.21.2. natter's column was last updated
2026-10-19. Sources are listed at the end.

| Mark | Meaning |
|---|---|
| ✅ | Supported |
| 🟡 | Partly (see note) |
| 🔌 | A separate component or the application's job |
| 🔜 *n* | natter: planned, in phase *n* of [PARITY.md](livekit/PARITY.md) |
| ❌ | Not supported |
| ❔ | Not confirmed in the docs |
| – | Doesn't apply |

mediasoup is a library: the application builds rooms, signalling and auth
on it. MediaMTX is a streaming server (one publisher per path, many
viewers), not a conferencing SFU.

## Conferencing and signalling

| | natter | LiveKit | mediasoup | Janus | Galène | Jitsi | MediaMTX |
|---|---|---|---|---|---|---|---|
| Many-to-many rooms | ✅ | ✅ | 🔌 | ✅ VideoRoom | ✅ | ✅ | ❌ |
| LiveKit SDKs and `lk` connect | ✅ `/rtc`, `/rtc/v1` | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Client SDKs | ✅ LiveKit's | ✅ web, iOS, Android, Flutter, RN, Rust, Unity, … | ✅ JS, C++ | ✅ janus.js | ✅ JS, Go | ✅ web, mobile | ❌ |
| Web app included | ✅ | ❌ (Meet is a separate example) | ❌ (demo repo) | 🟡 demos | ✅ | ✅ | 🟡 publish/view pages |
| Accounts and login | ✅ passkeys, invites | ❌ | 🔌 | ❌ | ✅ users, passwords | 🟡 via Prosody | ✅ users |
| Access tokens | ✅ LiveKit JWT | ✅ JWT | 🔌 | ✅ tokens, room PINs | ✅ JWT, tokens | ✅ JWT | ✅ JWT, callback |
| Resume after network change | ✅ | ✅ | 🔌 (`restartIce`) | ✅ ICE restart | 🟡 ICE restart on WHIP | ✅ ICE restart | ❔ |

## Audio

| | natter | LiveKit | mediasoup | Janus | Galène | Jitsi | MediaMTX |
|---|---|---|---|---|---|---|---|
| Opus | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| RED redundancy | ✅ incl. RED→Opus | ✅ | ❌ | 🟡 limited | ❔ | ✅ opt-in | ❔ |
| DTX | ✅ | ✅ | ✅ | ✅ | ❔ | ✅ opt-in | ❔ |
| Audio NACK resends | ✅ | ✅ | ✅ | ❔ | ❔ | ❔ | ❔ |
| Active speakers | ✅ LiveKit's algorithm | ✅ | ✅ | ✅ | 🟡 client-side | ✅ | ❌ |
| Connection quality | ✅ LiveKit's score | ✅ | 🔌 | ❔ | ❔ | ❔ | – |

## Video

| | natter | LiveKit | mediasoup | Janus | Galène | Jitsi | MediaMTX |
|---|---|---|---|---|---|---|---|
| VP8 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| H.264 | ✅ | ✅ | ✅ | ✅ | 🟡 no SVC | ✅ | ✅ |
| VP9 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| AV1 | ✅ | ✅ | ✅ | ✅ | 🟡 limited | ✅ | ✅ |
| H.265 | 🟡 negotiated, untested | ✅ default since 1.9.1¹ | ❌ removed in 3.16.6 | ✅ | ❌ | ❌ | ✅ |
| RTX retransmission | ✅ | ✅ | ✅ | ✅ | ❔ | ❔ | ❔ |
| Keyframe requests (PLI/FIR) | ✅ throttled as LiveKit | ✅ | ✅ | ✅ | ❔ | ✅ | ❔ |
| Simulcast | ✅ per-subscriber layers, dynacast, adaptive stream | ✅ incl. VP9, AV1 | ✅ VP8, H.264 | ✅ | ✅ | ✅ | ❔ |
| SVC | ✅ VP9, AV1 (dependency descriptor) | ✅ VP9, AV1 | ✅ VP9; AV1 forwarded | ✅ VP9, AV1 | ✅ VP8, VP9 | ✅ VP9, AV1 | ❔ |
| Congestion control | ✅ TWCC (GCC), REMB opt-in | ✅ REMB, TWCC opt-in | ✅ TWCC, REMB | 🟡 feedback only; layers by hand | ❔ | ✅ | ❔ |

¹ LiveKit's server enables H.265 by default (`config.go`, release 1.9.1),
though its codec docs don't list it.

## Transport and data

| | natter | LiveKit | mediasoup | Janus | Galène | Jitsi | MediaMTX |
|---|---|---|---|---|---|---|---|
| Data channels | ✅ reliable, lossy, data tracks; RPC and streams | ✅ (+ data tracks) | ✅ | ✅ | 🟡 peer to peer only | ✅ | ❔ |
| ICE over TCP | ✅ | ✅ | ✅ | ✅ | 🟡 via its TURN | 🟡 discouraged | ✅ |
| Built-in TURN | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ (coturn) | ❌ |
| End-to-end encryption pass-through | ✅ incl. packet trailers | ✅ | 🟡 works, undocumented | ✅ | ❌ | ✅ | – |
| WHIP ingest | ✅ | ✅ `/whip/v1` (🔌 Ingress to transcode) | ❌ | 🔌 simple-whip-server | ✅ | ❌ | ✅ |
| WHEP playback | 🔜 | ❌ | ❌ | 🔌 simple-whep-server | ❌ | ❌ | ✅ |

## Server and operations

| | natter | LiveKit | mediasoup | Janus | Galène | Jitsi | MediaMTX |
|---|---|---|---|---|---|---|---|
| Uses every core | ✅ engine per core; a room stays on one | ✅ | ✅ worker per core | ✅ threads | ✅ | ✅ | ✅ |
| Management API | ✅ LiveKit's Twirp (RoomService, AgentDispatch) | ✅ Twirp/HTTP | 🔌 in-process only | ✅ HTTP, WS, MQTT, … | ✅ admin HTTP | 🟡 Colibri REST, off by default | ✅ HTTP |
| Webhooks / events | ✅ LiveKit's, signed | ✅ | ❌ | ✅ event handlers | ❌ | ❔ | 🟡 run commands |
| Metrics | ✅ Prometheus, incl. LiveKit's names | ✅ Prometheus | 🔌 | ❔ | ❔ | ❔ | ✅ Prometheus |
| Recording | ❌ | 🔌 Egress | 🔌 RTP to FFmpeg | ✅ `.mjr` files | ✅ to disk | 🔌 Jibri | ✅ |
| RTMP / SRT / RTSP in | ❌ | 🔌 Ingress (RTMP, URL) | 🔌 | 🟡 RTSP, RTP | ❌ | ❌ | ✅ all |
| SIP telephony | ❌ | 🔌 livekit/sip | ❌ | ✅ SIP plugin | ❌ | 🔌 Jigasi | ❌ |
| AI agents | 🟡 dispatch and worker protocol; LiveKit Agents workers connect | 🔌 Agents | ❌ | ❌ | ❌ | ❌ | ❌ |
| Several servers | 🟡 Redis; a room stays on one node | 🟡 Redis; a room stays on one node | 🔌 pipe transports | ✅ remote publishers | ❌ | ✅ Octo cascading | ❌ |
| Language | Rust (str0m) | Go (Pion) | C++, Node/Rust API | C | Go (Pion) | Kotlin/Java | Go |
| Licence | MIT or Apache-2.0 | Apache-2.0 | ISC | GPL-3.0 | MIT | Apache-2.0 | MIT |

## What natter won't do

Recording, RTMP/SRT ingest and SIP are separate services in LiveKit
(Egress, Ingress, SIP) and are out of scope. Agents run in LiveKit's own
Agents framework: natter dispatches jobs to its workers, as LiveKit's
server does, but doesn't host them. Multi-node works as LiveKit's (Redis,
a room on one node), though it is rarely needed: one server carries 100
rooms of 8 on about one core ([benchmarks](benchmarks/README.md)).

## Sources

- LiveKit: [github.com/livekit/livekit](https://github.com/livekit/livekit),
  [releases](https://github.com/livekit/livekit/releases),
  [config-sample.yaml](https://github.com/livekit/livekit/blob/master/config-sample.yaml),
  [media docs](https://docs.livekit.io/transport/media/advanced/),
  [distributed setup](https://docs.livekit.io/transport/self-hosting/distributed/),
  [ingress](https://github.com/livekit/ingress), [egress](https://github.com/livekit/egress),
  [sip](https://github.com/livekit/sip); natter's own reading of the source in
  [`livekit/media.md`](livekit/media.md)
- mediasoup: [overview](https://mediasoup.org/documentation/overview/),
  [RTP capabilities](https://mediasoup.org/documentation/v3/mediasoup/rtp-parameters-and-capabilities/),
  [supportedRtpCapabilities.ts](https://github.com/versatica/mediasoup/blob/v3/node/src/supportedRtpCapabilities.ts),
  [API](https://mediasoup.org/documentation/v3/mediasoup/api/),
  [changelog](https://github.com/versatica/mediasoup/blob/v3/CHANGELOG.md)
- Janus: [VideoRoom](https://janus.conf.meetecho.com/docs/videoroom),
  [docs](https://janus.conf.meetecho.com/docs/),
  [changelog](https://github.com/meetecho/janus-gateway/blob/master/CHANGELOG.md),
  [simple-whep-server](https://github.com/meetecho/simple-whep-server)
- Galène: [galene.org](https://galene.org),
  [manual](https://github.com/jech/galene/blob/master/galene.md),
  [CHANGES](https://github.com/jech/galene/blob/master/CHANGES),
  [protocol](https://github.com/jech/galene/blob/master/galene-protocol.md),
  [API](https://github.com/jech/galene/blob/master/galene-api.md)
- Jitsi: [jitsi-videobridge](https://github.com/jitsi/jitsi-videobridge)
  ([reference.conf](https://github.com/jitsi/jitsi-videobridge/blob/master/jvb/src/main/resources/reference.conf),
  [TCP](https://github.com/jitsi/jitsi-videobridge/blob/master/doc/tcp.md)),
  [jitsi-meet config.js](https://github.com/jitsi/jitsi-meet/blob/master/config.js)
- MediaMTX: [github.com/bluenviron/mediamtx](https://github.com/bluenviron/mediamtx),
  [WebRTC clients](https://mediamtx.org/docs/publish/webrtc-clients),
  [WebRTC features](https://mediamtx.org/docs/features/webrtc-specific-features),
  [mediamtx.yml](https://github.com/bluenviron/mediamtx/blob/main/mediamtx.yml)

Corrections welcome: these projects move quickly.
