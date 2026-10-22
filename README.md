# natter

Browser voice chat with a Rust server that speaks LiveKit's protocol. The
server is a [str0m](https://github.com/algesten/str0m) SFU behind LiveKit's
signalling (`/rtc`, `/rtc/v1`, access tokens): each client sends its
microphone (and camera or screen, from LiveKit's SDKs) up once and the
server forwards the packets, undecoded, to everyone else in the room, with
rooms spread over one engine per core. Video covers VP8, H.264, VP9, AV1
and H.265 with resends and keyframe requests, and simulcast with each
subscriber getting the layer its video size needs (adaptive stream) and
publishers told which layers anyone watches (dynacast); congestion
control is next ([plan](docs/livekit/PARITY.md)). LiveKit's own SDKs and tools (`lk`,
`livekit-client`, the Go and Rust SDKs) connect to natter unchanged. The
web client is Leptos compiled to WASM, speaking the same protocol through
the browser's WebRTC stack.

## How it compares

Against LiveKit 1.13.10, driven by the same client on the same machine
(audio rooms of 4 publishers + 4 subscribers, no loss at any load):

| Rooms | Packets forwarded/s | natter CPU · memory | LiveKit CPU · memory |
|---|---|---|---|
| 8 | 6,400 | 6% · 48 MiB | 11% · 194 MiB |
| 30 | 24,000 | 24% · 137 MiB | 45% · 662 MiB |
| 60 | 48,000 | 55% · 262 MiB | 90% · 1.3 GiB |
| 100 | 80,000 | 106% · 427 MiB | 157% · 2.0 GiB |

CPU as a share of one core. natter also recovers more of a 5% packet loss
(0.002% left vs 0.26%). LiveKit sets clients up a little faster when many
join a busy server at once (p99 0.28 s vs 0.36 s at 60 rooms), which
`NATTER_DTLS_CRYPTO=aws-lc` closes; and it does far more (video, TURN,
server APIs). Charts, latency,
str0m's own chat example, other SFUs and method:
[`docs/benchmarks`](docs/benchmarks/README.md). What natter supports and
plans, next to LiveKit, mediasoup, Janus, Galène, Jitsi and MediaMTX:
[`docs/feature-parity.md`](docs/feature-parity.md).

## Layout

```
crates/
  protocol/  natter's /api bodies (accounts, invites, room tokens)
  server/    axum (/rtc, /api, static files) and the media engine thread
  client/    Leptos CSR app, built with trunk
  load/      natter-load, the load generator for benchmarks
vendor/str0m  str0m 0.24.1 with natter's patches (vendor/README.md)
docs/livekit  the LiveKit parity plan (PARITY.md) and reference material
docs/benchmarks  results against LiveKit and other SFUs, raw data
docs/feature-parity.md  supported and planned features vs other servers
bench/        benchmark scripts, other servers to compare with
```

## Run

```sh
rustup target add wasm32-unknown-unknown   # once
(cd crates/client && trunk build)          # -> crates/client/dist
cargo run -p natter-server
```

The first start logs an invite link (`no accounts yet; create the first one
at …`). Open it, pick a name and create a passkey. Then join a room from a
second browser profile using an invite from the first. Browsers only
allow the microphone on HTTPS or `localhost`, so from another machine put it
behind a reverse proxy that terminates TLS.

For UI work, `trunk serve` in `crates/client` serves on :8081 with hot reload
and proxies `/rtc` and `/api` to the server on :8080.

## Configuration

natter reads a LiveKit config file as `livekit-server` does, so a LiveKit
deployment's config moves over unchanged:

```sh
natter-server --config livekit.yaml        # or --config-body / LIVEKIT_CONFIG
natter-server --dev --bind 127.0.0.1       # LiveKit's dev mode: devkey/secret, 7880/7881/7882
natter-server --config livekit.yaml ports  # the ports it will use
natter-server generate-keys                # an API key and secret
```

LiveKit's flags work too (`--keys`, `--key-file`, `--node-ip`, `--udp-port`,
`--bind`, `--region`, `--turn-cert`, `--turn-key`, `--disable-strict-config`),
with their variables (`LIVEKIT_KEYS`, `NODE_IP`, `UDP_PORT`…), and the
generated ones for the keys natter reads (`--rtc.tcp_port 7881` or
`LIVEKIT_RTC_TCP_PORT`). A key LiveKit doesn't know is an error, as in
LiveKit's strict mode. natter acts on: `port`, `bind_addresses`, `keys`,
`key_file`, `region`, `development`, `log_level`/`logging.level`,
`prometheus` (port, basic auth), `rtc.udp_port` (a port or range: one media
engine per port), `rtc.port_range_start/end`, `rtc.tcp_port`, `rtc.node_ip`,
`rtc.use_external_ip` (STUN), `rtc.stun_servers`, `rtc.turn_servers`,
`rtc.congestion_control.enabled/allow_pause`, `room.auto_create`,
`room.empty_timeout/departure_timeout/max_participants`,
`room.enable_remote_unmute`, `turn.*` (enabled, udp_port, tls_port, domain,
cert/key files, external_tls) and `webhook.*` (urls, api_key,
filter_params). Other LiveKit keys are accepted and named in one warning at
startup. With a LiveKit config, LiveKit's defaults apply where natter's
differ (HTTP on 0.0.0.0:7880, ICE/TCP on 7881).

The variables below are natter's own; they override the config file.

| Variable | Default | Meaning |
|---|---|---|
| `NATTER_HTTP_ADDR` | `127.0.0.1:8080` | HTTP + WebSocket listener. Loopback by default; put a TLS proxy in front, or set `0.0.0.0:8080` to listen everywhere |
| `NATTER_UDP_PORT` | `50000` | UDP port of the first media engine; the others follow it |
| `NATTER_TCP_PORT` | `NATTER_UDP_PORT` | TCP port of the first media engine (ICE over TCP, for clients whose network blocks UDP; the others follow it, as for UDP), or `off`. LiveKit's `rtc.tcp_port` |
| `NATTER_TURN_PORT` | `off` | The embedded TURN server's port, UDP and TCP (LiveKit's `turn.udp_port`; 3478 is usual). See [TURN](#turn) |
| `NATTER_TURN_TLS` | none | TURN over TLS, as advertised: `domain:port`, e.g. `natter.example.com:443` |
| `NATTER_TURN_CERT`, `NATTER_TURN_KEY` | none | PEM certificate chain and key for natter to terminate TURN/TLS itself on `NATTER_TURN_TLS`'s port (needs a build with `--features tls`). Without them, terminate TLS in front of natter and forward it to `NATTER_TURN_PORT` over TCP |
| `NATTER_PUBLIC_IP` | default-route IP | Address browsers send media to. Set it to the IP clients reach this host on |
| `NATTER_UDP_BIND` | `NATTER_PUBLIC_IP` | Address the media socket binds. Use `0.0.0.0` only when the public IP isn't on this host (port forwarding) |
| `NATTER_ORIGIN` | `http://localhost:8080` | The URL people open. Passkeys are tied to its hostname, so changing it invalidates them |
| `NATTER_DATA_DIR` | `data` | Holds `natter.redb` (accounts, sessions, invites) |
| `NATTER_STATIC_DIR` | `crates/client/dist` | Built client |
| `NATTER_SHARDS` | number of cores | Media engines (threads). Engine *n* uses UDP `NATTER_UDP_PORT` + *n*, so open that whole range. Rooms are never split between engines |
| `NATTER_SHARD_FILL` | `120` | Participants an engine takes before new rooms go to the next; a busy engine is cheaper per packet than several idle ones, but a burst of joins queues on it. Rooms count as at least 8 people until they grow |
| `NATTER_PIN_ENGINES` | on | Keeps each engine on a CPU of its own (of those the process may use: a container's cpuset counts), so the scheduler doesn't move it between cores: about 15% less CPU at 100 audio rooms. `off` to leave it to the scheduler; also off when there are more engines than CPUs |
| `NATTER_DTLS_CRYPTO` | `aws-lc` in a build with `--features aws-lc` (as the `Dockerfile`'s), else `rust` | DTLS handshake crypto: `rust` (RustCrypto, P-256 on p256 0.14) or `aws-lc` (C and assembly; needs a build with `--features aws-lc`). `aws-lc` makes handshakes about three times cheaper, which only matters when hundreds of clients join one engine within a second: setup p99 at 30 rooms joining at once 0.38–0.41 s → 0.31–0.35 s |
| `NATTER_MAX_PEERS` | `200` | Joins beyond this are refused, so an overloaded server turns new calls away instead of degrading every call. See [Load testing](#load-testing) for what one core sustains |
| `NATTER_AUDIO_NACK` | `true` | Resend lost audio on request (RTCP NACK) to and from peers that negotiate it. Browsers don't; native clients and `lk` do. See [Loss recovery](#loss-recovery) |
| `NATTER_AUDIO_RED` | `true` | Redundant audio (RFC 2198): each packet also carries the previous frame |
| `NATTER_STATS` | `true` | RTP stats, 1s RTCP reports and per-peer [connection quality](#connection-quality) |
| `NATTER_CONGESTION_CONTROL` | `twcc` | How each subscriber's bandwidth is estimated, which decides the simulcast layers it gets: `twcc` (str0m's send-side estimator from the client's TWCC feedback, probing for more; LiveKit's `use_send_side_bwe`), `remb` (the client's own estimate, LiveKit's default; no probing) or `off` (every subscriber gets what its settings ask for) |
| `NATTER_WEBHOOK_URLS` | none | Comma-separated URLs that get LiveKit's webhooks. `https` needs a build with `--features tls`. See [Webhooks](#webhooks) |
| `NATTER_WEBHOOK_API_KEY` | the only key in `LIVEKIT_KEYS` | Which API key signs webhooks (LiveKit's `webhook.api_key`) |
| `NATTER_WEBHOOK_INCLUDE`, `NATTER_WEBHOOK_EXCLUDE` | none | Comma-separated events to send only, or not to send (LiveKit's `filter_params`) |
| `NATTER_WEBHOOK_TRACK_MUTES` | off | `1` also sends `track_muted` and `track_unmuted`, as Stoat's LiveKit fork does (upstream LiveKit doesn't). Naming either in the include list sends it too |
| `NATTER_REMOTE_UNMUTE` | `false` | Let RoomService's MutePublishedTrack unmute tracks, not only mute them (LiveKit's `room.enable_remote_unmute`) |
| `NATTER_ALLOW_PAUSE` | `false` | Let congestion control pause a subscriber's video entirely when even the lowest layer doesn't fit, with `stream_state_update` (LiveKit's `allow_pause`). A client can ask for it with `subscriber_allow_pause` |
| `LIVEKIT_KEYS` | none | API keys for LiveKit access tokens, `key: secret` per line (or comma-separated), as LiveKit. Anyone with a key can mint tokens for any room, e.g. with `lk token create`. Unset, natter only accepts the tokens it mints for its own signed-in users |
| `RUST_LOG` | `natter_server=info,str0m=warn` | Log filter |

The UDP port must be reachable from browsers (firewall rule). Clients
whose network blocks UDP fall back to ICE over TCP on the same port
numbers, or to [TURN](#turn) when it is on.

### TURN

Some networks allow only TLS on 443 through a proxy: neither UDP nor
ICE/TCP gets through, but TURN over TLS does. With `NATTER_TURN_PORT` set,
natter runs a TURN server (RFC 8656) and hands every client credentials for
it in the join response (`ice_servers`), as LiveKit's embedded TURN does:
`turn:` over UDP and TCP on that port, and `turns:` on `NATTER_TURN_TLS`.

- It relays only to natter's own media engines (as LiveKit's relay
  policy), so it can't be used as a general relay. Each allocation is a UDP
  socket beside the engines; to an engine a relayed client is just another
  address.
- Credentials are the TURN REST API's: `expiry:id` and an HMAC of it
  under a secret natter makes at startup, valid for starting allocations
  for 24 hours. A restart invalidates them, and clients get new ones when
  they rejoin or resume. A credential may hold 12 allocations.
- TLS: with `NATTER_TURN_CERT` and `NATTER_TURN_KEY` natter terminates it
  (rustls; `--features tls`, which brings in aws-lc). Otherwise, put
  anything that terminates TLS and forwards TCP to port 3478 in front, and set
  `NATTER_TURN_TLS` to the name and port clients connect to.

## Deploying

Build with `cargo build --release -p natter-server` and
`(cd crates/client && trunk build)`, or use the `Dockerfile`. Run
`natter-server` with `NATTER_HTTP_ADDR` on localhost behind a reverse proxy
that terminates TLS for `NATTER_ORIGIN`, `NATTER_STATIC_DIR` pointing at
`crates/client/dist`, and `NATTER_PUBLIC_IP` set to the address clients send
media to (UDP `NATTER_UDP_PORT` must be reachable there). The process keeps
its database in `NATTER_DATA_DIR`, and only one process can open it.

## With Stoat

[Stoat](https://github.com/stoatchat/self-hosted) runs voice on its own
LiveKit container (`livekit` in its `compose.yml`), and natter can take its
place with the same `livekit.yml`. Tested against Stoat 0.15.5's
self-hosted stack: its API's `join_call`, its web client in a voice channel,
and its `voice-ingress` events (join, leave, mute, unmute) all as with its
LiveKit. A `compose.override.yml` beside Stoat's:

```yaml
services:
  livekit:
    image: natter            # docker build -t natter .
    command: --config /etc/livekit.yml
    environment:
      NATTER_HTTP_ADDR: 0.0.0.0:7880
      NATTER_WEBHOOK_TRACK_MUTES: "1"   # voice-ingress needs mutes (see Webhooks)
    ports: !override
      - "7881-7896:7881-7896"
      - "50000-50100:50000-50100/udp"
```

natter runs one media engine per core, each on its own UDP and TCP port,
where LiveKit uses one TCP port. Stoat opens UDP 50000–50100, enough for
the UDP side, but only TCP 7881: publish (and allow through the firewall)
7881 up to 7881 + cores − 1 as above, or set `NATTER_SHARDS` to fit.
`natter-server --config livekit.yml ports` lists what it will use.

## Several nodes

With `redis.address` in a LiveKit config, natter nodes form one server,
as LiveKit's multi-node mode: each room lives on one node, placed by
LiveKit's `node_selector` (any/cpuload/sysload/regionaware, `sort_by`,
`algorithm`) among nodes that reported in the last 5 s, and recorded in
Redis. Clients and API calls can reach any node: a node relays the
signalling WebSocket, Twirp and WHIP requests for a room on another node
to that node, and lists every node's rooms. Media goes straight to the
room's node, which advertises its own address and TURN. Agent workers
registered with any node take jobs for rooms on any node.

Nodes reach each other over HTTP (LiveKit relays through Redis instead),
at `NATTER_INTERNAL_URL`, by default this host's address on the HTTP
port. `natter-server --config … list-nodes` shows the cluster. A node
stopped with SIGTERM leaves at once and frees its rooms; one that dies is
passed over after 5 s, and its rooms start afresh where clients rejoin.
Redis over TLS, Sentinel and Redis Cluster aren't supported yet.
`bench/cluster/` runs two nodes locally.

## Accounts

Sign-in is passkeys only ([webauthn-rs](https://github.com/kanidm/webauthn-rs)
0.6, which is RustCrypto rather than OpenSSL). There are no passwords.

- **Invite-only.** Any signed-in user can make an invite link
  (`/#invite=…`, single use, 7 days), with at most 10 unused at once. The
  token lives in the URL fragment, so it never appears in server or proxy
  logs.
- **Names** are NFKC-normalised, may not contain control or invisible
  characters, and are unique ignoring case and look-alikes (Unicode TR39
  confusables): `Bob`, `bob` and Cyrillic `Воb` are one name.
- **Sign in** with a name and passkey. The browser remembers the last name,
  so after the first time it's one click. Sessions last 30 days. A name with
  no account gets a normal-looking challenge that no passkey can answer, so
  the endpoint doesn't reveal who has an account.
- **Another device:** sign in on it (e.g. with your phone's passkey via the
  QR prompt) and use *Add passkey*.
- **Signing out** ends the session everywhere it is in use, including an
  open call.
- Session and invite tokens are stored only as SHA-256 hashes. The `/ws`
  upgrade requires a session and an `Origin` matching `NATTER_ORIGIN`.
  Expired sessions and invites are purged hourly.

### Administration

Stop the server, then run these with the same `NATTER_DATA_DIR` and
`NATTER_ORIGIN`:

| Command | Does |
|---|---|
| `natter-server invite` | prints a fresh invite (the way back in when locked out) |
| `natter-server users` | lists accounts |
| `natter-server delete-user NAME` | deletes an account, its sessions and its unused invites |
| `natter-server revoke-invites` | deletes every unused invite |

### Hostnames

Cookies are scoped to a hostname, not a port. Anything else served from
natter's hostname, such as another app on a different port of the same name,
receives natter's session cookie and can overwrite it. Give natter a
hostname of its own.

## How a call is set up

natter's web client joins the way LiveKit's JS SDK does by default:

1. It asks `POST /api/rtc/token` for a LiveKit access token for the room.
   The signed-in account is the identity.
2. It builds one `RTCPeerConnection` with the microphone (RED preferred)
   and three receive-only audio m-lines, makes an offer, and opens
   `/rtc/v1?access_token=…&join_request=…`. The join request carries the
   offer and the microphone's `add_track`, so it publishes in one round
   trip.
3. The server answers. When someone else's audio should reach it, the
   server labels one of the receive-only m-lines with that track
   (`a=msid:PA_…|TR_…`) and sends `media_sections_requirement`; the client
   offers again (adding m-lines if asked) and the answer carries the label.
4. Participant updates, active speakers and connection quality arrive as
   LiveKit's `update`, `speakers_changed` and `connection_quality`.

Clients that use LiveKit's classic dual-PC mode (`/rtc`, e.g. `lk` and the
Go SDK) get a publisher PC they offer on and a subscriber PC the server
offers on, with `_reliable`/`_lossy` data channels. Data packets are relayed
in both modes, stamped with the sender's identity.

## Speaking indicators

LiveKit's active-speaker algorithm, ported from its source: each packet's
RFC 6464 audio level counts for the time it covers; every 400 ms a window
active (≤ −35 dBov) for at least 40% of it gives a level, smoothed, and
the room's `speakers_changed` goes to everyone subscribed to the speaker.
The server also asks for Opus DTX (`usedtx=1`), so a silent microphone
sends about 5 packets a second instead of 50.

## Loss recovery

Two mechanisms, both on by default and matching LiveKit's audio defaults:

- **RED** (`NATTER_AUDIO_RED`). The client lists `audio/red` first with
  `setCodecPreferences`, and the server's answer lists it first, so Chrome
  wraps each Opus frame with a copy of the one before: about 2× the bytes,
  and any single lost packet is rebuilt on arrival. str0m unwraps RED from
  each speaker, recovers what it can, and wraps again for each listener
  that negotiated it, or converts it to plain Opus (with recovery, as
  LiveKit's `RedPrimaryReceiver`) for those that didn't. Measured in
  Chrome: ~60 bytes a packet without, ~124 with.
- **NACK** (`NATTER_AUDIO_NACK`). The server keeps the last 200 packets of
  every stream it sends (4s) and resends one on request, and asks speakers
  to resend what it missed. Audio resends go on the original stream, not
  over RTX, which upstream str0m can't do; `vendor/str0m` is patched for it
  (see `vendor/README.md`). Chrome doesn't negotiate NACK for Opus at all,
  so this is for native clients and LiveKit's Go SDK. With 5% uplink loss
  (measured with the earlier in-process load test), NACK cut what listeners
  missed from 5% to 0.02%, at the price of a ~50ms p99 for the packets that
  needed a resend.

## Connection quality

LiveKit's scorer, ported from its source: every 5s each published and
subscribed track is scored from its packet loss (weighted per codec: Opus
8, Opus with FEC 8/1.5, RED 4/1.5, video 10, and less for DTX-like rates),
rising slowly and falling fast; a participant's quality is its worst
track's. Changes go to the participant and to those subscribed to it as
`connection_quality`, with a mean opinion score. The client shows signal
bars, with the score in the tooltip. `NATTER_STATS=false` turns the stats
off.

## Recovering from network changes

As LiveKit: when ICE reports a media path lost, the server sends `leave`
with action `RESUME` and gives the client 5s; a client whose WebSocket
drops gets 15s. The client reconnects signalling to the same session
(`reconnect=1&sid=…`, or `reconnect` in the `/rtc/v1` join request), the
server answers with `ReconnectResponse` and the room's state, and both
sides restart ICE: the client re-offers (single PC, or the publisher PC),
the server re-offers the subscriber PC. natter's web client does this on
its own, with up to five tries backing off from half a second. Verified
with LiveKit's JS SDK (`signal-reconnect`; `resume-reconnect` fails the resume on purpose and ends in a full reconnect, which works too; single and
dual PC) and with natter's client: audio keeps flowing both ways. The
client's `sync_state` is checked against what the server has: a
publication the server doesn't know means a full reconnect (`leave` with
RECONNECT), as LiveKit; otherwise its disabled tracks and subscriptions
apply. Reliable data sent while the connection was down is resent by the
client after `last_message_seq`, and the server drops what it already
relayed.

A client whose connection keeps failing is moved to a more permissive
transport, as LiveKit does: UDP → ICE/TCP → TURN/TLS (when
`NATTER_TURN_TLS` is set). A connection that fails within 90 s of
connecting moves at once; an older one after two failures in a minute.
Held to TCP, the server leaves UDP candidates out of its SDP and ignores
the client's, and gives ICE/TCP 5–12 s (8× the signalling RTT) to connect
before moving on; held to TLS, the client gets only `turns:` servers and
`force_relay`. The preference lasts 5 minutes past the participant, so a
full reconnect keeps it. Verified with Chrome in a network namespace whose
only way out is a forwarder that cuts UDP, then TCP: the call moves to
ICE/TCP in 18 s and to TURN/TLS in 10 s, by resume, with one 0.23 s
freeze. LiveKit 1.13.10 in the same setup moves to TCP, then back to any
transport when the client reports the failure it already moved on, and
doesn't recover.

## WHIP

`POST /whip/v1` publishes into a room with one HTTP request (RFC 9725),
as LiveKit's native WHIP: the token as `Authorization: Bearer`, the SDP
offer as the body; the answer comes back with the session in `Location`,
TURN servers as `Link` headers and the ICE session as the `ETag`. The
session is a participant like any other to the room, with a track per
m-line it sends on (simulcast by its rids). `PATCH` trickles candidates
(`If-Match` the ETag) or restarts ICE (`If-Match: *`), `DELETE` ends it.
With nothing to signal to, a session whose transport fails is closed, and
its layer sizes come from the video itself (an SDP has none).

    ffmpeg -re -i in.mp4 -c:v libx264 -bf 0 -c:a libopus -ac 2 \
      -f whip -authorization "$TOKEN" https://natter.example/whip/v1

Verified with ffmpeg 9's WHIP muxer (H.264 Baseline + Opus) and a browser
WHIP client (trickle, VP8 simulcast, ICE restart, DELETE), watched with
LiveKit's JS SDK.

## Server API

LiveKit's RoomService at `/twirp/livekit.RoomService/<Rpc>` (Twirp,
protobuf or JSON), so server SDKs and `lk room …` work unchanged with an
API key from `LIVEKIT_KEYS`: CreateRoom, ListRooms, DeleteRoom,
ListParticipants, GetParticipant, RemoveParticipant, MutePublishedTrack,
UpdateParticipant (name, metadata, attributes, permissions: revoking
publishing unpublishes the tracks, revoking subscribing ends the
subscriptions), UpdateSubscriptions, SendData, UpdateRoomMetadata and
PerformRpc. ForwardParticipant and MoveParticipant are cloud-only in
LiveKit and answer `unimplemented`, after the same checks. Grants, error
codes and messages are LiveKit's; JSON responses are camelCase (LiveKit
writes snake_case; JSON parsers for protobuf take both).

    lk --url http://localhost:8080 --api-key KEY --api-secret SECRET room list

## Webhooks

As LiveKit's: `room_started`, `room_finished` (with its end reason),
`participant_joined` (once active), `participant_left` (or
`participant_connection_aborted` if it never got there),
`track_published` and `track_unpublished` (also for each track of a
participant that leaves), POSTed as protobuf JSON with
`Content-Type: application/webhook+json`, signed for LiveKit's
`WebhookReceiver` (a 5-minute token from the webhook key, the body's
SHA-256 in its `sha256` claim). Events queue per room, so they arrive in
order and one room's slow deliveries don't hold up another's; a failed send
is tried four more times backing off from 1 s, and events that waited
over 30 s, or past 200 in a queue, are dropped (all LiveKit's defaults).
Verified with `livekit-server-sdk`'s `WebhookReceiver`: the same events,
in the same order, as LiveKit 1.13.10 sends for the same session.

Optionally also `track_muted` and `track_unmuted` (the room, the
participant's sid and identity, and the track as it is now), when a client
or an admin mutes or unmutes a track. LiveKit doesn't send these; Stoat's
fork of it does, and Stoat's voice service needs them. Off unless
`NATTER_WEBHOOK_TRACK_MUTES=1` or the event is in the include list.

## Metrics

`GET /metrics` on the HTTP listener returns Prometheus text format:

| Metric | Type | Meaning |
|---|---|---|
| `natter_peers` | gauge | peers in a room |
| `natter_rooms` | gauge | rooms with at least one peer |
| `natter_media_connected_total` | counter | media paths that reached DTLS-connected |
| `natter_packets_received_total` | counter | RTP frames received |
| `natter_packets_forwarded_total` | counter | RTP frames written out, per recipient |
| `natter_bytes_received_total` | counter | RTP payload bytes received |
| `natter_bytes_forwarded_total` | counter | RTP payload bytes written, per recipient |
| `natter_audio_levels_total` | counter | frames carrying an audio level |
| `natter_udp_send_errors_total` | counter | failed UDP sends |
| `natter_joins_refused_total` | counter | joins refused at `NATTER_MAX_PEERS` |
| `natter_resumes_total` | counter | sessions resumed by reconnecting clients |
| `natter_engine_loops_total` | counter | passes of the media engine's loop |
| `natter_transport_polls_total` | counter | peer connections polled; per forwarded packet, the loop's overhead |

`/metrics` on the HTTP listener has no authentication, so keep that listener
private (behind the TLS proxy) and scrape it from there.
With `prometheus.port` in a LiveKit config, `/metrics` is also served on that
port, behind basic auth when `prometheus.username` and `password` are set.

After natter's own metrics come LiveKit's, under its names, labels and
buckets (`livekit_room_total`, `livekit_packet_total`, `livekit_rtt_ms`,
`livekit_session_start_time_ms`, …, each with `node_id` and
`node_type="SERVER"`), and the `process_*` collector, so LiveKit's dashboards
and alerts work unchanged. See `docs/livekit/PARITY.md` for the few it
doesn't have.

## Load testing

`natter-load` (`crates/load`) is one WebRTC client for every server, so
servers are compared on equal terms. Each room gets publishers sending fake
20ms Opus packets and subscribers receiving them all; every packet carries
its publisher, sequence number and send time, so subscribers count loss,
duplicates and one-way latency exactly, and the server's CPU and memory
come from `/proc/<pid>` or its container's cgroup over the same window. It
speaks LiveKit's `/rtc` (natter, LiveKit) and str0m's chat example
(`bench/servers/str0m-chat`); others are planned in
[`docs/benchmarks/landscape.md`](docs/benchmarks/landscape.md).
`--loss-down`/`--loss-up` drop that percentage of RTP/RTCP once everyone
is connected, to check resends. `--video` makes publishers send a VP8
camera track instead (`--video-kbps`, `--fps`; LiveKit signalling only):
loss and latency are then per frame, and subscribers also count frames a
decoder couldn't show (after a loss, until a keyframe), asking for
keyframes as a browser would.

`bench/compare.sh` runs it against each server in turn, each started
fresh, so they are compared within one session (results drift by up to a
quarter between sessions on a busy desktop):

```sh
cargo build --release -p natter-server -p natter-load
(cd bench/servers/str0m-chat && cargo build --release)
LOAD_ARGS="--duration 20s" bench/compare.sh 60 2 natter livekit   # 60 rooms, twice round
bench/compare.sh 100 natter:16 livekit                           # natter with 16 engines
```

Results, charts and raw data: [`docs/benchmarks`](docs/benchmarks/README.md).

`bench/lk-bench.sh` cross-checks with LiveKit's own load tester (`lk
load-test`, copied out of the `livekit/livekit-cli` image next to the
script, or `LK`). Its clients cost both servers more per packet than
`natter-load`'s (not yet pinned down why); earlier it measured:

| Rooms (4 + 4) | Forwards/s | natter | LiveKit 1.13.10 |
|---|---|---|---|
| 8 | 6,400 | 8.9% · 13.9µs · 41 MiB | 14.7% · 23.0µs · 261 MiB |
| 30 | 24,000 | 41.1% · 17.1µs · 103 MiB | 60.7% · 25.3µs · 956 MiB |
| 60 | 48,000 | 83.6% · 17.4µs · 200 MiB | 136.9% · 28.5µs · 1.75 GiB |

That table predates the ICE and scheduler commits (together −14% at 30
rooms, −6% at 60). With `lk` at 100 rooms one engine saturates and drops
up to 0.07% of packets; 16 engines packed 250 participants each carry it
at 176% of a core and 270 MiB, LiveKit at 242% and 2.2–2.5 GiB. Spreading
rooms evenly over all 16 costs ~40% more CPU than one engine at 60 rooms
(idle threads wake per packet); packing costs ~17%.

The media socket asks for 4 MiB buffers, enough to ride out a burst of
joins. Linux caps that at
`net.core.rmem_max`/`wmem_max` (often 208 KiB); the server warns if so.

To profile, build with frame pointers so call graphs are cheap and
trustworthy (the default build folds identical functions under one symbol),
and record the engine thread (`rtc`) while a load runs:

```sh
RUSTFLAGS="-C force-frame-pointers=yes" CARGO_TARGET_DIR=target/fp \
  CARGO_PROFILE_RELEASE_DEBUG=line-tables-only cargo build --release -p natter-server
# start target/fp/release/natter-server and a load, then:
perf record -F 2999 --call-graph fp -t <rtc thread id> -- sleep 10
perf report --children --percentage relative
```

`perf` needs `kernel.perf_event_paranoid` at 1 or lower. Without root,
Callgrind (`--tool=callgrind --separate-threads=yes`) gives instruction
counts per function instead.

## Rust coverage

Everything in this repo is Rust; the browser's WebRTC engine is the only
non-Rust code in the media path. The database is redb, the passkey checks
are webauthn-rs on RustCrypto, and DTLS, SRTP and the DTLS certificate are
RustCrypto. The default build has no C dependency in the server.

- **aws-lc** is optional (`cargo build --features aws-lc`, then
  `NATTER_DTLS_CRYPTO=aws-lc`): faster handshakes under join bursts.
  Upstream str0m generates its certificate with dimpl's `rcgen` feature,
  which links aws-lc; the vendored `str0m-rust-crypto` makes the same
  certificate with p256 and x509-cert instead (`vendor/README.md`).
  `install_crypto` still installs dimpl's RustCrypto provider explicitly,
  so an aws-lc build only uses aws-lc when asked.

## Next

[`docs/livekit/PARITY.md`](docs/livekit/PARITY.md) is the plan: LiveKit
parity phase by phase (resume, sharding over cores, video, simulcast...),
with what is done ticked.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. The vendored crates in `vendor/`
keep their own licenses.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in natter by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
