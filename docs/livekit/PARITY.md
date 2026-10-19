# LiveKit parity roadmap

Goal: natter is a drop-in replacement for the open-source LiveKit server
v1.13.10 core SFU. LiveKit's own SDKs (JS, Swift, Kotlin, Flutter, Rust,
Unity), `lk` and its load tester connect to natter unchanged and get the
same behaviour. Egress, Ingress, SIP and hosting agents come later, if
ever.

Reference material in this directory, all from the v1.13.10 tag and the
`livekit/protocol` commit it pins (`9bb708cdb819`):

- [`media.md`](media.md): codecs, extensions, congestion control,
  simulcast/SVC, NACK/RED, data channels, transport. Cites `file:line`.
- [`protocol.md`](protocol.md): signalling, auth, Twirp APIs, room model,
  webhooks, operational surface.
- [`str0m.md`](str0m.md): what str0m 0.24.1 gives us for each of those.
- [`capture/`](capture): real signalling between `lk load-test` and
  LiveKit 1.13.10 (one publisher, one subscriber), with SDPs.

Wire types come from the `livekit-protocol` crate 0.8.1, whose protos are
identical to the server's pinned ones.

## Status and next steps (2026-10-19)

All phases are done (1-9), apart from the open items below and under each
phase. Merged on master through #28 (multi-node). New work branches from
master.

**Done since:** `fast_publish` in the JoinResponse, as LiveKit
(`CanPublish() && !hasICEFallback`, `pkg/rtc/room.go`
`createJoinResponseLocked`; natter's ICE fallback is `Fallback::pref`
other than `Any`). Dual-PC SDKs now connect the publisher PC at join, so a
first publish skips ICE/DTLS: in `bench/e2e/fastpub.mjs` a subscriber gets
the first published microphone after ~61 ms instead of ~125 ms (LiveKit
~63 ms), and a participant without the publish grant still leaves its
publisher PC unconnected, as on LiveKit. The cost is one more DTLS
handshake per joiner allowed to publish: 2 publishers and 150 subscribers
joining at 50/s with `lk load-test` (12 s) took 6.5 CPU-s instead of
5.7 (+14%); LiveKit pays the same handshake.

**Joins faster than LiveKit's** (`lk load-test`, 1 audio + 1 video
publisher and 6 subscribers at 2/s, loopback, alternating runs):
`livekit_session_join_latency_ms` 12–19 ms → 1.4 ms (LiveKit 1.6–2.6),
`livekit_session_start_time_ms` 116–122 ms → 6.5 ms (LiveKit 8.6–10.3).
Timing each stage found:
- Building each peer connection cost 4.2 ms: dimpl self-tested its crypto
  provider per connection. One shared config per MTU (vendored
  `str0m-rust-crypto`), 0.11 ms, and 24% less CPU for a join burst.
- The engine only saw a command when its UDP read returned (a packet or
  the up-to-20 ms timeout). A command now wakes it with a datagram
  (`engine::Commands`).
- ICE waited Ta (50 ms) three times: to answer a check queued before the
  answer, for its first check after the answer, and for the nominating
  check. Ta now paces checks only, from the last one sent (vendored `is`).
- The join-latency clock now starts when the request comes in, before
  the token check, as LiveKit's.

**Next:** the open items, in order:
   - `perms.mjs`: LiveKit sends a participant's subscription permission
     events video first, natter audio first (on master too, so older than
     the join work).
   - Congestion control at 1 Mbit/s on a 150 ms path: p99 285 ms
     against LiveKit's 196 ms.
   - Temporal layers by `fps`.
   - `stream_state_update`.
   - DD rewrite (`DDForwarder`).
   - Stream trackers as LiveKit's.
   - `subscribed_audio_codec_update`.
   - A Chrome test under a real bottleneck.
   - UDP-unstable fallback.
   - WHEP.
   - `room_finished` with SERVER_SHUTDOWN when natter stops.
   - Multi-node: Redis TLS/Sentinel/Cluster, draining that waits for
     rooms, several replicas on Kubernetes (UDP ports per pod).

**How each piece was checked.** The JS SDK scenarios since #21, and the
Python agent scenarios, are in `bench/e2e/` (`setup.sh`, `run.sh
lk|natter`, see its README); older ones were rebuilt per session from the
pieces below.
- LiveKit JS SDK 2.23 UMD in a page served from :8765.
- playwright-core, with Playwright's Chromium build
  (`chromium-1248`).
- `lk` 2.19 CLI.
- LiveKit 1.13.10 in Docker (`livekit/livekit-server:v1.13.10 --dev` or
  `--config`).
- `livekit-server-sdk` (npm) `WebhookReceiver` for webhooks.
- Run every scenario against both natter and LiveKit, and record the
  differences in the PR.
- Network-cut tests without root: Chrome in `unshare -rn`, plus a child
  netns joined by veth. A Python forwarder whose halves talk over unix
  sockets cuts UDP or TCP.

## Architecture decisions

- **Two peer connections per participant**, as LiveKit's `/rtc` (v0). The
  publisher PC (client offers) only receives. The subscriber PC (server
  offers) only sends. Single-PC `/rtc/v1` comes later on the same core.
- **Forward in str0m RTP mode** on the subscriber side: one shared
  `Arc<[u8]>` per received packet, and seq/timestamp rewriting of our own
  (LiveKit's DownTrack/munger). It's the only mode that keeps simulcast
  switching, temporal/SVC layers, E2EE and low latency under loss. Because
  publisher and subscriber are separate `Rtc`s, each can pick its mode.
- **Same defaults as LiveKit** unless a deviation is listed under
  "Deliberate differences".
- natter's accounts (passkeys, invites) become a token server: the web app
  mints LiveKit access tokens for its signed-in users. Anyone with an API
  key can mint their own, as with LiveKit.

## Phases

Each phase ends with LiveKit's own clients passing against natter, plus a
benchmark against LiveKit with identical clients.

### 1. Wire protocol, audio (in progress)

- [x] Access tokens: HS256, LiveKit claims and grant defaults, `LIVEKIT_KEYS`
- [x] Ids (`RM_`, `PA_`, `TR_AM…`), signal framing (protobuf/protojson)
- [x] `/rtc` WebSocket: auth (header or `access_token`), query params,
      `join`, `refresh_token`, ping/`pong_resp`, `leave` both ways
- [x] Publisher PC: client offer → answer, trickle both ways, `add_track`
      → `track_published`, `mute`
- [x] Subscriber PC: server offers (data channels first), auto-subscribe,
      `subscription`, `track_subscribed`, `subscription_permission_update`
- [x] `_reliable`/`_lossy` data channels: relay `DataPacket`s with
      `destination_identities`, sender identity stamped
- [x] Room model: participant `update`s (versioned), `room_update`,
      `speakers_changed`, `connection_quality` (LiveKit's loss-based
      score), empty/departure timeouts, max participants
- [x] RTP-mode audio forwarding: Opus and RED pass-through, RED→Opus for
      non-RED subscribers, NACK both ways (200-packet buffers)
- [x] `/rtc/validate`, health check `/`
- [x] natter's web client on the LiveKit protocol (`/rtc/v1`, tokens from
      `POST /api/rtc/token`); old `/ws`, its SFU and in-process load test
      removed
- [x] Resume: `reconnect=1` (or `JoinRequest.reconnect`), session kept
      5s after a media failure (`leave` RESUME) or 15s after a signalling
      drop, `ReconnectResponse`, ICE restart both ways; natter's web
      client resumes on its own. `sync_state` and data replay: phase 7
- [x] `lk load-test` audio passes; benchmark (below)
- [x] `livekit-client` 2.23 (JS) in Chrome: audio, RED, data both ways,
      speakers, quality; same results as against LiveKit
- [x] Single-PC `/rtc/v1` with `join_request` (gzip too): the JS SDK's
      default. Forwarded tracks go out on the client's receive-only
      m-lines, labelled by msid in the answer; `media_sections_requirement`
      asks for a re-offer (with more m-lines when short). Tracks and offer
      in the join request are applied at once (fast publish).
- [x] Data channels verified end to end (reliable data across resumes, server SendData, RPC; phase 7–8)
- [x] NACK under loss verified (`natter-load --loss-down/--loss-up 5`)

Notes from the first runs:

- Health check: natter serves its web app at `/`, so no `OK` there; no
  LiveKit client uses it.
- LiveKit never drops a client for not sending signal pings (the Go SDK
  sends none); it pings the WebSocket every 10s. natter does the same.
- str0m ran ICE upkeep on every timeout fed in (one per forwarded packet);
  now only when due (vendor patch), and the engine polls due transports
  off a timer heap instead of scanning all of them. Interleaved runs:
  −14% CPU at 30 rooms, −6% at 60. Cross-session numbers vary by up to
  ~25% on the test machine; compare builds interleaved.

**Benchmark, identical clients** (2026-10-17, loopback): `lk
load-test --audio-publishers 4 --subscribers 4` per room against each
server, whole-process CPU over 15s once joined, no loss anywhere.

| Rooms | Forwards/s | natter (`/rtc`) | LiveKit 1.13.10 |
|---|---|---|---|
| 8 | 6,400 | 8.9% · 13.9µs · 41 MiB | 14.7% · 23.0µs · 261 MiB |
| 30 | 24,000 | 41.1% · 17.1µs · 103 MiB | 60.7% · 25.3µs · 956 MiB |
| 60 | 48,000 | 83.6% · 17.4µs · 200 MiB | 136.9% · 28.5µs · 1.75 GiB |

CPU as a share of one core, then CPU per forwarded packet, then RSS.
natter's media is still one thread; LiveKit uses every core.

### Benchmark suite (alongside the phases)

See [`../benchmarks/landscape.md`](../benchmarks/landscape.md).

- [x] `natter-load`: Rust/str0m load generator, identical media to every
      server, latency stamped in payloads, loss injection; adapters for
      LiveKit `/rtc` and str0m's chat example. Results in
      [`../benchmarks`](../benchmarks/README.md)
- [ ] Adapters for mediasoup, Galène, Janus, WHIP and WHEP
- [x] WHIP (`/whip/v1`, as LiveKit; phase 7). WHEP not done
- [ ] Scenario A (audio N↔N) and B (1→N) against the shortlist; C (video)
      after simulcast

### 2. Rooms sharded over cores

Before video, which multiplies packet rates: a 10-person video call is
~10-15k forwards/s against one core's ~60k.

- [x] One engine thread and UDP port per shard (`NATTER_SHARDS`, default
      one per core), each owning whole rooms. A room is placed when it
      starts: shards fill to `NATTER_SHARD_FILL` (250) one at a time,
      counting new rooms as 8, then the least loaded; placement is
      reference-counted by participants so a room never splits
- [x] Signalling routes joins and resumes by room; gauges and the
      participant limit are server-wide (room-wide APIs: phase 8)
- [x] Benchmarked: 100 rooms (80k forwards/s) need more than one core;
      16 packed shards 176% / 270 MiB vs LiveKit 242% / 2.3 GiB, no loss
- [ ] Later, if needed: one big room's fan-out over several threads
      (LiveKit can spread a room; one shard can't)

### 3. Video, single layer (in progress)

- [x] VP8, H.264 (LiveKit's profiles), VP9, AV1, H.265, RTX; LiveKit's
      codec table, payload types, fmtp and feedback per direction
      ([`media.md`](media.md) A). The publisher's `add_track` codec is put
      first in our answer, as LiveKit, so that is what the client sends.
      H.264 is matched by profile as libwebrtc classifies it.
- [x] Header extensions per direction as LiveKit (A.4); TWCC on video
      only (vendor patch); Dependency Descriptor passed through unchanged
      (Chrome's AV1 receiver needs it; rewriting it is phase 6)
- [x] PLI/FIR relay, one PLI upstream per 500 ms per layer (B.4);
      keyframe on subscribe; LiveKit's key-frame seeder (a PLI a second
      for up to 30 s until the first keyframe)
- [x] Camera, screen share, screen share audio sources;
      `canPublishSources` (screen share not yet browser-tested)
- [x] Video NACK/RTX both ways with 500-packet buffers, through str0m
      (its own NACK policy, not LiveKit's 5 tries / 3 resends)
- [x] Video under loss measured (`natter-load --video`, VP8 1.5 Mbit/s):
      nothing lost through natter at 5% each way, 1–2% of frames
      undecodable at 10% (LiveKit: 0.8–1.7%, 11%); see
      [`benchmarks`](../benchmarks/README.md#video-under-packet-loss)
- [ ] Our NACKs to publishers are str0m's: 5 tries ~33 ms apart, ~165 ms
      in all. Chrome resends a packet at most once per RTT, so above
      ~150 ms RTT a lost resend is never asked for again. libwebrtc's
      receiver retries 10 times, spaced by RTT (`set_nack_retries` is
      there; spacing isn't)
- [x] Until simulcast (phase 4): each subscriber follows the publisher's
      highest live layer, switching only at a keyframe with continuous
      sequence numbers and timestamps; VP9/AV1 SVC streams are forwarded
      whole

Verified with LiveKit's JS SDK in Chrome (single PC: VP8, H.264, VP9,
AV1, with and without simulcast, 1280×720 decoded both ways) and its Go
SDK through `lk load-test` (dual PC: VP8 and H.264 simulcast, no loss).
H.265 is negotiated but untested: Chrome on Linux doesn't encode it.

### 4. Simulcast, dynacast, adaptive stream (in progress)

- [x] Receive rid simulcast. A track's layers are its `add_track` layers
      sorted low to high (`q`, `h`, `f`), in `TrackInfo.layers`. Each
      subscriber follows the highest live layer its settings allow (else
      the lowest live one), switching up or down only at a keyframe of the
      new layer, with a PLI to it meanwhile (C.1–C.2). Temporal layers are
      all forwarded
- [ ] Stream trackers as LiveKit's (C.3): a layer counts as live while a
      packet came within 1 s; LiveKit's packet tracker takes ~2 s (layer 0)
      or ~10 s (layers 1–2) of steady packets to re-activate one
- [x] `update_track_settings`: `disabled`, `quality`, `width`/`height`
      (smallest layer ≥ 90% of the size, heights or portrait widths),
      kept for tracks subscribed later; adaptive-stream clients start at
      the lowest layer. Not yet: `fps` (temporal layers), LiveKit's 100 ms
      debounce
- [x] Dynacast: `subscribed_quality_update` with the highest quality
      anyone receives, none when nobody does; higher at once, lower after
      5 s (`dynacast_pause_delay`); not while muted, re-sent on unmute (C.4)
- [x] Publisher mute stops forwarding; on unmute subscribers restart at a
      keyframe. Subscriber `disabled` likewise (C.7)
- [ ] `stream_state_update` (PAUSED/ACTIVE): LiveKit sends it when
      congestion control pauses a track, so it comes with phase 5

Verified with LiveKit's JS SDK 2.23 in Chrome (single PC, VP8 and H.264
simulcast, adaptive stream and dynacast): a subscriber's 320×180 video
gets `q`, 640×360 `h`, 1280×720 `f`; 5 s after it shrinks, the publisher
stops encoding the layers above; hidden, it stops all three, and shown
again 720p is back within a second. LiveKit 1.13.10 did the same in the
same script. `lk load-test` (dual PC, speaker layout) gets HIGH for one
track and LOW for the other, no loss.

### 5. Congestion control

- [x] Bandwidth estimate per subscriber: TWCC send-side (str0m's GCC,
      natter's default), or the client's REMB (LiveKit's default, no
      probing), or off (`NATTER_CONGESTION_CONTROL`)
- [x] Allocation layer by layer, screen shares first (B.1); up one layer
      at a time, 2 s apart, with a 20% margin on the step; down after
      300 ms, or at once when the estimate collapses
- [x] Probing for the next step only (usage + 120% of it, ≥ 200 kbit/s)
- [x] Pausing when nothing fits, `stream_state_update` PAUSED/ACTIVE,
      `NATTER_ALLOW_PAUSE` / `subscriber_allow_pause`
- [x] Audio budgeted at its measured rate
- [x] Measured against LiveKit (`natter-load --simulcast --down-kbps`, see
      benchmarks): as clean or cleaner and lower latency on thin and
      shrinking links, a third of the CPU
- [x] Top layers about as often as LiveKit across loopback, 60 ms and
      150 ms paths (87–99% `f` at 4 Mbit/s against 100%), nothing lost on
      steady links
- [x] Own congestion detector on TWCC feedback (`cc_detect`: queueing
      delay, loss, delivered rate; vendored `Event::TwccFeedback`): when
      the link shrinks, a third of LiveKit's frame loss (3–4% vs 9–10%),
      undecodable frames well under, lower p99
- [ ] 1 Mbit/s on a 150 ms path: p99 ~285 ms against LiveKit's 196
      (natter runs one track on `h`, LiveKit stays on `q`)
- [ ] Unmanaged (single-layer) screen shares exempt, as LiveKit; temporal
      layers; per-track priority (unused by LiveKit 1.13.10 too)
- [ ] Not browser-tested under a real bottleneck (Chrome's network
      throttling doesn't touch WebRTC; would need tc/netem)

### 6. SVC and codec fallback

- [x] Dependency descriptor (VP9/AV1 SVC): parsed (`dd.rs`, AV1 RTP
      spec Appendix A; the structure kept per stream), each subscriber
      given the highest active decode target within its limits, frames
      outside it dropped with sequence numbers kept contiguous, the
      marker set at the end of its top spatial layer. Moves to another
      target at a keyframe, at a switch point whose references all reached
      the subscriber, or down at once when the lower target is nested in
      the current one (not so across `L3T3_KEY`'s spatial layers).
      Verified with LiveKit's JS SDK in Chrome, VP9 and AV1 `L3T3_KEY`:
      320×180 / 640×360 / 1280×720 elements get those sizes, no freezes,
      same as LiveKit in the same script. Congestion control allocates
      SVC by cumulative spatial-layer rates
- [ ] DD rewritten as LiveKit's `DDForwarder` (active decode targets);
      passed through unchanged for now, which Chrome accepts
- [ ] Temporal layers by `fps` (SVC and VP8)
- [x] Backup codecs and codec regression (C.6): `TrackInfo.codecs` lists
      the client's backup; a subscriber with no payload type for the
      primary takes the backup, which dynacast asks the publisher for (per
      codec `subscribed_codecs`); the client's second `add_track` (same
      sid) binds as the track's backup stream; under PREFER_REGRESSION in
      rooms under 5 (`codec_regression_threshold`) everyone moves to the
      backup and the primary is turned off, else both flow. Verified in
      Chrome (AV1 publisher, a subscriber without AV1): VP8 at every size;
      with 5 in the room AV1 to the others, VP8 to it; same as LiveKit
- [ ] `subscribed_audio_codec_update` (audio with more than one codec)

### 7. Transport and resilience

- [x] Resume completed: `sync_state` applied as LiveKit's `onSyncState`
      (every publication must be known, published or pending, else
      `leave` RECONNECT/STATE_MISMATCH and a full reconnect; then its
      disabled tracks and subscriptions), also from `/rtc/v1`'s join
      request; reliable data replay (`ReconnectResponse.last_message_seq`
      the last reliable sequence relayed; resent or out-of-order packets
      dropped). Verified with the JS SDK: a disabled track stays disabled
      over a resume, 300 reliable messages over two resumes arrive once
      each and in order, an unknown publication forces a full reconnect
      after which video flows again
- [x] ICE/TCP: a passive TCP candidate per engine (`NATTER_TCP_PORT`,
      default the UDP port; LiveKit 7881), RFC 4571 framing on a thread
      per engine. Verified in Chrome with UDP disabled: tcp/tcp both
      ways, simulcast switching and congestion control with no loss, as
      LiveKit over TCP
- [x] Embedded TURN: UDP and TCP on `NATTER_TURN_PORT`, TLS natively
      (`--features tls`) or terminated in front; REST-API credentials in
      the join and reconnect responses' `ice_servers`; relays only to
      natter's engines. Verified in Chrome with `iceTransportPolicy:
      relay` over TURN/UDP, TURN/TCP (UDP disabled) and TURN/TLS (a real
      certificate, only `turns:` kept): every size and hide/show, no loss
- [x] UDP→TCP→TURN/TLS fallback (LiveKit's `allow_tcp_fallback`): short
      (< 90 s) or repeated (2 in a minute) failures, from the server's
      ICE or the client's `reconnect_reason`; TCP by SDP candidate
      filtering, TLS by `turns:`-only servers and `force_relay`; the TCP
      ICE connect timer; remembered 5 min per room and identity. Unlike
      LiveKit, a move holds through the resume that reports it (LiveKit
      answers and restarts ICE with the stale preference, and falls back
      to any transport when a UDP failure is reported after the move).
      Not done: the UDP-unstable fallback (off by default in LiveKit)
- [x] Native WHIP `/whip/v1` (POST/GET/OPTIONS, PATCH trickle and ICE
      restart, DELETE; one-shot participants: tracks synthesized from the
      offer, closed on transport failure, layer sizes from keyframes).
      Not done: `X-LiveKit-ClientInfo`'s `subscribedParticipantTrackNames`
      (LiveKit's non-standard subscribe-on-create) and `X-Livekit-Ingress`

### 8. Server APIs and the rest of the room model

- [x] Twirp RoomService (every RPC LiveKit's OSS server implements;
      Forward/Move answer `unimplemented`, as the OSS server). Verified
      with `lk` 2.19 (protobuf) and JSON against LiveKit 1.13.10 in the
      same script: the same results, room events and error codes. Rooms
      now hold their placement while they exist, so an admin's empty room
      stays put; tracks a client unpublishes (inactive m-line) or an admin
      revokes are unpublished; single-PC participants turn ACTIVE
- [x] Webhooks (signed `application/webhook+json`), every room,
      participant and track event the OSS server sends (egress/ingress
      have no source here); per-resource queues, retries and drops as
      LiveKit's `ResourceURLNotifier`. Verified with the server SDK's
      `WebhookReceiver` against LiveKit 1.13.10: identical event
      sequences. Not sent: `room_finished` on server shutdown
- [x] Subscription permissions: a publisher's `SubscriptionPermission`
      (everyone, or per identity/sid: all tracks or listed ones) applies
      to auto-subscribe, explicit subscriptions and existing ones;
      refused subscriptions stay wanted and start once allowed;
      recorders and egress skip the check; `subscription_permission_update`
      as LiveKit (allowed at subscribe, refused/revoked on change).
      Verified with the JS SDK 2.23, single and dual PC, against LiveKit
      1.13.10: the same subscriptions, media and events at each step
      (scenario: audio-only grant, all, one other identity, unsubscribe
      then regrant, recorder). Not done: re-checking when the admin API
      changes a subscriber's `recorder` grant
- [x] Hidden, recorder and agent participants, and kinds from the token,
      as LiveKit: hidden ones are seen by nobody (data from them carries
      no identity) and never make `TrackSubscribed`, which a publisher
      gets once per track; recorders set `active_recording`; agents and
      recorders (and kinds AGENT/EGRESS) are dependent: not counted in
      `num_participants` or `max_participants`, and a room with only them
      left closes when idle (ROOM_CLOSED). Room counts go out at most
      every 5 s (`roomUpdateInterval`), recording changes at once.
      Verified with the JS SDK 2.23 against LiveKit 1.13.10 (publisher,
      subscriber, hidden publisher, hidden recorder, agent; data; leave
      down to dependents). LiveKit only recounts when something marks the
      room dirty, so its counts can lag natter's
- [x] Twirp JSON as Twirp's Go server writes it: proto field names, every
      field present, null for unset messages (prost-reflect over protox
      descriptors of the vendored protos). `ParticipantInfo` has
      `client_protocol`/`capabilities`; codecs have LiveKit's
      `video_layer_mode`; layers record SSRC and RTX SSRC. Compared key by
      key against LiveKit 1.13.10
- [x] Metadata/attributes updates (client and admin), participant
      permissions from the admin API
- [x] Data relay as LiveKit's `onDataMessage`: destinations from the
      packet or its `UserPacket` (and deprecated sids), nonce dedupe,
      transcriptions only from agents, agents speaking for others marked
      generated. RPC, text and byte streams verified through it
- [x] Data tracks: publish/unpublish with LiveKit's validation, explicit
      or `auto_subscribe_data_track` subscriptions under subscription
      permissions, `DataTrackSubscriberHandles`, `_data_track` forwarding
      with handle rewrite and LiveKit's 100 ms buffered-latency drop
- [x] Packet trailers (stripped for subscribers without
      `CAP_PACKET_TRAILER`), room SIF trailer, E2EE pass-through (audio,
      video, data, text with the SDK's worker; wrong key fails). Data
      blobs off, answered as LiveKit with them disabled (its default)
- [x] Reliable data for joiners: 2 s room cache replayed when the
      reliable channel opens (`replayJoiningReliableMessages`)
- [x] Single PC: free receive slots answered `inactive` without msid
      (Chrome no longer fires `track` twice per slot)
- [x] Agent dispatch and the `/agent` worker protocol (`agent.rs`,
      `engine/agents.rs`): workers register for room, publisher or
      participant jobs under an agent name, namespace and deployment
      (JSON or protobuf, pings, load/status/draining, simulated jobs,
      `migrate_job` ignored); a job goes to a random available worker
      weighted by load left, the next one if it declines or goes away;
      10 s to answer; the agent gets an hour's token with the worker's
      permissions, kind `agent` and `lk.agent_name`. Rooms dispatch from
      CreateRoom's or the token's `agents`, else one unnamed dispatch:
      room jobs at room start, participant jobs on join, publisher jobs on
      first publish (agents and recorders get none). An agent leaving
      ends its job (FAILED, "agent worker left the room"); deleting a
      dispatch terminates its jobs and removes agents still there after
      3 s. AgentDispatchService Create/Delete/List over Twirp (Create
      makes the room; a missing room reads "no response from servers",
      as LiveKit's psrpc). Verified with livekit-agents 1.8.8 and the
      Python server SDK against LiveKit 1.13.10 (`bench/e2e/agents/`),
      plus the worker protocol spoken by hand: the same jobs, offers,
      tokens, listings and errors. Not done: agent HTTP endpoints
      (`/agents/…`, which need LiveKit's WebTransport data plane; over the
      WebSocket LiveKit ignores them too), the WebTransport `/agent`,
      `agents.target_load` affinity (multi-node only), metadata and
      attribute size limits (none in natter's APIs yet)

### 9. Scale and operations

- [x] LiveKit config file shape (`config.rs`): `--config`,
      `--config-body`/`LIVEKIT_CONFIG`, LiveKit's base flags and their
      variables, generated flags/variables for the keys natter reads,
      `--dev` (devkey/secret, loopback, UDP 7882), strict mode against
      LiveKit 1.13.10's schema (`--disable-strict-config`), `generate-keys`
      and `ports`. Acted on: ports and binds, keys/key_file, region (in
      `ServerInfo`), node_ip, use_external_ip (STUN), stun_servers and
      turn_servers (static or shared-secret credentials, as LiveKit's
      `iceServersForParticipant`), room defaults and `auto_create` (404 on
      join, dispatch without creating), remote unmute, embedded TURN
      (udp_port, TLS on tls_port advertised as 443, external_tls as plain
      TCP there), webhooks, Prometheus port with basic auth, log level,
      congestion control on/off and allow_pause. Everything else LiveKit
      has is accepted and listed in one warning. Verified against LiveKit
      1.13.10 with the same files: `ports`, strict errors, `/rtc/validate`
      with auto_create off, room defaults over Twirp, dispatch to a missing
      room, Prometheus auth, JoinResponse ICE servers; natter started with
      LiveKit's dev command passes the agent protocol scenario unchanged.
      `NATTER_*` variables still override, and without a LiveKit config
      natter's own defaults apply
- [x] Prometheus metrics under LiveKit's names (`livekit/prom.rs`), with
      its labels, buckets and constant labels (one node id per process,
      now also every shard's `ServerInfo.node_id`), recorded where LiveKit
      records them: rooms and their durations, participants, published and
      subscribed tracks, publish/subscribe counters and times
      (`pubsubtime_ms`), join stages, join latency, session start time and
      duration, peer connection states, service operations, signal
      messages, Twirp status and latency, packets and bytes (initial and
      retransmit, from str0m's stream stats; resends via a vendored str0m
      patch), NACK/PLI/FIR, loss, reordering, jitter and RTT per stream,
      connection quality; plus `process_*`. Compared with LiveKit 1.13.10
      under the same `lk load-test`: the same series, and values that agree
      where both servers did the same thing. Not there: `forward_latency`
      (natter doesn't measure it), `node_packet_total` (host-wide qdisc
      counters), `webhook_queue_length`, `psrpc_*` (multi-node), `go_*`,
      and incoming-stream RTT (str0m has none for what it receives)
- [x] Multi-node with Redis (`livekit/cluster.rs`): LiveKit's model, a
      room on one node. Nodes register in Redis with their stats every
      2 s (`natter:nodes`); a node not heard from in 5 s, or draining,
      gets no rooms (LiveKit's `IsAvailable`). Rooms are placed by
      LiveKit's selectors (any/cpuload/sysload/regionaware, every
      `sort_by`, lowest/twochoice) and recorded compare-and-set
      (`natter:room_node_map`); a room on a dead node is placed afresh on
      the next join. Any node takes `/rtc`, `/rtc/v1` (and resumes),
      validation, Twirp (room-scoped calls passed to the room's node,
      ListRooms from every live node) and WHIP (sessions recorded so
      PATCH/DELETE reach their node). Agent workers on any node take jobs
      for rooms on any node: nodes publish their workers to Redis, a job
      goes to the node with the most affinity (LiveKit's target load 0.7),
      over an internal endpoint with a node token, and terminations follow
      it. `list-nodes`; SIGTERM drains and leaves. Checked with two nodes
      and LiveKit's own two-node cluster on the same Redis: `lk load-test`
      into one room through both nodes, the livekit-agents scenario with
      workers on the other node (same output as one node, and as LiveKit
      multi-node), failover after `kill -9`, graceful stop
      (`bench/cluster/`). Not done: Redis TLS, Sentinel and Cluster; a
      draining node waiting for its rooms to end (it leaves at once)

## Deliberate differences

- Room counts are exact when sent. LiveKit recounts only when something
  marks the room dirty (a join, a leave), so its `num_publishers` can
  trail a publish, and the last update before a room closes can be stale.
  Both send at most every 5 s.
- `Room.turn_password` is empty. LiveKit fills it with a random secret it
  never reads.
- `enabled_codecs` has no PCMU/PCMA: natter doesn't forward them.
- Data-track packets are dropped by the same rule and constants as
  LiveKit's (100 ms at the drain rate, 8 KiB floor), but the drain rate
  needs 100 ms of backlog to exist. On an idle, fast link pion's SCTP
  accrues that sooner than sctp-proto under natter, so a large burst
  (60 KB frame) can get through to a fresh subscriber where LiveKit
  drops it.
- Big-room speaker selection (the old engine forwarded only the 3 loudest
  when more than 6 spoke) has no LiveKit equivalent; LiveKit clients
  expect every subscribed track. Not carried into the new engine; could
  return as an opt-in room setting.
- Audio NACK is negotiated only when the remote offers it (LiveKit echoes
  it regardless). See `vendor/README.md`.
- No STUN server is handed to clients unless `rtc.stun_servers` (or
  TURN) is configured. LiveKit then hands out Google's; natter's clients
  reach it directly, and would only tell Google their address.
- Media ports: natter runs one engine per port, so a `udp_port` range
  (or the default 50000-60000) becomes one port per core from its start,
  and ICE/TCP takes as many ports from `tcp_port`. A single `udp_port` is
  one engine, as LiveKit's one port.
- natter's embedded TURN also serves TCP on `turn.udp_port`, and
  advertises it.
- `livekit_track_subscribed_total` falls back as subscribers leave; in
  LiveKit 1.13.10 it stayed up after a load test's participants had all
  gone (no unsubscribe recorded when a participant leaves).
- Multi-node: nodes pass requests to each other over HTTP (LiveKit uses
  psrpc over Redis pub/sub), so they need to reach each other's HTTP port.
  ListRooms lists the rooms of live nodes only; LiveKit's comes from its
  Redis room store and still listed a killed node's rooms. Placing a room
  is compare-and-set, where LiveKit's `HSET` lets two racing nodes both
  think they have it. Other nodes see a node's agent workers within about
  0.5 s of a change, where LiveKit invalidates its cache at once.
- A dispatch lists a job as soon as a worker takes it. LiveKit records it
  up to ~5 s later (seen when a worker declined first, or on a room's
  first dispatch), so its ListDispatch can briefly show no jobs.
- When natter removes an agent or deletes its room, livekit-agents ends
  the job ("room disconnected"). Against LiveKit 1.13.10 the same jobs
  kept running until the worker shut down: the leave messages are the
  same, but LiveKit closes the data channels as it sends them, and the
  agent's room then never reports the disconnect (plain SDK clients see
  it on both servers).
