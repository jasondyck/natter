# Vendored crates

## str0m (upstream main, after 0.24.1)

From [algesten/str0m](https://github.com/algesten/str0m) main at `dc317173`
(unreleased: still versioned 0.24.1) with the patches below, wired in with
`[patch.crates-io]` in the workspace `Cargo.toml`. The patches live as one
commit each on a branch of a str0m clone, rebased onto upstream; this
directory is that branch packaged with `cargo package` (`str0m`, `is`,
`str0m-rust-crypto`, `str0m-proto`). After extracting a new package, touch
its files: `tar` keeps the packaged mtimes and cargo would reuse the old
build.

Upstreamed since 0.24.1, so no longer patched here: a negotiated RED listed
before its primary (#1117), SDP parsed without `easy` errors first (#1118),
TWCC reports finding their first record by binary search (#1116), and
rejected m-lines keeping a `0` format.

### Patch: audio NACK without RTX

Upstream only does NACK through RTX: a stream with no RTX payload type never
sends NACKs, and a packet sent on one is never resent. Browsers never use RTX
for audio. When `nack` is negotiated for Opus they resend the lost packet on
the original stream, with its original sequence number, and expect the same
of us. LiveKit does this for audio, so natter needs it to compare like for
like (see the root README's load-testing section).

- `PayloadParams::resends_in_place`: audio, `fb_nack`, no RTX.
- Receive: such streams are not NACK-suppressed (`streams/mod.rs`, and
  `change/sdp.rs` for SSRCs announced in SDP).
- Send: such packets stay nackable and are cached; a resend is the cached
  packet unchanged, without the RTX original-sequence-number prefix
  (`streams/send.rs`).
- `OpusPacketizer::nackable` is now `true`; the send path still decides per
  payload type whether a packet can be resent.

RED packets stay unnackable, as upstream and as LiveKit: RED carries its own
redundancy. Upstream has an open PR for the same (#980), to be reworked after
it separates "RTX" from "resend" in its naming.

Audio `nack` is now intersected with the remote's, like `transport-cc`
(`PayloadParams::update_param`). Upstream echoes it in answers whatever the
offer said; Chrome never offers it for Opus, so we'd cache packets for NACKs
that never come.

Not filed upstream (see #980). Drop this copy once str0m resends audio in place.

### Patch: `Rtc::set_msid`

A public setter for a media line's `a=msid`. In LiveKit's single-PC mode
the client offers receive-only m-lines and the server, as answerer, starts
sending a track on one of them; the answer's msid (`PA_…|TR_…`) is how the
client learns whose track it is. Upstream only sets msid when it creates
the line.

### Patch: ICE upkeep only when due

`Rtc::handle_input(Timeout)` called the ICE agent's `handle_timeout` every
time, and that re-evaluates every candidate pair. An SFU feeds a timeout
per forwarded packet (the pacer needs one to release it), so ICE upkeep
ran per packet. It now runs only when the agent's `poll_timeout` is due.
Measured with identical `lk` load, interleaved: 10% less CPU at 30 rooms
of 4+4 (41.1% → 36.9% of a core).

### Patch: the answerer starts the DTLS handshake

Answering an `a=setup:actpass` offer, str0m took `passive`, so the
client started the handshake as soon as *its* ICE check succeeded. The
server's own check of that client often hadn't yet, and `Rtc::accepts`
drops non-STUN packets from a remote it hasn't validated, so the
ClientHello was lost and the handshake waited for DTLS's ~1s retransmit:
a quarter of publishers took over a second to connect. The answerer now
takes `active`, as RFC 5763 §5 recommends and browsers, Pion and LiveKit
do, and sends its ClientHello once its own pair is up. Setup p99 at 8
rooms of 4+4 (`natter-load`, interleaved): 1.24 s → 0.38 s.

### Patch: TWCC on video only

str0m negotiated the transport-wide congestion control extension on audio
m-lines too. LiveKit offers it for video only (`pkg/rtc/config.go`), and
recording every audio packet for TWCC feedback costs CPU per packet, so
`Extension::is_audio` no longer includes it (`rtp/ext.rs`).

### Patch: VP8 PictureIDs

The VP8 packetizer (frame mode: natter-load's publishers, not the server,
which forwards in RTP mode) wrote no PictureID. libwebrtc always sends
one, and LiveKit's VP8 munger rewrites it, so the packetizer now starts
at a random 15-bit PictureID and counts up (`Vp8Packetizer::with_picture_id`
in `packet/vp8.rs`, used in `packet/mod.rs`).

### Patch: `RtcConfig::set_nack_retries`

A missing packet is NACKed at most 5 times, about 33 ms apart, so a
receiver gives up after ~165 ms. libwebrtc tries 10 times. LiveKit resends
a packet at most once per 100 ms when it has no RTT (always, on
loopback), so if its one resend was lost too, str0m's later NACKs fall
inside that window and the packet is never recovered: natter-load saw 7%
of video frames lost through LiveKit at 5% loss, against 0.04% with 10
tries. The limit is now per `Rtc` (`config.rs`, carried through
`streams/mod.rs` and `receive.rs` into `register_nack.rs`); the default
stays 5. natter-load sets 10.

Upstream now spaces retries by the receive stream's RTT plus 5 ms (100 ms
before an RTT is known, #1070), still at most 5. On a short round trip
that is 5 tries in a few tens of ms, inside LiveKit's 100 ms resend window,
so the per-`Rtc` limit stays, applied on top of upstream's spacing.

### Patch: resends limited per packet

Upstream resends a packet for every NACK of it. A receiver that NACKs again
before the resend could arrive (libwebrtc and str0m retry every 20–33 ms on
a short round trip) then gets a copy per NACK, and on a congested link those
copies cause more loss and more NACKs. A packet is now resent at most 3
times, at least clamp(2 × RTT, 20 ms, 100 ms) apart (100 ms before an RTT is
known), as LiveKit's sequencer allows (`streams/send.rs`, `handle_nack`;
`SendStats::rtt` in `streams/send_stats.rs`).

### Patch: pacing at 2.5 × the estimate

With bandwidth estimation on, the leaky-bucket pacer released media at 1.1 ×
the estimate. Whenever the estimate dipped below what was being forwarded
(a keyframe's queueing delay, a probe), media queued inside str0m and left
later in a burst, which overflowed the link and dipped the estimate again:
natter-load saw 600 and 100 kbit/s in alternate seconds on a 500 kbit/s
link carrying 320 kbit/s of video, with 1.5 s latency. libwebrtc paces video
at 2.5 × its target (`kDefaultPaceMultiplier`), so the pacer smooths bursts
without holding media below its rate (`pacer/control.rs`). Same link
afterwards: steady 321 kbit/s, nothing lost, p99 13 ms.

### Patch: no estimator work without TWCC-capable media

With bandwidth estimation enabled, every timeout ran the estimator and asked
every send queue whether it could carry padding (an extension-map scan
each), and media went through a leaky-bucket pacer, even on a peer
connection with nothing to estimate with: natter only negotiates TWCC on
video, so an audio-only subscriber has none. That cost 12% more CPU at 30
audio rooms. Now a session checks at most once a second whether any media
can probe (`bwe_capable`); until one can, the estimator and its timers are
skipped, and the pacer stays the null pacer, switching to the leaky bucket
once video arrives. Audio-only overhead is down to 4–7% (`session.rs`).

### Patch: congestion signals on `StreamTx`; ALR probes every 3 s

`StreamTx::nacks_received` and `StreamTx::rtt` expose the NACK count and the
receiver-report round trip, which natter's allocator uses to tell real
congestion (loss, queueing) from a dip in GCC's estimate (`streams/send.rs`,
`streams/send_stats.rs`). Periodic probes in the application-limited region
come every 3 s instead of 5, as LiveKit's probe regulator
(`bwe/probe/control.rs`, `MIN_TIME_BETWEEN_ALR_PROBES`).

### Patch: `StreamTx::reception`

`StreamTx::reception` gives how many receiver reports came for the stream
and the latest one's fraction lost (`streams/send.rs`,
`streams/send_stats.rs`), for natter's UDP-unstable fallback
(`livekit::fallback::Unstable`), which counts reports as LiveKit's media
loss proxy does.

### Patch: `Event::TwccFeedback`

With `RtcConfig::enable_twcc_feedback_events`, every TWCC feedback report on
what we sent is also handed to the application as the packets it covered:
when each was sent, when it arrived (on the remote's clock, offset
unknown) or that it was lost, and its size (`bwe::TwccPacket`;
`session.rs`, after `apply_report`). natter's congestion detector
(`livekit::cc_detect`) reads queueing, loss and delivered rate from it.

### Patch: resends in `MediaEgressStats`

`packets_resent` and `bytes_resent`: of what a stream sent, what was a
resend. str0m counted them (`StreamTxStats`) but didn't hand them out;
natter's LiveKit metrics split `livekit_packet_total` into initial and
retransmit by them (`streams/send_stats.rs`, `stats.rs`).

### Test fix: `remap_exts_audio`

Its expectations followed upstream's TWCC on audio; updated for the
video-only patch above.

### Patch: `RtcConfig::set_ice_ufrag_prefix`

natter serves ICE/TCP for all its engines on one port, as LiveKit does, and
gives a new connection to the engine its first STUN request's USERNAME
names. Each engine's `Rtc`s start their local ufrags with its UDP port
(four hex digits). Upstream can only set the first credentials
(`set_local_ice_credentials`); an ICE restart, ours or the remote's, made
fresh random ones. Now the prefix is kept on the `Rtc` and every local
ufrag it generates starts with it (`lib.rs` `new_ice_creds`, the two
restart paths in `change/sdp.rs`).

### Patch: SCTP not walked when nothing changed

Every `Rtc::poll_output` that got past media ended in `RtcSctp::poll`,
which walks the association (retransmit and control queues, every stream's
reassembly) whether or not anything happened: ~7% of an engine's time
forwarding audio, with data channels that carried nothing. `RtcSctp` now
remembers that its last walk found nothing (`idle`) and skips the next
until something can change it: input, a due association timer, endpoint
events, or any call that writes, opens, closes or configures. A stream
whose open is being retried keeps it walking. 100 audio rooms: 13.3 →
12.0 µs of CPU per forwarded packet (−10%); the data-channel e2e
scenarios (data, RPC, streams, replay, data tracks, resume) unchanged.

Upstream's readiness gate (#1011) skips SCTP too, but only when nothing at
all changed since the last `poll_output`. An SFU writes media between
almost every poll, so the gate rarely holds and this flag still does the
work (without it, SCTP was ~5.5% of an engine forwarding audio).

### Patch: send-only timeouts (`Rtc::handle_send_timeout`)

Ported onto upstream's readiness gate (#1011): the fast path arms the
readiness after stamping writes (transmits to poll, the pacer's deadline
moved), and `non_send_at` is recorded whenever `poll_output` works the
timeout out again. The gate and the NullPacer drain (#1112) do not cover
this: without it, audio at 100 rooms cost 12.7 µs of CPU per forwarded
packet against 8.6 µs.

A written packet only reaches the pacer through a timeout: the stream
timestamps it and the pacer takes a fresh look at the queues there. And
after every send the no-op pacer (audio) asks for a timeout at once to
look again. So an SFU fed a full `handle_input(Input::Timeout)` about twice
per forwarded packet, each running every subsystem's timers (ICE, SCTP,
every stream's reports, NACK, TWCC, the estimator) with nothing due, plus
a `poll_output` that recomputed every deadline to say so: 4.6 calls of
`poll_output` per forwarded packet.

- `Session::poll_timeout` also keeps the soonest deadline other than
  sending's; `Rtc::poll_output` adds ICE, DTLS, SCTP, channels and stats
  to it (`non_send_at`; any input clears it).
- `Rtc::handle_send_timeout(now)`: when nothing but sending is due (and no
  probe runs, no keyframe or REMB request waits, no estimator re-check is
  due, no stream has yet to have its first timeout), it only timestamps
  writes and updates the pacer (`Session::handle_send_timeout`); else it
  is `handle_input(Input::Timeout(now))`.
- `poll_output` with nothing to send, when only sending is due, does the
  same itself and looks again instead of returning a timeout.

natter calls `handle_send_timeout(now)` before polling a transport it wrote
to. Audio, 100 rooms: 2.7 `poll_output` calls per forwarded packet and full
timeouts from ~2 per forward to almost none; 11.7 → 10.6 µs of CPU per
forward. Video: 33.9 → 31.5 µs, latency p99 3.8 → 1.5 ms.

### Patch: "can probe" worked out per timeout, not per poll

`Session::poll_event`, called on every `Rtc::poll_output`, decided whether
the estimator could probe by walking every m-line's extension map and
codec list (`probe_media`): most of `ExtensionMap::id_of`'s share of an
engine. It is now worked out once per full `handle_timeout` and kept
(`can_probe`); it only changes with renegotiation, which a timeout always
follows. Audio, 100 rooms: 10.8 → 10.4 µs of CPU per forward; the
congestion grid as before.

### Patch: pacer bursts without a timeout per packet

The leaky-bucket pacer let one packet out per `handle_timeout`: after
each, `poll_queue` stayed shut until another timeout, which snapshots
every stream's queue (`update_queue_state`) and picks again. Inside a
burst (debt below the burst interval) and with nothing queued between
two polls, that pick is the same queue until it is empty. So
`handle_timeout` now also hands `poll_queue` the rest of the chosen
queue's media packets (`Burst`; `QueueState::media_packets` counts
queued and resent packets, not padding), which it lets out while the
debt still leaves room, as `next_poll` would. Not while probing, not
for padding; any timeout starts over. Video, 15 rooms: −16%
instructions, 32.1 → 28.2 µs of CPU per forward, latency p50 0.31 →
0.27 ms; simulcast latency, the congestion grid (its noisy 4 Mbit/s,
0 ms cell over 12 runs each) and Chrome through a thin link
(`bench/e2e/thin`) as before.

### str0m's own tests

Run in the str0m clone, these fail with the patches above and pass on
upstream main. All are expected:

- `answer_remaps` and seven `ssrc-zero-probe` tests: TWCC on video only
  (they expect it on audio, or probing from audio).
- Three `tele` (DTMF) tests: the test harness moves the clock 10 ms per
  immediate timeout and str0m packetizes one frame-mode payload per
  timeout, so 150 payloads/s queue up; TWCC's timers on audio used to add
  enough timeouts. natter forwards in RTP mode and sends no DTMF.
- `contiguous_all_the_way`, `not_contiguous`: with the `is` patches the
  last frame is still in flight at the test's single final drain (more
  drains pass).
- `loss_heavy` (103 of 104 at ~10% bursty loss): the per-packet resend
  limit.
- `initial_exponential_probes_3x_and_6x`, `minimum_fills_media_gaps…`,
  `minimum_improves_recovery…`, `bwe_cellular`: pacing at 2.5 ×, ALR probes
  every 3 s, no estimator without TWCC.

## is 0.11.1

str0m's ICE agent, from the same upstream main (`crates/is`).

### Patch: a nomination skips the queue at once

A pair's next check time is cached. Nominating a pair makes its check due
immediately (`next_binding_attempt`), but the cache wasn't cleared, so the
controlled side's confirming check waited up to the STUN RTO (250 ms)
from its previous one. Setup p99 at 8 rooms, on top of the DTLS patch:
0.38 s → 0.25–0.31 s.

### Patch: Ta paces checks, not answers

`poll_timeout` held the next timeout at least Ta (50 ms) after the last
`handle_timeout`, whatever was due. Two things waited on it for nothing:
answering a request queued until the remote credentials arrived (a
client's first check often beats its answer), and the check carrying a
nomination (`next_binding_attempt` already makes it due at once). And the
first check on a pair that became possible between two timeouts (the
answer just set the credentials) waited out the rest of the Ta measured
from that last timeout. Now a queued request with credentials and a
pending nomination are due at once, and Ta is measured from the last check
sent. Without remote credentials no checks go out, so that case still
advances by Ta. A pair leaves the nominated state when its check is sent,
so the agent can't spin. Signal connect to subscriber PC connected, `lk`
on loopback: ~104 ms → ~6.5 ms. `host_host_disconnect` failed before
these patches and still does.

## str0m-rust-crypto 0.6.0

str0m's RustCrypto backend, from the same upstream main
(`crypto/rust-crypto`).

### Patch: certificates without rcgen

Upstream makes the self-signed DTLS certificate with dimpl's `rcgen`
feature, and rcgen builds on aws-lc: the only C in the stack. `cert.rs`
makes the same certificate (P-256 ECDSA with SHA-256, `O=DTLS, CN=DTLS
Peer`, a year's validity, a random positive 128-bit serial) with p256 0.14
and x509-cert 0.2 (0.3, which the rest of upstream's workspace uses, made
the certificate's fields private); its tests check it is self-signed and
that dimpl accepts it.
natter generates one per process (`rtc_config::certificate`), as mediasoup
and Janus do, instead of one per peer connection.

### Patch: AES-GCM in place

The SRTP AES-GCM ciphers (128 and 256) used `Aead::encrypt`/`decrypt`,
which return a new `Vec` per packet that was then copied into str0m's
output buffer. `srtp.rs` now uses `AeadInOut::{encrypt,decrypt}_inout_detached`
from str0m's input straight into its output, the tag written after. Same
bytes (a test checks it against `Aead` for several lengths, split AAD and a
changed byte); 2.7% fewer instructions per forwarded video packet.

### Patch: one dimpl config per MTU

dimpl's `ConfigBuilder::build` self-tests the crypto provider (an ECDH
exchange and signature checks on P-256 and P-384), and `new_dtls` built a
config per `Rtc`: ~4.2 ms per peer connection with RustCrypto, most of a
join. The config holds no per-connection state and dimpl takes it as an
`Arc`, so one is built per MTU and shared. Building a peer connection:
4.2 ms → 0.11 ms; 2 publishers and 150 subscribers joining at 50/s: 24%
less CPU. Tests still get a seeded config of their own.
