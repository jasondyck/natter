# Vendored crates

## str0m 0.24.1

From crates.io, wired in with `[patch.crates-io]` in the workspace
`Cargo.toml`. The first commit touching `vendor/str0m` is the crate as
published (minus its tests and examples); everything after it is ours.

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
redundancy.

Audio `nack` is now intersected with the remote's, like `transport-cc`
(`PayloadParams::update_param`). Upstream echoes it in answers whatever the
offer said; Chrome never offers it for Opus, so we'd cache packets for NACKs
that never come.

### Patch: RED listed before its primary

A browser sends with the first codec the other side lists. Upstream always
lists the primary (`111 63`), so Chrome sent plain Opus even when it
preferred RED. A negotiated RED now comes first (`63 111`), as LiveKit
answers (`change/sdp.rs`). RED is only kept when both sides offer it, so
peers without RED are unaffected.

Not filed upstream. Drop this copy once str0m resends audio in place.

### Patch: `Rtc::set_msid`

A public setter for a media line's `a=msid`. In LiveKit's single-PC mode
the client offers receive-only m-lines and the server, as answerer, starts
sending a track on one of them; the answer's msid (`PA_…|TR_…`) is how the
client learns whose track it is. Upstream only sets msid when it creates
the line.

### Patch: rejected m-lines keep a format

An m-line with no codec in common (say video, offered by a client before
natter forwards video) was written as `m=video 0 UDP/TLS/RTP/SAVPF ` with
an empty format list, which Chrome rejects for the whole answer. It now
lists `0`, as RFC 8866 requires at least one format.

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

### Patch: SDP parsed without `easy` errors first

`Sdp::parse` used `combine`'s `easy_parse`, which allocates, merges and
dedups an error for every alternative it tries: most of the cost of
parsing a valid SDP, and a join parses at least one. It now parses with
plain `&str` errors, which cost nothing, and only parses again with
`easy_parse` for the message when the input is invalid. A 13 KB Chrome
offer (audio, simulcast video, data channel, 72 candidates): 2.2 ms →
0.34 ms; a small Firefox offer 152 → 25 µs. The parsed result is the same
(compared by serialising both). `natter-server`'s ignored `parse_timing`
test measures it.

## is 0.11.1

str0m's ICE agent, vendored the same way (first commit as published).

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

str0m's RustCrypto backend, vendored the same way (first commit as
published).

### Patch: certificates without rcgen

Upstream makes the self-signed DTLS certificate with dimpl's `rcgen`
feature, and rcgen builds on aws-lc: the only C in the stack. `cert.rs`
makes the same certificate (P-256 ECDSA with SHA-256, `O=DTLS, CN=DTLS
Peer`, a year's validity, a random positive 128-bit serial) with p256 0.14
and x509-cert; its tests check it is self-signed and that dimpl accepts it.
natter generates one per process (`rtc_config::certificate`), as mediasoup
and Janus do, instead of one per peer connection.

### Patch: one dimpl config per MTU

dimpl's `ConfigBuilder::build` self-tests the crypto provider (an ECDH
exchange and signature checks on P-256 and P-384), and `new_dtls` built a
config per `Rtc`: ~4.2 ms per peer connection with RustCrypto, most of a
join. The config holds no per-connection state and dimpl takes it as an
`Arc`, so one is built per MTU and shared. Building a peer connection:
4.2 ms → 0.11 ms; 2 publishers and 150 subscribers joining at 50/s: 24%
less CPU. Tests still get a seeded config of their own.
