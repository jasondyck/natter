# Open-source SFUs to benchmark against

Researched 2026-10-17 (GitHub API, Docker Hub, project docs; "unverified"
marks recollection). Goal: compare natter with more than LiveKit, using
identical clients where possible.

## Status (2026-10-17)

- `natter-load` (`crates/load`) exists with two adapters: LiveKit `/rtc`
  (dual PC, as `lk`; drives natter and LiveKit) and str0m's chat example
  (`bench/servers/str0m-chat`, patched for rooms). Loss injection works
  both ways. `bench/compare.sh` runs servers interleaved. Results are in
  the README's "Load testing".
- Next adapters, in order: mediasoup (protoo), Galène (websocket), Janus
  (HTTP API), then WHIP/WHEP for scenario B (MediaMTX, live777, Broadcast
  Box), with natter gaining WHIP/WHEP.

## Candidates

| Server | Lang / licence | Latest | Docker | Multi-core | N↔N rooms? |
|---|---|---|---|---|---|
| LiveKit (reference) | Go/Pion, Apache-2 | 1.13.10 | livekit/livekit-server | all cores | yes |
| mediasoup (+ mediasoup-demo) | C++ worker + Node, ISC | 3.30.1, 2026-10-17 | none official | one single-threaded worker per core, rooms pinned to a worker | yes |
| Janus VideoRoom | C, GPL-3 | v1.4.3, 2026-10-12 | community only | threads per handle, `event_loops` | yes (multistream, one subscriber PC) |
| Jitsi Videobridge | Kotlin/JVM, Apache-2 | stable-11248, 2026-09 | jitsi/jvb | multi-threaded; needs Jicofo + Prosody | yes |
| Galène | Go/Pion v4, MIT | 1.2.1, 2026-09-08 | none official | goroutines, UDP mux | yes |
| MediaMTX | Go/Pion, MIT | v1.21.2, 2026-10-08 | bluenviron/mediamtx | goroutines, single UDP port | no, 1→N per path |
| OvenMediaEngine | C++, AGPL-3 | v0.21.0, 2026-08-13 | airensoft/ovenmediaengine | multi-threaded | no, 1→N |
| Broadcast Box | Go/Pion, MIT | v2.0.2 | seaduboi/broadcast-box | goroutines | no, 1→N |
| live777 | Rust/webrtc-rs, MPL-2 | v0.10.2, 2026-09-28 | ghcr live777-server | tokio | no, 1→N |
| webrtc-rs/sfu | Rust, MIT/Apache | v0.5.0, 2026-09-04 | none | example-grade | yes |
| str0m chat example | Rust, MIT | str0m 0.24.1 | none | one thread | yes |
| PulseBeam | Rust (str0m fork), AGPL | v0.4.9 | unverified | tokio | yes |

Skipped: ion-sfu (dead since 2023), Medooze (repos gone), Kurento (inactive,
GStreamer pipeline not comparable), atm0s (alpha), SaaS.

## Driving load

- LiveKit: own protocol (`lk`), native WHIP ingest at `/whip/v1`, no WHEP.
- MediaMTX: `/<path>/whip`, `/<path>/whep`. Broadcast Box: `/api/whip`,
  `/api/whep`. live777: WHIP/WHEP native (`whipinto`, `whepfrom`).
- Galène: WHIP ingest only; subscribers speak its websocket protocol
  (`jech/gclient` in Go).
- Janus: no WHIP in core; Meetecho's `simple-whip-server`/`simple-whep-server`
  shims (signalling only), or its JSON HTTP API.
- mediasoup: no WHIP; the demo's broadcaster is plain RTP, not DTLS-SRTP,
  so not an equivalent client. Needs protoo signalling.
- Jitsi: only Selenium-driven Chrome (jitsi-meet-torture); too heavy.

**Method:** one Rust load generator on str0m (`natter-load`) sending
identical fake Opus (later video) everywhere, with pluggable signalling:
WHIP, WHEP, LiveKit, Janus HTTP, Galène websocket, mediasoup protoo. natter
gains WHIP/WHEP (LiveKit already serves `/whip/v1`).

## Fairness

- PC topology: WHEP is one PC per track (a 4×4 room = 16 subscriber PCs)
  against one subscriber PC for LiveKit/natter/Janus multistream. Compare
  like with like; record PCs and DTLS sessions per run.
- One UDP mux port everywhere, host networking, no TURN/STUN, a single
  127.0.0.1 candidate.
- Force one SRTP profile from the client; crypto backends differ.
- Features equal: strip or enable RED everywhere (it doubles audio
  payload), NACK/TWCC alike, no transcoding/recording/MCU modes.
- Warn-level logging; same stats scraping (or none).
- JVM warm-up ≥ 60s; Go GOMAXPROCS default, note GC CPU.
- CPU two ways: the whole cgroup (all helper processes) and the media
  process alone. Pin server and load generator to separate cpusets.
- Multi-core: give mediasoup N workers; str0m chat is one core by design.

## Shortlist and matrix

Shortlist: mediasoup, Janus VideoRoom, Galène, MediaMTX, live777, str0m
chat (the library as its example uses it: one thread, frame mode, a scan of
every client per packet; not str0m's ceiling); Jitsi as a stretch.

| Scenario | Servers | Client |
|---|---|---|
| A. Audio N↔N rooms (4×4) at 6.4k/24k/48k fwd/s, then to saturation | LiveKit, natter, mediasoup, Janus, Galène, str0m chat | `natter-load` adapters (`lk` for LiveKit/natter, cross-checked) |
| B. Audio 1→N fan-out, N = 10/100/500 | natter (WHEP), MediaMTX, live777, Broadcast Box, Janus (shims), str0m chat; LiveKit via WHIP + `lk` | `natter-load` WHIP + WHEP |
| C. Video 1→N, 3-layer simulcast (after phase 3) | LiveKit, natter, mediasoup, Janus, Galène, Broadcast Box | same generator |

Metrics: CPU-µs per forwarded packet and cores at each rate, RSS (steady
and per PC), loss and jitter, p50/p99 one-way latency (send time stamped in
the payload; loopback shares a clock), setup time per PC, saturation point
(rate where loss passes 1%).
