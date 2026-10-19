# LiveKit SFU v1.13.10: media, RTP and WebRTC feature inventory

**Sources.** Everything here comes from the code itself:
- `github.com/livekit/livekit` at tag **v1.13.10**, cloned to `src/livekit`.
- The dependencies pinned in its `go.mod`:
  - `livekit/protocol` @ `9bb708cdb819` (`src/protocol`)
  - `livekit/mediatransportutil` @ `f234b534b095` (`src/mtu`)
  - pion/webrtc v4.2.18 is replaced by **`livekit/webrtc-pion/v4 v4.2.18-warp.6`**, and pion/ice v4.4.0 by **`livekit/ice/v4 v4.4.0-warp.3`**. These are LiveKit forks; the forks themselves were not read.

Paths are relative to the livekit repo unless they start with `protocol/` or `mtu/`. Line numbers refer to those trees. Anything I could not confirm from source is marked **UNVERIFIED**. One docs page was checked (docs.livekit.io self-hosting/ports-firewall); where docs and code disagree, it is called out.

---

## A. Codecs, codec negotiation, RTCP feedback, RTP header extensions

### A.1 Codec table (`protocol/codecs/codecs.go`)

**Enabled codecs.** The server enables `room.enabled_codecs`, a list of `{mime, fmtp_line}`.
- Default list (`pkg/config/config.go:602-613`): `audio/PCMU, audio/PCMA, audio/opus, audio/red, video/VP8, video/H264, video/VP9, video/AV1, video/H265, video/rtx`.
- Registration happens in `registerCodecs` (`pkg/rtc/mediaengine.go:33-112`).

| Codec | PT | Clock / ch | fmtp registered by server | Registered when |
|---|---|---|---|---|
| Opus | 111 | 48000/2 | `minptime=10;useinbandfec=1` (no `stereo`/`usedtx`/`sprop-stereo` in the engine; those are munged per track, see D.4) | `audio/opus` enabled |
| RED | 63 | 48000/2 | `111/111` | `audio/red` enabled **and** Opus enabled |
| PCMU | 0 | 8000 | none | enabled |
| PCMA | 8 | 8000 | none | enabled |
| VP8 | 96 | 90000 | none | enabled |
| VP9 profile 0 | 98 | 90000 | `profile-id=0` | enabled (non-strict fmtp) |
| VP9 profile 1 | 100 | 90000 | `profile-id=1` | enabled (non-strict fmtp) |
| H.264 CBP 42e01f pm0 | 125 | 90000 | `level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42e01f` | enabled (non-strict) |
| H.264 CBP 42e01f pm1 | 108 | 90000 | `…packetization-mode=1;profile-level-id=42e01f` | enabled (non-strict) |
| H.264 High 640032 pm1 | 123 | 90000 | `…packetization-mode=1;profile-level-id=640032` | enabled (non-strict); **filtered out on server-offerer PCs** (A.3) |
| H.264 42001f pm0/pm1 | 104/102 | 90000 | `…profile-level-id=42001f` | **strict fmtp**: only when an `enabled_codecs` entry carries exactly this fmtp. Not in the default config |
| H.264 Main 4d001f pm0/pm1 | 39/106 | 90000 | `…profile-level-id=4d001f` | strict fmtp, same as above. Not in the default config |
| AV1 | 35 | 90000 | none | enabled |
| H.265 | 116 | 90000 | none | enabled |
| RTX | codecPT+1 | 90000 | `apt=<codecPT>` | one RTX entry per registered video codec, if `video/rtx` is enabled (`mediaengine.go:101-110`) |
| FlexFEC-03 | 115 | 90000 | `repair-window=10000000` | **Defined but never registered.** It is not in `VideoCodecsParameters` and `registerCodecs` never adds it. Only `selectAlternativeVideoCodec` skips it by name |

**Matching rules** (`isCodecEnabledWithFmtp`, `mediaengine.go:140-150`):
- A non-strict codec matches on mime when the config fmtp is empty, or on case-insensitive equal fmtp.
- A strict codec needs the fmtp to match exactly.
- `ToWebrtcCodecParameters` (`protocol/codecs/codecs.go:225-270`) maps a `livekit.Codec` to these parameter sets. A non-standard fmtp is kept, with a warning.

**Per-client disables.** Client configuration (`pkg/clientconfiguration/conf.go`) disables some codecs per client (see C.6), for example AV1 on Safari and on Firefox below 157.

### A.2 Codec fallback, backup codec and regression

These are summarised here; the detail is in C.6.
- **Unsupported publish codec.** If the requested codec is not enabled, `selectAlternativeVideoCodec` and `selectAlternativeAudioCodec` pick the first enabled codec of that kind, falling back to VP8 or Opus (`mediaengine.go:151-173`).
- **Simulcast codecs / backup codec.** `AddTrackRequest.simulcast_codecs` becomes `TrackInfo.codecs[]`, with the primary first and the backup second. Dynacast asks the publisher to encode the backup only when some subscriber negotiated only the backup.
- **Backup policy.** `BackupCodecPolicy` is `PREFER_REGRESSION` (default), `SIMULCAST` or `REGRESSION`. Codec regression moves all subscribers to the backup codec and turns the primary off. It happens:
  - when the policy is REGRESSION, or
  - when the policy is PREFER_REGRESSION and the room has fewer than `video.codec_regression_threshold` (default **5**) participants.

  Regression is one-way.

### A.3 RTCP feedback per direction (`pkg/rtc/config.go:107-209`)

| PC / direction | Audio | Video |
|---|---|---|
| Publisher PC, dual-PC mode (`getPublisherConfig(false)`) | `nack` | `transport-cc`, `ccm fir`, `nack`, `nack pli` |
| Publisher PC, single-PC mode (`consolidated=true`; it also carries downstream media) | `nack` | `transport-cc`, **`goog-remb`**, `ccm fir`, `nack`, `nack pli` |
| Subscriber PC, default (remote BWE) | `nack` | `ccm fir`, `nack`, `nack pli`, **`goog-remb`** |
| Subscriber PC with `use_send_side_bwe` or `use_send_side_bwe_interceptor` | `nack` | `ccm fir`, `nack`, `nack pli`, **`transport-cc`** |

Notes:
- Feedback is applied per codec in `registerCodecs` and `filterCodecs` (`mediaengine.go:206-245`). RTX entries carry no feedback.
- Opus `nack` is stripped from subscriber transceivers when RED is used, or when the client lacks audio RED support (Firefox, Safari) (D.2).
- The server never generates REMB toward publishers. It answers publisher TWCC with its own responder (D.3).

### A.4 RTP header extensions per direction (`pkg/rtc/config.go`)

URIs: `sdp.*` constants are from pion/sdp; `dd.ExtensionURI` is in `pkg/sfu/rtpextension/dependencydescriptor/`; `act.AbsCaptureTimeURI` is in `pkg/sfu/rtpextension/abscapturetime/abscapturetime.go:26`.

| Extension (URI) | Publisher audio | Publisher video | Subscriber audio | Subscriber video |
|---|---|---|---|---|
| `urn:ietf:params:rtp-hdrext:sdes:mid` | yes | yes | no | no |
| `urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id` (rid) | yes | yes | no | no |
| `urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id` | no | yes | no | no |
| `urn:ietf:params:rtp-hdrext:ssrc-audio-level` (RFC 6464) | yes | – | **no** (speaker info goes over signalling) | – |
| `http://www.webrtc.org/experiments/rtp-hdrext/abs-capture-time` | yes | yes | yes | yes |
| `http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01` (TWCC) | no | yes | no | only when send-side BWE is on |
| `http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time` | no | only in single-PC mode | no | yes by default (REMB mode); not with send-side BWE |
| `urn:ietf:params:rtp-hdrext:framemarking` | no | yes (registered only; **no reader found** under `pkg/sfu` — UNVERIFIED that it is used) | no | no |
| Dependency Descriptor `https://aomediacodec.github.io/av1-rtp-spec/#dependency-descriptor-rtp-header-extension` | no | yes | no | yes |
| `http://www.webrtc.org/experiments/rtp-hdrext/playout-delay` | no | yes, if `room.playout_delay.enabled` | no | yes, if enabled (`transport.go:342-346`). The server writes it on subscriber video only (C.9) |
| video-layers-allocation00, color-space, video-orientation (urn:3gpp), video-content-type, video-timing, inband-cn | **not registered** (none of these appear anywhere in the tree) | | | |

Notes:
- **mid/rid on the subscriber side:** the server is the subscriber-PC offerer and signals SSRCs in SDP, so mid and rid are not needed there.
- **Dependency Descriptor:**
  - The publisher's DD is parsed by `pkg/sfu/buffer/dependencydescriptorparser.go`.
  - On the subscriber side, DD is rewritten per subscriber by the DD forwarder and selector (`pkg/sfu/videolayerselector/dependencydescriptor.go`).
  - The reader and writer are in `pkg/sfu/rtpextension/dependencydescriptor/`.
- **abs-capture-time:** passed through to subscribers. The sequencer stores it per packet, so retransmissions replay it (`pkg/sfu/sequencer.go:67`).
- **TWCC on publisher video:** wires the per-participant `twcc.Responder` into the buffer (`setTWCCForVideo`, `transport.go`). The TWCC extension on publisher RTX streams is honoured even without its own feedback attribute.
## B. Congestion control, connection quality, active speaker, PLI throttle (v1.13.10)

Paths are relative to `livekit/` at tag v1.13.10 unless prefixed. Line numbers are from that tag.

### B.1 Stream allocator / congestion control

**Where it runs.** There is one `StreamAllocator` per *send-side* PC. In dual-PC mode that is the subscriber PC. In single-PC or one-shot mode it is the publisher PC (`IsSendSide`). It is created in `pkg/rtc/transport.go:580-630`. `TransportManager.SetSubscriberAllowPause/SetSubscriberChannelCapacity` route to the right PC (`pkg/rtc/transportmanager.go:1061-1075`). Only **video** downtracks are managed. Audio is never allocated.

**Config keys** (`rtc.congestion_control.*`, `pkg/config/config.go` `CongestionControlConfig`; defaults at `DefaultConfig` ~l.560):

| key | default | meaning |
|---|---|---|
| `enabled` | **true** | If false, every track gets `AllocateOptimal` (no BWE-driven downgrade). Passed as `enabled` to `NewStreamAllocator`. |
| `allow_pause` | **false** | Lets the allocator pause video entirely when capacity is insufficient. A client can override it per participant with the join query param `subscriber_allow_pause` or `JoinRequest.ConnectionSettings.SubscriberAllowPause` (`pkg/service/rtcservice.go:262-285`, `pkg/service/roommanager.go:475-478`). |
| `use_send_side_bwe_interceptor` | false | Uses the pion `gcc` SendSideBWE interceptor (initial 1 Mbps, no-op pacer) plus the pion TWCC header-ext interceptor (`transport.go:~455-480`). It is only used when `use_send_side_bwe` is false. |
| `use_send_side_bwe` | false | Uses LiveKit's own TWCC-based `sendsidebwe` instead of the REMB-based `remotebwe` (`transport.go:584-613`). |
| `send_side_bwe_pacer` | `"no-queue"` | One of `pass-through` / `no-queue` / `leaky-bucket` (`pkg/sfu/pacer/pacer.go:39-42`). `leaky-bucket` is accepted but falls back to no-queue, because nothing drives its bitrate (`transport.go:596-610`). |
| `stream_allocator.*` | see below | `streamallocator.StreamAllocatorConfig` |
| `remote_bwe.*` | see below | `remotebwe.RemoteBWEConfig` |
| `send_side_bwe.congestion_detector.*` | see below | `sendsidebwe.SendSideBWEConfig` |

**REMB vs TWCC selection** (`pkg/rtc/config.go:175-209`):
- If `use_send_side_bwe || use_send_side_bwe_interceptor`: the subscriber video m-lines get the `transport-cc` header extension and `transport-cc` RTCP-FB.
- Otherwise (the **default**) they get `abs-send-time` and `goog-remb`. In this mode the BWE is `remotebwe` and the pacer is `PassThrough` (`transport.go:611-613`).
- When send-side BWE is used, the pacer (`pkg/sfu/pacer/base.go:95-...`) stamps abs-send-time and the transport-wide seq at send time.
- REMB handling: the allocator locks onto one SSRC's REMB (a downtrack SSRC or its RTX SSRC) to avoid duplicate per-track reports (`streamallocator.go:388-468` `OnREMB`).
- TWCC feedback goes to `BWE.HandleTWCCFeedback`, and also to the pion interceptor if that is enabled (`handleSignalFeedback`).

**StreamAllocatorConfig defaults** (`pkg/sfu/streamallocator/streamallocator.go:189-211`):

| yaml | default | notes |
|---|---|---|
| `min_channel_capacity` | 0 | Floor applied to the estimated capacity (`getAvailableChannelCapacity`, ~l.1260) |
| `disable_etimation_unmanaged_tracks` (sic) | false | If true, unmanaged tracks are not subtracted from capacity and are excluded from BWE (`IsBWEEnabled`) |
| `probe_mode` | `padding` | `padding` or `media`. Media mode = boost the deficient track one layer and reset the BWE (`maybeProbeWithMedia`) |
| `probe_overage_pct` | 120 | Probe goal = max(nextLayerDelta × 120 %, `probe_min_bps`) above expected usage (`maybeProbeWithPadding`, l.1356) |
| `probe_min_bps` | 200 000 | |
| `paused_min_wait` | 5 s | If pause is allowed, the allocator is DEFICIENT, the BWE is congested and nothing was sent for more than 5 s, the BWE is reset so probing can restart (`handleSignalPeriodicPing`, l.725) |

Hard-coded constants (l.40-58):
- Infinite capacity = 100 Mbps.
- Priority: screenshare 255, video 1. `SetTrackPriority` exists but has **no caller** in rtc/service, so these are effectively the only values.
- Overshoot flags: allowed while optimal, in probe and in boost. Not allowed while deficient or in catch-up. Allowed for exempt tracks.
- RTT pull every 30 s.
- Periodic ping: 15 s in STABLE, **100 ms** in DEFICIENT (`setState`).

**Allocation algorithm** (`streamallocator.go`):
- **States:** `STABLE` and `DEFICIENT`. The allocator is DEFICIENT if any track is below its desired layer (`adjustState`).
- **Managed vs exempt** (`track.go:112`): a single-layer *screenshare* is unmanaged ("exempt"). It always gets `AllocateOptimal` and its bitrate is subtracted first.
- **allocateTrack** (l.931) handles a per-track change:
  - If the allocator is not enabled, is STABLE and the BWE is not CONGESTED, or the track is unmanaged: `AllocateOptimal`. In EARLY_WARNING the track is "held" (no upgrade).
  - Otherwise it uses the available headroom. If that is not enough, it does a **cooperative transition**: it takes bandwidth from other tracks sorted by MinDistance (lower priority first, closest-to-desired first).
  - If not enough bandwidth can be freed and `allowPause`, it pauses the track. Without `allowPause` it commits anyway.
- **allocateAllTracks** (l.1149) runs on congestion or capacity change:
  1. Allocate exempt tracks.
  2. If remaining capacity is 0 and `allowPause`, pause all managed tracks.
  3. Otherwise allocate **layer by layer**: spatial 0..max, then temporal 0..max. For each layer, tracks are sorted by priority desc, then max layer desc (`TrackSorter`, `track.go:209-227`).
- **Boost** (`maybeBoostDeficientTracks`): after a successful probe, the allocator hands spare headroom one step at a time to the deficient track farthest from its desired layer (`MaxDistanceSorter`, priority first).
- **Stream-state signalling:** pausing for bandwidth sends `StreamStateUpdate` with state PAUSED. Muting sends INACTIVE (`updateStreamStateChange`, ~l.1440).
- **Congestion state change** (`handleSignalCongestionStateChange`): on CONGESTED, outside a probe, it commits the BWE's estimate as the channel capacity and runs `allocateAllTracks`. Leaving EARLY_WARNING into NONE while STABLE re-allocates every track optimally.

**Channel-capacity override.** `SetChannelCapacity` is only reached via `Room.SimulateScenario` `SubscriberBandwidth` (`pkg/rtc/room.go:1007`). A value > 0 replaces the estimate and disables probing. A value of 0 clears the override.

**Probing** (`pkg/sfu/ccutils/prober.go`, `probe_regulator.go`; downtrack `WriteProbePackets`, `pkg/sfu/downtrack.go:2501`):
- Probes are started only in DEFICIENT state, and only when `BWE.CanProbe()` (congestion NONE and the regulator allows it).
- Cluster mode is `Uniform` (`LinearChirp` exists). The cluster is split into buckets of `cSleepDuration` = 20 ms (min 10 ms). `cBytesPerProbe` = 1100 (`prober.go:302-310`).
- Probe bytes depend on whether RTX is negotiated:
  - **With RTX** on the downtrack, the allocator calls `WriteProbePackets(bytes, usePadding=false)` (`streamallocator/track.go:137`). This re-sends the last ~6 forwarded media packets on the **RTX** SSRC (`downtrack.go:2564-2590`). An RTX padding-only path exists, but the allocator does not use it.
  - **Without RTX**, it sends padding-only RTP (max 255 B payload each) on the primary SSRC via `WritePaddingRTP` (`downtrack.go:1292`). These are registered in the sequencer as padding, so NACKs for them are ignored.
- `ProbeRegulatorConfig` (`ccutils/probe_regulator.go:37-45`, yaml `probe_regulator.*`):
  - Interval: `base_interval` 3 s, ×`backoff_factor` 1.5 after a congesting probe, up to `max_interval` 2 min.
  - Duration: `min_duration` 200 ms, ×`duration_increase_factor` 1.5 after a good probe, up to `max_duration` 20 s.
- `ProbeClusterFinalize` returns a probe signal (Congesting / NotCongesting / Inconclusive). If the probe was not congesting, the measured capacity is committed and boosting runs.

**Remote BWE (REMB-based, default)** (`pkg/sfu/bwe/remotebwe/`):
- `DefaultRemoteBWEConfig` (`remote_bwe.go:39-46`): `nack_ratio_attenuator` 0.4, `expected_usage_threshold` 0.95.
- Channel observers combine REMB trend detection and a NACK-ratio tracker (`channel_observer.go:80-110`, `nack_tracker.go:34-44`):

  | | probe | non-probe |
  |---|---|---|
  | trend `required_samples` / min | 3 / 3 | 12 / 8 |
  | `downward_trend_threshold` | 0.0 | −0.6 |
  | `downward_trend_max_wait` | 5 s | 5 s |
  | `collapse_threshold` | 0 | 500 ms |
  | `validity_window` | 10 s | 10 s |
  | NACK window min / max | 0.5 s / 1 s | 2 s / 3 s |
  | NACK `ratio_threshold` | 0.04 | 0.08 |

- Probe controller settle wait (`probe_controller.go:61-67`): `settle_wait_num_rtt` 5, `settle_wait_min` 250 ms, `settle_wait_max` 5 s. The default RTT is 70 ms (`bwe/bwe.go`).
- Estimate on congestion (`remote_bwe.go:~196-222`):
  - Loss-caused: `expectedUsage × (1 − 0.4 × nackRatio)`.
  - Otherwise: the last REMB.
  - The result is capped at the last REMB, and is committed only if it is below 95 % of expected usage.
- Remote BWE has only NONE and CONGESTED states. It never emits EARLY_WARNING (`congestionDetectionStateMachine`).

**Send-side BWE (TWCC, opt-in)** (`pkg/sfu/bwe/sendsidebwe/`):
- The detector groups packets and classifies them by queuing region: JQR = "join queuing region" (congesting), DQR = "disjoint queuing region" (clear). Signals use propagated queuing delay and weighted loss. It emits NONE, EARLY_WARNING or CONGESTED.
- `defaultCongestionDetectorConfig` (`congestion_detector.go:478-519`):
  - `packet_group` {min_packets 30, max_window_duration 500 ms}; `packet_group_max_age` 10 s.
  - `jqr_min_delay` 50 ms, `jqr_min_trend_coefficient` 0.8, `dqr_max_delay` 20 ms.
  - `jqr_min_weighted_loss` 0.25, `dqr_max_weighted_loss` 0.1.
  - `weighted_loss` {min_duration_for_loss_validity 100 ms, base_duration 500 ms, base_pps 30, loss_penalty_factor 0.25} (`traffic_stats.go:35-40`).
  - Early-warning triggers (min groups / min duration): queuing-delay JQR 2/200 ms, queuing-delay DQR 3/300 ms, loss JQR 3/300 ms, loss DQR 4/400 ms.
  - Congested triggers: queuing-delay JQR 4/400 ms, queuing-delay DQR 5/500 ms, loss JQR 6/600 ms, loss DQR 6/600 ms (`congestion_detector.go:41-81`).
  - `congested_ctr_trend` {4/2 samples, threshold −0.5, max wait 2 s, collapse 500 ms, validity 10 s}; `congested_ctr_epsilon` 0.05; `congested_packet_group` {20 pkts, 150 ms}.
  - `estimaton_window_duration` (sic) 1 s.
  - `probe_packet_group` {16384 pkts, 1 min; settle 5 RTT, 250 ms to 5 s}.
  - `probe_signal` {min_bytes_ratio 0.5, min_duration_ratio 0.5, jqr_min_delay 50 ms, dqr_max_delay 20 ms, jqr_min_weighted_loss 0.25, dqr_max_weighted_loss 0.1} (`congestion_detector.go:118-130`).

**Pacers** (`pkg/sfu/pacer/`):
- `pass-through`: sends inline.
- `no-queue`: a single goroutine drains a FIFO deque with no rate limiting (`no_queue.go`).
- `leaky-bucket`: interval-based with 2× max overshoot (`leaky_bucket.go:27-29`), but in practice it is not reachable from config, as noted above.

### B.2 Connection quality (`pkg/sfu/connectionquality/`)

- **Cadence:** scoring runs every `UpdateInterval` = **5 s** (`connectionstats.go:34`). `Room.connectionQualityWorker` (`pkg/rtc/room.go:1680`) also ticks every 5 s and broadcasts `ConnectionQualityUpdate` only when a participant's quality *level* changes or a participant is new. Each recipient gets its own entry plus entries for the participants it is subscribed to.
- **Participant quality** = the minimum over published-track (upstream) and subscribed-downtrack (downstream) qualities. The tie-break is the lowest score (`pkg/rtc/participant.go:1857-1900`). LOST is downgraded to POOR for clients whose protocol version lacks `SupportsConnectionQualityLost`.
- **Two scorers:**
  - Upstream (`sfu/receiver.go:132`) uses receiver-side delta stats and the last sender-report time.
  - Downstream (`sfu/downtrack.go:416`) uses the subscriber's RTCP RR. If no RR arrives for more than 30 s while streaming, it scores with an empty stat, i.e. LOST (`connectionstats.go:35,249`).
- **Inputs actually used:**
  - Both callers leave `IncludeRTT`, `IncludeJitter` and `EnableBitrateScore` at **false**. So in v1.13.10 the delay term is 0 and the bitrate score is always 100. Unverified whether any other path enables them; grep of all callers found none.
  - The effective score therefore comes from (a) packet loss and (b) layer distance.
- **Packet score** (`scorer.go:73`, a simplified E-model after PingPlotter's write-up):
  - `score = 100 − delayEffect − lossEffect`.
  - `delayEffect = effDelay/40`, or `(effDelay − 120)/10` if effDelay > 160. Here `effDelay = rttMax/2 + 2·jitterMax/1000`, and is 0 when both flags are off.
  - `lossEffect = actualLost·100/(packets+padding) × adjustedLossWeight`.
  - `actualLost = lost − missing − outOfOrder`. Upstream, out-of-order is zeroed before scoring, because upstream loss is already net of RTX repairs (`connectionstats.go:~300`).
- **Packet-loss weight** (`connectionstats.go:405-430`):

  | codec | weight | falls to GOOD / POOR at |
  |---|---|---|
  | Opus | 8 | 2.5 % / 7.5 % |
  | Opus + FEC | 8/1.5 | 3.75 % / 11.25 % |
  | RED | 4 | 5 % / 15 % |
  | RED + FEC | 4/1.5 | 7.5 % / 22.5 % |
  | video | 10 | 2 % / 6 % |

  FEC is assumed for RED or when the fmtp has `useinbandfec=1` (`receiver.go:143-146`).
- **DTX / VBR handling** (`getAdjustedPacketLossWeight`, `scorer.go:548`):
  - The scorer keeps a histogram of packets per second (2-pps bins, 250 bins).
  - It recomputes the mode at most every 2 min, after more than 10 readings. Bins below 10 pps are ignored, and counts are halved after each recompute.
  - Weight = `sqrt(min(1, pps/modePPS)) × weight`. For example, DTX at about 5 % of the packet rate gives about 22 % of the weight.
- **Layer score** = `100 − avgLayerDistance × 35`. Each missing spatial layer costs about one quality level.
  - The distance comes from the publisher stream tracker's `DistanceToDesired` (upstream, `receiver.go:252-282`) or from the downtrack's allocation distance (downstream, `downtrack.go:1690-1694`).
- **Bitrate score** (disabled by default): `100 − 20·ln(expectedBits/actualBits)`.
- **Combining:**
  - `score = min(packet, bitrate, layer)`, then EMA with factor 0.4 when rising and 0.8 when falling, floored at 30.
  - No packets in the window gives POOR (40), or LOST (20) if the last RTCP is older than the window.
- **Mute and pause behaviour:**
  - On mute the score jumps to 100, unless the current score is LOST.
  - A track must have been unmuted for more than 50 % of the window to be scored.
  - A bandwidth pause sets the score to the minimum (30).
  - A layer mute sets it to 100.
- **Thresholds** (`scorer.go:50-54`, `scoreToConnectionQuality`): EXCELLENT if score > 80, GOOD if > 40, POOR if > 20, else LOST.
- **MOS** (`scorer.go:644`): `1 + 0.035·R + 7e-6·R·(R−60)·(100−R)`, clamped to [1.0, 4.5]. This is the ITU-T G.107 R→MOS mapping. MOS is what is reported as `ConnectionQualityInfo.Score`.

### B.3 Audio level and active speaker

Config is the `audio:` YAML block, `sfu.AudioConfig` (`pkg/sfu/receiver_base.go:70-83`), which inlines `audio.AudioLevelConfig` (`pkg/sfu/audio/audiolevel.go:28-48`):

| yaml | default | meaning |
|---|---|---|
| `active_level` | **35** (−35 dBov) | A packet level ≤ 35 counts as active. RFC 6464 levels run 0–127, where 0 is loudest. |
| `min_percentile` | **40** | Active time must be ≥ 40 % of the window |
| `update_interval` | **400** ms | Observe window. Also the room speaker-broadcast period. |
| `smooth_intervals` | **2** | EMA factor = 2/(n+1) ≈ 0.667. 0 disables smoothing. |
| `active_red_encoding` | false | Server-side Opus to RED encoding toward subscribers (used in `pkg/rtc/mediatrackreceiver.go:578`) |
| `enable_loss_proxying` | false | Declared, but **no reader found** in v1.13.10 (grep). Treat as unverified or no-op. |

**Level measurement:**
- The input is the RFC 6464 `ssrc-audio-level` header extension, parsed in `pkg/sfu/buffer/buffer_base.go:977-988`. With no extension negotiated there is no level, so the server does not decode audio.
- Duration per packet is the RTP-timestamp delta / clock rate (`ObserveWithRTPTimestamp`).
- Per window: if `activeDuration ≥ min_percentile% × interval`:
  - `adjusted = loudestLevel − 20·log10(activeDuration/interval)`
  - `linear = 10^(−adjusted/20)`
  - EMA smoothing is then applied.
- If the window is not active enough, the level resets to 0.
- If no packets arrive for 2 × interval (e.g. DTX silence), the level resets to 0 (`resetIfStaleLocked`).
- `active` means smoothedLevel ≥ `10^(−35/20)` ≈ 0.0178.

**Participant level** = the max over that participant's **MICROPHONE**-source tracks that are active (`pkg/rtc/uptrackmanager.go:445-458`). Other audio sources are ignored.

**Speaker updates** (`pkg/rtc/room.go:383-407, 1641-1678`):
- Every `update_interval` (400 ms) the room collects active speakers, sorts them by level descending, and quantizes each level to a multiple of 1/8 (rounded up).
- It sends only *changes*: a new speaker, a changed quantized level, or a speaker that stopped (sent with level 0 and active=false).
- Updates go as `SpeakersChanged` to clients whose protocol version has `SupportsSpeakerChanged`. Each participant receives only speakers it is subscribed to, plus itself (`pkg/rtc/participant_signal.go:130-148`).
- On a new subscription, the current speaker state of that publisher is pushed (`room.go:1322-1345`).

### B.4 PLI/FIR throttle (`rtc.pli_throttle`)

- `sfu.DefaultPLIThrottleConfig` (`pkg/sfu/receiver_base.go:54-66`): `low_quality` **500 ms**, `mid_quality` **1 s**, `high_quality` **1 s**. These are the minimum spacing between PLIs the SFU sends upstream, per layer buffer.
- The value is chosen by simulcast spatial layer: layer 0 → low, 1 → mid, 2 → high, anything else → mid (`receiver_base.go:731-744`). The buffer's built-in default is 500 ms (`buffer_base.go:250`).
- The check is `CheckAndUpdatePli(throttle, force)`, which skips if the last PLI was less than `throttle` ago, unless forced (`pkg/sfu/rtpstats/rtpstats_base_lite.go:247-257`).
- The SFU only ever sends **PLI** upstream (`pkg/sfu/buffer/buffer.go:436`).
- Subscriber PLI **and FIR** are both mapped to a single upstream keyframe request through `sendPliOnce` (`pkg/sfu/downtrack.go:2093-2104`). The server does not generate FIR.
## C. Simulcast / SVC / Dynacast / Adaptive Stream / Codec fallback / Mute / Permissions / Playout delay

All paths are relative to `livekit/` at tag v1.13.10 unless prefixed `protocol/` (livekit/protocol @ 9bb708cdb819). Line numbers are from that tree.

### C.1 Simulcast: layers and RID mapping

| Item | Value / behaviour | Source |
|---|---|---|
| Max layers | 3 spatial (0..2), 4 temporal (0..3): `DefaultMaxLayerSpatial=2`, `DefaultMaxLayerTemporal=3` | `pkg/sfu/buffer/videolayer.go:23-24` |
| Known RID sets | `q,h,f` (quarter/half/full) and `2,1,0` | `pkg/sfu/buffer/videolayerutils.go:26-39` |
| RID normalisation | Known RIDs get sorted into low-to-high order (`NormalizeVideoLayersRid`) | `videolayerutils.go:498` |
| RID to spatial layer | Uses the `TrackInfo` layer list for that codec (`SimulcastCodecInfo.Layers`, otherwise `TrackInfo.Layers`): `layer.Rid == rid` gives `layer.SpatialLayer`. An empty RID gives layer 0. If the codec's layers have no RIDs (SVC), the result is 0. An unknown RID logs and falls back to 0 instead of returning invalid (there is a `SIMULCAST-CODEC-TODO` about VP9 publishers that send `f`) | `GetSpatialLayerForRid`, `videolayerutils.go:375-446` |
| Legacy mapping | `RidToSpatialLayer` maps q/h/f to 0/1/2 when there is no TrackInfo. When only 2 qualities are published, the third RID collapses to 1 (with a warning) | `videolayerutils.go:66-138` |
| Quality and layer | `VideoQuality` LOW/MEDIUM/HIGH is matched to a spatial layer through `TrackInfo` layers. A request above what is published clamps to the highest available | `GetSpatialLayerForVideoQuality`, `videolayerutils.go:448` |
| Layer mode | `VideoLayer_Mode`: `ONE_SPATIAL_LAYER_PER_STREAM` (simulcast), `MULTIPLE_SPATIAL_LAYERS_PER_STREAM` (SVC), `ONE_SPATIAL_LAYER_PER_STREAM_INCOMPLETE_RTCP_SR` (OBS clients; skips the SR reference-TS alignment). When unset, SVC-capable mimes (VP9/AV1) default to MULTIPLE_SPATIAL | `pkg/rtc/participant.go:3208-3218`, `pkg/sfu/forwarder.go:401-403` |
| `TrackInfo.Simulcast` | Set for compatibility when the primary codec is not SVC and has more than 1 layer | `participant.go:~3270` |

**SSRC-only / "unhandled" simulcast interceptor** (`pkg/sfu/interceptor/unhandlesimulcast.go`): for streams listed in `SimTracks` (ssrc to {mid, rid, RTX ssrc}), it injects the missing `mid`/`rid` header extensions into the first packets. It probes `simulcastProbeCount=10` non-padding packets (`:29`) and stops once a packet already carries mid+rid. Wiring: `pkg/rtc/transport.go:492`, `participant.go:3683`. **Unverified/likely dormant in OSS:** `ParticipantParams.SimTracks` is never populated by `pkg/service/roommanager.go:488` (the only `rtc.NewParticipant` call), so this path is only reachable by embedders.

### C.2 Forwarding: layer selection per codec

`Forwarder.DetermineCodec` (`pkg/sfu/forwarder.go:397-496`) picks the selector:

| Codec / mode | Layer selector (`pkg/sfu/videolayerselector/`) | Temporal handling | Codec munger (`pkg/sfu/codecmunger/`) |
|---|---|---|---|
| VP8 without DD | `Simulcast` | `temporallayerselector/vp8.go` (uses TID, `S`+`Y` layer-sync bit) | `VP8` (PictureID / TL0PICIDX / KEYIDX) |
| VP8 with DD ext negotiated | `Simulcast` | `temporallayerselector/dependencydescriptor.go` | `DDTemporal` plus a `DDForwarder` that rewrites the DD |
| H.264, H.265 | `Simulcast` | unfiltered (`Null`): every temporal layer is forwarded | `Null` |
| VP9 / AV1 in simulcast mode | `Simulcast` | DD temporal filter if DD is present, otherwise unfiltered | DDTemporal / Null |
| VP9 SVC with DD | `DependencyDescriptor` | handled by the selector | `Null` |
| VP9 SVC without DD | `VP9` (uses VP9 payload descriptor bits) | handled by the selector | `Null` |
| AV1 SVC with DD | `DependencyDescriptor` | handled by the selector | `Null` |
| AV1 SVC without DD | falls back to `Simulcast` + unfiltered ("nothing tells the temporal layers apart") | — | `Null` |

Switching rules:
- **Simulcast spatial switch** (`simulcast.go` `Select`): it switches only on a **keyframe** of a layer between the current and target layers, either up (`layer > current && <= target`) or down. Overshoot above max is "dialed back" on a keyframe. With `EnableStartAtDesiredQuality`, the initial acquisition latches only onto the target layer.
- **VP8 temporal up-switch** happens only on a keyframe or on a sync frame (`tid == current+1 && S && Y`), one layer at a time. Down-switch happens at the frame end (marker) (`temporallayerselector/vp8.go`).
- **VP9 (no DD)** (`vp9.go:40-110`): needs a keyframe to start. Temporal up needs `U && B`; spatial up needs `!P && B` (start of a non-inter-predicted layer frame); down-switches happen at `E` (end of frame). The marker bit is synthesised at the end of the selected top spatial layer.
- **DD selector** (`dependencydescriptor.go`): it chooses the highest active decode target `<= target` whose chains are intact. It drops frames that are not present in that decode target or that reference dropped frames, and it invalidates and resets on a keyframe/structure mismatch.
- **Timestamp/sequence continuity across a switch:** `RTPMunger` (`pkg/sfu/rtpmunger.go`) keeps `snOffset`/`tsOffset` with a range map for out-of-order and dropped packets. On a source switch, `processSourceSwitch` (`forwarder.go:1909`) computes a new TS offset from the publisher RTCP Sender Reports of the reference and new layers (it errors out and waits until an SR exists for the layer). Thresholds: `ResumeBehindThresholdSeconds=0.2`, `ResumeBehindHighThresholdSeconds=2.0`, `LayerSwitchBehindThresholdSeconds=0.05`, `SwitchAheadThresholdSeconds=0.025` (`forwarder.go:38-41`).
- **VP8 munging** (`codecmunger/vp8.go`): it offsets PictureID (15-bit), TL0PICIDX and KEYIDX (5-bit) so the values stay contiguous across switches. PictureIDs of dropped temporal-layer frames are compacted out (`pictureIdOffset += 1`, `:264`), and there is a missing-PictureID cache for out-of-order packets.
- **Keyframe (PLI) requests on switch** (`pkg/sfu/downtrack.go:948-994`): a dedicated `keyFrameRequester` goroutine calls `forwarder.CheckSync()`. If the forwarder is not locked to the target layer, it sends a PLI for that layer (`Receiver().SendPLI(layer, false)`, which is subject to the buffer PLI throttle). It re-arms every `clamp(2*RTT, 200ms, 1000ms)` (`keyFrameIntervalMin/Max`, `downtrack.go:64-65`) until locked. It is triggered on target change, mute/unmute, bind, and similar events.
- **Initial acquisition grace** is 1s (`initialLayerAcquisitionGrace`, `forwarder.go:50`). Only when start-at-desired-quality is enabled does the forwarder wait up to 1s for the requested layer before falling back to the highest layer seen. `ParticipantParams.EnableStartAtDesiredQuality` is **not set by the OSS roommanager**, so it is off by default (unverified whether any client/config path sets it).

### C.3 Stream trackers (layer availability)

Config key: `video.stream_tracker_manager.{video,screenshare}` (`pkg/config/config.go` `VideoConfig.StreamTrackerManager`; types in `pkg/sfu/streamtrackermanager.go:57-107`).

| Setting | Video default | Screenshare default |
|---|---|---|
| `stream_tracker_type` | `packet` | `packet` |
| `bitrate_report_interval` (per layer 0/1/2) | 1s | 4s |
| `packet_tracker` layer 0 | `samples_required:1, cycles_required:4, cycle_duration:500ms` | `1, 1, 2s` |
| `packet_tracker` layers 1,2 | `5, 20, 500ms` | `1, 1, 2s` |
| `frame_tracker.min_fps` (only if type=`frame`) | 5.0 | 0.5 |

Sources: `pkg/sfu/streamtracker/streamtracker_packet.go:31-63`, `streamtracker_frame.go:38-62`.

Semantics (`streamtracker_packet.go` `Observe`/`CheckStatus`): the first packet marks a layer **active** immediately. A single cycle with fewer than `samples_required` packets marks it **stopped**. Re-activation needs `cycles_required` consecutive good cycles, which with the defaults means about 2s for layer 0 and about 10s for layers 1 and 2. For SVC with DD, a single `StreamTrackerDependencyDescriptor` drives per-layer trackers (`streamtrackermanager.go:215`).

Trackers are paused while the publisher track is muted (`streamtrackermanager.go:375-384`). When dynacast raises the expected max layer, the trackers for the newly expected layers are `Reset()` (`SetMaxExpectedSpatialLayer`, `:386-415`). Available-layer changes feed `OnAvailableLayersChanged` into the stream allocator.

### C.4 Dynacast

Code: `pkg/rtc/dynacast/*`, created per track in `pkg/rtc/mediatrack.go:154-168`.

1. **Per-subscriber input.** `SubscribedTrack.OnMaxSubscribedLayerChanged` takes the downtrack's `GetMaxSubscribedSpatial()` (`forwarder.go:777`). This is `max(max layer, current layer)`, or InvalidLayer (OFF) when subscriber-muted. It is held at the reference layer until an SR arrives. When a codec is first negotiated, HIGH is assumed for it (`subscribedtrack.go:456-482`).
2. **Aggregation per codec mime** (`dynacastqualityvideo.go`): the maximum across local subscribers and across remote nodes (`NotifySubscriberNodeMaxQuality`). After `Start`/`Restart`, a forced update fires after `initialQualityUpdateWait = 10s` (`:29`).
3. **Commit and debounce** (`dynacastmanagervideo.go` `update`): upgrades, and adding or removing a mime, are sent immediately. **Downgrade-only changes are debounced by `video.dynacast_pause_delay`** (default **5s**, `config.go` DefaultConfig `Video.DynacastPauseDelay`). With a delay of 0 there is no debounce.
4. **Publisher signal.** It sends `SubscribedQualityUpdate{track_sid, subscribed_qualities (first codec, legacy), subscribed_codecs[]{codec, qualities[]{quality, enabled}}}`. `enabled = q <= maxQuality`; OFF disables all three (`dynacastmanagervideo.go` `enqueueSubscribedQualityChange`; `participant.go:2901-2953`). The codec name is normalised to lower-case without the `video/` prefix. The update is **not sent while the publisher track is muted** (`mediatrack.go:674-680`). On unmute, `ForceUpdate()` re-sends (`mediatrack.go:636-645`). After a publisher SDP answer, the committed qualities are re-sent with a 2s delay (max 8s), because applying the answer re-enables encodings (`participant.go:77-78,1287-1303`).
5. **Server-side effect.** The committed quality also drives `receiver.SetMaxExpectedSpatialLayer`, which tells the stream trackers which layers are expected to stop or start (`mediatrack.go:684-697`).
6. **Audio dynacast.** It exists only when an audio track has more than 1 codec (`mediatrack.go:162`). It sends `SubscribedAudioCodecUpdate{subscribed_audio_codecs[]{codec, enabled}}` (`dynacastmanageraudio.go`, `participant.go:2955-2981`).
7. **Disable.** `ParticipantParams.DisableDynacast` exists (`participant.go:218`) but is never set in OSS, so the server always sends these updates. Whether the client acts on them depends on the client SDK's `dynacast` option (client side, not verified here).

### C.5 Adaptive stream (subscriber-requested settings)

`UpdateTrackSettings` (`protocol/protobufs/livekit_rtc.proto:339-358`) has these fields: `track_sids, disabled, quality (deprecated), width, height, fps, priority`. It is handled by `SubscribedTrack.UpdateSubscriberSettings`/`applySettings` (`pkg/rtc/subscribedtrack.go:317-393`):
- **Debounce:** 100ms (`subscriptionDebounceInterval`, `:40`). A transition to visible (`disabled` true to false) is applied immediately.
- **`disabled`:** calls `DownTrack.Mute(true)` (subscriber mute; the forwarder drops packets and the dynacast max becomes OFF). If the downtrack is currently paused by congestion, the mute is ignored so probing can still resume (`forwarder.go:603-640`).
- **`width/height`:** goes through `GetQualityForDimension` (`pkg/rtc/mediatrackreceiver.go:1057-1126`). It compares heights (or width for portrait) against the layer heights from `TrackInfo` (or measured `VideoSizes` from the media) and picks the smallest layer `>= requested*0.9` (`layerSelectionTolerance=0.9`, `:42`). The fallback ladder is `{180, 360, orig}`.
- **`fps`:** goes through `GetTemporalLayerForSpatialFps`, which picks the lowest temporal layer whose measured fps is `>= fps*0.9` (`mediatrackreceiver.go:1220-1234`).
- **Defaults on bind** (`subscribedtrack.go:200-230`): when the client joined with `adaptive_stream`, the initial setting is LOW (and any stored `disabled` flag is cleared on re-bind). Otherwise it is HIGH; with start-at-desired-quality it is also HIGH.
- **`priority`: NOT wired in v1.13.10.** `AddTrackToStreamAllocator` never passes `Priority` (`pkg/rtc/transport.go:2076-2090`). The allocator therefore uses source defaults: screenshare = 255 (`cPriorityMax`), camera/video = 1 (`cPriorityMin`) (`pkg/sfu/streamallocator/streamallocator.go:43-46`, `track.go:86-102`). The proto comment's promise about per-track priority is unimplemented on this path.

### C.6 Codec negotiation, backup codec, codec regression

- **Enabled codec list:** `room.enabled_codecs` (default PCMU, PCMA, opus, red, VP8, H264, VP9, AV1, H265, rtx; `config.go` DefaultConfig). It is copied into the room at creation (`pkg/service/roomallocator.go:188-197`). `video/rtx` is always appended if missing (`roommanager.go:480-485`). The same list is used for publish and subscribe (`roommanager.go:504-505`). There is no per-request override in `CreateRoomRequest` (none found).
- **Per-client disabling** comes from `ClientConfiguration.DisabledCodecs{codecs, publish}` (`participant.go:4254-4306`). Static rules in `pkg/clientconfiguration/conf.go:23-82`:
  - Safari: AV1 disabled.
  - Safari > 18.3: VP9 publish disabled.
  - Firefox < 157: AV1 disabled in both directions (FF 155/156 decode only the AV1 SVC base layer).
  - Firefox Android and Xiaomi 2201117TI Android: H.264 publish disabled.
- **Publish codec ordering:** VP8 first, then H.264, then other video, then audio (Flutter workaround).
- **Fallback when a requested codec is not enabled:** `selectAlternativeVideoCodec` returns the first enabled non-RTX/FEC video codec, or `video/VP8`. `selectAlternativeAudioCodec` returns the first enabled audio codec, or `audio/opus` (`pkg/rtc/mediaengine.go:151-173`; applied in `participant.go:3198-3237`).
- **Simulcast codecs:** `AddTrackRequest.simulcast_codecs[]{codec, cid, layers, video_layer_mode}` becomes `TrackInfo.codecs[]` (deduped by mime). Layers are per codec, falling back to `req.layers` (`participant.go:3166-3270`). Clients without simulcast codecs get one synthesised codec entry.
- **`BackupCodecPolicy`** (`protocol/protobufs/livekit_models.proto:92`): `PREFER_REGRESSION=0` (default), `SIMULCAST=1`, `REGRESSION=2`. Audio tracks are forced to SIMULCAST unless they ask for REGRESSION. `DisableCodecRegression` (a participant param, not set in OSS) also forces SIMULCAST (`participant.go:3117-3122`).
- **How the backup codec gets published:** subscriber downtracks `Bind` to the first upstream codec (TrackInfo order: primary, then backup) that the subscriber's SDP supports (`pkg/sfu/downtrack.go:451-500`, fuzzy fmtp match). If a subscriber can only take the backup codec, `OnCodecNegotiated` marks HIGH for that mime. Dynacast then includes that codec in `SubscribedQualityUpdate.subscribed_codecs`, which tells the client to start encoding it.
- **Codec regression** (`pkg/rtc/mediatrack.go:110-124,345-580`, `mediatrackreceiver.go:251-324`):
  - The regression target is `TrackInfo.codecs[1]`.
  - It is enabled when the policy is REGRESSION, or when the policy is PREFER_REGRESSION **and** `ShouldRegressCodec()` holds. That condition is `video.codec_regression_threshold == 0 || room participant count < threshold` (default **5**; `pkg/service/roommanager.go:1250-1252`).
  - When the backup-codec receiver gets its first RTP and regression is enabled, all other codec receivers are set to `ReceiverCodecStateSuspended`.
  - `HandleReceiverCodecChange` then calls `RegressTo(backup)` on the primary receiver. That moves its downtracks to the backup receiver and removes the primary from the potential codecs. Dynacast marks the primary OFF and the backup HIGH (`dynacastmanagervideo.go` `OnRegressCodec`). Stats and max-layer callbacks switch to the regressed codec.
  - Downtracks whose client lacks `SupportsCodecChange` are closed so they renegotiate (`downtrack.go:660-672`).
  - Regression is one-way: there is no upgrade back to the primary (TODO in `mediatrackreceiver.go:252`).
  - With 5 or more participants (default), PREFER_REGRESSION behaves like SIMULCAST: both codecs keep flowing.
  - A mid-stream payload-type change on a receiver sets the codec state to Invalid; there is no mid-session fallback (`receiver_base.go:1097-1101`).
- **H.264 High Profile filtering:** `createMediaEngine(..., filterOutH264HighProfile=params.IsOfferer)` (`pkg/rtc/transport.go:356`) drops `profile-level-id=64…` on the **server-offerer PC**, which is the subscriber PC in dual-PC mode (`transportmanager.go:193`). It is not applied on the publisher PC or in single-PC mode (server is answerer, IsOfferer=false). Reason in source: some browsers mishandle High Profile in signalling. `filterCodecs`/`isH264HighProfile` is in `mediaengine.go:206-258`.

### C.7 Mute semantics

| Kind | Trigger | Server action | Source |
|---|---|---|---|
| Publisher mute | `MuteTrackRequest{sid, muted}` from the publisher, or admin `RoomService.MutePublishedTrack` | Sets `TrackInfo.Muted` and broadcasts it in participant updates. Receiver buffers and stream trackers are paused. Every subscriber downtrack gets `PubMute(true)`: the forwarder drops all packets and resyncs so sequence numbers don't jump. Dynacast updates are suppressed while muted; on unmute `ForceUpdate()` re-sends qualities | `participant.go:3378-3425`, `mediatrack.go:636-645`, `receiver_base.go:288-295`, `mediatracksubscriptions.go:78-83`, `forwarder.go:653-669,1856-1864` |
| Admin unmute | `MutePublishedTrack{muted:false}` | Rejected with `ErrRemoteUnmuteNoteEnabled` unless `room.enable_remote_unmute: true` (default false). With an admin request, the server also signals `MuteTrackRequest` to the publisher (`sendTrackMuted`) | `pkg/service/roommanager.go:875-882`, `participant.go:3379-3382` |
| Subscriber mute | `UpdateTrackSettings.disabled` or adaptive-stream invisibility | `DownTrack.Mute(true)`: packets dropped and dynacast max = OFF. Ignored while the track is paused for congestion | `subscribedtrack.go:378-384`, `downtrack.go:1402-1409` |
| On any mute (audio) | — | Injects silence for `RTPBlankFramesMuteSeconds=1.0s` (Opus silence / RED-wrapped Opus / PCMU / PCMA at 50 fps) so the decoder's comfort-noise state is clean. On close, blank frames are flushed for 0.2s (`RTPBlankFramesCloseSeconds`); VP8/H.264 blank frames at 30 fps are used for the video close flush | `downtrack.go:59-60,1417-1471,1881-1960` |
| On mute/unmute | — | Max-layer notification posted (subscriber mute only matters for dynacast), keyframe-request event posted, stream allocator notified | `downtrack.go:1417-1460` |

### C.8 Track subscription permissions

- **API:** `UpdateSubscriptionPermission{all_participants, track_permissions[]{participant_sid|participant_identity, all_tracks, track_sids[]}}` (`protocol/protobufs/livekit_rtc.proto:477-488`). Implemented in `pkg/rtc/uptrackmanager.go:153-218,297-405`.
- **Default:** `subscriberPermissions == nil`, so everyone is allowed (`hasPermissionLocked`, `:359-362`).
- `all_participants=true` resets to allow-all. Otherwise each update **fully replaces** the permissions with a per-identity allow-list. Identities not listed are denied. `all_tracks` allows all of that publisher's tracks; otherwise only the listed `track_sids` are allowed. SIDs are resolved to identities, and an identity/SID mismatch is logged.
- **Versioning:** updates carry a TimedVersion so cross-node (migration/relay) updates apply last-writer-wins.
- After every update, `maybeRevokeSubscriptions` calls `RevokeDisallowedSubscribers` on each track (`:407-420`).
- **Subscribers** receive `SubscriptionPermissionUpdate{participant_sid, track_sid, allowed}` when permission flips for a track they want (`participant_signal.go:400-410`, `subscriptionmanager.go:835,1135`). The subscription stays desired and resumes when allowed (subscription manager reconciliation; detail not traced further).

### C.9 Playout delay and stream sync

- **Config:** `room.playout_delay: {enabled, min, max}` (ms; default disabled; `config.go` `PlayoutDelayConfig`). It can also be set per room via `CreateRoomRequest.min_playout_delay/max_playout_delay`, and any non-zero value implies enabled (`pkg/service/roomallocator.go:118-124,198-202`).
- **Negotiation:** when enabled, the `http://www.webrtc.org/experiments/rtp-hdrext/playout-delay` URI is added to the **video** header extensions of the PCs (`pkg/rtc/transport.go:344-346`; `AllowPlayoutDelay` = `PlayoutDelay.Enabled`, `participant.go:2156`). The wire format is 12-bit min/max in 10ms units, with a max representable value of 40950ms. If `max == 0 && min > 0`, max defaults to 10000ms (`pkg/sfu/rtpextension/playoutdelay/playoutdelay.go:22-45`).
- **Adaptive controller** (`pkg/sfu/playoutdelay.go`), per video downtrack (`downtrack.go:427-433`):
  - Each subscriber RTCP RR updates the target: `target = jitter_ms * 10`, plus `(nack% - 60) * 2` when the NACK rate exceeds 60%. Jitter is skipped for screenshare (`downtrack.go:2128-2134`).
  - The change rate is limited to 80ms/s (`maxDelayChangePerSec`), and the target is clamped to `[min, max]`.
  - The extension is attached to outgoing packets from a change until an RR acknowledges a sequence number at or beyond the first carrying packet (`OnSeqAcked`). It is not added to RTX retransmissions.
  - Initial delay is `min`.
- **`room.sync_streams`** (or `CreateRoomRequest.sync_streams`): the subscriber's MediaStream id becomes `<publisherSID>|<track.stream>` (`PackSyncStreamID`, `pkg/rtc/utils.go:49`; separator constant not verified). Tracks of the same publisher stream (default stream derived from the source, `StreamFromTrackSource`) share a stream, so the browser lip-syncs them. It needs `ProtocolVersion.SupportsSyncStreamID()` and is disabled for Firefox (`participant.go:4160-4162`, `subscribedtrack.go:124-127`). It disables transceiver reuse (`participant.go:4173`). Playout delay is turned off for Firefox when sync_streams is on (`participant.go:2171-2175`).

### C.10 Unverified / notes
- `SimTracks` (unhandled-simulcast interceptor), `DisableDynacast`, `DisableCodecRegression`, `EnableStartAtDesiredQuality` and `ForceBackupCodecPolicySimulcast` are not set anywhere in the OSS service layer. They appear to be hooks for embedders or LiveKit Cloud (inferred).
- The PLI throttle per quality (`rtc.pli_throttle`, `receiver_base.go:54-66,730-745`) is covered in the RTCP section, not here.
## D. NACK/RTX, RTCP, RED/audio, E2EE/trailers, data channels & data tracks (livekit v1.13.10)

Paths are relative to the livekit repo root unless prefixed `mtu/` (livekit/mediatransportutil @ f234b534b095) or `protocol/` (livekit/protocol @ 9bb708cdb819). Line numbers come from the cloned tags.

### D.1 Receive side (publisher to SFU): packet buffer and NACK generation

| Item | Value | Source |
|---|---|---|
| Max packet buffer, video | `rtc.packet_buffer_size_video`, default **500** | pkg/config/config.go (DefaultConfig), pkg/rtc/config.go NewWebRTCConfig |
| Max packet buffer, audio | `rtc.packet_buffer_size_audio`, default **200** | same |
| Legacy key | `rtc.packet_buffer_size` (deprecated), default 500. When it is set, it fills whichever per-kind value is 0 | pkg/rtc/config.go |
| Initial bucket capacity | video **300**, audio **70** packets | pkg/sfu/buffer/buffer.go:36-37 |
| Bucket slot size | 1500 B (`bucket.RTPMaxPktSize`), stored with a 2-byte seq-num offset | mtu/pkg/bucket/bucket.go:26-27; buffer_base.go:363-375 |
| Bucket growth | At bind time, video grows to fit `bitrate/8/1200` pps. After that, a check runs every 1 s (`bucketCapCheckInterval=1e9`) and grows to the measured pps, capped at the per-kind max. Growth runs in a goroutine, off the forwarding path | buffer_base.go:151, 381-388, 1261-1302 |
| NACK enabled | Only if the codec's negotiated RTCP-FB includes `nack`. It is **not** enabled for an `audio/red` buffer. It is always created for OOB-sequence-number streams | buffer_base.go:394-417 |
| NACK queue params | `NackQueueParamsDefault`: max 100 tracked SNs (FIFO evicts the oldest), max **5** tries per SN, 20 ms min to 400 ms max interval between tries, backoff ×1.25, 2 min lifetime. RTT default 70 ms, EWMA α=0.75; RTT comes from `buffer.SetRTT` | mtu/pkg/nack/nack.go (consts) ; buffer_base.go:700 |
| NACK emission | Checked on **every incoming packet** (`Buffer.calc` → `getRTCPPackets`). Lost ranges are pushed from `flowState.LossStart..LossEnd`, and the queue decides what is due. Sent as a `TransportLayerNack` from the buffer's SSRC | buffer.go:447-485; buffer_base.go:948-958 |
| Receiver Report to publisher | One RR per buffer, at most every **1 s** (`rtcpReceiverReportDelta=1e9`). RR is also driven by packet arrival | buffer.go:33, 487-503 |
| Audio loss proxying | `audio.enable_loss_proxying` (default false) is meant to put the worst subscriber loss into the publisher RR. **Observation:** `Buffer.SetAudioLossProxying` has no caller in this tag, so audio RR proxy loss is always forced to 0. MediaLossProxy still feeds `SetLastFractionLostReport` | pkg/sfu/receiver_base.go:76; buffer.go:116, 492; pkg/rtc/mediatrack.go:141-149 |
| Padding-only packets | In-order padding-only packets are dropped and excluded from the SN space (`snRangeMap`) so downstream SNs stay contiguous. libwebrtc SSRC-0 probe packets are ignored | buffer_base.go:838-866; buffer.go:180-186 |
| Key-frame seeder | After bind, sends a PLI every 1 s for up to 30 s until a key frame arrives. Needed for DD structure and stream-tracker start | buffer_base.go:1530-1590 |
| RTP stream restart detection | `rtc.enable_rtp_stream_restart_detection` (default **false**). When it is on, 5 consecutive packets that are contiguous in SN with non-decreasing TS, all arriving while the stream is judged old or pre-start, trigger `restartStreamLocked("discontinuity")`. When it is off, those packets are dropped | config.go; pkg/sfu/rtpstats/rtpstats_receiver.go:44, 335-338, 962-987; buffer_base.go:810-829; pkg/service/roommanager.go:544 |
| Codec change detection | An in-order video packet with a new PT triggers `handleCodecChange` | buffer_base.go:868-871 |

**RTX from the publisher**
- The server registers RTX for every enabled video codec with `PT = codecPT+1` and `apt=codecPT`, but only if `video/rtx` is in enabled codecs, which it is by default (pkg/rtc/mediaengine.go).
- Simulcast RTX pairing: `RTXInfoExtractorFactory` installs a `StreamInfoProbe`. It inspects up to **10** packets (`rtxProbeCount`) for the mid, rid, and repaired-rid (`urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id`) extensions and pairs repair and base SSRC on (mid, rid==rsid). Migrated streams are pre-seeded from SimTracks because a migrated client no longer sends rid/rsid (pkg/sfu/interceptor/rtx.go:17, 32, 76-165; pkg/rtc/transport.go:526-541).
- Non-simulcast RTX pairing: `a=ssrc-group:FID base rtx` lines are parsed from the SDP, and only for m-sections without `rid` (transport.go:3380-3420, 1717, 2995).
- Pairing goes through `Factory.SetRTXPair` → `repairBuffer.SetPrimaryBufferForRTX(base)`. RTX packets are then unwrapped: the PT is checked, OSN is read from the first 2 payload bytes, and the result is re-marshalled with the base SSRC/PT and fed into the base buffer as `isRTX=true`. Padding-only RTX packets are skipped. A stream cannot *start* on an RTX packet (pkg/sfu/buffer/factory.go:136-149; buffer.go:190-199, 304-375; buffer_base.go:797).
- A TWCC extension on an RTX stream is always honoured, since RTX has no RTCP-FB list of its own (transport.go `setTWCCForVideo`).

### D.2 Send side (SFU to subscriber): NACK, RTX, padding/probing

| Item | Value | Source |
|---|---|---|
| Sequencer size | `MaxTrack` = `packet_buffer_size_audio` (200) or `_video` (500). It is per DownTrack, and "sparse" (padding-aware) for video | pkg/rtc/subscribedtrack.go:108-116; pkg/sfu/downtrack.go:598 |
| Retransmits per packet | `maxAck = 3` | pkg/sfu/sequencer.go:30 |
| Min re-NACK interval | `min(100 ms, 2×RTT)` per packet. RTT defaults to 70 ms and is updated from subscriber RRs | sequencer.go:28-29, 145-153, 390 |
| Too-old cut-off | Packets older than the sequencer size are ignored | sequencer.go:372-375 |
| NACK handling | `TransportLayerNack` for the DownTrack SSRC → `postRetransmitPackets` → a separate worker goroutine. `forwarder.FilterRTX` drops layers that are no longer forwarded. The payload is re-read from the receiver bucket (`ReadRTP` by layer + source SN) | downtrack.go:2154-2163, 2420-2498 |
| Stop-RTX-on-PLI | `FlagStopRTXOnPLI = true`: after the subscriber sends PLI/FIR, NACK retransmits are suppressed until the next key frame is forwarded | downtrack.go:62, 2079-2090, 1197-1198, 2455 |
| RTX vs plain retransmit | If the subscriber negotiated RTX (`payloadTypeRTX` found via apt) and an RTX SSRC exists (`SSRCRetransmission()`), retransmits go on the **RTX SSRC/PT**: 2-byte OSN + payload, with their own RTX SN counter and RTX RTPStats. Otherwise the original SN, PT, and SSRC are resent. DD, abs-capture-time, and munged codec bytes recorded at first send are replayed. Any stripped packet trailer is stripped again | downtrack.go:571-573, 2268-2350 |
| RTCP on the RTX SSRC | RR (RTX stats), REMB, TWCC handled | downtrack.go:2215-2250 |
| Probing (congestion control) | `WriteProbePackets`: if RTX is unavailable, it uses padding-only packets on the media SSRC (`WritePaddingRTP`). With RTX, it uses either RTX padding packets (255 B padding each, est. 20 B header) or **retransmissions of the last ~5 packets** on RTX. It requires abs-send-time or TWCC ext, an RR received, and an active stream. Packets are tagged with ProbeClusterId for the pacer | downtrack.go:57-58, 2501-2595 |
| Padding gating | `WritePaddingRTP` waits until the first RTCP RR for the stream. Padding-only packets are registered in the sequencer so NACKs for them are filtered out | downtrack.go:1290-1360 |
| Dropped-slot padding | When a packet is dropped out of order or after a gap, a 1-byte padding packet fills the outgoing SN slot so the receiver does not NACK it | downtrack.go:1247-1288 |
| Padding on mute | For go-sdk/pion OnTrack: padding every 100 ms for up to 5 s (`maxPaddingOnMuteDuration`, `paddingOnMuteInterval`) | downtrack.go:72-73, 2721-2745 |
| Blank frames | On mute, **1.0 s** of blank frames; on close, **0.2 s**. Opus/RED/PCMU/PCMA use silence frames at 50 fps. VP8 uses an 8×8 key frame and H.264 an SPS/PPS/IDR STAP-A, both at 30 fps. Other codecs (VP9/AV1/H.265) get none | downtrack.go:59-60, 1880-1935, 1995-2050 |
| Subscriber audio NACK | Opus NACK feedback is removed from the subscriber transceiver when RED is used, or when the client does not support audio RED (Firefox, Safari). Because pion's MediaEngine is per-PC, it stays off for the session once removed | pkg/rtc/transport.go:1130-1136, 3186-3225; pkg/rtc/clientinfo.go:53-55 |

### D.3 RTCP generation and handling

- **SR + SDES to subscribers.** `subscriberRTCPWorker` runs every **3 s**. It sends one SR per DownTrack plus SDES chunks, in batches of `sdBatchSize=30` items (pkg/rtc/participant.go:74, 2813-2872).
  - SR NTP/RTP mapping is derived from the publisher's SR ("SR pass-through", `forwarder.GetSenderReportParams`) unless `DisableSenderReportPassThrough` is set.
  - That param exists, but nothing in the OSS server sets it, so it stays false (downtrack.go:1136-1160, 1870-1877; participant.go:579).
- **Publisher SRs.** These are captured into `buffer.SetSenderReportData` and propagated to DownTracks for A/V sync (pkg/rtc/mediatrack.go:310-320; downtrack.go:2825-2840).
- **RR from subscriber.** Only reports for the DownTrack SSRC are kept. They update RTT and the sequencer RTT, feed playout-delay jitter (not for screen share), and feed receiver-report listeners for the loss proxy (downtrack.go:2111-2152).
- **XR RTT.** On the publisher PC (non-offerer), `lkinterceptor.NewRTTFromXRFactory` sends RTCP-XR RRTR. The callback is empty because RTT is read from the buffer (transport.go `newPeerConnection`).
  - On a subscriber PC, an incoming XR RRTR is answered with a DLRR (delay 0) for the DownTrack SSRC only. Comment: browsers don't send XR; server-sdk-go does (downtrack.go:2172-2199).
- **PLI/FIR from subscriber.** These are collapsed into one PLI per RTCP compound toward the publisher layer currently being forwarded, via `Receiver().SendPLI(layer,false)`. Throttling is in the receiver; PLI throttle is covered in another section (downtrack.go:2072-2104).
- **REMB / TWCC from subscriber.** Forwarded to the stream allocator listener (`OnREMB`, `OnTransportCCFeedback`) (downtrack.go:2106-2110, 2165-2170).
- **REMB toward publisher.** Not generated. The buffer logs "REMB not supported, RTCP feedback will not be generated" (buffer_base.go:396-398).
- **TWCC responder toward publisher.** There is one `twcc.Responder` per participant (participant.go:2123). It is attached to video buffers whose negotiated FB has `transport-cc` (transport.go `setTWCCForVideo`). Feedback is built every **100 ms**, or **50 ms** after a marker packet, using pion's `twcc.Recorder` with a random sender SSRC (mtu/pkg/twcc/twcc.go:25-27, 41-66).

### D.4 RED and audio codecs

| Feature | Behaviour | Source |
|---|---|---|
| RED registration | `audio/red` is registered at `PT 63`, `fmtp 111/111`, only if Opus is enabled too. RED is in default enabled codecs | protocol/codecs/codecs.go; pkg/rtc/mediaengine.go |
| Per-track RED | `IsRedEnabled(ti)`: if `ti.Codecs[0]` is set, RED is on iff the primary mime is audio/red. Otherwise it is `!ti.DisableRed` | pkg/rtc/utils.go:201-207 |
| Publisher RED preference | If RED is enabled for the track and the offer has a `red` codec whose fmtp references the Opus PT, the publisher transceiver prefers `audio/red` | pkg/rtc/participant_sdp.go:264-323 |
| Upstream Opus/RED → Opus subscriber | `RedPrimaryReceiver` extracts the primary block. It also **recovers lost packets from redundant blocks**, using an 8-packet receive-history bitmap and up to `maxRedCount=2` redundant blocks. Non-RED-PT packets are passed through | pkg/sfu/redprimaryreceiver.go:24-90, 219-260 |
| Upstream Opus → RED subscriber ("active RED") | `audio.active_red_encoding` (**default false**: absent from `DefaultAudioConfig`; config-sample shows `true` as an example). When on and the track is RED-enabled, the WrappedReceiver advertises RED first. `RedReceiver` builds RED with up to **2** redundant packets (`maxRedCount=2`). Redundant blocks are skipped if the TS delta is ≥ 2^14. A packet whose payload is ≥ 1024 B (`maxRedPayload`, the 10-bit length field) is sent as plain Opus (primary PT) instead. Output PT is hard-coded 63, inner Opus PT 111 | pkg/sfu/receiver_base.go:70-83; pkg/rtc/mediatrackreceiver.go:578; pkg/rtc/wrappedreceiver.go:58-78; pkg/sfu/redreceiver.go:36-45, 77-104, 178-217 |
| Encrypted audio | Opus↔RED translation is **disabled** for E2EE tracks ("cannot match encrypted source") | wrappedreceiver.go:114-130 |
| Downtrack PT translation | If RED is negotiated but the publisher sent the primary PT (oversized payload), it is rewritten to the subscriber's primary PT | downtrack.go:2608-2615 |
| Opus fmtp (server default) | `minptime=10;useinbandfec=1`, PT 111, 48 kHz/2ch. No `stereo`/`usedtx` in the media engine | protocol/codecs/codecs.go |
| Publisher DTX/stereo | AddTrackRequest `stereo` → `TF_STEREO`; `disable_dtx` → `TF_NO_DTX`. `ti.AudioFeatures` is also accepted directly and via UpdateLocalAudioTrack. The **publisher answer** Opus fmtp is munged: `;usedtx=1` is appended unless TF_NO_DTX (and removed if NO_DTX); `;stereo=1;maxaveragebitrate=510000` is appended if TF_STEREO, else `;stereo=1` is removed | pkg/rtc/participant.go:3150-3162, 4364-4375; participant_sdp.go:325-419 |
| Subscriber stereo | `sprop-stereo=1` is added to the subscriber Opus fmtp if the track has TF_STEREO | pkg/rtc/mediatracksubscriptions.go:205-212; transport.go:3190-3225 |
| Other audio features | AGC/EC/NS/ENC/PRECONNECT_BUFFER are metadata only; no server media processing found | protocol/protobufs/livekit_models.proto:909-917 |
| PCMU/PCMA | Registered with PT 0/8 at 8 kHz, with audio RTCP-FB (nack). No transcoding. Server-specific handling covers silence frames for mute (0xFF μ-law / 0xD5 A-law) and a PT-0 special case in buffer bind | protocol/codecs/codecs.go; downtrack.go:126-160, 1897-1900; buffer_base.go:317 |

### D.5 E2EE and packet trailers

- **The server never decrypts.** `TrackInfo.Encryption` (`NONE`/`GCM`/`CUSTOM`, protocol/protobufs/livekit_models.proto:224-230) comes from the AddTrackRequest. `MediaTrackReceiver.IsEncrypted()` is `Encryption != NONE` (pkg/rtc/mediatrackreceiver.go:510-512). Effects found in this tag:
  1. Opus↔RED translation is disabled (see D.4).
  2. DownTrack codec matching for encrypted RED+Opus binds on the primary codec only (downtrack.go:468-484).
  3. Transceiver reuse can be disabled for E2EE tracks via `DisableTransceiverReuseForE2EE`. That is a participant param, and no YAML key was found for it (participant.go:4173).
  4. The **SIF trailer** is applied: each room has a random secret `trailer` (`utils.RandomSecret()`), sent to clients in JoinResponse as `sif_trailer`. For encrypted tracks the DownTrack appends it to **server-injected frames** (silence/blank frames) so the client E2EE layer recognises them as unencrypted server frames and skips decryption (pkg/rtc/room.go:138, 282, 340-342, 1069; subscribedtrack.go:129-133; downtrack.go:1995-2045).
- **Key frames and layers on encrypted video. UNVERIFIED in server code.** The SFU has no E2EE-specific parse path; buffer/forwarder never check `IsEncrypted`. It therefore parses VP8/H.264 payload headers and the Dependency Descriptor exactly as for plain tracks. This only works because the client SDKs leave codec headers unencrypted (e.g. the first VP8 bytes, H.264 NAL headers) and DD is an RTP header extension. That client behaviour is from LiveKit client SDK docs and knowledge, not verified here.
- **Packet trailer (`pkg/sfu/packettrailer`)** is a separate, non-E2EE feature.
  - Publishers may declare `packet_trailer_features` on AddTrackRequest: `PTF_USER_TIMESTAMP`, `PTF_FRAME_ID`, `PTF_USER_DATA` (protocol livekit_models.proto:919-923, livekit_rtc.proto:184).
  - The trailer sits at the end of the last packet of a frame: `[...trailer body][trailer_len ^ 0xFF][ 'L''K''T''S' ]`, with a 5-byte envelope (packet_trailer.go:17-45).
  - The SFU **forwards it untouched** to subscribers whose ClientInfo has `CAP_PACKET_TRAILER`. For other subscribers it **strips** the trailer on marker/end-of-layer-frame packets. It records `trailerStripped` in the sequencer so RTX retransmits are byte-identical (subscribedtrack.go:134-150; clientinfo.go:129-131; downtrack.go:1079-1084, 2341-2350).
  - `ProtocolVersion.SupportsPacketTrailer()` is `> 16` (pkg/rtc/types/protocol_version.go:103-105); which of the two checks gates what beyond the above is unverified.
- **Encrypted data packets.** `DataPacket.encrypted_packet` (`EncryptedPacket{encryption_type, iv, key_index, encrypted_value}`) is forwarded opaquely. The inner `EncryptedPacketPayload` can carry user/chat/rpc/stream messages (protocol livekit_models.proto:466, 476-496; participant.go:2679-2682). Data tracks also carry `Encryption` in DataTrackInfo (participant_data_track.go:111), and protocol has `datatrack/extension_e2ee.go`.

### D.6 Data channels

| Label | Init (server-created) | Writer | Source |
|---|---|---|---|
| `_reliable` | ordered=true, reliable | `NewDataChannelWriterReliable` with `rtc.datachannel_slow_threshold` (bit/s, default **0** = off) | pkg/rtc/transport.go:58-60; transportmanager.go:458-466 |
| `_lossy` | ordered=false, maxRetransmits=0 | `NewDataChannelWriterUnreliable` with `rtc.datachannel_lossy_target_latency` (default **0** = no drop control), min buffered 8 KiB | transportmanager.go:468-478; transport.go:79 |
| `_data_track` | ordered=false, maxRetransmits=0 | Unreliable, `rtc.datachannel_data_track_target_latency` default **100 ms**, min buffered 8 KiB; only if `enable_data_tracks` (default **true**) | transportmanager.go:480-488; config.go |
| other label | treated as "unlabeled" raw channel; used by one-shot signalling mode and raw forwarding | Reliable writer | transport.go:901-990 |

- **Where channels live.**
  - Dual-PC mode: the client creates `_reliable`/`_lossy` on the publisher PC, and the server creates the three on the subscriber PC when `SubscriberAsPrimary`.
  - Migration: channels are created as negotiated with id = client id + 6, because pion auto-ids step by 2. Old clients use the default ids with negotiated=false.
  - The publisher-side migrated DCs get explicit ids via `CreateDataChannelIfEmpty` (transportmanager.go:160-228, 431-490, 930-965).
  - Details unverified for single-PC mode.
- **Writer behaviour.**
  - Every write has a **50 ms** write deadline.
  - Reliable with slow threshold: deadline expiry keeps retrying while the measured bitrate is ≥ threshold. Otherwise it returns `ErrDataDroppedBySlowReader`.
  - Unreliable with target latency: the packet is dropped if `bufferedAmount > bitrate×targetLatency/8` and `> 8 KiB`.
  - The bitrate window is 2 s over 100 ms buckets, busy-only (pkg/sfu/datachannel/datachannel_writer.go:14-130; bitrate.go:13-14).
  - The deprecated `data_channel_max_buffered_amount` is only checked when the slow threshold is 0 (transport.go:1583-1596).
  - The receive buffer is 65535 B per read (transport.go:78). SCTP zero-checksum is enabled if the client supports it. WARP/SNAP (`rtc.enable_warp`, default off) puts SCTP INIT in SDP.
- **DataPacket types** (protocol/protobufs/livekit_models.proto:443-474): `user`, `speaker` (deprecated), `sip_dtmf`, `transcription`, `metrics`, `chat_message`, `rpc_request`, `rpc_ack`, `rpc_response`, `stream_header`, `stream_chunk`, `stream_trailer`, `encrypted_packet`, plus `participant_identity`, `destination_identities`, `sequence`, `participant_sid`. Server handling in `handleReceivedDataMessage` (participant.go:2550-2704):
  - The **sender identity/SID is always overwritten** with the authenticated one; it is blanked for hidden participants. Exception: an **agent** may send chat or text-stream headers on behalf of another identity, which are marked `generated=true`.
  - `transcription` is forwarded only if the sender is an agent.
  - `metrics` is not forwarded as data. It is timestamp-processed and echoed to all participants via `HandleMetrics`.
  - `rpc_response`/`rpc_ack` are consumed if they answer a **server-initiated** `PerformRpc` (API). Defaults: response timeout 10 s, max round-trip 2 s, max payload 15 KiB (protocol/utils/data_channel_rpc.go:30-34; participant.go:4645-4720). Otherwise they are forwarded.
  - Unknown types are logged as "unsupported" but still forwarded.
  - The channel kind it arrived on is authoritative (`dp.Kind = kind`).
  - Requires the `canPublishData` grant (participant.go:2491-2494).
- **Routing.**
  - `BroadcastDataPacketForRoom` excludes the sender and filters by `destination_identities` or legacy `UserPacket.destination_sids`.
  - Marshalling happens once, and fan-out runs in parallel above 4 recipients (`dataForwardLoadBalanceThreshold=4`).
  - UserPackets are de-duplicated by UUID `nonce`, keeping the last 100 per room (pkg/rtc/room.go:58, 2120-2180; pkg/rtc/user_packet_deduper.go:11-50).
- **Text/byte streams.** The server only forwards `stream_header`/`stream_chunk`/`stream_trailer` like other packets, and records a Prometheus metric on headers (participant.go:2648-2672). There is no reassembly or storage.
- **Reliable sequencing, dedupe, and replay.**
  - A reliable packet with `sequence>0` is dropped if `sequence <= lastPubReliableSeq` (participant.go:2550-2562).
  - The room caches sequenced reliable messages for **2 s**, up to 100,000 bytes (`dataMessageCacheTTL`, `dataMessageCacheSize`; room.go:62-63, 1292-1307).
  - A joining or resuming subscriber records the first seq per sender while its reliable DC is not writable. `replayJoiningReliableMessages` then replays cached messages (respecting destinations) and suppresses duplicates.
  - Unsequenced reliable messages (e.g. from the API SendData) are held up to 100,000 B while joining (participant.go:90, 4176-4237, 4309-4339).
  - Migrating in, the publisher's reliable stream waits up to **2 s** (cache ≤ 4 MiB) for the sequence to continue from the previous node (participant.go:82, 1779, 2505-2545; pkg/rtc/migrationdatacache.go:18-60).
- **Data tracks** (`enable_data_tracks`, default true).
  - `PublishDataTrackRequest` validation:
    - The participant needs `canPublishData`.
    - Handle must be 1..65535.
    - Name must be 1..256 chars, unique per participant.
    - Custom encoding id is limited to ≤ `limit.max_data_track_custom_encoding_length` (32).
  - The SFU allocates `DataTrackInfo{pub_handle, sid, name, encryption, frame_encoding, schema}` (participant_data_track.go:28-139).
  - Packets on `_data_track` use the binary header from protocol/datatrack/packet.go:84-110:
    - Fields: version; S/F start- and final-of-frame bits; X extension bit; 16-bit handle; 16-bit SN; 16-bit frame number; 32-bit timestamp; optional extensions.
    - Extensions include e2ee, participant SID, and user timestamp.
  - The SFU parses each packet, rewrites `Handle` to the subscriber-side handle, and forwards it on the subscriber's `_data_track` DC. Fan-out uses a spreader with threshold 20 (datatrack.go:66-80; datadowntrack.go:118-133).
  - Permission revocation follows track subscription permissions (participant_data_track.go:179-200).
  - `DataTrackSubscriptionOptions.target_fps` exists in proto, but `UpdateSubscriptionOptions` is a stub (`// DT-TODO`), so FPS decimation is **not implemented** (datadowntrack.go:135-137).
- **Data blobs** (`enable_participant_data_blob`, default **false**). This is per-participant key→bytes storage via `StoreDataBlobRequest`. Limits: key ≤ 256, total ≤ 64,000 B (`limit.max_data_blob_key_length`, `limit.max_data_blobs_size`) (config.go; participant_data_blob_handler.go:24-80; participant_data_blob.go).
## E. Transport: ICE, TURN, PC topology, DTLS, reconnect/resume, migration

### E.1 Ports and ICE (`mtu/pkg/rtcconfig/{config.go,webrtc_config.go}`, `pkg/config/config.go`)

| Item | Key | Default (code) | Notes / source |
|---|---|---|---|
| Signalling (HTTP/WS) | `port` | 7880 | `config.go:568`. Paths `/rtc` (v0, dual PC) and `/rtc/v1` (single PC with a wrapped `join_request`) (`pkg/service/rtcservice.go:101-104,152-176`). WHIP lives in `pkg/service/whipservice.go`. |
| WebTransport signalling | `webtransport.port` (+ `tls_cert_file`/`tls_key_file`) | 0 (off) | HTTP/3 listener (`config.go` WebTransportConfig; `pkg/service/webtransport.go`). Scope not researched further. |
| ICE/UDP port range | `rtc.port_range_start` / `port_range_end` | **50000-60000** in production. In `--dev` mode, a single UDP mux on 7882 instead | `rtcconfig/config.go` `Validate`: the range defaults apply only if `udp_port` is unset. Uses pion's ephemeral UDP port range, one socket per ICE agent. |
| ICE/UDP single-port mux | `rtc.udp_port` (`"7882"` or a range `"7882-7892"`) | unset | `webrtc_config.go:131-171`: `CreateUDPMuxesFromPorts` binds **min(NumCPU, len(range))** ports into a `MultiPortsUDPMux`, with 16 MiB socket buffers and optional `batch_io` write batching. **The code does not match docs:** if `port_range_*` is also set, the range wins (it is checked first), whereas docs say the mux takes precedence. |
| `use_stun_port_as_ice` | `rtc.use_stun_port_as_ice` | false | Adds a standalone UDP mux on **3478**. |
| ICE/TCP (passive) | `rtc.tcp_port` | **7881** | `ice.NewTCPMuxDefault`. Active TCP candidates are disabled (`pkg/rtc/config.go:71`). |
| Network types | `rtc.force_tcp` | UDP4/6 + TCP4/6 | `force_tcp` drops UDP (for testing). |
| External IP / NAT1:1 | `rtc.use_external_ip`, `node_ip`, `external_ip_only`, `advertise_internal_ip`, `stun_servers`, `require_ipv4`, `skip_external_ip_validation` | `use_external_ip` false | STUN discovery uses the default servers `global.stun.twilio.com:3478`, `stun.l.google.com:19302` and `stun1…`. The server adds STUN ICE servers to its own PC only when it is not ICE-lite and has no node IP or external IP. For Firefox (`!SupportsPrflxOverRelay`), only the external IP is advertised as host (`transport.go:433-456`). |
| Interface / IP filters | `rtc.interfaces.{includes,excludes}`, `rtc.ips.{includes,excludes}` | – | |
| mDNS | `rtc.use_mdns` | false | mDNS candidates are also filtered from SDP when this is off. |
| Loopback | `rtc.enable_loopback_candidate` | false | |
| ICE pair acceptance waits | `rtc.{host,srflx,prflx,relay}_acceptance_min_wait` | pion defaults | |
| ICE timeouts | hard-coded | disconnected 10 s, failed +5 s, keepalive 2 s | `transport.go:65-73`. Also TCP ICE connect 5-12 s, and connect-after-ICE 10-20 s. |
| SCTP tuning | `rtc.sctp_min_cwnd`, `sctp_fast_rtx_wnd`, `sctp_cwnd_ca_step` | pion defaults | Max message size is 65535 (`webrtc_config.go:55`). |
| IPv6 local exclusion | `ExcludeIPv6LocalCandidates` (participant param) | – | Wiring not traced. |

**ICE-lite** (`rtc.use_ice_lite`, default **false**):
- When it is enabled, ICE-lite is still turned off for a given PC in three cases:
  - protocol version ≤ 5;
  - Firefox (aggressive nomination; `transport.go:414-423`);
  - the client sets `JoinRequest/ParticipantInit.DisableICELite` (`pkg/service/roommanager.go:446-448`).
- With ICE-lite, the server does not add STUN servers.

### E.2 Embedded TURN (`pkg/service/turn.go`, `turnrelay.go`, `turnquota.go`; config `turn.*`)

| Key | Default (code) | Notes |
|---|---|---|
| `turn.enabled` | false | |
| `turn.udp_port` | **0** | Docs and config-sample suggest 3478, but no code default exists. TURN/UDP is also advertised as STUN. |
| `turn.tls_port` | **0** | Docs suggest 5349. Requires `turn.domain`, plus `cert_file`/`key_file` unless `external_tls`. Uses TLS ≥ 1.2. |
| Advertised TURNS URL | – | Always `turns:<domain>:443?transport=tcp`, **hard-coded to port 443** whatever `tls_port` is. This assumes an L4 load balancer or `tls_port: 443` (`roommanager.go:1078-1080`). |
| Advertised TURN/UDP | – | `turn:<nodeIP>:<udp_port>?transport=udp` for each node IP. |
| Relay ports | `relay_range_start/end` | 30000-40000 (dev: 30000-30002) (`config.go:713-722`) |
| Credentials | `ttl_seconds` | 300 s, clamped to [300, 86400]. Per-participant username/password come from the API key (`turnAuthHandler`). |
| Per-user allocation quota | `per_user_relay_allocation_limit` | 12 |
| Peer policy | `allow_restricted_peer_cidrs`, `deny_peer_cidrs` | Private, loopback, link-local, CGNAT, NAT64 and similar peers are denied by default. The relay policy restricts relaying to the node's own media ports. TCP relay allocations are rejected (`turnrelay.go`). |
| PROXY protocol | `proxy_protocol`, `proxy_protocol_trusted_cidrs` | false; trusted defaults to loopback |
| External TURN | `rtc.turn_servers[] {host, port, protocol udp/tcp/tls, username/credential or secret/secret_file, ttl}` | HMAC-SHA1 static-auth-secret credentials with a 4 h default TTL (`roommanager.go:1097-1140`) |

The TURN/TLS server is offered to clients in the JoinResponse and ReconnectResponse `ice_servers`. When the ICE preference is TLS, only TLS is offered (`iceServersForParticipant(..., tlsOnly)`).

### E.3 PC topology: dual PC vs single PC (`pkg/rtc/transportmanager.go:131-220`, `participant.go:2131`)

| Mode | How selected | PCs | Who offers | Notes |
|---|---|---|---|---|
| **Dual PC** (classic) | `/rtc` WebSocket without `join_request` | publisher PC (recvonly, client offers, server answers) **plus** subscriber PC (server offers, `IsOfferer=true`) | publisher: client; subscriber: server | `SubscriberAsPrimary` = protocol > 2 and canSubscribe. The primary PC carries data channels (created by the server on the subscriber PC). If the participant cannot subscribe, the publisher PC is primary. |
| **Single PC** | `/rtc/v1` with a base64 `WrappedJoinRequest` (NONE or GZIP) | one PC (`publisher` object, `IsSendSide=true`) | client | Registers the **union** of publish and subscribe codecs and filters per transceiver (`restrictReceiverCodecsToPublishList`, `configureSenderCodecs`). Uses the "consolidated" publisher header-extension/feedback set (adds abs-send-time and goog-remb). The stream allocator runs on this PC. The H.264 High filter is not applied (the server is the answerer). |
| **One-shot signalling** (WHIP / `UseOneShotSignallingMode`) | WHIP service | single PC | client | Data goes over the first unlabeled data channel as raw user payload. Transport failure closes the participant (no resume). |

### E.4 DTLS / SRTP / SCTP settings (`pkg/rtc/transport.go:349-431`)

**DTLS:**
- Curves: X25519, P-384, P-256.
- HelloVerify cookie is skipped (ICE already proved the address).
- Retransmission interval 100 ms; handshake timeout 1 min.
- `DisableCloseByDTLS` is set, so a DTLS close alert does not tear down the PC (needed for migration).

**DTLS role:**
- On the subscriber PC the server is the offerer. Pion offers `actpass`, and browsers answer `active`, so the server ends up as the DTLS server.
- On the publisher PC the server is the answerer, using pion's default answering role (client / `setup:active`). **UNVERIFIED** against the LiveKit pion fork.
- The only explicit role setting is in the migration replay path: `SetAnsweringDTLSRole(ExtractDTLSRole(previousAnswer))` (`transport.go:2143`; `protocol/sdp/sdp.go:66-97`).

**SRTP/SRTCP:** replay protection is **disabled**. Pion's RTX-less design otherwise breaks Firefox probing, which re-sends old packets.

**SCTP:**
- Data channels are detached (raw reads).
- Zero-checksum is enabled for SDKs that support it (not UNKNOWN SDK, Go ≥ 2.4.0).
- Optional block-write (when `datachannel_slow_threshold` > 0).

**WARP** (`rtc.enable_warp`, default false, experimental): SPED (DTLS-in-STUN) plus SNAP (SCTP INIT in SDP). These rely on the LiveKit pion forks.

**Other settings:**
- `SetIgnoreRidPauseForRecv(true)`: simulcast layer pause/resume is signalled over signalling, not via SDP `rid … paused`.
- `FireOnTrackBySdp`: OnTrack fires before the first RTP.

### E.5 Candidate-type fallback: UDP → ICE/TCP → TURN/TLS (`transportmanager.go:46-54, 779-887, 977-1055`)

**Keys:**
- `rtc.allow_tcp_fallback` (default **true**)
- `rtc.tcp_fallback_rtt_threshold` (ms, default 0 = no RTT gate)
- `rtc.allow_udp_unstable_fallback` (default false; requires the threshold > 0)

**When fallback triggers:**
- A PC fails short-lived (connected for less than 90 s; `shortConnectionThreshold`, `transport.go:75`), or
- 2 failures occur within 1 min (`failureCountThreshold`, `preferNextByFailureWindow`), and
- in either case, only while signalling is recent and valid.

**Step order:**
1. UDP → TCP, if the client supports ICE/TCP and the signalling RTT is under the threshold.
2. Otherwise → TLS, if TURNS is enabled.
3. TCP → TLS.
4. TLS is final.

The new `ICEConfig{PreferenceSubscriber, PreferencePublisher}` is applied to **both** PCs. With preferTCP, non-TCP candidates are stripped from local and remote SDP (`filterCandidatesParsed`, `transport.go:2533-2596`).

**UDP-unstable fallback:**
- An RR fraction-lost of ≥ 25 % counts as an unstable report.
- If 20 or more of the last 32 reports are unstable, and the signalling RTT is under 1.3× the UDP RTT and under the threshold, the server forces the fallback and asks the client to resume.

### E.6 Reconnect semantics

| Path | Trigger | Server behaviour | Source |
|---|---|---|---|
| **Resume** (quick reconnect, ICE restart) | Client reconnects the WS with `reconnect=1` (or `JoinRequest.reconnect`) and an optional `reconnect_reason` (`RR_SIGNAL_DISCONNECTED`, `RR_PUBLISHER_FAILED`, `RR_SUBSCRIBER_FAILED`, `RR_SWITCH_CANDIDATE`) | 1. Swaps the signal sink and sends `ReconnectResponse{ice_servers, client_configuration, server_info, last_message_seq}`. 2. Sends participant and room updates. 3. Runs `ICERestart`: published tracks `Restart()`, and on dual PC the **server-initiated subscriber ICE restart offer**. The client ICE-restarts the publisher PC with a new offer, which `isRemoteOfferRestartICE` detects. 4. Applies the client's `SyncState` (published tracks and data tracks must be known, otherwise it falls back to a full reconnect; also disabled tracks and subscriptions). Short connections feed the TCP/TLS fallback (`HandleClientReconnect`). | `roommanager.go:326-395`; `room.go:550-613, 623-700`; `participant.go:1834-1849` |
| Resume refused | Participant already closed, or not found while `reconnect` is set | `LeaveRequest{action: RECONNECT, reason: STATE_MISMATCH}` (protocol > 12) or `can_reconnect: true` | `roommanager.go:330-420` |
| **Transport failure** | Any PC fails | Sends `LeaveRequest{action: RESUME}` (only to clients that understand actions), closes the signal connection so the client resumes, and starts a **5 s** disconnect-cleanup timer (`disconnectCleanupDuration`). The participant closes if it has not resumed in time. | `participant.go:80, 2777-2811` |
| **Full reconnect** | Negotiation failure, ICE-restart failure, unknown tracks on resume. Optionally publication/subscription/data-channel errors (`rtc.reconnect_on_publication_error`, `reconnect_on_subscription_error`, `reconnect_on_data_channel_error`, all default **false**) | `LeaveRequest{action: RECONNECT}` + signal close. The client rejoins as a new participant. | `participant.go:4067-4090`; `roommanager.go:455-475` |
| Negotiation timeout | – | Failure after 15 s (`negotiationFailedTimeout`). Negotiation is debounced at 150 ms (10 ms fast path). | `transport.go:61-64` |
| Reliable-data continuity | – | `last_message_seq` in ReconnectResponse, plus the room's 2 s reliable-message replay cache (see D.6) | |

### E.7 Node migration

- **Code exists.** `MigrateState` Init → Sync → Complete (`pkg/rtc/types/interfaces.go:61-80`; `participant.go:1660-1800`).
  - `SetMigrateInfo` replays the previous offer and answer into a dummy PC to recreate transceivers and the DTLS role.
  - Data channels are re-created as *negotiated* with id + 6.
  - Tracks are synthesised from the previous state.
  - There is a 3 s migration wait (`migrationWaitDuration`), and a 2 s continuity wait for the reliable data sequence.
  - `SimulateScenario_Migration` exists for testing.
- **Not activated by the open-source service.** `ParticipantParams.Migration` is never set by `pkg/service` (grep finds no assignment). Rooms are pinned to one node in OSS, so session migration between nodes is effectively a LiveKit Cloud feature in this tag. This is an inference from grep; nothing in OSS triggers it.

### E.8 Data channels (summary; detail in D.6)

| Label | Ordered | maxRetransmits | Purpose |
|---|---|---|---|
| `_reliable` | true | – (reliable) | `DataPacket` RELIABLE |
| `_lossy` | false | 0 | `DataPacket` LOSSY |
| `_data_track` | false | 0 | Binary data-track packets (`enable_data_tracks`, default **true**) |
| any other label | – | – | "Unlabeled" raw channel (one-shot/WHIP, raw forwarding) |

Labels are defined at `transport.go:58-60`; server-created channel inits are at `transportmanager.go:458-488`.

---

## Z. Unverified items, gaps and surprises

**Not verified:**
- The DTLS answering role on the publisher PC (pion default assumed).
- Whether `urn:ietf:params:rtp-hdrext:framemarking` is used at all. It is registered, but no reader exists.
- E2EE keyframe and layer detection. This presumably works only because client SDKs leave codec headers and the DD in clear; that comes from client-side knowledge, not server code.
- The `sync_streams` stream-ID separator.
- Subscription resumption details after a permission re-grant.

**Present in code but dead or unwired in the OSS server (found by grep):**
- `ParticipantParams.Migration`.
- `SimTracks`, the unhandled-simulcast interceptor.
- `DisableDynacast`, `DisableCodecRegression`, `EnableStartAtDesiredQuality`, `DisableSenderReportPassThrough`.
- `audio.enable_loss_proxying`: `SetAudioLossProxying` has no caller.
- `UpdateTrackSettings.priority`: the stream allocator only uses its defaults, screenshare 255 and video 1.
- `DataTrackSubscriptionOptions.target_fps`.
- The `leaky-bucket` pacer, which falls back to no-queue.
- FlexFEC-03, which is defined but never registered.

**Code and docs disagree:**
- **TURN ports.** `turn.udp_port` and `turn.tls_port` have no code default (both 0). The docs' 3478 and 5349 are sample values.
- **TURNS URL.** It is advertised as `:443` regardless of `tls_port`.
- **UDP mux vs port range.** When both `rtc.udp_port` and `port_range_*` are set, the port range wins in code.

**Defaults worth knowing:**
- Send-side BWE (TWCC) is **off**, so subscribers use REMB with abs-send-time.
- `allow_pause` is false.
- `active_red_encoding` is false.
- `use_ice_lite` is false.
- `allow_tcp_fallback` is true.
