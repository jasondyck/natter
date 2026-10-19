# str0m 0.24.1: how close it gets to the LiveKit SFU feature set

Paths: `S/` = `vendor/str0m/src/`. `IS/` = the `is` 0.11.1 crate source in the cargo registry, the ICE crate str0m uses (re-exported at `S/lib.rs:726,752`).
Vendored patches, from a diff against the pristine source: audio NACK in place (`S/format/payload_params.rs` `resends_in_place`, `S/streams/send.rs`, `S/streams/mod.rs:300-302`, `S/change/sdp.rs:1388-1402`), RED PT ordering (`S/change/sdp.rs:1527-1537`), and Opus `nackable=true` (`S/packet/opus.rs:39-43`). None of them touch video.

## Summary

| # | Area | Verdict |
|---|---|---|
| 1 | Video codecs | **Partial**: VP8, VP9, H264 and AV1 work. H265 and AV1 are `#[doc(hidden)]`. Frame-mode repacketization flattens SVC and VP8 temporal layers. |
| 2 | Simulcast | **Partial**: rid receive and the SDP side are fully done. There is no layer-switching forwarder; the SFU has to write one. |
| 3 | Dependency Descriptor / SVC | **Missing**: no DD parsing at all. VP9 SVC fields are parsed but never rewritten. |
| 4 | PLI/FIR | **Supported**. No throttling. |
| 5 | BWE | **Supported**: TWCC send-side GCC (delay + loss + ALR + probes), pacer, padding, REMB. No receive-side estimator. |
| 6 | NACK/RTX | **Supported**. RED is audio-only. **No FEC.** |
| 7 | Data channels | **Supported**. |
| 8 | ICE | **Partial**: full ICE and ICE-lite, restart, TCP candidates. **No TURN client** (sans-IO). |
| 9 | Header extensions | **Supported**, plus custom `ExtensionSerializer`. No `extmap-allow-mixed` negotiation. |
| 10 | E2EE / playout-delay / VLA / abs-capture-time | E2EE passes through in RTP mode. Playout-delay and abs-capture-time are built in. VLA has a serializer you must register. |
| 11 | Performance | Video forwarding should use RTP mode. `rtp_mode` is **per `Rtc`**, so it applies to all media in that `Rtc`. |

## 1. Video codecs

| Codec | Depacketize / packetize | Keyframe detect | Notes |
|---|---|---|---|
| VP8 | Yes / Yes (`S/packet/mod.rs:350,397`) | `detect_vp8_keyframe` `S/packet/vp8.rs:403`; `Vp8CodecExtra.is_keyframe` `:393`, set `:751` | The depacketizer exposes `layer_index`/`tl0_picture_id`/`picture_id` (`vp8.rs:376-393`). The frame-mode packetizer writes **only its own PictureID**, with no TID/TL0PICIDX (`vp8.rs:458-500`), so temporal-layer info is lost downstream. For RTP mode there are `Vp8Descriptor::parse` (`vp8.rs:250`), `starts_keyframe` (`:341`) and `Vp8Patch` (PictureID/TL0PICIDX/KEYIDX rewrite, `vp8.rs:15-170`), applied through `RtpWrite::vp8_patch` (`S/streams/send.rs:292`). This is the same job as LiveKit's VP8 munger, built in. |
| VP9 | Yes / Yes (`mod.rs:351,398`) | `detect_vp9_keyframe(_bitstream)` `vp9.rs:131,164`; extra `vp9.rs:582` | Profiles 0 and 2 are offered (`S/format/codec_config.rs:453-480`). The depacketizer parses SVC fields (sid/tid/ns/SS/widths/heights, `vp9.rs:418-470`) and `Vp9CodecExtra.layers_scheme` lets you truncate spatial layers in frame mode (`vp9.rs:28-115`). The packetizer **always emits TID=0, SID=0, single-layer SS** (`vp9.rs:269-345`), so frame-mode output cannot carry SVC. Flexible and non-flexible modes are set through `set_vp9_packetizer_mode` (`S/config.rs:839`). |
| H264 | Yes / Yes (`mod.rs:347,394`) | `detect_h264_keyframe` `h264.rs:58` (IDR in single/STAP-A/FU-A); extra `h264.rs:307-381` | Default profiles are 42001f, 42e01f, 4d001f and 64001f, each in packetization-mode 1 and 0 variants (`codec_config.rs:366-382`, fmtp `S/format/format_params.rs:45-46`). Profile matching is in `payload_params.rs:434`. The depacketizer handles single NAL, STAP-A and FU-A. The packetizer **always uses FU-A/STAP-A** (`h264.rs:138,164`) and ignores mode 0, so a mode-0-only peer only works through RTP passthrough. |
| AV1 | Yes / Yes (`mod.rs:352,399`) | `detect_av1_keyframe` `av1.rs:16`; extra `av1.rs:358-367` | `Codec::Av1` is `#[doc(hidden)]` with "TODO show this when we support Av1" (`S/format/codec.rs:66-68`), yet enabled by default (`codec_config.rs:103,483`). There is no DD (see 3), so AV1 SVC is impossible. |
| H265 | Yes / Yes (`mod.rs:348,395`) | `detect_h265_keyframe` `h265.rs:65` | Also `#[doc(hidden)]` (`codec.rs:59-61`). Profile-tier-level negotiation exists (`codec_config.rs:192-206,390-406`). Treat it as experimental. |
| H266 | Yes / Yes | `detect_h266_keyframe` | Off by default (`codec_config.rs:100`). |

- In frame mode, `MediaData::is_keyframe()` covers every codec (`S/media/event.rs:289-298`).
- In RTP mode, the `format::detect_*_keyframe` functions are public (`S/format/mod.rs:12-17`).
- Video payloads get transport-cc, FIR, NACK, PLI and REMB feedback by default (`codec_config.rs:129-133`).

**SFU must build:** codec-aware layer filtering, VP9 SVC rewriting (all of it in RTP mode), and H264 mode-0 handling if it matters.

## 2. Simulcast

- **Receive: supported.**
  - `Extension::RtpStreamId` and `RepairedRtpStreamId` are in `S/rtp/ext.rs:53-58`, with standard IDs 10 and 11 (`ext.rs:378-379`).
  - SDP `a=simulcast`/`a=rid` is parsed and answered inverted (`S/change/sdp.rs:1359-1367`).
  - `MediaAdded.simulcast` is at `S/media/event.rs:60`. The `Simulcast`/`SimulcastLayer` types (with max-width/height/br/fps attributes) are at `event.rs:83-180`.
  - SSRC is learned per rid in `map_dynamic_by_rid` (`S/streams/mod.rs:196-228`), and RTX is paired through repaired-rid.
  - `MediaData.rid` is at `event.rs:231`.
  - In RTP mode, `RtpPacket` has **no rid field** (`S/streams/mod.rs:44-72`). Map it with `direct_api().stream_rx(&ssrc).rid()` (`S/streams/receive.rs:211`), because browsers stop sending the rid extension once the SSRC is acknowledged.
- **Send (subscriber side): Missing as a feature.**
  - Frame mode: `Writer::write(pt, wallclock, rtp_time, data)` (`S/media/writer.rs:165`). str0m assigns sequence numbers itself (`S/packet/payload.rs:92`) and the VP8/VP9 packetizers assign their own PictureID, so **seq/picture-id continuity across a layer switch is automatic**. The RTP timestamp is passed through as-is, though. Each simulcast SSRC has its own random timestamp base, so the SFU must rebase `rtp_time` when it switches, and must only switch on a keyframe of the target layer. str0m's `chat` example only forwards rid "h" and never switches (`examples/chat.rs:641-650`).
  - RTP mode: `StreamTx::write_rtp(RtpWrite::new(pt, seq_no, time, wallclock, payload))` (`S/streams/send.rs:233,437`). The caller supplies **seq and timestamp verbatim** (`send.rs:465-476`), so the SFU does all the seq/ts rewriting: offsets, gap handling for dropped packets, and late or RTX-recovered packets. `.nackable(true)` is **off by default** (`send.rs:268`). Leave it off and subscriber NACKs go unanswered.
  - `enable_raw_packets` is debug only and clones every packet (`S/config.rs:748`). It plays no part in this.
  - The direct API gives you `stream_tx_by_mid` (`S/change/direct.rs:355`), `reset_stream_tx` (`direct.rs:372`), and `declare_stream_tx`/`expect_stream_rx` (`direct.rs:252,303`).

## 3. SVC / Dependency Descriptor

- **DD: Missing.** Grepping for `dependency.descriptor` finds nothing, and the `Extension` enum has no DD variant (`ext.rs:26-69`).
  - You can add it through `Extension::with_serializer(uri, impl ExtensionSerializer)` (`ext.rs:248`, trait `ext.rs:121-141`). The serializer would parse into `ExtensionValues::user_values` (`ext.rs:927`, `UserExtensionValues::set/get` `ext.rs:1029,1050`) and write it back out.
  - DD is often longer than 16 bytes, so it needs the two-byte form. `requires_two_byte_form` exists (`ext.rs:138`), but str0m treats `a=extmap-allow-mixed` as an unused attribute (`S/sdp/parser.rs:1127`). Whether browsers accept its two-byte headers needs testing.
- **VP9 SVC layer dropping:** frame mode has only `layers_scheme`/`tid` truncation, and the repacketized output is single-layer (see 1). RTP mode has no VP9 descriptor rewriter, which LiveKit has. The SFU must parse descriptors itself or use `Vp9Depacketizer`'s public fields (`vp9.rs:418-470`, crate-private).
- The packetizers are only exported under the `unversioned` feature, and that export covers only H264, VP8 and Opus (`S/lib.rs:2261-2270`).

## 4. Keyframe requests: Supported

- **Send:**
  - Frame mode: `Writer::request_keyframe(rid, kind)` (`writer.rs:294`), with the guard `is_request_keyframe_possible` (`writer.rs:267`).
  - RTP mode: `StreamRx::request_keyframe(kind)` (`receive.rs:240`) builds a PLI, or a FIR with an incrementing seq (`receive.rs:652-680`).
  - `DirectApi::send_pli_feedback` is at `direct.rs:431`.
- **Receive:** `Event::KeyframeRequest(KeyframeRequest{mid, rid, kind})` (`S/lib.rs:1021`, `event.rs:309-330`).
- **No throttling or coalescing.** The SFU must rate-limit, map a subscriber's PLI to the publisher's currently forwarded layer, and send its own PLI when it switches layers.

## 5. Bandwidth estimation: Supported (send side)

- **Setup:** `RtcConfig::enable_bwe(Some(initial))` (`S/config.rs:468`), `Rtc::bwe()` (`S/lib.rs:2141`), `Bwe::set_desired_bitrate` / `reset` (`S/bwe/api.rs:60,71`).
- **Output:** `Event::EgressBitrateEstimate(BweKind::Twcc{..,can_probe} | Remb{estimate, mid})` (`lib.rs:1014`, `bwe/api.rs:10-36`).
- **Internals:** delay-based trendline (`S/bwe/delay/`), loss controller (`bwe/loss_controller.rs`), ALR (`bwe/alr_detector/`), probe clusters (`bwe/probe/`).
- **Pacer:** a leaky bucket with padding, used when BWE is on; otherwise a null pacer (`S/session.rs:170-174`, `S/pacer/leaky.rs`). Padding is sent over RTX (`S/streams/send.rs` `padding_enabled`) or through a probe SSRC. `set_unpaced` is at `send.rs:430`.
- **TWCC feedback** generation for incoming packets is at `session.rs:443`. It needs `Extension::TransportSequenceNumber` (standard ID 3, `ext.rs:375`). **natter clears it** (`sfu.rs:176-182`).
- **REMB:** outgoing REMB is only a number the app supplies, via `StreamRx::request_remb` (`receive.rs:247`). There is **no receive-side estimator**.
- **SFU must build:** a bitrate allocator that turns the estimate into layer choices across subscribed tracks (LiveKit's StreamAllocator), set the desired bitrate to the sum of the layers it wants, pause and resume tracks, and run any congestion-triggered layer downgrades.

## 6. NACK / RTX / RED / FEC

- **NACK and RTX: Supported.**
  - Receive: NACK registers (`S/streams/register_nack.rs`), RTX unwrapping into the main stream, and padding-only RTX dropped (`session.rs:727-750`).
  - Send: RTX cache of 1024 packets over 3s, with a resend cap of 0.15 (`send.rs:38-40`), set through `set_rtx_cache` (`send.rs:402`). Buffers are sized with `set_send_buffer_video` (`config.rs:666`).
- **RED: audio only.** `Codec::Red` "wraps a primary audio codec" (`codec.rs:86-89`) and is enabled through `enable_opus(_, use_red)` (`config.rs:249`). In RTP mode RED is **not unwrapped** (`session.rs:751,811`); it passes through raw.
- **FlexFEC/ULPFEC: Missing.** There are no hits anywhere in the source.

## 7. Data channels: Supported

- **SCTP:** sctp-proto 0.10.5.
- **`ChannelConfig`:** `{label, ordered, reliability: Reliable | MaxPacketLifetime{ms} | MaxRetransmits{n}, negotiated: Option<u16>, protocol}` (`S/sctp/mod.rs:190-234`).
- **Opening channels:** `SdpApi::add_channel(_with_config)` (`change/sdp.rs:414,441`) and `DirectApi::create_data_channel` (`direct.rs:171`).
- **Writing:** `Channel::write(binary, buf)`, `buffered_amount`, `set_buffered_amount_low_threshold` (`S/channel.rs:58,84,95`).
- **Events:** `ChannelOpen`, `ChannelData`, `ChannelClose` and `ChannelBufferedAmountLow` (`lib.rs:989-998`).
- **Max message size:** local 256 KiB (`sctp/mod.rs:35`, advertised at `change/sdp.rs:1460`). Remote defaults to 64 KiB, or whatever SDP says (`sctp/mod.rs:38,359-380`).
- **SNAP** fast setup: `set_snap_enabled` (`config.rs:771`).

## 8. ICE

| Feature | Status | Evidence |
|---|---|---|
| Full ICE | Supported | Default. `DirectApi::set_ice_controlling` is at `direct.rs:58`. |
| ICE-lite | Supported | `set_ice_lite` (`config.rs:149`). Lite pairs time out without inbound binding requests (`IS/pair.rs:12,228`). |
| ICE-TCP | Partial | Candidates can be `Protocol::{Udp,Tcp,SslTcp,Tls}` with a `TcpType` (`str0m-proto-0.7.0/src/net.rs:10-21`, `IS/candidate.rs:752,888`). Sans-IO: the app owns the sockets and RFC 4571 framing. |
| TURN client | **Missing** | Only `Candidate::relayed(addr, local, proto)` exists (`IS/candidate.rs:289-296`). The app must implement the TURN Allocate/Refresh/ChannelBind protocol. Running a TURN *server* is a separate project either way. |
| ICE restart | Supported | `SdpApi::ice_restart(keep_local)` (`change/sdp.rs:467`), `IceAgent::ice_restart` (`IS/agent.rs:988`). |
| Consent freshness | Partial | Periodic binding keepalives. A pair is pruned after `max_retransmits` unanswered (`IS/pair.rs:470-486`, `IS/agent.rs:1229-1243`). This is not RFC 7675's exact 30s rule. Timing is set through `set_max_stun_rto` / `set_max_stun_retransmits` (`config.rs:160-177`). |

## 9. Header extensions (`S/rtp/ext.rs:26-69`)

The `Extension` enum has:
- AbsoluteSendTime
- AbsoluteCaptureTime
- AudioLevel
- TransmissionTimeOffset
- VideoOrientation
- TransportSequenceNumber
- PlayoutDelay
- VideoContentType
- VideoTiming
- RtpStreamId
- RepairedRtpStreamId
- RtpMid
- FrameMarking
- ColorSpace
- `UnknownUri(String, Arc<dyn ExtensionSerializer>)` for custom extensions

Parsed values land in `ExtensionValues` (`ext.rs:887-927`). The standard map is `ext.rs:370-383`. Custom extensions use `Extension::with_serializer` (`ext.rs:248`). One-byte and two-byte forms are both handled (`ext.rs:76-86`).

## 10. E2EE / playout-delay / VLA / abs-capture-time

- **E2EE / insertable streams:** in RTP mode the payload is opaque and forwarded as an `Arc<[u8]>`, so no server work is needed beyond keyframe detection.
  - LiveKit-style E2EE leaves the codec header bytes unencrypted, so `detect_vp8_keyframe` and friends still work on the first payload bytes.
  - Frame mode **re-parses and repacketizes** the bitstream. H264 is split on Annex-B start codes, and VP9/AV1 headers are re-derived. That is fragile on encrypted payloads, so do not use frame mode for E2EE video.
- **Playout-delay:** `Extension::PlayoutDelay`, `Writer::playout_delay(min,max)` (`writer.rs:138`), `ExtensionValues.play_delay_min/max` (`ext.rs:912-914`).
- **Video-layers-allocation:** `str0m::rtp::vla::{VideoLayersAllocation, Serializer}`, URI at `S/rtp/vla.rs:7`, serializer at `:212`. It is not in the standard map. Register it with `Extension::with_serializer(vla::URI, vla::Serializer)`. Values come through `user_values`.
- **abs-capture-time:** built in (`ext.rs:29`, `Writer::abs_capture_time` `writer.rs:109`). natter already forwards it (`sfu.rs:1223`).

## 11. Performance: frame mode vs RTP mode

- **Frame mode:** every incoming video packet goes through a reorder/jitter buffer and is depacketized into a newly allocated frame (`media/mod.rs:389`). Frames are held until complete, up to `reordering_size_video`, default 30 packets (`config.rs:541-556`), so packet loss adds about one NACK RTT of delay before forwarding. Then **every subscriber's `Writer` repacketizes** the frame, which costs N packetize passes and copies per frame.
- **RTP mode:** one `Arc<[u8]>` allocation per received packet (`session.rs:818-820`), shared by all subscribers through `RtpWrite::new(.., payload: impl Into<Arc<[u8]>>)`. Each subscriber only pays for header build plus SRTP. Packets are forwarded immediately and may arrive out of order (`config.rs:556` doc). Only new packets are emitted (`session.rs:824-830`).
- **Verdict:** forward video in RTP mode. That is LiveKit's model, and it is the only one that keeps SVC and temporal layers, E2EE and low loss latency.
- **Catch:** `rtp_mode` is one flag for the whole `Rtc`. `Rtc::writer()` panics in RTP mode (`lib.rs:1464-1466`), and `MediaData` events stop. Audio would switch to RTP mode too, and lose frame-mode RED unwrapping/recovery and the audio reorder buffer (`session.rs:751-856`).

## natter today (`crates/server/src/sfu.rs`)

- One thread, one UDP socket, `Vec<Client>` each holding a `Box<Rtc>` (`sfu.rs:1-6`). Audio only.
- **Codecs:** Opus only, built through `CodecConfig::new_from_payload_params(vec![opus_params])` (`sfu.rs:194`). RED and NACK are optional and DTX is forced through a serde hack (`sfu.rs:201-212`).
- **Extensions:** the map is cleared and only AudioLevel(1), RtpMid(4) and AbsCaptureTime(5) are kept (`sfu.rs:176-182`). There is no TWCC and no rid/rrid.
- **Mode and forwarding:** frame mode. `Event::MediaData` becomes `Propagated::MediaData` (`sfu.rs:1048`), which fans out to `handle_media_data_out`. That function looks up the out mid, `writer.match_params`, sets `abs_capture_time` and calls `writer.write(pt, network_time, data.time, data.data.clone())` (`sfu.rs:1207-1233`). The source `data.time` passes straight through, and str0m numbers the packets.
- **Outbound m-lines:** added per publisher track with `add_media(kind, SendOnly, stream_id, None, None)` and no simulcast (`sfu.rs:1141`).
- **Not handled:** no `KeyframeRequest` handling and no BWE.

### What video + simulcast would need

1. **Config.**
   - Enable VP8 and H264 (plus VP9/AV1 if wanted) and keep RTX.
   - Restore `TransportSequenceNumber` and add `RtpStreamId`/`RepairedRtpStreamId`, plus `enable_bwe(Some(..))`.
   - Optionally register VLA and a DD serializer.
2. **Choose a mode.**
   - **(a) All RTP mode, recommended.** Turn on `set_rtp_mode(true)` and handle `Event::RtpPacket`.
     - Audio: forward RTP as-is with PT remapping, and send RED unchanged to subscribers that negotiated it. The vendored audio-NACK patch sits in `StreamTx`, so it should still apply as long as writes use `.nackable(true)`.
     - Video: build a per-subscriber/per-track forwarder (LiveKit's DownTrack + munger). It needs:
       - a rid lookup through `stream_rx(&ssrc).rid()`;
       - a target/current layer state machine;
       - switching only on keyframes (`format::detect_*_keyframe`);
       - a seq offset that skips dropped packets and remaps late or RTX packets;
       - a timestamp offset rebased on each switch;
       - VP8 PictureID/TL0PICIDX continuity via `Vp8Patch`;
       - `nackable(true)`;
       - PLI to the target layer, throttled;
       - subscriber `KeyframeRequest` mapped to the publisher's current layer;
       - a bitrate allocator driven by `EgressBitrateEstimate` and `set_desired_bitrate`.
   - **(b) Frame mode for video.** Possible for VP8/H264 simulcast only. It needs the keyframe-gated switch, a timestamp rebase, and PLIs relayed with the chosen rid. str0m handles seq and picture-id itself. It loses temporal/SVC layers, adds latency on loss, and costs repacketization per subscriber.
   - **(c) A second `Rtc`/PeerConnection per client for video in RTP mode,** keeping audio in frame mode. This doubles ICE/DTLS and the signalling work.
3. **Signalling.** Publisher offers carry `a=simulcast` and are handled automatically. Subscriber m-lines stay single-stream with `simulcast=None`, so the SFU picks one layer per subscriber. Also send layer and quality hints to clients (LiveKit sends these over its signalling protocol).
4. **Threading.** Video bitrates make the single-core loop the bottleneck: about 60k forwarded packets/s according to the memory notes. Plan to shard rooms across threads, each with its own socket, or use `SO_REUSEPORT`.
