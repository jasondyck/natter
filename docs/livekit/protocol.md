# LiveKit non-media surface: inventory (server v1.13.10)

**Sources** are local clones under this directory:
- `livekit/` is `github.com/livekit/livekit` at tag **v1.13.10** (`version/version.go`: `Version = "1.13.10"`, CHANGELOG dated 2026-10-14).
- `protocol/` is `github.com/livekit/protocol` at commit **9bb708cdb819** (2026-10-12). This is the exact pseudo-version pinned in `livekit/go.mod` (`v1.52.2-0.20261012065025-9bb708cdb819`).
- `mtu/rtcconfig.go` is `livekit/mediatransportutil` `pkg/rtcconfig/config.go` at the pinned commit `f234b534b095`.
- Docs come from docs.livekit.io (fetched 2026-10-17) and are marked **[docs]**.

Paths below are relative to this directory. Claims marked **[unverified]** were not confirmed in source.

---

## 1. Signalling protocol

### Endpoints (`livekit/pkg/service/rtcservice.go` `SetupRoutes`, `whipservice.go`, `server.go`)
| Path | What |
|---|---|
| `GET /rtc` (WS upgrade) | Classic "v0" signalling. Uses 2 PeerConnections (publisher + subscriber). Non-WS requests get a 404. |
| `GET /rtc/validate` | Same validation over plain HTTP. Returns `success` or an HTTP error. CORS `*`. Used by clients to diagnose join failures. |
| `GET /rtc/v1` (WS) | "Signalling v1.5" (CHANGELOG #4180). **Requires** the `join_request` query param and sets `UseSinglePeerConnection=true`, i.e. one PC for both directions. |
| `GET /rtc/v1/validate` | Validation for v1. |
| `POST/GET/OPTIONS /whip/v1`, `GET/PATCH/DELETE /whip/v1/{participant_id}` | **Native WHIP publish into a room**, built into the server and separate from the Ingress service. It uses "one-shot signalling mode" (`roommanager_service.go`). PATCH does ICE trickle/restart. Optional headers are `X-LiveKit-ClientInfo` (JSON ClientInfo) and `X-Livekit-Ingress`. Body limit is `http.DefaultMaxHeaderBytes`. |
| `/agent` (WS) | Agent worker control channel (`agentws.go`). Also available over WebTransport/HTTP3 when `webtransport.port` is set (`agentwt.go`). |
| `/agents/{agent_name}/{deployment}/{path...}` | "Agent endpoints": HTTP routes that the server reverse-tunnels to agent workers (`pkg/agent/endpoint/front.go`). New in 1.13.x; an unnamed deployment is addressed as `_`. Disable with `agents.endpoints.disabled`. |
| `/twirp/livekit.<Service>/<Method>` | Server APIs (see §3). Registered via `xtwirp.RegisterServer`. |
| `/` | Health check (`defaultHandler`): returns `OK`. |
| `/debug/pprof/*`, `/debug/goroutine`, `/debug/rooms` | Only on `debug_handler.port`, or on the main mux in development mode. |

**Auth.** Send `Authorization: Bearer <jwt>`, or fall back to the `?access_token=` query param, which is read from the URL query only and never from a form body (`auth.go` `APIKeyAuthMiddleware`).

### Query params on `/rtc` (v0) (`rtcservice.go` `validateInternal`, `utils.go` `ParseClientInfo`)
| Param | Meaning |
|---|---|
| `access_token` | JWT |
| `auto_subscribe` | Default true |
| `auto_subscribe_data_track` | Optional bool |
| `adaptive_stream` | Bool. Enables the server-side adaptive-stream semantics for this subscriber. |
| `disable_ice_lite` | Bool |
| `subscriber_allow_pause` | Optional bool. Overrides the congestion-control pause for this subscriber. |
| `reconnect` | `1` means **resume** (ICE restart, same session). Absent means full join. |
| `reconnect_reason` | int `ReconnectReason` |
| `sid` | Participant SID, used only when `reconnect=1` |
| `publish` | Publish-only hint, used by ValidateConnectRequest |
| `attributes` | base64url JSON map of initial participant attributes |
| `protocol` | Signalling protocol version (int) |
| `client_protocol` | Client feature protocol (int), separate from `protocol` |
| `sdk` | `js`, `ios`/`swift`, `android`, `flutter`, `go`, `unity`, `reactnative`, `rust`, `python`, `cpp`, `unityweb`, `node`, `esp32` |
| `version`, `os`, `os_version`, `browser`, `browser_version`, `device_model`, `network` | ClientInfo fields |
| `capabilities` | Comma-separated `ClientInfo.Capability` names: `CAP_PACKET_TRAILER`, `CAP_COMPRESSION_DEFLATE_RAW` |

### `/rtc/v1`: `join_request`
The value is base64url(`WrappedJoinRequest{compression: NONE|GZIP, join_request: bytes}`). The inner `JoinRequest` (`livekit_rtc.proto`) contains:
- `client_info`
- `connection_settings{auto_subscribe, adaptive_stream, subscriber_allow_pause?, disable_ice_lite, auto_subscribe_data_track?}`
- `metadata`, `participant_attributes`
- `add_track_requests[]` and `publisher_offer`, which allow publishing in the join (fast publish)
- `reconnect`, `reconnect_reason`, `participant_sid`, `sync_state`

The size cap is `http.DefaultMaxHeaderBytes` (1 MiB), also applied after gzip.

### Wire format (`livekit/pkg/service/wsprotocol.go`)
- **Binary WS frame** carries a protobuf `SignalRequest`/`SignalResponse`.
- **Text WS frame** carries protojson. The server mirrors whichever format the client last sent (`useJSON` toggles per message).
- The message size limit is `limit.signal_message_size_limit`, default 2 MiB.

### Protocol version
- `CurrentProtocol = 17` (`livekit/pkg/rtc/types/protocol_version.go`).
- Feature gates by version:

| Version | Feature |
|---|---|
| >0 | protobuf, packed stream id |
| >1 | data packets |
| >2 | subscriber-as-primary, speakers-changed |
| >3 | transceiver reuse |
| >4 | connection quality |
| >5 | session migrate, ICE-lite |
| >6 | unpublish |
| >7 | fast start |
| >8 | disconnected update |
| >9 | sync stream id |
| >10 | conn-quality LOST |
| >11 | async room id, identity-based reconnection |
| >12 | regions in LeaveRequest (`can_reconnect` obsoleted by `action` since v13) |
| >14 | non-error signal response (`RequestResponse`) |
| >15 | moving (`room_moved`) |
| >16 | packet trailer |

### `SignalRequest` oneof (client to server) (`protocol/protobufs/livekit_rtc.proto`)
| # | Field | One line |
|---|---|---|
| 1 | `offer` SessionDescription | Publisher-PC offer (in single-PC mode, the offer for the one PC) |
| 2 | `answer` SessionDescription | Answer to the server's subscriber offer |
| 3 | `trickle` TrickleRequest | ICE candidate `{candidateInit JSON, target PUBLISHER/SUBSCRIBER, final}` |
| 4 | `add_track` AddTrackRequest | Declare a track before sending media: cid, type, source, layers, simulcast_codecs, encryption, stream, backup_codec_policy, audio_features, packet_trailer_features |
| 5 | `mute` MuteTrackRequest | Mute or unmute your own published track |
| 6 | `subscription` UpdateSubscription | Subscribe or unsubscribe by track_sids / participant_tracks |
| 7 | `track_setting` UpdateTrackSettings | Per-subscription: disabled, quality, width/height, fps, priority (adaptive stream) |
| 8 | `leave` LeaveRequest | Client leaving |
| 10 | `update_layers` | **Deprecated.** Update video layers. |
| 11 | `subscription_permission` SubscriptionPermission | Who may subscribe to my tracks (`all_participants` or a per-participant TrackPermission list) |
| 12 | `sync_state` SyncState | Resume or migration: answer/offer, subscriptions, published tracks, data channels, disabled tracks, DC receive seq, data tracks |
| 13 | `simulate` SimulateScenario | Testing: speaker_update, node_failure, migration, server_leave, switch_candidate_protocol, subscriber_bandwidth, disconnect_signal_on_resume(_no_messages), leave_request_full_reconnect |
| 14 | `ping` int64 | **Deprecated.** Replaced by `ping_req`. |
| 15 | `update_metadata` UpdateParticipantMetadata | Own metadata/name/attributes, with request_id. Requires `canUpdateOwnMetadata`. |
| 16 | `ping_req` Ping | `{timestamp, rtt}` |
| 17 | `update_audio_track` UpdateLocalAudioTrack | Change AudioTrackFeatures of a published track |
| 18 | `update_video_track` UpdateLocalVideoTrack | Change published dimensions |
| 19 | `publish_data_track_request` | Publish a **data track**: pub_handle (u16), name, encryption, frame_encoding, schema |
| 20 | `unpublish_data_track_request` | Unpublish a data track |
| 21 | `update_data_subscription` | Subscribe or unsubscribe to data tracks |
| 22 | `store_data_blob_request` | Store a participant **data blob** ("async attributes", #4619). Gated by `enable_participant_data_blob`. |
| 23 | `get_data_blob_request` | Fetch another participant's blob |

### `SignalResponse` oneof (server to client)
| # | Field | One line |
|---|---|---|
| 1 | `join` JoinResponse | room, participant, other_participants, server_version, ice_servers, subscriber_primary, alternative_url, client_configuration, server_region, ping_timeout, ping_interval, server_info, sif_trailer, enabled_publish_codecs, fast_publish |
| 2 | `answer` | Answer to the publisher offer |
| 3 | `offer` | Server offer for the subscriber PC |
| 4 | `trickle` | ICE candidate |
| 5 | `update` ParticipantUpdate | Participant join, leave, or changes (full ParticipantInfo, versioned) |
| 6 | `track_published` | Ack for add_track (cid to TrackInfo) |
| 8 | `leave` LeaveRequest | Server-initiated disconnect with `action` DISCONNECT/RESUME/RECONNECT, `reason` DisconnectReason, `regions` |
| 9 | `mute` | Server-initiated mute (admin) |
| 10 | `speakers_changed` | Active speaker deltas |
| 11 | `room_update` | Room metadata or other changes |
| 12 | `connection_quality` | Per-participant quality (EXCELLENT, GOOD, POOR, LOST) and score |
| 13 | `stream_state_update` | Subscribed stream ACTIVE/PAUSED (congestion control) |
| 14 | `subscribed_quality_update` | **Dynacast**: which codec layers subscribers need |
| 15 | `subscription_permission_update` | Permission to a track changed |
| 16 | `refresh_token` string | Refreshed JWT, so a long session does not use an expired token |
| 17 | `track_unpublished` | Server-initiated unpublish |
| 18 | `pong` int64 | **Deprecated.** |
| 19 | `reconnect` ReconnectResponse | Reply to a resume (ice_servers, client_configuration, server_info, last_message_seq) |
| 20 | `pong_resp` Pong | `{last_ping_timestamp, timestamp}` |
| 21 | `subscription_response` | Subscription error (NOT_FOUND, codec unsupported, etc.) |
| 22 | `request_response` | Result for request_id-carrying requests (OK, NOT_FOUND, NOT_ALLOWED, LIMIT_EXCEEDED, QUEUED, UNSUPPORTED_TYPE, INVALID_HANDLE, INVALID_NAME, DUPLICATE_*, INVALID_REQUEST) |
| 23 | `track_subscribed` | Tells the publisher its track got its first subscriber |
| 24 | `room_moved` | Participant was moved to another room (new token and room info) |
| 25 | `media_sections_requirement` | Single-PC: number of audio/video m-sections needed |
| 26 | `subscribed_audio_codec_update` | Audio codec simulcast according to subscribers |
| 27–29 | `publish_data_track_response`, `unpublish_data_track_response`, `data_track_subscriber_handles` | Data-track handshake and handle-to-SID map |
| 30–31 | `store_data_blob_response`, `get_data_blob_response` | Data blob replies |

### Ping, leave, reconnect
- **Ping/pong.** `PingIntervalSeconds=5` and `PingTimeoutSeconds=15` (`livekit/pkg/rtc/participant.go:92`). Both are sent in JoinResponse. The server answers `ping` with `pong` and `ping_req` with `pong_resp` directly in the WS handler (`rtcservice.go`, the `SignalRequest_Ping`/`PingReq` cases).
- **Leave.** The client sends `leave` and the WS closes. The server sends `leave` with an `action`:
  - DISCONNECT: give up.
  - RESUME: reconnect with `reconnect=1&sid=…` and keep the session (ICE restart plus `sync_state`).
  - RECONNECT: do a full new join.
  - `regions` carries failover URLs (a Cloud feature; self-hosted has none).
- **Reconnect.** On resume the server replies with `ReconnectResponse` instead of `JoinResponse`. A client that drops the WS without leaving keeps its participant until `departure_timeout`. That value is the room-level default for a disconnected participant [unverified exact semantics].
- **Signal relay.** The signal node and the media node may differ. Messages are relayed over psrpc with the `signal_relay` config (retry_timeout 7.5s, connect_attempts 3).

### "v1 signalling", single PC, WHIP: what exists
- **Single PeerConnection mode: exists.** It is enabled by `/rtc/v1` with `join_request` (#3873, #4180) and supports join-time publish (`publisher_offer` and `add_track_requests` in JoinRequest). The server can also request m-sections (`media_sections_requirement`).
- **"Signal v2" (envelope/fragments wire format, #3794/#3800): no longer present.** There is no `Envelope`, `Fragment` or `ConnectRequest` message in the pinned protobufs, so it appears to have been dropped. Treat it as **[unverified/abandoned]**.
- **WHIP into the SFU: exists natively** at `/whip/v1` (one-shot signalling).
- **Experimental WARP:** `rtc.enable_warp` adds SPED (DTLS-in-STUN) and SNAP (SCTP INIT in SDP). It is off by default.

### Data channels (`livekit/pkg/rtc/transport.go:71`)
- `_reliable` and `_lossy` carry `DataPacket` protobuf.
- `_data_track` carries data-track frames.
- `DataPacket.value` oneof: `user`, `sip_dtmf`, `transcription`, `metrics`, `chat_message`, `rpc_request`, `rpc_ack`, `rpc_response`, `stream_header`, `stream_chunk`, `stream_trailer`, `encrypted_packet`, plus deprecated `speaker`. Envelope fields are `participant_identity`, `destination_identities`, `sequence` and `participant_sid`.

---

## 2. Auth (`protocol/auth/grants.go`, `accesstoken.go`, `verifier.go`)

**JWT format**
- Signed **HS256 only** with the API secret.
- `iss` = API key, `sub` = identity. `nbf`, `iat` and `exp` are set; `exp` is required, and verification allows some leeway.
- Default TTL is 6h.

**Top-level claims (`ClaimGrants`)**
| Claim | Contents |
|---|---|
| `name`, `kind`, `kindDetails[]` | Display name and participant kind |
| `video` | VideoGrant (below) |
| `sip` | `{admin, call}` |
| `agent` | `{admin, simulationAdmin, databaseAdmin, dispatchAdmin}`. Mostly Cloud. |
| `inference` | `{perform}`. Cloud. |
| `observability` | `{write}`. Cloud. |
| `agentEndpoint` | `{call, agentName, deployment}`. Can call non-public agent HTTP endpoints. |
| `agentThread` | `{all, scope, threadIds[], list, create, write, read, delete}`. Cloud [unverified]. |
| `roomConfig` | RoomConfiguration applied if this participant creates the room |
| `roomPreset` | Cloud-only |
| `sha256` | Body hash, used for webhooks |
| `metadata` | String |
| `attributes` | map |

**VideoGrant**

| Field | Meaning |
|---|---|
| roomCreate, roomList, roomRecord | Room-level API rights |
| roomAdmin, roomJoin, room | Scoped to one room |
| canPublish, canSubscribe, canPublishData | `*bool`. If none are set, all are granted. |
| canPublishSources[] | `camera`, `microphone`, `screen_share`, `screen_share_audio`. Supersedes canPublish. |
| canUpdateOwnMetadata | Default false |
| ingressAdmin | Ingress API |
| hidden | Invisible to other participants |
| recorder | Marks a recorder participant |
| agent | May register as an agent worker on `/agent` |
| canSubscribeMetrics | Receive client metrics |
| canManageAgentSession | RemoteSession |
| destinationRoom | Forward/Move target |

**Server-side checks (`livekit/pkg/service/auth.go`)**
- `EnsureJoinPermission`: roomJoin
- `EnsureAdminPermission`: roomAdmin **and** room equals the claimed room
- `EnsureCreatePermission`: roomCreate
- `EnsureListPermission`: roomList
- `EnsureRecordPermission`: roomRecord (egress)
- `EnsureIngressAdminPermission`: ingressAdmin
- `EnsureSIPAdminPermission`: sip.admin
- `EnsureSIPCallPermission`: sip.call
- `EnsureDestRoomPermission`: roomAdmin, room, and destinationRoom

**API keys**
- Configured in `keys: {key: secret}`, `key_file` (YAML; must have mode `o-rwx` = 0) or env `LIVEKIT_KEYS` (`livekit/pkg/service/wire.go` `createKeyProvider`, `cmd/server/main.go`).
- Secrets shorter than 32 chars log an error.
- `--dev` uses `devkey: secret`.

**Webhook signing (`protocol/webhook/url_notifier.go` `send`)**
- POST body is protojson(`WebhookEvent`) with `Content-Type: application/webhook+json` and `User-Agent: LiveKit`.
- `Authorization: <JWT>` is signed with `webhook.api_key`'s secret, valid for 5 min, with claim `sha256` = base64(SHA-256(body)).
- The receiver verifies with `webhook.Receive` (`protocol/webhook/verifier.go`).

---

## 3. Server APIs (Twirp)

**Transport.** Twirp over HTTP: `POST /twirp/livekit.<Svc>/<Rpc>` with protobuf or JSON body and `Authorization: Bearer`.
- The OSS server registers **RoomService, AgentDispatchService, Egress, Ingress, SIP** (`livekit/pkg/service/server.go:113-131`).
- Egress, Ingress and SIP RPCs are forwarded via psrpc/Redis to separate services. Their info is stored in Redis (`redisstore.go` keys `egress`, `ingress`, `{ingress}_stream_key`, …).

### RoomService (`livekit_room.proto`; impl. `livekit/pkg/service/roomservice.go`)

| RPC | Grant | Notes |
|---|---|---|
| CreateRoom | roomCreate | name, room_preset (cloud), empty_timeout, departure_timeout, max_participants, node_id, metadata, tags, egress (auto), min/max_playout_delay, sync_streams, replay_enabled (cloud), agents[] |
| ListRooms | roomList | |
| DeleteRoom | roomCreate | |
| ListParticipants | roomAdmin | |
| GetParticipant | roomAdmin | |
| RemoveParticipant | roomAdmin | |
| MutePublishedTrack | roomAdmin | Unmute only if `room.enable_remote_unmute` |
| UpdateParticipant | roomAdmin | metadata, permission, name, attributes |
| UpdateSubscriptions | roomAdmin | Server-side subscribe or unsubscribe for a participant |
| SendData | roomAdmin | bytes, kind, destination_identities, topic, nonce |
| UpdateRoomMetadata | roomAdmin | |
| ForwardParticipant | roomAdmin + destinationRoom | **Returns "not implemented" in OSS** (`roommanager.go:912`). Cloud-only. |
| MoveParticipant | roomAdmin + destinationRoom | **Not implemented in OSS** (`roommanager.go:916`). Cloud-only. |
| PerformRpc | roomAdmin | Server-to-participant RPC. Implemented (`participant.go:4659`). |

### AgentDispatchService (`livekit_agent_dispatch.proto`, `agent_dispatch_service.go`)
- RPCs: `CreateDispatch`, `DeleteDispatch`, `ListDispatch`.
- Request fields: agent_name, room, metadata, deployment, attributes. `restart_policy` is cloud-only.

### Egress (`livekit_egress.proto`, `egress.go`)
- **Legacy RPCs:** `StartRoomCompositeEgress`, `StartWebEgress`, `StartParticipantEgress`, `StartTrackCompositeEgress`, `StartTrackEgress`.
- **Unified RPC:** `StartEgress`, with source oneof template/web/media, encoding preset or advanced, outputs[], storage, webhooks.
- **Management:** `UpdateLayout`, `UpdateStream`, `ListEgress`, `StopEgress`.

### Ingress (`livekit_ingress.proto`, `ingress.go`)
- RPCs: `CreateIngress`, `UpdateIngress`, `ListIngress`, `DeleteIngress`.
- Input types: `RTMP_INPUT`, `WHIP_INPUT`, `URL_INPUT` (HTTP file or HLS).

### SIP (`livekit_sip.proto`, `sip.go`)
- Trunks:
  - `CreateSIPInboundTrunk`, `CreateSIPOutboundTrunk`
  - `UpdateSIPInboundTrunk`, `UpdateSIPOutboundTrunk`
  - `GetSIPInboundTrunk`, `GetSIPOutboundTrunk`
  - `ListSIPInboundTrunk`, `ListSIPOutboundTrunk`, `ListSIPTrunk` (deprecated)
  - `DeleteSIPTrunk`
- Dispatch rules: `CreateSIPDispatchRule`, `UpdateSIPDispatchRule`, `ListSIPDispatchRule`, `DeleteSIPDispatchRule`.
- Calls: `CreateSIPParticipant` (outbound call; sip.call), `TransferSIPParticipant` (SIP REFER).

### Cloud-only services (defined in protocol, not registered by the OSS server)
| Service | RPCs |
|---|---|
| **Connector** (`livekit_connector.proto`) | DialWhatsAppCall, DisconnectWhatsAppCall, ConnectWhatsAppCall, AcceptWhatsAppCall, ConnectTwilioCall |
| **PhoneNumberService** | Search, Purchase, List, Get, Update, ReleasePhoneNumbers |
| **CloudAgent** | Create/CreateV2/Promote/List/ListVersions/ListSecrets/Update/Restart/Deploy/DeployV2/UpdateSecrets/Rollback/DeleteAgent, GetClientSettings, Create/Destroy/List/GetStatus PrivateLink |
| **AgentSimulation** | CreateSimulationRun, ConfirmSimulationSourceUpload, Get/List/Count/CancelSimulationRun, CreateScenarioFromSession |
| **Replay** (`cloud_replay.proto`) | ListReplays, Playback, Seek, Close, Export, DeleteReplay |

- `livekit_agentdb.proto` and `livekit_token_source.proto` define messages only; there is no service in them.
- Internal psrpc services live under `protocol/protobufs/rpc/` and are not public.

---

## 4. Room / participant model (`livekit_models.proto`)

**Room**

| Field | Notes |
|---|---|
| sid, name | |
| empty_timeout | Default 300 s |
| departure_timeout | Default 20 s |
| max_participants | 0 means unlimited |
| creation_time(_ms) | |
| turn_password | |
| enabled_codecs | |
| metadata | String; limit 512 KiB by default |
| num_participants, num_publishers | |
| active_recording | |
| version | |

- **Defaults** come from `livekit/pkg/config/config.go` `DefaultConfig`. `room.auto_create` defaults to true.
- **Named room configurations:** `room.room_configurations` in the config file. A token's `roomConfig` applies these settings at create time.
- **RoomConfiguration** fields: name, empty/departure timeout, max_participants, metadata, egress, playout delay, sync_streams, agents[], tags.

**ParticipantInfo**

| Field | Notes |
|---|---|
| sid, identity, name | |
| state | JOINING, JOINED, ACTIVE, DISCONNECTED |
| tracks[], data_tracks[] | |
| metadata | String |
| attributes | `map<string,string>`; limit 64 KiB |
| joined_at(_ms) | |
| version | |
| permission | ParticipantPermission (below) |
| region | |
| is_publisher | |
| kind | STANDARD=0, INGRESS=1, EGRESS=2, SIP=3, AGENT=4, CONNECTOR=7, BRIDGE=8 |
| kind_details | CLOUD_AGENT, FORWARDED, CONNECTOR_WHATSAPP, CONNECTOR_TWILIO, BRIDGE_RTSP, SIMULATION |
| disconnect_reason | |
| client_protocol, capabilities | |

**ParticipantPermission**
- `can_subscribe`, `can_publish`, `can_publish_data`, `can_publish_sources[]`
- `hidden`, `recorder` (deprecated)
- `can_update_metadata`, `agent` (deprecated)
- `can_subscribe_metrics`, `can_manage_agent_session`
- Updatable live via `UpdateParticipant`.

**Tracks**
- TrackSource: UNKNOWN, CAMERA, MICROPHONE, SCREEN_SHARE, SCREEN_SHARE_AUDIO.
- TrackInfo carries `encryption` (NONE, GCM, CUSTOM), `codecs` (simulcast codecs), `backup_codec_policy` (PREFER_REGRESSION, SIMULCAST, REGRESSION), `audio_features`, `packet_trailer_features` (PTF_USER_TIMESTAMP, PTF_FRAME_ID, PTF_USER_DATA) and `stream` (grouping).

**Subscription permissions**
- The publisher sends `SubscriptionPermission{all_participants, track_permissions[{participant_identity|sid, all_tracks, track_sids}]}`.
- Subscribers get a `subscription_permission_update`.
- Data tracks are also permission-checked (#4588).

**Limits** (`limit:` config; defaults in `config.go`)

| Key | Default |
|---|---|
| max_metadata_size | 512 KiB |
| max_attributes_size | 64 KiB |
| max_room_name_length | 256 |
| max_participant_identity_length | 256 |
| max_participant_name_length | 256 |
| max_data_blob_key_length | 256 |
| max_data_blobs_size | 64000 |
| max_data_track_custom_encoding_length | 32 |
| signal_message_size_limit | 2 MiB |
| agent_signal_message_size_limit | 2 MiB |
| max_api_request_body_size | 10 MiB |

Other limit keys: `num_tracks`, `bytes_per_sec`, `subscription_limit_video`, `subscription_limit_audio`.

**RPC between participants**
- Relayed as DataPacket `rpc_request{id, method, payload, response_timeout_ms, version, compressed_payload}`, followed by `rpc_ack`, then `rpc_response`, all addressed via `destination_identities`.
- A server can also call into a participant via `RoomService.PerformRpc`.
- Payload size and timeout limits are enforced client-side, around 15 KiB per the docs **[unverified]**.

**Other data primitives**
- Data streams (text and byte, `DataStream.Header/Chunk/Trailer`, optional DEFLATE_RAW compression for clients with `CAP_COMPRESSION_DEFLATE_RAW`).
- Chat messages and transcriptions.
- Data tracks, on by default via `enable_data_tracks: true`; the low-latency lossy transport is `_data_track`.
- Data blobs: async attributes, off by default.

---

## 5. Webhooks (`protocol/webhook/consts.go`, `livekit_webhook.proto`)

| Event | Emitted by OSS server? |
|---|---|
| room_started | yes |
| room_finished | yes |
| participant_joined | yes |
| participant_left | yes |
| participant_connection_aborted | yes |
| track_published | yes |
| track_unpublished | yes |
| egress_started | yes |
| egress_updated | yes |
| egress_ended | yes |
| ingress_started | yes |
| ingress_ended | yes |
| agent_job_started | **no**. Defined, but no emitter found in `livekit/pkg`, so it is likely Cloud. |
| agent_job_ended | **no**. Same as above. |

- **Payload:** `WebhookEvent{event, room, participant, track, egress_info, ingress_info, job, id, created_at, room_end_reason}`.
- **Config:** `webhook: {api_key, urls[], url_notifier{num_workers, queue_size}, resource_url_notifier, filter_params{include_events, exclude_events}}`. Egress requests can also carry per-request webhooks.

---

## 6. Separate services

**Egress** (`livekit/egress`, Apache-2.0, separate Go service)
- Request types:
  - RoomComposite: headless **Chrome** renders a layout template and **GStreamer** encodes it.
  - Web: any URL, rendered by Chrome.
  - Participant: one participant's audio and video.
  - TrackComposite: one audio plus one video track.
  - Track: one track, no transcode.
  - The unified `StartEgress` uses template, web or media sources.
- Outputs:
  - file: MP4/OGG/MP3
  - stream: RTMP/SRT/WEBSOCKET
  - segments: HLS
  - images: thumbnails with index, timestamp or overwrite suffix
- Upload targets: S3, GCP, Azure.
- **A self-hoster runs** `livekit/egress` sharing the server's **Redis** (required), with ≥4 CPU / 4 GB per instance, and `--cap-add=SYS_ADMIN` for Chrome. One room composite per instance. **[docs]**
- Statuses: STARTING, ACTIVE, ENDING, COMPLETE, FAILED, ABORTED, LIMIT_REACHED.
- Auto-egress is possible via `RoomConfiguration.egress`.

**Ingress** (`livekit/ingress`, Apache-2.0)
- Pulls or receives media and publishes it as an `INGRESS`-kind participant.
- Inputs:
  - RTMP/RTMPS: TCP 1935
  - WHIP: HTTP 8080 plus UDP 7885
  - URL pull: HTTP file or HLS
- Uses a **GStreamer** pipeline to transcode into simulcast. WHIP can bypass transcoding.
- **A self-hoster runs** `livekit/ingress` plus the same Redis, ≥4 CPU / 4 GB. Set `ingress.rtmp_base_url` and `ingress.whip_base_url` in livekit.yaml. **[docs]**
- This is distinct from the server's native `/whip/v1`.

**SIP** (`livekit/sip`, Apache-2.0)
- A SIP UA/B2BUA bridging phone calls to rooms as `SIP`-kind participants.
- Inbound trunks: allowed numbers and addresses, auth, krisp flag.
- Outbound trunks: provider address, transport, auth.
- Dispatch rules decide the room for inbound calls:
  - Direct: fixed room
  - Individual: new room per caller, prefix
  - Callee: room by dialled number
- Supports DTMF (`sip_dtmf` data packets), REFER transfer and outbound calls (`CreateSIPParticipant`).
- **A self-hoster runs** `livekit/sip` plus Redis, with UDP/TCP 5060 and an RTP range of 10000-20000 open to the internet. **[docs]**
- Phone-number purchase and WhatsApp/Twilio connectors are Cloud-only.

**Agents** (`livekit/agents` Python and `agents-js`, Apache-2.0)
- Workers open a WS to the server's `/agent` endpoint.
  - The token needs `video.agent=true`.
  - Agent protocol `CurrentProtocol=1` (`livekit/pkg/agent/worker.go:52`).
- `WorkerMessage`: register, availability, update_worker, update_job, ping, simulate_job, migrate_job.
- `ServerMessage`: register, availability (request), assignment, termination, pong, go_away.
- Job types: JT_ROOM, JT_PUBLISHER, JT_PARTICIPANT.
- **The OSS server includes** the worker registry and load-based dispatch (`pkg/agent`, `agents.target_load`):
  - automatic dispatch when an agent has no name
  - explicit dispatch via `AgentDispatchService` or the token's `roomConfig.agents`
  - agent HTTP endpoints (`/agents/...`)
- **A self-hoster runs** their own worker processes. Those call their own STT/LLM/TTS providers.
- Cloud-only: Cloud Agents hosting, LiveKit Inference, restart_policy, simulations, observability.

---

## 7. Operational

**Single node vs multi-node**
- Without `redis`, the server uses a LocalRouter and in-memory stores.
- With `redis` (address, sentinel or cluster options), it uses RedisRouter (`livekit/pkg/routing/redisrouter.go`):
  - Redis hash `nodes` holds node stats and keep-alives.
  - Redis hash `room_node_map` maps room to node.
  - A room lives on **one** node.
  - The signal connection may land on any node and is relayed via **psrpc over Redis pub/sub** (`wire.go` `getMessageBus`: `redisbus` or local).
- Egress, ingress, SIP and agent dispatch also use psrpc over the same Redis.

**Node selection** (`node_selector` config; `livekit/pkg/routing/selector/`)

| Setting | Values |
|---|---|
| `kind` | `any` (default), `cpuload`, `sysload`, `regionaware`, `random` |
| `sort_by` | `random` (default), `sysload`, `cpuload`, `rooms`, `clients`, `tracks`, `bytespersec` |
| `algorithm` | `lowest` (default), `twochoice` |
| `cpu_load_limit` | 0.9 |
| `sysload_limit` | 0.9 |
| `regions[]` | `{name, lat, lon}`, for `regionaware` |

- `region` is set by the config key or `LIVEKIT_REGION`.

**Prometheus**
- Enabled with `prometheus: {port, username, password}`; basic auth needs both. `prometheus_port` is deprecated.
- Metrics are under the `livekit_` namespace (`livekit/pkg/telemetry/prometheus/*.go`, parsed from source):
  - **node:** node_messages, node_packet_total, node_service_operation, node_twirp_request_status, node_twirp_request_latency_ms
  - **rooms and participants:** room_total, room_duration_seconds, participant_total, participant_join_total
  - **tracks:** track_published_total, track_subscribed_total, track_publish_counter, track_subscribe_counter
  - **sessions and connections:** session_start_time_ms, session_join_latency_ms, session_duration_ms, connection_total, peer_connection_state
  - **packets:** packet_total, packet_bytes, packet_loss_total, packet_loss_percent, packet_out_of_order_total, packet_out_of_order_percent
  - **RTCP and network:** nack_total, pli_total, fir_total, rtt_ms, jitter_us, forward_latency(_ns)
  - **quality:** quality_score, quality_rating
  - **other:** datapacket_stream_bytes, datapacket_stream_dest_count, pubsubtime_ms, debug_ref_count
  - Plus Go and process collectors.

**Config file shape** (top-level YAML keys, `livekit/pkg/config/config.go` `Config`)

| Key | Purpose / notable sub-keys |
|---|---|
| port | 7880 |
| bind_addresses | |
| prometheus | |
| debug_handler | |
| webtransport | port, tls_cert_file, tls_key_file |
| rtc | See the RTC table below |
| redis | |
| audio | |
| video | dynacast_pause_delay 5s, … |
| room | auto_create, enabled_codecs (PCMU, PCMA, opus, RED, VP8, H264, VP9, AV1, H265, RTX), max_participants, empty_timeout, departure_timeout, enable_remote_unmute, playout_delay, sync_streams, create_room_timeout/attempts, max_metadata_size, room_configurations, … |
| turn | See the TURN table below |
| ingress | rtmp_base_url, whip_base_url, enable_udp_url_pull |
| sip | |
| webhook | |
| node_selector | |
| key_file, keys | |
| region | |
| signal_relay | |
| psrpc | |
| log_level, logging | |
| limit | |
| agents | |
| development | |
| metric | |
| trace | |
| node_stats | |
| enable_data_tracks | Default true |
| enable_participant_data_blob | |
| api | execution_timeout, check_interval, enable_psrpc_for_get_list_participants |

**`rtc` keys** (from livekit `config.go` and `mtu/rtcconfig.go`)

| Group | Keys |
|---|---|
| Ports | udp_port, tcp_port (7881), port_range_start, port_range_end, use_stun_port_as_ice |
| IP / external address | node_ip, stun_servers, use_external_ip, external_ip_only, require_ipv4, skip_external_ip_validation, advertise_internal_ip |
| ICE / candidates | use_ice_lite, interfaces, ips (includes/excludes), enable_loopback_candidate, use_mdns, force_tcp, *_acceptance_min_wait |
| SCTP / batching | batch_io, sctp_* |
| Server-side TURN advertising | turn_servers |
| Experimental | enable_warp |
| Packet buffers | packet_buffer_size_video/audio |
| Congestion control and RTCP | pli_throttle, congestion_control |
| TCP / UDP fallback | allow_tcp_fallback, tcp_fallback_rtt_threshold, allow_udp_unstable_fallback |
| Forced reconnects | reconnect_on_publication/subscription/data_channel_error |
| Data channels | datachannel_slow_threshold, datachannel_lossy_target_latency, datachannel_data_track_target_latency |
| Other | forward_stats, enable_rtp_stream_restart_detection |

**TURN**
- Built-in pion TURN server, keys under `turn:`: enabled, domain, cert_file/key_file, tls_port, udp_port, relay_range_start/end, external_tls, bind_addresses, proxy_protocol(+trusted_cidrs), per_user_relay_allocation_limit, ttl_seconds, allow_restricted_peer_cidrs, deny_peer_cidrs.
- Credentials are TTL-based. Non-TTL auth was removed in 1.13.0.
- External TURN can be configured via `rtc.turn_servers`, including static-auth-secret (#3796).
- Clients receive the ICE servers in `JoinResponse.ice_servers`.

**Key provider**
- File/map provider only (`auth.NewFileBasedKeyProviderFromMap`).
- Sources: `keys`, `key_file` or `LIVEKIT_KEYS`.
- Multiple keys are allowed. The webhook key must be one of them.

**Environment variables**
- `LIVEKIT_CONFIG`, `LIVEKIT_KEYS`, `LIVEKIT_REGION`, `NODE_IP`, `UDP_PORT`, `REDIS_HOST`, `REDIS_PASSWORD`, `LIVEKIT_TURN_CERT`, `LIVEKIT_TURN_KEY` (`cmd/server/main.go`).

---

## 8. Client SDK features that need server support

| Feature | Server side | Self-host? |
|---|---|---|
| Adaptive stream | `adaptive_stream` flag plus `UpdateTrackSettings` (dims, fps, disabled); the SFU picks layers | yes |
| Dynacast | `SubscribedQualityUpdate` and `SubscribedAudioCodecUpdate`; `video.dynacast_pause_delay` | yes |
| Simulcast / SVC / backup codec | `AddTrackRequest.simulcast_codecs`, `layers`, `backup_codec_policy`; codec regression | yes |
| Congestion control and stream pause | `StreamStateUpdate`; `rtc.congestion_control.allow_pause`, `subscriber_allow_pause` | yes |
| Connection quality, active speakers | `ConnectionQualityUpdate`, `SpeakersChanged` | yes |
| E2EE | The server only relays `Encryption.Type` (GCM/CUSTOM) on tracks, data tracks and `encrypted_packet`. **Keys are never distributed by the server**: the app distributes a shared or per-participant key. **[docs]** | yes |
| Data channel encryption | Client `RoomOptions.encryption` produces `DataPacket.encrypted_packet` | yes |
| RPC, data streams, chat | Relayed DataPacket variants; `PerformRpc` API | yes |
| Data tracks | Signal 19–21/27–29 plus the `_data_track` DC; `enable_data_tracks` | yes (default on) |
| Data blobs (async attributes) | Signal 22/23, 30/31; `enable_participant_data_blob` | yes (opt-in) |
| Participant attributes and metadata self-update | `UpdateParticipantMetadata`; `canUpdateOwnMetadata` | yes |
| Preconnect audio buffer | `TF_PRECONNECT_BUFFER` audio feature; audio is sent via byte stream to the agent | yes |
| Packet trailer (user timestamp, frame id, user data) | `PacketTrailerFeature`, protocol >16, `CAP_PACKET_TRAILER`, `JoinResponse.sif_trailer` | yes |
| Single-PC and fast publish | `/rtc/v1` join_request; `JoinResponse.fast_publish` | yes |
| Token refresh | `refresh_token` signal response | yes |
| Resume and migration | `reconnect=1`, SyncState, LeaveRequest.action | Resume: yes. Cross-node migration needs a multi-node setup. |
| Region failover | `LeaveRequest.regions`, `RegionSettings` | Cloud (no regions list in OSS) [unverified] |
| Krisp enhanced noise cancellation (`TF_ENHANCED_NOISE_CANCELLATION`) | "available when you use LiveKit Cloud for realtime transport" | **Cloud-only** [docs]. ai-coustics can be self-hosted with your own licence. |
| Move/Forward participant (`room_moved`) | RoomService Move/Forward | **Cloud-only**: OSS returns "not implemented" |
| Client metrics (`MetricsBatch`, `canSubscribeMetrics`) | Relayed DataPacket | Partly; analytics are Cloud [unverified] |
