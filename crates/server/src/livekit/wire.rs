//! Framing for `/rtc` and `/agent`: a binary WebSocket message is a
//! protobuf (`SignalRequest`/`SignalResponse`, `WorkerMessage`/
//! `ServerMessage`), a text one is protojson. Like LiveKit
//! (`pkg/service/wsprotocol.go`), replies use whichever form the client
//! last sent.

use axum::extract::ws::Message;
use serde::Serialize;
use serde::de::DeserializeOwned;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Format {
    #[default]
    Protobuf,
    Json,
}

/// Decodes one client message (`what` names it in errors). `Ok(None)` for
/// frames that carry no request (ping/pong/close handled by the caller).
pub fn decode<T: prost::Message + Default + DeserializeOwned>(
    msg: &Message,
    what: &str,
) -> Result<Option<(T, Format)>, String> {
    match msg {
        Message::Binary(bytes) => T::decode(bytes.as_ref())
            .map(|r| Some((r, Format::Protobuf)))
            .map_err(|e| format!("bad protobuf {what}: {e}")),
        Message::Text(text) => serde_json::from_str(text.as_str())
            .map(|r| Some((r, Format::Json)))
            .map_err(|e| format!("bad JSON {what}: {e}")),
        _ => Ok(None),
    }
}

pub fn encode<T: prost::Message + Serialize>(resp: &T, format: Format) -> Message {
    match format {
        Format::Protobuf => Message::Binary(resp.encode_to_vec().into()),
        Format::Json => Message::Text(serde_json::to_string(resp).expect("serialises").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use livekit_protocol::{SignalRequest, SignalResponse, signal_request, signal_response};
    use prost::Message as _;

    /// A request exactly as `lk load-test` sent it to LiveKit, as protojson.
    #[test]
    fn decodes_captured_json_requests() {
        let text = r#"{"addTrack":{"cid":"TR_eQT7m7fJ7HWe","name":"audio","type":"AUDIO","source":"MICROPHONE","encryption":"NONE","backupCodecPolicy":"PREFER_REGRESSION"}}"#;
        let (req, format) = decode::<SignalRequest>(&Message::Text(text.into()), "signal request")
            .unwrap()
            .unwrap();
        assert_eq!(format, Format::Json);
        let Some(signal_request::Message::AddTrack(add)) = req.message else {
            panic!("{req:?}");
        };
        assert_eq!(add.cid, "TR_eQT7m7fJ7HWe");
        assert_eq!(add.source(), livekit_protocol::TrackSource::Microphone);
    }

    #[test]
    fn replies_in_the_clients_format() {
        let resp = SignalResponse {
            message: Some(signal_response::Message::RefreshToken("t".into())),
        };
        let Message::Text(json) = encode(&resp, Format::Json) else {
            panic!()
        };
        assert_eq!(json.as_str(), r#"{"refreshToken":"t"}"#);
        let Message::Binary(bin) = encode(&resp, Format::Protobuf) else {
            panic!()
        };
        assert_eq!(SignalResponse::decode(bin.as_ref()).unwrap(), resp);

        let req = SignalRequest {
            message: Some(signal_request::Message::PingReq(livekit_protocol::Ping {
                timestamp: 5,
                rtt: 0,
            })),
        };
        let (decoded, format) = decode(
            &Message::Binary(req.encode_to_vec().into()),
            "signal request",
        )
        .unwrap()
        .unwrap();
        assert_eq!((decoded, format), (req, Format::Protobuf));
    }
}
