//! The bodies of natter's own `/api` endpoints (accounts, invites, room
//! tokens), serialised as JSON. Calls themselves speak LiveKit's protocol
//! (`livekit-protocol`) over `/rtc` and `/rtc/v1`.

use serde::{Deserialize, Serialize};

/// The signed-in account. `GET /api/me`, and the result of every ceremony.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Me {
    pub id: String,
    pub name: String,
}

/// `POST /api/register/start`. A new account needs `invite` and `name`; a
/// signed-in user sends neither to add a passkey on another device.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegisterStart {
    pub invite: Option<String>,
    pub name: Option<String>,
}

/// `POST /api/login/start`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginStart {
    pub name: String,
}

/// Reply to either `start`: WebAuthn options to hand to the browser, plus
/// the id the matching `finish` must quote.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CeremonyStart {
    pub ceremony: String,
    pub options: serde_json::Value,
}

/// `POST /api/register/finish` and `/api/login/finish`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CeremonyFinish {
    pub ceremony: String,
    pub credential: serde_json::Value,
}

/// `POST /api/invites`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InviteCreated {
    pub url: String,
}

/// Body of every non-2xx `/api` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiError {
    pub error: String,
}

/// `POST /api/rtc/token`: a LiveKit access token for a room, so the client
/// can join over `/rtc/v1`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RtcTokenRequest {
    pub room: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RtcToken {
    pub token: String,
    /// natter's TURN servers, with credentials, for the peer connection
    /// the client builds before it joins.
    #[serde(default)]
    pub ice_servers: Vec<IceServer>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IceServer {
    pub urls: Vec<String>,
    pub username: String,
    pub credential: String,
}
