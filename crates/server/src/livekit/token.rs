//! LiveKit access tokens: HS256 JWTs signed with an API secret, carrying the
//! same claims as LiveKit's `protocol/auth` package, so tokens minted by any
//! LiveKit server SDK (or `lk token create`) work here unchanged.
//!
//! `iss` is the API key, `sub` the participant identity. The grants decide
//! what the bearer may do; unset `canPublish`/`canSubscribe` mean yes, as in
//! LiveKit.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, KeyInit, Mac};
use livekit_protocol::{RoomConfiguration, TrackSource};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::util::secs_at;

/// Clock skew tolerated on `exp` and `nbf`, as LiveKit's verifier.
const LEEWAY: Duration = Duration::from_secs(60);

/// Below this, LiveKit logs that a secret is too weak to be safe.
pub const MIN_SECRET_LEN: usize = 32;

/// API key → secret, from `LIVEKIT_KEYS` (`key: secret` per line, as
/// LiveKit's YAML) or a key file in the same format.
#[derive(Clone, Default)]
pub struct Keys(HashMap<String, String>);

impl Keys {
    pub fn parse(text: &str) -> Result<Keys, String> {
        let mut keys = HashMap::new();
        for line in text.lines().flat_map(|l| l.split(',')) {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, secret) = line
                .split_once(':')
                .ok_or_else(|| format!("expected `key: secret`, got {line:?}"))?;
            let (key, secret) = (unquote(key.trim()), unquote(secret.trim()));
            if key.is_empty() || secret.is_empty() {
                return Err(format!("empty key or secret in {line:?}"));
            }
            keys.insert(key.to_owned(), secret.to_owned());
        }
        Ok(Keys(keys))
    }

    pub fn secret(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, s)| (k.as_str(), s.as_str()))
    }

    /// Any one key, for minting tokens on the server's own behalf.
    pub fn any(&self) -> Option<(&str, &str)> {
        self.iter().min_by_key(|(k, _)| *k)
    }
}

fn unquote(s: &str) -> &str {
    s.strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .or_else(|| s.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
        .unwrap_or(s)
}

/// Everything in a token besides the registered claims. Field names follow
/// LiveKit's `ClaimGrants` JSON.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimGrants {
    /// Filled from `sub` (or the legacy `jti`) when verifying.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Participant kind: `standard`, `ingress`, `egress`, `sip`, `agent`...
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video: Option<VideoGrant>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sip: Option<SipGrant>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<AgentGrant>,
    /// Applied when this participant's join creates the room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room_config: Option<RoomConfiguration>,
    /// Webhook body hash, base64 SHA-256.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sha256: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub metadata: String,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VideoGrant {
    #[serde(default, skip_serializing_if = "is_false")]
    pub room_create: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub room_list: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub room_record: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub room_admin: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub room_join: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub room: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub can_publish: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub can_subscribe: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub can_publish_data: Option<bool>,
    /// `camera`, `microphone`, `screen_share`, `screen_share_audio`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub can_publish_sources: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub can_update_own_metadata: Option<bool>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub ingress_admin: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub hidden: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub recorder: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub agent: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub can_subscribe_metrics: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub can_manage_agent_session: Option<bool>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub destination_room: String,
}

impl VideoGrant {
    pub fn can_publish(&self) -> bool {
        self.can_publish.unwrap_or(true)
    }

    pub fn can_publish_source(&self, source: TrackSource) -> bool {
        self.can_publish()
            && (self.can_publish_sources.is_empty()
                || self
                    .can_publish_sources
                    .iter()
                    .any(|s| s.eq_ignore_ascii_case(source.as_str_name())))
    }

    pub fn can_publish_data(&self) -> bool {
        self.can_publish_data.unwrap_or_else(|| self.can_publish())
    }

    pub fn can_subscribe(&self) -> bool {
        self.can_subscribe.unwrap_or(true)
    }

    pub fn can_update_own_metadata(&self) -> bool {
        self.can_update_own_metadata.unwrap_or(false)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SipGrant {
    #[serde(default, skip_serializing_if = "is_false")]
    pub admin: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub call: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentGrant {
    #[serde(default, skip_serializing_if = "is_false")]
    pub admin: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub dispatch_admin: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    iss: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    sub: String,
    /// Older SDKs put the identity here instead of `sub`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    jti: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    nbf: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    iat: Option<u64>,
    #[serde(default)]
    exp: Option<u64>,
    #[serde(flatten)]
    grants: ClaimGrants,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenError {
    Malformed,
    UnknownKey,
    BadSignature,
    Expired,
    NotYetValid,
    /// A token without `exp` would never expire; LiveKit refuses those too.
    NoExpiry,
}

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TokenError::Malformed => "malformed access token",
            TokenError::UnknownKey => "unknown API key",
            TokenError::BadSignature => "invalid access token signature",
            TokenError::Expired => "access token expired",
            TokenError::NotYetValid => "access token not valid yet",
            TokenError::NoExpiry => "access token has no expiry",
        })
    }
}

/// Checks a token's signature against the secret of the API key it names,
/// and its validity window. Returns the API key and the grants, with
/// `identity` filled in.
pub fn verify(
    token: &str,
    keys: &Keys,
    now: SystemTime,
) -> Result<(String, ClaimGrants), TokenError> {
    let mut parts = token.split('.');
    let (Some(header), Some(payload), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(TokenError::Malformed);
    };

    #[derive(Deserialize)]
    struct Header {
        alg: String,
    }
    let header: Header = decode_json(header)?;
    if header.alg != "HS256" {
        return Err(TokenError::Malformed);
    }
    let claims: Claims = decode_json(payload)?;
    let secret = keys.secret(&claims.iss).ok_or(TokenError::UnknownKey)?;

    let signature = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| TokenError::Malformed)?;
    let signed = &token[..header_len(token)];
    let mut mac = hmac(secret);
    mac.update(signed.as_bytes());
    mac.verify_slice(&signature)
        .map_err(|_| TokenError::BadSignature)?;

    let now = secs_at(now);
    let leeway = LEEWAY.as_secs();
    let exp = claims.exp.ok_or(TokenError::NoExpiry)?;
    if now > exp + leeway {
        return Err(TokenError::Expired);
    }
    if claims.nbf.is_some_and(|nbf| now + leeway < nbf) {
        return Err(TokenError::NotYetValid);
    }

    let mut grants = claims.grants;
    grants.identity = if claims.sub.is_empty() {
        claims.jti
    } else {
        claims.sub
    };
    Ok((claims.iss, grants))
}

/// Mints a token as LiveKit's server SDKs do.
pub fn sign(
    key: &str,
    secret: &str,
    grants: &ClaimGrants,
    ttl: Duration,
    now: SystemTime,
) -> String {
    let now = secs_at(now);
    let claims = Claims {
        iss: key.to_owned(),
        sub: grants.identity.clone(),
        jti: String::new(),
        nbf: Some(now),
        iat: None,
        exp: Some(now + ttl.as_secs()),
        grants: ClaimGrants {
            identity: String::new(),
            ..grants.clone()
        },
    };
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).expect("claims serialise"));
    let signed = format!("{header}.{payload}");
    let mut mac = hmac(secret);
    mac.update(signed.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{signed}.{signature}")
}

fn hmac(secret: &str) -> Hmac<Sha256> {
    Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC takes any key length")
}

/// Length of `header.payload`, the part the signature covers.
fn header_len(token: &str) -> usize {
    token.rfind('.').expect("checked three parts")
}

fn decode_json<T: serde::de::DeserializeOwned>(part: &str) -> Result<T, TokenError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(part)
        .map_err(|_| TokenError::Malformed)?;
    serde_json::from_slice(&bytes).map_err(|_| TokenError::Malformed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> Keys {
        Keys::parse("devkey: secret\nother: \"0123456789abcdef0123456789abcdef\"").unwrap()
    }

    fn join_grants() -> ClaimGrants {
        ClaimGrants {
            identity: "alice".into(),
            name: "Alice".into(),
            video: Some(VideoGrant {
                room_join: true,
                room: "lobby".into(),
                ..VideoGrant::default()
            }),
            ..ClaimGrants::default()
        }
    }

    #[test]
    fn parses_key_lines_and_commas() {
        let keys = keys();
        assert_eq!(keys.secret("devkey"), Some("secret"));
        assert_eq!(keys.secret("other").map(str::len), Some(32));
        assert!(Keys::parse("a: b, c: d").unwrap().secret("c").is_some());
        assert!(Keys::parse("nocolon").is_err());
    }

    #[test]
    fn round_trips_and_checks_signature() {
        let now = SystemTime::now();
        let token = sign(
            "devkey",
            "secret",
            &join_grants(),
            Duration::from_secs(6 * 60 * 60),
            now,
        );
        let (key, grants) = verify(&token, &keys(), now).unwrap();
        assert_eq!(key, "devkey");
        assert_eq!(grants, join_grants());

        let mut tampered = token.clone();
        tampered.insert(token.find('.').unwrap() + 2, 'x');
        assert!(verify(&tampered, &keys(), now).is_err());
        assert_eq!(
            verify(&token, &Keys::parse("devkey: wrong").unwrap(), now),
            Err(TokenError::BadSignature)
        );
        assert_eq!(
            verify(&token, &Keys::parse("someone: secret").unwrap(), now),
            Err(TokenError::UnknownKey)
        );
    }

    #[test]
    fn enforces_the_validity_window_with_leeway() {
        let now = SystemTime::now();
        let token = sign(
            "devkey",
            "secret",
            &join_grants(),
            Duration::from_secs(60),
            now,
        );
        assert!(verify(&token, &keys(), now + Duration::from_secs(100)).is_ok());
        assert_eq!(
            verify(&token, &keys(), now + Duration::from_secs(200)),
            Err(TokenError::Expired)
        );
        assert_eq!(
            verify(&token, &keys(), now - Duration::from_secs(120)),
            Err(TokenError::NotYetValid)
        );
    }

    /// A token as LiveKit's Go SDK mints it (`lk token create --join
    /// --room lobby --identity bob --valid-for 87600h`, secret `secret`),
    /// to pin the claim names.
    #[test]
    fn reads_a_livekit_minted_token() {
        let payload = serde_json::json!({
            "iss": "devkey", "sub": "bob", "nbf": 1_700_000_000u64,
            "exp": 4_000_000_000u64, "name": "Bob",
            "video": {"roomJoin": true, "room": "lobby", "canPublishSources": ["microphone"]},
            "metadata": "m", "attributes": {"k": "v"},
            "roomConfig": {"name": "lobby", "emptyTimeout": 30},
        });
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let body = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap());
        let mut mac = hmac("secret");
        mac.update(format!("{header}.{body}").as_bytes());
        let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        let token = format!("{header}.{body}.{sig}");

        let (_, grants) = verify(&token, &keys(), SystemTime::now()).unwrap();
        assert_eq!(grants.identity, "bob");
        let video = grants.video.unwrap();
        assert!(video.room_join && video.room == "lobby");
        assert!(video.can_publish_source(TrackSource::Microphone));
        assert!(!video.can_publish_source(TrackSource::Camera));
        assert!(video.can_subscribe() && video.can_publish_data());
        assert_eq!(grants.attributes["k"], "v");
        assert_eq!(grants.room_config.unwrap().empty_timeout, 30);
    }

    #[test]
    fn rejects_tokens_without_expiry_or_with_other_algorithms() {
        let enc = |v: serde_json::Value| URL_SAFE_NO_PAD.encode(serde_json::to_vec(&v).unwrap());
        let body = enc(serde_json::json!({"iss": "devkey", "sub": "x"}));
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256"}"#);
        let mut mac = hmac("secret");
        mac.update(format!("{header}.{body}").as_bytes());
        let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        assert_eq!(
            verify(
                &format!("{header}.{body}.{sig}"),
                &keys(),
                SystemTime::now()
            ),
            Err(TokenError::NoExpiry)
        );
        let none = URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
        assert_eq!(
            verify(&format!("{none}.{body}."), &keys(), SystemTime::now()),
            Err(TokenError::Malformed)
        );
    }
}
