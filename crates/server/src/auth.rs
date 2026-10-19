//! Passkey accounts: invite-only signup, passkey login, cookie sessions.
//!
//! Each WebAuthn ceremony is two requests. `start` returns options for the
//! browser plus a ceremony id; the server keeps the challenge state in memory
//! under that id until `finish` presents the signed result.
//!
//! Handlers do their database and signature work on the blocking pool, via
//! [`blocking`], so redb's fsyncs never stall the async workers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::extract::{FromRef, FromRequestParts, State};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_extra::extract::cookie::{Cookie, CookieJar, SameSite};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use natter_protocol::{
    ApiError, CeremonyFinish, CeremonyStart, InviteCreated, LoginStart, Me, RegisterStart,
};
use tracing::{error, info, warn};
use url::Url;
use uuid::Uuid;
use webauthn_rs::prelude::{
    PasskeyAuthentication, PasskeyRegistration, PublicKeyCredential, RegisterPublicKeyCredential,
    Webauthn, WebauthnBuilder,
};

use crate::store::{self, CreateUserError, Store, User};

const SESSION_COOKIE: &str = "natter_session";
const CEREMONY_TTL: Duration = Duration::from_secs(5 * 60);
const MAX_NAME_LEN: usize = 32;
/// Ceremonies in flight across all clients. `login/start` needs no account,
/// so this bounds the memory anyone who can reach the server can use.
const MAX_PENDING: usize = 1024;
/// Unused invites a user may have at once.
pub const MAX_OPEN_INVITES: usize = 10;

pub struct Auth {
    webauthn: Webauthn,
    pub store: Store,
    origin: Url,
    pending: Mutex<HashMap<String, (Instant, Pending)>>,
}

enum Pending {
    Register {
        user_id: Uuid,
        state: PasskeyRegistration,
        signup: Option<Signup>,
    },
    Login {
        user_id: Uuid,
        state: PasskeyAuthentication,
    },
    /// A sign-in for a name with no account. It can never succeed.
    Decoy,
}

/// Present when a registration creates a new account rather than adding a
/// passkey to the signed-in one.
struct Signup {
    name: String,
    invite: String,
}

/// The account behind the request's session cookie. Rejects with 401.
pub struct AuthUser {
    pub id: Uuid,
    pub name: String,
}

impl Auth {
    pub fn new(origin: Url, store: Store) -> Result<Self> {
        let rp_id = origin.host_str().context("origin has no host")?.to_owned();
        let webauthn = WebauthnBuilder::new(&rp_id, &origin)?
            .rp_name("natter")
            .build()?;
        Ok(Auth {
            webauthn,
            store,
            origin,
            pending: Mutex::default(),
        })
    }

    /// `scheme://host[:port]`, as browsers send it in the Origin header.
    pub fn origin(&self) -> String {
        self.origin.origin().ascii_serialization()
    }

    pub fn invite_url(&self, token: &str) -> String {
        format!("{}/#invite={token}", self.origin())
    }

    fn signed_in(&self, jar: &CookieJar) -> Result<Option<(User, AuthUser)>> {
        let Some(token) = jar.get(SESSION_COOKIE) else {
            return Ok(None);
        };
        let hash = store::token_hash(token.value());
        let Some(session) = self.store.session(&hash)? else {
            return Ok(None);
        };
        Ok(self.store.user(session.user_id)?.map(|user| {
            let auth_user = AuthUser {
                id: user.id,
                name: user.name.clone(),
            };
            (user, auth_user)
        }))
    }

    fn session_cookie(&self, token: String) -> Cookie<'static> {
        Cookie::build((SESSION_COOKIE, token))
            .http_only(true)
            .secure(self.origin.scheme() == "https")
            .same_site(SameSite::Strict)
            .path("/")
            // The server enforces the real expiry; this only keeps the
            // cookie from vanishing when the browser closes.
            .permanent()
            .build()
    }

    fn insert_pending(&self, pending: Pending) -> Result<String, ApiErr> {
        let mut map = self.pending.lock().expect("pending lock");
        map.retain(|_, (at, _)| at.elapsed() < CEREMONY_TTL);
        if map.len() >= MAX_PENDING {
            warn!("too many passkey ceremonies in flight");
            return Err(ApiErr::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "too many sign-ins in progress; try again in a minute",
            ));
        }
        let id = store::random_token();
        map.insert(id.clone(), (Instant::now(), pending));
        Ok(id)
    }

    fn take_pending(&self, id: &str) -> Option<Pending> {
        let (at, pending) = self.pending.lock().expect("pending lock").remove(id)?;
        (at.elapsed() < CEREMONY_TTL).then_some(pending)
    }
}

/// Runs `f` on the blocking pool.
pub async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, ApiErr> + Send + 'static,
) -> Result<T, ApiErr> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| internal(e.into()))?
}

impl<S> FromRequestParts<S> for AuthUser
where
    Arc<Auth>: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = ApiErr;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let auth = Arc::<Auth>::from_ref(state);
        let jar = CookieJar::from_headers(&parts.headers);
        blocking(move || match auth.signed_in(&jar)? {
            Some((_, user)) => Ok(user),
            None => Err(ApiErr::new(StatusCode::UNAUTHORIZED, "not signed in")),
        })
        .await
    }
}

pub fn routes<S>() -> Router<S>
where
    Arc<Auth>: FromRef<S>,
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/api/me", get(me))
        .route("/api/register/start", post(register_start))
        .route("/api/register/finish", post(register_finish))
        .route("/api/login/start", post(login_start))
        .route("/api/login/finish", post(login_finish))
        .route("/api/logout", post(logout))
        .route("/api/invites", post(create_invite))
}

async fn me(user: AuthUser) -> Json<Me> {
    Json(Me {
        id: user.id.to_string(),
        name: user.name,
    })
}

/// Normalises a requested name and checks it is one people can tell apart.
fn clean_name(raw: &str) -> Result<String, ApiErr> {
    let name = store::display_name(raw);
    if name.is_empty() || name.chars().count() > MAX_NAME_LEN {
        return Err(bad(&format!("name must be 1–{MAX_NAME_LEN} characters")));
    }
    if name.chars().any(invisible) {
        return Err(bad("names can't contain invisible or control characters"));
    }
    Ok(name)
}

/// Control characters plus Unicode's default-ignorable code points: zero
/// width spaces and joiners, bidi overrides, variation selectors, and so on.
fn invisible(c: char) -> bool {
    c.is_control()
        || matches!(
            c,
            '\u{AD}'
                | '\u{34F}'
                | '\u{61C}'
                | '\u{115F}'..='\u{1160}'
                | '\u{17B4}'..='\u{17B5}'
                | '\u{180B}'..='\u{180F}'
                | '\u{200B}'..='\u{200F}'
                | '\u{202A}'..='\u{202E}'
                | '\u{2060}'..='\u{206F}'
                | '\u{3164}'
                | '\u{FE00}'..='\u{FE0F}'
                | '\u{FEFF}'
                | '\u{FFA0}'
                | '\u{FFF0}'..='\u{FFF8}'
                | '\u{1BCA0}'..='\u{1BCA3}'
                | '\u{1D173}'..='\u{1D17A}'
                | '\u{E0000}'..='\u{E0FFF}'
        )
}

async fn register_start(
    State(auth): State<Arc<Auth>>,
    jar: CookieJar,
    Json(req): Json<RegisterStart>,
) -> Result<Json<CeremonyStart>, ApiErr> {
    blocking(move || {
        let (user_id, name, existing, signup) = match auth.signed_in(&jar)? {
            // Signed in: add a passkey for another device.
            Some((user, _)) => {
                let existing = user.passkeys.iter().map(|p| p.cred_id().clone()).collect();
                (user.id, user.name, Some(existing), None)
            }
            None => {
                let invite = req.invite.ok_or_else(|| bad("an invite is required"))?;
                if !auth.store.invite_valid(&invite)? {
                    return Err(bad("this invite has expired or was already used"));
                }
                let name = clean_name(req.name.as_deref().unwrap_or_default())?;
                if auth.store.name_taken(&name)? {
                    return Err(conflict("that name is taken"));
                }
                let signup = Signup {
                    name: name.clone(),
                    invite,
                };
                (Uuid::new_v4(), name, None, Some(signup))
            }
        };

        let (options, state) = auth
            .webauthn
            .start_passkey_registration(user_id, &name, &name, existing)
            .map_err(|e| internal(e.into()))?;

        let mut options = serde_json::to_value(options).map_err(|e| internal(e.into()))?;
        // webauthn-rs 0.6 hardcodes `require_resident_key(false)`, which
        // serialises as `residentKey: "discouraged"`. Browsers read that as "a
        // server-side credential, please", so they offer only security keys and
        // hide password managers — which can only store discoverable
        // credentials. `finish` destructures `require_resident_key` away and
        // never checks it, so asking for a discoverable passkey here is safe.
        if let Some(selection) = options.pointer_mut("/publicKey/authenticatorSelection") {
            selection["residentKey"] = serde_json::json!("required");
            selection["requireResidentKey"] = serde_json::json!(true);
        }

        let ceremony = auth.insert_pending(Pending::Register {
            user_id,
            state,
            signup,
        })?;
        Ok(Json(CeremonyStart { ceremony, options }))
    })
    .await
}

async fn register_finish(
    State(auth): State<Arc<Auth>>,
    jar: CookieJar,
    Json(req): Json<CeremonyFinish>,
) -> Result<(CookieJar, Json<Me>), ApiErr> {
    blocking(move || {
        let Some(Pending::Register {
            user_id,
            state,
            signup,
        }) = auth.take_pending(&req.ceremony)
        else {
            return Err(bad("sign-up expired, try again"));
        };
        // Adding a passkey needs the session that asked for it to still be
        // signed in to the same account, not just the ceremony id.
        if signup.is_none() && auth.signed_in(&jar)?.is_none_or(|(u, _)| u.id != user_id) {
            return Err(ApiErr::new(
                StatusCode::UNAUTHORIZED,
                "sign in again to add a passkey",
            ));
        }
        let credential: RegisterPublicKeyCredential =
            serde_json::from_value(req.credential).map_err(|e| bad(&e.to_string()))?;
        let passkey = auth
            .webauthn
            .finish_passkey_registration(&credential, &state)
            .map_err(|e| {
                warn!("passkey registration failed: {e}");
                bad("the passkey could not be verified")
            })?;

        let (user, jar) = match signup {
            Some(Signup { name, invite }) => {
                let user = User {
                    id: user_id,
                    name,
                    passkeys: vec![passkey],
                    created_at: store::now(),
                };
                match auth.store.create_user_with_invite(&user, &invite)? {
                    Ok(()) => {}
                    Err(CreateUserError::InviteInvalid) => {
                        return Err(bad("this invite has expired or was already used"));
                    }
                    Err(CreateUserError::NameTaken) => {
                        return Err(conflict("that name is taken"));
                    }
                }
                info!(user = %user.name, "account created");
                let jar = jar.add(auth.session_cookie(auth.store.create_session(user.id)?));
                (user, jar)
            }
            None => {
                // Re-read so a passkey added concurrently isn't lost.
                let mut user = auth
                    .store
                    .user(user_id)?
                    .ok_or_else(|| bad("account no longer exists"))?;
                user.passkeys.push(passkey);
                auth.store.save_user(&user)?;
                info!(user = %user.name, "passkey added");
                (user, jar)
            }
        };

        Ok((
            jar,
            Json(Me {
                id: user.id.to_string(),
                name: user.name,
            }),
        ))
    })
    .await
}

async fn login_start(
    State(auth): State<Arc<Auth>>,
    Json(req): Json<LoginStart>,
) -> Result<Json<CeremonyStart>, ApiErr> {
    blocking(move || {
        let user = auth
            .store
            .user_by_name(&req.name)?
            .filter(|u| !u.passkeys.is_empty());
        let (options, pending) = match user {
            Some(user) => {
                let (options, state) = auth
                    .webauthn
                    .start_passkey_authentication(&user.passkeys)
                    .map_err(|e| internal(e.into()))?;
                let options = serde_json::to_value(options).map_err(|e| internal(e.into()))?;
                let pending = Pending::Login {
                    user_id: user.id,
                    state,
                };
                (options, pending)
            }
            // Answer as if the account existed, so this endpoint doesn't
            // reveal who has one. The browser then finds no matching passkey.
            None => {
                let (options, _) = auth
                    .webauthn
                    .start_passkey_authentication(&[])
                    .map_err(|e| internal(e.into()))?;
                let mut options = serde_json::to_value(options).map_err(|e| internal(e.into()))?;
                if let Some(allow) = options.pointer_mut("/publicKey/allowCredentials") {
                    let id = URL_SAFE_NO_PAD.encode(auth.store.decoy_credential_id(&req.name));
                    *allow = serde_json::json!([{ "type": "public-key", "id": id }]);
                }
                (options, Pending::Decoy)
            }
        };
        let ceremony = auth.insert_pending(pending)?;
        Ok(Json(CeremonyStart { ceremony, options }))
    })
    .await
}

async fn login_finish(
    State(auth): State<Arc<Auth>>,
    jar: CookieJar,
    Json(req): Json<CeremonyFinish>,
) -> Result<(CookieJar, Json<Me>), ApiErr> {
    blocking(move || {
        let not_verified = || {
            ApiErr::new(
                StatusCode::UNAUTHORIZED,
                "the passkey could not be verified",
            )
        };
        let (user_id, state) = match auth.take_pending(&req.ceremony) {
            Some(Pending::Login { user_id, state }) => (user_id, state),
            Some(Pending::Decoy) => return Err(not_verified()),
            _ => return Err(bad("sign-in expired, try again")),
        };
        let credential: PublicKeyCredential =
            serde_json::from_value(req.credential).map_err(|e| bad(&e.to_string()))?;
        let result = auth
            .webauthn
            .finish_passkey_authentication(&credential, &state)
            .map_err(|e| {
                warn!("passkey sign-in failed: {e}");
                not_verified()
            })?;

        let mut user = auth
            .store
            .user(user_id)?
            .ok_or_else(|| bad("account no longer exists"))?;
        // Keep signature counters current so cloned authenticators are caught.
        let mut changed = false;
        for passkey in &mut user.passkeys {
            changed |= passkey.update_credential(&result) == Some(true);
        }
        if changed {
            auth.store.save_user(&user)?;
        }

        let token = auth.store.create_session(user.id)?;
        info!(user = %user.name, "signed in");
        Ok((
            jar.add(auth.session_cookie(token)),
            Json(Me {
                id: user.id.to_string(),
                name: user.name,
            }),
        ))
    })
    .await
}

async fn logout(State(auth): State<Arc<Auth>>, jar: CookieJar) -> Result<CookieJar, ApiErr> {
    if let Some(token) = jar.get(SESSION_COOKIE) {
        let hash = store::token_hash(token.value());
        let auth = Arc::clone(&auth);
        blocking(move || {
            auth.store.delete_session(&hash)?;
            Ok(())
        })
        .await?;
    }
    Ok(jar.remove(Cookie::build(SESSION_COOKIE).path("/")))
}

async fn create_invite(
    State(auth): State<Arc<Auth>>,
    user: AuthUser,
) -> Result<Json<InviteCreated>, ApiErr> {
    blocking(move || {
        let Some(token) = auth.store.create_invite(Some(user.id), MAX_OPEN_INVITES)? else {
            return Err(ApiErr::new(
                StatusCode::TOO_MANY_REQUESTS,
                &format!(
                    "you already have {MAX_OPEN_INVITES} unused invites; \
                     wait for them to be used or expire"
                ),
            ));
        };
        info!(user = %user.name, "invite created");
        Ok(Json(InviteCreated {
            url: auth.invite_url(&token),
        }))
    })
    .await
}

/// An `/api` error: a status plus a message safe to show the user.
pub struct ApiErr(StatusCode, String);

impl ApiErr {
    pub fn new(status: StatusCode, message: &str) -> Self {
        ApiErr(status, message.to_owned())
    }
}

fn bad(message: &str) -> ApiErr {
    ApiErr::new(StatusCode::BAD_REQUEST, message)
}

fn conflict(message: &str) -> ApiErr {
    ApiErr::new(StatusCode::CONFLICT, message)
}

fn internal(e: anyhow::Error) -> ApiErr {
    error!("{e:#}");
    ApiErr::new(StatusCode::INTERNAL_SERVER_ERROR, "server error")
}

impl From<anyhow::Error> for ApiErr {
    fn from(e: anyhow::Error) -> Self {
        internal(e)
    }
}

impl IntoResponse for ApiErr {
    fn into_response(self) -> Response {
        (self.0, Json(ApiError { error: self.1 })).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_cleaned_and_checked() {
        assert_eq!(clean_name("  Ｂob  ").ok().as_deref(), Some("Bob"));
        assert!(clean_name("   ").is_err());
        assert!(clean_name(&"x".repeat(MAX_NAME_LEN + 1)).is_err());
        // Right-to-left override, zero-width space, and a control character.
        assert!(clean_name("bob\u{202E}").is_err());
        assert!(clean_name("b\u{200B}ob").is_err());
        assert!(clean_name("bob\u{7}").is_err());
    }
}
