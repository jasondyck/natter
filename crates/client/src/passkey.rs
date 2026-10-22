//! `navigator.credentials` from Rust.
//!
//! The server sends WebAuthn options as JSON with binary fields in base64url;
//! the browser API wants those as bytes, and returns bytes that the server
//! wants back as base64url. This module does both conversions.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use js_sys::{Array, ArrayBuffer, JSON, Object, Reflect, Uint8Array};
use serde_json::{Value, json};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::CredentialsContainer;

use crate::call::js_err;

/// Creates a passkey from registration options. Returns the credential in the
/// shape webauthn-rs expects.
pub async fn create(options: &Value) -> Result<Value, String> {
    let pk = public_key(options)?;
    decode_field(&pk, "challenge")?;
    decode_field(&prop(&pk, "user")?, "id")?;
    decode_ids(&pk, "excludeCredentials")?;

    let arg = Object::new();
    set(&arg, "publicKey", &pk)?;
    let cred = JsFuture::from(
        credentials()?
            .create_with_options(arg.unchecked_ref())
            .map_err(js_err)?,
    )
    .await
    .map_err(cancelled)?;

    let response = prop(&cred, "response")?;
    envelope(
        &cred,
        json!({
            "attestationObject": encode(&prop(&response, "attestationObject")?),
            "clientDataJSON": encode(&prop(&response, "clientDataJSON")?),
        }),
    )
}

/// Signs a login challenge with an existing passkey.
pub async fn get(options: &Value) -> Result<Value, String> {
    let pk = public_key(options)?;
    decode_field(&pk, "challenge")?;
    decode_ids(&pk, "allowCredentials")?;

    let arg = Object::new();
    set(&arg, "publicKey", &pk)?;
    let cred = JsFuture::from(
        credentials()?
            .get_with_options(arg.unchecked_ref())
            .map_err(js_err)?,
    )
    .await
    .map_err(cancelled)?;

    let response = prop(&cred, "response")?;
    let user_handle = prop(&response, "userHandle")?;
    envelope(
        &cred,
        json!({
            "authenticatorData": encode(&prop(&response, "authenticatorData")?),
            "clientDataJSON": encode(&prop(&response, "clientDataJSON")?),
            "signature": encode(&prop(&response, "signature")?),
            "userHandle": (!user_handle.is_null() && !user_handle.is_undefined())
                .then(|| encode(&user_handle)),
        }),
    )
}

/// A credential as webauthn-rs reads it, around its `response` fields.
fn envelope(cred: &JsValue, response: Value) -> Result<Value, String> {
    Ok(json!({
        "id": prop(cred, "id")?.as_string(),
        "rawId": encode(&prop(cred, "rawId")?),
        "type": "public-key",
        "response": response,
    }))
}

fn credentials() -> Result<CredentialsContainer, String> {
    let window = web_sys::window().ok_or("no window")?;
    if Reflect::get(&window, &"PublicKeyCredential".into()).map_or(true, |v| v.is_undefined()) {
        return Err("this browser doesn't support passkeys (or the page isn't on HTTPS)".into());
    }
    Ok(window.navigator().credentials())
}

/// `options.publicKey` as a JS object, with nulls dropped: WebIDL treats a
/// missing member as unset but rejects `null` for most of them.
fn public_key(options: &Value) -> Result<JsValue, String> {
    let mut pk = options
        .get("publicKey")
        .cloned()
        .ok_or("server sent no publicKey options")?;
    strip_nulls(&mut pk);
    JSON::parse(&pk.to_string()).map_err(js_err)
}

fn strip_nulls(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            map.values_mut().for_each(strip_nulls);
        }
        Value::Array(items) => items.iter_mut().for_each(strip_nulls),
        _ => {}
    }
}

/// Replaces a base64url string field with its bytes.
fn decode_field(obj: &JsValue, key: &str) -> Result<(), String> {
    let text = prop(obj, key)?
        .as_string()
        .ok_or_else(|| format!("{key} is not a string"))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(text.trim_end_matches('='))
        .map_err(|e| format!("{key}: {e}"))?;
    set(obj, key, &Uint8Array::from(bytes.as_slice()))
}

/// Decodes the `id` of each entry in an optional credential list.
fn decode_ids(obj: &JsValue, key: &str) -> Result<(), String> {
    let list = prop(obj, key)?;
    if list.is_undefined() {
        return Ok(());
    }
    for item in list.unchecked_into::<Array>().iter() {
        decode_field(&item, "id")?;
    }
    Ok(())
}

fn encode(buffer: &JsValue) -> String {
    let bytes = Uint8Array::new(buffer.unchecked_ref::<ArrayBuffer>()).to_vec();
    URL_SAFE_NO_PAD.encode(bytes)
}

fn prop(obj: &JsValue, key: &str) -> Result<JsValue, String> {
    Reflect::get(obj, &key.into()).map_err(js_err)
}

fn set(obj: &JsValue, key: &str, value: &JsValue) -> Result<(), String> {
    Reflect::set(obj, &key.into(), value)
        .map(drop)
        .map_err(js_err)
}

/// Turns the browser's rejections into plain messages. NotAllowedError covers
/// cancel, timeout and "no matching passkey" alike.
fn cancelled(e: JsValue) -> String {
    let name = Reflect::get(&e, &"name".into())
        .ok()
        .and_then(|n| n.as_string());
    match name.as_deref() {
        Some("NotAllowedError") => "cancelled, or no passkey for this site on this device".into(),
        Some("InvalidStateError") => "this device already has a passkey for your account".into(),
        _ => js_err(e),
    }
}
