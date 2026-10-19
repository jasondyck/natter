//! JSON calls to the server's `/api` endpoints.

use natter_protocol::ApiError;
use serde::Serialize;
use serde::de::DeserializeOwned;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Request, RequestCredentials, RequestInit, Response};

use crate::call::js_err;

/// Errors are messages fit to show the user.
pub async fn get<R: DeserializeOwned>(path: &str) -> Result<R, String> {
    parse(&request("GET", path, None).await?)
}

pub async fn post<B: Serialize, R: DeserializeOwned>(path: &str, body: &B) -> Result<R, String> {
    let body = serde_json::to_string(body).expect("request serialises");
    parse(&request("POST", path, Some(&body)).await?)
}

/// For endpoints whose reply carries nothing but its status.
pub async fn post_empty(path: &str) -> Result<(), String> {
    request("POST", path, None).await.map(drop)
}

async fn request(method: &str, path: &str, body: Option<&str>) -> Result<String, String> {
    let init = RequestInit::new();
    init.set_method(method);
    init.set_credentials(RequestCredentials::SameOrigin);
    if let Some(body) = body {
        init.set_body(&body.into());
    }
    let request = Request::new_with_str_and_init(path, &init).map_err(js_err)?;
    if body.is_some() {
        let _ = request.headers().set("Content-Type", "application/json");
    }

    let window = web_sys::window().ok_or("no window")?;
    let response: Response = JsFuture::from(window.fetch_with_request(&request))
        .await
        .map_err(js_err)?
        .unchecked_into();
    let text = JsFuture::from(response.text().map_err(js_err)?)
        .await
        .map_err(js_err)?
        .as_string()
        .unwrap_or_default();

    if response.ok() {
        Ok(text)
    } else {
        Err(serde_json::from_str::<ApiError>(&text)
            .map(|e| e.error)
            .unwrap_or_else(|_| format!("server returned {}", response.status())))
    }
}

fn parse<R: DeserializeOwned>(text: &str) -> Result<R, String> {
    serde_json::from_str(text).map_err(|e| format!("bad reply: {e}"))
}
