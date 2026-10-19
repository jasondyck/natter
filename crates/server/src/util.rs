//! Small helpers used across the server.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, header};

/// Seconds since the Unix epoch at `t` (0 before it).
pub fn secs_at(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

fn since_epoch() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
}

pub fn unix_secs() -> u64 {
    since_epoch().as_secs()
}

pub fn unix_ms() -> i64 {
    since_epoch().as_millis() as i64
}

pub fn unix_ns() -> i64 {
    since_epoch().as_nanos() as i64
}

pub fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).expect("OS randomness");
    bytes
}

/// The request's media type: its Content-Type without parameters.
pub fn content_type(headers: &HeaderMap) -> &str {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .unwrap_or("")
        .trim()
}
