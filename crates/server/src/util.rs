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

/// The CPUs this process may run on (its affinity mask: a container's
/// cpuset, `taskset`), in order. Empty where that isn't known.
pub fn allowed_cpus() -> Vec<usize> {
    #[cfg(target_os = "linux")]
    // SAFETY: `set` is a plain bitmask the call fills in.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) == 0 {
            return (0..libc::CPU_SETSIZE as usize)
                .filter(|&c| libc::CPU_ISSET(c, &set))
                .collect();
        }
    }
    vec![]
}

/// Keeps the calling thread on one CPU.
pub fn pin_to_cpu(cpu: usize) {
    #[cfg(target_os = "linux")]
    // SAFETY: as above; the call only reads `set`.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            tracing::warn!(cpu, "could not pin engine: {}", std::io::Error::last_os_error());
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = cpu;
}
