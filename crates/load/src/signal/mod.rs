//! One module per server's signalling. Each joins a client and hands its
//! negotiated peer connections to a media worker, then keeps serving that
//! client's signalling (renegotiation, candidates, pings) in the
//! background.

pub mod chat;
pub mod livekit;

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use str0m::media::MediaKind;

use crate::media::{ClientKey, Worker};

/// One simulated participant.
#[derive(Debug, Clone)]
pub struct Client {
    pub key: ClientKey,
    pub room: String,
    pub identity: String,
    /// Publisher id stamped into its packets, if it publishes.
    pub publisher: Option<u32>,
}

pub struct Ctx {
    pub url: String,
    pub bind: IpAddr,
    /// What publishers publish.
    pub kind: MediaKind,
    /// Video: three simulcast layers, the top one at `video_bps`.
    pub simulcast: bool,
    pub video_bps: u32,
    pub workers: Vec<Worker>,
    next: AtomicUsize,
}

impl Ctx {
    pub fn new(
        url: String,
        bind: IpAddr,
        kind: MediaKind,
        simulcast: bool,
        video_bps: u32,
        workers: Vec<Worker>,
    ) -> Arc<Self> {
        Arc::new(Ctx {
            url: url.trim_end_matches('/').to_owned(),
            bind,
            kind,
            simulcast,
            video_bps,
            workers,
            next: AtomicUsize::new(0),
        })
    }

    /// Workers in turn, so connections spread evenly.
    pub fn worker(&self) -> Worker {
        self.workers[self.next.fetch_add(1, Relaxed) % self.workers.len()].clone()
    }
}
