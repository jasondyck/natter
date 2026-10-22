//! LiveKit wire compatibility: the signalling, auth and APIs LiveKit's own
//! SDKs and tools speak, so they connect to natter unchanged.

// LiveKit still fills protocol fields it has deprecated (`recorder`,
// `simulcast`, `DataPacket::kind`...), because older SDKs read them. So
// does natter.
#![allow(deprecated)]

pub mod agent;
pub mod allocator;
pub mod audio_level;
pub mod cc_detect;
pub mod cluster;
pub mod dd;
pub mod dtls_crypto;
pub mod engine;
pub mod fallback;
pub mod ids;
pub mod placement;
pub mod prom;
pub mod quality;
pub mod red;
pub mod rtc_config;
pub mod sdp;
pub mod signal;
#[cfg(feature = "aws-lc")]
pub mod srtp_aws_lc;
pub mod tcp;
pub mod token;
pub mod twirp;
pub mod video;
pub mod webhook;
pub mod whip;
pub mod wire;
