//! DTLS implementation using dimpl with RustCrypto backend.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use str0m_proto::crypto::CryptoError;
use str0m_proto::crypto::DtlsVersion;
use str0m_proto::crypto::dtls::ProtocolVersion;
use str0m_proto::crypto::dtls::{DtlsCert, DtlsImplError, DtlsInstance, DtlsOutput, DtlsProvider};

/// natter: dimpl's `ConfigBuilder::build` self-tests the crypto provider
/// (an ECDH exchange and signature checks on P-256 and P-384), about 4 ms
/// with RustCrypto, and str0m built one per `Rtc`: most of a join. The
/// config depends only on the MTU (the provider is installed once per
/// process), so one is built per MTU and shared, as `Arc<Config>` allows.
fn shared_config(mtu: Option<usize>) -> Result<Arc<dimpl::Config>, CryptoError> {
    static CONFIGS: Mutex<Vec<(Option<usize>, Arc<dimpl::Config>)>> = Mutex::new(Vec::new());
    let mut configs = CONFIGS.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((_, config)) = configs.iter().find(|(m, _)| *m == mtu) {
        return Ok(Arc::clone(config));
    }
    let config = build_config(mtu, false)?;
    configs.push((mtu, Arc::clone(&config)));
    Ok(config)
}

fn build_config(mtu: Option<usize>, test: bool) -> Result<Arc<dimpl::Config>, CryptoError> {
    // ICE verifies return routability before DTLS, making server cookies redundant.
    let mut builder = dimpl::Config::builder().use_server_cookie(false);
    if let Some(mtu) = mtu {
        builder = builder.mtu(mtu);
    }
    if test {
        // We need the DTLS impl to be deterministic for the BWE tests.
        builder = builder.dangerously_set_rng_seed(42);
    }
    let config = builder
        .build()
        .map_err(|e| CryptoError::Other(format!("dimpl config creation failed: {}", e)))?;
    Ok(Arc::new(config))
}

// ============================================================================
// DTLS Provider Implementation
// ============================================================================

#[derive(Debug)]
pub(super) struct RustCryptoDtlsProvider;

impl DtlsProvider for RustCryptoDtlsProvider {
    fn generate_certificate(&self) -> Option<DtlsCert> {
        // natter: in pure Rust (cert.rs) rather than dimpl's rcgen/aws-lc.
        crate::cert::generate().map(|cert| DtlsCert {
            certificate: cert.certificate,
            private_key: cert.private_key,
        })
    }

    fn new_dtls(
        &self,
        cert: &DtlsCert,
        now: Instant,
        dtls_version: DtlsVersion,
        mtu: Option<usize>,
    ) -> Result<Box<dyn DtlsInstance>, CryptoError> {
        let dimpl_cert = dimpl::DtlsCertificate {
            certificate: cert.certificate.clone(),
            private_key: cert.private_key.clone(),
        };

        let config = if self.is_test() {
            build_config(mtu, true)?
        } else {
            shared_config(mtu)?
        };
        let dtls = match dtls_version {
            DtlsVersion::Dtls12 => dimpl::Dtls::new_12(config, dimpl_cert, now),
            DtlsVersion::Dtls13 => dimpl::Dtls::new_13(config, dimpl_cert, now),
            DtlsVersion::Auto => dimpl::Dtls::new_auto(config, dimpl_cert, now),
            _ => {
                return Err(CryptoError::Other(format!(
                    "Unsupported DTLS version: {dtls_version}"
                )));
            }
        };

        Ok(Box::new(RustCryptoDtlsInstance { dtls }))
    }
}

// ============================================================================
// DTLS Instance Wrapper
// ============================================================================

struct RustCryptoDtlsInstance {
    dtls: dimpl::Dtls,
}

impl std::fmt::Debug for RustCryptoDtlsInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoDtlsInstance").finish()
    }
}

impl DtlsInstance for RustCryptoDtlsInstance {
    fn set_active(&mut self, active: bool) {
        self.dtls.set_active(active);
    }

    fn handle_packet(&mut self, packet: &[u8]) -> Result<(), DtlsImplError> {
        self.dtls.handle_packet(packet)
    }

    fn poll_output<'a>(&mut self, buf: &'a mut [u8]) -> DtlsOutput<'a> {
        self.dtls.poll_output(buf)
    }

    fn handle_timeout(&mut self, now: Instant) -> Result<(), DtlsImplError> {
        self.dtls.handle_timeout(now)
    }

    fn send_application_data(&mut self, data: &[u8]) -> Result<usize, DtlsImplError> {
        self.dtls.send_application_data(data)
    }

    fn is_active(&self) -> bool {
        self.dtls.is_active()
    }

    fn protocol_version(&self) -> Option<ProtocolVersion> {
        self.dtls.protocol_version()
    }

    fn is_closing(&self) -> bool {
        self.dtls.is_closing()
    }

    fn is_closed(&self) -> bool {
        self.dtls.is_closed()
    }

    fn close(&mut self) -> Result<(), DtlsImplError> {
        self.dtls.close()
    }
}
