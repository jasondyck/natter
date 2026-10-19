//! DTLS crypto: dimpl's RustCrypto provider, with P-256 ECDSA signing and
//! verification moved to p256 0.14.
//!
//! Every DTLS handshake costs the server one ECDSA signature (its key
//! exchange) and one verification (the client's certificate proof), both on
//! the engine thread. dimpl 0.7 builds on p256 0.13, which has no
//! precomputed tables: 131 µs to sign and 222 µs to verify on a Ryzen 7 3800X. p256
//! 0.14 takes 55 and 150 µs, still pure Rust. When hundreds of clients join
//! one engine at once those handshakes are what keeps it busy (see the
//! README's "Load testing").
//!
//! Anything that isn't P-256 (P-384 keys or certificates, other key
//! encodings) goes to dimpl's own implementations unchanged.

use der::Decode;
use dimpl::crypto::{
    Buf, CryptoProvider, HashAlgorithm, KeyProvider, NamedGroup, SignatureAlgorithm,
    SignatureVerifier, SigningKey, check_verify_scheme,
};
use dimpl::{CryptoError, CryptoOperation};
use p256::ecdsa::signature::hazmat::{PrehashSigner, PrehashVerifier};
use p256::pkcs8::DecodePrivateKey;
use sha2::Digest;
use x509_cert::Certificate;
use x509_cert::spki::ObjectIdentifier;

const OID_EC_PUBLIC_KEY: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.10045.2.1");
const OID_P256: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.10045.3.1.7");

/// dimpl's RustCrypto provider with faster P-256 signatures.
pub fn provider() -> CryptoProvider {
    let base = dimpl::crypto::rust_crypto::default_provider();
    static KEYS: FastKeys = FastKeys;
    static VERIFIER: FastVerifier = FastVerifier;
    CryptoProvider {
        key_provider: &KEYS,
        signature_verification: &VERIFIER,
        ..base
    }
}

fn fallback() -> CryptoProvider {
    dimpl::crypto::rust_crypto::default_provider()
}

/// The digest ECDSA signs, for the hash the handshake asked for.
fn digest(data: &[u8], hash: HashAlgorithm) -> Option<Box<[u8]>> {
    match hash {
        HashAlgorithm::SHA256 => Some(Box::from(sha2::Sha256::digest(data).as_slice())),
        HashAlgorithm::SHA384 => Some(Box::from(sha2::Sha384::digest(data).as_slice())),
        _ => None,
    }
}

#[derive(Debug)]
struct FastKeys;

impl KeyProvider for FastKeys {
    fn load_private_key(&self, key_der: &[u8]) -> Result<Box<dyn SigningKey>, CryptoError> {
        match p256::ecdsa::SigningKey::from_pkcs8_der(key_der) {
            Ok(key) => Ok(Box::new(P256Key(key))),
            Err(_) => fallback().key_provider.load_private_key(key_der),
        }
    }
}

struct P256Key(p256::ecdsa::SigningKey);

impl std::fmt::Debug for P256Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("P256Key")
    }
}

impl SigningKey for P256Key {
    fn sign(&mut self, data: &[u8], hash: HashAlgorithm, out: &mut Buf) -> Result<(), CryptoError> {
        let digest = digest(data, hash).ok_or(CryptoError::SigningKeyUnsupportedHash {
            group: NamedGroup::Secp256r1,
            hash,
        })?;
        let signature: p256::ecdsa::Signature = self
            .0
            .sign_prehash(&digest)
            .map_err(|_| CryptoError::OperationFailed(CryptoOperation::Sign))?;
        out.clear();
        out.extend_from_slice(signature.to_der().as_bytes());
        Ok(())
    }

    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::ECDSA
    }

    fn hash_algorithm(&self) -> HashAlgorithm {
        HashAlgorithm::SHA256
    }

    fn supported_hash_algorithms(&self) -> &[HashAlgorithm] {
        &[HashAlgorithm::SHA256, HashAlgorithm::SHA384]
    }
}

#[derive(Debug)]
struct FastVerifier;

impl SignatureVerifier for FastVerifier {
    fn verify_signature(
        &self,
        cert_der: &[u8],
        data: &[u8],
        signature: &[u8],
        hash: HashAlgorithm,
        sig_alg: SignatureAlgorithm,
    ) -> Result<(), CryptoError> {
        let Some(public_key) = p256_public_key(cert_der) else {
            return fallback()
                .signature_verification
                .verify_signature(cert_der, data, signature, hash, sig_alg);
        };
        if sig_alg != SignatureAlgorithm::ECDSA {
            return Err(CryptoError::UnsupportedSignatureAlgorithm(sig_alg));
        }
        check_verify_scheme(sig_alg, hash, NamedGroup::Secp256r1)?;
        let digest =
            digest(data, hash).ok_or(CryptoError::UnsupportedSignatureAlgorithm(sig_alg))?;
        let key = p256::ecdsa::VerifyingKey::from_sec1_bytes(&public_key)
            .map_err(|_| CryptoError::InvalidPublicKey(NamedGroup::Secp256r1))?;
        let signature = p256::ecdsa::Signature::from_der(signature)
            .map_err(|_| CryptoError::InvalidSignatureFormat)?;
        key.verify_prehash(&digest, &signature).map_err(|_| {
            CryptoError::SignatureVerificationFailed {
                signature: sig_alg,
                hash,
                group: NamedGroup::Secp256r1,
            }
        })
    }
}

/// The certificate's P-256 public key (SEC1 point), if that's what it has.
fn p256_public_key(cert_der: &[u8]) -> Option<Vec<u8>> {
    let cert = Certificate::from_der(cert_der).ok()?;
    let spki = &cert.tbs_certificate.subject_public_key_info;
    if spki.algorithm.oid != OID_EC_PUBLIC_KEY {
        return None;
    }
    let curve: ObjectIdentifier = spki.algorithm.parameters.as_ref()?.decode_as().ok()?;
    (curve == OID_P256).then_some(spki.subject_public_key.as_bytes()?.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Our signatures verify with dimpl's own code and theirs with ours,
    /// over a certificate as str0m generates them.
    #[test]
    fn interoperates_with_dimpls_rustcrypto() {
        let cert = str0m::crypto::from_feature_flags()
            .dtls_provider
            .generate_certificate()
            .expect("certificate");
        let ours = provider();
        let theirs = fallback();
        let data = b"server key exchange params";

        let mut sig = Buf::new();
        let mut key = ours
            .key_provider
            .load_private_key(&cert.private_key)
            .unwrap();
        assert_eq!(format!("{key:?}"), "P256Key", "the fast path is taken");
        key.sign(data, HashAlgorithm::SHA256, &mut sig).unwrap();
        for verifier in [ours.signature_verification, theirs.signature_verification] {
            verifier
                .verify_signature(
                    &cert.certificate,
                    data,
                    &sig,
                    HashAlgorithm::SHA256,
                    SignatureAlgorithm::ECDSA,
                )
                .unwrap();
        }

        let mut theirs_sig = Buf::new();
        let mut key = theirs
            .key_provider
            .load_private_key(&cert.private_key)
            .unwrap();
        key.sign(data, HashAlgorithm::SHA384, &mut theirs_sig)
            .unwrap();
        ours.signature_verification
            .verify_signature(
                &cert.certificate,
                data,
                &theirs_sig,
                HashAlgorithm::SHA384,
                SignatureAlgorithm::ECDSA,
            )
            .unwrap();

        let last = theirs_sig.len() - 1;
        theirs_sig[last] ^= 1;
        assert!(
            ours.signature_verification
                .verify_signature(
                    &cert.certificate,
                    data,
                    &theirs_sig,
                    HashAlgorithm::SHA384,
                    SignatureAlgorithm::ECDSA
                )
                .is_err()
        );
    }
}

/// What a DTLS handshake costs each side with the installed provider:
/// `cargo test --release -p natter-server handshake_cost -- --ignored --nocapture`
/// (`NATTER_DTLS_CRYPTO=aws-lc` with `--features aws-lc` for the other).
/// Two str0m peers in one thread, packets passed in memory, virtual time;
/// certificates made beforehand, as the server makes one per process.
#[cfg(test)]
mod handshake_cost {
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};

    use str0m::net::{Protocol, Receive};
    use str0m::{Candidate, Event, Input, Output, Rtc};

    use crate::livekit::rtc_config::{DtlsCrypto, install_crypto};

    struct Side {
        rtc: Rtc,
        addr: SocketAddr,
        cpu: Duration,
        connected: bool,
    }

    /// Drains output; returns packets for the other side and the next timeout.
    fn drain(s: &mut Side) -> (Vec<Vec<u8>>, Instant) {
        let start = Instant::now();
        let mut out = Vec::new();
        let next = loop {
            match s.rtc.poll_output().expect("output") {
                Output::Timeout(t) => break t,
                Output::Transmit(t) => out.push(t.contents.to_vec()),
                Output::Event(Event::Connected) => s.connected = true,
                Output::Event(_) => {}
            }
        };
        s.cpu += start.elapsed();
        (out, next)
    }

    fn feed(s: &mut Side, from: SocketAddr, packets: Vec<Vec<u8>>, now: Instant) {
        let start = Instant::now();
        for p in packets {
            let input = Input::Receive(
                now,
                Receive {
                    proto: Protocol::Udp,
                    source: from,
                    destination: s.addr,
                    contents: p.as_slice().try_into().expect("a packet"),
                },
            );
            s.rtc.handle_input(input).expect("input");
        }
        s.cpu += start.elapsed();
    }

    fn pair(cert: &str0m::config::DtlsCert) -> (Side, Side) {
        let side = |port: u16| {
            let addr: SocketAddr = ([127, 0, 0, 1], port).into();
            let mut rtc = Rtc::builder()
                .set_dtls_cert(cert.clone())
                .build(Instant::now());
            rtc.add_local_candidate(Candidate::host(addr, "udp").unwrap());
            Side {
                rtc,
                addr,
                cpu: Duration::ZERO,
                connected: false,
            }
        };
        let (mut client, mut server) = (side(1000), side(2000));
        let mut api = client.rtc.sdp_api();
        api.add_channel("x".into());
        let (offer, pending) = api.apply().unwrap();
        let answer = server.rtc.sdp_api().accept_offer(offer).unwrap();
        client.rtc.sdp_api().accept_answer(pending, answer).unwrap();
        (client, server)
    }

    /// The handshake's public-key operations, one at a time.
    #[test]
    #[ignore]
    fn primitive_cost() {
        use dimpl::crypto::{Buf, HashAlgorithm, SignatureAlgorithm};
        let dtls: DtlsCrypto = std::env::var("NATTER_DTLS_CRYPTO")
            .unwrap_or_else(|_| "rust".into())
            .parse()
            .unwrap();
        install_crypto(dtls);
        let provider = dimpl::crypto::CryptoProvider::get_default().expect("installed");
        let cert = str0m::crypto::from_feature_flags()
            .dtls_provider
            .generate_certificate()
            .unwrap();
        const N: u32 = 500;
        let time = |name: &str, f: &mut dyn FnMut()| {
            f();
            let start = Instant::now();
            for _ in 0..N {
                f();
            }
            println!(
                "{dtls} {name}: {:.0} µs",
                start.elapsed().as_secs_f64() * 1e6 / f64::from(N)
            );
        };
        let mut key = provider
            .key_provider
            .load_private_key(&cert.private_key)
            .unwrap();
        let mut sig = Buf::new();
        time("ECDSA P-256 sign", &mut || {
            sig = Buf::new();
            key.sign(b"params", HashAlgorithm::SHA256, &mut sig)
                .unwrap();
        });
        time("ECDSA P-256 verify", &mut || {
            provider
                .signature_verification
                .verify_signature(
                    &cert.certificate,
                    b"params",
                    &sig,
                    HashAlgorithm::SHA256,
                    SignatureAlgorithm::ECDSA,
                )
                .unwrap();
        });
        for group in provider.kx_groups {
            let peer = group.start_exchange(Buf::new()).unwrap().pub_key().to_vec();
            time(
                &format!("ECDHE {:?} (keypair + shared secret)", group.name()),
                &mut || {
                    let kx = group.start_exchange(Buf::new()).unwrap();
                    let mut out = Buf::new();
                    kx.complete(&peer, &mut out).unwrap();
                },
            );
        }
    }

    #[test]
    #[ignore]
    fn handshake_cost() {
        let dtls: DtlsCrypto = std::env::var("NATTER_DTLS_CRYPTO")
            .unwrap_or_else(|_| "rust".into())
            .parse()
            .unwrap();
        install_crypto(dtls);
        let cert = str0m::crypto::from_feature_flags()
            .dtls_provider
            .generate_certificate()
            .unwrap();
        const N: u32 = 200;
        let (mut client_cpu, mut server_cpu) = (Duration::ZERO, Duration::ZERO);
        for _ in 0..N {
            let (mut c, mut s) = pair(&cert);
            let mut now = Instant::now();
            while !(c.connected && s.connected) {
                let (to_s, tc) = drain(&mut c);
                feed(&mut s, c.addr, to_s, now);
                let (to_c, ts) = drain(&mut s);
                feed(&mut c, s.addr, to_c, now);
                // Nothing in flight: jump to the next timer.
                now = now.max(tc.min(ts));
                let start = Instant::now();
                c.rtc.handle_input(Input::Timeout(now)).unwrap();
                c.cpu += start.elapsed();
                let start = Instant::now();
                s.rtc.handle_input(Input::Timeout(now)).unwrap();
                s.cpu += start.elapsed();
            }
            client_cpu += c.cpu;
            server_cpu += s.cpu;
        }
        println!(
            "{dtls}: per connection setup (ICE + DTLS + SCTP), answerer {:.0} µs, offerer {:.0} µs",
            server_cpu.as_secs_f64() * 1e6 / f64::from(N),
            client_cpu.as_secs_f64() * 1e6 / f64::from(N),
        );
    }
}

/// Where a P-256 verification's time goes (ignored; run with
/// `cargo test --release -p natter-server verify_parts -- --ignored --nocapture`).
/// On a Ryzen 7 3800X, 152 µs: parsing the certificate, key and signature under 3 µs,
/// `u1·G` 30 µs from p256's table, `u2·Q` 123 µs of ~256 point doublings
/// (0.39 µs each, complete formulas) and ~50 additions. Computing the two
/// halves separately is slower than ecdsa's combined wNAF pass, and
/// fiat-crypto's field multiply (tried in a vendored p256) was 15% slower
/// than p256's own reduction, which uses the special form of the prime.
/// What is left is the formulas (Jacobian doubling for a = -3 would need
/// p256's private field type) and aws-lc's assembly.
#[cfg(test)]
mod verify_parts {
    use super::*;
    use p256::elliptic_curve::group::Group;
    use p256::elliptic_curve::ops::{MulByGeneratorVartime, Reduce};
    use p256::elliptic_curve::point::AffineCoordinates;
    use p256::{ProjectivePoint, Scalar};
    use std::time::Instant;

    fn time(name: &str, n: u32, f: &mut dyn FnMut()) {
        f();
        let start = Instant::now();
        for _ in 0..n {
            f();
        }
        println!(
            "{name}: {:.1} µs",
            start.elapsed().as_secs_f64() * 1e6 / f64::from(n)
        );
    }

    /// u1·G from the generator table and u2·Q by wNAF, separately.
    fn split_verify(q: &ProjectivePoint, z: &Scalar, sig: &p256::ecdsa::Signature) -> bool {
        let (r, s) = sig.split_scalars();
        let s_inv = s.invert_vartime().unwrap();
        let (u1, u2) = (*z * s_inv, *r * s_inv);
        let p = ProjectivePoint::mul_by_generator_vartime(&u1) + q.mul_vartime(&u2);
        let x = p.to_affine().x();
        <Scalar as Reduce<_>>::reduce(&x) == *r
    }

    #[test]
    #[ignore]
    fn verify_parts() {
        let cert = str0m::crypto::from_feature_flags()
            .dtls_provider
            .generate_certificate()
            .unwrap();
        let data = b"server key exchange params";
        let mut sig = Buf::new();
        let mut key = provider()
            .key_provider
            .load_private_key(&cert.private_key)
            .unwrap();
        key.sign(data, HashAlgorithm::SHA256, &mut sig).unwrap();
        let v = provider().signature_verification;
        const N: u32 = 2000;
        time("whole verify_signature", N, &mut || {
            v.verify_signature(
                &cert.certificate,
                data,
                &sig,
                HashAlgorithm::SHA256,
                SignatureAlgorithm::ECDSA,
            )
            .unwrap()
        });
        time("  certificate parse", N, &mut || {
            p256_public_key(&cert.certificate).unwrap();
        });
        let pk = p256_public_key(&cert.certificate).unwrap();
        time("  key from SEC1", N, &mut || {
            p256::ecdsa::VerifyingKey::from_sec1_bytes(&pk).unwrap();
        });
        time("  signature from DER", N, &mut || {
            p256::ecdsa::Signature::from_der(&sig).unwrap();
        });
        let vk = p256::ecdsa::VerifyingKey::from_sec1_bytes(&pk).unwrap();
        let signature = p256::ecdsa::Signature::from_der(&sig).unwrap();
        let d = digest(data, HashAlgorithm::SHA256).unwrap();
        time("  verify_prehash (ecdsa 0.17)", N, &mut || {
            vk.verify_prehash(&d, &signature).unwrap()
        });
        let q = ProjectivePoint::from(*vk.as_affine());
        let z = <Scalar as Reduce<_>>::reduce(&p256::FieldBytes::try_from(&d[..]).unwrap());
        assert!(split_verify(&q, &z, &signature));
        time("  split: table u1·G + wNAF u2·Q", N, &mut || {
            assert!(split_verify(&q, &z, &signature))
        });
        let (r, s) = signature.split_scalars();
        let s_inv = s.invert_vartime().unwrap();
        let (u1, u2) = (z * s_inv, *r * s_inv);
        time("    s⁻¹", N, &mut || {
            std::hint::black_box(s.invert_vartime());
        });
        time("    u1·G (table, vartime)", N, &mut || {
            std::hint::black_box(ProjectivePoint::mul_by_generator_vartime(&u1));
        });
        time("    u2·Q (wNAF, vartime)", N, &mut || {
            std::hint::black_box(q.mul_vartime(&u2));
        });
        time("    point double ×100", N / 10, &mut || {
            let mut p = q;
            for _ in 0..100 {
                p = std::hint::black_box(p.double());
            }
        });
        time("    point add ×100", N / 10, &mut || {
            let mut p = q;
            for _ in 0..100 {
                p = std::hint::black_box(p + q);
            }
        });
        time("    to_affine", N, &mut || {
            std::hint::black_box(q.to_affine());
        });
    }
}
