//! natter: self-signed DTLS certificates in pure Rust.
//!
//! Upstream generates them with dimpl's `rcgen` feature, which builds rcgen
//! on aws-lc, the only C in an otherwise all-Rust WebRTC stack. This makes
//! the same certificate (P-256 ECDSA with SHA-256, `O=DTLS, CN=DTLS Peer`,
//! a year's validity, a random serial number) with p256 and x509-cert.

use std::time::{Duration, SystemTime};

use der::asn1::{BitString, ObjectIdentifier, UtcTime};
use der::{Any, Encode};
use p256_cert::ecdsa::signature::hazmat::PrehashSigner;
use p256_cert::ecdsa::{Signature, SigningKey};
use p256_cert::pkcs8::EncodePrivateKey;
use sha2_cert::Digest;
use spki::{AlgorithmIdentifierOwned, SubjectPublicKeyInfoOwned};
use x509_cert::certificate::{Certificate, TbsCertificate, Version};
use x509_cert::name::Name;
use x509_cert::serial_number::SerialNumber;
use x509_cert::time::{Time, Validity};

const ID_EC_PUBLIC_KEY: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.10045.2.1");
const SECP256R1: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.10045.3.1.7");
const ECDSA_WITH_SHA256: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.10045.4.3.2");

/// A certificate (DER) and its private key (PKCS#8 DER).
pub(crate) struct Generated {
    pub certificate: Vec<u8>,
    pub private_key: Vec<u8>,
}

fn random<const N: usize>() -> Option<[u8; N]> {
    let mut b = [0u8; N];
    getrandom::fill(&mut b).ok()?;
    Some(b)
}

pub(crate) fn generate() -> Option<Generated> {
    // A random scalar; the rare out-of-range draw is redrawn.
    let key = std::iter::repeat_with(random::<32>)
        .take(8)
        .flatten()
        .find_map(|b| SigningKey::from_slice(&b).ok())?;
    let point = key.verifying_key().to_sec1_point(false);

    // Unique across processes and machines, as Firefox needs
    // (mediasoup#127, str0m#517); positive, so the top bit is clear.
    let mut serial = random::<16>()?;
    serial[0] &= 0x7f;
    serial[0] |= 0x01;

    let name: Name = "O=DTLS,CN=DTLS Peer".parse().ok()?;
    let now = SystemTime::now();
    let time = |t: SystemTime| -> Option<Time> {
        let unix = t.duration_since(SystemTime::UNIX_EPOCH).ok()?;
        Some(Time::UtcTime(UtcTime::from_unix_duration(unix).ok()?))
    };
    let algorithm = AlgorithmIdentifierOwned {
        oid: ECDSA_WITH_SHA256,
        parameters: None,
    };
    let tbs = TbsCertificate {
        version: Version::V3,
        serial_number: SerialNumber::new(&serial).ok()?,
        signature: algorithm.clone(),
        issuer: name.clone(),
        validity: Validity {
            not_before: time(now)?,
            not_after: time(now + Duration::from_secs(365 * 24 * 3600))?,
        },
        subject: name,
        subject_public_key_info: SubjectPublicKeyInfoOwned {
            algorithm: AlgorithmIdentifierOwned {
                oid: ID_EC_PUBLIC_KEY,
                parameters: Some(Any::from(&SECP256R1)),
            },
            subject_public_key: BitString::from_bytes(point.as_bytes()).ok()?,
        },
        issuer_unique_id: None,
        subject_unique_id: None,
        extensions: None,
    };
    let digest = sha2_cert::Sha256::digest(tbs.to_der().ok()?);
    let signature: Signature = key.sign_prehash(&digest).ok()?;
    let certificate = Certificate {
        tbs_certificate: tbs,
        signature_algorithm: algorithm,
        signature: BitString::from_bytes(signature.to_der().as_bytes()).ok()?,
    };
    Some(Generated {
        certificate: certificate.to_der().ok()?,
        private_key: key.to_pkcs8_der().ok()?.as_bytes().to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use der::Decode;
    use p256_cert::ecdsa::VerifyingKey;
    use p256_cert::ecdsa::signature::hazmat::PrehashVerifier;
    use p256_cert::pkcs8::DecodePrivateKey;

    #[test]
    fn self_signed_and_matching_its_key() {
        let g = generate().expect("certificate");
        let cert = Certificate::from_der(&g.certificate).expect("parses");
        let tbs = &cert.tbs_certificate;
        assert_eq!(tbs.version, Version::V3);
        assert_eq!(tbs.issuer.to_string(), "O=DTLS,CN=DTLS Peer");
        let spki = &tbs.subject_public_key_info;
        let public = VerifyingKey::from_sec1_bytes(spki.subject_public_key.raw_bytes()).unwrap();
        let signature = Signature::from_der(cert.signature.raw_bytes()).unwrap();
        let digest = sha2_cert::Sha256::digest(tbs.to_der().unwrap());
        public.verify_prehash(&digest, &signature).expect("self-signed");
        let key = SigningKey::from_pkcs8_der(&g.private_key).expect("PKCS#8");
        assert_eq!(key.verifying_key(), &public);
        assert_ne!(generate().unwrap().certificate, g.certificate);
    }

    /// dimpl handshakes with these: a full DTLS 1.2 exchange between two
    /// certificates made here.
    #[test]
    fn dimpl_accepts_them() {
        use std::sync::Arc;
        use std::time::Instant;
        let cert = |g: Generated| dimpl::DtlsCertificate {
            certificate: g.certificate,
            private_key: g.private_key,
        };
        let config = Arc::new(dimpl::Config::builder().use_server_cookie(false).build().unwrap());
        let now = Instant::now();
        let mut client = dimpl::Dtls::new_12(config.clone(), cert(generate().unwrap()), now);
        let mut server = dimpl::Dtls::new_12(config, cert(generate().unwrap()), now);
        client.set_active(true);
        server.set_active(false);
        client.handle_timeout(now).unwrap();
        server.handle_timeout(now).unwrap();
        // Hands one side's output to the other; true once it is connected.
        fn pump(from: &mut dimpl::Dtls, to: &mut dimpl::Dtls) -> bool {
            let mut buf = vec![0u8; 2000];
            let (mut packets, mut up) = (vec![], false);
            loop {
                match from.poll_output(&mut buf) {
                    dimpl::Output::Packet(p) => packets.push(p.to_vec()),
                    dimpl::Output::Connected => up = true,
                    dimpl::Output::Timeout(_) => break,
                    _ => {}
                }
            }
            for p in packets {
                to.handle_packet(&p).unwrap();
            }
            up
        }
        let (mut client_up, mut server_up) = (false, false);
        for _ in 0..50 {
            client_up |= pump(&mut client, &mut server);
            server_up |= pump(&mut server, &mut client);
        }
        assert!(client_up && server_up);
    }
}
