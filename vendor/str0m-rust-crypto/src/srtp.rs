//! SRTP cipher implementations using RustCrypto.

use aes::cipher::Array;
use aes_gcm::aead::inout::InOutBuf;
use aes_gcm::aead::{AeadInOut, KeyInit};
use aes_gcm::{Aes128Gcm, Aes256Gcm, Nonce};
use ctr::cipher::{KeyIvInit, StreamCipher};

use str0m_proto::crypto::CryptoError;
use str0m_proto::crypto::{AeadAes128Gcm, AeadAes128GcmCipher};
use str0m_proto::crypto::{AeadAes256Gcm, AeadAes256GcmCipher, Aes128CmSha1_80Cipher};
use str0m_proto::crypto::{SrtpProvider, SupportedAeadAes128Gcm};
use str0m_proto::crypto::{SupportedAeadAes256Gcm, SupportedAes128CmSha1_80};

// Type alias for AES-128 in CTR mode
type Aes128Ctr = ctr::Ctr128BE<aes::Aes128>;

// ============================================================================
// AES-128-CM-SHA1-80 Cipher
// ============================================================================

struct RustCryptoAes128CmSha1_80Cipher {
    key: [u8; 16],
}

impl std::fmt::Debug for RustCryptoAes128CmSha1_80Cipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoAes128CmSha1_80Cipher").finish()
    }
}

impl Aes128CmSha1_80Cipher for RustCryptoAes128CmSha1_80Cipher {
    fn encrypt(
        &mut self,
        iv: &[u8; 16],
        input: &[u8],
        output: &mut [u8],
    ) -> Result<(), CryptoError> {
        // AES-128 Counter Mode (CTR)
        let mut cipher = Aes128Ctr::new(&self.key.into(), iv.into());

        // Copy input to output
        output[..input.len()].copy_from_slice(input);

        // Apply CTR mode encryption (XOR with keystream)
        cipher.apply_keystream(&mut output[..input.len()]);

        Ok(())
    }

    fn decrypt(
        &mut self,
        iv: &[u8; 16],
        input: &[u8],
        output: &mut [u8],
    ) -> Result<(), CryptoError> {
        // AES-CTR is symmetric (same operation for encrypt and decrypt)
        self.encrypt(iv, input, output)
    }
}

// ============================================================================
// AEAD-AES-128-GCM Cipher
// ============================================================================

// ============================================================================
// AES-GCM, either key size (natter patch): straight from str0m's input to its
// output buffer, as `AeadInOut` allows, rather than `Aead`'s new `Vec` per
// packet copied over afterwards.
// ============================================================================

fn gcm_encrypt<C: AeadInOut>(
    cipher: &C,
    nonce: &aes_gcm::aead::Nonce<C>,
    aad: &[u8],
    input: &[u8],
    output: &mut [u8],
) -> Result<(), CryptoError> {
    let (ciphertext, rest) = output.split_at_mut(input.len());
    let buf = InOutBuf::new(input, ciphertext)
        .map_err(|e| CryptoError::Other(format!("AES-GCM buffers: {e:?}")))?;
    let tag = cipher
        .encrypt_inout_detached(nonce, aad, buf)
        .map_err(|e| CryptoError::Other(format!("AES-GCM encrypt failed: {:?}", e)))?;
    rest[..tag.len()].copy_from_slice(&tag);
    Ok(())
}

fn gcm_decrypt<C: AeadInOut>(
    cipher: &C,
    nonce: &aes_gcm::aead::Nonce<C>,
    aads: &[&[u8]],
    input: &[u8],
    output: &mut [u8],
    tag_len: usize,
) -> Result<usize, CryptoError> {
    let (ciphertext, tag) = input.split_at(input.len() - tag_len);
    let tag = aes_gcm::aead::Tag::<C>::try_from(tag)
        .map_err(|_| CryptoError::Other("AES-GCM tag length".into()))?;
    // Concatenate AAD slices if needed
    let aad_vec: Vec<u8>;
    let aad = if aads.len() == 1 {
        aads[0]
    } else {
        aad_vec = aads.concat();
        &aad_vec
    };
    let buf = InOutBuf::new(ciphertext, &mut output[..ciphertext.len()])
        .map_err(|e| CryptoError::Other(format!("AES-GCM buffers: {e:?}")))?;
    cipher
        .decrypt_inout_detached(nonce, aad, buf, &tag)
        .map_err(|e| CryptoError::Other(format!("AES-GCM decrypt failed: {:?}", e)))?;
    Ok(ciphertext.len())
}

struct RustCryptoAeadAes128GcmCipher {
    cipher: Aes128Gcm,
}

impl std::fmt::Debug for RustCryptoAeadAes128GcmCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoAeadAes128GcmCipher").finish()
    }
}

impl AeadAes128GcmCipher for RustCryptoAeadAes128GcmCipher {
    fn encrypt(
        &mut self,
        iv: &[u8; AeadAes128Gcm::IV_LEN],
        aad: &[u8],
        input: &[u8],
        output: &mut [u8],
    ) -> Result<(), CryptoError> {
        assert!(
            aad.len() >= 12,
            "Associated data length MUST be at least 12 octets"
        );
        gcm_encrypt(&self.cipher, &Nonce::from(*iv), aad, input, output)
    }

    fn decrypt(
        &mut self,
        iv: &[u8; AeadAes128Gcm::IV_LEN],
        aads: &[&[u8]],
        input: &[u8],
        output: &mut [u8],
    ) -> Result<usize, CryptoError> {
        // Caller contract: `SrtpContext::unprotect_rtp` and `unprotect_rtcp` guarantee
        // the input holds at least the auth tag. A shorter buffer is a bug in str0m,
        // not bad input, so fail fast rather than hiding it in an error.
        assert!(input.len() >= AeadAes128Gcm::TAG_LEN);
        gcm_decrypt(
            &self.cipher,
            &Nonce::from(*iv),
            aads,
            input,
            output,
            AeadAes128Gcm::TAG_LEN,
        )
    }
}

// ============================================================================
// AEAD-AES-256-GCM Cipher
// ============================================================================

struct RustCryptoAeadAes256GcmCipher {
    cipher: Aes256Gcm,
}

impl std::fmt::Debug for RustCryptoAeadAes256GcmCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoAeadAes256GcmCipher").finish()
    }
}

impl AeadAes256GcmCipher for RustCryptoAeadAes256GcmCipher {
    fn encrypt(
        &mut self,
        iv: &[u8; AeadAes256Gcm::IV_LEN],
        aad: &[u8],
        input: &[u8],
        output: &mut [u8],
    ) -> Result<(), CryptoError> {
        assert!(
            aad.len() >= 12,
            "Associated data length MUST be at least 12 octets"
        );
        gcm_encrypt(&self.cipher, &Nonce::from(*iv), aad, input, output)
    }

    fn decrypt(
        &mut self,
        iv: &[u8; AeadAes256Gcm::IV_LEN],
        aads: &[&[u8]],
        input: &[u8],
        output: &mut [u8],
    ) -> Result<usize, CryptoError> {
        // Caller contract: `SrtpContext::unprotect_rtp` and `unprotect_rtcp` guarantee
        // the input holds at least the auth tag. A shorter buffer is a bug in str0m,
        // not bad input, so fail fast rather than hiding it in an error.
        assert!(input.len() >= AeadAes256Gcm::TAG_LEN);
        gcm_decrypt(
            &self.cipher,
            &Nonce::from(*iv),
            aads,
            input,
            output,
            AeadAes256Gcm::TAG_LEN,
        )
    }
}

// ============================================================================
// SRTP Profile Support Implementations
// ============================================================================

struct RustCryptoSupportedAes128CmSha1_80;

impl std::fmt::Debug for RustCryptoSupportedAes128CmSha1_80 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoSupportedAes128CmSha1_80")
            .finish()
    }
}

impl SupportedAes128CmSha1_80 for RustCryptoSupportedAes128CmSha1_80 {
    fn create_cipher(&self, key: [u8; 16], _encrypt: bool) -> Box<dyn Aes128CmSha1_80Cipher> {
        Box::new(RustCryptoAes128CmSha1_80Cipher { key })
    }
}

struct RustCryptoSupportedAeadAes128Gcm;

impl std::fmt::Debug for RustCryptoSupportedAeadAes128Gcm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoSupportedAeadAes128Gcm").finish()
    }
}

impl SupportedAeadAes128Gcm for RustCryptoSupportedAeadAes128Gcm {
    fn create_cipher(&self, key: [u8; 16], _encrypt: bool) -> Box<dyn AeadAes128GcmCipher> {
        let cipher = Aes128Gcm::new(&key.into());
        Box::new(RustCryptoAeadAes128GcmCipher { cipher })
    }
}

struct RustCryptoSupportedAeadAes256Gcm;

impl std::fmt::Debug for RustCryptoSupportedAeadAes256Gcm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoSupportedAeadAes256Gcm").finish()
    }
}

impl SupportedAeadAes256Gcm for RustCryptoSupportedAeadAes256Gcm {
    fn create_cipher(&self, key: [u8; 32], _encrypt: bool) -> Box<dyn AeadAes256GcmCipher> {
        let cipher = Aes256Gcm::new(&key.into());
        Box::new(RustCryptoAeadAes256GcmCipher { cipher })
    }
}

// ============================================================================
// SRTP Provider Implementation
// ============================================================================

pub(super) struct RustCryptoSrtpProvider;

impl std::fmt::Debug for RustCryptoSrtpProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoSrtpProvider").finish()
    }
}

impl SrtpProvider for RustCryptoSrtpProvider {
    fn aes_128_cm_sha1_80(&self) -> &'static dyn SupportedAes128CmSha1_80 {
        &RustCryptoSupportedAes128CmSha1_80
    }

    fn aead_aes_128_gcm(&self) -> &'static dyn SupportedAeadAes128Gcm {
        &RustCryptoSupportedAeadAes128Gcm
    }

    fn aead_aes_256_gcm(&self) -> &'static dyn SupportedAeadAes256Gcm {
        &RustCryptoSupportedAeadAes256Gcm
    }

    fn srtp_aes_128_ecb_round(&self, key: &[u8], input: &[u8], output: &mut [u8]) {
        // Use aes crate for ECB mode
        use aes::Aes128;
        use aes::cipher::{BlockCipherEncrypt, KeyInit};

        let cipher = Aes128::new(key.try_into().expect("valid AES key length"));

        // Input is 16 bytes, output needs to be 32 bytes (with PKCS7 padding)
        // For a 16-byte input, PKCS7 adds a full block of padding (16 bytes of 0x10)
        assert!(input.len() == 16);
        assert!(output.len() >= 32);

        // First block: encrypt the input
        let mut block1 = Array::try_from(&input[0..16]).expect("AES block length");
        cipher.encrypt_block(&mut block1);
        output[0..16].copy_from_slice(&block1);

        // Second block: PKCS7 padding (16 bytes of 0x10)
        let mut block2 = Array::from([0x10u8; 16]);
        cipher.encrypt_block(&mut block2);
        output[16..32].copy_from_slice(&block2);
    }

    fn srtp_aes_256_ecb_round(&self, key: &[u8], input: &[u8], output: &mut [u8]) {
        // Use aes crate for ECB mode
        use aes::Aes256;
        use aes::cipher::{BlockCipherEncrypt, KeyInit};

        let cipher = Aes256::new(key.try_into().expect("valid AES key length"));

        // Input is 16 bytes, output needs to be 32 bytes (with PKCS7 padding)
        // For a 16-byte input, PKCS7 adds a full block of padding (16 bytes of 0x10)
        assert!(input.len() == 16);
        assert!(output.len() >= 32);

        // First block: encrypt the input
        let mut block1 = Array::try_from(&input[0..16]).expect("AES block length");
        cipher.encrypt_block(&mut block1);
        output[0..16].copy_from_slice(&block1);

        // Second block: PKCS7 padding (16 bytes of 0x10)
        let mut block2 = Array::from([0x10u8; 16]);
        cipher.encrypt_block(&mut block2);
        output[16..32].copy_from_slice(&block2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes_gcm::aead::{Aead, Payload};

    /// The in-place path (natter patch) gives exactly what `Aead` gives, and
    /// decrypts it back; a changed byte fails.
    #[test]
    fn gcm_inout_matches_aead() {
        let key = [7u8; 32];
        let cipher = Aes256Gcm::new_from_slice(&key).unwrap();
        let mut c = RustCryptoAeadAes256GcmCipher {
            cipher: Aes256Gcm::new_from_slice(&key).unwrap(),
        };
        for len in [0usize, 1, 15, 16, 17, 160, 1200] {
            let input: Vec<u8> = (0..len).map(|i| (i * 31 + len) as u8).collect();
            let iv = [len as u8; 12];
            let aad = [0x80u8; 12];
            let want = cipher
                .encrypt(&Nonce::from(iv), Payload { msg: &input, aad: &aad })
                .unwrap();
            let mut out = vec![0u8; len + AeadAes256Gcm::TAG_LEN];
            c.encrypt(&iv, &aad, &input, &mut out).unwrap();
            assert_eq!(out, want, "len {len}");
            // AAD in two slices, as SRTCP passes it.
            let mut back = vec![0u8; len];
            let n = c.decrypt(&iv, &[&aad[..4], &aad[4..]], &out, &mut back).unwrap();
            assert_eq!((n, &back), (len, &input));
            out[len / 2] ^= 1;
            assert!(c.decrypt(&iv, &[&aad], &out, &mut back).is_err());
        }
        let key = [9u8; 16];
        let cipher = Aes128Gcm::new_from_slice(&key).unwrap();
        let mut c = RustCryptoAeadAes128GcmCipher {
            cipher: Aes128Gcm::new_from_slice(&key).unwrap(),
        };
        let input = [5u8; 300];
        let (iv, aad) = ([3u8; 12], [1u8; 12]);
        let want = cipher
            .encrypt(&Nonce::from(iv), Payload { msg: &input, aad: &aad })
            .unwrap();
        let mut out = vec![0u8; input.len() + AeadAes128Gcm::TAG_LEN];
        c.encrypt(&iv, &aad, &input, &mut out).unwrap();
        assert_eq!(out, want);
    }
}
