//! STUN messages (RFC 8489) with the TURN methods and attributes (RFC 8656),
//! and TURN ChannelData.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::ops::Range;

use hmac::{KeyInit, Mac};

pub const MAGIC: u32 = 0x2112_A442;
const HEADER: usize = 20;

pub mod method {
    pub const BINDING: u16 = 0x001;
    pub const ALLOCATE: u16 = 0x003;
    pub const REFRESH: u16 = 0x004;
    pub const SEND: u16 = 0x006;
    pub const DATA: u16 = 0x007;
    pub const CREATE_PERMISSION: u16 = 0x008;
    pub const CHANNEL_BIND: u16 = 0x009;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Request,
    Indication,
    Success,
    Error,
}

pub mod attr {
    pub const USERNAME: u16 = 0x0006;
    pub const MESSAGE_INTEGRITY: u16 = 0x0008;
    pub const ERROR_CODE: u16 = 0x0009;
    pub const CHANNEL_NUMBER: u16 = 0x000C;
    pub const LIFETIME: u16 = 0x000D;
    pub const XOR_PEER_ADDRESS: u16 = 0x0012;
    pub const DATA: u16 = 0x0013;
    pub const REALM: u16 = 0x0014;
    pub const NONCE: u16 = 0x0015;
    pub const XOR_RELAYED_ADDRESS: u16 = 0x0016;
    pub const REQUESTED_ADDRESS_FAMILY: u16 = 0x0017;
    pub const REQUESTED_TRANSPORT: u16 = 0x0019;
    pub const XOR_MAPPED_ADDRESS: u16 = 0x0020;
    #[cfg(test)]
    pub const SOFTWARE: u16 = 0x8022;
    pub const FINGERPRINT: u16 = 0x8028;
}

/// What a byte stream or datagram starts with.
pub enum Kind {
    Stun,
    /// ChannelData: a channel number (0x4000-0x7FFF) and a length.
    Channel,
    Other,
}

pub fn kind(data: &[u8]) -> Kind {
    match data.first().map(|b| b >> 6) {
        Some(0) => Kind::Stun,
        Some(1) => Kind::Channel,
        _ => Kind::Other,
    }
}

/// A whole frame's length at the start of a byte stream (RFC 8656 §12.5:
/// over TCP, ChannelData is padded to a multiple of 4), or None while the
/// header is incomplete. Err: neither STUN nor ChannelData.
pub fn frame_len(data: &[u8]) -> Result<Option<usize>, ()> {
    if data.len() < 4 {
        return Ok(None);
    }
    let len = usize::from(u16::from_be_bytes([data[2], data[3]]));
    match kind(data) {
        Kind::Stun => Ok(Some(HEADER + len)),
        Kind::Channel => Ok(Some(4 + len.next_multiple_of(4))),
        Kind::Other => Err(()),
    }
}

/// A parsed STUN message, borrowing the bytes it came from.
pub struct Message<'a> {
    pub method: u16,
    pub class: Class,
    pub txid: [u8; 12],
    raw: &'a [u8],
    attrs: Vec<(u16, Range<usize>)>,
    /// Where MESSAGE-INTEGRITY's header starts, if present.
    integrity_at: Option<usize>,
}

impl<'a> Message<'a> {
    pub fn parse(raw: &'a [u8]) -> Option<Self> {
        if raw.len() < HEADER || raw[0] >> 6 != 0 {
            return None;
        }
        let ty = u16::from_be_bytes([raw[0], raw[1]]);
        let len = usize::from(u16::from_be_bytes([raw[2], raw[3]]));
        if u32::from_be_bytes(raw[4..8].try_into().ok()?) != MAGIC
            || len % 4 != 0
            || raw.len() < HEADER + len
        {
            return None;
        }
        let raw = &raw[..HEADER + len];
        let method = (ty & 0x000F) | ((ty & 0x00E0) >> 1) | ((ty & 0x3E00) >> 2);
        let class = match ((ty >> 7) & 0b10) | ((ty >> 4) & 0b1) {
            0 => Class::Request,
            1 => Class::Indication,
            2 => Class::Success,
            _ => Class::Error,
        };
        let mut attrs = vec![];
        let mut integrity_at = None;
        let mut at = HEADER;
        while at + 4 <= raw.len() {
            let t = u16::from_be_bytes([raw[at], raw[at + 1]]);
            let l = usize::from(u16::from_be_bytes([raw[at + 2], raw[at + 3]]));
            let value = at + 4..at + 4 + l;
            if value.end > raw.len() {
                return None;
            }
            // Only FINGERPRINT may follow MESSAGE-INTEGRITY; anything else
            // after it is ignored (RFC 8489 §14.5).
            if integrity_at.is_none() || t == attr::FINGERPRINT {
                if t == attr::MESSAGE_INTEGRITY {
                    integrity_at = Some(at);
                }
                attrs.push((t, value));
            }
            at += 4 + l.next_multiple_of(4);
        }
        Some(Message {
            method,
            class,
            txid: raw[8..20].try_into().ok()?,
            raw,
            attrs,
            integrity_at,
        })
    }

    pub fn get(&self, ty: u16) -> Option<&'a [u8]> {
        self.attrs
            .iter()
            .find(|(t, _)| *t == ty)
            .map(|(_, r)| &self.raw[r.clone()])
    }

    pub fn get_all(&self, ty: u16) -> impl Iterator<Item = &'a [u8]> + '_ {
        self.attrs
            .iter()
            .filter(move |(t, _)| *t == ty)
            .map(|(_, r)| &self.raw[r.clone()])
    }

    pub fn str(&self, ty: u16) -> Option<&'a str> {
        std::str::from_utf8(self.get(ty)?).ok()
    }

    pub fn u32(&self, ty: u16) -> Option<u32> {
        Some(u32::from_be_bytes(self.get(ty)?.get(..4)?.try_into().ok()?))
    }

    pub fn xor_address(&self, ty: u16) -> Option<SocketAddr> {
        decode_xor(self.get(ty)?, &self.txid)
    }

    pub fn xor_addresses(&self, ty: u16) -> impl Iterator<Item = Option<SocketAddr>> + '_ {
        self.get_all(ty).map(|v| decode_xor(v, &self.txid))
    }

    /// Whether MESSAGE-INTEGRITY is present and right for `key`.
    pub fn verify(&self, key: &[u8]) -> bool {
        let Some(at) = self.integrity_at else {
            return false;
        };
        let Some(given) = self.raw.get(at + 4..at + 24) else {
            return false;
        };
        let mut mac = hmac::Hmac::<sha1::Sha1>::new_from_slice(key).expect("any key length");
        // The length covers up to and including MESSAGE-INTEGRITY.
        let len = (at + 24 - HEADER) as u16;
        mac.update(&self.raw[..2]);
        mac.update(&len.to_be_bytes());
        mac.update(&self.raw[4..at]);
        mac.verify_slice(given).is_ok()
    }
}

/// The long-term credential key (RFC 8489 §9.2.2): MD5 of
/// `username:realm:password`.
pub fn long_term_key(username: &str, realm: &str, password: &str) -> [u8; 16] {
    use md5::Digest;
    md5::Md5::digest(format!("{username}:{realm}:{password}")).into()
}

fn decode_xor(v: &[u8], txid: &[u8; 12]) -> Option<SocketAddr> {
    let port = u16::from_be_bytes(v.get(2..4)?.try_into().ok()?) ^ (MAGIC >> 16) as u16;
    let ip = match v.get(1)? {
        1 => {
            let b: [u8; 4] = v.get(4..8)?.try_into().ok()?;
            IpAddr::V4(Ipv4Addr::from(u32::from_be_bytes(b) ^ MAGIC))
        }
        2 => {
            let mut b: [u8; 16] = v.get(4..20)?.try_into().ok()?;
            for (x, k) in b.iter_mut().zip(MAGIC.to_be_bytes().iter().chain(txid)) {
                *x ^= k;
            }
            IpAddr::V6(Ipv6Addr::from(b))
        }
        _ => return None,
    };
    Some(SocketAddr::new(ip, port))
}

/// Builds a message: attributes in order, then optionally
/// MESSAGE-INTEGRITY and FINGERPRINT.
pub struct Builder {
    buf: Vec<u8>,
    txid: [u8; 12],
}

impl Builder {
    pub fn new(method: u16, class: Class, txid: [u8; 12]) -> Self {
        let c = match class {
            Class::Request => 0,
            Class::Indication => 1,
            Class::Success => 2,
            Class::Error => 3,
        };
        let ty = (method & 0x000F)
            | ((method & 0x0070) << 1)
            | ((method & 0x0F80) << 2)
            | ((c & 1) << 4)
            | ((c & 2) << 7);
        let mut buf = Vec::with_capacity(128);
        buf.extend(ty.to_be_bytes());
        buf.extend([0, 0]);
        buf.extend(MAGIC.to_be_bytes());
        buf.extend(txid);
        Builder { buf, txid }
    }

    pub fn attr(mut self, ty: u16, value: &[u8]) -> Self {
        self.buf.extend(ty.to_be_bytes());
        self.buf.extend((value.len() as u16).to_be_bytes());
        self.buf.extend(value);
        self.buf.resize(self.buf.len().next_multiple_of(4), 0);
        self
    }

    pub fn u32(self, ty: u16, value: u32) -> Self {
        self.attr(ty, &value.to_be_bytes())
    }

    pub fn xor_address(self, ty: u16, addr: SocketAddr) -> Self {
        let mut v = vec![0u8];
        let port = addr.port() ^ (MAGIC >> 16) as u16;
        match addr.ip() {
            IpAddr::V4(ip) => {
                v.push(1);
                v.extend(port.to_be_bytes());
                v.extend((u32::from(ip) ^ MAGIC).to_be_bytes());
            }
            IpAddr::V6(ip) => {
                v.push(2);
                v.extend(port.to_be_bytes());
                let key = MAGIC.to_be_bytes().into_iter().chain(self.txid);
                v.extend(ip.octets().iter().zip(key).map(|(a, k)| a ^ k));
            }
        }
        self.attr(ty, &v)
    }

    pub fn error(self, code: u16, reason: &str) -> Self {
        let mut v = vec![0, 0, (code / 100) as u8, (code % 100) as u8];
        v.extend(reason.as_bytes());
        self.attr(attr::ERROR_CODE, &v)
    }

    fn set_len(&mut self, len: usize) {
        let len = (len - HEADER) as u16;
        self.buf[2..4].copy_from_slice(&len.to_be_bytes());
    }

    pub fn finish(mut self, key: Option<&[u8]>) -> Vec<u8> {
        if let Some(key) = key {
            self.set_len(self.buf.len() + 24);
            let mut mac = hmac::Hmac::<sha1::Sha1>::new_from_slice(key).expect("any key length");
            mac.update(&self.buf);
            let tag = mac.finalize().into_bytes();
            self = self.attr(attr::MESSAGE_INTEGRITY, &tag);
        }
        self.set_len(self.buf.len() + 8);
        let crc = crc32fast::hash(&self.buf) ^ 0x5354_554E;
        self = self.attr(attr::FINGERPRINT, &crc.to_be_bytes());
        self.buf
    }
}

/// ChannelData: `channel`, length, data, padded to 4 bytes when `pad`
/// (byte streams; over UDP the padding is optional and left off).
pub fn channel_data(channel: u16, data: &[u8], pad: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + data.len() + 3);
    out.extend(channel.to_be_bytes());
    out.extend((data.len() as u16).to_be_bytes());
    out.extend(data);
    if pad {
        out.resize(out.len().next_multiple_of(4), 0);
    }
    out
}

/// A ChannelData message's channel and data.
pub fn parse_channel_data(raw: &[u8]) -> Option<(u16, &[u8])> {
    let channel = u16::from_be_bytes(raw.get(..2)?.try_into().ok()?);
    let len = usize::from(u16::from_be_bytes(raw.get(2..4)?.try_into().ok()?));
    Some((channel, raw.get(4..4 + len)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 5769 §2.4: a request with long-term authentication.
    #[test]
    fn rfc5769_long_term() {
        let raw = hex::decode(concat!(
            "000100602112a44278ad3433c6ad72c029da412e",
            "00060012e3839ee38388e383aae38383e382afe382b90000",
            "0015001c662f2f3439396b39353464364f4c33346f4c39465354767936347341",
            "0014000b6578616d706c652e6f726700",
            "00080014f67024656dd64a3e02b8e0712e85c9a28ca89666",
        ))
        .unwrap();
        let m = Message::parse(&raw).unwrap();
        assert_eq!(m.method, method::BINDING);
        assert_eq!(m.class, Class::Request);
        assert_eq!(m.str(attr::USERNAME), Some("マトリックス"));
        assert_eq!(m.str(attr::REALM), Some("example.org"));
        let key = long_term_key("マトリックス", "example.org", "TheMatrIX");
        assert!(m.verify(&key));
        assert!(!m.verify(&long_term_key("マトリックス", "example.org", "wrong")));
    }

    #[test]
    fn build_then_parse() {
        let txid = [7u8; 12];
        let v4: SocketAddr = "192.0.2.1:3478".parse().unwrap();
        let v6: SocketAddr = "[2001:db8::1]:50000".parse().unwrap();
        let key = long_term_key("u", "r", "p");
        let raw = Builder::new(method::ALLOCATE, Class::Success, txid)
            .xor_address(attr::XOR_RELAYED_ADDRESS, v4)
            .xor_address(attr::XOR_MAPPED_ADDRESS, v6)
            .u32(attr::LIFETIME, 600)
            .attr(attr::SOFTWARE, b"natter")
            .finish(Some(&key));
        let m = Message::parse(&raw).unwrap();
        assert_eq!(
            (m.method, m.class, m.txid),
            (method::ALLOCATE, Class::Success, txid)
        );
        assert_eq!(m.xor_address(attr::XOR_RELAYED_ADDRESS), Some(v4));
        assert_eq!(m.xor_address(attr::XOR_MAPPED_ADDRESS), Some(v6));
        assert_eq!(m.u32(attr::LIFETIME), Some(600));
        assert_eq!(m.str(attr::SOFTWARE), Some("natter"));
        assert!(m.verify(&key));
        // Every class and method round-trips through the type field.
        for class in [
            Class::Request,
            Class::Indication,
            Class::Success,
            Class::Error,
        ] {
            for method in [method::BINDING, method::CHANNEL_BIND, 0xFFF] {
                let raw = Builder::new(method, class, txid).finish(None);
                let m = Message::parse(&raw).unwrap();
                assert_eq!((m.method, m.class), (method, class));
            }
        }
    }

    #[test]
    fn stream_framing() {
        let stun = Builder::new(method::BINDING, Class::Request, [0; 12]).finish(None);
        assert_eq!(frame_len(&stun), Ok(Some(stun.len())));
        let cd = channel_data(0x4000, &[1, 2, 3, 4, 5], true);
        assert_eq!(cd.len(), 12);
        assert_eq!(frame_len(&cd), Ok(Some(12)));
        assert_eq!(
            parse_channel_data(&cd),
            Some((0x4000, &[1, 2, 3, 4, 5][..]))
        );
        assert_eq!(frame_len(&[0x80, 0, 0, 0]), Err(()));
        assert_eq!(frame_len(&[0x40, 0]), Ok(None));
    }
}
