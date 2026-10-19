//! The server over real loopback sockets, as a TURN client would use it.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::time::Duration;

use super::stun::{Builder, Class, Message, attr, method};
use super::*;

/// An "engine" and a server that may relay only to it.
fn setup() -> (Turn, SocketAddr, UdpSocket) {
    let engine = UdpSocket::bind("127.0.0.1:0").unwrap();
    engine
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let engine_addr = engine.local_addr().unwrap();
    // A free port for both UDP and TCP.
    let port = loop {
        let u = UdpSocket::bind("127.0.0.1:0").unwrap();
        let p = u.local_addr().unwrap().port();
        if std::net::TcpListener::bind(("127.0.0.1", p)).is_ok() {
            break p;
        }
    };
    let turn = start(Config {
        port,
        tls: None,
        bind: "127.0.0.1".parse().unwrap(),
        public_ip: "127.0.0.1".parse().unwrap(),
        peers: HashMap::from([(engine_addr, engine_addr)]),
    })
    .unwrap();
    (turn, SocketAddr::from(([127, 0, 0, 1], port)), engine)
}

/// A TURN client over UDP or TCP.
enum Wire {
    Udp(UdpSocket, SocketAddr),
    Tcp(TcpStream),
}

impl Wire {
    fn send(&mut self, data: &[u8]) {
        match self {
            Wire::Udp(s, to) => {
                s.send_to(data, *to).unwrap();
            }
            Wire::Tcp(s) => s.write_all(data).unwrap(),
        }
    }

    fn recv(&mut self) -> Vec<u8> {
        match self {
            Wire::Udp(s, _) => {
                let mut b = vec![0u8; 2048];
                let n = s.recv(&mut b).unwrap();
                b.truncate(n);
                b
            }
            Wire::Tcp(s) => {
                let mut head = [0u8; 4];
                s.read_exact(&mut head).unwrap();
                let len = stun::frame_len(&head).unwrap().unwrap();
                let mut rest = vec![0u8; len - 4];
                s.read_exact(&mut rest).unwrap();
                [&head[..], &rest].concat()
            }
        }
    }

    fn request(&mut self, b: Builder, key: Option<&[u8]>) -> Vec<u8> {
        self.send(&b.finish(key));
        self.recv()
    }
}

fn client(server: SocketAddr, tcp: bool) -> Wire {
    if tcp {
        let s = TcpStream::connect(server).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        Wire::Tcp(s)
    } else {
        let s = UdpSocket::bind("127.0.0.1:0").unwrap();
        s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        Wire::Udp(s, server)
    }
}

fn error_code(m: &Message) -> Option<u16> {
    let v = m.get(attr::ERROR_CODE)?;
    Some(u16::from(v[2]) * 100 + u16::from(v[3]))
}

fn txid(n: u8) -> [u8; 12] {
    [n; 12]
}

/// Allocate (challenged, then authenticated), permission, Send and Data
/// indications, then a channel both ways; a peer that isn't an engine is
/// refused.
fn relay(tcp: bool) {
    let (turn, server, engine) = setup();
    let creds = turn.ice_server("PA_test");
    assert_eq!(creds.urls.len(), 2);
    let mut c = client(server, tcp);

    let allocate = |t| {
        Builder::new(method::ALLOCATE, Class::Request, txid(t))
            .attr(attr::REQUESTED_TRANSPORT, &[17, 0, 0, 0])
    };
    let raw = c.request(allocate(1), None);
    let m = Message::parse(&raw).unwrap();
    assert_eq!((m.class, error_code(&m)), (Class::Error, Some(401)));
    let nonce = m.str(attr::NONCE).unwrap().to_owned();
    let key = stun::long_term_key(&creds.username, REALM, &creds.credential);
    let authed = |b: Builder| {
        b.attr(attr::USERNAME, creds.username.as_bytes())
            .attr(attr::REALM, REALM.as_bytes())
            .attr(attr::NONCE, nonce.as_bytes())
    };

    // The wrong password.
    let wrong = stun::long_term_key(&creds.username, REALM, "nope");
    let raw = c.request(authed(allocate(2)), Some(&wrong));
    assert_eq!(error_code(&Message::parse(&raw).unwrap()), Some(401));

    let raw = c.request(authed(allocate(3)), Some(&key));
    let m = Message::parse(&raw).unwrap();
    assert_eq!(m.class, Class::Success, "{:?}", error_code(&m));
    assert!(m.verify(&key));
    let relayed = m.xor_address(attr::XOR_RELAYED_ADDRESS).unwrap();
    assert_eq!(m.u32(attr::LIFETIME), Some(DEFAULT_LIFETIME));

    // Not an engine: refused.
    let elsewhere: SocketAddr = "192.0.2.9:50000".parse().unwrap();
    let raw = c.request(
        authed(Builder::new(
            method::CREATE_PERMISSION,
            Class::Request,
            txid(4),
        ))
        .xor_address(attr::XOR_PEER_ADDRESS, elsewhere),
        Some(&key),
    );
    assert_eq!(error_code(&Message::parse(&raw).unwrap()), Some(403));

    let engine_addr = engine.local_addr().unwrap();
    let raw = c.request(
        authed(Builder::new(
            method::CREATE_PERMISSION,
            Class::Request,
            txid(5),
        ))
        .xor_address(attr::XOR_PEER_ADDRESS, engine_addr),
        Some(&key),
    );
    assert_eq!(Message::parse(&raw).unwrap().class, Class::Success);

    // Send indication → the engine, from the relayed address.
    c.send(
        &Builder::new(method::SEND, Class::Indication, txid(6))
            .xor_address(attr::XOR_PEER_ADDRESS, engine_addr)
            .attr(attr::DATA, b"hello")
            .finish(None),
    );
    let mut b = [0u8; 64];
    let (n, from) = engine.recv_from(&mut b).unwrap();
    assert_eq!((&b[..n], from), (&b"hello"[..], relayed));

    // The engine answers: a Data indication.
    engine.send_to(b"hi", relayed).unwrap();
    let raw = c.recv();
    let m = Message::parse(&raw).unwrap();
    assert_eq!((m.method, m.class), (method::DATA, Class::Indication));
    assert_eq!(m.xor_address(attr::XOR_PEER_ADDRESS), Some(engine_addr));
    assert_eq!(m.get(attr::DATA), Some(&b"hi"[..]));

    // A channel: ChannelData both ways.
    let raw = c.request(
        authed(Builder::new(method::CHANNEL_BIND, Class::Request, txid(7)))
            .attr(attr::CHANNEL_NUMBER, &[0x40, 0x01, 0, 0])
            .xor_address(attr::XOR_PEER_ADDRESS, engine_addr),
        Some(&key),
    );
    assert_eq!(Message::parse(&raw).unwrap().class, Class::Success);
    c.send(&stun::channel_data(0x4001, b"abcde", tcp));
    let (n, _) = engine.recv_from(&mut b).unwrap();
    assert_eq!(&b[..n], b"abcde");
    engine.send_to(b"xyz", relayed).unwrap();
    assert_eq!(
        stun::parse_channel_data(&c.recv()),
        Some((0x4001, &b"xyz"[..]))
    );

    // Refresh to 0 ends it: the relay no longer forwards.
    let raw = c.request(
        authed(Builder::new(method::REFRESH, Class::Request, txid(8))).u32(attr::LIFETIME, 0),
        Some(&key),
    );
    assert_eq!(Message::parse(&raw).unwrap().class, Class::Success);
    c.send(&stun::channel_data(0x4001, b"late", tcp));
    engine
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    assert!(engine.recv_from(&mut b).is_err());
}

#[test]
fn relay_over_udp() {
    relay(false);
}

#[test]
fn relay_over_tcp() {
    relay(true);
}

#[test]
fn expired_credentials_and_bad_nonces() {
    let (turn, server, _engine) = setup();
    let secret = turn.secret.clone();
    let mut c = client(server, false);
    let raw = c.request(
        Builder::new(method::ALLOCATE, Class::Request, txid(1))
            .attr(attr::REQUESTED_TRANSPORT, &[17, 0, 0, 0]),
        None,
    );
    let nonce = Message::parse(&raw)
        .unwrap()
        .str(attr::NONCE)
        .unwrap()
        .to_owned();
    let try_with = |c: &mut Wire, username: &str, nonce: &str| {
        let key = stun::long_term_key(username, REALM, &password(&*secret, username));
        let raw = c.request(
            Builder::new(method::ALLOCATE, Class::Request, txid(2))
                .attr(attr::REQUESTED_TRANSPORT, &[17, 0, 0, 0])
                .attr(attr::USERNAME, username.as_bytes())
                .attr(attr::REALM, REALM.as_bytes())
                .attr(attr::NONCE, nonce.as_bytes()),
            Some(&key),
        );
        error_code(&Message::parse(&raw).unwrap())
    };
    let past = format!("{}:x", unix_now() - 1);
    assert_eq!(try_with(&mut c, &past, &nonce), Some(401));
    let forged = format!("{:016x}{}", unix_now(), "00".repeat(8));
    let future = format!("{}:x", unix_now() + 60);
    assert_eq!(try_with(&mut c, &future, &forged), Some(438));
    assert_eq!(try_with(&mut c, &future, &nonce), None);
}
