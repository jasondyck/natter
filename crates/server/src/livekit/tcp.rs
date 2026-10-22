//! ICE over TCP (RFC 6544, passive candidates; RFC 4571 framing): for
//! clients whose network blocks UDP. Each media engine advertises a passive
//! TCP candidate beside its UDP one, as LiveKit does on `rtc.tcp_port`
//! (`docs/livekit/media.md` E.1).
//!
//! One port and one thread serve every engine, as LiveKit's one
//! `rtc.tcp_port`, so the engines' UDP loops (one blocking read per packet)
//! are unchanged. A connection belongs to the engine named by its first
//! STUN request: each engine's ICE ufrags start with its UDP port
//! ([`ufrag_prefix`]). Framed packets reach the engine over a channel; the
//! thread wakes it with a one-byte datagram to its UDP socket from a known
//! address. What the engines send over TCP comes back over another channel,
//! with a mio waker.

use std::collections::{HashMap, VecDeque};
use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::time::{Duration, Instant};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token, Waker};
use tracing::{debug, info, warn};

use crate::netio;

const LISTENER: Token = Token(0);
const WAKER: Token = Token(1);
/// Connections idle this long are closed (chosen: ICE consent checks come
/// every few seconds, so a live connection is never this quiet).
const IDLE: Duration = Duration::from_secs(30);
/// Bytes queued for a connection beyond which it is dropped: a client this
/// far behind can't be caught up (chosen: a few seconds of video).
const MAX_QUEUED: usize = 4 << 20;
/// Connections per engine (chosen).
const MAX_CONNECTIONS: usize = 4096;

/// The start of every local ICE ufrag of the engine on UDP `port`: the
/// port in four hex digits.
pub fn ufrag_prefix(port: u16) -> String {
    format!("{port:04x}")
}

/// The engine's side: frames in, frames out.
pub struct Link {
    /// Where the TCP listener is, as str0m's ICE sees it (the advertised
    /// address and the listener's port).
    pub local: SocketAddr,
    /// The wake datagram's source: datagrams from it mean "frames waiting".
    pub wake_from: SocketAddr,
    inbound: Receiver<(SocketAddr, Vec<u8>)>,
    outbound: Sender<(SocketAddr, Vec<u8>)>,
    waker: Arc<Waker>,
    pending: Arc<AtomicBool>,
}

impl Link {
    /// Frames that arrived, by peer.
    pub fn drain(&self) -> impl Iterator<Item = (SocketAddr, Vec<u8>)> + '_ {
        self.pending.store(false, Relaxed);
        std::iter::from_fn(|| self.inbound.try_recv().ok())
    }

    /// Sends a packet to a peer's TCP connection.
    pub fn send(&self, peer: SocketAddr, packet: &[u8]) {
        if self.outbound.send((peer, packet.to_vec())).is_ok() {
            let _ = self.waker.wake();
        }
    }
}

struct Conn {
    stream: TcpStream,
    peer: SocketAddr,
    /// Whose it is, from its first STUN request.
    engine: Option<usize>,
    read: Vec<u8>,
    write: VecDeque<u8>,
    last_active: Instant,
}

/// The thread's end of an engine's link.
struct Engine {
    udp: SocketAddr,
    inbound: Sender<(SocketAddr, Vec<u8>)>,
    pending: Arc<AtomicBool>,
}

/// Starts the TCP thread for the engines whose UDP sockets are `engines`;
/// a link for each, in the same order.
pub fn start(
    listener: std::net::TcpListener,
    advertised: IpAddr,
    engines: &[SocketAddr],
) -> std::io::Result<Vec<Link>> {
    listener.set_nonblocking(true)?;
    let mut listener = TcpListener::from_std(listener);
    let local = SocketAddr::new(advertised, listener.local_addr()?.port());
    // The wake datagram comes from the engines' own IP, so it reaches
    // their sockets however they are bound.
    let wake = UdpSocket::bind((engines[0].ip(), 0))?;
    let wake_from = wake.local_addr()?;
    let poll = Poll::new()?;
    poll.registry()
        .register(&mut listener, LISTENER, Interest::READABLE)?;
    let waker = Arc::new(Waker::new(poll.registry(), WAKER)?);
    let (out_tx, out_rx) = channel();
    let mut links = vec![];
    let mut ends = vec![];
    for &udp in engines {
        let (in_tx, in_rx) = channel();
        let pending = Arc::new(AtomicBool::new(false));
        ends.push(Engine {
            udp,
            inbound: in_tx,
            pending: pending.clone(),
        });
        links.push(Link {
            local,
            wake_from,
            inbound: in_rx,
            outbound: out_tx.clone(),
            waker: waker.clone(),
            pending,
        });
    }
    std::thread::Builder::new()
        .name("ice-tcp".into())
        .spawn(move || run(poll, listener, ends, out_rx, wake))?;
    info!(%local, "ICE/TCP listening");
    Ok(links)
}

fn run(
    mut poll: Poll,
    listener: TcpListener,
    engines: Vec<Engine>,
    outbound: Receiver<(SocketAddr, Vec<u8>)>,
    wake: UdpSocket,
) {
    let by_prefix: HashMap<String, usize> = engines
        .iter()
        .enumerate()
        .map(|(i, e)| (ufrag_prefix(e.udp.port()), i))
        .collect();
    let mut events = Events::with_capacity(256);
    let mut conns: HashMap<Token, Conn> = HashMap::new();
    let mut by_peer: HashMap<SocketAddr, Token> = HashMap::new();
    let mut next = 2usize;
    let mut buf = vec![0u8; 64 * 1024];
    let mut last_sweep = Instant::now();
    loop {
        if let Err(e) = poll.poll(&mut events, Some(Duration::from_secs(1))) {
            if e.kind() != ErrorKind::Interrupted {
                warn!("TCP poll failed: {e}");
            }
            continue;
        }
        let mut received = vec![false; engines.len()];
        let mut closed = vec![];
        for event in &events {
            match event.token() {
                LISTENER => loop {
                    match listener.accept() {
                        Ok((mut stream, peer)) => {
                            if conns.len() >= MAX_CONNECTIONS {
                                continue;
                            }
                            let _ = stream.set_nodelay(true);
                            let token = Token(next);
                            next += 1;
                            if poll
                                .registry()
                                .register(&mut stream, token, Interest::READABLE)
                                .is_ok()
                            {
                                debug!(%peer, "ICE/TCP connection");
                                by_peer.insert(peer, token);
                                conns.insert(
                                    token,
                                    Conn {
                                        stream,
                                        peer,
                                        engine: None,
                                        read: vec![],
                                        write: VecDeque::new(),
                                        last_active: Instant::now(),
                                    },
                                );
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(e) => {
                            debug!("TCP accept failed: {e}");
                            break;
                        }
                    }
                },
                WAKER => {}
                token => {
                    let Some(c) = conns.get_mut(&token) else {
                        continue;
                    };
                    if event.is_readable() {
                        match read_frames(c, &mut buf, &engines, &by_prefix) {
                            Ok(true) => received[c.engine.expect("set by a frame")] = true,
                            Ok(false) => {}
                            Err(()) => closed.push(token),
                        }
                    }
                    if event.is_writable() && netio::flush(&mut c.stream, &mut c.write).is_err() {
                        closed.push(token);
                    }
                }
            }
        }
        // What the engine sends: framed onto each peer's connection.
        loop {
            match outbound.try_recv() {
                Ok((peer, packet)) => {
                    let Some(token) = by_peer.get(&peer) else {
                        continue;
                    };
                    let Some(c) = conns.get_mut(token) else {
                        continue;
                    };
                    if packet.len() > usize::from(u16::MAX) || c.write.len() > MAX_QUEUED {
                        closed.push(*token);
                        continue;
                    }
                    c.write.extend((packet.len() as u16).to_be_bytes());
                    c.write.extend(packet);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return,
            }
        }
        for (token, c) in conns.iter_mut() {
            if c.write.is_empty() {
                continue;
            }
            match netio::flush(&mut c.stream, &mut c.write) {
                Ok(true) => {
                    let _ = poll
                        .registry()
                        .reregister(&mut c.stream, *token, Interest::READABLE);
                }
                Ok(false) => {
                    let _ = poll.registry().reregister(
                        &mut c.stream,
                        *token,
                        Interest::READABLE | Interest::WRITABLE,
                    );
                }
                Err(()) => closed.push(*token),
            }
        }
        if last_sweep.elapsed() > Duration::from_secs(5) {
            last_sweep = Instant::now();
            closed.extend(
                conns
                    .iter()
                    .filter(|(_, c)| c.last_active.elapsed() > IDLE)
                    .map(|(t, _)| *t),
            );
        }
        for token in closed {
            if let Some(mut c) = conns.remove(&token) {
                let _ = poll.registry().deregister(&mut c.stream);
                by_peer.remove(&c.peer);
                debug!(peer = %c.peer, "ICE/TCP connection closed");
            }
        }
        // Wake each engine once for whatever arrived.
        for (e, received) in engines.iter().zip(received) {
            if received && !e.pending.swap(true, Relaxed) {
                let _ = wake.send_to(&[0], e.udp);
            }
        }
    }
}

/// Reads what is available and passes each whole frame to the connection's
/// engine. Ok(true): frames went to it. Err: closed, or its first frame
/// names no engine.
fn read_frames(
    c: &mut Conn,
    buf: &mut [u8],
    engines: &[Engine],
    by_prefix: &HashMap<String, usize>,
) -> Result<bool, ()> {
    let mut frames = false;
    if netio::read_into(&mut c.stream, buf, &mut c.read)? {
        c.last_active = Instant::now();
    }
    let mut at = 0;
    while c.read.len() - at >= 2 {
        let len = usize::from(u16::from_be_bytes([c.read[at], c.read[at + 1]]));
        if c.read.len() - at - 2 < len {
            break;
        }
        let frame = c.read[at + 2..at + 2 + len].to_vec();
        at += 2 + len;
        let engine = match c.engine {
            Some(e) => e,
            None => {
                let ufrag = stun_ufrag(&frame);
                let Some(&e) = ufrag.and_then(|u| by_prefix.get(u.get(..4)?)) else {
                    debug!(peer = %c.peer, ?ufrag, "ICE/TCP first frame names no engine");
                    return Err(());
                };
                c.engine = Some(e);
                e
            }
        };
        if engines[engine].inbound.send((c.peer, frame)).is_err() {
            return Err(());
        }
        frames = true;
    }
    c.read.drain(..at);
    Ok(frames)
}

/// The recipient's ufrag in a STUN request's USERNAME (`theirs:ours`, so
/// ours as the server), if `frame` is STUN with one (RFC 8489).
fn stun_ufrag(frame: &[u8]) -> Option<&str> {
    const USERNAME: u16 = 0x0006;
    if frame.len() < 20 || frame[0] >= 0x40 || frame[4..8] != [0x21, 0x12, 0xa4, 0x42] {
        return None;
    }
    let mut attrs = &frame[20..];
    while attrs.len() >= 4 {
        let kind = u16::from_be_bytes([attrs[0], attrs[1]]);
        let len = usize::from(u16::from_be_bytes([attrs[2], attrs[3]]));
        let value = attrs.get(4..4 + len)?;
        if kind == USERNAME {
            let username = std::str::from_utf8(value).ok()?;
            return username.split(':').next();
        }
        attrs = attrs.get((4 + len).next_multiple_of(4)..)?;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// A STUN binding request whose USERNAME is `ours:theirs`.
    fn binding(ours: &str) -> Vec<u8> {
        let username = format!("{ours}:client");
        let padded = username.len().next_multiple_of(4);
        let mut m = vec![0, 1];
        m.extend(((4 + padded) as u16).to_be_bytes());
        m.extend([0x21, 0x12, 0xa4, 0x42]);
        m.extend([7; 12]);
        // An attribute before USERNAME, to be skipped.
        m.extend([0x80, 0x29, 0, 0]);
        m.extend([0, 6]);
        m.extend((username.len() as u16).to_be_bytes());
        m.extend(username.as_bytes());
        m.resize(m.len() + padded - username.len(), 0);
        let len = (m.len() - 20) as u16;
        m[2..4].copy_from_slice(&len.to_be_bytes());
        m
    }

    fn framed(frame: &[u8]) -> Vec<u8> {
        let mut f = (frame.len() as u16).to_be_bytes().to_vec();
        f.extend(frame);
        f
    }

    #[test]
    fn reads_the_ufrag() {
        let m = binding("01f4abcd");
        assert_eq!(stun_ufrag(&m), Some("01f4abcd"));
        assert_eq!(stun_ufrag(&m[..19]), None);
        assert_eq!(stun_ufrag(&[0x80; 40]), None);
    }

    /// Two engines on one port: each connection reaches the engine its
    /// first STUN request names, and frames go back framed.
    #[test]
    fn frames_both_ways() {
        let engines: Vec<UdpSocket> = (0..2)
            .map(|_| {
                let e = UdpSocket::bind("127.0.0.1:0").unwrap();
                e.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                e
            })
            .collect();
        let addrs: Vec<_> = engines.iter().map(|e| e.local_addr().unwrap()).collect();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let links = start(listener, "127.0.0.1".parse().unwrap(), &addrs).unwrap();
        assert_eq!(links[1].local.port(), port);

        let ufrag = format!("{}xyz", ufrag_prefix(addrs[1].port()));
        let first = binding(&ufrag);
        let mut client = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        // The request, then a frame split across writes.
        let mut bytes = framed(&first);
        bytes.extend([0, 2, 9]);
        client.write_all(&bytes).unwrap();
        client.flush().unwrap();
        std::thread::sleep(Duration::from_millis(50));
        client.write_all(&[8]).unwrap();
        // Engine 1 is woken from the links' address.
        let mut b = [0u8; 8];
        let (_, from) = engines[1].recv_from(&mut b).unwrap();
        assert_eq!(from, links[1].wake_from);
        std::thread::sleep(Duration::from_millis(100));
        let peer = client.local_addr().unwrap();
        assert_eq!(
            links[1].drain().collect::<Vec<_>>(),
            [(peer, first), (peer, vec![9, 8])]
        );
        assert_eq!(links[0].drain().count(), 0);

        // And back, framed.
        links[1].send(peer, &[7, 7]);
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut got = [0u8; 4];
        std::io::Read::read_exact(&mut client, &mut got).unwrap();
        assert_eq!(got, [0, 2, 7, 7]);

        // A first frame naming no engine closes the connection.
        let mut stranger = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        stranger.write_all(&framed(&binding("ffffxyz"))).unwrap();
        stranger
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        assert_eq!(std::io::Read::read(&mut stranger, &mut got).unwrap(), 0);
    }
}
