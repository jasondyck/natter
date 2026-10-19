//! ICE over TCP (RFC 6544, passive candidates; RFC 4571 framing): for
//! clients whose network blocks UDP. Each media engine advertises a passive
//! TCP candidate beside its UDP one, as LiveKit does on `rtc.tcp_port`
//! (`docs/livekit/media.md` E.1).
//!
//! TCP runs on a thread of its own per engine, so the engine's UDP loop
//! (one blocking read per packet) is unchanged. Framed packets reach the
//! engine over a channel; the thread wakes it with a one-byte datagram to
//! its UDP socket from a known address. What the engine sends over TCP
//! comes back over another channel, with a mio waker.

use std::collections::{HashMap, VecDeque};
use std::io::ErrorKind;
use std::net::{SocketAddr, UdpSocket};
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
    read: Vec<u8>,
    write: VecDeque<u8>,
    last_active: Instant,
}

/// Starts the TCP thread for an engine whose UDP socket is `engine_udp`.
pub fn start(
    shard: usize,
    listener: std::net::TcpListener,
    advertised: SocketAddr,
    engine_udp: SocketAddr,
) -> std::io::Result<Link> {
    listener.set_nonblocking(true)?;
    let mut listener = TcpListener::from_std(listener);
    let local = SocketAddr::new(advertised.ip(), listener.local_addr()?.port());
    // The wake datagram comes from the engine's own IP, so it reaches the
    // socket however that is bound.
    let wake = UdpSocket::bind((engine_udp.ip(), 0))?;
    let wake_from = wake.local_addr()?;
    let poll = Poll::new()?;
    poll.registry()
        .register(&mut listener, LISTENER, Interest::READABLE)?;
    let waker = Arc::new(Waker::new(poll.registry(), WAKER)?);
    let (in_tx, in_rx) = channel();
    let (out_tx, out_rx) = channel();
    let pending = Arc::new(AtomicBool::new(false));
    let thread_pending = pending.clone();
    std::thread::Builder::new()
        .name(format!("tcp-{shard}"))
        .spawn(move || {
            run(
                poll,
                listener,
                in_tx,
                out_rx,
                wake,
                engine_udp,
                thread_pending,
            );
        })?;
    info!(%local, "ICE/TCP listening");
    Ok(Link {
        local,
        wake_from,
        inbound: in_rx,
        outbound: out_tx,
        waker,
        pending,
    })
}

fn run(
    mut poll: Poll,
    listener: TcpListener,
    inbound: Sender<(SocketAddr, Vec<u8>)>,
    outbound: Receiver<(SocketAddr, Vec<u8>)>,
    wake: UdpSocket,
    engine_udp: SocketAddr,
    pending: Arc<AtomicBool>,
) {
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
        let mut received = false;
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
                        match read_frames(c, &mut buf, &inbound) {
                            Ok(n) => received |= n > 0,
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
        // Wake the engine once for whatever arrived.
        if received && !pending.swap(true, Relaxed) {
            let _ = wake.send_to(&[0], engine_udp);
        }
    }
}

/// Reads what is available and passes on each whole frame. Err: closed.
fn read_frames(
    c: &mut Conn,
    buf: &mut [u8],
    inbound: &Sender<(SocketAddr, Vec<u8>)>,
) -> Result<usize, ()> {
    let mut frames = 0;
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
        if inbound.send((c.peer, frame)).is_err() {
            return Err(());
        }
        frames += 1;
    }
    c.read.drain(..at);
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Frames in both directions over a real loopback connection.
    #[test]
    fn frames_both_ways() {
        let engine = UdpSocket::bind("127.0.0.1:0").unwrap();
        engine
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let link = start(
            0,
            listener,
            "127.0.0.1:0".parse().unwrap(),
            engine.local_addr().unwrap(),
        )
        .unwrap();
        assert_eq!(link.local.port(), port);

        let mut client = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        // Two frames, the second split across writes.
        client.write_all(&[0, 3, 1, 2, 3, 0, 2, 9]).unwrap();
        client.flush().unwrap();
        std::thread::sleep(Duration::from_millis(50));
        client.write_all(&[8]).unwrap();
        // The engine is woken from the link's address.
        let mut b = [0u8; 8];
        let (_, from) = engine.recv_from(&mut b).unwrap();
        assert_eq!(from, link.wake_from);
        std::thread::sleep(Duration::from_millis(100));
        let frames: Vec<_> = link.drain().collect();
        let peer = client.local_addr().unwrap();
        assert_eq!(frames, [(peer, vec![1, 2, 3]), (peer, vec![9, 8])]);

        // And back, framed.
        link.send(peer, &[7, 7]);
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut got = [0u8; 4];
        std::io::Read::read_exact(&mut client, &mut got).unwrap();
        assert_eq!(got, [0, 2, 7, 7]);
    }
}
