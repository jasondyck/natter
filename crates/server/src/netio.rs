//! Non-blocking reads and writes for the TCP loops, and batched UDP sends
//! for the engines.

use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, UdpSocket};
use std::os::fd::AsRawFd;

use socket2::SockAddr;

/// Reads what is available into `out`. Ok(true): read something; Err:
/// closed.
pub fn read_into(r: &mut impl Read, buf: &mut [u8], out: &mut Vec<u8>) -> Result<bool, ()> {
    let mut got = false;
    loop {
        match r.read(buf) {
            Ok(0) => return Err(()),
            Ok(n) => {
                out.extend_from_slice(&buf[..n]);
                got = true;
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(got),
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(_) => return Err(()),
        }
    }
}

/// Writes what is queued. Ok(true): all written; Ok(false): the socket is
/// full; Err: closed.
pub fn flush(w: &mut impl Write, queue: &mut VecDeque<u8>) -> Result<bool, ()> {
    while !queue.is_empty() {
        let (a, _) = queue.as_slices();
        match w.write(a) {
            Ok(0) => return Err(()),
            Ok(n) => {
                queue.drain(..n);
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(false),
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(_) => return Err(()),
        }
    }
    Ok(true)
}

/// Datagrams an engine sends during one pass, flushed with one
/// `sendmmsg(2)` per [`SendBatch::MAX`] instead of a syscall each: a
/// packet in is often several out (one per subscriber).
#[derive(Default)]
pub struct SendBatch {
    queue: Vec<(SockAddr, Vec<u8>)>,
}

impl SendBatch {
    /// Datagrams per `sendmmsg` (chosen: the kernel's UIO_MAXIOV is 1024;
    /// past a few dozen the syscall's fixed cost is already spread thin).
    pub const MAX: usize = 64;

    /// Queues a datagram. True: the batch is full, flush it.
    pub fn push(&mut self, to: SocketAddr, data: Vec<u8>) -> bool {
        self.queue.push((SockAddr::from(to), data));
        self.queue.len() >= Self::MAX
    }

    /// Sends what is queued. Returns how many datagrams failed (each
    /// logged at debug, as a lone `send_to` failing).
    pub fn flush(&mut self, socket: &UdpSocket) -> u64 {
        let mut failed = 0;
        let mut at = 0;
        while at < self.queue.len() {
            let chunk = &self.queue[at..(at + Self::MAX).min(self.queue.len())];
            // SAFETY: all-zero iovec and mmsghdr are valid (null pointers,
            // zero lengths); every one used is filled in below.
            let mut iov: [libc::iovec; Self::MAX] = unsafe { std::mem::zeroed() };
            let mut msgs: [libc::mmsghdr; Self::MAX] = unsafe { std::mem::zeroed() };
            for (k, (to, data)) in chunk.iter().enumerate() {
                iov[k] = libc::iovec {
                    iov_base: data.as_ptr() as *mut libc::c_void,
                    iov_len: data.len(),
                };
                let h = &mut msgs[k].msg_hdr;
                h.msg_name = to.as_ptr() as *mut libc::c_void;
                h.msg_namelen = to.len();
                h.msg_iov = &mut iov[k];
                h.msg_iovlen = 1;
            }
            // SAFETY: the headers point into `iov` and `chunk`, which
            // outlive the call; the count is at most their length.
            let n = unsafe {
                libc::sendmmsg(
                    socket.as_raw_fd(),
                    msgs.as_mut_ptr(),
                    chunk.len() as libc::c_uint,
                    0,
                )
            };
            if n > 0 {
                at += n as usize;
                continue;
            }
            let e = std::io::Error::last_os_error();
            if e.kind() == ErrorKind::Interrupted {
                continue;
            }
            // The first datagram failed: drop it, as send_to would, and
            // go on with the rest.
            tracing::debug!("UDP send failed: {e}");
            failed += 1;
            at += 1;
        }
        self.queue.clear();
        failed
    }
}

/// Datagrams an engine reads in one `recvmmsg(2)`: it waits for the
/// first (up to the socket's read timeout) and takes whatever else is
/// already queued, up to [`RecvBatch::MAX`].
pub struct RecvBatch {
    bufs: Vec<[u8; RecvBatch::LEN]>,
    from: Vec<libc::sockaddr_storage>,
    lens: Vec<usize>,
    got: usize,
}

impl Default for RecvBatch {
    fn default() -> Self {
        RecvBatch {
            bufs: vec![[0; Self::LEN]; Self::MAX],
            // SAFETY: an all-zero sockaddr_storage is valid.
            from: vec![unsafe { std::mem::zeroed() }; Self::MAX],
            lens: vec![0; Self::MAX],
            got: 0,
        }
    }
}

impl RecvBatch {
    /// Datagrams per call (chosen).
    pub const MAX: usize = 32;
    /// Room per datagram: more than any MTU media is sent at.
    pub const LEN: usize = 2000;

    /// Reads a batch; Err as `recv_from` (WouldBlock/TimedOut: nothing came
    /// in time).
    pub fn recv(&mut self, socket: &UdpSocket) -> std::io::Result<usize> {
        // SAFETY: as in `SendBatch::flush`.
        let mut iov: [libc::iovec; Self::MAX] = unsafe { std::mem::zeroed() };
        let mut msgs: [libc::mmsghdr; Self::MAX] = unsafe { std::mem::zeroed() };
        for k in 0..Self::MAX {
            iov[k] = libc::iovec {
                iov_base: self.bufs[k].as_mut_ptr() as *mut libc::c_void,
                iov_len: Self::LEN,
            };
            let h = &mut msgs[k].msg_hdr;
            h.msg_name = &mut self.from[k] as *mut _ as *mut libc::c_void;
            h.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            h.msg_iov = &mut iov[k];
            h.msg_iovlen = 1;
        }
        self.got = 0;
        let n = loop {
            // SAFETY: the headers point into `iov`, `self.bufs` and
            // `self.from`, which outlive the call.
            let n = unsafe {
                libc::recvmmsg(
                    socket.as_raw_fd(),
                    msgs.as_mut_ptr(),
                    Self::MAX as libc::c_uint,
                    libc::MSG_WAITFORONE,
                    std::ptr::null_mut(),
                )
            };
            if n >= 0 {
                break n as usize;
            }
            let e = std::io::Error::last_os_error();
            if e.kind() != ErrorKind::Interrupted {
                return Err(e);
            }
        };
        for (len, msg) in self.lens.iter_mut().zip(&msgs[..n]) {
            *len = msg.msg_len as usize;
        }
        self.got = n;
        Ok(n)
    }

    /// The `k`th datagram of the last batch and who sent it.
    pub fn get(&self, k: usize) -> Option<(&[u8], SocketAddr)> {
        if k >= self.got {
            return None;
        }
        // SAFETY: the kernel filled in a sockaddr of the socket's family.
        let from = unsafe {
            SockAddr::new(
                std::ptr::read(&self.from[k] as *const _ as *const _),
                std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            )
        };
        Some((&self.bufs[k][..self.lens[k]], from.as_socket()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// More than one batch's worth reaches two sockets in order, and an
    /// unsendable datagram in the middle costs only itself.
    #[test]
    fn sends_in_batches() {
        let tx = UdpSocket::bind("127.0.0.1:0").unwrap();
        let rx: Vec<UdpSocket> = (0..2)
            .map(|_| {
                let s = UdpSocket::bind("127.0.0.1:0").unwrap();
                s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                s
            })
            .collect();
        let mut batch = SendBatch::default();
        let n = SendBatch::MAX + 10;
        for k in 0..n {
            if k == 5 {
                // IPv6 from an IPv4 socket: EAFNOSUPPORT.
                batch.push("[::1]:9".parse().unwrap(), vec![0]);
            }
            batch.push(
                rx[k % 2].local_addr().unwrap(),
                (k as u32).to_be_bytes().to_vec(),
            );
        }
        assert_eq!(batch.flush(&tx), 1);
        let mut buf = [0u8; 8];
        for k in 0..n {
            let len = rx[k % 2].recv(&mut buf).unwrap();
            assert_eq!(&buf[..len], (k as u32).to_be_bytes());
        }
        assert!(batch.queue.is_empty());
    }

    /// Whatever is queued comes in one call, with its senders; then a
    /// timeout as from `recv_from`.
    #[test]
    fn receives_in_batches() {
        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        rx.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let tx: Vec<UdpSocket> = (0..2)
            .map(|_| UdpSocket::bind("127.0.0.1:0").unwrap())
            .collect();
        for k in 0..5u8 {
            tx[usize::from(k) % 2]
                .send_to(&[k; 3], rx.local_addr().unwrap())
                .unwrap();
        }
        std::thread::sleep(Duration::from_millis(20));
        let mut batch = RecvBatch::default();
        assert_eq!(batch.recv(&rx).unwrap(), 5);
        for k in 0..5u8 {
            let (data, from) = batch.get(usize::from(k)).unwrap();
            assert_eq!(data, [k; 3]);
            assert_eq!(from, tx[usize::from(k) % 2].local_addr().unwrap());
        }
        assert!(batch.get(5).is_none());
        let e = batch.recv(&rx).unwrap_err();
        assert!(matches!(
            e.kind(),
            ErrorKind::WouldBlock | ErrorKind::TimedOut
        ));
    }
}
