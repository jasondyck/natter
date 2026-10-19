//! Non-blocking reads and writes for the TCP loops.

use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Write};

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
