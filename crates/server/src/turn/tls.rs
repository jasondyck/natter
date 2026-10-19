//! TURN over TLS with rustls: records in and out of a connection's
//! buffers, so the TURN loop treats TLS and TCP alike.

use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use mio::net::TcpStream;
use rustls::ServerConnection;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub fn config(cert: &Path, key: &Path) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    let certs = CertificateDer::pem_file_iter(cert)
        .and_then(|c| c.collect::<Result<Vec<_>, _>>())
        .with_context(|| format!("reading {}", cert.display()))?;
    let key =
        PrivateKeyDer::from_pem_file(key).with_context(|| format!("reading {}", key.display()))?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("TURN TLS certificate")?;
    Ok(Arc::new(config))
}

/// Reads records from the socket, decrypting into `plain`; handshake
/// replies go to `out`. Err: closed or broken.
pub fn read(
    tls: &mut ServerConnection,
    stream: &mut TcpStream,
    plain: &mut Vec<u8>,
    out: &mut VecDeque<u8>,
    buf: &mut [u8],
) -> Result<(), ()> {
    loop {
        match tls.read_tls(stream) {
            Ok(0) => return Err(()),
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(_) => return Err(()),
        }
        let state = tls.process_new_packets();
        // An alert for the client, if any, before giving up.
        let _ = drain(tls, out);
        let state = state.map_err(|_| ())?;
        if state.plaintext_bytes_to_read() > 0 {
            loop {
                match tls.reader().read(buf) {
                    Ok(0) => break,
                    Ok(n) => plain.extend_from_slice(&buf[..n]),
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(_) => return Err(()),
                }
            }
        }
        if state.peer_has_closed() {
            return Err(());
        }
    }
    drain(tls, out)
}

/// Encrypts `data` into `out`.
pub fn write(tls: &mut ServerConnection, out: &mut VecDeque<u8>, data: &[u8]) {
    let _ = tls.writer().write_all(data);
    let _ = drain(tls, out);
}

fn drain(tls: &mut ServerConnection, out: &mut VecDeque<u8>) -> Result<(), ()> {
    while tls.wants_write() {
        tls.write_tls(out).map_err(|_| ())?;
    }
    Ok(())
}
