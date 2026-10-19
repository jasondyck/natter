# str0m chat, for benchmarks

str0m 0.24.1's `examples/chat.rs`, changed only so `natter-load` can drive
it (`upstream.patch` is the whole diff):

- `?room=` on the offer POST keeps clients' media within their room;
  upstream, everyone is in one room.
- `?recv=0` marks a client that only publishes, so nothing is forwarded to
  it, as in the other servers' scenarios.
- Plain HTTP on `CHAT_HTTP` (default 127.0.0.1:3000) and UDP on `CHAT_UDP`
  (default 127.0.0.1:0), instead of TLS on :3000 and the first public
  interface.
- RustCrypto for SRTP and DTLS, as natter, instead of aws-lc.

Everything else is upstream: one thread; frame mode (packets are
reassembled into frames and packetized again); each packet is matched to its
client by asking every client in turn, and every client gets a timeout poll
on every pass of the loop. So it shows the library as its example uses it,
not the most str0m can do: its cost per packet grows with the number of
clients on the server.

    cargo build --release
    CHAT_HTTP=127.0.0.1:3900 target/release/str0m-chat
    ../../../target/release/natter-load --target str0m-chat --url http://127.0.0.1:3900 --rooms 4
