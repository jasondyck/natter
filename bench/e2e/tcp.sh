#!/bin/bash
# tcp.sh <out.txt>: tcp.mjs against natter with UDP blocked, so ICE has to
# use TCP. Needs no root: Chrome and the SDK page run in a network namespace
# of their own (unshare), natter in a child one (3 engines, a room on each,
# one ICE/TCP port), joined by a veth pair; nftables drops UDP to and from
# natter's media ports. Chrome only runs ICE/TCP to a server on another host, hence
# two namespaces. Needs what run.sh needs (setup.sh run once, natter built).
set -eu
here=$(cd "$(dirname "$0")" && pwd)
out=$(realpath -m "$1")
exec unshare -rn bash -c '
  set -eu
  here=$0 out=$1
  data=$(mktemp -d)
  ip link set lo up
  unshare -n sleep infinity &
  server=$!
  sleep 0.2
  in_server() { nsenter -t $server -n "$@"; }
  ip link add n1 type veth peer name n0
  ip link set n0 netns $server
  ip addr add 10.99.0.2/24 dev n1
  ip link set n1 up
  in_server ip link set lo up
  in_server ip addr add 10.99.0.1/24 dev n0
  in_server ip link set n0 up
  nft add table inet noudp
  nft add chain inet noudp in "{ type filter hook input priority 0; }"
  nft add rule inet noudp in ip saddr 10.99.0.1 udp sport 7882-7884 drop
  nft add chain inet noudp out "{ type filter hook output priority 0; }"
  nft add rule inet noudp out ip daddr 10.99.0.1 udp dport 7882-7884 drop
  cd "$here/../.."
  nsenter -t $server -n env LIVEKIT_KEYS="devkey: secret" RUST_LOG=${RUST_LOG:-natter_server=debug} \
    NATTER_SHARDS=3 NATTER_SHARD_FILL=1 NATTER_HTTP_ADDR=0.0.0.0:7880 \
    NATTER_ORIGIN=http://localhost:7880 NATTER_PUBLIC_IP=10.99.0.1 NATTER_UDP_PORT=7882 \
    NATTER_TURN_PORT=off NATTER_DATA_DIR="$data" \
    target/release/natter-server >"$here/natter.log" 2>&1 &
  natter=$!
  cd "$here"
  python3 -m http.server 8766 --bind 127.0.0.1 -d www >/dev/null 2>&1 &
  web=$!
  # natter drains for up to 10 s after SIGTERM.
  trap "kill $natter $web $server 2>/dev/null; wait $natter; rm -rf $data" EXIT
  sleep 2
  LK_URL=ws://10.99.0.1:7880 timeout 150 node tcp.mjs >"$out" 2>&1 || true
' "$here" "$out"
