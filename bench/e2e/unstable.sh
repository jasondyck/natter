#!/bin/bash
# unstable.sh natter|livekit <out.txt>: unstable.mjs over a lossy UDP path
# (LiveKit's UDP-unstable fallback). Needs no root: Chrome and the SDK page
# in a network namespace of their own (unshare), the server in a child one,
# joined by a veth pair delaying everything 40 ms each way (netem), and
# nftables dropping LOSS percent (default 35) of the UDP the server sends.
# TCP is untouched, so signalling and ICE/TCP stay clean. Both servers run
# with unstable.yaml (allow_udp_unstable_fallback, tcp_fallback_rtt_threshold).
# natter: target/release/natter-server; livekit: LK_BIN, the server binary
# (docker cp it out of livekit/livekit-server:v1.13.10). Needs what run.sh
# needs (setup.sh run once).
set -eu
here=$(cd "$(dirname "$0")" && pwd)
export SERVER=$1 LOSS=${LOSS:-35} LK_BIN=${LK_BIN:-}
out=$(realpath -m "$2")
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
  tc qdisc add dev n1 root netem delay 40ms limit 10000
  in_server ip link set lo up
  in_server ip addr add 10.99.0.1/24 dev n0
  in_server ip link set n0 up
  in_server tc qdisc add dev n0 root netem delay 40ms limit 10000
  in_server nft add table inet lossy
  in_server nft add chain inet lossy out "{ type filter hook output priority 0; }"
  in_server nft add rule inet lossy out udp sport 7882 numgen random mod 100 "<" $LOSS drop
  cd "$here/../.."
  if [ $SERVER = livekit ]; then
    in_server $LK_BIN --dev --bind 10.99.0.1 --node-ip 10.99.0.1 --config "$here/unstable.yaml" >"$here/lk.log" 2>&1 &
  else
    in_server env LIVEKIT_KEYS="devkey: secret" RUST_LOG=${RUST_LOG:-natter_server=info} \
      NATTER_SHARDS=1 NATTER_HTTP_ADDR=10.99.0.1:7880 NATTER_ORIGIN=http://localhost:7880 \
      NATTER_PUBLIC_IP=10.99.0.1 NATTER_UDP_PORT=7882 NATTER_TURN_PORT=off NATTER_DATA_DIR="$data" \
      target/release/natter-server --config "$here/unstable.yaml" >"$here/natter.log" 2>&1 &
  fi
  srv=$!
  cd "$here"
  python3 -m http.server 8766 --bind 127.0.0.1 -d www >/dev/null 2>&1 &
  web=$!
  trap "kill $srv $web $server 2>/dev/null; wait $srv; rm -rf $data" EXIT
  sleep 2
  LK_URL=ws://10.99.0.1:7880 timeout 150 node unstable.mjs >"$out" 2>&1 || true
' "$here" "$out"
