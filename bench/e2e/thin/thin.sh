#!/bin/bash
# thin.sh natter|livekit OUT.json [KBPS]: a Chrome call (call.mjs) through a
# thin link, as natter-load's --down-kbps 1000 --down-delay-ms 150, but with
# Chrome's own receivers. Needs no root (the kernel's tc may not be
# available): three network namespaces, Chrome (10.99.0.2) -- router.py
# (10.99.0.1 | 10.98.0.2) -- the server (10.98.0.1), which has no route to
# Chrome, so all media crosses the router's shaped downlink.
# natter: target/release/natter-server (NATTER_BIN to override).
# livekit: LK_BIN, the server binary (docker cp it out of
# livekit/livekit-server:v1.13.10), with lk.yaml (send-side BWE).
# Also needs what ../run.sh needs (setup.sh's www/, PLAYWRIGHT_DIR, CHROME).
# OUT gets call.mjs's result plus the router's report under "edge".
set -u
here=$(cd "$(dirname "$0")" && pwd)
export SERVER=$1 OUT=$(realpath -m "$2") KBPS=${3:-1000} here
export NATTER_BIN=${NATTER_BIN:-$here/../../../target/release/natter-server} LK_BIN=${LK_BIN:-}
export WORK=$(mktemp -d)
unshare -rn bash -c '
  set -u
  ip link set lo up
  unshare -n sleep infinity & R=$!
  unshare -n sleep infinity & S=$!
  sleep 0.2
  inR() { nsenter -t $R -n "$@"; }; inS() { nsenter -t $S -n "$@"; }
  ip link add c0 type veth peer name r0; ip link set r0 netns $R
  ip addr add 10.99.0.2/24 dev c0; ip link set c0 up
  ip link add s0 type veth peer name r1; ip link set s0 netns $S; ip link set r1 netns $R
  inR ip link set lo up; inR ip addr add 10.99.0.1/24 dev r0; inR ip link set r0 up
  inR ip addr add 10.98.0.2/24 dev r1; inR ip link set r1 up
  inS ip link set lo up; inS ip addr add 10.98.0.1/24 dev s0; inS ip link set s0 up
  data=$(mktemp -d)
  # Background nsenter itself (not a function), so $! is the process.
  if [ $SERVER = livekit ]; then
    nsenter -t $S -n $LK_BIN --dev --bind 10.98.0.1 --node-ip 10.99.0.1 --config $here/lk.yaml >$WORK/server.log 2>&1 &
  else
    nsenter -t $S -n env LIVEKIT_KEYS="devkey: secret" RUST_LOG=natter_server=warn NATTER_SHARDS=1 \
      NATTER_HTTP_ADDR=10.98.0.1:7880 NATTER_ORIGIN=http://localhost:7880 NATTER_PUBLIC_IP=10.99.0.1 \
      NATTER_UDP_BIND=10.98.0.1 NATTER_UDP_PORT=7882 NATTER_TCP_PORT=off NATTER_TURN_PORT=off \
      NATTER_DATA_DIR=$data $NATTER_BIN >$WORK/server.log 2>&1 &
  fi
  srv=$!
  nsenter -t $R -n python3 $here/router.py $WORK $KBPS 200 150 >$WORK/router.log 2>&1 &
  router=$!
  python3 -m http.server 8766 --bind 127.0.0.1 -d $here/../www >/dev/null 2>&1 &
  web=$!
  sleep 2
  cd $here
  LK_URL=ws://10.99.0.1:7880 ROUTER_PID=$router OUT=$WORK/call.json timeout 200 node call.mjs >$WORK/call.log 2>&1
  sleep 0.5
  kill $srv; wait $srv
  kill $router $web $R $S 2>/dev/null
  rm -rf $data
'
jq -s '.[0] + {edge: .[1]}' $WORK/call.json $WORK/edge-report.json >"$OUT" \
  && rm -rf $WORK || echo "failed: see $WORK" >&2
