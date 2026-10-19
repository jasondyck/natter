#!/bin/bash
# run.sh lk|natter <script.mjs|script.py> <out.txt>: starts LiveKit 1.13.10 (Docker,
# --dev) or natter (target/release/natter-server, built beforehand: this
# does not build) on :7880/:7882, runs the script, stops the server.
# DUAL=1 in the environment runs the clients in dual-PC mode.
set -u
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
data=$(mktemp -d)
if [ "$1" = lk ]; then
  docker rm -f natter-e2e-lk >/dev/null 2>&1
  docker run -d --name natter-e2e-lk --network host livekit/livekit-server:v1.13.10 \
    --dev --bind 127.0.0.1 --node-ip 127.0.0.1 >/dev/null
  sleep 3
else
  (cd "$repo" && LIVEKIT_KEYS="devkey: secret" RUST_LOG=natter_server=debug NATTER_SHARDS=1 \
    NATTER_HTTP_ADDR=127.0.0.1:7880 NATTER_ORIGIN=http://localhost:7880 NATTER_PUBLIC_IP=127.0.0.1 \
    NATTER_UDP_PORT=7882 NATTER_TURN_PORT=off NATTER_DATA_DIR="$data" \
    setsid nohup target/release/natter-server >"$here/natter.log" 2>&1 &)
  sleep 2
fi
case "$2" in
  *.py) (cd "$here" && timeout 150 "${PYTHON:-python3}" "$2" >"$3" 2>&1) ;;
  *) (cd "$here" && timeout 150 node "$2" >"$3" 2>&1) ;;
esac
if [ "$1" = lk ]; then
  docker logs natter-e2e-lk >"$here/lk.log" 2>&1
  docker rm -f natter-e2e-lk >/dev/null
else
  pkill -x natter-server
fi
rm -rf "$data"
