#!/bin/bash
# Part of natter: see README "Load testing". Runs natter-load against each
# server in turn, starting every server fresh, ROUNDS times round, so the
# servers are compared within one session (results drift up to ~25%
# between sessions). One JSON per run lands in $OUT; a table is printed.
#
# usage: bench/compare.sh ROOMS [ROUNDS] SERVER...   (natter-load options in $LOAD_ARGS)
#   natter       natter, one engine            (target/release/natter-server)
#   natter:N     natter, N engines packed 250 participants each
#   livekit      livekit/livekit-server in Docker ($LIVEKIT_IMAGE)
#   str0m-chat   str0m's chat example, patched for rooms (bench/servers/str0m-chat)
# Needs: cargo build --release -p natter-server -p natter-load, Docker for
# LiveKit, and `cargo build --release` in bench/servers/str0m-chat.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOMS=$1; shift
ROUNDS=1; [[ ${1:-} =~ ^[0-9]+$ ]] && { ROUNDS=$1; shift; }
OUT=${OUT:-$(mktemp -d /tmp/natter-compare.XXXX)}; mkdir -p "$OUT"
LIVEKIT_IMAGE=${LIVEKIT_IMAGE:-livekit/livekit-server:v1.13.10}
LOAD=target/release/natter-load
DATA=$(mktemp -d)
PID=""

stop() {
  [[ -n $PID ]] && kill "$PID" 2>/dev/null && while kill -0 "$PID" 2>/dev/null; do sleep 0.2; done
  PID=""
  docker rm -f natter-compare-lk >/dev/null 2>&1 || true
}
trap stop EXIT

wait_http() { for _ in $(seq 50); do curl -s -o /dev/null "$1" && return; sleep 0.2; done; echo "no server at $1" >&2; exit 1; }

run() { # SERVER ROUND
  local server=$1 round=$2 probe url=http://127.0.0.1:7880 target=livekit
  local name=${server/:/_}   # natter:16 -> natter_16: no colons in file names
  case $server in
    natter|natter:*)
      local shards=1; [[ $server == natter:* ]] && shards=${server#natter:}
      rm -rf "$DATA"/*
      NATTER_SHARDS=$shards NATTER_SHARD_FILL=250 NATTER_MAX_PEERS=5000 LIVEKIT_KEYS="devkey: secret" \
        RUST_LOG=natter_server=warn NATTER_HTTP_ADDR=127.0.0.1:7880 NATTER_ORIGIN=http://localhost:7880 \
        NATTER_PUBLIC_IP=127.0.0.1 NATTER_UDP_PORT=7882 NATTER_DATA_DIR="$DATA" \
        target/release/natter-server >"$OUT/$name-$round.log" 2>&1 &
      PID=$!; probe="--pid $PID" ;;
    livekit)
      docker run -d --name natter-compare-lk --network host "$LIVEKIT_IMAGE" \
        --dev --bind 127.0.0.1 --node-ip 127.0.0.1 >/dev/null
      probe="--cgroup /sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' natter-compare-lk).scope" ;;
    str0m-chat)
      CHAT_HTTP=127.0.0.1:3900 CHAT_UDP=127.0.0.1:7882 RUST_LOG=warn \
        bench/servers/str0m-chat/target/release/str0m-chat >"$OUT/$name-$round.log" 2>&1 &
      PID=$!; probe="--pid $PID"; url=http://127.0.0.1:3900; target=str0m-chat ;;
    *) echo "unknown server $server" >&2; exit 1 ;;
  esac
  wait_http "$url"
  # shellcheck disable=SC2086
  $LOAD --target $target --url $url --rooms "$ROOMS" $probe --room-prefix "r$round" \
    --json "$OUT/$name-$round.json" ${LOAD_ARGS:-} >"$OUT/$name-$round.txt" 2>&1 \
    || echo "$server round $round failed: see $OUT/$name-$round.txt" >&2
  stop
}

for round in $(seq "$ROUNDS"); do
  for server in "$@"; do run "$server" "$round"; done
done

printf '%-14s %5s %8s %8s %9s %8s %8s %9s %8s\n' server rooms fwd/s cpu% us/fwd MiB loss% p50ms p99ms
for f in "$OUT"/*.json; do
  [[ -e $f ]] || continue
  jq -r --arg s "$(basename "$f" .json)" '[$s, .rooms, (.forwarded_per_s|round),
      (.server.cpu_percent*10|round/10), (.server.cpu_us_per_forward*10|round/10),
      (.server.memory_mib|round), (.loss_percent*1000|round/1000),
      (.latency_ms_p50*100|round/100), (.latency_ms_p99*100|round/100)] | @tsv' "$f"
done | sort | while IFS=$'\t' read -r s r f c u m l p50 p99; do
  printf '%-14s %5s %8s %8s %9s %8s %8s %9s %8s\n' "$s" "$r" "$f" "$c" "$u" "$m" "$l" "$p50" "$p99"
done
echo "results in $OUT"
