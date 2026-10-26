#!/bin/bash
# soak.sh OUT_DIR [HOURS]: natter for HOURS (default 12) under steady load
# plus churn, then a quiet period (QUIET s), sampling the server every
# SAMPLE s (60). DUR=<seconds> instead of HOURS for a short trial. What
# leaks shows as growth in samples.csv and as participants, rooms or fds
# left after the quiet period; summary.py reads the lot.
#
#   steady   natter-load for the whole run: STEADY_ROOMS audio rooms
#   churn    CHURN_LOOPS loops of short natter-load runs (audio, simulcast
#            video, loss, a thin downlink), a fifth of them SIGKILLed
#            partway so the server has to notice clients vanish
#   whip     every 3 min an H.264 WHIP publisher for 60 s (natter-whip;
#            every fifth killed), and every other time a WHEP viewer
#            (headless Chrome, whep-view.mjs) watching it for 30 s
#
# Its own natter (copied binary, so rebuilds don't touch it) on HTTP
# :7890, UDP 7900-7903, ICE/TCP 7899: e2e tests on :7880 can run meanwhile.
# Needs: cargo build --release (server and load), ffmpeg, and for WHEP
# PLAYWRIGHT_DIR, CHROME and bench/e2e/www served on :8766 (setup.sh).
set -u
OUT=$(mkdir -p "$1" && cd "$1" && pwd)
HOURS=${2:-12}
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
STEADY_ROOMS=${STEADY_ROOMS:-8}
CHURN_LOOPS=${CHURN_LOOPS:-3}
QUIET=${QUIET:-300}
URL=http://127.0.0.1:7890
DUR=${DUR:-$((HOURS * 3600))}
SAMPLE=${SAMPLE:-60}
end=$(($(date +%s) + DUR))

mkdir -p "$OUT/bin" "$OUT/data" "$OUT/churn"
cp "$repo/target/release/natter-server" "$repo/target/release/natter-load" "$repo/target/release/natter-whip" "$OUT/bin/"
export LIVEKIT_API_KEY=devkey LIVEKIT_API_SECRET=secret

LIVEKIT_KEYS="devkey: secret" RUST_LOG=natter_server=info NATTER_SHARDS=4 \
  NATTER_HTTP_ADDR=127.0.0.1:7890 NATTER_ORIGIN=http://localhost:7890 NATTER_PUBLIC_IP=127.0.0.1 \
  NATTER_UDP_PORT=7900 NATTER_TCP_PORT=7899 NATTER_TURN_PORT=off NATTER_DATA_DIR="$OUT/data" \
  "$OUT/bin/natter-server" >"$OUT/server.log" 2>&1 &
server=$!
echo $server >"$OUT/server.pid"
sleep 2
kill -0 $server || { echo "server did not start"; exit 1; }

# One line a minute: the process's memory, fds, threads and CPU, and the
# server's own gauges.
sample() {
  local t s
  t=$(date +%s)
  s=$(awk '/^VmRSS/{r=$2} /^Threads/{th=$2} END{print r","th}' /proc/$server/status 2>/dev/null) || return
  local fds cpu gauges
  fds=$(ls /proc/$server/fd 2>/dev/null | wc -l)
  cpu=$(awk '{print $14+$15}' /proc/$server/stat 2>/dev/null)
  gauges=$(curl -s --max-time 5 $URL/metrics | awk '
    $1 == "natter_peers" {p=$2} $1 == "natter_rooms" {r=$2} END {print p "," r}')
  echo "$t,$s,$fds,$cpu,$gauges" >>"$OUT/samples.csv"
}
echo "time,rss_kb,threads,fds,cpu_ticks,peers,rooms" >"$OUT/samples.csv"
(while kill -0 $server 2>/dev/null; do sample; sleep "$SAMPLE"; done) &
sampler=$!

load() { nice -n 10 "$OUT/bin/natter-load" --target livekit --url $URL --workers 2 "$@"; }
LOAD=(nice -n 10 "$OUT/bin/natter-load" --target livekit --url $URL --workers 2)

load --room-prefix steady --rooms "$STEADY_ROOMS" --publishers 3 --subscribers 3 \
  --warmup 30s --duration $((DUR - 120))s --pid $server --json "$OUT/steady.json" \
  >"$OUT/steady.log" 2>&1 &
steady=$!

churn() {
  local n=0 id=$1
  while [ "$(date +%s)" -lt $((end - 300)) ]; do
    n=$((n + 1))
    local secs=$((20 + RANDOM % 130)) args name
    case $((RANDOM % 4)) in
      0) name=audio args=(--rooms $((1 + RANDOM % 6)) --publishers $((2 + RANDOM % 4)) --subscribers $((1 + RANDOM % 5))) ;;
      1) name=video args=(--video --simulcast --video-kbps 800 --rooms $((1 + RANDOM % 2)) --publishers 2 --subscribers 2) ;;
      2) name=loss args=(--rooms $((1 + RANDOM % 4)) --publishers 3 --subscribers 3 --loss-down 5 --loss-up 5) ;;
      3) name=thin args=(--video --simulcast --rooms 1 --publishers 2 --subscribers 2 --down-kbps 1500 --down-delay-ms 60) ;;
    esac
    local tag="c$id-$n" killed=no limit=()
    if [ $((RANDOM % 5)) -eq 0 ]; then
      killed=yes limit=(timeout -s KILL $((5 + RANDOM % secs)))
    fi
    "${limit[@]}" "${LOAD[@]}" --room-prefix "$tag" "${args[@]}" --warmup 2s --duration ${secs}s \
      --ready-timeout 30s --json "$OUT/churn/$tag.json" >"$OUT/churn/$tag.log" 2>&1
    local rc=$?
    echo "$(date +%s),$tag,$name,${secs},$killed,$rc" >>"$OUT/churn.csv"
    sleep $((RANDOM % 10))
  done
}
echo "time,tag,scenario,secs,killed,rc" >"$OUT/churn.csv"
pids=()
# (2>: bash reports each killed run.)
for i in $(seq 1 "$CHURN_LOOPS"); do churn "$i" 2>>"$OUT/churn.err" & pids+=($!); done

token() { node -e '
const c=require("crypto"),b=(x)=>Buffer.from(x).toString("base64url"),n=Math.floor(Date.now()/1e3);
const h=b(JSON.stringify({alg:"HS256",typ:"JWT"})),p=b(JSON.stringify({iss:"devkey",sub:process.argv[1],name:process.argv[1],nbf:n,exp:n+3600,video:{room:"soak-whip",roomJoin:true,canPublish:true,canSubscribe:true}}));
console.log(h+"."+p+"."+c.createHmac("sha256","secret").update(h+"."+p).digest("base64url"))' "$1"; }
whip() {
  local n=0
  echo "time,n,whip_rc,whep" >"$OUT/whip.csv"
  while [ "$(date +%s)" -lt $((end - 300)) ]; do
    n=$((n + 1))
    local kill_at=70
    [ $((n % 5)) -eq 0 ] && kill_at=$((10 + RANDOM % 40))
    timeout -s KILL $kill_at bash -c "ffmpeg -hide_banner -loglevel error -re -f lavfi -i testsrc2=size=640x360:rate=30 -t 60 \
      -c:v libx264 -profile:v baseline -preset ultrafast -tune zerolatency -g 60 -bsf:v h264_metadata=aud=insert -f h264 - \
      | TOKEN=$(token cam) nice -n 10 '$OUT/bin/natter-whip' --codec h264 $URL/whip/v1" >>"$OUT/whip.log" 2>&1 &
    local w=$! viewed=-
    if [ $((n % 2)) -eq 0 ] && [ -n "${CHROME:-}" ]; then
      sleep 5
      viewed=$(TOK=$(token "viewer$n") WHEP=$URL/whep/v1?participant=cam timeout 90 node "$here/whep-view.mjs" 2>>"$OUT/whep.log")
    fi
    wait $w
    echo "$(date +%s),$n,$?,$viewed" >>"$OUT/whip.csv"
    sleep $((180 - 60))
  done
}
whip &
whipper=$!

wait "${pids[@]}" $whipper
wait $steady
echo "load done $(date)" >>"$OUT/events.log"
# Everyone has gone: what's left after the quiet period was never freed.
sleep "$QUIET"
sample
kill $sampler
kill $server
wait $server 2>/dev/null
echo "server stopped $(date)" >>"$OUT/events.log"
python3 "$here/summary.py" "$OUT" | tee "$OUT/summary.txt"
