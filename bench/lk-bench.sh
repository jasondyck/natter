#!/bin/bash
# Part of natter: see README "Load testing". Needs `lk` (copied out of the
# livekit/livekit-cli image) next to this script or on PATH as $LK.
# usage: bench.sh TARGET ROOMS   (TARGET = natter:<pid> | livekit)
# Starts ROOMS `lk load-test` rooms of 4 audio publishers + 4 subscribers
# against localhost:7880, then samples the server's CPU for 15s. natter
# needs NATTER_MAX_PEERS above 8 x ROOMS.
S=$(dirname "$0"); T=$1; R=$2
L=$(mktemp -d)   # lk output per room
cpu_usec() {
  case $T in
    livekit) awk '/usage_usec/{print $2}' /sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' lk-bench).scope/cpu.stat ;;
    natter:*) pid=${T#natter:}; awk -v t=$(getconf CLK_TCK) '{print ($14+$15)*1000000/t}' /proc/$pid/stat ;;
  esac
}
rss() {
  case $T in
    livekit) docker stats --no-stream --format '{{.MemUsage}}' lk-bench | cut -d/ -f1 ;;
    natter:*) awk '/VmRSS/{printf "%.0fMiB", $2/1024}' /proc/${T#natter:}/status ;;
  esac
}
for i in $(seq 1 $R); do
  "${LK:-$S/lk}" load-test --dev --room b$R-$i --audio-publishers 4 --subscribers 4 --num-per-second 8 --duration 60s > "$L/out-$R-$i.log" 2>&1 &
done
sleep 30
u0=$(cpu_usec); t0=$(date +%s%N); sleep 15; u1=$(cpu_usec); t1=$(date +%s%N)
mem=$(rss)
wait
fwd=$((R*16*50))
recv=$(grep -h "│ Total" "$L"/out-$R-*.log | wc -l)
loss=$(grep -h "│ Total" "$L"/out-$R-*.log | grep -oE '\(([0-9.]+)%\)' | sort -u | tr '\n' ' ')
awk -v a=$u0 -v b=$u1 -v t0=$t0 -v t1=$t1 -v f=$fwd -v r=$R -v m="$mem" -v ok=$recv -v l="$loss" -v tg="${T%%:*}" 'BEGIN{
  cpu=(b-a)*100000/(t1-t0); printf "%-7s rooms=%-3d fwd/s=%-6d cpu=%6.1f%% us/fwd=%6.2f rss=%-9s rooms_reporting=%d loss=%s\n", tg, r, f, cpu, (b-a)*1e9/(t1-t0)/f, m, ok, l}'
