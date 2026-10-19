#!/bin/bash
# Same room through two nodes: publishers + 1 subscriber via A, 2 subscribers via B.
S=$(dirname "$0")
LKT="docker run --rm --network host --entrypoint /lk livekit/livekit-cli:latest"
A=${A:-7880}; B=${B:-7890}; ROOM=${ROOM:-shared}
timeout 60 $LKT load-test --url ws://127.0.0.1:$A --api-key devkey --api-secret secret --room $ROOM --audio-publishers 1 --video-publishers 1 --subscribers 1 --duration 15s > $S/lt-a.txt 2>&1 &
sleep 3
timeout 60 $LKT load-test --url ws://127.0.0.1:$B --api-key devkey --api-secret secret --room $ROOM --subscribers 2 --duration 10s > $S/lt-b.txt 2>&1 &
sleep 6
for u in $A $B; do echo "rooms via $u:"; $LKT room list --url http://127.0.0.1:$u --api-key devkey --api-secret secret 2>&1 | tail -3; echo "participants via $u:"; $LKT room participants list --url http://127.0.0.1:$u --api-key devkey --api-secret secret $ROOM 2>&1 | grep -c '"identity"\|Identity\|PA_'; done
wait
echo "== A"; tail -7 $S/lt-a.txt; echo "== B"; tail -7 $S/lt-b.txt
