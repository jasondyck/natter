#!/bin/bash
# up.sh natter|lk: Redis (Docker, :6390) and two nodes on one host:
# A on HTTP 7880 (ICE/TCP 7881, UDP 7882), B on 7890 (7891, 7892).
# down.sh stops them. natter-server must be built (release).
set -eu
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
docker rm -f natter-redis >/dev/null 2>&1 || true
docker run -d --name natter-redis -p 127.0.0.1:6390:6379 redis:7-alpine >/dev/null
sleep 1
for n in a b; do
  port=$([ $n = a ] && echo 7880 || echo 7890)
  cat > "$here/$n.yaml" <<YAML
port: $port
bind_addresses: [127.0.0.1]
rtc:
  udp_port: $((port + 2))
  tcp_port: $((port + 1))
  node_ip: 127.0.0.1
keys:
  devkey: secret
redis:
  address: 127.0.0.1:6390
YAML
  if [ "$1" = lk ]; then
    docker rm -f lk-$n >/dev/null 2>&1 || true
    docker run -d --name lk-$n --network host -v "$here":/c livekit/livekit-server:v1.13.10 --config /c/$n.yaml >/dev/null
  else
    data=$(mktemp -d)
    (cd "$repo" && NATTER_DATA_DIR="$data" setsid nohup target/release/natter-server --config "$here/$n.yaml" >"$here/$n.log" 2>&1 &)
  fi
done
sleep 3
