#!/bin/bash
# Fetches the JS SDK these tests were checked with (livekit-client 2.23.0)
# into www/, with its E2EE and frame-metadata workers, and serves www/ on
# :8766. Needs npm and python3.
set -euo pipefail
cd "$(dirname "$0")"
tmp=$(mktemp -d)
(cd "$tmp" && npm pack livekit-client@2.23.0 --silent >/dev/null && tar xzf livekit-client-2.23.0.tgz)
cp "$tmp/package/dist/livekit-client.umd.js" www/livekit-client.js
cp "$tmp/package/dist/livekit-client.e2ee.worker.js" "$tmp/package/dist/livekit-client.fm.worker.js" www/
rm -rf "$tmp"
if ! ss -ltn | grep -q ':8766 '; then
  (cd www && setsid nohup python3 -m http.server 8766 --bind 127.0.0.1 >/dev/null 2>&1 &)
fi
echo "www/ ready on http://localhost:8766"
