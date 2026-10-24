#!/bin/bash
# Stops what up.sh started (by pid: a natter running as a service is left alone).
here=$(cd "$(dirname "$0")" && pwd)
if [ -f "$here/pids" ]; then
  xargs kill <"$here/pids" 2>/dev/null
  rm -f "$here/pids"
fi
docker rm -f lk-a lk-b natter-redis >/dev/null 2>&1
true
