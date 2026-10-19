#!/bin/bash
# Stops what up.sh started.
pkill -x natter-server
docker rm -f lk-a lk-b natter-redis >/dev/null 2>&1
true
