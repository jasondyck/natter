#!/bin/bash
# h265.sh lk|natter out.txt: runs h265.mjs with a headed Chrome in a virtual
# KWin session (kwin_wayland --virtual: nothing shows on the desktop), since
# Chrome decodes H.265 only on the GPU and its GPU process fails to headless.
# Needs ffmpeg with libx265, a VA-API H.265 decoder and natter-whip built
# (cargo build --release -p natter-load).
set -u
here=$(cd "$(dirname "$0")" && pwd)
sock=natter-e2e-h265
kwin_wayland --virtual --no-lockscreen --socket "$sock" --width 1600 --height 1000 >/dev/null 2>&1 &
kwin=$!
sleep 2
WAYLAND_DISPLAY=$sock HEADFUL=1 "$here/run.sh" "$1" h265.mjs "$2"
kill "$kwin"
