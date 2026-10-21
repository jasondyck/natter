# JS SDK scenarios: natter against LiveKit

Each script drives real LiveKit JS SDK clients (livekit-client 2.23.0) in
headless Chrome through one scenario and prints what each participant
saw, in a stable order. Run it against LiveKit 1.13.10 and against natter,
then diff the two outputs: they should match, apart from the differences
listed in `docs/livekit/PARITY.md` ("Deliberate differences").

| Script | Covers |
|---|---|
| `perms.mjs` | Subscription permissions: per-identity/per-track grants, revoke, regrant, unsubscribe, recorder exemption |
| `hidden.mjs` | Hidden, recorder and agent participants: visibility, room counts (signalling and Twirp), `TrackSubscribed`, data from hidden senders, idle close with only dependents |
| `tw2.mjs` | Twirp JSON shape, key by key (volatile values masked) |
| `data.mjs` | Data packets with destinations, text and byte streams, RPC and its errors |
| `raw.mjs` | Crafted data packets: agents speaking for others, transcriptions, sid destinations, resends |
| `dtrack.mjs` | Data tracks: publish errors, frames, permissions, unpublish |
| `dtresume.mjs` | Data tracks across a signal resume |
| `trailer.mjs` | Packet trailers (frame metadata) with and without the worker |
| `e2ee.mjs` | End-to-end encryption with the right and a wrong key |
| `joindata.mjs` | Reliable data to a participant while it joins |
| `sstate.mjs` | `stream_state_update` around first subscribe, publisher mute/unmute and subscriber disable/enable |
| `shutdown.mjs` | Stopping the server mid-call: what clients are told and the webhooks that follow (`CONFIG=shutdown.yaml`) |
| `leave.mjs` | How soon the others hear a participant left (`ParticipantDisconnected`, track unsubscribed), in ms, five times |
| `mutes.mjs` | `track_muted`/`track_unmuted` webhooks from the client and an admin, against Stoat's LiveKit fork (`CONFIG=shutdown.yaml`, `LK_IMAGE=ghcr.io/stoatchat/livekit-server:v1.9.14`; natter with `NATTER_WEBHOOK_TRACK_MUTES=1`) |
| `fastpub.mjs` | `fast_publish`: which PCs connect at join, first publish reaching a subscriber (always dual-PC; `TIMES=1` prints the timings) |
| `agents/agents.py` | Agent dispatch with livekit-agents workers: automatic room and publisher jobs, explicit dispatch, token and CreateRoom dispatches, removal, errors (Python) |
| `agents/raw.py` | The `/agent` worker protocol by hand: JSON/protobuf, declined offers, `terminate`, full workers, simulated jobs, refused registrations, deployments (Python) |

## Running

```sh
cargo build --release          # run.sh does not build
bench/e2e/setup.sh             # SDK + workers into www/, served on :8766
export PLAYWRIGHT_DIR=/path/with/node_modules/playwright-core/   # directory to resolve playwright-core from
export CHROME=/path/to/chrome  # e.g. ~/.cache/ms-playwright/chromium-1248/chrome-linux64/chrome
cd bench/e2e
./run.sh lk perms.mjs lk.txt && ./run.sh natter perms.mjs natter.txt && diff lk.txt natter.txt
DUAL=1 ./run.sh natter perms.mjs natter-dual.txt   # dual-PC clients
```

Both servers use `devkey: secret` on 127.0.0.1:7880 (UDP/TCP 7882). Only
one can run at a time.

`CONFIG=<file.yaml>` gives both servers a LiveKit config file as well
(`shutdown.mjs` needs `CONFIG=shutdown.yaml` for its webhook receiver on
:8797). Scripts see `SERVER=lk|natter`.

## Agent scenarios (Python)

The agent scripts need livekit-agents (1.8.8 was used) and the server SDK
in a Python 3.12 environment; `run.sh` runs `*.py` with `$PYTHON`:

```sh
uv venv -p 3.12 /tmp/agents-venv
VIRTUAL_ENV=/tmp/agents-venv uv pip install "livekit-agents==1.*" livekit-api
cd bench/e2e
PYTHON=/tmp/agents-venv/bin/python ./run.sh lk agents/agents.py lk.txt
PYTHON=/tmp/agents-venv/bin/python ./run.sh natter agents/agents.py natter.txt
diff <(grep -v 'closed unexpectedly' lk.txt) natter.txt
```

`agents.py` starts three workers itself (`agents/worker.py`: unnamed room,
unnamed publisher, and `helper`), logging their jobs to
`$AGENTS_LOG_DIR/agents.jsonl` (worker output beside it). Expected
differences: the jobs' final `shutdown` reasons (see "Deliberate
differences" in `docs/livekit/PARITY.md`), and the SDK's "data channel
closed unexpectedly" lines against LiveKit.

