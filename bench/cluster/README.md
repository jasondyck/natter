# Multi-node checks

Two nodes and Redis on one host, natter or LiveKit 1.13.10, for the
scenarios multi-node was checked with (`docs/livekit/PARITY.md`, phase 9).

```sh
cargo build --release
bench/cluster/up.sh natter        # or: up.sh lk
bench/cluster/shared.sh           # one room joined through both nodes
target/release/natter-server --config bench/cluster/a.yaml list-nodes
bench/cluster/down.sh
```

- `shared.sh`: `lk load-test` publishers and a subscriber through node A,
  two more subscribers through node B, in the same room. Both nodes list
  the room; B's subscribers receive A's publishers (signalling relayed to
  the room's node, media straight to it).
- Agents across nodes: run `bench/e2e/agents/agents.py` with
  `LIVEKIT_URL=ws://127.0.0.1:7880 WORKER_URL=ws://127.0.0.1:7890`: the
  workers register with B, rooms land on either node, and the output is
  the same as against a single node, but for the race in the final
  `shutdown` reasons (one job may say "parent process shutdown"; LiveKit's
  own two-node cluster does the same).
- Failover: kill one node (`kill -9`), wait 5 s, and join its rooms through
  the other; they are placed afresh there. A node stopped with SIGTERM
  leaves the cluster and frees its rooms at once.
