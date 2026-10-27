#!/usr/bin/env python3
"""summary.py OUT_DIR: what soak.sh measured. Memory, fds and threads at
the start, the end of the load and after the quiet period, with the
memory trend over the loaded hours; what the server still counted once
everyone had gone; the churn and WHIP/WHEP runs that failed; and the
server's warnings, by kind."""
import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path

out = Path(sys.argv[1])
rows = list(csv.DictReader(open(out / "samples.csv")))
if len(rows) < 3:
    sys.exit("too few samples")


def gauges(r):
    return {"peers": r["peers"], "rooms": r["rooms"]}


t0 = int(rows[0]["time"])
hours = lambda r: (int(r["time"]) - t0) / 3600
rss = lambda r: int(r["rss_kb"]) / 1024

# The loaded part: up to the last sample before the quiet one.
loaded = rows[:-1]
quiet = rows[-1]
print(f"samples: {len(rows)} over {hours(quiet):.1f} h")
for label, r in [("first", rows[0]), ("+1 h", next((r for r in rows if hours(r) >= 1), rows[0])),
                 ("end of load", loaded[-1]), ("after quiet", quiet)]:
    print(f"  {label:12} {hours(r):5.1f} h  rss {rss(r):7.1f} MiB  fds {r['fds']:>5}  threads {r['threads']:>3}  {gauges(r)}")

# Memory trend after the first hour (allocator warm-up): least squares.
warm = [r for r in loaded if hours(r) >= 1]
if len(warm) >= 10:
    xs = [hours(r) for r in warm]
    ys = [rss(r) for r in warm]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sum((x - mx) ** 2 for x in xs)
    print(f"rss trend after the first hour: {slope:+.2f} MiB/h (min {min(ys):.1f}, max {max(ys):.1f})")
    fd = [int(r["fds"]) for r in warm]
    print(f"fds after the first hour: min {min(fd)} max {max(fd)}")

left = {k: v for k, v in gauges(quiet).items() if v and float(v) != 0}
print("left after quiet period:", left or "nothing")

churn = list(csv.DictReader(open(out / "churn.csv")))
bad = [c for c in churn if c["killed"] == "no" and c["rc"] != "0"]
print(f"churn runs: {len(churn)} ({sum(c['killed'] == 'yes' for c in churn)} killed), failed: {len(bad)}")
for c in bad[:10]:
    log = (out / "churn" / f"{c['tag']}.log").read_text(errors="replace").strip().splitlines()
    print(f"  {c['tag']} {c['scenario']}: rc {c['rc']}: {log[-1] if log else ''}")
by = Counter()
for c in churn:
    j = out / "churn" / f"{c['tag']}.json"
    if c["killed"] == "no" and j.exists():
        r = json.loads(j.read_text())
        loss = r.get("loss_percent")
        if loss is None and r.get("video"):
            loss = r["video"].get("loss_percent")
        if loss is not None and loss > (6 if c["scenario"] == "loss" else 1):
            by[c["scenario"]] += 1
print("churn runs with loss over 1% (6% with injected loss):", dict(by) or "none")

if (out / "steady.json").exists():
    s = json.loads((out / "steady.json").read_text())
    keys = ["loss_percent", "latency_ms_p50", "latency_ms_p99", "latency_ms_max"]
    print("steady:", {k: s.get(k) for k in keys}, "server", s.get("server"))
else:
    print("steady: no result;", (out / "steady.log").read_text(errors="replace").strip().splitlines()[-1:])

if (out / "whip.csv").exists():
    w = list(csv.DictReader(open(out / "whip.csv")))
    views = [int(x["whep"]) for x in w if x["whep"] not in ("-", "")]
    print(f"whip runs: {len(w)}, whep views: {len(views)}, views with no frames: {sum(v == 0 for v in views)}")

warn = Counter()
for line in open(out / "server.log", errors="replace"):
    m = re.search(r"\b(WARN|ERROR)\b.*?:\s+(.*)", re.sub(r"\x1b\[[0-9;]*m", "", line))
    if m:
        msg = re.sub(r"\b[0-9a-f]{6,}\b|\d+(\.\d+)?", "#", m.group(2))[:90]
        warn[(m.group(1), msg)] += 1
print("server warnings:")
for (lvl, msg), n in warn.most_common(15):
    print(f"  {n:6} {lvl} {msg}")
