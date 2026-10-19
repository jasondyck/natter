#!/usr/bin/env python3
"""Charts for docs/benchmarks/README.md from bench/compare.sh results.

usage: bench/charts.py DATA_DIR OUT_DIR

DATA_DIR holds one directory per scenario as compare.sh writes them
(r8/, r30/, ... for natter vs LiveKit; chat-r2/ ... for str0m chat), each
with <server>-<round>.json. Rounds are averaged. Needs matplotlib.
"""

import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("svg")
import matplotlib.pyplot as plt  # noqa: E402

NATTER = "#2563eb"
LIVEKIT = "#9ca3af"
CHAT = "#f59e0b"
plt.rcParams.update(
    {
        "font.family": "sans-serif",
        "font.size": 11,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.3,
        "axes.axisbelow": True,
        "svg.fonttype": "none",
    }
)


def load(directory, scenario):
    """{server: {rooms: [result, ...]}} from the scenario directories whose
    names match `scenario`."""
    out = defaultdict(lambda: defaultdict(list))
    dirs = [d for d in Path(directory).iterdir() if re.fullmatch(scenario, d.name)]
    for f in sorted(f for d in dirs for f in d.glob("*.json")):
        server = re.sub(r"-\d+$", "", f.stem).split("_")[0]
        r = json.loads(f.read_text())
        out[server][r["rooms"]].append(r)
    return out


def mean(results, key, sub=None):
    vals = [(r[sub] if sub else r)[key] for r in results]
    return statistics.mean(vals)


def grouped(ax, rooms, series, fmt):
    width = 0.8 / len(series)
    for i, (label, colour, values) in enumerate(series):
        xs = [x + (i - (len(series) - 1) / 2) * width for x in range(len(rooms))]
        bars = ax.bar(xs, values, width, label=label, color=colour)
        ax.bar_label(bars, labels=[fmt(v) for v in values], padding=2, fontsize=9)
    ax.set_xticks(range(len(rooms)), [f"{r} rooms\n{r * 800:,}/s" for r in rooms])
    ax.legend(frameon=False)


def main():
    data_dir, out_dir = Path(sys.argv[1]), Path(sys.argv[2])
    out_dir.mkdir(parents=True, exist_ok=True)
    data = load(data_dir, r"r\d+")
    rooms = sorted(set(data["natter"]) & set(data["livekit"]))

    for name, title, key, sub, fmt, scale in [
        ("cpu", "CPU (% of one core)", "cpu_percent", "server", lambda v: f"{v:.0f}%", 1),
        ("us-per-packet", "CPU time per forwarded packet (µs)", "cpu_us_per_forward",
         "server", lambda v: f"{v:.1f}", 1),
        ("memory", "Memory (MiB)", "memory_mib", "server", lambda v: f"{v:,.0f}", 1),
    ]:
        fig, ax = plt.subplots(figsize=(7.5, 4))
        grouped(
            ax,
            rooms,
            [
                ("natter", NATTER, [mean(data["natter"][r], key, sub) * scale for r in rooms]),
                ("LiveKit 1.13.10", LIVEKIT,
                 [mean(data["livekit"][r], key, sub) * scale for r in rooms]),
            ],
            fmt,
        )
        ax.set_title(title, loc="left", fontweight="bold")
        fig.tight_layout()
        fig.savefig(out_dir / f"{name}.svg")
        plt.close(fig)

    chats = load(data_dir, r"chat-r\d+")
    chat = chats.get("str0m-chat")
    if chat:
        crooms = sorted(chat)
        fig, ax = plt.subplots(figsize=(7.5, 4))
        ax.plot(crooms, [mean(chat[r], "cpu_us_per_forward", "server") for r in crooms],
                "o-", color=CHAT, label="str0m chat example")
        nat = sorted(chats["natter"])
        ax.plot(nat, [mean(chats["natter"][r], "cpu_us_per_forward", "server") for r in nat],
                "o-", color=NATTER, label="natter")
        for r in crooms:
            loss = mean(chat[r], "loss_percent")
            if loss > 1:
                ax.annotate(f"{loss:.0f}% lost", (r, mean(chat[r], "cpu_us_per_forward", "server")),
                            textcoords="offset points", xytext=(-60, -4), fontsize=9)
        ax.set_xlabel("rooms of 4 + 4")
        ax.set_xticks(crooms)
        ax.set_ylim(bottom=0)
        ax.set_title("CPU time per forwarded packet (µs)", loc="left", fontweight="bold")
        ax.legend(frameon=False)
        fig.tight_layout()
        fig.savefig(out_dir / "str0m-chat.svg")
        plt.close(fig)


if __name__ == "__main__":
    main()
