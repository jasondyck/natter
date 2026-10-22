#!/usr/bin/env python3
"""A NAT-ing router with a shaped downlink, between Chrome and a server.

  router.py OUTDIR KBPS QUEUE_MS UP_MS

Runs in a netns with 10.99.0.1 (towards Chrome) and 10.98.0.2 (towards the
server at 10.98.0.1). Forwards TCP 7880 (signalling) as is, and UDP 7882
(media) per client address (each peer connection) through its own socket,
so the server sees 10.98.0.2:<port> as the client. Server->client packets
queue at KBPS and are dropped past QUEUE_MS of queue (tail drop, as
natter-load's --down-kbps); client->server packets are delayed UP_MS (the
round trip, as --down-delay-ms). SIGUSR1 starts measuring, SIGUSR2 writes
OUTDIR/edge-report.json: per link packets/drops, queueing delay and drops by
RTP payload type.
"""
import asyncio, json, os, signal, socket, sys, time
from collections import defaultdict

OUT, KBPS, QUEUE_MS, UP_MS = sys.argv[1], float(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
CLIENT_SIDE, SERVER_SIDE, SERVER = "10.99.0.1", "10.98.0.2", "10.98.0.1"
RATE, QMAX, UP = KBPS * 1000 / 8, QUEUE_MS / 1000, UP_MS / 1000


class Link:
    def __init__(self):
        self.busy_until = 0.0
        self.sent = self.dropped = 0


def pt_of(data):
    if len(data) >= 12 and data[0] >> 6 == 2 and not 192 <= data[1] <= 223:
        return data[1] & 0x7F
    return None  # STUN, DTLS, RTCP


def pct(xs, p):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(p * len(xs)))], 1) if xs else None


async def pipe(r, w):
    try:
        while data := await r.read(65536):
            w.write(data)
            await w.drain()
    except Exception:
        pass
    finally:
        w.close()


async def main():
    loop = asyncio.get_running_loop()
    front = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    front.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 << 20)
    front.bind((CLIENT_SIDE, 7882))
    front.setblocking(False)
    backs = {}
    links = defaultdict(Link)
    qdelay, drops_pt = defaultdict(list), defaultdict(int)
    measuring = [False]

    def to_client(addr, data):
        try:
            front.sendto(data, addr)
        except OSError:
            pass

    def from_server(addr, s):
        while True:
            try:
                data = s.recv(65536)
            except (BlockingIOError, ConnectionRefusedError):
                return
            l = links[addr]
            now = time.monotonic()
            start = max(l.busy_until, now)
            pt = pt_of(data)
            if start - now > QMAX:
                l.dropped += 1
                if measuring[0] and pt is not None:
                    drops_pt[pt] += 1
                continue
            l.busy_until = start + (len(data) + 28) / RATE
            l.sent += 1
            if measuring[0] and pt is not None:
                qdelay[pt].append((l.busy_until - now) * 1000)
            loop.call_later(l.busy_until - now, to_client, addr, data)

    def to_server(s, data):
        try:
            s.send(data)
        except OSError:
            pass

    def from_client():
        while True:
            try:
                data, addr = front.recvfrom(65536)
            except BlockingIOError:
                return
            s = backs.get(addr)
            if s is None:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 << 20)
                s.bind((SERVER_SIDE, 0))
                s.connect((SERVER, 7882))
                s.setblocking(False)
                backs[addr] = s
                loop.add_reader(s, from_server, addr, s)
            loop.call_later(UP, to_server, s, data)

    loop.add_reader(front, from_client)

    async def on_tcp(r, w):
        try:
            sr, sw = await asyncio.open_connection(SERVER, 7880)
        except OSError:
            w.close()
            return
        await asyncio.gather(pipe(r, sw), pipe(sr, w))

    def start_measuring():
        qdelay.clear()
        drops_pt.clear()
        measuring[0] = True

    def report():
        out = {
            "links": {f"{a[0]}:{a[1]}": [l.sent, l.dropped] for a, l in links.items()},
            "queue_ms_by_pt": {pt: {"n": len(v), "p50": pct(v, 0.5), "p99": pct(v, 0.99), "max": pct(v, 1.0)} for pt, v in sorted(qdelay.items())},
            "drops_by_pt": dict(sorted(drops_pt.items())),
        }
        with open(os.path.join(OUT, "edge-report.json"), "w") as f:
            json.dump(out, f, indent=1)

    loop.add_signal_handler(signal.SIGUSR1, start_measuring)
    loop.add_signal_handler(signal.SIGUSR2, report)
    server = await asyncio.start_server(on_tcp, CLIENT_SIDE, 7880)
    async with server:
        await server.serve_forever()


asyncio.run(main())
