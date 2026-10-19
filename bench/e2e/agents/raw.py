"""The /agent worker protocol spoken by hand: what the agents frameworks
leave out. Two workers with the same agent name, one declining jobs; JSON
and protobuf framing; load and full status; `terminate` answers; simulated
jobs; the agent token's claims; registrations the server must refuse.
Prints what the workers saw, ids masked.
"""

import asyncio
import base64
import json
import os
import re
import sys

import aiohttp
from google.protobuf import json_format
from livekit import api
from livekit.protocol import agent as pa

URL = "http://127.0.0.1:7880"
KEY, SECRET = "devkey", "secret"


def mask(s):
    s = re.sub(r"\b(AD|AJ|AW|PA|RM|ND)_[A-Za-z0-9]+", r"\1_*", str(s))
    return re.sub(r'"(\d{10,})"|\b\d{10,}\b', "<t>", s)


def out(*p):
    print(mask(" ".join(str(x) for x in p)), flush=True)


def agent_token():
    return api.AccessToken(KEY, SECRET).with_grants(api.VideoGrants(agent=True)).to_jwt()


class Worker:
    def __init__(self, name, ws, json_mode=False):
        self.name, self.ws, self.json = name, ws, json_mode

    @classmethod
    async def connect(cls, session, name, json_mode=False, token=None):
        ws = await session.ws_connect(f"{URL.replace('http', 'ws')}/agent?protocol=1",
                                      headers={"Authorization": "Bearer " + (token or agent_token())})
        return cls(name, ws, json_mode)

    async def send(self, **fields):
        msg = pa.WorkerMessage(**fields)
        if self.json:
            await self.ws.send_str(json_format.MessageToJson(msg))
        else:
            await self.ws.send_bytes(msg.SerializeToString())

    async def recv(self, timeout=3):
        try:
            m = await self.ws.receive(timeout=timeout)
        except asyncio.TimeoutError:
            return None
        if m.type == aiohttp.WSMsgType.TEXT:
            return ("json", json_format.Parse(m.data, pa.ServerMessage(), ignore_unknown_fields=True))
        if m.type == aiohttp.WSMsgType.BINARY:
            return ("pb", pa.ServerMessage.FromString(m.data))
        return ("closed", m.type.name)

    async def expect(self, timeout=3):
        r = await self.recv(timeout)
        if r is None:
            return None, None
        kind, msg = r
        if kind == "closed":
            return "closed", None
        return msg.WhichOneof("message"), msg


def claims(tok):
    payload = tok.split(".")[1]
    c = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    keep = {k: c[k] for k in ("sub", "name", "kind", "metadata", "attributes", "video") if k in c}
    keep["ttl"] = c["exp"] - c["nbf"] if "nbf" in c else c["exp"]
    return json.dumps(keep, sort_keys=True)


async def main():
    lk = api.LiveKitAPI(URL, KEY, SECRET)

    async def listed(room):
        # LiveKit records a job a while after the worker took it.
        t0 = asyncio.get_running_loop().time()
        while asyncio.get_running_loop().time() - t0 < 8:
            if any(x.state.jobs for x in await lk.agent_dispatch.list_dispatch(room)):
                break
            await asyncio.sleep(0.1)

    async with aiohttp.ClientSession() as s:
        out("== 1. access")
        async with s.get(f"{URL}/agent") as r:
            out("plain GET", r.status)
        try:
            await Worker.connect(s, "x", token=api.AccessToken(KEY, SECRET).with_identity("x").with_grants(api.VideoGrants(room_join=True, room="a")).to_jwt())
            out("no agent grant: connected")
        except aiohttp.WSServerHandshakeError as e:
            out("no agent grant:", e.status)

        out("== 2. JSON registration and ping")
        j = await Worker.connect(s, "json", json_mode=True)
        await j.send(ping=pa.WorkerPing(timestamp=5))
        kind, m = await j.expect()
        out("ping before register ->", kind, m.pong.last_timestamp if m else None)
        await j.send(register=pa.RegisterWorkerRequest(type=pa.JT_ROOM, agent_name="raw", version="t1"))
        r = await j.recv()
        out("register ->", r[0], r[1].WhichOneof("message"), r[1].register.worker_id[:3],
            r[1].register.server_info.agent_protocol, r[1].register.server_info.protocol,
            r[1].register.server_info.edition)

        out("== 3. a worker that declines, one that accepts")
        b = await Worker.connect(s, "pb")
        await b.send(register=pa.RegisterWorkerRequest(type=pa.JT_ROOM, agent_name="raw", version="t2"))
        kind, m = await b.expect()
        out("pb register ->", kind)
        d = await lk.agent_dispatch.create_dispatch(api.CreateAgentDispatchRequest(
            agent_name="raw", room="q1", metadata="meta", attributes={"k": "v"}))
        # Ask both; the first to get the offer declines.
        first = asyncio.Event()
        async def serve_ordered(w):
            kind, m = await w.expect(4)
            if kind != "availability":
                out(w.name, "no offer", kind)
                return
            job = m.availability.job
            accept = first.is_set()
            first.set()
            out("offer: type", job.type, "room", job.room.name, "agent", job.agent_name,
                "dispatch", bool(job.dispatch_id), "state", job.state.status, "accept", accept)
            await w.send(availability=pa.AvailabilityResponse(
                job_id=job.id, available=accept, participant_identity="bot-" + w.name,
                participant_name="Bot", participant_metadata="pm", participant_attributes={"x": "y"}))
            if accept:
                kind, m = await w.expect(2)
                out("then", kind, m.assignment.job.state.status if m else None,
                    m.assignment.job.state.participant_identity.split("-")[0] if m else None)
                out("token claims", claims(m.assignment.token).replace(w.name, "W"))
                w.job = m.assignment.job
        await asyncio.gather(serve_ordered(j), serve_ordered(b))
        await listed("q1")
        ds = await lk.agent_dispatch.list_dispatch("q1")
        for x in sorted(ds, key=lambda x: x.agent_name):
            out("dispatch", repr(x.agent_name), [(jb.state.status, jb.state.participant_identity.split("-")[0], jb.state.error) for jb in x.state.jobs])

        out("== 4. terminate answer")
        await lk.agent_dispatch.create_dispatch(api.CreateAgentDispatchRequest(agent_name="raw", room="q2"))
        async def term(w):
            kind, m = await w.expect(3)
            if kind == "availability":
                await w.send(availability=pa.AvailabilityResponse(job_id=m.availability.job.id, terminate=True))
                out("terminated by", "a worker")
        await asyncio.gather(term(j), term(b))
        await listed("q2")
        for x in await lk.agent_dispatch.list_dispatch("q2"):
            out("dispatch", repr(x.agent_name), [(jb.state.status, bool(jb.state.ended_at)) for jb in x.state.jobs])

        out("== 5. full workers get nothing")
        await j.send(update_worker=pa.UpdateWorkerStatus(status=pa.WS_FULL, load=1.0))
        await b.send(update_worker=pa.UpdateWorkerStatus(status=pa.WS_FULL, load=1.0))
        await asyncio.sleep(0.3)
        await lk.agent_dispatch.create_dispatch(api.CreateAgentDispatchRequest(agent_name="raw", room="q3"))
        res = await asyncio.gather(j.expect(2), b.expect(2))
        out("offers", sorted(str(k) for k, _ in res))
        await asyncio.sleep(0.5)
        for x in await lk.agent_dispatch.list_dispatch("q3"):
            out("dispatch", repr(x.agent_name), len(x.state.jobs))
        await b.send(update_worker=pa.UpdateWorkerStatus(status=pa.WS_AVAILABLE, load=0.2))
        await asyncio.sleep(0.3)
        await lk.agent_dispatch.create_dispatch(api.CreateAgentDispatchRequest(agent_name="raw", room="q4"))
        kind, m = await b.expect(3)
        out("available again ->", kind)
        if m:
            await b.send(availability=pa.AvailabilityResponse(job_id=m.availability.job.id, available=True, participant_identity="late"))
            kind, m = await b.expect(2)
            out("then", kind)
            job4 = m.assignment.job

        out("== 6. the agent leaves: its job ends, the worker is told")
        from livekit import rtc
        r = rtc.Room()
        await r.connect(URL.replace("http", "ws"), m.assignment.token)
        await asyncio.sleep(1)
        await r.disconnect()
        kind, m = await b.expect(3)
        out("after the agent left ->", kind, m.termination.job_id == job4.id if m else None)
        await b.send(update_job=pa.UpdateJobStatus(job_id=job4.id, status=pa.JS_SUCCESS))

        out("== 7. deleting a dispatch terminates its jobs")
        ds = await lk.agent_dispatch.list_dispatch("q1")
        raw = next(x for x in ds if x.agent_name == "raw")
        holder = j if getattr(j, "job", None) else b
        await lk.agent_dispatch.delete_dispatch(raw.id, "q1")
        kind, m = await holder.expect(3)
        out("on delete ->", kind, m.termination.job_id == holder.job.id if m else None)

        out("== 8. simulated job")
        await b.send(simulate_job=pa.SimulateJobRequest(type=pa.JT_ROOM, room=api.Room(name="sim")))
        kind, m = await b.expect(3)
        out("simulate ->", kind, m.availability.job.type if m else None, m.availability.job.room.name if m else None,
            m.availability.job.agent_name if m else None, bool(m.availability.job.dispatch_id) if m else None)
        await b.send(availability=pa.AvailabilityResponse(job_id=m.availability.job.id, available=False))

        out("== 9. migrate is ignored, worker job update for an unknown job too")
        await b.send(migrate_job=pa.MigrateJobRequest(job_ids=["AJ_x"]))
        await b.send(update_job=pa.UpdateJobStatus(job_id="AJ_unknown", status=pa.JS_FAILED))
        await b.send(ping=pa.WorkerPing(timestamp=7))
        kind, m = await b.expect(2)
        out("still alive ->", kind, m.pong.last_timestamp if m else None)

        out("== 10. registrations refused")
        for label, kw in [
            ("deployment a_b", dict(type=pa.JT_ROOM, agent_name="z", deployment="a_b")),
            ("job type 7", dict(type=7, agent_name="z")),
        ]:
            w = await Worker.connect(s, "z")
            await w.send(register=pa.RegisterWorkerRequest(**kw))
            kind, _ = await w.expect(3)
            out(label, "->", kind)
        w = await Worker.connect(s, "z")
        await w.send(update_worker=pa.UpdateWorkerStatus(load=0.5))
        kind, _ = await w.expect(3)
        out("update before register ->", kind)
        w = await Worker.connect(s, "z")
        await w.send(register=pa.RegisterWorkerRequest(type=pa.JT_ROOM, agent_name="z"))
        await w.expect(3)
        await w.send(register=pa.RegisterWorkerRequest(type=pa.JT_ROOM, agent_name="z"))
        kind, _ = await w.expect(3)
        out("register twice ->", kind)

        out("== 11. deployments are separate")
        dw = await Worker.connect(s, "dep")
        await dw.send(register=pa.RegisterWorkerRequest(type=pa.JT_ROOM, agent_name="raw", deployment="blue"))
        await dw.expect()
        await lk.agent_dispatch.create_dispatch(api.CreateAgentDispatchRequest(agent_name="raw", room="q5", deployment="blue"))
        res = await asyncio.gather(j.expect(2), b.expect(2), dw.expect(2))
        out("offers (json, pb, blue)", [str(k) for k, _ in res])
        if res[2][1]:
            out("blue job deployment", res[2][1].availability.job.deployment)
        for w in (j, b, dw):
            await w.ws.close()
    await lk.aclose()


asyncio.run(main())
sys.stdout.flush()
os._exit(0)  # skip the SDK's noisy teardown at interpreter exit
