"""Agent dispatch against a LiveKit-compatible server, with the Python
agents framework (livekit-agents) as the workers and the Python server SDK
as the admin. Prints what each side saw, ids masked, in a stable order.

Covers automatic dispatch (room and publisher jobs), explicit dispatch
(AgentDispatchService create/list/delete), dispatches from a token's room
configuration and from CreateRoom, the agent participant (kind, attributes,
permissions), its removal ending the job, and the API's errors.
"""

import asyncio
import json
import os
import re
import subprocess
import sys
import tempfile

from livekit import api, rtc

URL = os.environ.get("LIVEKIT_URL", "ws://127.0.0.1:7880")
KEY = os.environ.get("LIVEKIT_API_KEY", "devkey")
SECRET = os.environ.get("LIVEKIT_API_SECRET", "secret")
HERE = os.path.dirname(os.path.abspath(__file__))


def mask(s):
    s = re.sub(r"\b(AD|AJ|AW|PA|RM|TR|ND)_[A-Za-z0-9]+", r"\1_*", str(s))
    return s


def out(*parts):
    print(mask(" ".join(str(p) for p in parts)), flush=True)


def token(identity, room, config=None):
    t = (
        api.AccessToken(KEY, SECRET)
        .with_identity(identity)
        .with_grants(api.VideoGrants(room_join=True, room=room))
    )
    if config is not None:
        t = t.with_room_config(config)
    return t.to_jwt()


async def join(identity, room, config=None):
    r = rtc.Room()
    await r.connect(URL, token(identity, room, config))
    return r


def others(room):
    return sorted(
        (
            p.identity.split("-")[0],
            int(p.kind),
            json.dumps(dict(sorted(p.attributes.items()))),
        )
        for p in room.remote_participants.values()
    )


def dispatches(resp):
    rows = []
    for d in resp:
        jobs = sorted(
            (int(j.type), j.participant.identity if j.HasField("participant") else "-",
             int(j.state.status), bool(j.state.participant_identity))
            for j in d.state.jobs
        )
        rows.append((d.agent_name, d.metadata, d.room, jobs))
    return sorted(rows)


async def err(what, coro):
    try:
        r = await coro
        out(what, "ok", r)
    except api.TwirpError as e:
        out(what, "error", e.code, e.message)


async def main():
    log = os.path.join(os.environ.get("AGENTS_LOG_DIR") or tempfile.mkdtemp(), "agents.jsonl")
    open(log, "w").close()
    # WORKER_URL: workers on another node of a cluster than the clients.
    worker_url = os.environ.get("WORKER_URL", URL)
    env = dict(os.environ, LIVEKIT_URL=worker_url, LIVEKIT_API_KEY=KEY, LIVEKIT_API_SECRET=SECRET, LOG=log)
    workers = [
        subprocess.Popen(
            [sys.executable, os.path.join(HERE, "worker.py")],
            env=dict(env, AGENT_NAME=name, WORKER_TYPE=wtype),
            stdout=open(f"{log}.{name or 'unnamed'}-{wtype}.err", "w"),
            stderr=subprocess.STDOUT,
        )
        for name, wtype in [("", "room"), ("", "publisher"), ("helper", "room")]
    ]
    lk = api.LiveKitAPI(URL.replace("ws", "http"), KEY, SECRET)

    async def listed(room, jobs):
        # LiveKit records a job up to ~5 s after the worker took it.
        for _ in range(80):
            ds = await lk.agent_dispatch.list_dispatch(room)
            if sum(len(d.state.jobs) for d in ds) >= jobs:
                break
            await asyncio.sleep(0.1)

    try:
        await asyncio.sleep(6)  # workers register

        out("== 1. automatic dispatch: room job on room start")
        alice = await join("alice", "r1")
        await asyncio.sleep(3)
        out("alice sees", others(alice))
        await listed("r1", 1)
        out("r1 dispatches", dispatches((await lk.agent_dispatch.list_dispatch("r1"))))

        out("== 2. publisher job on first publish")
        src = rtc.AudioSource(48000, 1)
        track = rtc.LocalAudioTrack.create_audio_track("mic", src)
        await alice.local_participant.publish_track(
            track, rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE)
        )
        await asyncio.sleep(3)
        out("alice sees", others(alice))
        out("r1 dispatches", dispatches(await lk.agent_dispatch.list_dispatch("r1")))

        out("== 3. explicit dispatch")
        d = await lk.agent_dispatch.create_dispatch(
            api.CreateAgentDispatchRequest(agent_name="helper", room="r1", metadata="hello")
        )
        out("created", d.agent_name, d.room, d.metadata, d.id[:3], d.state.created_at > 0, len(d.state.jobs))
        await asyncio.sleep(3)
        out("alice sees", others(alice))
        out("r1 dispatches", dispatches(await lk.agent_dispatch.list_dispatch("r1")))
        one = await lk.agent_dispatch.get_dispatch(d.id, "r1")
        out("get one", one.agent_name if one else None)

        out("== 4. delete dispatch: its agent goes")
        gone = await lk.agent_dispatch.delete_dispatch(d.id, "r1")
        out("deleted", gone.agent_name, gone.metadata, len(gone.state.jobs))
        await asyncio.sleep(4)
        out("alice sees", others(alice))
        out("r1 dispatches", dispatches(await lk.agent_dispatch.list_dispatch("r1")))

        out("== 5. dispatch from the token's room config")
        cfg = api.RoomConfiguration(agents=[api.RoomAgentDispatch(agent_name="helper", metadata="from-token")])
        bob = await join("bob", "r2", cfg)
        await asyncio.sleep(3)
        out("bob sees", others(bob))
        out("r2 dispatches", dispatches(await lk.agent_dispatch.list_dispatch("r2")))

        out("== 6. dispatch from CreateRoom, nobody in it")
        await lk.room.create_room(
            api.CreateRoomRequest(name="r3", empty_timeout=30, agents=[api.RoomAgentDispatch(agent_name="helper", metadata="from-create")])
        )
        await asyncio.sleep(3)
        parts = await lk.room.list_participants(api.ListParticipantsRequest(room="r3"))
        out("r3 participants", sorted((p.identity.split("-")[0], int(p.kind)) for p in parts.participants))
        out("r3 dispatches", dispatches(await lk.agent_dispatch.list_dispatch("r3")))

        out("== 7. CreateDispatch creates the room")
        await lk.agent_dispatch.create_dispatch(api.CreateAgentDispatchRequest(agent_name="helper", room="r4"))
        await asyncio.sleep(3)
        parts = await lk.room.list_participants(api.ListParticipantsRequest(room="r4"))
        out("r4 participants", sorted((p.identity.split("-")[0], int(p.kind)) for p in parts.participants))
        out("r4 dispatches", dispatches(await lk.agent_dispatch.list_dispatch("r4")))

        out("== 8. removing the agent ends its job")
        agent = next(p for p in alice.remote_participants.values() if p.kind == rtc.ParticipantKind.PARTICIPANT_KIND_AGENT)
        await lk.room.remove_participant(api.RoomParticipantIdentity(room="r1", identity=agent.identity))
        await asyncio.sleep(3)
        out("alice sees", others(alice))

        out("== 9. errors")
        await err("list missing room", lk.agent_dispatch.list_dispatch("nope"))
        await err("delete missing dispatch", lk.agent_dispatch.delete_dispatch("AD_missing", "r1"))
        await err("bad deployment", lk.agent_dispatch.create_dispatch(
            api.CreateAgentDispatchRequest(agent_name="helper", room="r1", deployment="a_b")))
        await err("dispatch nobody serves", lk.agent_dispatch.create_dispatch(
            api.CreateAgentDispatchRequest(agent_name="nobody", room="r1")))
        await asyncio.sleep(2)
        out("r1 dispatches", dispatches(await lk.agent_dispatch.list_dispatch("r1")))

        out("== 10. deleting the rooms ends the agents' jobs")
        for room in ["r1", "r2", "r3", "r4"]:
            await lk.room.delete_room(api.DeleteRoomRequest(room=room))
        await asyncio.sleep(float(os.environ.get("AGENTS_END_WAIT", "3")))
        await alice.disconnect()
        await bob.disconnect()
    finally:
        await lk.aclose()
        for w in workers:
            w.terminate()
        for w in workers:
            try:
                w.wait(10)
            except subprocess.TimeoutExpired:
                w.kill()

    out("== worker log")
    events = [json.loads(l) for l in open(log)]
    for e in sorted(events, key=lambda e: json.dumps(e, sort_keys=True)):
        if e["event"] == "connected":
            e["attributes"] = dict(sorted(e["attributes"].items()))
        out(json.dumps(e, sort_keys=True))


asyncio.run(main())
sys.stdout.flush()
os._exit(0)  # skip the SDK's noisy teardown at interpreter exit
