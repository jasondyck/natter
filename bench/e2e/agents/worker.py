"""An agent worker for agents.py: logs each job it gets as a JSON line.

Run by agents.py, configured by environment: AGENT_NAME, WORKER_TYPE
(room|publisher), LOG (file to append to), LIVEKIT_URL/API_KEY/API_SECRET.
"""

import asyncio
import json
import os
import sys

from livekit import rtc
from livekit.agents import JobContext, JobExecutorType, WorkerOptions, WorkerType, cli

NAME = os.environ.get("AGENT_NAME", "")
TYPE = os.environ.get("WORKER_TYPE", "room")
LOG = os.environ["LOG"]


def log(**event):
    with open(LOG, "a") as f:
        f.write(json.dumps({"worker": NAME or "-", "wtype": TYPE, **event}) + "\n")


async def entrypoint(ctx: JobContext):
    job = ctx.job
    log(
        event="job",
        job_type=int(job.type),
        room=job.room.name,
        participant=job.participant.identity if job.HasField("participant") else None,
        metadata=job.metadata,
        agent_name=job.agent_name,
        has_dispatch_id=bool(job.dispatch_id),
    )

    async def ended(reason: str):
        log(event="shutdown", room=job.room.name, reason=reason)

    ctx.add_shutdown_callback(ended)
    await ctx.connect()
    lp = ctx.room.local_participant
    log(
        event="connected",
        room=ctx.room.name,
        kind=int(lp.kind),
        attributes=dict(lp.attributes),
        permissions={
            "can_publish": lp.permissions.can_publish,
            "can_subscribe": lp.permissions.can_subscribe,
            "can_publish_data": lp.permissions.can_publish_data,
        },
    )


if __name__ == "__main__":
    sys.argv = [sys.argv[0], "start"]
    cli.run_app(
        WorkerOptions(
            entrypoint_fnc=entrypoint,
            agent_name=NAME,
            worker_type=WorkerType.PUBLISHER if TYPE == "publisher" else WorkerType.ROOM,
            load_fnc=lambda *_: 0.0,
            load_threshold=1.0,
            num_idle_processes=1,
            job_executor_type=JobExecutorType.THREAD,
            port=0,
            log_level="INFO",
        )
    )
