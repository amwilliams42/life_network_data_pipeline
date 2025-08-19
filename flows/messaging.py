import asyncio, json, os

from prefect import task, flow, logging

import nats


@task
async def send_payload(subject, payload):
    async def _inner():
        nc = await nats.connect("nats://nats:4222")
        await nc.publish(subject, json.dumps(payload).encode())
        await nc.flush()


@flow
async def run_test():
    await send_payload("test", {"msg":"test message"})