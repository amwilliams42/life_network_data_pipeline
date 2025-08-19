import asyncio, json, os

from prefect import task, flow

import nats

@task
def run_test_call():
    async def _inner():
        nc = await nats.connect("nats://nats:4222")

        await nc.publish("test", b'test from prefect')

        await nc.drain()

@flow
async def run_test():
    run_test_call()