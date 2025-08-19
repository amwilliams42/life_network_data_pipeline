import asyncio, json, os

from prefect import task, flow, logging

import nats


@task
async def send_payload(subject, payload):
    async def _inner():
        nc = await nats.connect("nats://nats:4222")
        try:
            await nc.publish(subject, json.dumps(payload).encode())
        finally:
            await nc.drain()


@flow
async def run_test():
    logger = logging.get_run_logger()
    result = await send_payload("test", {"msg":"test message"})
    logger.info(result)