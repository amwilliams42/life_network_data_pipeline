import asyncio, json, os

from prefect import task, flow, logging

import nats


@task
async def send_payload(subject, payload):
    async def _inner():
        nc = await nats.connect("nats://nats:4222")
        try:
            msg = await nc.request(subject, json.dumps(payload).encode(), timeout=5)
            return json.loads(msg.data.decode("utf-8"))
        finally:
            await nc.drain()


@flow
async def run_test():
    logger = logging.get_run_logger()
    result = await send_payload("test", {"msg":"test message"})
    logger.info(result)