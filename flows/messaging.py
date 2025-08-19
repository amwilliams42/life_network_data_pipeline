import asyncio, json, os

from prefect import task, flow, logging

import nats


@task
async def send_payload(subject, payload):
        nc = await nats.connect("nats://nats:4222")
        await nc.publish(subject, payload)
        await nc.flush()


@flow
async def run_test():
    logger = logging.get_run_logger()
    await send_payload("test", b'test message from prefect')
    logger.info("Sent test message")