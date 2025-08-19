import asyncio
import json
import os

from prefect import task, flow, logging

import nats


@task
async def send_and_wait_response(subject, message, timeout: float = 60.0):
    """Send message and wait for response."""
    logger = logging.get_run_logger()
    nc = await nats.connect("nats://nats:4222")
    
    try:
        inbox = nc.new_inbox()
        sub = await nc.subscribe(inbox)
        
        await nc.publish(subject, message.encode() if isinstance(message, str) else message, reply=inbox)
        await nc.flush()
        logger.info(f"Sent message to {subject}")
        
        try:
            msg = await sub.next_msg(timeout=timeout)
            response = msg.data.decode('utf-8', errors='replace')
            logger.info(f"Response: {response}")
        except asyncio.TimeoutError:
            logger.info(f"No response within {timeout} seconds")
            
        await sub.unsubscribe()
        
    finally:
        await nc.close()


@flow
async def test_flow():
    await send_and_wait_response("test", "test message from prefect")


@flow
async def run_evidence_build():
    await send_and_wait_response("cmd.evidence", "full.build", timeout=300.0)