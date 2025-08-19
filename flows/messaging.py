import asyncio, json, os

from prefect import task, flow, logging

import nats


@task
async def send_payload(subject, payload):
    nc = await nats.connect("nats://nats:4222")
    # Normalize subject and payload types: subject must be str, payload must be bytes
    if isinstance(subject, (bytes, bytearray)):
        subject = subject.decode("utf-8")
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    elif not isinstance(payload, (bytes, bytearray)):
        # Fallback: JSON-serialize non-bytes payloads
        payload = json.dumps(payload).encode("utf-8")
    await nc.publish(subject, payload)
    await nc.flush()

@task
async def listen_response(inbox, timeout: float = 60.0):
    logger = logging.get_run_logger()
    nc = await nats.connect("nats://nats:4222")
    sub = await nc.subscribe(inbox)
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                logger.info(f"No success/fail response on {inbox} within {timeout:.1f}s")
                break
            try:
                msg = await sub.next_msg(timeout=remaining)
            except asyncio.TimeoutError:
                logger.info(f"No success/fail response on {inbox} within {timeout:.1f}s")
                break

            data = msg.data
            text = data.decode("utf-8", errors="replace") if isinstance(data, (bytes, bytearray)) else str(data)
            lt = text.lower()
            if "success" in lt or "fail" in lt:
                logger.info(f"NATS response on {inbox}: {text}")
                break
            # Ignore non-matching messages and continue waiting until timeout
    finally:
        try:
            await sub.unsubscribe()
        except Exception:
            pass
        try:
            await nc.drain()
        except Exception:
            pass
        try:
            await nc.close()
        except Exception:
            pass

@flow
async def run_test():
    logger = logging.get_run_logger()
    # Start listening BEFORE sending to avoid missing a quick response
    listener = asyncio.create_task(listen_response("test.inbox", timeout=60.0))
    await send_payload("test", b'test message from prefect')
    logger.info("Sent test message")
    await listener

@flow
async def run_evidence_build():
    logger = logging.get_run_logger()
    listener = asyncio.create_task(listen_response("evidence.build.inbox"))
    await send_payload("evidence.build", 'full.build',reply='evidence.build.inbox')
    logger.info("Sent evidence build message")
    await listener