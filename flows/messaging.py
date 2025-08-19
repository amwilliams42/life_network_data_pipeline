import asyncio
import json
import os

from prefect import task, flow, logging

import nats


@task
async def send_payload_with_response(subject, payload, timeout: float = 60.0):
    """Send a payload and wait for a success/fail response."""
    logger = logging.get_run_logger()
    nc = await nats.connect("nats://nats:4222")
    
    try:
        # Generate a unique inbox for the response
        inbox = nc.new_inbox()
        logger.info(f"Generated inbox for response: {inbox}")
        
        # Subscribe to the inbox before sending to avoid race conditions
        sub = await nc.subscribe(inbox)
        
        # Send the message with the reply inbox
        await nc.publish(subject, payload, reply=inbox)
        await nc.flush()
        logger.info(f"Sent message to {subject}, awaiting response on {inbox}")
        
        # Wait for response
        try:
            msg = await sub.next_msg(timeout=timeout)
            response_text = msg.data.decode('utf-8', errors='replace')
            
            # Check if it's a success or fail message
            response_lower = response_text.lower()
            if 'success' in response_lower:
                logger.info(f"SUCCESS response: {response_text}")
            elif 'fail' in response_lower:
                logger.info(f"FAIL response: {response_text}")
            else:
                logger.info(f"Response received: {response_text}")
                
        except asyncio.TimeoutError:
            logger.info(f"No response received within {timeout} seconds")
            
        # Clean up subscription
        await sub.unsubscribe()
        
    finally:
        await nc.close()


@task
async def run_evidence_build(timeout: float = 300.0):
    """Send evidence build command and wait for response."""
    logger = logging.get_run_logger()
    nc = await nats.connect("nats://nats:4222")
    
    try:
        # Generate a unique inbox for the response
        inbox = nc.new_inbox()
        logger.info(f"Starting evidence build, response inbox: {inbox}")
        
        # Subscribe to the inbox before sending to avoid race conditions
        sub = await nc.subscribe(inbox)
        
        # Send the evidence build command
        await nc.publish("cmd.evidence", b"full.build", reply=inbox)
        await nc.flush()
        logger.info("Sent evidence build command to cmd.evidence")
        
        # Wait for response
        try:
            msg = await sub.next_msg(timeout=timeout)
            response_text = msg.data.decode('utf-8', errors='replace')
            
            # Check if it's a success or fail message
            response_lower = response_text.lower()
            if 'success' in response_lower:
                logger.info(f"Evidence build SUCCESS: {response_text}")
            elif 'fail' in response_lower:
                logger.info(f"Evidence build FAILED: {response_text}")
            else:
                logger.info(f"Evidence build response: {response_text}")
                
        except asyncio.TimeoutError:
            logger.info(f"Evidence build did not respond within {timeout} seconds")
            
        # Clean up subscription
        await sub.unsubscribe()
        
    finally:
        await nc.close()


@task
async def send_payload(subject, payload):
    """Simple send without waiting for response."""
    nc = await nats.connect("nats://nats:4222")
    try:
        await nc.publish(subject, payload)
        await nc.flush()
    finally:
        await nc.close()


@task
async def listen_response(inbox, timeout: float = 60.0):
    """Listen for responses on a specific inbox."""
    logger = logging.get_run_logger()
    nc = await nats.connect("nats://nats:4222")
    
    try:
        sub = await nc.subscribe(inbox)
        logger.info(f"Listening for responses on {inbox}")
        
        try:
            msg = await sub.next_msg(timeout=timeout)
            response_text = msg.data.decode('utf-8', errors='replace')
            
            response_lower = response_text.lower()
            if 'success' in response_lower or 'fail' in response_lower:
                logger.info(f"NATS response on {inbox}: {response_text}")
            else:
                logger.info(f"Response on {inbox}: {response_text}")
                
        except asyncio.TimeoutError:
            logger.info(f"No response on {inbox} within {timeout} seconds")
            
        await sub.unsubscribe()
        
    finally:
        await nc.close()


@flow
async def run_test():
    """Test flow that sends a message and waits for response."""
    logger = logging.get_run_logger()
    
    # Option 1: Send and wait for response in one task
    await send_payload_with_response("test", b'test message from prefect', timeout=30.0)
    
    # Option 2: Send and listen separately (if you need more control)
    # nc = await nats.connect("nats://nats:4222")
    # inbox = nc.new_inbox()
    # await nc.close()
    # 
    # # Start listening first
    # listener_task = asyncio.create_task(listen_response(inbox, timeout=30.0))
    # 
    # # Then send with reply inbox
    # await send_payload_with_reply("test", b'test message from prefect', inbox)
    # logger.info("Sent test message")
    # 
    # # Wait for response
    # await listener_task


@task
async def send_payload_with_reply(subject, payload, reply_inbox):
    """Send payload with a specific reply inbox."""
    nc = await nats.connect("nats://nats:4222")
    try:
        await nc.publish(subject, payload, reply=reply_inbox)
        await nc.flush()
    finally:
        await nc.close()