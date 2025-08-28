#!/usr/bin/env python3
"""
broker.py — Minimal MQTT broker + message printer in one process.

- Starts an MQTT broker on 0.0.0.0:1883
- Attaches an internal MQTT client that subscribes to '#' and prints any payloads

Usage:
  python broker.py
"""

import asyncio
import json
import logging
import signal
from datetime import datetime

from amqtt.broker import Broker
from amqtt.client import MQTTClient
from amqtt.mqtt.constants import QOS_1

# -----------------------------------------------------------------------------
# Broker config (anonymous, no TLS, for local dev ONLY)
# -----------------------------------------------------------------------------
BROKER_BIND = "0.0.0.0:1883"  # change port if you want
BROKER_CONFIG = {
    "listeners": {
        "default": {"type": "tcp", "bind": BROKER_BIND}
    },
    "sys_interval": 10,
    "auth": {
        "allow-anonymous": True
    },
    # disable strict topic checks for dev convenience
    "topic-check": {"enabled": False},
}

# -----------------------------------------------------------------------------
# Logging (quiet broker noise; show prints cleanly)
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(name)s: %(message)s"
)
# Make broker logs less chatty
logging.getLogger("amqtt.broker").setLevel(logging.WARNING)
logging.getLogger("amqtt.client").setLevel(logging.WARNING)

async def start_broker() -> Broker:
    broker = Broker(BROKER_CONFIG)
    await broker.start()
    print(f"[BROKER] Listening on {BROKER_BIND}")
    return broker

async def printer_client():
    """Subscribes to '#' and prints all messages as they arrive."""
    client = MQTTClient(client_id="broker-printer")
    await client.connect("mqtt://127.0.0.1:1883/")
    await client.subscribe([("#", QOS_1)])
    print("[PRINTER] Subscribed to '#' (all topics)")

    try:
        while True:
            msg = await client.deliver_message()
            pkt = msg.publish_packet
            topic = pkt.variable_header.topic_name
            payload = bytes(pkt.payload.data or b"")
            ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"

            # Try to pretty-print JSON; otherwise show raw bytes
            try:
                obj = json.loads(payload.decode("utf-8"))
                pretty = json.dumps(obj, ensure_ascii=False, indent=2)
                print(f"\n[{ts}] Topic: {topic}\n{pretty}")
            except Exception:
                print(f"\n[{ts}] Topic: {topic}\n{payload!r}")
    except asyncio.CancelledError:
        pass
    finally:
        await client.disconnect()

async def main():
    broker = await start_broker()
    task_printer = asyncio.create_task(printer_client())

    # Graceful shutdown on Ctrl+C
    stop_event = asyncio.Event()

    def _stop(*_):
        stop_event.set()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _stop)
        except NotImplementedError:
            # Windows without Proactor support: ignore
            pass

    await stop_event.wait()
    task_printer.cancel()
    try:
        await task_printer
    except asyncio.CancelledError:
        pass
    await broker.shutdown()
    print("[BROKER] Stopped")

if __name__ == "__main__":
    asyncio.run(main())
