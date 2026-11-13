
"""Ingest Bluesky Jetstream events and deliver them reliably to Kafka.

Events are first persisted in a local SQLite buffer. A background worker
delivers buffered events to Kafka and removes them only after receiving
a successful Kafka acknowledgement.

The script also publishes incoming event throughput to the React
dashboard through a WebSocket server.
"""

import asyncio
import json
import os
import random
import sqlite3
import threading
import time
import uuid

import websockets
from dotenv import load_dotenv
from kafka import KafkaProducer
from websockets.asyncio.server import serve


# Load environment variables from the .env file
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)


JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe"

KAFKA_TOPIC = "bluesky-events"
BUFFER_DB = "kafka_buffer.db"


# ------------------------------------------
# WEBSOCKET SERVER FOR REACT
# ------------------------------------------

websocket_clients = set()
websocket_loop = None
websocket_ready = threading.Event()


async def websocket_handler(websocket):
    """Manage a React dashboard WebSocket client connection."""

    websocket_clients.add(websocket)

    print("\nReact dashboard connected to incoming throughput.")

    try:
        await websocket.wait_closed()

    finally:
        websocket_clients.discard(websocket)
        print("\nReact dashboard disconnected from incoming throughput.")


async def broadcast_throughput(throughput):
    """Broadcast incoming event throughput to connected dashboard clients."""

    if not websocket_clients:
        return

    message = json.dumps({
        "type": "incoming_throughput",
        "events_per_second": round(throughput, 2)
    })

    clients = list(websocket_clients)

    results = await asyncio.gather(
        *[client.send(message) for client in clients],
        return_exceptions=True
    )

    for client, result in zip(clients, results):

        if isinstance(result, Exception):
            websocket_clients.discard(client)


async def websocket_server():
    """Start the WebSocket server used to publish incoming throughput."""

    global websocket_loop

    websocket_loop = asyncio.get_running_loop()

    async with serve(
        websocket_handler,
        "localhost",
        8767
    ):

        print("Incoming throughput WebSocket running at ws://localhost:8767")

        websocket_ready.set()

        await asyncio.Future()


def run_websocket_server():
    """Run the WebSocket server using its own asyncio event loop."""

    asyncio.run(websocket_server())


def publish_throughput(throughput):
    """Schedule a throughput broadcast on the WebSocket event loop."""

    asyncio.run_coroutine_threadsafe(
        broadcast_throughput(throughput),
        websocket_loop
    )


# ------------------------------------------
# KAFKA PRODUCER
# ------------------------------------------

def serialize_event(event):
    """Serialize a Kafka message into UTF-8 encoded JSON bytes."""

    return json.dumps(event).encode("utf-8")


producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=serialize_event,
    acks="all",
    retries=3,
    request_timeout_ms=10000,
    max_block_ms=10000
)


# ------------------------------------------
# LOCAL SQLITE BUFFER
# ------------------------------------------

def initialize_buffer():
    """Create and configure the local SQLite event buffer."""

    connection = sqlite3.connect(BUFFER_DB)

    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")

    connection.execute("""
        CREATE TABLE IF NOT EXISTS buffered_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL
        )
    """)

    connection.commit()
    connection.close()

    print("Local Kafka buffer initialized.")


def save_to_buffer(connection, kafka_message):
    """Persist a Kafka message in the local SQLite buffer."""

    payload = json.dumps(kafka_message)

    connection.execute(
        "INSERT INTO buffered_events (payload) VALUES (?)",
        (payload,)
    )

    connection.commit()


# ------------------------------------------
# BACKGROUND KAFKA DELIVERY THREAD
# ------------------------------------------

stop_delivery = threading.Event()


def deliver_buffered_events():
    """Deliver buffered events to Kafka and remove acknowledged events."""

    connection = sqlite3.connect(BUFFER_DB)

    print("Kafka delivery worker started.")

    try:

        while not stop_delivery.is_set():

            # Get the oldest buffered event
            row = connection.execute("""
                SELECT id, payload
                FROM buffered_events
                ORDER BY id
                LIMIT 1
            """).fetchone()

            # Nothing to deliver
            if row is None:
                stop_delivery.wait(0.2)
                continue

            event_id, payload = row

            try:

                kafka_message = json.loads(payload)

                # Send event and wait for Kafka acknowledgement
                future = producer.send(
                    KAFKA_TOPIC,
                    value=kafka_message
                )

                future.get(timeout=60)

                # Delete only after successful acknowledgement
                connection.execute(
                    "DELETE FROM buffered_events WHERE id = ?",
                    (event_id,)
                )

                connection.commit()

            except Exception as delivery_error:

                print(
                    f"\nKafka delivery failed: {delivery_error}"
                )

                print(
                    "Event retained in local buffer. "
                    "Retrying in 5 seconds."
                )

                stop_delivery.wait(5)

    finally:
        connection.close()


# ------------------------------------------
# BLUESKY JETSTREAM CONSUMER
# ------------------------------------------

async def consume_bluesky():
    """Receive Bluesky events, buffer them, and publish incoming throughput."""

    incoming_events = 0
    last_update = time.monotonic()

    buffer_connection = sqlite3.connect(BUFFER_DB)

    # Select approximately one random event per second on average
    next_sample_time = (
        time.monotonic() + random.expovariate(1.0)
    )

    try:

        async with websockets.connect(JETSTREAM_URL) as websocket:

            async for message in websocket:

                # Capture when the event arrives at our producer
                received_at_ns = time.perf_counter_ns()

                event = json.loads(message)

                current_time = time.monotonic()

                # --------------------------------------
                # RANDOM EVENT SELECTION FOR LATENCY
                # --------------------------------------

                latency_tracking = None

                if current_time >= next_sample_time:

                    latency_tracking = {
                        "sample_id": str(uuid.uuid4()),
                        "received_at_ns": received_at_ns
                    }

                    next_sample_time = (
                        current_time + random.expovariate(1.0)
                    )

                # --------------------------------------
                # PREPARE KAFKA MESSAGE
                # --------------------------------------

                kafka_message = {
                    "event": event,
                    "latency_tracking": latency_tracking
                }

                # --------------------------------------
                # SAVE EVENT TO LOCAL BUFFER
                # --------------------------------------

                save_to_buffer(
                    buffer_connection,
                    kafka_message
                )

                # Count incoming events
                incoming_events += 1

                # --------------------------------------
                # INCOMING THROUGHPUT
                # --------------------------------------

                if current_time - last_update >= 1:

                    elapsed_seconds = current_time - last_update

                    throughput = incoming_events / elapsed_seconds

                    print(
                        f"\rIncoming throughput: {throughput:.2f} events/sec",
                        end="",
                        flush=True
                    )

                    publish_throughput(throughput)

                    incoming_events = 0
                    last_update = current_time

    finally:
        buffer_connection.close()


# ------------------------------------------
# START WEBSOCKET SERVER
# ------------------------------------------

websocket_thread = threading.Thread(
    target=run_websocket_server,
    daemon=True
)

websocket_thread.start()

if not websocket_ready.wait(timeout=5):
    raise RuntimeError(
        "Incoming throughput WebSocket server failed to start."
    )


# ------------------------------------------
# INITIALIZE LOCAL BUFFER
# ------------------------------------------

initialize_buffer()


# ------------------------------------------
# START KAFKA DELIVERY WORKER
# ------------------------------------------

delivery_thread = threading.Thread(
    target=deliver_buffered_events,
    daemon=True
)

delivery_thread.start()


# ------------------------------------------
# START BLUESKY CONSUMER
# ------------------------------------------

try:

    asyncio.run(consume_bluesky())

except KeyboardInterrupt:

    print("\nStopping Bluesky consumer...")

finally:

    stop_delivery.set()

    # Allow the delivery worker to finish its current attempt
    delivery_thread.join()

    producer.flush()
    producer.close()

    print("\nBluesky consumer stopped.")
