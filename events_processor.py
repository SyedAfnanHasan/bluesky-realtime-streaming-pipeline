
"""Process Bluesky events from Kafka and publish live analytics.

This module consumes events from the bluesky-events Kafka topic, detects
duplicates, calculates event metrics, tracks sampled event latency, and
stores periodic metric snapshots in PostgreSQL.

A WebSocket server broadcasts live metrics to the React dashboard.
"""

import asyncio
import json
import threading
import time

import psycopg
from kafka import KafkaConsumer
from websockets.asyncio.server import serve


# ------------------------------------------
# WEBSOCKET SERVER
# ------------------------------------------

websocket_clients = set()
websocket_loop = None
websocket_ready = threading.Event()

latest_database_metrics = None
latest_processing_metrics = None
latest_latency_metrics = None
latest_accuracy_metrics = None


async def websocket_handler(websocket):
    """Handle a dashboard client and send the latest available metrics."""

    websocket_clients.add(websocket)

    print("\nReact dashboard connected.")

    try:

        if latest_database_metrics is not None:
            await websocket.send(json.dumps(latest_database_metrics))

        if latest_processing_metrics is not None:
            await websocket.send(json.dumps(latest_processing_metrics))

        if latest_latency_metrics is not None:
            await websocket.send(json.dumps(latest_latency_metrics))

        if latest_accuracy_metrics is not None:
            await websocket.send(json.dumps(latest_accuracy_metrics))

        await websocket.wait_closed()

    finally:
        websocket_clients.discard(websocket)
        print("\nReact dashboard disconnected.")


async def broadcast_metrics(metrics):
    """Broadcast a metric message to all connected dashboard clients."""

    if not websocket_clients:
        return

    message = json.dumps(metrics)

    clients = list(websocket_clients)

    results = await asyncio.gather(
        *[
            client.send(message)
            for client in clients
        ],
        return_exceptions=True
    )

    for client, result in zip(clients, results):

        if isinstance(result, Exception):
            websocket_clients.discard(client)


async def websocket_server():
    """Start the WebSocket server and retain its asyncio event loop."""

    global websocket_loop

    websocket_loop = asyncio.get_running_loop()

    async with serve(
        websocket_handler,
        "localhost",
        8765
    ):

        print("WebSocket server running at ws://localhost:8765")

        websocket_ready.set()

        await asyncio.Future()


def run_websocket_server():
    """Run the WebSocket server in its own asyncio event loop."""

    asyncio.run(websocket_server())


# ------------------------------------------
# PUBLISH LIVE EVENT METRICS
# ------------------------------------------

def publish_metrics(posts, likes, reposts, follows):
    """Publish the latest per-second Bluesky activity counters."""

    metrics = {
        "type": "event_metrics",
        "posts": posts,
        "likes": likes,
        "reposts": reposts,
        "follows": follows
    }

    asyncio.run_coroutine_threadsafe(
        broadcast_metrics(metrics),
        websocket_loop
    )


# ------------------------------------------
# PUBLISH PROCESSING THROUGHPUT
# ------------------------------------------

def publish_processing_throughput(throughput):
    """Publish the latest Kafka event processing throughput."""

    global latest_processing_metrics

    metrics = {
        "type": "processing_throughput",
        "events_per_second": round(throughput, 2)
    }

    latest_processing_metrics = metrics

    asyncio.run_coroutine_threadsafe(
        broadcast_metrics(metrics),
        websocket_loop
    )


# ------------------------------------------
# PUBLISH INDIVIDUAL LATENCY
# ------------------------------------------

def publish_latency(latency_ms):
    """Publish the latency measurement of a sampled event."""

    global latest_latency_metrics

    metrics = {
        "type": "latency_metrics",
        "latency_ms": round(latency_ms, 2)
    }

    latest_latency_metrics = metrics

    asyncio.run_coroutine_threadsafe(
        broadcast_metrics(metrics),
        websocket_loop
    )


# ------------------------------------------
# PUBLISH PROCESSING ACCURACY
# ------------------------------------------

def publish_accuracy(total_attempted, successful, failed):
    """Publish event processing success and failure statistics."""

    global latest_accuracy_metrics

    if total_attempted == 0:
        return

    success_rate = (successful / total_attempted) * 100

    metrics = {
        "type": "accuracy_metrics",
        "success_rate": round(success_rate, 2),
        "total_attempted": total_attempted,
        "successfully_processed": successful,
        "failed_events": failed
    }

    latest_accuracy_metrics = metrics

    asyncio.run_coroutine_threadsafe(
        broadcast_metrics(metrics),
        websocket_loop
    )


# ------------------------------------------
# PUBLISH DATABASE METRICS
# ------------------------------------------

def publish_database_metrics(avg_posts, avg_likes, avg_reposts, avg_follows):
    """Publish the latest average activity metrics stored in PostgreSQL."""

    global latest_database_metrics

    metrics = {
        "type": "database_metrics",
        "avg_posts_per_second": avg_posts,
        "avg_likes_per_second": avg_likes,
        "avg_reposts_per_second": avg_reposts,
        "avg_follows_per_second": avg_follows
    }

    latest_database_metrics = metrics

    asyncio.run_coroutine_threadsafe(
        broadcast_metrics(metrics),
        websocket_loop
    )


# ------------------------------------------
# KAFKA CONSUMER
# ------------------------------------------

consumer = KafkaConsumer(
    "bluesky-events",
    bootstrap_servers=["localhost:9092"],
    group_id="events-processing-group",
    auto_offset_reset="latest",
    value_deserializer=lambda data: json.loads(data.decode("utf-8"))
)


# ------------------------------------------
# POSTGRESQL CONNECTION
# ------------------------------------------

connection: psycopg.Connection = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="bluesky_streaming",
    user="syedafnan"
)


# ------------------------------------------
# LOAD LATEST DATABASE METRICS
# ------------------------------------------

def load_latest_database_metrics():
    """Load the most recently stored event metrics from PostgreSQL."""

    query = """
        SELECT
            avg_posts_per_second,
            avg_likes_per_second,
            avg_reposts_per_second,
            avg_follows_per_second
        FROM event_metrics
        ORDER BY recorded_at DESC
        LIMIT 1;
    """

    with psycopg.Connection.cursor(connection) as cursor:

        cursor.execute(query)

        row = cursor.fetchone()

    if row is None:
        print("No previous database metrics found.")
        return None

    metrics = {
        "type": "database_metrics",
        "avg_posts_per_second": row[0],
        "avg_likes_per_second": row[1],
        "avg_reposts_per_second": row[2],
        "avg_follows_per_second": row[3]
    }

    print("Latest database metrics loaded from PostgreSQL.")

    return metrics


latest_database_metrics = load_latest_database_metrics()


# ------------------------------------------
# START WEBSOCKET SERVER
# ------------------------------------------

websocket_thread = threading.Thread(
    target=run_websocket_server,
    daemon=True
)

websocket_thread.start()

if not websocket_ready.wait(timeout=5):
    raise RuntimeError("WebSocket server failed to start.")


# ------------------------------------------
# IDEMPOTENCY / DUPLICATE DETECTION
# ------------------------------------------

processed_event_ids = set()

MAX_STORED_IDS = 1000


# ------------------------------------------
# TERMINAL COUNTERS (1 SECOND)
# ------------------------------------------

posts = 0
likes = 0
reposts = 0
follows = 0

last_terminal_update = time.monotonic()


# ------------------------------------------
# PROCESSING THROUGHPUT COUNTER
# ------------------------------------------

processed_events = 0
last_throughput_update = time.monotonic()

latest_throughput = 0


# ------------------------------------------
# LATENCY TRACKING
# ------------------------------------------

latest_latency_ms = None


# ------------------------------------------
# PROCESSING ACCURACY COUNTERS
# ------------------------------------------

total_attempted = 0
successfully_processed = 0
failed_events = 0


# ------------------------------------------
# DATABASE COUNTERS (60 SECONDS)
# ------------------------------------------

db_posts = 0
db_likes = 0
db_reposts = 0
db_follows = 0

db_interval_start = time.monotonic()


# ------------------------------------------
# SAVE METRICS TO POSTGRESQL
# ------------------------------------------

def save_metrics(posts, likes, reposts, follows, interval_seconds):
    """Calculate average event rates and save a snapshot to PostgreSQL."""

    avg_posts = round(posts / interval_seconds)
    avg_likes = round(likes / interval_seconds)
    avg_reposts = round(reposts / interval_seconds)
    avg_follows = round(follows / interval_seconds)

    query = """
        INSERT INTO event_metrics (
            avg_posts_per_second,
            avg_likes_per_second,
            avg_reposts_per_second,
            avg_follows_per_second
        )
        VALUES (%s, %s, %s, %s);
    """

    with psycopg.Connection.cursor(connection) as cursor:
        cursor.execute(
            query,
            (avg_posts, avg_likes, avg_reposts, avg_follows)
        )

    psycopg.Connection.commit(connection)

    print("\n60-second metrics saved to PostgreSQL.")

    return avg_posts, avg_likes, avg_reposts, avg_follows


# ------------------------------------------
# PROCESS EVENTS
# ------------------------------------------

print("Starting Bluesky live metrics...\n")

try:

    while True:

        records = consumer.poll(timeout_ms=100)

        for topic_messages in records.values():

            for message in topic_messages:

                try:

                    kafka_message = message.value

                    # ----------------------------------
                    # EXTRACT EVENT AND LATENCY TRACKING
                    # ----------------------------------

                    if (
                        isinstance(kafka_message, dict)
                        and "event" in kafka_message
                        and "latency_tracking" in kafka_message
                    ):

                        event = kafka_message["event"]
                        latency_tracking = kafka_message["latency_tracking"]

                    else:

                        event = kafka_message
                        latency_tracking = None

                    # ----------------------------------
                    # IDEMPOTENCY CHECK
                    # ----------------------------------

                    commit = event.get("commit", {})

                    did = event.get("did")
                    revision = commit.get("rev")
                    collection = commit.get("collection")
                    rkey = commit.get("rkey")

                    # Only perform deduplication when
                    # all identifying fields are available

                    if all((did, revision, collection, rkey)):

                        event_id = (
                            f"{did}:"
                            f"{revision}:"
                            f"{collection}:"
                            f"{rkey}"
                        )

                        # Check whether event was already processed
                        if event_id in processed_event_ids:

                            print("\nDuplicate event detected. Skipping.")
                            continue

                        # Store new event ID
                        processed_event_ids.add(event_id)

                        # Clear the set after 1,000 IDs
                        if len(processed_event_ids) >= MAX_STORED_IDS:

                            processed_event_ids.clear()

                            print(
                                "\nIdempotency set reached 1,000 IDs. "
                                "Set cleared."
                            )

                    # ----------------------------------
                    # COUNT UNIQUE PROCESSING ATTEMPTS
                    # ----------------------------------

                    total_attempted += 1
                    processed_events += 1

                    # ----------------------------------
                    # PROCESS ORIGINAL BLUESKY EVENT
                    # ----------------------------------

                    operation = commit.get("operation")

                    if operation == "create":

                        if collection == "app.bsky.feed.post":
                            posts += 1
                            db_posts += 1

                        elif collection == "app.bsky.feed.like":
                            likes += 1
                            db_likes += 1

                        elif collection == "app.bsky.feed.repost":
                            reposts += 1
                            db_reposts += 1

                        elif collection == "app.bsky.graph.follow":
                            follows += 1
                            db_follows += 1

                    # Message was handled without a processing error
                    successfully_processed += 1

                    # ----------------------------------
                    # CALCULATE SAMPLED EVENT LATENCY
                    # ----------------------------------

                    if latency_tracking is not None:

                        try:

                            received_at_ns = latency_tracking["received_at_ns"]

                            processing_completed_ns = time.perf_counter_ns()

                            latency_ms = (
                                processing_completed_ns - received_at_ns
                            ) / 1_000_000

                            latest_latency_ms = latency_ms

                            publish_latency(latency_ms)

                        except Exception as latency_error:

                            print(
                                "\nLatency tracking error:",
                                latency_error
                            )

                except Exception as processing_error:

                    failed_events += 1

                    print(
                        "\nEvent processing failed:",
                        processing_error
                    )

        current_time = time.monotonic()

        # ------------------------------------------
        # PROCESSING THROUGHPUT
        # EVERY SECOND
        # ------------------------------------------

        if current_time - last_throughput_update >= 1:

            elapsed_seconds = current_time - last_throughput_update

            latest_throughput = processed_events / elapsed_seconds

            publish_processing_throughput(latest_throughput)

            processed_events = 0
            last_throughput_update = current_time

        # ------------------------------------------
        # TERMINAL AND WEBSOCKET UPDATE
        # EVERY SECOND
        # ------------------------------------------

        if current_time - last_terminal_update >= 1:

            print("\033[H\033[J", end="")

            print("========== BLUESKY LIVE METRICS ==========")
            print("ACTIVITY PER SECOND")
            print("------------------------------------------")
            print(f"Posts/sec:          {posts}")
            print(f"Likes/sec:          {likes}")
            print(f"Reposts/sec:        {reposts}")
            print(f"Follows/sec:        {follows}")
            print("------------------------------------------")
            print(f"Processing throughput: {latest_throughput:.2f} events/sec")
            print("------------------------------------------")
            print("LATEST SAMPLED EVENT LATENCY")

            if latest_latency_ms is not None:
                print(f"Latency: {latest_latency_ms:.2f} ms")
            else:
                print("Waiting for first sampled event...")

            print("------------------------------------------")
            print("PROCESSING ACCURACY")
            print(f"Total attempted:       {total_attempted}")
            print(f"Successfully processed: {successfully_processed}")
            print(f"Failed events:         {failed_events}")

            if total_attempted > 0:

                success_rate = (
                    successfully_processed / total_attempted
                ) * 100

                print(f"Success rate:          {success_rate:.2f}%")

            print("==========================================")

            publish_metrics(
                posts,
                likes,
                reposts,
                follows
            )

            # Publish processing accuracy once per second
            publish_accuracy(
                total_attempted,
                successfully_processed,
                failed_events
            )

            # Reset terminal activity counters
            posts = 0
            likes = 0
            reposts = 0
            follows = 0

            last_terminal_update = current_time

        # ------------------------------------------
        # DATABASE UPDATE EVERY 60 SECONDS
        # ------------------------------------------

        if current_time - db_interval_start >= 60:

            interval_seconds = current_time - db_interval_start

            avg_posts, avg_likes, avg_reposts, avg_follows = save_metrics(
                db_posts,
                db_likes,
                db_reposts,
                db_follows,
                interval_seconds
            )

            publish_database_metrics(
                avg_posts,
                avg_likes,
                avg_reposts,
                avg_follows
            )

            db_posts = 0
            db_likes = 0
            db_reposts = 0
            db_follows = 0

            db_interval_start = current_time


except KeyboardInterrupt:

    print("\nStopping Bluesky processor...")


finally:

    consumer.close()
    psycopg.Connection.close(connection)

    print("Bluesky processor stopped.")
