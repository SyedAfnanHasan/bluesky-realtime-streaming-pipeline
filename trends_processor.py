
"""
Process Bluesky trending-topic updates from Kafka.

The processor compares incoming trend data with the previous snapshot,
stores changed snapshots in PostgreSQL, and broadcasts updates to the
React dashboard through a WebSocket server.
"""

import asyncio
import json
import threading

from datetime import datetime, timezone

import psycopg
from kafka import KafkaConsumer
from websockets.asyncio.server import serve


# -----------------------------------
# KAFKA CONFIGURATION
# -----------------------------------

def deserialize_data(data):
    """Deserialize Kafka message bytes into a Python object."""
    return json.loads(data.decode("utf-8"))


consumer = KafkaConsumer(
    "bluesky-trends",
    bootstrap_servers=["localhost:9092"],
    group_id="trends-processing-group",
    auto_offset_reset="latest",
    value_deserializer=deserialize_data
)


# -----------------------------------
# POSTGRESQL CONFIGURATION
# -----------------------------------

connection: psycopg.Connection = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="bluesky_streaming",
    user="syedafnan",
    autocommit=True
)


# -----------------------------------
# LOAD LAST SAVED SNAPSHOT
# -----------------------------------

def load_previous_snapshot():
    """Load the most recently saved trend snapshot from PostgreSQL."""
    query = """
        SELECT rank, topic_name, post_count, category, status
        FROM trend_metrics
        WHERE recorded_at = (
            SELECT MAX(recorded_at)
            FROM trend_metrics
        )
        ORDER BY rank;
    """

    with psycopg.Connection.cursor(connection) as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    return tuple(tuple(row) for row in rows)


previous_snapshot = load_previous_snapshot()

# Keep the latest snapshot available for new WebSocket clients
latest_snapshot = previous_snapshot


# -----------------------------------
# WEBSOCKET SERVER
# -----------------------------------

websocket_clients = set()
websocket_loop = None
websocket_ready = threading.Event()


def create_snapshot_message(snapshot):
    """Convert a trend snapshot into the WebSocket message format."""
    trends = []

    for rank, topic_name, post_count, category, status in snapshot:

        trends.append({
            "rank": rank,
            "topic_name": topic_name,
            "post_count": post_count,
            "category": category,
            "status": status
        })

    return {
        "type": "trend_snapshot",
        "trends": trends
    }


async def websocket_handler(websocket):
    """Handle a React dashboard WebSocket connection."""
    websocket_clients.add(websocket)

    print("\nReact trends table connected.")

    try:

        # Send the latest available snapshot immediately
        if latest_snapshot:

            await websocket.send(
                json.dumps(create_snapshot_message(latest_snapshot))
            )

        await websocket.wait_closed()

    finally:

        websocket_clients.discard(websocket)

        print("\nReact trends table disconnected.")


async def broadcast_snapshot(snapshot):
    """Broadcast a trend snapshot to all connected WebSocket clients."""
    if not websocket_clients:
        return

    message = json.dumps(create_snapshot_message(snapshot))

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
    """Start the WebSocket server and keep it running."""
    global websocket_loop

    websocket_loop = asyncio.get_running_loop()

    async with serve(
        websocket_handler,
        "localhost",
        8766
    ):

        print("Trends WebSocket server running at ws://localhost:8766")

        websocket_ready.set()

        await asyncio.Future()


def run_websocket_server():
    """Run the WebSocket server using a new asyncio event loop."""
    asyncio.run(websocket_server())


def publish_snapshot(snapshot):
    """Schedule a snapshot broadcast on the WebSocket event loop."""
    asyncio.run_coroutine_threadsafe(
        broadcast_snapshot(snapshot),
        websocket_loop
    )


# Start WebSocket server in background thread
websocket_thread = threading.Thread(
    target=run_websocket_server,
    daemon=True
)

websocket_thread.start()

if not websocket_ready.wait(timeout=5):
    raise RuntimeError("Trends WebSocket server failed to start.")


# -----------------------------------
# PREPARE NEW SNAPSHOT
# -----------------------------------

def prepare_snapshot(trend_data):
    """Extract and organise trend information from the API response."""
    trends = trend_data.get("trends", [])

    snapshot = []

    for rank, trend in enumerate(trends, start=1):

        topic_name = trend.get("displayName")

        if not topic_name:
            raise ValueError("A trend is missing its displayName")

        post_count = trend.get("postCount")
        category = trend.get("category")
        status = trend.get("status")

        snapshot.append(
            (
                rank,
                topic_name,
                post_count,
                category,
                status
            )
        )

    return tuple(snapshot)


# -----------------------------------
# DISPLAY LATEST SNAPSHOT
# -----------------------------------

def display_snapshot(snapshot):
    """Display the current trend snapshot in the terminal."""
    print("\n========== BLUESKY TOP TRENDS ==========")

    rank_width = 8
    topic_width = 38
    post_width = 15
    category_width = 22
    status_width = 15

    header = (
        f"{'Rank':<{rank_width}}"
        f"{'Topic':<{topic_width}}"
        f"{'Post Count':<{post_width}}"
        f"{'Category':<{category_width}}"
        f"{'Status':<{status_width}}"
    )

    print(header)
    print("-" * len(header))

    for rank, topic_name, post_count, category, status in snapshot:

        if len(topic_name) > topic_width - 3:
            topic_name = topic_name[:topic_width - 3] + "..."

        print(
            f"{rank:<{rank_width}}"
            f"{topic_name:<{topic_width}}"
            f"{str(post_count):<{post_width}}"
            f"{str(category):<{category_width}}"
            f"{str(status):<{status_width}}"
        )

    print("=" * len(header))


# -----------------------------------
# SAVE COMPLETE SNAPSHOT
# -----------------------------------

def save_snapshot(snapshot):
    """Save all trends in a snapshot to PostgreSQL."""
    recorded_at = datetime.now(timezone.utc)

    query = """
        INSERT INTO trend_metrics (
            recorded_at,
            rank,
            topic_name,
            post_count,
            category,
            status
        )
        VALUES (%s, %s, %s, %s, %s, %s);
    """

    rows_to_insert = []

    for trend in snapshot:

        rank, topic_name, post_count, category, status = trend

        rows_to_insert.append(
            (
                recorded_at,
                rank,
                topic_name,
                post_count,
                category,
                status
            )
        )

    with psycopg.Connection.transaction(connection):

        with psycopg.Connection.cursor(connection) as cursor:
            cursor.executemany(query, rows_to_insert)

    print(f"\nSnapshot saved successfully at {recorded_at}")


# -----------------------------------
# PROCESS KAFKA MESSAGES
# -----------------------------------

print("Starting Bluesky trends processor...")

if previous_snapshot:

    print("\nPrevious snapshot loaded from PostgreSQL.")

    display_snapshot(previous_snapshot)

else:

    print("\nNo previous snapshot found. First snapshot will be saved.")

print("\nWaiting for trend updates...\n")


try:

    for message in consumer:

        trend_data = message.value

        try:

            current_snapshot = prepare_snapshot(trend_data)

            if not current_snapshot:
                print("No trends received. Skipping snapshot.")
                continue

            # Check whether anything has changed

            if current_snapshot != previous_snapshot:

                print("\nChange detected!")

                save_snapshot(current_snapshot)

                previous_snapshot = current_snapshot

                # Update the latest snapshot
                latest_snapshot = current_snapshot

                # Broadcast the new snapshot to React
                publish_snapshot(current_snapshot)

                # Display the newly saved snapshot
                display_snapshot(current_snapshot)

            else:

                print("No changes detected. Skipping database insert.")

        except Exception as error:

            print("Error processing trend data:", error)


except KeyboardInterrupt:

    print("\nStopping trends processor...")


finally:

    consumer.close()
    psycopg.Connection.close(connection)

    print("Trends processor stopped.")
