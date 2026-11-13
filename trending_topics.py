
"""
Fetch trending topics from the Bluesky API and publish them to Kafka.

The producer polls the Bluesky trends endpoint at a fixed interval
and sends the complete API response to the bluesky-trends topic.
"""

import json
import time
from urllib.request import urlopen

from kafka import KafkaProducer


API_URL = (
    "https://public.api.bsky.app/xrpc/"
    "app.bsky.unspecced.getTrends?limit=10"
)

POLL_INTERVAL = 30


def serialize_data(data):
    """Serialize data into UTF-8 encoded JSON bytes for Kafka."""
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=serialize_data
)


def fetch_trends():
    """Fetch the latest trends and publish the API response to Kafka."""
    with urlopen(API_URL, timeout=15) as response:
        data = json.load(response)

    # Send the complete original API response to Kafka
    producer.send("bluesky-trends", value=data)
    producer.flush()


if __name__ == "__main__":

    try:
        while True:

            try:
                fetch_trends()

            except Exception as error:
                print("Error:", error)

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        pass

    finally:
        producer.close()
