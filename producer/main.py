
import asyncio
import json
import logging
import os
import random
from datetime import datetime, timezone
from urllib.parse import urlencode

import websockets
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

JETSTREAM_BASE = os.getenv(
    "JETSTREAM_URL",
    "wss://jetstream2.us-east.bsky.network/subscribe",
)

COLLECTIONS = ["app.bsky.feed.post"]

EVENTHUB_CONNECTION_STRING = os.environ[
    "EVENTHUB_CONNECTION_STRING"
]
EVENTHUB_NAME = os.environ["EVENTHUB_NAME"]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def build_url():
    query = urlencode([
        ("wantedCollections", collection)
        for collection in COLLECTIONS
    ])
    return f"{JETSTREAM_BASE}?{query}"


async def main():
    delay = 1

    async with EventHubProducerClient.from_connection_string(
        conn_str=EVENTHUB_CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME,
    ) as producer:

        while True:
            try:
                url = build_url()
                logging.info("Connecting to Jetstream")

                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=2**20,
                ) as ws:

                    logging.info("Connected to Jetstream")
                    delay = 1

                    async for message in ws:
                        received_at = utc_now()

                        try:
                            event = json.loads(message)
                        except json.JSONDecodeError:
                            logging.warning("Invalid JSON received")
                            continue

                        envelope = {
                            "source": "bluesky_jetstream",
                            "ingestion_timestamp": received_at,
                            "jetstream_event": event,
                        }

                        payload = json.dumps(
                            envelope,
                            separators=(",", ":"),
                        )

                        batch = await producer.create_batch()
                        batch.add(EventData(payload))
                        await producer.send_batch(batch)

                        logging.info(
                            "Published kind=%s collection=%s",
                            event.get("kind"),
                            (event.get("commit") or {}).get(
                                "collection"
                            ),
                        )

            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception(
                    "Connection or publishing failure"
                )

                await asyncio.sleep(
                    min(delay, 60) + random.uniform(0, 0.5)
                )
                delay = min(delay * 2, 60)


if __name__ == "__main__":
    asyncio.run(main())
