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
    level= logging.INFO,
    format = "%(asctime)s %(levelname)s %(message)s",
)

JETSTREAM_BASE = os.getenv(
    "JETSTREAM_URL",
    "wss.//jetstream2.us-east.bsky.network/subscribe",
)