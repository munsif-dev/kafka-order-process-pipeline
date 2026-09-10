import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_DIR = BASE_DIR / "schema"
AVRO_SCHEMA_PATH = SCHEMA_DIR / "order.avsc"

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_ORDERS = os.getenv("TOPIC_ORDERS", "orders")
TOPIC_ORDERS_DLQ = os.getenv("TOPIC_ORDERS_DLQ", "orders-dlq")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "order-processing-group")

# Resilience & Retry Settings
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BASE_DELAY_SECONDS = float(os.getenv("RETRY_BASE_DELAY_SECONDS", "1.0"))
RETRY_BACKOFF_FACTOR = float(os.getenv("RETRY_BACKOFF_FACTOR", "2.0"))
RETRY_JITTER_MAX_SECONDS = float(os.getenv("RETRY_JITTER_MAX_SECONDS", "0.5"))

# Producer Pacing
PRODUCER_DEFAULT_INTERVAL = float(os.getenv("PRODUCER_DEFAULT_INTERVAL", "1.0"))
