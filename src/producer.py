import argparse
import os
import random
import time
from datetime import datetime
from typing import Dict, Any

from confluent_kafka import Producer
from rich.console import Console
from rich.table import Table

from src.avro_helper import AvroHelper
from src.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_ORDERS, PRODUCER_DEFAULT_INTERVAL

console = Console()

# Catalog of realistic items
PRODUCTS = [
    "Laptop Pro 15-inch",
    "Ultra-Wide 4K Monitor",
    "Mechanical Gaming Keyboard",
    "Wireless Ergonomic Mouse",
    "Noise-Cancelling Headphones",
    "USB-C Docking Station",
    "Smartwatch Series 8",
    "External NVMe SSD 2TB",
    "HD Webcam 1080p",
    "Smartphone 5G 256GB"
]


class OrderProducer:
    """Producer for generating and publishing Avro-serialized orders to Kafka."""

    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, topic: str = TOPIC_ORDERS):
        self.topic = topic
        self.avro = AvroHelper()
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'order-producer',
            'acks': 'all',
            'retries': 3,
            'linger.ms': 5,
        })
        self._order_sequence = 1000

    def _delivery_callback(self, err, msg):
        if err:
            console.print(f"[bold red]Delivery failed for order {msg.key()}: {err}[/bold red]")
        else:
            console.print(
                f"[dim]Delivered to {msg.topic()} [part: {msg.partition()} | off: {msg.offset()}] (key: {msg.key().decode('utf-8') if msg.key() else 'None'})[/dim]"
            )

    def generate_order(self, mode: str = "normal") -> tuple[Dict[str, Any], bytes]:
        """Generate an order dictionary and corresponding binary payload based on mode."""
        self._order_sequence += 1
        order_id = f"ORD-{self._order_sequence}"

        if mode == "normal":
            product = random.choice(PRODUCTS)
            price = round(random.uniform(15.0, 1200.0), 2)
            record = {"orderId": order_id, "product": product, "price": float(price)}
            binary_payload = self.avro.serialize(record)
            return record, binary_payload

        elif mode == "transient":
            # Tagged with simulated transient glitch in product name
            product = "TRIGGER_TRANSIENT_GLITCH: " + random.choice(PRODUCTS)
            price = round(random.uniform(50.0, 300.0), 2)
            record = {"orderId": order_id, "product": product, "price": float(price)}
            binary_payload = self.avro.serialize(record)
            return record, binary_payload

        elif mode == "poison-pill":
            # Violates business logic: negative price
            product = "POISON_PILL: " + random.choice(PRODUCTS)
            price = -999.00
            record = {"orderId": order_id, "product": product, "price": float(price)}
            binary_payload = self.avro.serialize(record)
            return record, binary_payload

        elif mode == "malformed-avro":
            # Raw corrupted non-Avro bytes
            record = {"orderId": order_id, "product": "CORRUPT_BYTES", "price": 0.0}
            binary_payload = b"\xde\xad\xbe\xef\x00\xffCORRUPT_BINARY_NOISE_DATA"
            return record, binary_payload

        else:
            raise ValueError(f"Unknown mode: {mode}")

    def produce_one(self, mode: str = "normal"):
        """Produce a single order event."""
        record, payload = self.generate_order(mode=mode)
        order_id = record["orderId"]

        console.print(
            f"[bold cyan][PRODUCER][/bold cyan] Order: [yellow]{order_id}[/yellow] | "
            f"Product: [green]{record['product']}[/green] | "
            f"Price: [bold magenta]${record['price']:.2f}[/bold magenta] | "
            f"Mode: [bold]{mode.upper()}[/bold] | "
            f"Avro Size: [blue]{len(payload)} bytes[/blue]"
        )

        self.producer.produce(
            topic=self.topic,
            key=order_id.encode("utf-8"),
            value=payload,
            on_delivery=self._delivery_callback,
        )
        self.producer.poll(0)

    def run(self, mode: str = "normal", count: int = 0, interval: float = PRODUCER_DEFAULT_INTERVAL):
        """Run the producer loop."""
        console.rule("[bold green]Starting Order Stream Producer[/bold green]")
        console.print(f"Target Topic: [bold yellow]{self.topic}[/bold yellow]")
        console.print(f"Mode: [bold magenta]{mode}[/bold magenta] | Interval: {interval}s")
        console.print("Press Ctrl+C to terminate cleanly.\n")

        sent = 0
        try:
            while True:
                self.produce_one(mode=mode)
                sent += 1
                if count > 0 and sent >= count:
                    break
                time.sleep(interval)
        except KeyboardInterrupt:
            console.print("\n[yellow]Stopping producer... Flushing pending events.[/yellow]")
        finally:
            self.producer.flush(timeout=5)
            console.print(f"[bold green]Producer finished. Total orders sent: {sent}[/bold green]")


def main():
    parser = argparse.ArgumentParser(description="Kafka Avro Order Stream Producer")
    parser.add_argument(
        "--mode",
        choices=["normal", "transient", "poison-pill", "malformed-avro"],
        default="normal",
        help="Generation mode: normal, transient (simulates network timeout), poison-pill (negative price), malformed-avro (corrupt binary)",
    )
    parser.add_argument("--count", type=int, default=0, help="Number of orders to send (0 = infinite)")
    parser.add_argument("--interval", type=float, default=PRODUCER_DEFAULT_INTERVAL, help="Seconds between messages")
    parser.add_argument("--topic", default=TOPIC_ORDERS, help="Kafka destination topic")

    args = parser.parse_args()
    producer = OrderProducer(topic=args.topic)
    producer.run(mode=args.mode, count=args.count, interval=args.interval)


if __name__ == "__main__":
    main()
