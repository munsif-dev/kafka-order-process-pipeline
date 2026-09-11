import base64
import json
import random
import time
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.avro_helper import AvroHelper, AvroDeserializationError
from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ORDERS,
    TOPIC_ORDERS_DLQ,
    CONSUMER_GROUP_ID,
    MAX_RETRIES,
    RETRY_BASE_DELAY_SECONDS,
    RETRY_BACKOFF_FACTOR,
    RETRY_JITTER_MAX_SECONDS,
)

console = Console()


class TransientProcessingError(Exception):
    """Raised when an external transient error (e.g., timeout, lock) occurs."""
    pass


class PermanentProcessingError(Exception):
    """Raised when an unrecoverable business or schema error occurs."""
    pass


class StreamOrderConsumer:
    """Production-grade stream consumer with Avro deserialization, real-time running aggregation,
    exponential backoff retry logic, and Dead Letter Queue (DLQ) quarantine."""

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        input_topic: str = TOPIC_ORDERS,
        dlq_topic: str = TOPIC_ORDERS_DLQ,
        group_id: str = CONSUMER_GROUP_ID,
    ):
        self.input_topic = input_topic
        self.dlq_topic = dlq_topic
        self.avro = AvroHelper()

        # Consumer configuration
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Manual commit ensures zero message loss
            'session.timeout.ms': 45000,
            'max.poll.interval.ms': 300000,
        })

        # Dedicated DLQ Producer
        self.dlq_producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'order-dlq-producer',
            'acks': 'all',
        })

        # Stateful Aggregation Counters
        self.order_count = 0
        self.cumulative_price = 0.0
        self.dlq_count = 0
        self.transient_recoveries = 0

    def _send_to_dlq(
        self,
        raw_key: Optional[bytes],
        raw_value: bytes,
        partition: int,
        offset: int,
        error_type: str,
        error_message: str,
        retry_attempts: int,
    ):
        """Construct an enriched diagnostic envelope and forward unrecoverable records to orders-dlq."""
        self.dlq_count += 1
        failed_timestamp = datetime.now(timezone.utc).isoformat()

        # Try to decode or base64 encode raw value for diagnostic visibility
        if raw_value is None:
            safe_payload_preview = "<null>"
        else:
            try:
                safe_payload_preview = raw_value.decode("utf-8")
            except (UnicodeDecodeError, AttributeError):
                safe_payload_preview = base64.b64encode(raw_value).decode("ascii")

        dlq_envelope = {
            "originalTopic": self.input_topic,
            "originalPartition": partition,
            "originalOffset": offset,
            "originalKey": raw_key.decode("utf-8", errors="ignore") if raw_key else None,
            "failedAt": failed_timestamp,
            "retryAttempts": retry_attempts,
            "errorType": error_type,
            "errorMessage": error_message,
            "rawPayload": safe_payload_preview,
        }

        envelope_bytes = json.dumps(dlq_envelope, indent=2).encode("utf-8")

        self.dlq_producer.produce(
            topic=self.dlq_topic,
            key=raw_key if raw_key else b"DLQ_ERROR",
            value=envelope_bytes,
        )
        self.dlq_producer.flush(timeout=3)

        console.print(
            Panel(
                f"[bold red]DEAD LETTER QUEUE (DLQ) QUARANTINE[/bold red]\n"
                f"Offset: [yellow]{offset}[/yellow] | Partition: [yellow]{partition}[/yellow]\n"
                f"Reason: [white]{error_message}[/white]\n"
                f"Error Type: [bold red]{error_type}[/bold red] | Attempts: [cyan]{retry_attempts}[/cyan]\n"
                f"Forwarded safely to topic: [bold yellow]{self.dlq_topic}[/bold yellow]",
                border_style="red",
            )
        )

    def _execute_business_logic(self, order: Dict[str, Any], attempt: int = 1):
        """Simulate order processing and external dependencies."""
        product = order.get("product", "")
        price = order.get("price", 0.0)

        # 1. Business logic check: Price must be strictly positive
        if price <= 0:
            raise PermanentProcessingError(f"Validation failed: Price cannot be non-positive ({price})")

        # 2. Simulated transient downstream dependency error
        if "TRIGGER_TRANSIENT_GLITCH" in product:
            # Simulate recovery on attempt 2 to demonstrate self-healing retry
            if attempt < 2:
                raise TransientProcessingError("Simulated downstream payment gateway timeout (HTTP 504)")

    def _process_with_retry(
        self,
        order: Dict[str, Any],
        raw_key: Optional[bytes],
        raw_value: bytes,
        partition: int,
        offset: int,
    ) -> bool:
        """Execute processing with exponential backoff retry logic."""
        attempt = 1
        while attempt <= MAX_RETRIES:
            try:
                self._execute_business_logic(order, attempt=attempt)
                if attempt > 1:
                    self.transient_recoveries += 1
                    console.print(
                        f"[bold green]✓ RECOVERED[/bold green] Order [yellow]{order['orderId']}[/yellow] "
                        f"succeeded on retry attempt {attempt}!"
                    )
                return True

            except TransientProcessingError as err:
                if attempt < MAX_RETRIES:
                    delay = (RETRY_BASE_DELAY_SECONDS * (RETRY_BACKOFF_FACTOR ** (attempt - 1))) + random.uniform(
                        0, RETRY_JITTER_MAX_SECONDS
                    )
                    console.print(
                        f"[bold yellow][RETRY][/bold yellow] Order [cyan]{order['orderId']}[/cyan]: "
                        f"{err}. Waiting [bold]{delay:.2f}s[/bold] before attempt {attempt + 1}/{MAX_RETRIES}..."
                    )
                    time.sleep(delay)
                    attempt += 1
                else:
                    console.print(
                        f"[bold red]Exhausted {MAX_RETRIES} retries[/bold red] for order [cyan]{order['orderId']}[/cyan]."
                    )
                    self._send_to_dlq(
                        raw_key=raw_key,
                        raw_value=raw_value,
                        partition=partition,
                        offset=offset,
                        error_type="RetryExhaustionError",
                        error_message=f"Exhausted {MAX_RETRIES} retry attempts: {err}",
                        retry_attempts=attempt,
                    )
                    return False

            except PermanentProcessingError as err:
                # Permanent failure: bypass retries directly to DLQ
                self._send_to_dlq(
                    raw_key=raw_key,
                    raw_value=raw_value,
                    partition=partition,
                    offset=offset,
                    error_type="BusinessValidationError",
                    error_message=str(err),
                    retry_attempts=attempt,
                )
                return False

        return False

    def _update_running_average(self, price: float) -> float:
        """Stateful calculation of the running average price in O(1) time."""
        self.order_count += 1
        self.cumulative_price += price
        return self.cumulative_price / self.order_count

    def display_metrics_dashboard(self, order: Dict[str, Any], running_avg: float, partition: int, offset: int):
        """Render a clean live metrics dashboard in the terminal."""
        table = Table(title="Kafka Real-Time Order Stream Aggregator", style="cyan")
        table.add_column("Metric", style="bold white")
        table.add_column("Current Value", style="bold green")

        table.add_row("Last Processed Order ID", str(order.get("orderId")))
        table.add_row("Last Item Purchased", str(order.get("product")))
        table.add_row("Last Transaction Price", f"${order.get('price', 0.0):,.2f}")
        table.add_row("Kafka Partition / Offset", f"{partition} / {offset}")
        table.add_row("Total Valid Orders Processed", f"{self.order_count:,}")
        table.add_row("Cumulative Gross Revenue", f"${self.cumulative_price:,.2f}")
        table.add_row("Current Running Average Price", f"${running_avg:,.2f}")
        table.add_row("Transient Self-Healed Recoveries", f"{self.transient_recoveries}")
        table.add_row("Quarantined Poison Pills (DLQ)", f"{self.dlq_count}")

        console.print(table)
        console.print()

    def run(self, max_messages: int = 0, timeout_seconds: float = 0):
        """Main event loop for consuming and processing stream records."""
        self.consumer.subscribe([self.input_topic])
        console.rule("[bold green]Kafka Fault-Tolerant Order Consumer Started[/bold green]")
        console.print(f"Subscribed to topic: [bold yellow]{self.input_topic}[/bold yellow]")
        console.print(f"DLQ Topic: [bold red]{self.dlq_topic}[/bold red]")
        console.print(f"Consumer Group: [cyan]{CONSUMER_GROUP_ID}[/cyan]")
        console.print("Awaiting order events... Press Ctrl+C to stop.\n")

        processed_total = 0
        start_time = time.time()
        idle_polls = 0

        try:
            while True:
                if max_messages > 0 and processed_total >= max_messages:
                    console.print(f"[bold green]Reached target of {max_messages} messages. Exiting cleanly.[/bold green]")
                    break

                if timeout_seconds > 0 and (time.time() - start_time) > timeout_seconds:
                    console.print(f"[bold yellow]Timeout reached ({timeout_seconds}s). Exiting.[/bold yellow]")
                    break

                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    idle_polls += 1
                    # If max_messages was set and we have been idle for 5 consecutive polls AFTER processing something, break
                    if max_messages > 0 and processed_total > 0 and idle_polls >= 5:
                        break
                    continue

                idle_polls = 0
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())

                raw_val = msg.value()
                if raw_val is None:
                    self.consumer.commit(msg, asynchronous=False)
                    continue

                raw_key = msg.key()
                partition = msg.partition()
                offset = msg.offset()
                processed_total += 1

                # Step 1: Avro Deserialization
                try:
                    order = self.avro.deserialize(raw_val)
                except AvroDeserializationError as err:
                    # Deserialization failure is an immediate unrecoverable poison pill -> DLQ
                    self._send_to_dlq(
                        raw_key=raw_key,
                        raw_value=raw_val,
                        partition=partition,
                        offset=offset,
                        error_type="AvroDeserializationError",
                        error_message=str(err),
                        retry_attempts=0,
                    )
                    self.consumer.commit(msg, asynchronous=False)
                    continue

                # Step 2: Processing with Exponential Backoff Retry Logic
                success = self._process_with_retry(
                    order=order,
                    raw_key=raw_key,
                    raw_value=raw_val,
                    partition=partition,
                    offset=offset,
                )

                # Step 3: Stateful Aggregation (if valid)
                if success:
                    running_avg = self._update_running_average(order["price"])
                    self.display_metrics_dashboard(order, running_avg, partition, offset)

                # Step 4: Commit offset regardless so bad messages never block the stream
                self.consumer.commit(msg, asynchronous=False)

        except KeyboardInterrupt:
            console.print("\n[yellow]Shutting down consumer...[/yellow]")
        finally:
            self.consumer.close()
            self.dlq_producer.flush(timeout=5)
            console.print("[bold green]Consumer closed cleanly.[/bold green]")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Kafka Order Stream Consumer")
    parser.add_argument("--max-messages", type=int, default=0, help="Maximum messages to consume before exiting (0 = infinite)")
    parser.add_argument("--timeout", type=float, default=0, help="Timeout in seconds before exiting (0 = infinite)")
    args = parser.parse_args()

    consumer = StreamOrderConsumer()
    consumer.run(max_messages=args.max_messages, timeout_seconds=args.timeout)


if __name__ == "__main__":
    main()

