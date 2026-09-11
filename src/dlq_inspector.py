import json
from confluent_kafka import Consumer, KafkaError, KafkaException
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_ORDERS_DLQ

console = Console()


def inspect_dlq(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    dlq_topic: str = TOPIC_ORDERS_DLQ,
    max_messages: int = 0,
    timeout_seconds: float = 0,
):
    """Consume and display quarantined poison pills and failure envelopes from orders-dlq."""
    import time
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': f'dlq-inspector-group-{int(time.time())}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })

    consumer.subscribe([dlq_topic])
    console.rule(f"[bold red]Dead Letter Queue (DLQ) Inspector - {dlq_topic}[/bold red]")
    console.print(f"Monitoring topic: [bold yellow]{dlq_topic}[/bold yellow]")
    console.print("Waiting for quarantined failure events... (Press Ctrl+C to exit)\n")

    count = 0
    start_time = time.time()
    idle_polls = 0

    try:
        while True:
            if max_messages > 0 and count >= max_messages:
                break
            if timeout_seconds > 0 and (time.time() - start_time) > timeout_seconds:
                break

            msg = consumer.poll(timeout=1.0)
            if msg is None:
                idle_polls += 1
                if count > 0 and idle_polls >= 3:
                    break
                continue

            idle_polls = 0
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            raw_bytes = msg.value()
            if raw_bytes is None:
                continue

            count += 1
            raw_val = raw_bytes.decode("utf-8", errors="replace")

            try:
                envelope = json.loads(raw_val)
                table = Table(title=f"Quarantined Event #{count} [Offset: {msg.offset()}]", style="red")
                table.add_column("Field", style="bold white")
                table.add_column("Value", style="yellow")

                table.add_row("Original Topic", str(envelope.get("originalTopic")))
                table.add_row("Partition / Offset", f"{envelope.get('originalPartition')} / {envelope.get('originalOffset')}")
                table.add_row("Order Key", str(envelope.get("originalKey")))
                table.add_row("Timestamp", str(envelope.get("failedAt")))
                table.add_row("Retry Attempts", str(envelope.get("retryAttempts")))
                table.add_row("Error Type", f"[bold red]{envelope.get('errorType')}[/bold red]")
                table.add_row("Error Message", str(envelope.get("errorMessage")))
                table.add_row("Payload Preview", str(envelope.get("rawPayload"))[:80] + "...")

                console.print(table)
                console.print()
            except json.JSONDecodeError:
                console.print(Panel(f"Raw Non-JSON DLQ Payload: {raw_val}", border_style="red"))

    except KeyboardInterrupt:
        console.print("\n[yellow]Stopping DLQ inspector...[/yellow]")
    finally:
        consumer.close()
        console.print(f"[bold green]DLQ inspector exited. Inspected {count} quarantined records.[/bold green]")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Dead Letter Queue Inspector")
    parser.add_argument("--max-messages", type=int, default=0, help="Max DLQ messages to read")
    parser.add_argument("--timeout", type=float, default=0, help="Timeout in seconds")
    args = parser.parse_args()

    inspect_dlq(max_messages=args.max_messages, timeout_seconds=args.timeout)


if __name__ == "__main__":
    main()

