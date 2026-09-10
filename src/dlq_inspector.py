import json
from confluent_kafka import Consumer, KafkaError, KafkaException
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_ORDERS_DLQ

console = Console()


def inspect_dlq(bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, dlq_topic: str = TOPIC_ORDERS_DLQ):
    """Consume and display quarantined poison pills and failure envelopes from orders-dlq."""
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'dlq-inspector-group-unique',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })

    consumer.subscribe([dlq_topic])
    console.rule(f"[bold red]Dead Letter Queue (DLQ) Inspector - {dlq_topic}[/bold red]")
    console.print(f"Monitoring topic: [bold yellow]{dlq_topic}[/bold yellow]")
    console.print("Waiting for quarantined failure events... (Press Ctrl+C to exit)\n")

    count = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            count += 1
            raw_val = msg.value().decode("utf-8", errors="replace")

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


if __name__ == "__main__":
    inspect_dlq()

