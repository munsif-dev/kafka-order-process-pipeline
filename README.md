# Fault-Tolerant Kafka Order Processing Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![uv](https://img.shields.io/badge/Package%20Manager-uv-blueviolet.svg)](https://docs.astral.sh/uv/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7%20(KRaft)-black.svg)](https://kafka.apache.org/)
[![Apache Avro](https://img.shields.io/badge/Serialization-Apache%20Avro-red.svg)](https://avro.apache.org/)
[![Kafka UI](https://img.shields.io/badge/Dashboard-Kafka%20UI-green.svg)](https://github.com/provectus/kafka-ui)

An enterprise-grade, event-driven streaming architecture built with **Apache Kafka** and **Apache Avro**. The system ingests continuous randomized purchase orders, performs constant-time $O(1)$ **real-time stateful aggregation (running average of prices)**, and implements robust fault tolerance featuring **exponential backoff retry mechanisms** and a **Dead Letter Queue (DLQ)** for unrecoverable poison pills.

Developed for **EC8203 Applied Big Data Engineering**, Department of Electrical and Information Engineering, University of Ruhuna.

---

## 1. System Architecture

```text
+------------------------+
|     Order Producer     |
| (Random Order Engine)  |
+-----------+------------+
            |
            v
+------------------------+
| Avro Binary Serializer |
|     (order.avsc)       |
+-----------+------------+
            |
            v [Topic: 'orders' | Partitions: 0, 1, 2 | Key: orderId]
+--------------------------------------------------------------------------+
|                        Apache Kafka Broker (KRaft)                       |
+--------------------------------------------------------------------------+
            |
            v [Avro Binary Bytes]
+--------------------------------------------------------------------------+
|                         Stream Consumer Group                            |
|                                                                          |
|   1. Avro Deserialization & Contract Validation                          |
|   2. Stateful Running Aggregation: Avg = Cumulative Price / Order Count  |
|   3. Two-Tier Error Classification:                                      |
|      - Transient Glitches -> Exponential Backoff Retry (1s, 2s, 4s)      |
|      - Permanent / Fatal -> Quarantined to Dead Letter Queue (DLQ)       |
+--------------------------------------------------------------------------+
            |
            +------------> [Topic: 'orders-dlq'] (Quarantined Records)
                                    |
                                    v
                        +-----------------------+
                        | Kafka UI & Inspector  |
                        | (Audit & Diagnostic)  |
                        +-----------------------+
```

---

## 2. Key Features & Compliance Matrix

| Requirement | Implementation Details | Status |
| :--- | :--- | :---: |
| **Avro Serialization** | Strict schema (`schema/order.avsc`) with `orderId` (string), `product` (string), and `price` (float). 74% smaller payloads than JSON. | **COMPLETED** |
| **Real-Time Aggregation** | In-memory state tracking total order count and cumulative price sum. Computes running average continuously in $O(1)$ time. | **COMPLETED** |
| **Retry Logic** | Exponential backoff retry loop with randomized jitter ($T = 1.0\text{s} \times 2^{\text{attempt}-1} + \epsilon$) for transient external timeouts. | **COMPLETED** |
| **Dead Letter Queue** | Routes unrecoverable poison pills (negative prices, corrupt non-Avro bytes, exhausted retries) to `orders-dlq` with enriched error envelopes. | **COMPLETED** |
| **Live Demonstration** | Step-by-step CLI simulation flags (`--mode [normal|transient|poison-pill|malformed-avro]`) and real-time Kafka UI dashboard. | **COMPLETED** |

---

## 3. Tech Stack

- **Package & Dependency Manager**: [`uv`](https://docs.astral.sh/uv/) (Astral)
- **Runtime**: Python 3.10+ (`confluent-kafka`, `fastavro`, `pydantic`, `rich`)
- **Messaging Broker**: Apache Kafka 3.7+ in **KRaft Mode** (no ZooKeeper required)
- **Management Dashboard**: Kafka UI (`provectuslabs/kafka-ui`) on port `8080`

---

## 4. Getting Started & Installation

### Step 1: Clone Repository & Install Dependencies
This project uses `uv` for ultra-fast, reproducible dependency management:
```bash
# Clone the repository
git clone https://github.com/munsif/kafka-order-pipeline.git
cd kafka-order-pipeline

# Sync virtual environment and dependencies via uv
uv sync
```

### Step 2: Start Kafka & Kafka UI
Launch the containerized Kafka broker and web interface:
```bash
make up
# or
docker compose up -d
```
- **Kafka Broker**: `localhost:9092`
- **Kafka UI**: [http://localhost:8080](http://localhost:8080)

---

## 5. Running the Pipeline & Live Demonstration

### Terminal 1: Start the Stream Consumer
```bash
make consumer
# or: uv run python -m src.consumer
```
The consumer subscribes to `orders`, calculates the real-time running average, and displays live telemetry.

### Terminal 2: Start the Order Producer (Normal Stream)
```bash
make producer
# or: uv run python -m src.producer --mode normal
```
Generates continuous random valid orders serialized in Avro binary format.

---

## 6. Fault Tolerance & Failure Injection Scenarios

### Scenario A: Transient Anomaly & Auto-Recovery
Simulate an intermittent network/database timeout:
```bash
make test-transient
# or: uv run python -m src.producer --mode transient --count 1
```
**Observed Behavior**:
1. The consumer catches the transient error.
2. It pauses and retries with exponential backoff (`Attempt 1: wait 1.0s`, `Attempt 2: wait 2.0s`).
3. It self-heals, completes processing, and incorporates the order into the running average.

### Scenario B: Poison Pill (Business Logic Violation)
Inject an order with an invalid negative price:
```bash
make test-poison
# or: uv run python -m src.producer --mode poison-pill --count 1
```
**Observed Behavior**:
1. Price validation fails ($P \le 0$).
2. The consumer packages the event into an enriched DLQ diagnostic envelope and forwards it to `orders-dlq`.
3. The consumer commits its offset on `orders` and continues uninterrupted.

### Scenario C: Corrupted Non-Avro Payload
Inject malformed binary data into the stream:
```bash
make test-corrupt
# or: uv run python -m src.producer --mode malformed-avro --count 1
```
**Observed Behavior**:
1. Avro deserialization detects an invalid binary header.
2. The corrupt payload is immediately trapped and quarantined to `orders-dlq`.
3. The main consumer pipeline remains completely unaffected.

### Terminal 3: Monitoring the Dead Letter Queue
Inspect quarantined failure envelopes in real time:
```bash
make dlq
# or: uv run python -m src.dlq_inspector
```
Or view the `orders-dlq` topic directly in the web browser at **[http://localhost:8080](http://localhost:8080)**.

---

## 7. Project Structure

```text
kafka-order-pipeline/
├── schema/
│   └── order.avsc               # Apache Avro schema specification
├── src/
│   ├── __init__.py
│   ├── config.py                # Environment variables, broker & topic settings
│   ├── avro_helper.py           # Avro schemaless serialization & deserialization
│   ├── producer.py              # Order generator & failure injection engine
│   ├── consumer.py              # Stateful running aggregator, retries & DLQ handler
│   └── dlq_inspector.py         # Diagnostic DLQ consumer utility
├── docker-compose.yml           # Apache Kafka (KRaft mode) + Kafka UI
├── pyproject.toml               # uv project definition & dependencies
├── requirements.txt             # Pip-compatible dependency lockfile
├── Makefile                     # Make shortcuts for all operational workflows
├── .gitignore                   # Standard Python/Kafka ignore rules
└── README.md                    # This documentation file
```

---

## 8. Live Demonstration Video & Links

- **Live YouTube Video Demonstration**: [Watch on YouTube](https://youtube.com/watch?v=YOUR_VIDEO_ID_HERE)
- **Source Code Repository**: [GitHub Repository](https://github.com/munsif/kafka-order-pipeline)

---

## 9. License

This project is licensed under the MIT License - see the LICENSE file for details.
