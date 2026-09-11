# 5 Minute Video Walkthrough Guide
# Kafka Order Processing Pipeline

======================================================================
Screen Setup
======================================================================
Left Screen: Web browser open to Kafka UI (http://localhost:8080)
Right Top: Terminal 1 (Producer & Test Commands)
Right Bottom: Terminal 2 (Consumer - keep running)

======================================================================
1. Introduction (0:00 to 0:30)
======================================================================
On Screen:
Show the report title page or GitHub README.

What to Say:
"Hello, my name is Munsif, EG/2021/4684, Computer Engineering, University of Ruhuna. 
Today I am demonstrating our Kafka stream processing pipeline.
It handles real time orders using Apache Avro, calculates running averages in constant O(1) time, and handles failures with exponential retries and a Dead Letter Queue."

======================================================================
2. Start Cluster & Show Kafka UI (0:30 to 1:15)
======================================================================
On Screen:
Terminal 1 and Web browser side by side.

Command (Terminal 1):
make up

Actions in Kafka UI (http://localhost:8080):
1. Show cluster status: Healthy, KRaft mode on port 9092.
2. Click "Topics" in left menu.
3. Point out two topics:
   * "orders": 3 partitions for parallel processing.
   * "orders-dlq": quarantine queue for bad orders.

What to Say:
"First, we start Kafka in KRaft mode using Docker. KRaft manages metadata internally without ZooKeeper.
In Kafka UI, our cluster is online. We have two topics: 'orders' with three partitions, and 'orders-dlq' for isolating bad messages."

======================================================================
3. Normal Orders & O(1) Running Average (1:15 to 2:15)
======================================================================
On Screen:
Terminal 2 (bottom), Terminal 1 (top), and Kafka UI (left).

Step 1: Start Consumer (Terminal 2):
uv run python -m src.consumer

Step 2: Send 10 Orders (Terminal 1):
uv run python -m src.producer --mode normal --count 10

Actions in Kafka UI:
1. Click topic "orders" -> Click "Messages" tab.
2. Click any message to show decoded Avro fields: orderId, product, price.

What to Point Out in Terminal 2:
Point to the Rich table updating live: Total count (Nt), cumulative sum (St), and average price.

What to Say:
"In Terminal 2, we start our consumer.
In Terminal 1, we send 10 orders. Apache Avro packs each order into just 22 bytes, saving 74% bandwidth compared to JSON.
In Terminal 2, the table updates live. The consumer calculates the running average in O(1) time and minimal memory.
In Kafka UI, we see messages distributed across all three partitions."

======================================================================
4. Transient Error & Retry Recovery (2:15 to 3:00)
======================================================================
On Screen:
Terminal 1 and Terminal 2.

Command (Terminal 1):
uv run python -m src.producer --mode transient --count 1

What to Point Out in Terminal 2:
1. Attempt 1 fails: Shows yellow retry log with 1 second delay.
2. Attempt 2 fails: Shows yellow retry log with 2 second delay.
3. Attempt 3 succeeds: Shows green success log, updates average, and commits offset.

What to Say:
"Now we inject a temporary network glitch.
Instead of failing, the consumer catches the error and applies exponential backoff with jitter.
It waits 1 second, then 2 seconds. On attempt 3, the service recovers.
The order succeeds, updates the average, and no data is lost."

======================================================================
5. Poison Pill & DLQ Isolation (3:00 to 3:45)
======================================================================
On Screen:
Terminal 1 and Terminal 2.

Command (Terminal 1):
uv run python -m src.producer --mode poison-pill --count 1

What to Point Out in Terminal 2:
1. Order has negative price (-$999.00).
2. Classified as fatal BusinessValidationError.
3. Retries skipped immediately.
4. Sent to 'orders-dlq' and main offset committed so consumer never freezes.

What to Say:
"Now we test a fatal poison pill by sending an order with a negative price.
Retrying a bad price will never fix it. The consumer skips retries, packages the error, and pushes it directly to the Dead Letter Queue.
It commits the offset so valid orders can keep flowing without getting stuck."

======================================================================
6. Inspecting the DLQ in UI & CLI (3:45 to 4:30)
======================================================================
On Screen:
Left: Web browser in Kafka UI.
Right: Terminal 1.

Actions in Kafka UI:
1. Click "Topics" -> Click "orders-dlq".
2. Click "Messages" tab.
3. Click on the message to expand JSON envelope.
4. Point to: originalTopic, partition, offset, timestamp, errorType, and errorMessage.

Command (Terminal 1):
uv run python -m src.dlq_inspector

What to Say:
"In Kafka UI under 'orders-dlq', we inspect the quarantined message.
The envelope saves the original topic, partition, offset, timestamp, error type, and failure reason.
We also built a CLI DLQ Inspector in Terminal 1, which prints this audit report cleanly."

======================================================================
7. Automated Tests & Conclusion (4:30 to 5:00)
======================================================================
On Screen:
Terminal 1, then switch browser to GitHub repository.

Command (Terminal 1):
uv run pytest -v

Actions in Browser:
Show GitHub repository: https://github.com/munsif-dev/kafka-order-process-pipeline

What to Say:
"Finally, we run our test suite using pytest.
All five unit tests pass, verifying Avro encoding, corrupt data rejection, running average math, and retry logic.
All code and configurations are open source on GitHub.
Thank you for watching."

======================================================================
Live Recording Cheat Sheet (Commands Only)
======================================================================
Terminal 2 (Consumer - keep running):
uv run python -m src.consumer

Terminal 1 (Run in order):
1. Start Docker:
   make up

2. Normal orders:
   uv run python -m src.producer --mode normal --count 10

3. Transient retry test:
   uv run python -m src.producer --mode transient --count 1

4. Poison pill DLQ test:
   uv run python -m src.producer --mode poison-pill --count 1

5. Inspect DLQ:
   uv run python -m src.dlq_inspector

6. Run unit tests:
   uv run pytest -v
