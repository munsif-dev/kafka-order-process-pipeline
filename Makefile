.PHONY: help setup up down run-local-kafka producer consumer dlq test-transient test-poison test-corrupt test clean

help:
	@echo "Available commands:"
	@echo "  make setup            Install dependencies via uv"
	@echo "  make up               Start Kafka (KRaft) and Kafka UI in Docker Desktop"
	@echo "  make down             Stop Docker containers"
	@echo "  make run-local-kafka  Run native local Kafka broker (KRaft mode via Java)"
	@echo "  make consumer         Run stream consumer with running aggregation, retries & DLQ"
	@echo "  make producer         Run producer (normal continuous random stream)"
	@echo "  make dlq              Run DLQ inspector to monitor orders-dlq topic"
	@echo "  make test-transient   Inject a transient failure (tests exponential retry & recovery)"
	@echo "  make test-poison      Inject a poison pill order (price < 0, sent to DLQ)"
	@echo "  make test-corrupt     Inject corrupted non-Avro bytes (sent to DLQ)"
	@echo "  make test             Run full unit test suite with pytest"

setup:
	uv sync

up:
	docker compose up -d
	@echo "Kafka broker listening on localhost:9092"
	@echo "Kafka UI available at http://localhost:8080"

down:
	docker compose down

run-local-kafka:
	./scripts/run_local_kafka.sh

consumer:
	uv run python -m src.consumer

producer:
	uv run python -m src.producer --mode normal

dlq:
	uv run python -m src.dlq_inspector

test-transient:
	uv run python -m src.producer --mode transient --count 1

test-poison:
	uv run python -m src.producer --mode poison-pill --count 1

test-corrupt:
	uv run python -m src.producer --mode malformed-avro --count 1

test:
	uv run pytest -v

clean:
	rm -rf __pycache__ src/__pycache__ .pytest_cache
