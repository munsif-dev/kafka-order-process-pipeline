import json
import pytest

from src.avro_helper import AvroHelper, AvroDeserializationError, AvroSerializationError
from src.consumer import StreamOrderConsumer, PermanentProcessingError, TransientProcessingError


@pytest.fixture
def avro():
    return AvroHelper()


def test_avro_roundtrip_valid_order(avro):
    order = {"orderId": "ORD-1001", "product": "Laptop", "price": 1250.75}
    binary = avro.serialize(order)
    decoded = avro.deserialize(binary)

    assert decoded["orderId"] == "ORD-1001"
    assert decoded["product"] == "Laptop"
    assert pytest.approx(decoded["price"], 0.01) == 1250.75


def test_avro_rejects_corrupted_bytes(avro):
    corrupt_bytes = b"\x00\x01NOT_AVRO_DATA\xff\xfe"
    with pytest.raises(AvroDeserializationError):
        avro.deserialize(corrupt_bytes)


def test_running_average_math():
    consumer = StreamOrderConsumer.__new__(StreamOrderConsumer)
    consumer.order_count = 0
    consumer.cumulative_price = 0.0

    avg1 = consumer._update_running_average(100.0)
    assert consumer.order_count == 1
    assert consumer.cumulative_price == 100.0
    assert avg1 == 100.0

    avg2 = consumer._update_running_average(200.0)
    assert consumer.order_count == 2
    assert consumer.cumulative_price == 300.0
    assert avg2 == 150.0

    avg3 = consumer._update_running_average(300.0)
    assert consumer.order_count == 3
    assert consumer.cumulative_price == 600.0
    assert avg3 == 200.0


def test_business_validation_negative_price():
    consumer = StreamOrderConsumer.__new__(StreamOrderConsumer)
    invalid_order = {"orderId": "ORD-BAD", "product": "Item", "price": -50.0}

    with pytest.raises(PermanentProcessingError, match="Price cannot be non-positive"):
        consumer._execute_business_logic(invalid_order)


def test_transient_glitch_simulation():
    consumer = StreamOrderConsumer.__new__(StreamOrderConsumer)
    transient_order = {"orderId": "ORD-RETRY", "product": "TRIGGER_TRANSIENT_GLITCH: Laptop", "price": 99.0}

    # Attempt 1 should trigger transient error
    with pytest.raises(TransientProcessingError, match="timeout"):
        consumer._execute_business_logic(transient_order, attempt=1)

    # Attempt 2 should succeed (self-heal)
    consumer._execute_business_logic(transient_order, attempt=2)
