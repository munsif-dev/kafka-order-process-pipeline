import io
import json
from typing import Any, Dict
import fastavro
from fastavro.schema import load_schema

from src.config import AVRO_SCHEMA_PATH


class AvroSerializationError(Exception):
    """Raised when an order record cannot be serialized to Avro."""
    pass


class AvroDeserializationError(Exception):
    """Raised when binary bytes cannot be deserialized as an Avro record."""
    pass


class AvroHelper:
    """Helper for Apache Avro schema management, serialization, and deserialization."""

    def __init__(self, schema_path=AVRO_SCHEMA_PATH):
        self.schema_path = schema_path
        self._schema = None
        self._load_schema()

    def _load_schema(self):
        if not self.schema_path.exists():
            raise FileNotFoundError(f"Avro schema not found at {self.schema_path}")
        with open(self.schema_path, "r", encoding="utf-8") as f:
            schema_json = json.load(f)
        self._schema = fastavro.parse_schema(schema_json)

    @property
    def schema(self):
        return self._schema

    def serialize(self, record: Dict[str, Any]) -> bytes:
        """Serialize a Python dictionary into schemaless Avro binary format."""
        try:
            buffer = io.BytesIO()
            fastavro.schemaless_writer(buffer, self._schema, record)
            return buffer.getvalue()
        except Exception as exc:
            raise AvroSerializationError(f"Failed to serialize record {record}: {exc}") from exc

    def deserialize(self, binary_data: bytes) -> Dict[str, Any]:
        """Deserialize schemaless Avro binary data into a Python dictionary."""
        try:
            buffer = io.BytesIO(binary_data)
            record = fastavro.schemaless_reader(buffer, self._schema)
            return record
        except Exception as exc:
            raise AvroDeserializationError(f"Failed to deserialize binary payload ({len(binary_data)} bytes): {exc}") from exc

