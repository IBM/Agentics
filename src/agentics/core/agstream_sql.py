"""
AGStreamSQL - Flink SQL Compatible Streaming for Agentics

A simplified AGStream variant optimized for Flink SQL queries.
Uses Avro format and sends only state data (no envelope) for direct SQL access.
"""

from __future__ import annotations

import json
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, Type

from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel
from pydantic_core import PydanticUndefined

from agentics.core.streaming_utils import (
    create_kafka_topic,
    get_subject_name,
    kafka_topic_exists,
    schema_exists,
)


def pydantic_to_avro_schema(model: Type[BaseModel]) -> dict:
    """
    Convert a Pydantic model to an Avro schema.

    Args:
        model: Pydantic model class

    Returns:
        Avro schema as a dictionary
    """
    json_schema = model.model_json_schema()

    # Map JSON Schema types to Avro types
    type_mapping = {
        "string": "string",
        "integer": "long",
        "number": "double",
        "boolean": "boolean",
        "array": "array",
        "object": "record",
    }

    # Get default values from Pydantic model fields
    model_fields = model.model_fields

    # Build Avro fields from Pydantic fields
    avro_fields = []
    properties = json_schema.get("properties", {})
    required = json_schema.get("required", [])

    for field_name, field_info in properties.items():
        field_type = field_info.get("type", "string")
        avro_base_type = type_mapping.get(field_type, "string")

        # Get the actual default value from Pydantic model
        pydantic_field = model_fields.get(field_name)
        has_default = (
            pydantic_field is not None
            and pydantic_field.default is not PydanticUndefined
            and pydantic_field.default is not None
        )
        default_value = (
            pydantic_field.default if pydantic_field and has_default else None
        )

        # Build field definition
        field_def = {"name": field_name}

        # Handle optional fields (union with null)
        if field_name not in required:
            field_def["type"] = ["null", avro_base_type]
            # For optional fields, default must be null (JSON null, not Python None)
            field_def["default"] = None  # This becomes JSON null
        else:
            field_def["type"] = avro_base_type
            # For required fields with defaults, use the actual default value
            if has_default:
                field_def["default"] = default_value

        avro_fields.append(field_def)

    # Build Avro schema
    avro_schema = {
        "type": "record",
        "name": model.__name__,
        "namespace": "agentics.types",
        "fields": avro_fields,
    }

    return avro_schema


def register_avro_schema(
    atype: Type[BaseModel],
    schema_registry_url: str = "http://localhost:8081",
    subject: Optional[str] = None,
) -> Optional[int]:
    """
    Register a Pydantic model as an Avro schema in the schema registry.

    Args:
        atype: Pydantic model class
        schema_registry_url: Schema Registry URL
        subject: Subject name (defaults to {atype.__name__}-value)

    Returns:
        Schema ID on success, None on error
    """
    import requests

    if subject is None:
        subject = get_subject_name(atype.__name__, is_key=False, add_suffix=True)

    try:
        # Convert Pydantic model to Avro schema
        avro_schema = pydantic_to_avro_schema(atype)

        # Register in Schema Registry
        schema_data = {"schemaType": "AVRO", "schema": json.dumps(avro_schema)}

        url = f"{schema_registry_url.rstrip('/')}/subjects/{subject}/versions"
        response = requests.post(
            url,
            json=schema_data,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=10,
        )

        if response.status_code in (200, 201):
            schema_id = response.json().get("id")
            sys.stderr.write(
                f"✓ Registered Avro schema '{subject}' (ID: {schema_id})\n"
            )
            sys.stderr.flush()
            return schema_id
        else:
            sys.stderr.write(
                f"✗ Failed to register Avro schema '{subject}': "
                f"{response.status_code} {response.text}\n"
            )
            sys.stderr.flush()
            return None

    except Exception as e:
        sys.stderr.write(f"✗ Error registering Avro schema '{subject}': {e}\n")
        sys.stderr.flush()
        return None


class AGStreamSQL:
    """
    Simplified AGStream for Flink SQL compatibility.

    Key differences from AGStream:
    - Uses Avro format instead of JSON Schema
    - Sends only state data (no envelope) for direct SQL access
    - Optimized for Flink SQL queries
    - Simpler API focused on streaming use cases

    Example:
        >>> from pydantic import BaseModel
        >>>
        >>> class Question(BaseModel):
        ...     text: str
        ...     timestamp: int
        >>>
        >>> # Create stream
        >>> stream = AGStreamSQL(
        ...     atype=Question,
        ...     topic="questions",
        ...     schema_registry_url="http://localhost:8081"
        ... )
        >>>
        >>> # Produce states
        >>> states = [
        ...     Question(text="What is AI?", timestamp=123456),
        ...     Question(text="How does ML work?", timestamp=123457)
        ... ]
        >>> stream.produce(states)
        >>>
        >>> # Query with Flink SQL:
        >>> # CREATE TABLE questions (
        >>> #   text STRING,
        >>> #   timestamp BIGINT
        >>> # ) WITH (
        >>> #   'connector' = 'kafka',
        >>> #   'topic' = 'questions',
        >>> #   'properties.bootstrap.servers' = 'kafka:9092',
        >>> #   'format' = 'avro-confluent',
        >>> #   'avro-confluent.url' = 'http://schema-registry:8081'
        >>> # );
        >>> #
        >>> # SELECT * FROM questions WHERE timestamp > 123456;
    """

    def __init__(
        self,
        atype: Type[BaseModel],
        topic: str,
        kafka_server: str = "localhost:9092",
        schema_registry_url: str = "http://localhost:8081",
        auto_create_topic: bool = True,
        num_partitions: int = 1,
        states: Optional[List[BaseModel]] = None,
    ):
        """
        Initialize AGStreamSQL.

        Args:
            atype: Pydantic model class for the data
            topic: Kafka topic name
            kafka_server: Kafka bootstrap server
            schema_registry_url: Schema Registry URL
            auto_create_topic: Automatically create topic if it doesn't exist
            num_partitions: Number of partitions for new topics
            states: Initial states to produce (optional)
        """
        self.atype = atype
        self.topic = topic
        self.kafka_server = kafka_server
        self.schema_registry_url = schema_registry_url
        self.states = states or []

        # Create topic if needed
        if auto_create_topic and not kafka_topic_exists(topic, kafka_server):
            sys.stderr.write(f"📝 Creating topic '{topic}'...\n")
            create_kafka_topic(topic, kafka_server, num_partitions=num_partitions)

        # Register Avro schema
        subject = get_subject_name(atype.__name__, is_key=False, add_suffix=True)
        if not schema_exists(subject, schema_registry_url):
            sys.stderr.write(f"📝 Registering Avro schema for '{atype.__name__}'...\n")
            register_avro_schema(atype, schema_registry_url)

    def produce(self, states: Optional[List[BaseModel]] = None) -> List[str]:
        """
        Produce states to Kafka using Avro serialization.

        Args:
            states: List of Pydantic model instances (if None, uses self.states)

        Returns:
            List of message IDs
        """
        # Use provided states or self.states
        states_to_produce = states if states is not None else self.states

        if not states_to_produce:
            sys.stderr.write("⚠️  No states to produce\n")
            return []
        try:
            # Use modern confluent_kafka.schema_registry API (not deprecated avro module)
            from confluent_kafka import Producer
            from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroSerializer
            from confluent_kafka.serialization import (
                MessageField,
                SerializationContext,
                StringSerializer,
            )

            message_ids = []

            # Setup Schema Registry client
            schema_registry_conf = {"url": self.schema_registry_url}
            schema_registry_client = SchemaRegistryClient(schema_registry_conf)

            # Convert Pydantic model to Avro schema
            avro_schema = pydantic_to_avro_schema(self.atype)
            avro_schema_str = json.dumps(avro_schema)

            # Create Schema object with explicit AVRO type
            schema_obj = Schema(avro_schema_str, schema_type="AVRO")

            # Create Avro serializer with explicit schema object
            avro_serializer = AvroSerializer(
                schema_registry_client,
                schema_obj,
                lambda obj, ctx: obj,  # obj is already a dict from model_dump()
            )

            # Create string serializer for keys
            string_serializer = StringSerializer("utf_8")

            # Create producer
            producer_conf = {"bootstrap.servers": self.kafka_server}
            producer = Producer(producer_conf)

            sys.stderr.write(
                f"\n📤 Producing {len(states_to_produce)} states to '{self.topic}'...\n"
            )

            for idx, state in enumerate(states_to_produce, 1):
                message_id = str(uuid.uuid4())
                state_dict = state.model_dump()

                # Serialize key and value
                key_bytes = string_serializer(
                    message_id, SerializationContext(self.topic, MessageField.KEY)
                )
                value_bytes = avro_serializer(
                    state_dict, SerializationContext(self.topic, MessageField.VALUE)
                )

                producer.produce(topic=self.topic, key=key_bytes, value=value_bytes)

                message_ids.append(message_id)
                sys.stderr.write(
                    f"  ✓ [{idx}/{len(states_to_produce)}] Sent {self.atype.__name__}\n"
                )

            producer.flush()

            sys.stderr.write(f"\n✅ Successfully produced {len(message_ids)} states\n")
            sys.stderr.flush()

            return message_ids

        except Exception as e:
            sys.stderr.write(f"\n✗ Error producing states: {e}\n")
            sys.stderr.flush()
            raise

    def consume(
        self,
        limit: Optional[int] = None,
        timeout_ms: int = 5000,
        from_beginning: bool = True,
    ) -> List[BaseModel]:
        """
        Consume states from Kafka.

        Args:
            limit: Maximum number of messages to consume (None = all available)
            timeout_ms: Consumer timeout in milliseconds
            from_beginning: Start from beginning or latest offset

        Returns:
            List of Pydantic model instances
        """
        try:
            # Import Avro deserializer
            try:
                from confluent_kafka import avro
                from confluent_kafka.avro import AvroConsumer

                use_confluent = True
            except ImportError:
                import fastavro

                use_confluent = False

            states = []

            if use_confluent:
                # Use Confluent's AvroConsumer
                consumer = AvroConsumer(
                    {
                        "bootstrap.servers": self.kafka_server,
                        "group.id": f"agstreamsql-{uuid.uuid4()}",
                        "schema.registry.url": self.schema_registry_url,
                        "auto.offset.reset": "earliest" if from_beginning else "latest",
                    }
                )

                consumer.subscribe([self.topic])

                sys.stderr.write(f"\n📥 Consuming from '{self.topic}'...\n")

                count = 0
                while limit is None or count < limit:
                    msg = consumer.poll(timeout_ms / 1000.0)

                    if msg is None:
                        break
                    if msg.error():
                        continue

                    state_dict = msg.value()
                    state = self.atype(**state_dict)
                    states.append(state)
                    count += 1

                consumer.close()

            else:
                # Manual Avro deserialization
                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.kafka_server,
                    auto_offset_reset="earliest" if from_beginning else "latest",
                    consumer_timeout_ms=timeout_ms,
                    group_id=f"agstreamsql-{uuid.uuid4()}",
                )

                avro_schema = pydantic_to_avro_schema(self.atype)

                sys.stderr.write(f"\n📥 Consuming from '{self.topic}'...\n")

                count = 0
                for message in consumer:
                    import io

                    bytes_reader = io.BytesIO(message.value)
                    state_dict = fastavro.schemaless_reader(bytes_reader, avro_schema)
                    state = self.atype(**state_dict)
                    states.append(state)
                    count += 1

                    if limit and count >= limit:
                        break

                consumer.close()

            sys.stderr.write(f"✅ Consumed {len(states)} states\n")
            sys.stderr.flush()

            return states

        except Exception as e:
            sys.stderr.write(f"\n✗ Error consuming states: {e}\n")
            sys.stderr.flush()
            raise


# Made with Bob
