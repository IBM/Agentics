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

from .streaming_utils import (
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
        kafka_server: Optional[str] = None,
        schema_registry_url: Optional[str] = None,
        auto_create_topic: bool = True,
        num_partitions: int = 1,
        states: Optional[List[BaseModel]] = None,
        consumer_group: Optional[str] = None,
    ):
        """
        Initialize AGStreamSQL.

        Args:
            atype: Pydantic model class for the data
            topic: Kafka topic name
            kafka_server: Kafka bootstrap server. If None, reads from
                AGSTREAM_BACKENDS_KAFKA_BOOTSTRAP or KAFKA_SERVER environment
                variables (default: "localhost:9092")
            schema_registry_url: Schema Registry URL. If None, reads from
                AGSTREAM_BACKENDS_SCHEMA_REGISTRY_URL environment variable
                (default: "http://localhost:8081")
            auto_create_topic: Automatically create topic if it doesn't exist
            num_partitions: Number of partitions for new topics
            states: Initial states to produce (optional)
            consumer_group: Consumer group ID for tracking offsets (optional)
        """
        import os

        # Get Kafka server from environment if not provided
        if kafka_server is None:
            kafka_server = os.getenv(
                "AGSTREAM_BACKENDS_KAFKA_BOOTSTRAP",
                os.getenv("KAFKA_SERVER", "localhost:9092"),
            )

        # Get Schema Registry URL from environment if not provided
        if schema_registry_url is None:
            schema_registry_url = os.getenv(
                "AGSTREAM_BACKENDS_SCHEMA_REGISTRY_URL", "http://localhost:8081"
            )

        self.atype = atype
        self.topic = topic
        self.kafka_server = kafka_server
        self.schema_registry_url = schema_registry_url
        self.states = states or []
        self.consumer_group = consumer_group

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

            # Create producer with suppressed logging
            producer_conf = {
                "bootstrap.servers": self.kafka_server,
                "log.connection.close": False,  # Suppress connection close logs
                "log_level": 0,  # Suppress all librdkafka logs (0=EMERG, 7=DEBUG)
            }
            producer = Producer(producer_conf)

            for idx, state in enumerate(states_to_produce, 1):
                message_id = str(uuid.uuid4())

                # Handle both BaseModel and TransductionResult objects
                if hasattr(state, "model_dump"):
                    # Pydantic BaseModel
                    state_dict = state.model_dump()
                elif hasattr(state, "value") and hasattr(state.value, "model_dump"):
                    # TransductionResult has a .value attribute that is a BaseModel
                    state_dict = state.value.model_dump()
                elif isinstance(state, dict):
                    state_dict = state
                else:
                    raise TypeError(
                        f"Cannot serialize object of type {type(state)}. Expected BaseModel, TransductionResult, or dict."
                    )

                # Serialize key and value
                key_bytes = string_serializer(
                    message_id, SerializationContext(self.topic, MessageField.KEY)
                )
                value_bytes = avro_serializer(
                    state_dict, SerializationContext(self.topic, MessageField.VALUE)
                )

                producer.produce(topic=self.topic, key=key_bytes, value=value_bytes)
                message_ids.append(message_id)

            producer.flush()

            return message_ids

        except Exception as e:
            sys.stderr.write(f"\n✗ Error producing states: {e}\n")
            sys.stderr.flush()
            raise

    def consume(
        self,
        limit: Optional[int] = None,
        timeout_ms: int = 5000,
        from_beginning: bool = False,
    ) -> List[BaseModel]:
        """
        Consume states from Kafka.

        Args:
            limit: Maximum number of messages to consume (None = all available)
            timeout_ms: Consumer timeout in milliseconds
            from_beginning: Start from beginning (True) or latest offset (False, default)

        Returns:
            List of Pydantic model instances
        """
        try:
            # Use modern confluent_kafka with schema registry
            from confluent_kafka import Consumer
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroDeserializer
            from confluent_kafka.serialization import (
                MessageField,
                SerializationContext,
                StringDeserializer,
            )

            states = []

            # Setup Schema Registry client
            schema_registry_conf = {"url": self.schema_registry_url}
            schema_registry_client = SchemaRegistryClient(schema_registry_conf)

            # Convert Pydantic model to Avro schema
            avro_schema = pydantic_to_avro_schema(self.atype)
            avro_schema_str = json.dumps(avro_schema)

            # Create Avro deserializer for values only
            avro_deserializer = AvroDeserializer(
                schema_registry_client,
                avro_schema_str,
                lambda obj, ctx: obj,  # Return dict as-is
            )

            # Create string deserializer for keys
            string_deserializer = StringDeserializer("utf_8")

            # Create consumer with persistent group ID if provided
            group_id = (
                self.consumer_group
                if self.consumer_group
                else f"agstreamsql-{uuid.uuid4()}"
            )
            consumer_conf = {
                "bootstrap.servers": self.kafka_server,
                "group.id": group_id,
                "auto.offset.reset": "earliest" if from_beginning else "latest",
                "enable.auto.commit": True,  # Commit offsets automatically
                "auto.commit.interval.ms": 1000,  # Commit every second
                "log.connection.close": False,  # Suppress connection close logs
                "log_level": 0,  # Suppress all librdkafka logs (0=EMERG, 7=DEBUG)
            }
            consumer = Consumer(consumer_conf)
            consumer.subscribe([self.topic])

            count = 0
            while limit is None or count < limit:
                msg = consumer.poll(timeout_ms / 1000.0)

                if msg is None:
                    break
                if msg.error():
                    continue

                try:
                    # Deserialize value with Avro, key with string
                    state_dict = avro_deserializer(
                        msg.value(),
                        SerializationContext(self.topic, MessageField.VALUE),
                    )
                    state = self.atype(**state_dict)
                    states.append(state)
                    count += 1
                except Exception as e:
                    # Skip messages that cannot be deserialized (e.g., missing schema ID)
                    # Log the error but continue to next message
                    sys.stderr.write(
                        f"\n⚠️  Skipping message at offset {msg.offset()}: {e}\n"
                    )
                    sys.stderr.flush()
                    # Manually commit this offset to move past the bad message
                    consumer.commit(msg)
                    continue

            consumer.close()

            return states

        except Exception as e:
            sys.stderr.write(f"\n✗ Error consuming states: {e}\n")
            sys.stderr.flush()
            raise

    def listen(
        self,
        transduction_fn: Optional[callable] = None,
        output_stream: Optional["AGStreamSQL"] = None,
        timeout_ms: int = 1000,
        max_iterations: Optional[int] = None,
        verbose: bool = False,
        auto_offset_reset: str = "earliest",
    ):
        """
        Continuously listen to the Kafka topic, consume messages, apply a transduction
        function, and optionally produce results to an output topic.

        This is an Avro-compatible listener that works with Flink SQL topics.

        Args:
            transduction_fn: Function to apply to each consumed message.
                Should accept a Pydantic model instance and return a Pydantic model instance.
                If None, messages are only consumed (no transduction).
            output_stream: AGStreamSQL instance for producing results.
                If None, results are not produced (consume-only mode).
            timeout_ms: Consumer poll timeout in milliseconds (default: 1000).
            max_iterations: Maximum number of consume iterations. None means run forever.
            verbose: If True, print detailed progress (default: False).
            auto_offset_reset: Kafka consumer auto.offset.reset setting.
                "earliest" (default) reads from beginning, "latest" reads only new messages.

        Returns:
            None (runs until interrupted or max_iterations reached)

        Raises:
            KeyboardInterrupt: When user interrupts the listener.

        Example:
            >>> from agentics.core.agstream_sql import AGStreamSQL
            >>> from agentics.core.streaming_utils import get_atype_from_registry
            >>> from agentics.core.transducible_functions import make_transducible_function
            >>> import asyncio
            >>>
            >>> # Load types
            >>> Question = get_atype_from_registry("Question", "http://localhost:8081")
            >>> Answer = get_atype_from_registry("Answer", "http://localhost:8081")
            >>>
            >>> # Create transduction function
            >>> fn = make_transducible_function(
            ...     InputModel=Question,
            ...     OutputModel=Answer,
            ...     instructions="Answer the question concisely."
            ... )
            >>>
            >>> # Create streams
            >>> input_stream = AGStreamSQL(
            ...     atype=Question,
            ...     topic="Q",
            ...     consumer_group="qa-listener"
            ... )
            >>> output_stream = AGStreamSQL(
            ...     atype=Answer,
            ...     topic="A"
            ... )
            >>>
            >>> # Start listening
            >>> def transduce(msg):
            ...     return asyncio.run(fn(msg)).value
            >>>
            >>> input_stream.listen(
            ...     transduction_fn=transduce,
            ...     output_stream=output_stream,
            ...     verbose=True
            ... )
        """
        import asyncio
        import time

        if verbose:
            print(f"🎧 Starting AGStreamSQL listener...")
            print(f"   Input topic: {self.topic}")
            if output_stream:
                print(f"   Output topic: {output_stream.topic}")
            print(f"   Consumer group: {self.consumer_group or 'auto-generated'}")
            print(f"   Auto offset reset: {auto_offset_reset}")
            print(f"   Press Ctrl+C to stop\n")

        iteration = 0
        messages_processed = 0

        try:
            while True:
                iteration += 1

                if max_iterations and iteration > max_iterations:
                    if verbose:
                        print(f"\n✓ Reached max iterations ({max_iterations})")
                    break

                try:
                    # Consume messages with from_beginning=True to process existing messages
                    from_beginning = auto_offset_reset == "earliest"
                    messages = self.consume(
                        limit=1, timeout_ms=timeout_ms, from_beginning=from_beginning
                    )

                    if messages:
                        for msg in messages:
                            if verbose:
                                print(f"\n📥 INPUT [{self.atype.__name__}]:")
                                if hasattr(msg, "dict"):
                                    for key, value in msg.dict().items():
                                        print(f"  {key}: {value}")
                                else:
                                    print(f"  {msg}")

                            # Apply transduction if function provided
                            if transduction_fn:
                                try:
                                    result = transduction_fn(msg)

                                    # Handle TransductionResult wrapper
                                    from agentics.core.transducible_functions import (
                                        TransductionResult,
                                    )

                                    if isinstance(result, TransductionResult):
                                        actual_result = result.value
                                        explanation = result.explanation
                                    else:
                                        actual_result = result
                                        explanation = None

                                    if verbose:
                                        if output_stream:
                                            print(
                                                f"\n📤 OUTPUT [{output_stream.atype.__name__}]:"
                                            )
                                        else:
                                            print(f"\n📤 RESULT:")
                                        if hasattr(actual_result, "dict"):
                                            for (
                                                key,
                                                value,
                                            ) in actual_result.dict().items():
                                                print(f"  {key}: {value}")
                                        else:
                                            print(f"  {actual_result}")
                                        if explanation:
                                            print(f"\n💡 Explanation: {explanation}")

                                    # Produce to output topic if stream provided
                                    if output_stream:
                                        output_stream.produce([actual_result])

                                except Exception as e:
                                    if verbose:
                                        print(f"\n⚠️  Error in transduction: {e}")
                                    # Continue processing next message

                            messages_processed += 1

                            if verbose:
                                print(f"\n✓ Processed message {messages_processed}")
                                print("=" * 60)
                    else:
                        if verbose and iteration % 10 == 0:
                            print(f"  Waiting for messages... (iteration {iteration})")

                except Exception as e:
                    if verbose:
                        print(f"\n⚠️  Error in iteration {iteration}: {e}")
                    # Continue to next iteration - this handles schema errors gracefully
                    time.sleep(0.5)

                # Small delay between iterations
                time.sleep(0.1)

        except KeyboardInterrupt:
            if verbose:
                print(f"\n\n⚠️  Listener stopped by user")
                print(f"   Total messages processed: {messages_processed}")
                print(f"   Total iterations: {iteration}")

        if verbose:
            print("\n✓ Listener stopped")


# Made with Bob
