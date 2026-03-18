from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Type

from kafka import KafkaProducer
from pydantic import BaseModel, Field, ValidationError

# Suppress Kafka consumer error logs and IPv6 connection errors
logging.getLogger("kafka.consumer.fetcher").setLevel(logging.CRITICAL)
logging.getLogger("kafka.conn").setLevel(
    logging.CRITICAL
)  # Suppress IPv6 ECONNREFUSED errors
logging.getLogger("kafka").setLevel(logging.WARNING)

# PyFlink imports for the listener
from pyflink.datastream.functions import MapFunction

from agentics.core.atype import (
    make_all_fields_optional,
    pydantic_model_from_dict,
)
from agentics.core.default_types import Explanation
from agentics.core.utils import import_pydantic_from_code

from ..agentics import AG
from .streaming_utils import (
    create_kafka_topic,
    get_atype_from_registry,
    get_subject_name,
    get_topic_partition_count,
    increase_topic_partitions,
    kafka_topic_exists,
    register_atype_schema,
    schema_exists,
)

# Module-level registry: maps job_name -> threading.Event
# ProcessTransducibleFn.map() sets the event on each processed message so that
# the main thread can detect idle and stop the Flink daemon thread.
_ACTIVITY_REGISTRY: Dict[str, threading.Event] = {}

# Module-level registry: maps job_name -> TransducibleFunction
# Avoids pickling Pydantic model classes (which fail to unpickle from __main__)
# by storing the live fn object and looking it up by job_name in map().
_FN_REGISTRY: Dict[str, Any] = {}

# Module-level registry: maps job_name -> message counter (for stop_after_lookback)
_MESSAGE_COUNTER_REGISTRY: Dict[str, Dict[str, int]] = {}

# Module-level registry: maps job_name -> list of filter keys
_FILTER_KEYS_REGISTRY: Dict[str, Optional[List[str]]] = {}

# Module-level registry: maps job_name -> dict mapping partition-offset to actual key
_KEY_MAP_REGISTRY: Dict[str, Dict[str, str]] = {}

# Module-level registry: maps job_name -> list of (timestamp, state) tuples for ordering
_PRODUCED_STATES_REGISTRY: Dict[str, List[tuple]] = {}

# Lock for thread-safe access to produced states registry
import threading

_PRODUCED_STATES_LOCK = threading.Lock()

# Module-level registry: maps job_name -> completion flag
_COMPLETION_REGISTRY: Dict[str, bool] = {}


# Helper class for writing to Kafka output
# Serializer functions defined at module level to be picklable
def _key_serializer(k):
    """Serialize Kafka key to bytes"""
    return k.encode("utf-8") if k else b""


def _value_serializer(v):
    """Serialize Kafka value to bytes"""
    return v.encode("utf-8") if v else b"{}"


def _normalize_bootstrap_server(server: str) -> str:
    """
    Normalize bootstrap server address to avoid IPv6 connection issues.

    Replaces 'localhost' with '127.0.0.1' to force IPv4 connections,
    preventing ECONNREFUSED errors from failed IPv6 connection attempts.

    Args:
        server: Bootstrap server address

    Returns:
        Normalized bootstrap server address
    """
    if "localhost" in server:
        return server.replace("localhost", "127.0.0.1")
    return server


class WriteToKafkaOutput(MapFunction):
    """Write processed records back to Kafka output topic"""

    def __init__(self, kafka_server: str, output_topic: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kafka_server = kafka_server
        self.output_topic = output_topic

    def map(self, record) -> str:
        """Write a processed record to Kafka output topic"""
        import sys

        if record is None:
            return "✗ Skipped None record"

        key = None
        timestamp = None
        serialized_agentics = None

        try:
            # Unpack the record tuple
            key, timestamp, serialized_agentics = record

            # Validate that we have all required fields
            if key is None or serialized_agentics is None:
                sys.stderr.write(
                    f"Invalid record: key={key}, timestamp={timestamp}, value={'None' if serialized_agentics is None else 'present'}\n"
                )
                sys.stderr.flush()
                return f"✗ Skipped invalid record | Key: {key}"

            # Ensure key and value are strings
            key_str = str(key) if key is not None else "unknown"
            value_str = (
                str(serialized_agentics) if serialized_agentics is not None else "{}"
            )

            # Create Kafka producer (created fresh for each message to avoid pickle issues)
            from kafka import KafkaProducer

            producer = KafkaProducer(
                bootstrap_servers=_normalize_bootstrap_server(self.kafka_server),
                key_serializer=_key_serializer,
                value_serializer=_value_serializer,
            )

            # Send to output topic
            producer.send(
                self.output_topic,
                key=key_str,
                value=value_str,
                timestamp_ms=timestamp if timestamp else int(time.time() * 1000),
            )
            producer.flush()
            producer.close()

            # Return formatted string for logging
            return f"✓ Sent to {self.output_topic} | Key: {key_str} | Timestamp: {timestamp}"

        except Exception as e:
            sys.stderr.write(f"Error writing to Kafka: {e}\n")
            sys.stderr.write(
                f"Record details: key={key}, timestamp={timestamp}, value={'None' if serialized_agentics is None else 'present'}\n"
            )
            sys.stderr.flush()
            return (
                f"✗ Failed to write | Key: {key if key else 'None'} | Error: {str(e)}"
            )


# Helper class for processing SQL rows
class ProcessSQLRow(MapFunction):
    """Process SQL query results (key, timestamp, value) and perform transduction"""

    def __init__(
        self,
        TargetAType: Type[BaseModel],
        SourceAType: Optional[Type[BaseModel]] = None,
        schema_registry_url: str = "http://localhost:8081",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.aType = TargetAType
        self.sourceAtype = SourceAType
        self.schema_registry_url = schema_registry_url

    def map(self, row) -> tuple | None:
        """Process a row from the SQL query result (key, timestamp, value)"""
        import logging
        import sys

        try:
            key = str(row[0]) if row[0] else "no-key"
            timestamp = int(row[1]) if row[1] else int(time.time() * 1000)
            value = str(row[2]) if row[2] else "{}"

            # Perform transduction
            target_ag = AGStream(atype=self.aType)
            source_json = json.loads(value)
            source = AGStream.deserialize(source_json, atype=self.sourceAtype)
            target_ag.get_instructions_from_source(source)

            # Log input AG with actual content
            sys.stdout.write(f"\n{'='*60}\n")
            sys.stdout.write(f"📥 INPUT AG [{source.atype.__name__}]:\n")
            if hasattr(source, "states") and source.states:
                # Access Pydantic model fields using model_dump()
                state_dict = source.states[0].model_dump()
                for field_name, field_value in state_dict.items():
                    sys.stdout.write(f"  {field_name}: {field_value}\n")
            else:
                sys.stdout.write(f"  {source.pretty_print()}\n")
            sys.stdout.flush()

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(target_ag.__lshift__(source))
            loop.close()

            # Log output AG with actual content
            sys.stdout.write(f"\n📤 OUTPUT AG [{result.atype.__name__}]:\n")
            if hasattr(result, "states") and result.states:
                # Access Pydantic model fields using model_dump()
                state_dict = result.states[0].model_dump()
                for field_name, field_value in state_dict.items():
                    sys.stdout.write(f"  {field_name}: {field_value}\n")
            else:
                sys.stdout.write(f"  {result.pretty_print()}\n")
            sys.stdout.write(f"{'='*60}\n\n")
            sys.stdout.flush()

            # Serialize the result
            serialized_agentics = json.dumps(result.serialize())

            # Return tuple of (key, timestamp, serialized_agentics)
            # Use the original key from the SQL query (partition-offset based)
            return (key, timestamp, serialized_agentics)
        except Exception as e:
            sys.stderr.write(f"Error processing SQL row: {e}\n")
            sys.stderr.flush()
            return None


class ProcessTransducibleFn(MapFunction):
    """
    Flink MapFunction that applies a transducible function to each incoming
    Kafka message.

    The live ``TransducibleFunction`` object is stored in the module-level
    ``_FN_REGISTRY`` dict (keyed by ``job_name``) so that it is accessible
    from all Flink task-slot threads **without pickling**.  Only plain
    strings and primitive values are stored as instance attributes, which
    are safely picklable.

    This avoids the ``typing.Any`` / ``NameError`` failures that occur when
    Pydantic model classes defined in a Jupyter notebook (``__main__``) are
    pickled and unpickled by PyFlink's serialisation layer.
    """

    def __init__(
        self,
        job_name: str,
        kafka_server: str,
        output_topic: str,
        schema_registry_url: str = "http://localhost:8081",
        validate_schema: bool = True,
        produce_results: bool = True,
        target_atype_name: Optional[str] = None,
        num_partitions: int = 3,
        target_message_count: Optional[int] = None,
    ):
        super().__init__()
        # Only picklable primitives stored here.
        # The live fn object lives in _FN_REGISTRY[job_name].
        self.job_name = job_name
        self.kafka_server = kafka_server
        self.output_topic = output_topic
        self.schema_registry_url = schema_registry_url
        self.validate_schema = validate_schema
        self.produce_results = produce_results
        self.target_atype_name = target_atype_name
        self.num_partitions = num_partitions
        self.target_message_count = target_message_count

    def map(self, row) -> str:
        """
        Process one row (key, timestamp_ms, value_json_str) from the Flink
        DataStream.  Looks up the live TransducibleFunction from
        ``_FN_REGISTRY``, runs the async coroutine in a brand-new event
        loop, and optionally produces the result to the output Kafka topic.
        """
        import asyncio
        import sys

        from pydantic import ValidationError

        from agentics.core.transducible_functions import TransductionResult

        # Signal activity so the idle-timeout watcher in the main thread knows
        # a message arrived.
        if self.job_name in _ACTIVITY_REGISTRY:
            _ACTIVITY_REGISTRY[self.job_name].set()

        try:
            source_key = str(row[0]) if row[0] else None
            timestamp = int(row[1]) if row[1] else int(time.time() * 1000)

            # ── Deserialise the incoming state ─────────────────────────────
            value_obj = row[2]

            if value_obj is None:
                raw = {}
            elif isinstance(value_obj, bytes):
                # Binary data (Avro format)
                try:
                    from agentics.core.streaming.avro_utils import avro_deserialize

                    # Deserialize using avro_utils
                    raw, schema_id = avro_deserialize(
                        value_obj, self.schema_registry_url, schema_cache={}
                    )

                    sys.stderr.write(
                        f"  ✓ Deserialized Avro message (schema_id={schema_id}) for key={source_key}\n"
                    )
                    sys.stderr.flush()

                except Exception as avro_err:
                    sys.stderr.write(
                        f"  ❌ Avro decode error for key={source_key}: {avro_err}\n"
                        f"     Raw value: {repr(value_obj[:50])}\n"
                    )
                    sys.stderr.flush()
                    return f"✗ Avro decode error for key={source_key}: {avro_err}"
            elif isinstance(value_obj, str):
                # Check if it's Avro format (starts with magic byte 0x00)
                if value_obj and ord(value_obj[0]) == 0:
                    # Avro format with Schema Registry wire format
                    # Flink read binary as string, need to convert back carefully
                    try:
                        from agentics.core.streaming.avro_utils import avro_deserialize

                        # Convert string to bytes using ISO-8859-1 which preserves byte values 0-255
                        # This is safer than latin-1 for binary data that was read as string
                        try:
                            value_bytes = value_obj.encode("iso-8859-1")
                        except UnicodeEncodeError:
                            # If encoding fails, the string contains characters outside 0-255 range
                            # This means Flink corrupted the binary data when reading as string
                            sys.stderr.write(
                                f"  ❌ Binary data corrupted for key={source_key} (contains invalid characters)\n"
                                f"     Raw value: {repr(value_obj[:50])}\n"
                            )
                            sys.stderr.flush()
                            return f"✗ Binary data corrupted for key={source_key}"

                        # Deserialize using avro_utils
                        raw, schema_id = avro_deserialize(
                            value_bytes, self.schema_registry_url, schema_cache={}
                        )

                        sys.stderr.write(
                            f"  ✓ Deserialized Avro message (schema_id={schema_id}) for key={source_key}\n"
                        )
                        sys.stderr.flush()

                    except Exception as avro_err:
                        sys.stderr.write(
                            f"  ❌ Avro decode error for key={source_key}: {avro_err}\n"
                            f"     Raw value: {repr(value_obj[:50])}\n"
                        )
                        sys.stderr.flush()
                        return f"✗ Avro decode error for key={source_key}: {avro_err}"
                else:
                    # JSON format (legacy)
                    try:
                        raw = json.loads(value_obj) if value_obj.strip() else {}
                    except json.JSONDecodeError as json_err:
                        sys.stderr.write(
                            f"  ❌ JSON decode error for key={source_key}: {json_err}\n"
                            f"     Raw value: {repr(value_obj[:200])}\n"
                        )
                        sys.stderr.flush()
                        return f"✗ JSON decode error for key={source_key}: {json_err}"
            else:
                # Flink Row object (when using Avro format connector)
                try:
                    # Flink Row objects have field names and can be converted to dict
                    if hasattr(value_obj, "as_dict"):
                        raw = value_obj.as_dict()
                    elif hasattr(value_obj, "__dict__"):
                        raw = value_obj.__dict__
                    else:
                        # Try to iterate over fields
                        raw = dict(value_obj)
                except Exception as conv_err:
                    sys.stderr.write(
                        f"  ❌ Error converting Row to dict for key={source_key}: {conv_err}\n"
                        f"     Row type: {type(value_obj)}\n"
                    )
                    sys.stderr.flush()
                    return f"✗ Row conversion error for key={source_key}: {conv_err}"

            # Try to get the actual Kafka key from the key map registry
            # The source_key is "partition-X-offset-Y", we need to map it to the actual UUID
            key_map = _KEY_MAP_REGISTRY.get(self.job_name)
            if key_map and source_key in key_map:
                actual_key = key_map[source_key]
            else:
                # Fallback: try to get from streaming_key field (though it's usually None)
                actual_key = raw.get("streaming_key") if isinstance(raw, dict) else None
                if not actual_key:
                    actual_key = source_key

            # ------------------------------------------
            # Filter by keys if specified
            # ------------------------------------------
            filter_keys = _FILTER_KEYS_REGISTRY.get(self.job_name)
            if filter_keys is not None and actual_key not in filter_keys:
                return None  # Silently skip filtered messages

            # ── Look up the live fn from the module-level registry ─────────
            fn = _FN_REGISTRY.get(self.job_name)
            if fn is None:
                sys.stderr.write(
                    f"  ❌ fn not found in _FN_REGISTRY for job '{self.job_name}'\n"
                )
                sys.stderr.flush()
                return f"✗ fn not found for job={self.job_name}"

            input_model = fn.input_model
            target_model = fn.target_model

            states_list = (
                raw["states"] if isinstance(raw, dict) and raw.get("states") else [raw]
            )

            results = []
            for state_dict in states_list:
                try:
                    try:
                        state_obj = input_model(**state_dict)
                    except (ValidationError, TypeError) as ve:
                        sys.stderr.write(f"  ❌ Schema validation failed: {ve}\n")
                        sys.stderr.flush()
                        if self.validate_schema:
                            continue
                        state_obj = input_model.model_construct(**state_dict)

                    # ── Log the source (input) state ────────────────────────
                    sys.stderr.write(f"\n📥 INPUT STATE [{input_model.__name__}]:\n")
                    sys.stderr.write(f"  Key: {actual_key}\n")
                    state_dump = state_obj.model_dump()
                    for field_name, field_value in state_dump.items():
                        sys.stderr.write(f"  {field_name}: {field_value}\n")
                    sys.stderr.write(f"{'='*60}\n\n")
                    sys.stderr.flush()

                    # ── Run the async transducible function ────────────────
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        result = loop.run_until_complete(fn(state_obj))
                    finally:
                        loop.close()

                    # Unwrap TransductionResult if needed
                    if isinstance(result, TransductionResult):
                        result = result.value

                    if result is None or not isinstance(result, target_model):
                        sys.stderr.write(
                            f"  ⚠️  Unexpected result type {type(result).__name__}; skipping.\n"
                        )
                        sys.stderr.flush()
                        continue

                    results.append(result)

                except Exception as state_err:
                    sys.stderr.write(f"  ❌ Error processing state: {state_err}\n")
                    sys.stderr.flush()

            if not results:
                return f"✗ No valid results for key={actual_key}"

            # ── Log the generated (output) states ────────────────────────
            sys.stderr.write(f"\n📤 OUTPUT STATE(S) [{target_model.__name__}]:\n")
            for idx, result in enumerate(results, 1):
                sys.stderr.write(f"  [{idx}/{len(results)}]\n")
                result_dump = result.model_dump()
                for field_name, field_value in result_dump.items():
                    sys.stderr.write(f"    {field_name}: {field_value}\n")
            sys.stderr.write(f"{'='*60}\n\n")
            sys.stderr.flush()

            # ------------------------------------------
            # Increment message counter if tracking
            # ------------------------------------------
            if self.target_message_count is not None:
                if self.job_name not in _MESSAGE_COUNTER_REGISTRY:
                    _MESSAGE_COUNTER_REGISTRY[self.job_name] = {
                        "count": 0,
                        "target": self.target_message_count,
                    }

                _MESSAGE_COUNTER_REGISTRY[self.job_name]["count"] += 1
                current_count = _MESSAGE_COUNTER_REGISTRY[self.job_name]["count"]

                sys.stderr.write(
                    f"  📊 Message counter: {current_count}/{self.target_message_count}\n"
                )
                sys.stderr.flush()

                if current_count >= self.target_message_count:
                    sys.stderr.write(
                        f"\n✅ Reached target message count ({current_count}/{self.target_message_count}). "
                        f"Signaling completion...\n"
                    )
                    sys.stderr.flush()
                    # Signal completion by setting the completion flag
                    _COMPLETION_REGISTRY[self.job_name] = True
                    sys.stderr.write(
                        f"  🚩 Completion flag set for job: {self.job_name}\n"
                    )
                    sys.stderr.flush()
                    # Also clear the activity event to trigger idle timeout
                    if self.job_name in _ACTIVITY_REGISTRY:
                        _ACTIVITY_REGISTRY[self.job_name].clear()

            # ── Store results in registry for collection (with timestamp for ordering) ────────────────────
            with _PRODUCED_STATES_LOCK:
                if self.job_name not in _PRODUCED_STATES_REGISTRY:
                    _PRODUCED_STATES_REGISTRY[self.job_name] = []
                # Store each result with its timestamp for proper ordering
                for result in results:
                    _PRODUCED_STATES_REGISTRY[self.job_name].append((timestamp, result))

            # ── Produce results to the output topic ────────────────────────
            if self.produce_results:
                output_ag = AGStream(
                    atype=target_model,
                    kafka_server=self.kafka_server,
                    input_topic=self.output_topic,
                    output_topic=self.output_topic,
                    schema_registry_url=self.schema_registry_url,
                    target_atype_name=self.target_atype_name,
                )
                output_ag.states = results
                try:
                    msg_ids = output_ag.produce(
                        register_if_missing=True,
                        key=actual_key,
                        num_partitions=self.num_partitions,
                    )
                    return (
                        f"✓ Produced {len(results)} state(s) to '{self.output_topic}' "
                        f"| Key: {actual_key} | IDs: {[m[:8] for m in (msg_ids or [])]}"
                    )
                except Exception as prod_err:
                    sys.stderr.write(f"  ❌ Failed to produce result: {prod_err}\n")
                    sys.stderr.flush()
                    return f"✗ Produce failed for key={actual_key}: {prod_err}"

            return f"✓ Processed {len(results)} state(s) for key={actual_key} (produce_results=False)"

        except Exception as e:
            sys.stderr.write(f"Error in ProcessTransducibleFn.map: {e}\n")
            sys.stderr.flush()
            return f"✗ Error: {e}"


class AGStream(AG):
    model_config = {"arbitrary_types_allowed": True}
    streaming_key: Optional[str] = Field(
        None,
        description="Kafka streaming key, to be used when the object is sent to streaming transduction",
    )
    kafka_server: str = Field(
        default_factory=lambda: os.getenv(
            "AGSTREAM_BACKENDS_KAFKA_BOOTSTRAP",
            os.getenv("KAFKA_SERVER", "localhost:9092"),
        ),
        description="Kafka bootstrap server. Reads from AGSTREAM_BACKENDS_KAFKA_BOOTSTRAP or KAFKA_SERVER environment variables if not provided.",
    )
    input_topic: str = "agentics-stream"
    output_topic: str = "agentics-output"
    schema_registry_url: str = Field(
        default_factory=lambda: os.getenv(
            "AGSTREAM_BACKENDS_SCHEMA_REGISTRY_URL", "http://localhost:8081"
        ),
        description="Schema Registry URL. Reads from AGSTREAM_BACKENDS_SCHEMA_REGISTRY_URL environment variable if not provided.",
    )
    target_atype_name: Optional[str] = Field(
        None,
        description="Name of the target atype as registered in the schema registry. "
        "When set, this name is used as the subject name for schema registry "
        "lookups instead of deriving it from the atype class name.",
    )
    source_atype_name: Optional[str] = Field(
        None,
        description="Name of the source atype as registered in the schema registry. "
        "When set, this name is used as the subject name for source schema "
        "registry lookups instead of deriving it from the atype class name.",
    )

    def _get_subject_name(self, topic: str, is_key: bool = False) -> str:
        """
        Generate subject name for schema registry based on atype class name.

        Prefers ``source_atype_name`` when set, otherwise uses
        ``self.atype.__name__``, falling back to ``topic``.
        Delegates suffix logic to :func:`streaming_utils.get_subject_name`.
        """
        if self.source_atype_name:
            type_name = self.source_atype_name
        elif self.atype:
            type_name = self.atype.__name__
        else:
            type_name = topic
        return get_subject_name(type_name, is_key)

    def _get_target_subject_name(self, is_key: bool = False) -> str:
        """
        Generate subject name for the **target** atype in the schema registry.

        Uses ``target_atype_name`` when explicitly set, otherwise falls back to
        ``self.atype.__name__``.
        """
        if self.target_atype_name:
            type_name = self.target_atype_name
        elif self.atype:
            type_name = self.atype.__name__
        else:
            type_name = "unknown"
        return get_subject_name(type_name, is_key)

    def produce(
        self,
        register_if_missing: bool = True,
        compatibility_mode: str = "BACKWARD",
        key: Optional[str] = None,
        num_partitions: Optional[int] = None,
    ) -> List[str]:
        """
        Produce all states in self.states to Kafka one-by-one with schema registry enforcement.

        This method iterates through self.states and validates each state against the
        registered schema before sending. If the schema doesn't exist and register_if_missing
        is True, it will register it.

        Args:
            register_if_missing: If True, register schema if it doesn't exist
            compatibility_mode: Schema compatibility mode (BACKWARD, FORWARD, FULL, NONE)
            key: Optional Kafka message key to use for all produced messages. When provided
                (e.g. the key of the source message that triggered this produce), the same
                key is reused so that consumers can correlate input and output messages.
                If None, a fresh UUID is generated for each message.
            num_partitions: Number of partitions to create if the topic doesn't exist.
                If None, defaults to 3. For optimal parallelism, set this equal to the
                parallelism parameter used in listener methods.

        Returns:
            List of message IDs for successfully sent states

        Raises:
            ValueError: If any state doesn't match registered schema or wrong type

        Example:
            >>> from pydantic import BaseModel
            >>> from agentics.core.streaming import AGStream
            >>>
            >>> class Question(BaseModel):
            >>>     text: str
            >>>     category: str
            >>>
            >>> ag = AGStream(atype=Question, input_topic="questions")
            >>> ag.states = [
            >>>     Question(text="What is AI?", category="technology"),
            >>>     Question(text="What is ML?", category="technology")
            >>> ]
            >>> msg_ids = ag.produce()
            >>> print(f"Sent {len(msg_ids)} messages")
        """
        if not self.states:
            sys.stderr.write("⚠️  No states to produce\n")
            sys.stderr.flush()
            return []

        try:
            import uuid

            from pydantic import ValidationError

            # When target_atype_name is set the output schema subject is derived
            # from the target name, not from self.atype (which may be a placeholder).
            # _get_target_subject_name() returns "<target_atype_name>-value" when
            # target_atype_name is set, otherwise falls back to the normal subject.
            if self.target_atype_name:
                subject = self._get_target_subject_name(is_key=False)
                effective_type_name = self.target_atype_name
            else:
                subject = self._get_subject_name(self.input_topic, is_key=False)
                effective_type_name = self.atype.__name__

            subject_exists = schema_exists(subject, self.schema_registry_url)

            # Register schema if it doesn't exist and registration is enabled
            if not subject_exists and register_if_missing:
                sys.stderr.write(
                    f"📝 Schema not found, registering {effective_type_name}...\n"
                )
                sys.stderr.flush()
                schema_id = register_atype_schema(
                    atype=self.atype,
                    schema_registry_url=self.schema_registry_url,
                    topic=self.input_topic,
                    is_key=False,
                    compatibility=compatibility_mode,
                )
                if not schema_id:
                    raise ValueError(
                        f"Failed to register schema for {effective_type_name}"
                    )
            elif not subject_exists:
                raise ValueError(
                    f"Schema not registered for topic '{self.input_topic}'. "
                    f"Set register_if_missing=True to auto-register."
                )

            # Ensure the output topic exists with correct partition count
            if not kafka_topic_exists(self.output_topic, self.kafka_server):
                # Topic doesn't exist - create it
                partitions = num_partitions if num_partitions is not None else 3
                sys.stderr.write(
                    f"📝 Topic '{self.output_topic}' not found, creating with {partitions} partitions...\n"
                )
                sys.stderr.flush()
                topic_created = create_kafka_topic(
                    topic_name=self.output_topic,
                    kafka_server=self.kafka_server,
                    num_partitions=partitions,
                    replication_factor=1,
                )
                if not topic_created:
                    raise ValueError(f"Failed to create topic '{self.output_topic}'")
            elif num_partitions is not None:
                # Topic exists - check if we need to increase partitions
                current_partitions = get_topic_partition_count(
                    self.output_topic, self.kafka_server
                )
                if current_partitions > 0 and num_partitions > current_partitions:
                    sys.stderr.write(
                        f"⚠️  Topic '{self.output_topic}' has {current_partitions} partitions, "
                        f"increasing to {num_partitions} for optimal parallelism...\n"
                    )
                    sys.stderr.flush()
                    increased = increase_topic_partitions(
                        self.output_topic, num_partitions, self.kafka_server
                    )
                    if not increased:
                        sys.stderr.write(
                            f"⚠️  Warning: Could not increase partitions. "
                            f"Parallelism will be limited to {current_partitions} workers.\n"
                        )
                        sys.stderr.flush()
                elif current_partitions > 0 and num_partitions < current_partitions:
                    sys.stderr.write(
                        f"ℹ️  Topic '{self.output_topic}' has {current_partitions} partitions "
                        f"(requested {num_partitions}). Cannot decrease partitions in Kafka.\n"
                    )
                    sys.stderr.flush()

            # Create producer once for all states
            producer: KafkaProducer = KafkaProducer(
                bootstrap_servers=_normalize_bootstrap_server(self.kafka_server),
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )

            message_ids = []

            sys.stderr.write(
                f"\n📤 Producing {len(self.states)} states with schema enforcement...\n"
            )
            sys.stderr.flush()

            # Unwrap any TransductionResult objects in self.states so callers can
            # pass the raw output of a transducible function directly without
            # manually extracting the .value field.  This is a framework-level fix
            # that makes produce() robust to the common pattern:
            #   stream.states = await my_transducible_fn(inputs)
            from agentics.core.transducible_functions import TransductionResult

            self.states = [
                s.value if isinstance(s, TransductionResult) else s for s in self.states
            ]

            # Iterate through each state and produce individually
            for idx, state in enumerate(self.states, 1):
                try:
                    # Validate that state matches the effective output type.
                    # When target_atype_name is set, self.atype may be a placeholder
                    # class that differs from the actual state class (which was
                    # produced by the transducer using the registry-fetched type).
                    # In that case we accept any state whose class name matches
                    # target_atype_name; otherwise we fall back to a strict
                    # isinstance check.
                    if self.target_atype_name:
                        state_class_name = type(state).__name__
                        if state_class_name != self.target_atype_name:
                            raise ValueError(
                                f"State {idx} class name '{state_class_name}' does not match "
                                f"target_atype_name '{self.target_atype_name}'"
                            )
                    elif not isinstance(state, self.atype):
                        raise ValueError(
                            f"State {idx} must be an instance of {self.atype.__name__}, "
                            f"got {type(state).__name__}"
                        )

                    # Validate state against schema
                    try:
                        state_dict = state.model_dump()
                        # Ensure it can be serialized
                        json.dumps(state_dict)
                    except (ValidationError, TypeError, ValueError) as e:
                        raise ValueError(f"State {idx} validation failed: {e}")

                    # Use provided key if available, otherwise generate new UUID
                    # When key is provided (e.g., from source message), preserve it for correlation
                    message_id = str(uuid.uuid4())
                    kafka_key = key if key is not None else message_id
                    timestamp_ms = int(time.time() * 1000)

                    # Clone self so all config (instructions, transduction_type,
                    # transduce_fields, streaming_key, etc.) is preserved in the
                    # serialized envelope alongside the single state.
                    temp_ag = AGStream.clone(self)
                    temp_ag.states = [state]
                    serialized = temp_ag.serialize()

                    # Prepare headers for traceability
                    headers = []
                    if key is not None:
                        # When using source key, store message_id in headers for tracking
                        headers.append(("message_id", message_id.encode("utf-8")))
                    else:
                        # When generating new key, store it in headers too
                        headers.append(("message_id", kafka_key.encode("utf-8")))

                    # Send to Kafka with preserved or generated key
                    producer.send(
                        topic=self.output_topic,
                        key=kafka_key,  # Use source key if provided, otherwise UUID
                        value=serialized,
                        timestamp_ms=timestamp_ms,
                        headers=headers if headers else None,
                    )

                    message_ids.append(message_id)

                    sys.stderr.write(
                        f"  ✓ [{idx}/{len(self.states)}] Sent {self.atype.__name__} "
                        f"(ID: {message_id[:8]}...)\n"
                        f"to topic: {self.output_topic}\n"
                    )
                    sys.stderr.flush()

                except ValueError as e:
                    sys.stderr.write(f"  ✗ [{idx}/{len(self.states)}] Failed: {e}\n")
                    sys.stderr.flush()
                    producer.close()
                    raise

            producer.flush()
            producer.close()

            sys.stderr.write(
                f"\n✅ Successfully produced {len(message_ids)}/{len(self.states)} states\n"
            )
            sys.stderr.flush()

            return message_ids

        except ValueError as e:
            sys.stderr.write(f"\n✗ Schema enforcement failed: {e}\n")
            sys.stderr.flush()
            raise
        except Exception as e:
            sys.stderr.write(f"\n✗ Error producing with schema enforcement: {e}\n")
            sys.stderr.flush()
            return []

    async def aproduce_and_collect(
        self,
        source_atype_name: Optional[str] = None,
        result_atype: Optional[Type[BaseModel]] = None,
        register_if_missing: bool = True,
        compatibility_mode: str = "BACKWARD",
        timeout: float = 120.0,
        poll_interval: float = 0.5,
        validate_schema: bool = True,
        verbose: bool = False,
        listener_manager: Optional[Any] = None,
    ) -> List[Optional["AGStream"]]:
        """
        Produce all states to Kafka and asynchronously await their transduced results
        from the output topic, returning them in the same order as the input states.

        This method is the async "fire-and-collect" counterpart to the blocking
        ``listen()`` loop.  It is designed to be used when a ``listen()`` worker is
        already running on the output topic (or will be started externally):

        1. Produces every state in ``self.states`` to ``self.input_topic``, each
           tagged with a unique UUID key.
        2. Starts a background thread that polls ``self.output_topic`` and collects
           any message whose Kafka key matches one of the produced UUIDs.
        3. Awaits (non-blocking) until every key has been collected or ``timeout``
           seconds have elapsed.
        4. Returns the results in the **same order** as the original ``self.states``
           list, using the key→result mapping to reconstruct the order.

        Args:
            source_atype_name: Optional name of the source type in the schema
                registry.  Passed through to the listener for schema validation.
                Not used directly by this method but stored for documentation
                consistency with ``listen()``.
            result_atype: Optional Pydantic model class for the **output** (target)
                type.  When provided, collected messages from ``output_topic`` are
                deserialized into this type instead of ``self.atype`` (which is the
                source/input type).  Use this when the output topic carries a
                different schema than the input topic (the common case in
                transduction pipelines).
            register_if_missing: If ``True``, auto-register the source schema
                before producing (default: ``True``).
            compatibility_mode: Schema compatibility mode for auto-registration
                (default: ``"BACKWARD"``).
            timeout: Maximum seconds to wait for all results (default: 120).
            poll_interval: Seconds between output-topic poll cycles (default: 0.5).
            validate_schema: If ``True``, validate collected results against the
                target schema (default: ``True``).
            verbose: If ``True``, print per-message progress to stderr.
            listener_manager: Optional :class:`~agentics.core.listener_manager.ListenerManager`
                instance.  When provided, a pre-flight check verifies that at
                least one alive listener is consuming ``self.input_topic`` before
                any messages are produced.  Raises ``RuntimeError`` if no active
                listener is found.

        Returns:
            List of ``AGStream`` objects (one per input state) in the same order
            as ``self.states``.  If a result was not received before ``timeout``,
            the corresponding entry is ``None``.

        Raises:
            ValueError: If ``self.states`` is empty or schema registration fails.
            RuntimeError: If *listener_manager* is provided but no alive listener
                is consuming ``self.input_topic``.

        Example::

            from agentics.core.listener_manager import ListenerManager

            mgr = ListenerManager(agstream_factory=make_ag)
            mgr.start(fn=summarise, input_topic='movie-reviews',
                      output_topic='movie-summaries')

            producer = AGStream(
                atype=MovieReview,
                kafka_server=KAFKA_SERVER,
                input_topic='movie-reviews',
                output_topic='movie-summaries',
                schema_registry_url=SCHEMA_REGISTRY_URL,
                instructions='Summarise the review in one sentence.',
            )
            producer.states = sample_reviews

            results = await producer.aproduce_and_collect(
                result_atype=MovieSummary,
                timeout=60,
                listener_manager=mgr,   # pre-flight check enabled
            )

            for review, result_ag in zip(sample_reviews, results):
                if result_ag:
                    print(review.title, '->', result_ag.states[0].one_line_summary)
        """
        import threading as _threading
        import uuid as _uuid

        from kafka import KafkaConsumer, TopicPartition

        # ── Pre-flight: verify a listener is active (if a registry was given) ──
        if listener_manager is not None:
            if not listener_manager.has_listener_for(self.input_topic):
                raise RuntimeError(
                    f"No active listener found for input_topic '{self.input_topic}'. "
                    "Start a listener via ListenerManager.start() or "
                    "ListenerManager.start_simple_listener() before calling "
                    "aproduce_and_collect()."
                )

        if not self.states:
            raise ValueError("No states to produce")

        # ── Step 1: record the current end-offset of the output topic so the
        #    consumer only reads messages produced AFTER this point.  This avoids
        #    picking up stale results from previous runs while still catching
        #    results that arrive before the consumer thread fully starts.
        bootstrap = _normalize_bootstrap_server(self.kafka_server)

        _start_offsets: Dict[TopicPartition, int] = {}
        try:
            _probe = KafkaConsumer(
                bootstrap_servers=bootstrap,
                consumer_timeout_ms=2000,
            )
            _partitions = _probe.partitions_for_topic(self.output_topic) or set()
            _tps = [TopicPartition(self.output_topic, p) for p in _partitions]
            if _tps:
                _probe.assign(_tps)
                _probe.seek_to_end(*_tps)
                _start_offsets = {tp: _probe.position(tp) for tp in _tps}
            _probe.close()
        except Exception:
            pass  # If we can't probe, fall back to reading from latest

        # ── Step 2: produce all states, recording keys in order ──────────────
        msg_ids = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self.produce(
                register_if_missing=register_if_missing,
                compatibility_mode=compatibility_mode,
            ),
        )

        if not msg_ids:
            raise ValueError(
                "produce() returned no message IDs — check schema registration"
            )

        # Map key → original index so we can restore order later
        key_to_index: Dict[str, int] = {k: i for i, k in enumerate(msg_ids)}
        pending_keys: set = set(msg_ids)
        results: Dict[str, Optional["AGStream"]] = {k: None for k in msg_ids}

        if verbose:
            sys.stderr.write(
                f"\n⏳ Waiting for {len(msg_ids)} transduced result(s) on '{self.output_topic}'...\n"
            )
            sys.stderr.flush()

        # ── Step 3: background consumer thread ───────────────────────────────
        # Shared state between the consumer thread and the async waiter.
        _lock = _threading.Lock()
        _stop = _threading.Event()

        def _consume():
            """Poll output_topic and collect messages whose key is in pending_keys."""
            if _start_offsets:
                # Seek to the recorded end-offsets so we only read new messages
                consumer = KafkaConsumer(
                    bootstrap_servers=_normalize_bootstrap_server(self.kafka_server),
                    enable_auto_commit=False,
                    group_id=f"agstream-aproduce-{_uuid.uuid4()}",
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=500,
                    request_timeout_ms=40000,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=3000,
                )
                tps = list(_start_offsets.keys())
                consumer.assign(tps)
                for tp, offset in _start_offsets.items():
                    consumer.seek(tp, offset)
            else:
                consumer = KafkaConsumer(
                    self.output_topic,
                    bootstrap_servers=bootstrap,
                    auto_offset_reset="latest",
                    enable_auto_commit=False,
                    group_id=f"agstream-aproduce-{_uuid.uuid4()}",
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=500,
                    request_timeout_ms=40000,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=3000,
                )

            try:
                while not _stop.is_set():
                    batch = consumer.poll(timeout_ms=500, max_records=50)
                    for _tp, messages in batch.items():
                        for msg in messages:
                            msg_key = msg.key.decode("utf-8") if msg.key else None
                            if msg_key is None or msg_key not in key_to_index:
                                continue

                            try:
                                _deserialize_atype = (
                                    result_atype
                                    if result_atype is not None
                                    else self.atype
                                )
                                ag = AGStream.deserialize(
                                    msg.value,
                                    atype=_deserialize_atype,
                                )
                            except Exception as exc:
                                if verbose:
                                    sys.stderr.write(
                                        f"  ⚠️  Could not deserialize result for key {msg_key[:8]}...: {exc}\n"
                                    )
                                    sys.stderr.flush()
                                continue

                            with _lock:
                                if msg_key in pending_keys:
                                    results[msg_key] = ag
                                    pending_keys.discard(msg_key)
                                    if verbose:
                                        idx = key_to_index[msg_key]
                                        sys.stderr.write(
                                            f"  ✓ [{idx + 1}/{len(msg_ids)}] Received result "
                                            f"(key: {msg_key[:8]}...)\n"
                                        )
                                        sys.stderr.flush()

                    with _lock:
                        if not pending_keys:
                            break
            finally:
                consumer.close()

        consumer_thread = _threading.Thread(target=_consume, daemon=True)
        consumer_thread.start()

        # ── Step 4: async wait until all results arrive or timeout ────────────
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            with _lock:
                remaining = len(pending_keys)

            if remaining == 0:
                break

            if asyncio.get_event_loop().time() >= deadline:
                if verbose:
                    sys.stderr.write(
                        f"\n⚠️  Timeout after {timeout}s — "
                        f"{remaining} result(s) not received\n"
                    )
                    sys.stderr.flush()
                break

            await asyncio.sleep(poll_interval)

        # Signal the consumer thread to stop and wait briefly for it to exit
        _stop.set()
        consumer_thread.join(timeout=2.0)

        # ── Step 5: reconstruct results in original order ─────────────────────
        ordered: List[Optional["AGStream"]] = [results[k] for k in msg_ids]

        if verbose:
            received = sum(1 for r in ordered if r is not None)
            sys.stderr.write(
                f"\n✅ aproduce_and_collect complete: "
                f"{received}/{len(msg_ids)} result(s) received\n"
            )
            sys.stderr.flush()

        return ordered

    def collect_latest_source(self, timeout_seconds: int = 30) -> Optional["AGStream"]:
        """
        Wait for and get the NEXT new message from the topic with a timeout.
        Waits for messages that arrive AFTER this method is called.

        Args:
            timeout_seconds: Maximum time to wait for a new message (default: 30)

        Returns:
            The next new AGStream message, or None if timeout
        """
        import time as time_module

        from kafka import KafkaConsumer, TopicPartition

        try:
            # Create consumer
            bootstrap_server = self.kafka_server
            if "localhost" in bootstrap_server:
                bootstrap_server = bootstrap_server.replace("localhost", "127.0.0.1")

            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_server,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,
                enable_auto_commit=False,
            )

            # Get partitions
            partitions = consumer.partitions_for_topic(self.input_topic)
            if not partitions:
                return None

            topic_partitions = [TopicPartition(self.input_topic, p) for p in partitions]
            consumer.assign(topic_partitions)

            # Seek to end to get current end offsets
            consumer.seek_to_end()

            # Record the current end positions - we want messages AFTER this
            start_positions = {tp: consumer.position(tp) for tp in topic_partitions}

            # Wait for NEW messages (messages that arrive after start_positions)
            start_time = time_module.time()

            while time_module.time() - start_time < timeout_seconds:
                message_batch = consumer.poll(timeout_ms=1000, max_records=1)

                if message_batch:
                    # Get the first new message
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            # Only return messages that are beyond our start position
                            if message.offset >= start_positions[topic_partition]:
                                try:
                                    ag = AGStream.deserialize(message.value)
                                    consumer.close()
                                    return ag
                                except Exception:
                                    continue

            consumer.close()
            return None

        except Exception as e:
            return None

    def collect_sources(
        self,
        max_messages: int = 100,
        timeout_ms: int = 5000,
        mode: str = "all",
        from_timestamp: Optional[int] = None,
        group_id: Optional[str] = None,
        validate_schema: bool = True,
        verbose: bool = False,
        atype_name: Optional[str] = None,
    ) -> list["AGStream"]:
        """
        Collect AGStream objects from Kafka with schema registry validation.

        This method is the consumer counterpart to produce().
        It collects messages and validates them against the schema registered in the
        schema registry. You can either provide an atype on the AGStream instance or
        specify an atype_name to fetch the type dynamically from the schema registry.

        Args:
            max_messages: Maximum number of messages to collect (default: 100)
            timeout_ms: Timeout in milliseconds for polling (default: 5000)
            mode: Collection mode - 'all', 'latest', or 'timestamp' (default: 'all')
            from_timestamp: Unix timestamp in ms to start from (only with mode='timestamp')
            group_id: Consumer group ID (default: None, generates unique ID)
            validate_schema: If True, validates each message against registry schema (default: True)
            verbose: If True, print detailed progress (default: False)
            atype_name: Optional type name to fetch from schema registry. If provided,
                       overrides the AGStream's atype. Useful when you don't have the
                       Pydantic class but know the schema name. (default: None)

        Returns:
            List of AGStream objects that passed schema validation

        Raises:
            ValueError: If schema validation fails and validate_schema=True

        Examples:
            >>> from agentics.core.streaming import AGStream
            >>> from pydantic import BaseModel
            >>>
            >>> class UserProfile(BaseModel):
            ...     user_id: str
            ...     username: str
            ...     email: str
            >>>
            >>> # Method 1: Collect with explicit atype
            >>> ag = AGStream(
            ...     atype=UserProfile,
            ...     input_topic="user-events",
            ...     schema_registry_url="http://localhost:8081"
            ... )
            >>> users = ag.collect_sources(
            ...     mode="latest",
            ...     max_messages=50,
            ...     validate_schema=True
            ... )
            >>>
            >>> # Method 2: Collect by type name (fetches schema from registry)
            >>> ag = AGStream(
            ...     input_topic="user-events",
            ...     schema_registry_url="http://localhost:8081"
            ... )
            >>> users = ag.collect_sources(
            ...     atype_name="UserProfile",  # Fetches UserProfile schema from registry
            ...     mode="latest",
            ...     max_messages=50,
            ...     validate_schema=True
            ... )
            >>>
            >>> print(f"Collected {len(users)} validated messages")
        """
        import sys

        # Determine the atype to use
        target_atype = self.atype

        # If atype_name is provided, fetch the type from schema registry
        if atype_name:
            if verbose:
                sys.stderr.write(
                    f"\n📥 Fetching type '{atype_name}' from schema registry...\n"
                )
                sys.stderr.flush()

            target_atype = get_atype_from_registry(
                atype_name=atype_name,
                schema_registry_url=self.schema_registry_url,
                is_key=False,
                version="latest",
                add_suffix=True,
            )

            if not target_atype:
                raise ValueError(
                    f"Could not fetch type '{atype_name}' from schema registry"
                )

            if verbose:
                sys.stderr.write(f"   ✓ Type fetched: {target_atype.__name__}\n")
                sys.stderr.flush()

        if not target_atype:
            raise ValueError(
                "Either atype must be set on AGStream or atype_name must be provided"
            )

        if verbose:
            sys.stderr.write(
                f"\n📥 Collecting {target_atype.__name__} messages with schema enforcement\n"
            )
            sys.stderr.write(f"   Topic: {self.input_topic}\n")
            sys.stderr.flush()

        # Check if schema exists in registry
        if validate_schema:
            # Determine schema name in order of preference:
            # 1. atype_name parameter (explicit)
            # 2. target_atype_name field (set on AGStream)
            # 3. source_atype_name field (fallback)
            # 4. __agentics_schema_name__ metadata (set by get_atype_from_registry)
            # 5. target_atype.__name__ (class name, may be "DynamicModel")
            schema_name = (
                atype_name
                or self.target_atype_name
                or self.source_atype_name
                or getattr(target_atype, "__agentics_schema_name__", None)
                or target_atype.__name__
            )
            subject = get_subject_name(schema_name, is_key=False, add_suffix=True)

            if not schema_exists(subject, self.schema_registry_url):
                raise ValueError(
                    f"Schema '{subject}' not found in registry at {self.schema_registry_url}. "
                    f"Messages cannot be validated."
                )

            if verbose:
                sys.stderr.write(f"   ✓ Schema found: {subject}\n")
                sys.stderr.flush()

        # Collect messages directly from Kafka (don't use collect_sources which wraps in AGStream)
        import time as time_module
        import uuid

        from kafka import KafkaConsumer, TopicPartition

        if verbose:
            sys.stderr.write(f"📥 Collecting messages with schema enforcement...\n")
            sys.stderr.flush()

        try:
            # Determine group ID
            if group_id is None:
                group_id = f"agstream-collector-{uuid.uuid4()}"

            # Determine auto_offset_reset based on mode
            if mode == "latest":
                auto_offset_reset = "latest"
            elif mode == "all":
                auto_offset_reset = "earliest"
            elif mode == "timestamp":
                if from_timestamp is None:
                    raise ValueError(
                        "mode='timestamp' requires from_timestamp parameter"
                    )
                auto_offset_reset = "earliest"
            else:
                raise ValueError(
                    f"Invalid mode '{mode}'. Must be 'all', 'latest', or 'timestamp'"
                )

            # Create consumer
            bootstrap_server = self.kafka_server
            if "localhost" in bootstrap_server:
                bootstrap_server = bootstrap_server.replace("localhost", "127.0.0.1")

            if mode == "timestamp":
                consumer = KafkaConsumer(
                    bootstrap_servers=_normalize_bootstrap_server(bootstrap_server),
                    auto_offset_reset=auto_offset_reset,
                    enable_auto_commit=True,
                    group_id=group_id,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=timeout_ms,
                    request_timeout_ms=40000,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=3000,
                )
            else:
                consumer = KafkaConsumer(
                    self.input_topic,
                    bootstrap_servers=bootstrap_server,
                    auto_offset_reset=auto_offset_reset,
                    enable_auto_commit=True,
                    group_id=group_id,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=timeout_ms,
                    request_timeout_ms=40000,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=3000,
                )

            # Handle timestamp mode
            if mode == "timestamp":
                time_module.sleep(0.5)
                partitions = consumer.partitions_for_topic(self.input_topic)
                if partitions:
                    topic_partitions = [
                        TopicPartition(self.input_topic, p) for p in partitions
                    ]
                    consumer.assign(topic_partitions)
                    timestamp_dict = {tp: from_timestamp for tp in topic_partitions}
                    offsets = consumer.offsets_for_times(timestamp_dict)
                    for tp, offset_and_timestamp in offsets.items():
                        if offset_and_timestamp is not None:
                            consumer.seek(tp, offset_and_timestamp.offset)
            else:
                consumer.poll(timeout_ms=100, max_records=1)
                time_module.sleep(0.2)

            collected_ags = []
            message_count = 0
            invalid_count = 0

            try:
                poll_count = 0
                max_empty_polls = 20
                empty_poll_count = 0

                while (
                    message_count < max_messages and empty_poll_count < max_empty_polls
                ):
                    try:
                        message_batch = consumer.poll(timeout_ms=500, max_records=10)
                        poll_count += 1

                        if not message_batch:
                            empty_poll_count += 1
                            continue

                        empty_poll_count = 0

                        for topic_partition, messages in message_batch.items():
                            for message in messages:
                                try:
                                    # Message value is the serialized AGStream
                                    serialized_ag = message.value

                                    # Extract states from the serialized AGStream
                                    if (
                                        "states" in serialized_ag
                                        and serialized_ag["states"]
                                    ):
                                        # Create AGStream objects for each state
                                        for state_dict in serialized_ag["states"]:
                                            try:
                                                # Validate state against atype
                                                if validate_schema:
                                                    state_obj = target_atype(
                                                        **state_dict
                                                    )
                                                else:
                                                    # Skip validation, just create object
                                                    state_obj = target_atype(
                                                        **state_dict
                                                    )

                                                # Create AGStream wrapper with single state
                                                ag = AGStream(atype=target_atype)
                                                ag.states = [state_obj]
                                                collected_ags.append(ag)
                                                message_count += 1

                                                if verbose:
                                                    sys.stderr.write(
                                                        f"  ✓ [{message_count}/{max_messages}] Collected {target_atype.__name__}\n"
                                                    )
                                                    sys.stderr.flush()

                                                if message_count >= max_messages:
                                                    break

                                            except (ValidationError, TypeError) as e:
                                                invalid_count += 1
                                                if verbose:
                                                    sys.stderr.write(
                                                        f"  ❌ Validation failed: {e}\n"
                                                    )
                                                    sys.stderr.flush()
                                                if validate_schema:
                                                    # In strict mode, raise on validation error
                                                    raise ValueError(
                                                        f"Schema validation failed: {e}"
                                                    )

                                    if message_count >= max_messages:
                                        break

                                except Exception as e:
                                    if verbose:
                                        sys.stderr.write(
                                            f"  ⚠️  Error processing message: {e}\n"
                                        )
                                        sys.stderr.flush()
                                    if validate_schema:
                                        raise

                            if message_count >= max_messages:
                                break

                    except Exception as e:
                        if verbose:
                            sys.stderr.write(f"  ⚠️  Poll error: {e}\n")
                            sys.stderr.flush()
                        break

            finally:
                consumer.close()

            if verbose:
                sys.stderr.write(
                    f"\n✅ Collected {len(collected_ags)} validated messages\n"
                )
                if invalid_count > 0:
                    sys.stderr.write(f"⚠️  Skipped {invalid_count} invalid messages\n")
                sys.stderr.flush()

            return collected_ags

        except Exception as e:
            sys.stderr.write(f"\n❌ Error collecting with schema enforcement: {e}\n")
            sys.stderr.flush()
            raise

    def listen(
        self,
        source_atype_name: Optional[str] = None,
        timeout_ms: int = 500,
        poll_interval_ms: int = 100,
        max_empty_polls: Optional[int] = None,
        group_id: Optional[str] = None,
        validate_schema: bool = True,
        produce_results: bool = True,
        verbose: bool = False,
        schema_fetch_retries: int = 5,
        schema_fetch_retry_delay: float = 2.0,
        stop_event=None,
        log_queue=None,
        auto_offset_reset: str = "earliest",
        lookback_messages: Optional[int] = None,
        filter_keys: Optional[List[str]] = None,
        stop_after_lookback: bool = False,
        parallelism: int = 1,
    ):
        """
        Continuously listen to the input Kafka topic, validate each incoming state
        against the schema registry, transduce it using the AGStream's configured
        atype and instructions, and optionally produce the result to the output topic.

        This method creates a transducible function from the AGStream's parameters
        (source type, target type, and instructions) and delegates to
        ``transducible_function_listener()`` for execution.

        For each message received:
          1. Deserialize and validate the state against the schema registry.
          2. Wrap the single state in a source AGStream.
          3. Transduce: ``self << source`` (uses the LLM-based logical transduction).
          4. If ``produce_results=True``, produce the transduced state to the output topic
             using schema enforcement.

        By default this method runs indefinitely (blocking) until interrupted
        (e.g. ``KeyboardInterrupt`` or ``stop_event``).  Pass ``max_empty_polls``
        to make it exit automatically after a fixed number of consecutive empty polls.

        Args:
            source_atype_name: Optional name of the source type to fetch from the schema
                registry. If None, the source type is inferred from the incoming message.
            timeout_ms: Consumer poll timeout in milliseconds (default: 500).
            poll_interval_ms: Sleep interval between polls in milliseconds (default: 100).
            max_empty_polls: Maximum number of consecutive empty polls before the listener
                exits automatically.  ``None`` (default) means run forever — the listener
                only stops on ``KeyboardInterrupt`` or when ``stop_event`` is set.
                Set to a small integer (e.g. ``5``) for finite / dry-run scenarios.
            group_id: Kafka consumer group ID. If None, a unique ID is generated so that
                the listener always reads from the earliest available offset.
            auto_offset_reset: Kafka consumer ``auto.offset.reset`` setting.
                ``"earliest"`` (default) reads all messages from the beginning of the
                topic when no committed offset exists.  Use ``"latest"`` to process only
                messages produced *after* the listener starts — useful for dry-run or
                one-shot scenarios where you don't want to replay old messages.
            validate_schema: If True, validate each incoming state against the schema
                registry before transducing (default: True).
            produce_results: If True, produce each transduced result to the output topic
                with schema enforcement (default: True).
            verbose: If True, print detailed progress to stderr (default: False).
            schema_fetch_retries: Number of times to retry fetching the source schema
                from the registry before raising an error (default: 5). Useful when the
                listener is started before ``register_atype_schema()`` has been called,
                or when the schema registry is temporarily unavailable.
            schema_fetch_retry_delay: Seconds to wait between schema-fetch retries
                (default: 2.0).
            lookback_messages: Optional number of messages to look back from the end of
                the topic. If set, the consumer will seek to this offset before processing.
                Use with ``stop_after_lookback=True`` to process only historical messages.
            filter_keys: Optional list of message keys to filter. Only messages with keys
                in this list will be processed. Useful for reprocessing specific messages.
            stop_after_lookback: If True and ``lookback_messages`` or ``filter_keys`` is set,
                the listener will stop after processing the target messages and return the
                transduced results. If False (default), continues listening indefinitely.
            parallelism: Number of parallel workers for processing messages (default: 1).
                Uses ThreadPoolExecutor to process multiple messages concurrently. Higher
                values increase throughput but consume more resources. Note: This is different
                from PyFlink parallelism - it controls concurrent message processing in the
                KafkaConsumer-based listener.

        Returns:
            List[AGStream]: When ``stop_after_lookback=True``, returns a list of AGStream
                objects containing the transduced results. Otherwise returns None (runs forever).

        Raises:
            ValueError: If ``self.atype`` is not set on the AGStream instance, or if
                the source schema cannot be fetched after all retries are exhausted.
            KeyboardInterrupt: Raised when the user interrupts the listener loop.

        Examples:
            >>> from pydantic import BaseModel
            >>> from agentics.core.streaming import AGStream
            >>>
            >>> class MovieReview(BaseModel):
            ...     title: str
            ...     review: str
            >>>
            >>> class MovieSummary(BaseModel):
            ...     title: str
            ...     one_line_summary: str
            >>>
            >>> ag = AGStream(
            ...     atype=MovieSummary,
            ...     input_topic="movie-reviews",
            ...     output_topic="movie-summaries",
            ...     schema_registry_url="http://localhost:8081",
            ...     instructions="Summarise the review in one sentence.",
            ... )
            >>> ag.listen(verbose=True)
        """
        # ------------------------------------------------------------------
        # Import make_transducible_function
        # ------------------------------------------------------------------
        # ------------------------------------------------------------------
        # Resolve the source atype from the schema registry if requested
        # ------------------------------------------------------------------
        import time as _time_module

        from agentics.core.transducible_functions import make_transducible_function

        # Validate that we have either atype or target_atype_name
        if not self.atype and not self.target_atype_name:
            raise ValueError(
                "Either self.atype or self.target_atype_name must be set before calling listen(). "
                "Create the AGStream with: AGStream(atype=YourTargetModel, ...) or "
                "AGStream(target_atype_name='YourModel', ...)"
            )

        source_atype = None
        if source_atype_name:
            sys.stderr.write(
                f"\n📥 Fetching source type '{source_atype_name}' from schema registry...\n"
            )
            sys.stderr.flush()

            # If the caller already passed a fully-qualified subject name (e.g.
            # "Question-value") do NOT append another "-value" suffix.
            _src_add_suffix = not (
                source_atype_name.endswith("-value")
                or source_atype_name.endswith("-key")
            )

            last_error: Optional[str] = None
            for attempt in range(1, schema_fetch_retries + 1):
                source_atype = get_atype_from_registry(
                    atype_name=source_atype_name,
                    schema_registry_url=self.schema_registry_url,
                    is_key=False,
                    version="latest",
                    add_suffix=_src_add_suffix,
                )
                if source_atype:
                    break
                _subject_display = (
                    source_atype_name
                    if not _src_add_suffix
                    else f"{source_atype_name}-value"
                )
                last_error = (
                    f"Subject '{_subject_display}' not found in registry "
                    f"at {self.schema_registry_url}."
                )
                if attempt < schema_fetch_retries:
                    sys.stderr.write(
                        f"   ⚠️  Schema not found (attempt {attempt}/{schema_fetch_retries}). "
                        f"Retrying in {schema_fetch_retry_delay:.1f}s — "
                        f"make sure you have called register_atype_schema() first.\n"
                    )
                    sys.stderr.flush()
                    _time_module.sleep(schema_fetch_retry_delay)

            if not source_atype:
                raise ValueError(
                    f"Could not fetch source type '{source_atype_name}' from schema registry "
                    f"after {schema_fetch_retries} attempt(s). "
                    f"Register the schema first with:\n"
                    f"    AGStream(atype=<YourModel>, ...).register_atype_schema()\n"
                    f"Last error: {last_error}"
                )
            sys.stderr.write(f"   ✓ Source type fetched: {source_atype.__name__}\n")
            sys.stderr.flush()

        # ------------------------------------------------------------------
        # Determine the effective target atype
        # ------------------------------------------------------------------
        effective_target_atype = self.atype
        if self.target_atype_name:
            sys.stderr.write(
                f"\n📥 Fetching target type '{self.target_atype_name}' from schema registry...\n"
            )
            sys.stderr.flush()

            _tgt_add_suffix = not (
                self.target_atype_name.endswith("-value")
                or self.target_atype_name.endswith("-key")
            )

            last_target_error: Optional[str] = None
            for attempt in range(1, schema_fetch_retries + 1):
                _fetched = get_atype_from_registry(
                    atype_name=self.target_atype_name,
                    schema_registry_url=self.schema_registry_url,
                    is_key=False,
                    version="latest",
                    add_suffix=_tgt_add_suffix,
                )
                if _fetched is not None:
                    effective_target_atype = _fetched
                    break
                _tgt_subject_display = (
                    self.target_atype_name
                    if not _tgt_add_suffix
                    else f"{self.target_atype_name}-value"
                )
                last_target_error = (
                    f"Subject '{_tgt_subject_display}' not found in registry "
                    f"at {self.schema_registry_url}."
                )
                if attempt < schema_fetch_retries:
                    sys.stderr.write(
                        f"   ⚠️  Target schema not found (attempt {attempt}/{schema_fetch_retries}). "
                        f"Retrying in {schema_fetch_retry_delay:.1f}s — "
                        f"make sure register_atype_schema() has been called first.\n"
                    )
                    sys.stderr.flush()
                    _time_module.sleep(schema_fetch_retry_delay)

            if effective_target_atype is None or (
                self.atype is not None and effective_target_atype == self.atype
            ):
                raise ValueError(
                    f"Could not fetch target type '{self.target_atype_name}' from schema registry "
                    f"after {schema_fetch_retries} attempt(s). "
                    f"Register the schema first with:\n"
                    f"    AGStream(atype=<YourModel>, ...).register_atype_schema()\n"
                    f"Last error: {last_target_error}"
                )
            sys.stderr.write(
                f"   ✓ Target type fetched: {effective_target_atype.__name__}\n"
            )
            sys.stderr.flush()

        # Ensure we have a valid target atype
        if not effective_target_atype:
            raise ValueError(
                "Could not determine target atype. Either set self.atype or provide "
                "a valid self.target_atype_name that exists in the schema registry."
            )

        # ------------------------------------------------------------------
        # Create a transducible function from AGStream parameters
        # ------------------------------------------------------------------
        if verbose:
            sys.stderr.write(
                f"\n🔧 Creating transducible function:\n"
                f"   Input  : {source_atype.__name__ if source_atype else 'inferred'}\n"
                f"   Output : {effective_target_atype.__name__}\n"
            )
            sys.stderr.flush()

        # Build the transducible function with AGStream's configuration
        fn = make_transducible_function(
            InputModel=source_atype if source_atype else effective_target_atype,
            OutputModel=effective_target_atype,
            instructions=self.instructions
            or f"Transduce from input to {effective_target_atype.__name__}",
            tools=self.tools if hasattr(self, "tools") else [],
            schema_registry_url=self.schema_registry_url,
            auto_register=True,
            llm=self.llm if hasattr(self, "llm") else None,
            reasoning=self.reasoning if hasattr(self, "reasoning") else False,
            max_iter=self.max_iter if hasattr(self, "max_iter") else 10,
            verbose_transduction=verbose,
            provide_explanation=False,
        )

        if verbose:
            sys.stderr.write(f"   ✓ Transducible function created\n\n")
            sys.stderr.flush()

        # ------------------------------------------------------------------
        # Delegate to transducible_function_listener (Flink-based)
        # ------------------------------------------------------------------
        # Adjust max_empty_polls for Flink startup time
        # Flink needs time to start up and seek to lookback position
        # Default to 300 (5 minutes with 1000ms poll interval) if not specified
        effective_max_empty_polls = (
            max_empty_polls if max_empty_polls is not None else 300
        )

        return self.transducible_function_listener(
            fn=fn,
            timeout_ms=timeout_ms,
            poll_interval_ms=poll_interval_ms,
            max_empty_polls=effective_max_empty_polls,
            group_id=group_id,
            validate_schema=validate_schema,
            produce_results=produce_results,
            verbose=verbose,
            schema_fetch_retries=schema_fetch_retries,
            schema_fetch_retry_delay=schema_fetch_retry_delay,
            stop_event=stop_event,
            log_queue=log_queue,
            parallelism=parallelism,
            lookback_messages=lookback_messages,
            filter_keys=filter_keys,
            stop_after_lookback=stop_after_lookback,
        )

    def transducible_function_listener(
        self,
        fn: Any,
        timeout_ms: int = 500,
        poll_interval_ms: int = 1000,
        max_empty_polls: int = 30,
        group_id: Optional[str] = None,
        validate_schema: bool = True,
        produce_results: bool = True,
        verbose: bool = False,
        schema_fetch_retries: int = 5,
        schema_fetch_retry_delay: float = 2.0,
        stop_event: Optional[Any] = None,
        log_queue: Optional[Any] = None,
        background: bool = False,
        flink_startup_wait_s: float = 5.0,
        parallelism: int = 4,
        lookback_messages: Optional[int] = None,
        filter_keys: Optional[List[str]] = None,
        stop_after_lookback: bool = False,
    ):
        """
        Continuously listen to the input Kafka topic, validate each incoming state
        against the schema registry, apply a **transducible function** to it, and
        optionally produce the result to the output topic.

        This method uses a **PyFlink streaming pipeline** (mirroring ``listen()``)
        for parallel, distributed processing.  The transducible function is
        reconstructed fresh inside each Flink task slot via
        ``make_transducible_function()`` so that only picklable data
        (Pydantic classes + instruction strings) is serialised across workers.
        Flink's ``set_parallelism(4)`` means up to 4 states are processed
        concurrently, each in its own thread with its own ``asyncio`` event loop.

        **Execution Modes:**
        - ``execution_mode="local"`` (default): Runs PyFlink embedded in the current process
        - ``execution_mode="cluster"``: Note - PyFlink does not support direct remote submission.
          For cluster execution, use FlinkListenerManager which submits jobs via REST API.

        The transducible function must have been decorated with ``@transducible`` (or
        created via ``make_transducible_function``).  Its ``input_model`` and
        ``target_model`` attributes are used to:

        * Fetch (and optionally validate) the source schema from the registry.
        * Register the output schema in the registry if it is missing.
        * Deserialize incoming states into ``fn.input_model`` instances.
        * Serialize outgoing states as ``fn.target_model`` instances.

        Key propagation: each output message is produced with the **same Kafka key**
        as the source message that triggered it.

        .. note::
            ``stop_event`` and ``log_queue`` are accepted for API compatibility but
            are **not functional** in the Flink execution model.  Flink jobs run
            until the cluster cancels them or the process is interrupted (Ctrl+C /
            SIGTERM).  To stop the job programmatically, cancel it via the Flink
            REST API or ``env.execute()`` future.

        Args:
            fn: A transducible function (decorated with ``@transducible`` or created
                via ``make_transducible_function``).  Must expose ``fn.input_model``
                and ``fn.target_model`` attributes.  When ``fn`` was created with
                ``@transducible``, its ``__original_fn__`` source code is extracted
                via ``inspect.getsource()`` and stored as a string so that custom
                pre/post-processing logic is preserved across Flink task slots.
                For dynamically-created functions (no source available), the
                function's ``__doc__`` string is used as the transduction
                instructions instead.
            timeout_ms: Unused (kept for API compatibility with the previous
                KafkaConsumer-based implementation).
            poll_interval_ms: Milliseconds between idle checks in the main thread
                (default: 1000).  Combined with ``max_empty_polls`` to compute the
                idle timeout: ``max_empty_polls × poll_interval_ms`` ms of no new
                Kafka messages causes the listener to stop automatically.
            max_empty_polls: Number of idle poll intervals before the listener stops
                (default: 30).  With the default ``poll_interval_ms=1000`` this gives
                a 30-second idle timeout.  Increase this value if you expect long gaps
                between messages (e.g. ``max_empty_polls=300`` → 5-minute timeout).
            group_id: Kafka consumer group ID passed to the Flink SQL source table.
                Defaults to ``"agstream-fn-listener"`` if None.
            validate_schema: If True, reject incoming states that fail Pydantic
                validation inside each task slot (default: True).
            produce_results: If True, produce each result to the output topic with
                schema enforcement (default: True).
            verbose: If True, print detailed startup info to stderr (default: False).
            schema_fetch_retries: Number of times to retry fetching the source schema
                from the registry before raising an error (default: 5).
            schema_fetch_retry_delay: Seconds to wait between schema-fetch retries
                (default: 2.0).
            stop_event: **Not functional in Flink mode.**  Accepted for API
                compatibility only.
            log_queue: **Not functional in Flink mode.**  Accepted for API
                compatibility only.
            parallelism: Number of parallel task slots for processing messages
                (default: 4). Higher values increase throughput but consume more
                resources. Limited by the number of Kafka topic partitions.
            lookback_messages: Optional number of messages to look back from the end of
                the topic. If set, the consumer will start from this offset. Use with
                ``stop_after_lookback=True`` to process only historical messages.
            filter_keys: Optional list of message keys to filter. Only messages with keys
                in this list will be processed. Useful for reprocessing specific messages.
            stop_after_lookback: If True and ``lookback_messages`` or ``filter_keys`` is set,
                the listener will stop after processing the target messages. If False (default),
                continues listening indefinitely.

        Raises:
            ValueError: If ``fn`` does not expose ``input_model`` / ``target_model``,
                or if the source schema cannot be fetched after all retries.

        Examples:
            >>> from pydantic import BaseModel
            >>> from agentics.core.streaming import AGStream
            >>> from agentics.core.transducible_functions import transducible, Transduce
            >>>
            >>> class MovieReview(BaseModel):
            ...     title: str
            ...     review: str
            ...     rating: float
            >>>
            >>> class MovieSummary(BaseModel):
            ...     title: str
            ...     one_line_summary: str
            ...     sentiment: str
            >>>
            >>> @transducible()
            ... async def summarise(review: MovieReview) -> MovieSummary:
            ...     \"\"\"Summarise the review in one sentence and classify sentiment.\"\"\"
            ...     return Transduce(review)
            >>>
            >>> ag = AGStream(
            ...     kafka_server="localhost:9092",
            ...     input_topic="movie-reviews",
            ...     output_topic="movie-summaries",
            ...     schema_registry_url="http://localhost:8081",
            ... )
            >>> ag.transducible_function_listener(summarise, verbose=True)
        """
        import time as _time_module

        from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
        from pyflink.table import EnvironmentSettings, StreamTableEnvironment

        # ------------------------------------------------------------------
        # Validate the transducible function
        # ------------------------------------------------------------------
        if not hasattr(fn, "input_model") or not hasattr(fn, "target_model"):
            raise ValueError(
                "fn must be a transducible function with 'input_model' and "
                "'target_model' attributes.  Decorate it with @transducible."
            )

        source_atype = fn.input_model
        target_atype = fn.target_model

        # ------------------------------------------------------------------
        # If the TransducibleFunction carries its own schema_registry_url,
        # use it in preference to self.schema_registry_url.
        # ------------------------------------------------------------------
        effective_registry_url: str = (
            getattr(fn, "schema_registry_url", None) or self.schema_registry_url
        )

        # ------------------------------------------------------------------
        # Verify source schema in registry (with retries)
        # ------------------------------------------------------------------
        sys.stderr.write(
            f"\n📥 Verifying source schema '{source_atype.__name__}' in registry...\n"
        )
        sys.stderr.flush()

        last_error: Optional[str] = None
        source_schema_ok = False
        for attempt in range(1, schema_fetch_retries + 1):
            fetched = get_atype_from_registry(
                atype_name=source_atype.__name__,
                schema_registry_url=effective_registry_url,
                is_key=False,
                version="latest",
                add_suffix=True,
            )
            if fetched is not None:
                source_schema_ok = True
                break
            last_error = (
                f"Subject '{source_atype.__name__}-value' not found in registry "
                f"at {effective_registry_url}."
            )
            if attempt < schema_fetch_retries:
                sys.stderr.write(
                    f"   ⚠️  Schema not found (attempt {attempt}/{schema_fetch_retries}). "
                    f"Retrying in {schema_fetch_retry_delay:.1f}s — "
                    f"make sure register_atype_schema() has been called first.\n"
                )
                sys.stderr.flush()
                _time_module.sleep(schema_fetch_retry_delay)

        if not source_schema_ok:
            raise ValueError(
                f"Could not find source schema '{source_atype.__name__}' in registry "
                f"after {schema_fetch_retries} attempt(s). "
                f"Register it first with:\n"
                f"    AGStream(atype={source_atype.__name__}, ...).register_atype_schema()\n"
                f"Last error: {last_error}"
            )
        sys.stderr.write(f"   ✓ Source schema verified: {source_atype.__name__}\n")
        sys.stderr.flush()

        # ------------------------------------------------------------------
        # Optionally verify / register target schema
        # ------------------------------------------------------------------
        if produce_results:
            subject = get_subject_name(
                target_atype.__name__, is_key=False, add_suffix=True
            )
            if not schema_exists(subject, effective_registry_url):
                sys.stderr.write(
                    f"⚠️  Target schema '{subject}' not found — will be registered "
                    f"on first produce.\n"
                )
                sys.stderr.flush()
            elif verbose:
                sys.stderr.write(f"   ✓ Target schema found: {subject}\n")
                sys.stderr.flush()

        # ------------------------------------------------------------------
        # Extract instructions and source code from the transducible function.
        # fn.__doc__ carries the instructions; fn.__original_fn__ is the raw
        # decorated function whose source code we can retrieve with inspect.
        # Storing source code as a string keeps ProcessTransducibleFn picklable
        # while preserving any custom pre/post-processing logic in the body.
        # ------------------------------------------------------------------

        # Calculate target message count
        # - If filter_keys is set, always stop after processing all filtered messages
        # - If lookback_messages is set with stop_after_lookback, stop after N messages
        target_message_count = None
        if filter_keys is not None:
            # Auto-stop after processing all filtered messages
            target_message_count = len(filter_keys)
        elif lookback_messages is not None and stop_after_lookback:
            target_message_count = lookback_messages

        sys.stderr.write(
            f"\n🎧 transducible_function_listener (Flink) started\n"
            f"   Input topic  : {self.input_topic}\n"
            f"   Output topic : {self.output_topic}\n"
            f"   Function     : {fn.__name__}\n"
            f"   Input type   : {source_atype.__name__}\n"
            f"   Output type  : {target_atype.__name__}\n"
            f"   fn registry  : stored in _FN_REGISTRY['{fn.__name__}']\n"
        )
        if lookback_messages:
            sys.stderr.write(f"   Lookback     : {lookback_messages} messages\n")
        if filter_keys:
            sys.stderr.write(f"   Filter keys  : {len(filter_keys)} keys\n")
        if stop_after_lookback:
            sys.stderr.write(
                f"   Mode         : Stop after processing target messages\n"
            )
        sys.stderr.write("\n")
        sys.stderr.flush()

        # ------------------------------------------------------------------
        # Build the Flink streaming pipeline (mirrors listen())
        # ------------------------------------------------------------------
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        env.set_parallelism(parallelism)

        settings = EnvironmentSettings.in_streaming_mode()
        table_env = StreamTableEnvironment.create(env, settings)

        # Add Kafka connector JAR - use project's tools/agstream_manager/flink-lib directory
        import pathlib

        # Go up 4 levels: streaming.py -> core -> agentics -> src -> workspace root
        project_root = pathlib.Path(__file__).parent.parent.parent.parent
        flink_jar = (
            project_root
            / "tools"
            / "agstream_manager"
            / "flink-lib"
            / "flink-sql-connector-kafka-3.3.0-1.18.jar"
        )

        if not flink_jar.exists():
            # Fallback to environment variable if set
            flink_jar_env = os.getenv("FLINK_KAFKA_CONNECTOR_JAR")
            if flink_jar_env:
                flink_jar = pathlib.Path(flink_jar_env)
            else:
                raise FileNotFoundError(
                    f"Flink Kafka connector JAR not found at {flink_jar}. "
                    f"Please ensure the JAR exists or set FLINK_KAFKA_CONNECTOR_JAR environment variable."
                )

        table_env.get_config().get_configuration().set_string(
            "pipeline.jars",
            f"file://{flink_jar.absolute()}",
        )

        # Use earliest-offset when filter_keys is set or lookback_messages > 0 to read existing messages
        # Use latest-offset by default (wait for new messages only)
        if filter_keys is not None or (
            lookback_messages is not None and lookback_messages > 0
        ):
            scan_mode = "earliest-offset"
        else:
            scan_mode = "latest-offset"

        # Define job name early so we can use it in registries
        job_name = f"AGStream transducible_function_listener: {fn.__name__}"

        # Build a map of partition-offset -> actual Kafka key
        # This is ALWAYS done to preserve keys in output messages
        from kafka import KafkaConsumer

        sys.stderr.write(f"\n📋 Building key map to preserve message keys...\n")
        sys.stderr.flush()

        key_map = {}  # Maps "partition-X-offset-Y" -> actual UUID key
        matching_partition_offsets = (
            []
        )  # List of partition-offset combos that match filter_keys (if filtering)

        temp_consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=_normalize_bootstrap_server(self.kafka_server),
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            enable_auto_commit=False,
        )

        for message in temp_consumer:
            partition_offset_key = (
                f"partition-{message.partition}-offset-{message.offset}"
            )
            actual_key = message.key.decode() if message.key else None
            if actual_key:
                key_map[partition_offset_key] = actual_key
                # If filtering is enabled and this key matches, add its partition-offset to the list
                if filter_keys is not None and actual_key in filter_keys:
                    matching_partition_offsets.append(
                        (message.partition, message.offset)
                    )

        temp_consumer.close()

        # Store the key map in the registry so ProcessTransducibleFn can use it
        _KEY_MAP_REGISTRY[job_name] = key_map
        sys.stderr.write(f"   ✓ Built key map with {len(key_map)} entries\n")

        # Start a background thread to continuously update the key map for new messages
        # Capture variables in closure
        input_topic_name = self.input_topic
        kafka_server_addr = self.kafka_server

        def update_key_map_continuously():
            """Background thread to keep key_map updated with new messages."""
            import time

            # Track the highest offset seen per partition from initial scan
            max_offsets = {}
            for key in key_map.keys():
                if key.startswith("partition-") and "-offset-" in key:
                    parts = key.split("-")
                    partition = int(parts[1])
                    offset = int(parts[3])
                    max_offsets[partition] = max(max_offsets.get(partition, -1), offset)

            sys.stderr.write(f"   📊 Initial max offsets: {max_offsets}\n")
            sys.stderr.flush()

            while job_name in _KEY_MAP_REGISTRY:
                try:
                    # Create a new consumer to check for new messages
                    update_consumer = KafkaConsumer(
                        input_topic_name,
                        bootstrap_servers=_normalize_bootstrap_server(
                            kafka_server_addr
                        ),
                        auto_offset_reset="earliest",
                        consumer_timeout_ms=2000,
                        enable_auto_commit=False,
                    )

                    new_keys_added = 0
                    for message in update_consumer:
                        # Only process messages with offsets higher than what we've seen
                        if message.offset > max_offsets.get(message.partition, -1):
                            partition_offset_key = (
                                f"partition-{message.partition}-offset-{message.offset}"
                            )
                            actual_key = message.key.decode() if message.key else None

                            if (
                                actual_key
                                and partition_offset_key
                                not in _KEY_MAP_REGISTRY[job_name]
                            ):
                                _KEY_MAP_REGISTRY[job_name][
                                    partition_offset_key
                                ] = actual_key
                                max_offsets[message.partition] = message.offset
                                new_keys_added += 1
                                sys.stderr.write(
                                    f"   ✓ Added key: {partition_offset_key} -> {actual_key[:8]}...\n"
                                )
                                sys.stderr.flush()

                    update_consumer.close()

                    if new_keys_added > 0:
                        sys.stderr.write(
                            f"   📊 Key map now has {len(_KEY_MAP_REGISTRY[job_name])} entries\n"
                        )
                        sys.stderr.flush()

                    time.sleep(2)  # Check for new messages every 2 seconds

                except Exception as e:
                    sys.stderr.write(f"   ⚠️  Key map update error: {e}\n")
                    sys.stderr.flush()
                    time.sleep(5)

        # Start the background thread
        import threading

        key_map_thread = threading.Thread(
            target=update_key_map_continuously, daemon=True
        )
        key_map_thread.start()
        sys.stderr.write(f"   ✓ Started background key map updater\n")

        # Build SQL WHERE clause to filter at source (only if filter_keys was provided)
        partition_offset_filter = None
        if filter_keys is not None:
            if matching_partition_offsets:
                conditions = [
                    f"(kafka_partition = {p} AND kafka_offset = {o})"
                    for p, o in matching_partition_offsets
                ]
                partition_offset_filter = " OR ".join(conditions)
                sys.stderr.write(
                    f"   ✓ Found {len(matching_partition_offsets)} matching messages for SQL filtering\n"
                )
            else:
                sys.stderr.write(f"   ⚠️  No matching keys found in topic\n")
        sys.stderr.flush()

        # Use BYTES type to preserve binary Avro data without corruption
        # Note: Flink SQL Kafka connector does NOT support reading message keys as metadata
        # We use partition+offset to look up keys from the key_map built at startup
        create_table_ddl = f"""
            CREATE TABLE kafka_source (
                `value` BYTES,
                `event_timestamp` TIMESTAMP(3) METADATA FROM 'timestamp',
                `kafka_partition` INT METADATA FROM 'partition',
                `kafka_offset` BIGINT METADATA FROM 'offset',
                WATERMARK FOR `event_timestamp` AS `event_timestamp` - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{self.input_topic}',
                'properties.bootstrap.servers' = '{self.kafka_server}',
                'properties.group.id' = '{group_id or "agstream-fn-listener"}',
                'scan.startup.mode' = '{scan_mode}',
                'format' = 'raw'
            )
        """
        table_env.execute_sql(create_table_ddl)

        # Build SQL query with optional WHERE clause for filtering
        if partition_offset_filter:
            sql_query = f"""
                SELECT
                    CONCAT('partition-', CAST(kafka_partition AS STRING), '-offset-', CAST(kafka_offset AS STRING)) as key,
                    UNIX_TIMESTAMP(CAST(event_timestamp AS STRING)) * 1000 as timestamp_ms,
                    `value`
                FROM kafka_source
                WHERE {partition_offset_filter}
            """
        else:
            sql_query = """
                SELECT
                    CONCAT('partition-', CAST(kafka_partition AS STRING), '-offset-', CAST(kafka_offset AS STRING)) as key,
                    UNIX_TIMESTAMP(CAST(event_timestamp AS STRING)) * 1000 as timestamp_ms,
                    `value`
                FROM kafka_source
            """

        result_table = table_env.sql_query(sql_query)
        ds = table_env.to_data_stream(result_table)

        # Register an activity event for this job so ProcessTransducibleFn.map()
        # can signal the idle-timeout watcher below.
        activity_event = threading.Event()
        _ACTIVITY_REGISTRY[job_name] = activity_event

        # Store the live fn object in the module-level registry so that
        # ProcessTransducibleFn.map() can look it up without pickling.
        # (Pydantic model classes defined in __main__ / Jupyter notebooks
        # do not survive pickle/unpickle, so we avoid storing them as
        # instance attributes on the MapFunction.)
        _FN_REGISTRY[job_name] = fn

        # Store filter keys if provided
        if filter_keys is not None:
            _FILTER_KEYS_REGISTRY[job_name] = filter_keys

        # Apply the transducible function via ProcessTransducibleFn MapFunction
        processed_stream = ds.map(
            func=ProcessTransducibleFn(
                job_name=job_name,
                kafka_server=self.kafka_server,
                output_topic=self.output_topic,
                schema_registry_url=effective_registry_url,
                validate_schema=validate_schema,
                produce_results=produce_results,
                target_atype_name=self.target_atype_name,
                num_partitions=parallelism,  # Match partitions to parallelism
                target_message_count=target_message_count,
            )
        )

        # Filter out error/skip results and print progress
        processed_stream.filter(lambda x: x is not None).print()

        # ------------------------------------------------------------------
        # Run env.execute() in a daemon thread so we can implement idle-timeout
        # termination (mirrors the old max_empty_polls behaviour).
        # The idle timeout is: max_empty_polls * poll_interval_ms milliseconds.
        # ------------------------------------------------------------------
        idle_timeout_s = (max_empty_polls * poll_interval_ms) / 1000.0

        flink_exc: list = []

        def _run_flink():
            try:
                env.execute(job_name)
            except Exception as exc:
                flink_exc.append(exc)

        flink_thread = threading.Thread(target=_run_flink, daemon=True)
        flink_thread.start()

        if verbose:
            sys.stderr.write(
                f"   Idle timeout : {idle_timeout_s:.1f}s "
                f"(max_empty_polls={max_empty_polls} × poll_interval_ms={poll_interval_ms})\n\n"
            )
            sys.stderr.flush()

        # Wait for the first message to arrive (give Flink time to start up)
        # then switch to idle-timeout mode.
        startup_timeout_s = max(idle_timeout_s, 30.0)
        got_first = activity_event.wait(timeout=startup_timeout_s)

        if not got_first:
            # No messages arrived during startup window — stop.
            if verbose:
                sys.stderr.write(
                    f"⏹  No messages received within {startup_timeout_s:.0f}s startup window. Stopping.\n"
                )
                sys.stderr.flush()
            _ACTIVITY_REGISTRY.pop(job_name, None)
            return

        # Idle-timeout loop: reset the event and wait; if it doesn't fire
        # within idle_timeout_s, no new messages arrived → stop.
        # Also check completion flag for immediate stop when target is reached.
        # Use shorter polling interval to detect completion quickly.
        poll_check_interval = min(
            1.0, idle_timeout_s / 10
        )  # Check every 1s or 10% of timeout
        idle_time = 0.0

        while True:
            # Check if completion flag is set
            if _COMPLETION_REGISTRY.get(job_name, False):
                if verbose:
                    sys.stderr.write(
                        f"⏹  Target message count reached — stopping listener.\n"
                    )
                    sys.stderr.flush()
                break

            activity_event.clear()
            fired = activity_event.wait(timeout=poll_check_interval)

            if fired:
                # Activity detected, reset idle timer
                idle_time = 0.0
            else:
                # No activity, increment idle timer
                idle_time += poll_check_interval
                if idle_time >= idle_timeout_s:
                    # Idle timeout reached — no new messages
                    if verbose:
                        sys.stderr.write(
                            f"⏹  Idle for {idle_timeout_s:.1f}s — stopping listener.\n"
                        )
                        sys.stderr.flush()
                    break

        _ACTIVITY_REGISTRY.pop(job_name, None)
        _FILTER_KEYS_REGISTRY.pop(job_name, None)
        _MESSAGE_COUNTER_REGISTRY.pop(job_name, None)
        _COMPLETION_REGISTRY.pop(job_name, None)
        _KEY_MAP_REGISTRY.pop(job_name, None)

        # Collect produced states from registry
        produced_states_with_timestamps = _PRODUCED_STATES_REGISTRY.pop(job_name, [])

        # flink_thread is a daemon — it will be killed when this method returns.
        if flink_exc:
            raise flink_exc[0]

        # Return an AG with all produced states (sorted by timestamp)
        if produced_states_with_timestamps:
            # Sort by timestamp to maintain emission order
            produced_states_with_timestamps.sort(key=lambda x: x[0])

            # Extract just the states (without timestamps)
            produced_states = [state for _, state in produced_states_with_timestamps]

            # Get the target model from the function
            target_model = (
                fn.target_model if hasattr(fn, "target_model") else self.atype
            )

            result_ag = AGStream(
                atype=target_model,
                kafka_server=self.kafka_server,
                input_topic=self.output_topic,
                output_topic=self.output_topic,
                schema_registry_url=effective_registry_url,
                target_atype_name=self.target_atype_name,
            )
            result_ag.states = produced_states

            if verbose:
                sys.stderr.write(
                    f"\n✅ Listener completed. Collected {len(produced_states)} state(s) in emission order.\n"
                )
                sys.stderr.flush()

            return result_ag
        else:
            if verbose:
                sys.stderr.write(f"\n⚠️  Listener completed. No states were produced.\n")
                sys.stderr.flush()
            return None

    def transducible_function_listener_background(
        self,
        fn: Any,
        flink_startup_wait_s: float = 5.0,
        **kwargs,
    ) -> threading.Thread:
        """
        Start ``transducible_function_listener`` in a background daemon thread
        and return the thread so the caller can ``.join()`` it later.

        This is the recommended pattern when you want to produce messages
        **after** the listener is already running (so that ``latest-offset``
        captures them):

        .. code-block:: python

            t = ag.transducible_function_listener_background(fn=summarise_review, verbose=True)
            time.sleep(5)          # wait for Flink to start up
            producer.produce()
            t.join()               # wait for idle-timeout → listener stops
            # collect results …

        Args:
            fn: The transducible function to apply.
            flink_startup_wait_s: Seconds to sleep after starting the thread
                before returning, giving Flink time to initialise the Kafka
                source table (default: 5.0).
            **kwargs: All other keyword arguments are forwarded to
                ``transducible_function_listener``.

        Returns:
            The background ``threading.Thread`` running the listener.
        """
        exc_holder: list = []

        def _target():
            try:
                self.transducible_function_listener(fn=fn, **kwargs)
            except Exception as exc:
                exc_holder.append(exc)

        t = threading.Thread(target=_target, daemon=True)
        t.start()
        if flink_startup_wait_s > 0:
            time.sleep(flink_startup_wait_s)
        return t

    @classmethod
    def collect_by_key(cls, key: str, timeout_seconds: int = 30) -> "AGStream | None":
        """
        Collect a specific AGStream object from Kafka by its key.

        This method waits for a message with the specified key to arrive in Kafka.
        It's useful for request-response patterns where you send a message and wait for the result.

        Args:
            key: The message key to look for (UUID string)

            timeout_seconds: Maximum time to wait for the message in seconds (default: 30)

        Returns:
            AGStream object if found, None if timeout or not found

        Example:
            >>> from agentics.core.streaming import AGStream
            >>> from pydantic import BaseModel
            >>>
            >>> class Question(BaseModel):
            >>>     text: str
            >>>
            >>> # Send a question and get the key
            >>> question = AGStream(atype=Question)
            >>> question.states = [Question(text="What is AI?")]
            >>> key = question.stream(kafka_topic="questions-topic")
            >>>
            >>> # Wait for the response with that key
            >>> collector = AGStream(atype=Answer)
            >>> response = collector.collect_by_key(
            >>>     key=key,
            >>>     kafka_topic="answers-topic",
            >>>     timeout_seconds=60
            >>> )
            >>>
            >>> if response:
            >>>     print(f"Got answer: {response.states[0]}")
            >>> else:
            >>>     print("Timeout waiting for response")
        """
        import time as time_module
        import uuid

        from kafka import KafkaConsumer

        try:
            # Create consumer with unique group ID to read from beginning
            consumer = KafkaConsumer(
                self.input_topic,
                bootstrap_servers=_normalize_bootstrap_server(self.kafka_server),
                auto_offset_reset="earliest",
                enable_auto_commit=False,  # Don't commit offsets
                group_id=f"agstream-key-collector-{uuid.uuid4()}",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,  # Poll every second
            )

            sys.stderr.write(
                f"Waiting for message with key '{key}' on topic '{self.input_topic}' (timeout: {timeout_seconds}s)...\n"
            )
            sys.stderr.flush()

            start_time = time_module.time()
            messages_checked = 0

            while True:
                # Check if timeout exceeded
                elapsed = time_module.time() - start_time
                if elapsed > timeout_seconds:
                    sys.stderr.write(
                        f"\n✗ Timeout after {timeout_seconds}s. Checked {messages_checked} messages.\n"
                    )
                    sys.stderr.flush()
                    consumer.close()
                    return None

                # Poll for messages
                message_batch = consumer.poll(timeout_ms=1000)

                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        messages_checked += 1

                        # Decode the message key
                        message_key = (
                            message.key.decode("utf-8") if message.key else None
                        )

                        # Check if this is the key we're looking for
                        if message_key == key:
                            try:
                                # Deserialize the AGStream
                                ag = AGStream.deserialize(message.value)

                                sys.stderr.write(
                                    f"\n✓ Found message with key '{key}' after checking {messages_checked} messages ({elapsed:.1f}s)\n"
                                )
                                sys.stderr.flush()

                                consumer.close()
                                return ag

                            except Exception as e:
                                sys.stderr.write(
                                    f"\n✗ Error deserializing message with key '{key}': {e}\n"
                                )
                                sys.stderr.flush()
                                consumer.close()
                                return None

                        # Progress indicator every 100 messages
                        if messages_checked % 100 == 0:
                            sys.stderr.write(
                                f"  Checked {messages_checked} messages ({elapsed:.1f}s elapsed)...\n"
                            )
                            sys.stderr.flush()

        except Exception as e:
            sys.stderr.write(f"\n✗ Error collecting by key: {e}\n")
            sys.stderr.flush()
            return None

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize the Agentic instance to a dictionary that can be saved to JSON.

        Returns:
            Dict containing all serializable data including atype code, states, and configuration

        Example:
            >>> ag = AG(atype=MyType, states=[...])
            >>> serialized = ag.serialize()
            >>> with open('agentic.json', 'w') as f:
            ...     json.dump(serialized, f)
        """
        # Get the atype source code
        import inspect

        atype_code = None
        if self.atype:
            try:
                atype_code = inspect.getsource(self.atype)
            except (OSError, TypeError):
                # If we can't get source (e.g., dynamically created), store the schema
                atype_code = None

        # Serialize states to dictionaries
        serialized_states = [state.model_dump() for state in self.states]

        # Build the serialization dictionary
        serialized = {
            "atype_name": self.atype.__name__ if self.atype else None,
            "atype_code": atype_code,
            "atype_schema": self.atype.model_json_schema() if self.atype else None,
            "states": serialized_states,
            "transduce_fields": self.transduce_fields,
            "instructions": self.instructions,
            "transduction_type": self.transduction_type,
            "provide_explanations": self.provide_explanations,
            "explanations": (
                [exp.model_dump() for exp in self.explanations]
                if self.explanations
                else None
            ),
            "reasoning": self.reasoning,
            "max_iter": self.max_iter,
            "transient_pbar": self.transient_pbar,
            "transduction_logs_path": self.transduction_logs_path,
            "prompt_template": self.prompt_template,
            "transduction_timeout": self.transduction_timeout,
            "verbose_transduction": self.verbose_transduction,
            "verbose_agent": self.verbose_agent,
            "areduce_batch_size": self.areduce_batch_size,
            "amap_batch_size": self.amap_batch_size,
            "save_amap_batches_to_path": self.save_amap_batches_to_path,
            "crew_prompt_params": self.crew_prompt_params,
            "streaming_key": self.streaming_key,
            "target_atype_name": self.target_atype_name,
            "source_atype_name": self.source_atype_name,
        }

        return serialized

    @classmethod
    def deserialize(
        cls, data: Dict[str, Any], atype: Optional[Type[BaseModel]] = None
    ) -> AGStream:
        """
        Deserialize an Agentic instance from a dictionary.

        Args:
            data: Dictionary containing serialized Agentic data
            atype: Optional Pydantic model class. If not provided, will attempt to reconstruct from serialized data

        Returns:
            AGStream instance reconstructed from the serialized data

        Example:
            >>> with open('agentic.json', 'r') as f:
            ...     data = json.load(f)
            >>> ag = AGStream.deserialize(data)

            # Or provide the atype explicitly:
            >>> ag = AGStream.deserialize(data, atype=MyType)
        """
        # Reconstruct or use provided atype
        if atype is None:
            # Priority 1: Try to use atype_schema if available
            # if data.get("atype_schema"):
            #     try:
            #         atype = cls._create_model_from_schema(data["atype_schema"])
            #         sys.stderr.write(f"Created atype from schema: {atype.__name__}\n")
            #         sys.stderr.flush()
            #     except Exception as e:
            #         sys.stderr.write(f"Could not create atype from schema: {e}\n")
            #         sys.stderr.flush()
            #         atype = None

            # Priority 2: Try to reconstruct from source code
            if atype is None and data.get("atype_code"):
                try:
                    atype = import_pydantic_from_code(data["atype_code"])
                    # Removed verbose output: Created atype from code
                except Exception as e:
                    # Removed verbose output: Could not reconstruct atype from code
                    atype = None

            # Priority 3: Try to create from first state as sample
            if atype is None and data.get("states") and len(data["states"]) > 0:
                try:
                    atype = pydantic_model_from_dict(data["states"][0])
                    # Removed verbose output: Created atype from first state sample
                except Exception as e:
                    sys.stderr.write(f"Could not create atype from state sample: {e}\n")
                    sys.stderr.flush()
                    atype = None

            # Fallback: Create a minimal generic type
            if atype is None:
                sys.stderr.write(
                    "No atype information found. Creating minimal generic type.\n"
                )
                sys.stderr.flush()
                from pydantic import create_model

                atype = create_model("GenericType")

        # Reconstruct states
        states = []
        if data.get("states"):
            for state_dict in data["states"]:
                try:
                    states.append(atype(**state_dict))
                except (ValidationError, TypeError) as e:
                    sys.stderr.write(
                        f"Could not validate state with atype {atype.__name__}: {e}\n"
                    )
                    sys.stderr.flush()
                    # Try with make_all_fields_optional
                    try:
                        optional_atype = make_all_fields_optional(atype)
                        states.append(optional_atype(**state_dict))
                    except Exception as e2:
                        sys.stderr.write(
                            f"Could not create state with optional fields: {e2}. Keeping as dict.\n"
                        )
                        sys.stderr.flush()
                        # If all else fails, keep as dict
                        states.append(state_dict)

        # Reconstruct explanations if present
        explanations = None
        if data.get("explanations"):
            explanations = [Explanation(**exp) for exp in data["explanations"]]

        # Create the AG instance
        ag = cls(
            atype=atype,
            states=states,
            transduce_fields=data.get("transduce_fields"),
            instructions=data.get(
                "instructions",
                "Generate an object of the specified type from the following input.",
            ),
            transduction_type=data.get("transduction_type", "amap"),
            provide_explanations=data.get("provide_explanations", False),
            explanations=explanations,
            reasoning=data.get("reasoning"),
            max_iter=data.get("max_iter", 3),
            transient_pbar=data.get("transient_pbar", False),
            transduction_logs_path=data.get("transduction_logs_path"),
            prompt_template=data.get("prompt_template"),
            transduction_timeout=data.get("transduction_timeout", 300),
            verbose_transduction=data.get("verbose_transduction", True),
            verbose_agent=data.get("verbose_agent", False),
            areduce_batch_size=data.get("areduce_batch_size"),
            amap_batch_size=data.get("amap_batch_size", 20),
            save_amap_batches_to_path=data.get("save_amap_batches_to_path"),
            crew_prompt_params=data.get(
                "crew_prompt_params",
                {
                    "role": "Task Executor",
                    "goal": "You execute tasks",
                    "backstory": "You are always faithful and provide only fact based answers.",
                    "expected_output": "Described by Pydantic Type",
                },
            ),
            streaming_key=data.get("streaming_key"),
            target_atype_name=data.get("target_atype_name"),
            source_atype_name=data.get("source_atype_name"),
        )

        return ag

    def get_instructions_from_source(self, source: AGStream) -> AGStream:
        """
        Get the instructions from the source.

        When ``source.target_atype_name`` is set the method also attempts to
        fetch the corresponding Pydantic model from the schema registry and
        assign it to ``self.atype``, so that the copy is immediately usable
        with the correct target type without waiting for
        ``listen`` to perform the fetch at startup.

        Args:
            source:AGStream: The source to get the instructions from.
        Returns:
            AGStream: The instructions from the source.
        """
        self = super().get_instructions_from_source(source)
        if source.streaming_key:
            self.streaming_key = source.streaming_key
        if source.target_atype_name:
            self.target_atype_name = source.target_atype_name
            # Attempt to resolve the target atype from the registry so that
            # self.atype reflects the intended target type immediately.
            registry_url = source.schema_registry_url or self.schema_registry_url
            if registry_url:
                fetched = get_atype_from_registry(
                    atype_name=source.target_atype_name,
                    schema_registry_url=registry_url,
                )
                if fetched is not None:
                    self.atype = fetched
        if source.source_atype_name:
            self.source_atype_name = source.source_atype_name
        return self


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="AGStream: Stream, Listen, or Create Kafka topics"
    )
    parser.add_argument(
        "mode",
        choices=["stream", "listen", "create-topic"],
        help='Mode: "stream" to produce messages, "listen" to consume messages with Flink SQL, "create-topic" to create a new Kafka topic',
    )
    parser.add_argument(
        "--kafka-server",
        default="localhost:9092",
        help="Kafka server address (default: localhost:9092)",
    )
    parser.add_argument(
        "--input-topic",
        default="agentics-stream",
        help="Kafka input topic name (default: agentics-stream)",
    )
    parser.add_argument(
        "--output-topic",
        default="agentics-output",
        help="Kafka output topic name (default: agentics-output)",
    )

    parser.add_argument(
        "--target-type",
        default=None,
        help="Python code for the target class in a .py text file",
    )
    parser.add_argument(
        "--csv-path",
        default="/Users/gliozzo/Code/agentics911/agentics/tutorials/data/movies_small.csv",
        help="Path to CSV file for streaming mode",
    )
    parser.add_argument(
        "--limit", type=int, default=1, help="Number of records to process (default: 1)"
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=1,
        help="Number of partitions for create-topic mode (default: 1)",
    )
    parser.add_argument(
        "--replication-factor",
        type=int,
        default=1,
        help="Replication factor for create-topic mode (default: 1)",
    )

    args = parser.parse_args()

    if args.mode == "stream":
        # Stream mode: produce messages to Kafka
        movies = AGStream.from_csv(args.csv_path)
        movies.kafka_server = args.kafka_server
        movies.input_topic = args.input_topic
        movies.output_topic = args.output_topic

        movies.states = movies.states[: args.limit]
        movies.produce()
        print(f"✓ Streamed {len(movies.states)} records to {args.input_topic}")

    elif args.mode == "listen":
        # Listen mode: consume messages from Kafka using Flink SQL and write to output topic
        class Summary(BaseModel):
            text: str

        print(
            f"Listening for messages on Kafka topic: {args.input_topic} -> {args.output_topic} (using Flink SQL)"
        )
        target = AGStream(
            kafka_server=args.kafka_server,
            input_topic=args.input_topic,
            output_topic=args.output_topic,
        )
        if args.target_type:
            target.atype = import_pydantic_from_code(open(args.target_type).read())

        else:
            target = target.atype = Summary

        target.listen()

    elif args.mode == "create-topic":
        # Create topic mode: create a new Kafka topic
        create_kafka_topic(args.output_topic, args.kafka_server)
        print(f"Created topic: {args.output_topic}")
