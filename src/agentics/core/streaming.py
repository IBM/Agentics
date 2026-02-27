from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Type

from pydantic import BaseModel, Field

# Suppress Kafka consumer error logs
logging.getLogger("kafka.consumer.fetcher").setLevel(logging.CRITICAL)
logging.getLogger("kafka").setLevel(logging.WARNING)
import asyncio
import json
import os
import time
from typing import Any, Dict, List, Optional, Type

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from pydantic import BaseModel, ValidationError

from agentics import AG
from agentics.core.atype import (
    make_all_fields_optional,
    pydantic_model_from_dict,
)
from agentics.core.default_types import Explanation
from agentics.core.utils import (
    import_pydantic_from_code,
)

load_dotenv()

# PyFlink imports for the listener
from pyflink.datastream.functions import MapFunction


# Helper class for writing to Kafka output
# Serializer functions defined at module level to be picklable
def _key_serializer(k):
    """Serialize Kafka key to bytes"""
    return k.encode("utf-8") if k else b""


def _value_serializer(v):
    """Serialize Kafka value to bytes"""
    return v.encode("utf-8") if v else b"{}"


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
                bootstrap_servers=self.kafka_server,
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
        self._schema_cache = {}  # Cache for retrieved schemas

    def _get_schema_from_registry(
        self, subject: str, version: str = "latest"
    ) -> Optional[dict]:
        """
        Fetch schema from registry with caching.
        Reuses AGStream's schema registry functions.
        """
        cache_key = f"{subject}:{version}"

        # Check cache first
        if cache_key in self._schema_cache:
            return self._schema_cache[cache_key]

        try:
            import requests

            url = f"{self.schema_registry_url}/subjects/{subject}/versions/{version}"
            response = requests.get(url)

            if response.status_code == 200:
                result = response.json()
                schema_str = result.get("schema")
                if schema_str:
                    schema = json.loads(schema_str)
                    self._schema_cache[cache_key] = schema
                    return schema
            else:
                sys.stderr.write(
                    f"Failed to fetch schema: {response.status_code} - {response.text}\n"
                )
                sys.stderr.flush()
                return None

        except Exception as e:
            sys.stderr.write(f"Error fetching schema: {e}\n")
            sys.stderr.flush()
            return None

    def _get_atype_from_registry(
        self, topic: str, is_key: bool = False, version: str = "latest"
    ) -> Optional[Type[BaseModel]]:
        """
        Get atype from schema registry during PyFlink job execution.
        Reuses the same logic as AGStream.get_atype_from_registry.
        """
        # Generate subject name
        subject = self._get_subject_name(topic, is_key)

        # Get schema from registry
        schema = self._get_schema_from_registry(subject, version)
        if not schema:
            return None

        # Extract model name
        model_name = schema.get("title", "DynamicModel")

        # Create Pydantic model from schema
        try:
            import typing

            from pydantic import Field, create_model

            properties = schema.get("properties", {})
            required = schema.get("required", [])

            field_definitions = {}

            for field_name, field_schema in properties.items():
                field_type = self._json_type_to_python(field_schema)
                is_required = field_name in required

                # Handle optional fields
                if not is_required:
                    field_type = typing.Optional[field_type]

                # Create field with description if available
                field_info = {}
                if "description" in field_schema:
                    field_info["description"] = field_schema["description"]

                if is_required:
                    field_definitions[field_name] = (field_type, Field(**field_info))
                else:
                    field_definitions[field_name] = (field_type, None)

            # Create and return the model
            return create_model(model_name, **field_definitions)

        except Exception as e:
            sys.stderr.write(f"Error creating model from schema: {e}\n")
            sys.stderr.flush()
            return None

    def _get_subject_name(self, topic: str, is_key: bool = False) -> str:
        """
        Generate subject name for schema registry following Kafka conventions.
        Same implementation as in AGStream.
        """
        suffix = "key" if is_key else "value"
        return f"{topic}-{suffix}"

    def _json_type_to_python(self, field_schema: dict) -> type:
        """
        Convert JSON Schema type to Python type.
        Same implementation as in AGStream.
        """
        type_mapping = {
            "string": str,
            "integer": int,
            "number": float,
            "boolean": bool,
            "array": list,
            "object": dict,
        }

        json_type = field_schema.get("type")

        # Handle arrays with item types
        if json_type == "array" and "items" in field_schema:
            item_type = self._json_type_to_python(field_schema["items"])
            return typing.List[item_type]

        return type_mapping.get(json_type, str)

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


class AGStream(AG):
    model_config = {"arbitrary_types_allowed": True}
    streaming_key: Optional[str] = Field(
        None,
        description="Kafka streaming key, to be used when the object is sent to streaming transduction",
    )
    kafka_server: str = "localhost:9092"
    input_topic: str = "agentics-stream"
    output_topic: str = "agentics-output"
    schema_registry_url: str = "http://localhost:8081"  # Karapace Schema Registry URL

    @staticmethod
    def _json_type_to_python_static(field_schema: Dict[str, Any]) -> type:
        """Convert JSON Schema type to Python type (static version)."""
        json_type = field_schema.get("type", "string")

        type_mapping = {
            "string": str,
            "integer": int,
            "number": float,
            "boolean": bool,
            "array": list,
            "object": dict,
        }

        return type_mapping.get(json_type, str)

    @staticmethod
    def _create_pydantic_from_json_schema_static(
        json_schema: Dict[str, Any],
    ) -> Optional[Type[BaseModel]]:
        """
        Create a Pydantic model from JSON Schema (static version).

        This is a simplified implementation that handles basic types.
        For complex schemas, consider using a dedicated library.
        """
        try:
            from pydantic import create_model

            # Extract model name and properties
            model_name = json_schema.get("title", "DynamicModel")
            properties = json_schema.get("properties", {})
            required = json_schema.get("required", [])

            # Build field definitions
            field_definitions = {}
            for field_name, field_schema in properties.items():
                field_type = AGStream._json_type_to_python_static(field_schema)
                is_required = field_name in required

                if is_required:
                    field_definitions[field_name] = (field_type, ...)
                else:
                    field_definitions[field_name] = (Optional[field_type], None)

            # Create the model
            model = create_model(model_name, **field_definitions)
            return model

        except Exception as e:
            sys.stderr.write(f"✗ Error creating Pydantic model: {e}\n")
            sys.stderr.flush()
            return None

    @staticmethod
    def _get_subject_name_static(topic: str, is_key: bool = False) -> str:
        """
        Generate subject name for schema registry following Kafka conventions (static version).

        Args:
            topic: Kafka topic name
            is_key: If True, generates key subject name, otherwise value subject name

        Returns:
            Subject name string (e.g., "topic-name-value" or "topic-name-key")
        """
        suffix = "key" if is_key else "value"
        return f"{topic}-{suffix}"

    @staticmethod
    def get_atype_from_registry_static(
        topic: str,
        schema_registry_url: str = "http://localhost:8081",
        is_key: bool = False,
        version: str = "latest",
    ) -> Optional[Type[BaseModel]]:
        """
        Retrieve and reconstruct atype from Schema Registry (static method).

        This static method can be called without creating an AGStream instance.

        Args:
            topic: Kafka topic name
            schema_registry_url: URL of the schema registry
            is_key: If True, retrieves key schema, otherwise value schema
            version: Schema version ("latest" or specific version number)

        Returns:
            Pydantic BaseModel class if successful, None on error

        Example:
            >>> from agentics.core.streaming import AGStream
            >>>
            >>> # Retrieve schema from registry without creating an instance
            >>> Question = AGStream.get_atype_from_registry_static(
            ...     topic="questions",
            ...     schema_registry_url="http://localhost:8081"
            ... )
            >>>
            >>> if Question:
            ...     # Use the retrieved type
            ...     ag = AGStream(atype=Question, input_topic="questions")
        """
        subject = AGStream._get_subject_name_static(topic, is_key)

        try:
            # Fetch schema from registry
            url = f"{schema_registry_url}/subjects/{subject}/versions/{version}"
            response = requests.get(url)

            if response.status_code != 200:
                sys.stderr.write(
                    f"✗ Failed to fetch schema: {response.status_code} - {response.text}\n"
                )
                sys.stderr.flush()
                return None

            result = response.json()
            schema_str = result.get("schema")
            schema_id = result.get("id")

            if not schema_str:
                sys.stderr.write("✗ No schema found in response\n")
                sys.stderr.flush()
                return None

            # Parse JSON Schema
            json_schema = json.loads(schema_str)

            # Create Pydantic model from JSON Schema
            atype = AGStream._create_pydantic_from_json_schema_static(json_schema)

            if atype:
                sys.stderr.write(
                    f"✓ Retrieved schema '{subject}' (ID: {schema_id}, version: {version})\n"
                )
                sys.stderr.flush()
                return atype
            else:
                sys.stderr.write("✗ Failed to create Pydantic model from schema\n")
                sys.stderr.flush()
                return None

        except Exception as e:
            sys.stderr.write(f"✗ Error retrieving schema: {e}\n")
            sys.stderr.flush()
            return None

    def _get_subject_name(self, topic: str, is_key: bool = False) -> str:
        """
        Generate subject name for schema registry following Kafka conventions.

        Args:
            topic: Kafka topic name
            is_key: If True, generates key subject name, otherwise value subject name

        Returns:
            Subject name string (e.g., "topic-name-value" or "topic-name-key")
        """
        return self._get_subject_name_static(topic, is_key)

    def register_atype_schema(
        self,
        topic: Optional[str] = None,
        is_key: bool = False,
        compatibility: str = "BACKWARD",
    ) -> Optional[int]:
        """
        Register the atype's JSON Schema in Karapace Schema Registry.

        This method converts the Pydantic model to JSON Schema and registers it
        with the schema registry, enabling schema evolution and validation.

        Args:
            topic: Kafka topic name (uses self.input_topic if not provided)
            is_key: If True, registers as key schema, otherwise as value schema
            compatibility: Compatibility mode (BACKWARD, FORWARD, FULL, NONE)

        Returns:
            Schema ID if successful, None on error

        Example:
            >>> from pydantic import BaseModel
            >>> from agentics.core.streaming import AGStream
            >>>
            >>> class Question(BaseModel):
            >>>     text: str
            >>>     category: str
            >>>
            >>> ag = AGStream(atype=Question, input_topic="questions")
            >>> schema_id = ag.register_atype_schema()
            >>> print(f"Registered schema with ID: {schema_id}")
        """
        if not self.atype:
            sys.stderr.write("✗ No atype defined, cannot register schema\n")
            sys.stderr.flush()
            return None

        topic = topic or self.input_topic
        subject = self._get_subject_name(topic, is_key)

        try:
            # Get JSON Schema from Pydantic model
            json_schema = self.atype.model_json_schema()

            # Wrap in schema registry format
            schema_data = {"schemaType": "JSON", "schema": json.dumps(json_schema)}

            # Register schema
            url = f"{self.schema_registry_url}/subjects/{subject}/versions"
            response = requests.post(
                url,
                json=schema_data,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            )

            if response.status_code in [200, 201]:
                result = response.json()
                schema_id = result.get("id")
                sys.stderr.write(
                    f"✓ Registered schema for '{subject}' with ID: {schema_id}\n"
                )
                sys.stderr.flush()

                # Set compatibility mode if specified
                if compatibility:
                    self._set_compatibility(subject, compatibility)

                return schema_id
            else:
                sys.stderr.write(
                    f"✗ Failed to register schema: {response.status_code} - {response.text}\n"
                )
                sys.stderr.flush()
                return None

        except Exception as e:
            sys.stderr.write(f"✗ Error registering schema: {e}\n")
            sys.stderr.flush()
            return None

    def _set_compatibility(self, subject: str, compatibility: str) -> bool:
        """Set compatibility mode for a subject."""
        try:
            url = f"{self.schema_registry_url}/config/{subject}"
            response = requests.put(
                url,
                json={"compatibility": compatibility},
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            )

            if response.status_code == 200:
                sys.stderr.write(
                    f"✓ Set compatibility mode to {compatibility} for '{subject}'\n"
                )
                sys.stderr.flush()
                return True
            return False
        except Exception:
            return False

    def get_atype_from_registry(
        self, topic: Optional[str] = None, is_key: bool = False, version: str = "latest"
    ) -> Optional[Type[BaseModel]]:
        """
        Retrieve and reconstruct atype from Schema Registry.

        This method fetches the JSON Schema from the registry and dynamically
        creates a Pydantic model from it.

        Args:
            topic: Kafka topic name (uses self.input_topic if not provided)
            is_key: If True, retrieves key schema, otherwise value schema
            version: Schema version ("latest" or specific version number)

        Returns:
            Pydantic BaseModel class if successful, None on error

        Example:
            >>> from agentics.core.streaming import AGStream
            >>>
            >>> # Retrieve schema from registry
            >>> ag = AGStream(input_topic="questions")
            >>> Question = ag.get_atype_from_registry()
            >>>
            >>> if Question:
            >>>     # Use the retrieved type
            >>>     ag.atype = Question
            >>>     questions = ag.collect_sources(max_messages=10)
        """
        topic = topic or self.input_topic
        subject = self._get_subject_name(topic, is_key)

        try:
            # Fetch schema from registry
            url = f"{self.schema_registry_url}/subjects/{subject}/versions/{version}"
            response = requests.get(url)

            if response.status_code != 200:
                sys.stderr.write(
                    f"✗ Failed to fetch schema: {response.status_code} - {response.text}\n"
                )
                sys.stderr.flush()
                return None

            result = response.json()
            schema_str = result.get("schema")
            schema_id = result.get("id")

            if not schema_str:
                sys.stderr.write("✗ No schema found in response\n")
                sys.stderr.flush()
                return None

            # Parse JSON Schema
            json_schema = json.loads(schema_str)

            # Create Pydantic model from JSON Schema
            atype = self._create_pydantic_from_json_schema(json_schema)

            if atype:
                sys.stderr.write(
                    f"✓ Retrieved schema '{subject}' (ID: {schema_id}, version: {version})\n"
                )
                sys.stderr.flush()
                return atype
            else:
                sys.stderr.write("✗ Failed to create Pydantic model from schema\n")
                sys.stderr.flush()
                return None

        except Exception as e:
            sys.stderr.write(f"✗ Error retrieving schema: {e}\n")
            sys.stderr.flush()
            return None

    def _create_pydantic_from_json_schema(
        self, json_schema: Dict[str, Any]
    ) -> Optional[Type[BaseModel]]:
        """
        Create a Pydantic model from JSON Schema.

        This is a simplified implementation that handles basic types.
        For complex schemas, consider using a dedicated library.
        """
        try:
            from pydantic import create_model

            # Extract model name and properties
            model_name = json_schema.get("title", "DynamicModel")
            properties = json_schema.get("properties", {})
            required = json_schema.get("required", [])

            # Build field definitions
            field_definitions = {}
            for field_name, field_schema in properties.items():
                field_type = self._json_type_to_python(field_schema)
                is_required = field_name in required

                if is_required:
                    field_definitions[field_name] = (field_type, ...)
                else:
                    field_definitions[field_name] = (Optional[field_type], None)

            # Create the model
            model = create_model(model_name, **field_definitions)
            return model

        except Exception as e:
            sys.stderr.write(f"✗ Error creating Pydantic model: {e}\n")
            sys.stderr.flush()
            return None

    def _json_type_to_python(self, field_schema: Dict[str, Any]) -> type:
        """Convert JSON Schema type to Python type."""
        json_type = field_schema.get("type", "string")

        type_mapping = {
            "string": str,
            "integer": int,
            "number": float,
            "boolean": bool,
            "array": list,
            "object": dict,
        }

        return type_mapping.get(json_type, str)

    def list_registered_schemas(
        self, topic: Optional[str] = None, is_key: bool = False
    ) -> Optional[List[int]]:
        """
        List all versions of registered schemas for a topic.

        Args:
            topic: Kafka topic name (uses self.input_topic if not provided)
            is_key: If True, lists key schemas, otherwise value schemas

        Returns:
            List of version numbers if successful, None on error

        Example:
            >>> ag = AGStream(input_topic="questions")
            >>> versions = ag.list_registered_schemas()
            >>> print(f"Available versions: {versions}")
        """
        topic = topic or self.input_topic
        subject = self._get_subject_name(topic, is_key)

        try:
            url = f"{self.schema_registry_url}/subjects/{subject}/versions"
            response = requests.get(url)

            if response.status_code == 200:
                versions = response.json()
                sys.stderr.write(
                    f"✓ Found {len(versions)} version(s) for '{subject}': {versions}\n"
                )
                sys.stderr.flush()
                return versions
            elif response.status_code == 404:
                sys.stderr.write(f"✗ No schemas found for '{subject}'\n")
                sys.stderr.flush()
                return []
            else:
                sys.stderr.write(
                    f"✗ Failed to list schemas: {response.status_code} - {response.text}\n"
                )
                sys.stderr.flush()
                return None

        except Exception as e:
            sys.stderr.write(f"✗ Error listing schemas: {e}\n")
            sys.stderr.flush()
            return None

    def listen(self: AGStream, atype: Optional[Type[BaseModel]] = None):
        """
        Start AGStream listener using Flink SQL to query Kafka with key, timestamp, and value columns.
        Processes messages and writes results back to an output Kafka topic.
        """
        from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
        from pyflink.table import EnvironmentSettings, StreamTableEnvironment

        # Create execution environment
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        env.set_parallelism(4)

        # Create Table Environment
        settings = EnvironmentSettings.in_streaming_mode()
        table_env = StreamTableEnvironment.create(env, settings)

        # Add Kafka connector JAR
        table_env.get_config().get_configuration().set_string(
            "pipeline.jars",
            "file:///Users/gliozzo/Code/flink_tutorial/flink-sql-connector-kafka-3.3.0-1.20.jar",
        )

        # Create Kafka table - 'raw' format only supports single physical column
        # Note: Kafka keys cannot be read as metadata in Flink SQL
        # Keys will be generated from partition-offset to ensure consistency
        create_table_ddl = f"""
            CREATE TABLE kafka_source (
                `value` STRING,
                `event_timestamp` TIMESTAMP(3) METADATA FROM 'timestamp',
                `kafka_partition` INT METADATA FROM 'partition',
                `kafka_offset` BIGINT METADATA FROM 'offset',
                WATERMARK FOR `event_timestamp` AS `event_timestamp` - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{self.input_topic}',
                'properties.bootstrap.servers' = '{self.kafka_server}',
                'properties.group.id' = 'flink-sql-consumer-group',
                'scan.startup.mode' = 'latest-offset',
                'format' = 'raw'
            )
        """

        # Removed verbose output: Creating Kafka source table
        table_env.execute_sql(create_table_ddl)

        # Query to generate consistent keys from partition-offset
        # This ensures the same message always gets the same key
        # Removed verbose output: Executing SQL query

        ## Generate deterministic key to read the
        result_table = table_env.sql_query(
            """
            SELECT
                CONCAT('partition-', CAST(kafka_partition AS STRING), '-offset-', CAST(kafka_offset AS STRING)) as key,
                UNIX_TIMESTAMP(CAST(event_timestamp AS STRING)) * 1000 as timestamp_ms,
                `value`
            FROM kafka_source
        """
        )

        # Convert to DataStream - each row has (key, timestamp_ms, value)
        ds = table_env.to_data_stream(result_table)

        # Apply processing using MapFunction class
        # Pass both target atype (self.atype) and source atype (atype parameter)
        processed_stream = ds.map(func=ProcessSQLRow(self.atype, atype))

        # Filter out None values
        filtered_stream = processed_stream.filter(lambda x: x is not None)

        # Write output back to Kafka using MapFunction class
        output_stream = filtered_stream.map(
            WriteToKafkaOutput(self.kafka_server, self.output_topic)
        )
        output_stream.print()

        # Removed verbose output: Starting AGStream SQL listener

        # Execute the job
        env.execute("PyFlink SQL Kafka Consumer")

    def produce(self) -> str | None:
        """
        Stream the AG state to Kafka with a unique identifier as the key and timestamp
        """

        try:
            import uuid

            producer: KafkaProducer = KafkaProducer(
                bootstrap_servers=self.kafka_server,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )

            # Generate unique identifier for this transduction
            transduction_id = str(uuid.uuid4())

            # Get current timestamp in milliseconds (Kafka standard)
            timestamp_ms = int(time.time() * 1000)

            serialized = self.serialize()
            producer.send(
                topic=self.input_topic,
                key=transduction_id,
                value=serialized,
                timestamp_ms=timestamp_ms,
            )
            # Removed: print(serialized) - too verbose
            producer.flush()
            producer.close()

            # Removed: print(f"✓ Sent to Kafka topic...") - too verbose
            return transduction_id
        except Exception as e:
            # Silently fail - errors will be caught by caller if needed
            return None

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
        verbose: bool = False,
    ) -> list["AGStream"]:
        """
        Collect AGStream objects from a Kafka topic with flexible collection modes.

        This method reads messages from Kafka and deserializes them into AGStream objects.
        It uses the simple Kafka consumer (not PyFlink) for straightforward collection.

        Args:
            max_messages: Maximum number of messages to collect (default: 100)
            timeout_ms: Timeout in milliseconds for polling (default: 5000)
            mode: Collection mode - one of:
                - 'all': Collect all messages from the beginning (default)
                - 'latest': Collect only new messages (from latest offset)
                - 'timestamp': Collect messages from a specific timestamp (requires from_timestamp)
            from_timestamp: Unix timestamp in milliseconds to start collecting from (only used with mode='timestamp')
            group_id: Consumer group ID. If None, a unique ID is generated for each call.
                     Use a persistent group_id to track offset across multiple calls.
            verbose: If True, print detailed collection progress (default: False)

        Returns:
            List of AGStream objects deserialized from Kafka messages

        Examples:
            >>> from agentics.core.streaming import AGStream
            >>> from pydantic import BaseModel
            >>> import time
            >>>
            >>> class Question(BaseModel):
            >>>     text: str
            >>>
            >>> # Collect all messages from beginning
            >>> collector = AGStream(atype=Question, input_topic="questions-topic")
            >>> all_questions = collector.collect(mode='all', max_messages=10)
            >>>
            >>> # Collect only latest/new messages
            >>> new_questions = collector.collect(mode='latest', max_messages=1, timeout_ms=10000)
            >>>
            >>> # Collect messages from last hour
            >>> one_hour_ago = int((time.time() - 3600) * 1000)
            >>> recent_questions = collector.collect(mode='timestamp', from_timestamp=one_hour_ago)
            >>>
            >>> # Use persistent group ID to track offset across calls
            >>> session_id = "my-session-123"
            >>> q1 = collector.collect(mode='latest', group_id=session_id, max_messages=1)
            >>> q2 = collector.collect(mode='latest', group_id=session_id, max_messages=1)  # Gets next message
        """
        import uuid

        from kafka import KafkaConsumer, TopicPartition

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
                auto_offset_reset = "earliest"  # Will be overridden by seek
            else:
                raise ValueError(
                    f"Invalid mode '{mode}'. Must be 'all', 'latest', or 'timestamp'"
                )

            # Create consumer with improved connection settings
            # Force IPv4 by using 127.0.0.1 instead of localhost if needed
            bootstrap_server = self.kafka_server
            if "localhost" in bootstrap_server:
                bootstrap_server = bootstrap_server.replace("localhost", "127.0.0.1")

            # For timestamp mode, create consumer WITHOUT subscribing (we'll assign manually)
            # For other modes, subscribe to the topic
            if mode == "timestamp":
                consumer = KafkaConsumer(
                    bootstrap_servers=bootstrap_server,
                    auto_offset_reset=auto_offset_reset,
                    enable_auto_commit=True,
                    group_id=group_id,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=timeout_ms,
                    request_timeout_ms=40000,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=3000,
                    max_poll_interval_ms=300000,
                    connections_max_idle_ms=540000,
                    fetch_max_wait_ms=500,
                    api_version_auto_timeout_ms=10000,
                    retry_backoff_ms=100,
                    metadata_max_age_ms=300000,
                    security_protocol="PLAINTEXT",
                    client_id=f"{group_id}-client",
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
                    max_poll_interval_ms=300000,
                    connections_max_idle_ms=540000,
                    fetch_max_wait_ms=500,
                    api_version_auto_timeout_ms=10000,
                    retry_backoff_ms=100,
                    metadata_max_age_ms=300000,
                    security_protocol="PLAINTEXT",
                    client_id=f"{group_id}-client",
                )

            if verbose:
                sys.stderr.write(
                    f"Created consumer with bootstrap_servers={bootstrap_server}, group_id={group_id}\n"
                )
                sys.stderr.flush()

            # Give consumer time to establish connection and fetch metadata
            import time as time_module

            time_module.sleep(0.5)

            # If timestamp mode, manually assign partitions and seek
            if mode == "timestamp":
                partitions = consumer.partitions_for_topic(self.input_topic)
                if partitions:
                    topic_partitions = [
                        TopicPartition(self.input_topic, p) for p in partitions
                    ]
                    consumer.assign(topic_partitions)

                    # Create timestamp dict for all partitions
                    timestamp_dict = {tp: from_timestamp for tp in topic_partitions}

                    # Get offsets for timestamp
                    offsets = consumer.offsets_for_times(timestamp_dict)

                    # Seek to the offsets
                    for tp, offset_and_timestamp in offsets.items():
                        if offset_and_timestamp is not None:
                            consumer.seek(tp, offset_and_timestamp.offset)
                            if verbose:
                                sys.stderr.write(
                                    f"Seeking partition {tp.partition} to offset {offset_and_timestamp.offset} (timestamp: {from_timestamp})\n"
                                )
                                sys.stderr.flush()
            else:
                # For non-timestamp modes, trigger partition assignment by polling
                if verbose:
                    sys.stderr.write(f"Triggering partition assignment...\n")
                    sys.stderr.flush()
                consumer.poll(timeout_ms=100, max_records=1)
                time_module.sleep(0.2)

            collected_ags = []
            message_count = 0

            if verbose:
                mode_desc = f"mode={mode}"
                if mode == "timestamp" and from_timestamp:
                    mode_desc += f", from_timestamp={from_timestamp}"
                sys.stderr.write(
                    f"Collecting AGStream objects from topic '{self.input_topic}' ({mode_desc})...\n"
                )
                sys.stderr.flush()

            # Log consumer assignment info after initial poll
            if verbose:
                try:
                    assignment = consumer.assignment()
                    sys.stderr.write(f"Consumer assigned to partitions: {assignment}\n")
                    if not assignment:
                        sys.stderr.write(
                            f"⚠ WARNING: No partitions assigned! Waiting for rebalance...\n"
                        )
                        sys.stderr.flush()
                        # Wait a bit more for rebalance
                        time_module.sleep(1.0)
                        consumer.poll(timeout_ms=100, max_records=1)
                        assignment = consumer.assignment()
                        sys.stderr.write(
                            f"After rebalance, assigned to: {assignment}\n"
                        )
                    sys.stderr.flush()
                except Exception as e:
                    sys.stderr.write(f"Could not get consumer assignment: {e}\n")
                    sys.stderr.flush()

            try:
                # Use poll() instead of iterator for better error handling
                poll_count = 0
                max_empty_polls = 20  # Allow up to 20 empty polls before giving up
                empty_poll_count = 0

                while (
                    message_count < max_messages and empty_poll_count < max_empty_polls
                ):
                    try:
                        # Poll for messages with a shorter timeout
                        message_batch = consumer.poll(timeout_ms=500, max_records=10)
                        poll_count += 1

                        if not message_batch:
                            empty_poll_count += 1
                            if verbose and empty_poll_count % 5 == 0:
                                sys.stderr.write(
                                    f"⏳ Waiting for messages... (poll #{poll_count}, {empty_poll_count} empty polls)\n"
                                )
                                sys.stderr.flush()
                            continue

                        # Reset empty poll counter when we get messages
                        empty_poll_count = 0

                        # Process messages from all partitions
                        for topic_partition, messages in message_batch.items():
                            for message in messages:
                                try:
                                    # Deserialize the AGStream from the message value
                                    ag = AGStream.deserialize(message.value)
                                    collected_ags.append(ag)
                                    message_count += 1

                                    if verbose:
                                        sys.stderr.write(
                                            f"✓ Collected message {message_count}/{max_messages} (key: {message.key.decode('utf-8') if message.key else 'None'}, timestamp: {message.timestamp})\n"
                                        )
                                        sys.stderr.flush()

                                    if message_count >= max_messages:
                                        break

                                except Exception as e:
                                    if verbose:
                                        sys.stderr.write(
                                            f"✗ Error deserializing message: {e}\n"
                                        )
                                        sys.stderr.flush()
                                    continue

                            if message_count >= max_messages:
                                break

                    except Exception as poll_error:
                        if verbose:
                            sys.stderr.write(f"✗ Error during poll: {poll_error}\n")
                            sys.stderr.write(
                                f"   Error type: {type(poll_error).__name__}\n"
                            )
                            sys.stderr.flush()
                        # Continue trying to poll
                        empty_poll_count += 1

            except KeyboardInterrupt:
                if verbose:
                    sys.stderr.write("\n⚠ Collection interrupted by user\n")
                    sys.stderr.flush()
            except Exception as fetch_error:
                if verbose:
                    sys.stderr.write(f"✗ Error during message fetch: {fetch_error}\n")
                    sys.stderr.write(f"   Error type: {type(fetch_error).__name__}\n")
                import traceback

                traceback.print_exc(file=sys.stderr)
                sys.stderr.flush()
            finally:
                try:
                    consumer.close()
                except Exception as close_error:
                    if verbose:
                        sys.stderr.write(f"⚠ Error closing consumer: {close_error}\n")
                        sys.stderr.flush()

            if verbose:
                sys.stderr.write(
                    f"\n✓ Collected {len(collected_ags)} AGStream objects from Kafka\n"
                )
                sys.stderr.flush()

            return collected_ags

        except Exception as e:
            if verbose:
                sys.stderr.write(f"✗ Error collecting from Kafka: {e}\n")
                sys.stderr.flush()
            return []

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
                bootstrap_servers=self.kafka_server,
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

    @staticmethod
    def create_topic(
        topic_name: str,
        kafka_server: str = "localhost:9092",
        num_partitions: int = 1,
        replication_factor: int = 1,
    ) -> bool:
        """
        Create a new Kafka topic.

        This is a static method that creates a Kafka topic with the specified configuration.
        It handles the case where the topic already exists gracefully.

        Args:
            topic_name: Name of the topic to create
            kafka_server: Kafka server address (default: "localhost:9092")
            num_partitions: Number of partitions for the topic (default: 1)
            replication_factor: Replication factor for the topic (default: 1)

        Returns:
            True if topic was created successfully or already exists, False on error

        Example:
            >>> from agentics.core.streaming import AGStream
            >>>
            >>> # Create a new topic for questions
            >>> success = AGStream.create_topic(
            >>>     topic_name="questions-topic",
            >>>     num_partitions=3,
            >>>     replication_factor=1
            >>> )
            >>>
            >>> if success:
            >>>     print("Topic ready for use!")
        """
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=kafka_server, client_id="agstream-topic-creator"
            )

            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )

            try:
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                sys.stderr.write(f"✓ Topic '{topic_name}' created successfully!\n")
                sys.stderr.write(f"  - Partitions: {num_partitions}\n")
                sys.stderr.write(f"  - Replication Factor: {replication_factor}\n")
                sys.stderr.flush()
                return True

            except TopicAlreadyExistsError:
                sys.stderr.write(f"✓ Topic '{topic_name}' already exists!\n")
                sys.stderr.flush()
                return True

            except Exception as e:
                sys.stderr.write(f"✗ Error creating topic '{topic_name}': {e}\n")
                sys.stderr.flush()
                return False

            finally:
                admin_client.close()

        except Exception as e:
            sys.stderr.write(f"✗ Error connecting to Kafka admin: {e}\n")
            sys.stderr.flush()
            return False

    @staticmethod
    def topic_exists(topic_name: str, kafka_server: str = "localhost:9092") -> bool:
        """
        Check if a Kafka topic exists.

        This is a static method that queries the Kafka cluster to determine
        if a topic with the given name exists.

        Args:
            topic_name: Name of the topic to check
            kafka_server: Kafka server address (default: "localhost:9092")

        Returns:
            True if the topic exists, False otherwise

        Example:
            >>> from agentics.core.streaming import AGStream
            >>>
            >>> # Check if a topic exists before using it
            >>> if AGStream.topic_exists("questions-topic"):
            >>>     print("Topic exists, ready to use!")
            >>> else:
            >>>     print("Topic doesn't exist, creating it...")
            >>>     AGStream.create_topic("questions-topic")
        """
        from kafka.admin import KafkaAdminClient

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=kafka_server, client_id="agstream-topic-checker"
            )

            try:
                # Get list of all topics
                topics = admin_client.list_topics()
                exists = topic_name in topics

                if exists:
                    sys.stderr.write(f"✓ Topic '{topic_name}' exists\n")
                else:
                    sys.stderr.write(f"✗ Topic '{topic_name}' does not exist\n")
                sys.stderr.flush()

                return exists

            except Exception as e:
                sys.stderr.write(f"✗ Error checking topic '{topic_name}': {e}\n")
                sys.stderr.flush()
                return False

            finally:
                admin_client.close()

        except Exception as e:
            sys.stderr.write(f"✗ Error connecting to Kafka admin: {e}\n")
            sys.stderr.flush()
            return False
            sys.stderr.write(f"✗ Error connecting to Kafka admin: {e}\n")
            sys.stderr.flush()
            return False

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
        )

        return ag

    def get_instructions_from_source(self, source: AGStream) -> AGStream:
        """
        Get the instructions from the source.

        Args:
            source:AGStream: The source to get the instructions from.
        Returns:
            AG: The instructions from the source.
        """
        self = super().get_instructions_from_source(source)
        if source.streaming_key:
            self.streaming_key = source.streaming_key
        return self


from agentics.core.utils import import_pydantic_from_code

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
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        AGstream.create_topic(args.kafka_server, args.output_topic)
        print(f"Created topic: {args.output_topic}")
