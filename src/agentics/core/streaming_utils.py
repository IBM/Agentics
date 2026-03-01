"""
Streaming utilities for Agentics PyFlink integration.

This module provides utility functions for working with the Karapace Schema
Registry, Pydantic model construction from JSON Schema, and Kafka topic
administration.  These functions are used by ``AGStream`` (``streaming.py``)
and can also be called directly from external code.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any, Dict, List, Optional, Type, Union

import requests
from dotenv import load_dotenv
from pydantic import BaseModel, create_model

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# JSON Schema → Python type helpers
# ---------------------------------------------------------------------------


def json_type_to_python(field_schema: Dict[str, Any]) -> Any:
    """
    Convert a JSON Schema field definition to a Python type.

    Handles simple types (string, integer, number, boolean, array, object)
    and ``anyOf`` unions (e.g. optional fields).

    Args:
        field_schema: A single JSON Schema property dict, e.g.
            ``{"type": "string"}`` or ``{"anyOf": [{"type": "string"}, {"type": "null"}]}``.

    Returns:
        A Python type (``str``, ``int``, ``float``, ``bool``, ``list``,
        ``dict``, or a ``Union`` for anyOf fields).  Defaults to ``str``
        for unknown types.
    """
    if "anyOf" in field_schema:
        types = []
        for type_def in field_schema["anyOf"]:
            if type_def.get("type") == "string":
                types.append(str)
            elif type_def.get("type") == "integer":
                types.append(int)
            elif type_def.get("type") == "number":
                types.append(float)
            elif type_def.get("type") == "boolean":
                types.append(bool)
            elif type_def.get("type") == "array":
                types.append(list)
            elif type_def.get("type") == "object":
                types.append(dict)
            elif type_def.get("type") == "null":
                types.append(type(None))
        if len(types) > 1:
            return Union[tuple(types)]
        elif len(types) == 1:
            return types[0]
        return str

    type_mapping: Dict[str, type] = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": bool,
        "array": list,
        "object": dict,
    }
    json_type = field_schema.get("type", "string")
    return type_mapping.get(json_type, str)


def create_pydantic_from_json_schema(
    json_schema: Dict[str, Any],
) -> Optional[Type[BaseModel]]:
    """
    Dynamically create a Pydantic ``BaseModel`` from a JSON Schema dict.

    Handles required vs optional fields.  For complex nested schemas consider
    using a dedicated library such as ``datamodel-code-generator``.

    Args:
        json_schema: A JSON Schema dict (as returned by
            ``BaseModel.model_json_schema()``).

    Returns:
        A dynamically created Pydantic model class, or ``None`` on error.
    """
    try:
        model_name = json_schema.get("title", "DynamicModel")
        properties = json_schema.get("properties", {})
        required = json_schema.get("required", [])

        field_definitions: Dict[str, Any] = {}
        for field_name, field_schema in properties.items():
            field_type = json_type_to_python(field_schema)
            if field_name in required:
                field_definitions[field_name] = (field_type, ...)
            else:
                field_definitions[field_name] = (Optional[field_type], None)

        return create_model(model_name, **field_definitions)

    except Exception as e:
        sys.stderr.write(f"✗ Error creating Pydantic model from JSON schema: {e}\n")
        sys.stderr.flush()
        return None


# ---------------------------------------------------------------------------
# Schema Registry subject-name helpers
# ---------------------------------------------------------------------------


def get_subject_name(
    atype_name: str,
    is_key: bool = False,
    add_suffix: bool = True,
) -> str:
    """
    Build the Karapace / Confluent Schema Registry subject name for a type.

    Args:
        atype_name: Pydantic model class name (e.g. ``"UserProfile"``).
        is_key: ``True`` for the key schema, ``False`` (default) for value.
        add_suffix: When ``True`` (default) appends ``-value`` or ``-key``.
            Pass ``False`` to use ``atype_name`` verbatim.

    Returns:
        Subject name string, e.g. ``"UserProfile-value"``.
    """
    if not add_suffix:
        return atype_name
    suffix = "key" if is_key else "value"
    return f"{atype_name}-{suffix}"


# ---------------------------------------------------------------------------
# Schema Registry fetch helpers
# ---------------------------------------------------------------------------


def get_atype_from_registry(
    atype_name: str,
    schema_registry_url: str = "http://localhost:8081",
    is_key: bool = False,
    version: str = "latest",
    add_suffix: bool = True,
) -> Optional[Type[BaseModel]]:
    """
    Fetch a schema from the registry and return a Pydantic model class.

    Args:
        atype_name: Pydantic model class name (e.g. ``"UserProfile"``).
        schema_registry_url: Base URL of the schema registry.
        is_key: ``True`` for the key schema, ``False`` (default) for value.
        version: Schema version string (``"latest"`` or a version number).
        add_suffix: Passed to :func:`get_subject_name`.

    Returns:
        A dynamically created Pydantic model class, or ``None`` on error.

    Examples:
        >>> UserProfile = get_atype_from_registry(
        ...     "UserProfile", schema_registry_url="http://localhost:8081"
        ... )
    """
    subject = get_subject_name(atype_name, is_key, add_suffix)

    try:
        url = f"{schema_registry_url.rstrip('/')}/subjects/{subject}/versions/{version}"
        response = requests.get(url, timeout=10)

        if response.status_code != 200:
            sys.stderr.write(
                f"✗ Failed to fetch schema '{subject}': "
                f"{response.status_code} - {response.text}\n"
            )
            sys.stderr.flush()
            return None

        result = response.json()
        schema_str = result.get("schema")
        schema_id = result.get("id")

        if not schema_str:
            sys.stderr.write(f"✗ No schema string in response for '{subject}'\n")
            sys.stderr.flush()
            return None

        json_schema = json.loads(schema_str)
        atype = create_pydantic_from_json_schema(json_schema)

        if atype:
            sys.stderr.write(
                f"✓ Retrieved schema '{subject}' (ID: {schema_id}, version: {version})\n"
            )
            sys.stderr.flush()
            return atype

        sys.stderr.write(f"✗ Failed to build Pydantic model for '{subject}'\n")
        sys.stderr.flush()
        return None

    except Exception as e:
        sys.stderr.write(f"✗ Error retrieving schema '{subject}': {e}\n")
        sys.stderr.flush()
        return None


def get_schema_info(
    atype_name: str,
    schema_registry_url: str = "http://localhost:8081",
    is_key: bool = False,
    version: str = "latest",
    add_suffix: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Fetch raw schema metadata from the registry without building a model.

    Args:
        atype_name: Pydantic model class name (e.g. ``"UserProfile"``).
        schema_registry_url: Base URL of the schema registry.
        is_key: ``True`` for the key schema, ``False`` (default) for value.
        version: Schema version string (``"latest"`` or a version number).
        add_suffix: Passed to :func:`get_subject_name`.

    Returns:
        A dict with keys ``id``, ``version``, ``schema`` (parsed JSON Schema
        dict), and ``subject``.  Returns ``None`` on error.

    Examples:
        >>> info = get_schema_info("UserProfile")
        >>> if info:
        ...     print(info["id"], info["version"], info["subject"])
    """
    subject = get_subject_name(atype_name, is_key, add_suffix)

    try:
        url = f"{schema_registry_url.rstrip('/')}/subjects/{subject}/versions/{version}"
        response = requests.get(url, timeout=10)

        if response.status_code != 200:
            sys.stderr.write(
                f"✗ Failed to fetch schema info '{subject}': "
                f"{response.status_code} - {response.text}\n"
            )
            sys.stderr.flush()
            return None

        result = response.json()
        schema_str = result.get("schema")

        if not schema_str:
            sys.stderr.write(f"✗ No schema string in response for '{subject}'\n")
            sys.stderr.flush()
            return None

        return {
            "id": result.get("id"),
            "version": result.get("version"),
            "schema": json.loads(schema_str),
            "subject": subject,
        }

    except Exception as e:
        sys.stderr.write(f"✗ Error retrieving schema info '{subject}': {e}\n")
        sys.stderr.flush()
        return None


def register_schema(
    atype: Type[BaseModel],
    schema_registry_url: str = "http://localhost:8081",
    subject: Optional[str] = None,
    compatibility: Optional[str] = "BACKWARD",
) -> Optional[int]:
    """
    Register a Pydantic model's JSON Schema in the schema registry.

    Args:
        atype: Pydantic model class to register.
        schema_registry_url: Base URL of the schema registry.
        subject: Subject name override.  Defaults to ``"{atype.__name__}-value"``.
        compatibility: Compatibility mode to set after registration
            (``"BACKWARD"``, ``"FORWARD"``, ``"FULL"``, ``"NONE"``).
            Pass ``None`` to skip setting compatibility.

    Returns:
        Schema ID (int) on success, ``None`` on error.
    """
    if subject is None:
        subject = get_subject_name(atype.__name__, is_key=False, add_suffix=True)

    try:
        json_schema = atype.model_json_schema()
        schema_data = {"schemaType": "JSON", "schema": json.dumps(json_schema)}

        url = f"{schema_registry_url.rstrip('/')}/subjects/{subject}/versions"
        response = requests.post(
            url,
            json=schema_data,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=10,
        )

        if response.status_code in (200, 201):
            schema_id = response.json().get("id")
            sys.stderr.write(f"✓ Registered schema '{subject}' (ID: {schema_id})\n")
            sys.stderr.flush()

            if compatibility:
                set_compatibility(subject, schema_registry_url, compatibility)

            return schema_id

        sys.stderr.write(
            f"✗ Failed to register schema '{subject}': "
            f"{response.status_code} - {response.text}\n"
        )
        sys.stderr.flush()
        return None

    except Exception as e:
        sys.stderr.write(f"✗ Error registering schema '{subject}': {e}\n")
        sys.stderr.flush()
        return None


def register_atype_schema(
    atype: Type[BaseModel],
    schema_registry_url: str = "http://localhost:8081",
    topic: Optional[str] = None,
    is_key: bool = False,
    compatibility: str = "BACKWARD",
) -> Optional[int]:
    """
    Register a Pydantic model's JSON Schema in the schema registry using the
    topic-derived subject name.

    This is the high-level convenience wrapper around :func:`register_schema`.
    It derives the subject name from ``atype.__name__`` (or ``topic`` if
    ``atype.__name__`` is not available) and delegates to
    :func:`register_schema`.

    Args:
        atype: Pydantic model class to register.
        schema_registry_url: Base URL of the schema registry.
        topic: Kafka topic name used as a fallback for the subject name when
            ``atype.__name__`` is not available.  Ignored when ``atype`` is
            provided (the class name is used instead).
        is_key: If ``True``, registers as key schema; otherwise value schema.
        compatibility: Compatibility mode (``"BACKWARD"``, ``"FORWARD"``,
            ``"FULL"``, ``"NONE"``).

    Returns:
        Schema ID (int) on success, ``None`` on error.
    """
    type_name = atype.__name__ if atype else (topic or "unknown")
    subject = get_subject_name(type_name, is_key=is_key, add_suffix=True)
    return register_schema(
        atype=atype,
        schema_registry_url=schema_registry_url,
        subject=subject,
        compatibility=compatibility,
    )


def set_compatibility(
    subject: str,
    schema_registry_url: str = "http://localhost:8081",
    compatibility: str = "BACKWARD",
) -> bool:
    """
    Set the compatibility mode for a schema registry subject.

    Args:
        subject: Full subject name (e.g. ``"UserProfile-value"``).
        schema_registry_url: Base URL of the schema registry.
        compatibility: Compatibility mode string.

    Returns:
        ``True`` on success, ``False`` on error.
    """
    try:
        url = f"{schema_registry_url.rstrip('/')}/config/{subject}"
        response = requests.put(
            url,
            json={"compatibility": compatibility},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=10,
        )
        if response.status_code == 200:
            sys.stderr.write(f"✓ Set compatibility '{compatibility}' for '{subject}'\n")
            sys.stderr.flush()
            return True
        return False
    except Exception:
        return False


def list_schema_versions(
    atype_name: str,
    schema_registry_url: str = "http://localhost:8081",
    is_key: bool = False,
    add_suffix: bool = True,
) -> Optional[List[int]]:
    """
    List all registered versions for a schema subject.

    Args:
        atype_name: Pydantic model class name.
        schema_registry_url: Base URL of the schema registry.
        is_key: ``True`` for key schema, ``False`` for value.
        add_suffix: Passed to :func:`get_subject_name`.

    Returns:
        List of version integers, empty list if subject not found, or
        ``None`` on error.
    """
    subject = get_subject_name(atype_name, is_key, add_suffix)

    try:
        url = f"{schema_registry_url.rstrip('/')}/subjects/{subject}/versions"
        response = requests.get(url, timeout=10)

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

        sys.stderr.write(
            f"✗ Failed to list schemas for '{subject}': "
            f"{response.status_code} - {response.text}\n"
        )
        sys.stderr.flush()
        return None

    except Exception as e:
        sys.stderr.write(f"✗ Error listing schemas for '{subject}': {e}\n")
        sys.stderr.flush()
        return None


def schema_exists(
    subject: str,
    schema_registry_url: str = "http://localhost:8081",
    version: str = "latest",
) -> bool:
    """
    Check whether a subject/version exists in the schema registry.

    Args:
        subject: Full subject name (e.g. ``"UserProfile-value"``).
        schema_registry_url: Base URL of the schema registry.
        version: Version to check (default ``"latest"``).

    Returns:
        ``True`` if the subject/version exists, ``False`` otherwise.
    """
    try:
        url = f"{schema_registry_url.rstrip('/')}/subjects/{subject}/versions/{version}"
        response = requests.get(url, timeout=10)
        return response.status_code == 200
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Kafka topic administration helpers
# ---------------------------------------------------------------------------


def create_kafka_topic(
    topic_name: str,
    kafka_server: str = "localhost:9092",
    num_partitions: int = 1,
    replication_factor: int = 1,
) -> bool:
    """
    Create a new Kafka topic.

    Args:
        topic_name: Name of the topic to create.
        kafka_server: Kafka bootstrap server address (default: ``"localhost:9092"``).
        num_partitions: Number of partitions (default: 1).
        replication_factor: Replication factor (default: 1).

    Returns:
        ``True`` if the topic was created or already exists, ``False`` on error.
    """
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_server, client_id="agstream-topic-creator"
        )
        try:
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            sys.stderr.write(
                f"✓ Topic '{topic_name}' created successfully!\n"
                f"  - Partitions: {num_partitions}\n"
                f"  - Replication Factor: {replication_factor}\n"
            )
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


def kafka_topic_exists(
    topic_name: str,
    kafka_server: str = "localhost:9092",
) -> bool:
    """
    Check if a Kafka topic exists.

    Args:
        topic_name: Name of the topic to check.
        kafka_server: Kafka bootstrap server address (default: ``"localhost:9092"``).

    Returns:
        ``True`` if the topic exists, ``False`` otherwise.
    """
    from kafka.admin import KafkaAdminClient

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_server, client_id="agstream-topic-checker"
        )
        try:
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


# ---------------------------------------------------------------------------
# Backward-compat alias: create_model_from_schema → create_pydantic_from_json_schema
# ---------------------------------------------------------------------------


def create_model_from_schema(schema: Dict[str, Any]) -> Type[BaseModel]:
    """
    Alias for :func:`create_pydantic_from_json_schema` kept for backward
    compatibility with code that imported this name from ``streaming_utils``.
    """
    result = create_pydantic_from_json_schema(schema)
    if result is None:
        # Fallback: return an empty model rather than None (old behaviour)
        return create_model(schema.get("title", "DynamicModel"))
    return result


# Made with Bob
