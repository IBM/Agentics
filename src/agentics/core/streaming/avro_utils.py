"""
AVRO serialization utilities for AGStream.

Provides Pydantic to AVRO schema conversion and message serialization/deserialization
with Confluent Schema Registry wire format support.

The Confluent Schema Registry wire format consists of:
- Byte 0: Magic byte (0x00)
- Bytes 1-4: Schema ID (big-endian int32)
- Bytes 5+: AVRO binary data

This module enables AGStream to use AVRO format for better Flink SQL compatibility
and improved performance compared to JSON.
"""

from __future__ import annotations

import io
import json
import struct
import sys
from typing import Any, Dict, List, Optional, Tuple, Type, Union, get_args, get_origin

import requests
from pydantic import BaseModel
from pydantic.fields import FieldInfo

try:
    import fastavro
except ImportError:
    fastavro = None
    sys.stderr.write("⚠️  fastavro not installed. Install with: pip install fastavro\n")
    sys.stderr.flush()


def pydantic_to_avro_schema(
    model: Type[BaseModel], namespace: str = "agentics"
) -> dict:
    """
    Convert Pydantic model to AVRO schema.

    Maps Pydantic types to AVRO types:
    - str -> "string"
    - int -> "long"
    - float -> "double"
    - bool -> "boolean"
    - Optional[T] -> ["null", T]
    - List[T] -> {"type": "array", "items": T}
    - Dict[str, T] -> {"type": "map", "values": T}
    - Nested BaseModel -> nested record

    Args:
        model: Pydantic model class
        namespace: AVRO namespace (default: "agentics")

    Returns:
        AVRO schema dict

    Example:
        >>> from pydantic import BaseModel
        >>> class User(BaseModel):
        ...     name: str
        ...     age: int
        ...     email: Optional[str] = None
        >>> schema = pydantic_to_avro_schema(User)
        >>> print(schema["type"])
        'record'
    """
    if not fastavro:
        raise ImportError(
            "fastavro is required for AVRO support. Install with: pip install fastavro"
        )

    fields = []
    for field_name, field_info in model.model_fields.items():
        avro_field = _pydantic_field_to_avro(field_name, field_info, namespace)
        fields.append(avro_field)

    return {
        "type": "record",
        "name": model.__name__,
        "namespace": namespace,
        "fields": fields,
    }


def _pydantic_field_to_avro(
    field_name: str, field_info: FieldInfo, namespace: str
) -> dict:
    """
    Convert a single Pydantic field to AVRO field definition.

    Args:
        field_name: Name of the field
        field_info: Pydantic FieldInfo object
        namespace: AVRO namespace

    Returns:
        AVRO field dict
    """
    from pydantic_core import PydanticUndefined

    field_type = field_info.annotation
    is_optional = not field_info.is_required()

    avro_type = _python_type_to_avro(field_type, namespace)

    # Handle optional fields
    if is_optional and avro_type != "null":
        if isinstance(avro_type, list):
            # Already a union, add null if not present
            if "null" not in avro_type:
                avro_type = ["null"] + avro_type
        else:
            # Make it a union with null
            avro_type = ["null", avro_type]

    field_def = {"name": field_name, "type": avro_type}

    # Add description from Pydantic Field to Avro doc field
    if field_info.description and field_info.description.strip():
        field_def["doc"] = field_info.description

    # Add default value if present (skip PydanticUndefined)
    if (
        field_info.default is not None
        and field_info.default is not ...
        and field_info.default is not PydanticUndefined
    ):
        field_def["default"] = field_info.default
    elif is_optional:
        field_def["default"] = None

    return field_def


def _python_type_to_avro(python_type: Any, namespace: str) -> Union[str, dict, list]:
    """
    Convert Python type annotation to AVRO type.

    Args:
        python_type: Python type annotation
        namespace: AVRO namespace

    Returns:
        AVRO type (string, dict, or list for unions)
    """
    # Handle None type
    if python_type is type(None):
        return "null"

    # Get origin for generic types (List, Dict, Optional, etc.)
    origin = get_origin(python_type)

    # Handle Optional[T] which is Union[T, None]
    if origin is Union:
        args = get_args(python_type)
        # Filter out None type
        non_none_args = [arg for arg in args if arg is not type(None)]

        if len(non_none_args) == 1:
            # Optional[T] case
            inner_type = _python_type_to_avro(non_none_args[0], namespace)
            if isinstance(inner_type, list):
                return ["null"] + inner_type
            return ["null", inner_type]
        else:
            # Union of multiple types
            return [_python_type_to_avro(arg, namespace) for arg in non_none_args]

    # Handle List[T]
    if origin is list or origin is List:
        args = get_args(python_type)
        if args:
            item_type = _python_type_to_avro(args[0], namespace)
            return {"type": "array", "items": item_type}
        return {"type": "array", "items": "string"}

    # Handle Dict[str, T]
    if origin is dict or origin is Dict:
        args = get_args(python_type)
        if len(args) >= 2:
            value_type = _python_type_to_avro(args[1], namespace)
            return {"type": "map", "values": value_type}
        return {"type": "map", "values": "string"}

    # Handle nested Pydantic models
    if isinstance(python_type, type) and issubclass(python_type, BaseModel):
        return pydantic_to_avro_schema(python_type, namespace)

    # Handle basic types
    type_mapping = {
        str: "string",
        int: "long",
        float: "double",
        bool: "boolean",
        bytes: "bytes",
    }

    return type_mapping.get(python_type, "string")


def avro_serialize(data: dict, schema_id: int, avro_schema: dict) -> bytes:
    """
    Serialize data to AVRO with Confluent Schema Registry wire format.

    Wire format:
    - Byte 0: Magic byte (0x00)
    - Bytes 1-4: Schema ID (big-endian int32)
    - Bytes 5+: AVRO binary data

    Args:
        data: Dictionary to serialize
        schema_id: Schema Registry schema ID
        avro_schema: AVRO schema dict

    Returns:
        Serialized bytes with Schema Registry header

    Raises:
        ImportError: If fastavro is not installed
        ValueError: If serialization fails

    Example:
        >>> data = {"name": "Alice", "age": 30}
        >>> schema = {"type": "record", "name": "User", "fields": [...]}
        >>> serialized = avro_serialize(data, schema_id=1, avro_schema=schema)
        >>> serialized[0]  # Magic byte
        0
    """
    if not fastavro:
        raise ImportError(
            "fastavro is required for AVRO support. Install with: pip install fastavro"
        )

    try:
        # Serialize AVRO data
        output = io.BytesIO()
        fastavro.schemaless_writer(output, avro_schema, data)
        avro_bytes = output.getvalue()

        # Prepend Schema Registry header
        # Magic byte (0x00) + Schema ID (4 bytes, big-endian)
        header = struct.pack(">bI", 0, schema_id)

        return header + avro_bytes

    except Exception as e:
        raise ValueError(f"Failed to serialize AVRO data: {e}") from e


def avro_deserialize(
    message: bytes, schema_registry_url: str, schema_cache: Optional[dict] = None
) -> Tuple[Dict[str, Any], int]:
    """
    Deserialize AVRO message with Schema Registry wire format.

    Extracts schema ID from header, fetches schema from registry (with caching),
    and deserializes the AVRO binary data.

    Args:
        message: Serialized message bytes
        schema_registry_url: Schema Registry URL
        schema_cache: Optional dict to cache schemas by ID (for performance)

    Returns:
        Tuple of (deserialized_data, schema_id)

    Raises:
        ImportError: If fastavro is not installed
        ValueError: If message format is invalid or deserialization fails

    Example:
        >>> message = b'\\x00\\x00\\x00\\x00\\x01...'  # AVRO message
        >>> data, schema_id = avro_deserialize(message, "http://localhost:8081")
        >>> print(data["name"])
        'Alice'
    """
    if not fastavro:
        raise ImportError(
            "fastavro is required for AVRO support. Install with: pip install fastavro"
        )

    if len(message) < 5:
        raise ValueError(
            f"Message too short ({len(message)} bytes). "
            "Expected at least 5 bytes for Schema Registry header."
        )

    # Parse header
    magic_byte = message[0]
    if magic_byte != 0:
        raise ValueError(
            f"Invalid magic byte: {magic_byte}. Expected 0 for AVRO format."
        )

    # Extract schema ID (bytes 1-4, big-endian)
    schema_id = struct.unpack(">I", message[1:5])[0]

    # Get AVRO schema (with caching)
    if schema_cache is not None and schema_id in schema_cache:
        avro_schema = schema_cache[schema_id]
    else:
        avro_schema = _fetch_avro_schema(schema_id, schema_registry_url)
        if schema_cache is not None:
            schema_cache[schema_id] = avro_schema

    # Deserialize AVRO data (bytes 5+)
    try:
        avro_bytes = message[5:]
        input_stream = io.BytesIO(avro_bytes)
        data = fastavro.schemaless_reader(input_stream, avro_schema)
        # fastavro returns dict-like object, type checker limitation
        return data, schema_id  # type: ignore[return-value]

    except Exception as e:
        raise ValueError(f"Failed to deserialize AVRO data: {e}") from e


def _fetch_avro_schema(schema_id: int, schema_registry_url: str) -> dict:
    """
    Fetch AVRO schema from Schema Registry by ID.

    Args:
        schema_id: Schema ID
        schema_registry_url: Schema Registry URL

    Returns:
        AVRO schema dict

    Raises:
        ValueError: If schema cannot be fetched
    """
    try:
        url = f"{schema_registry_url.rstrip('/')}/schemas/ids/{schema_id}"
        response = requests.get(url, timeout=10)

        if response.status_code != 200:
            raise ValueError(
                f"Failed to fetch schema ID {schema_id}: "
                f"{response.status_code} - {response.text}"
            )

        result = response.json()
        schema_str = result.get("schema")

        if not schema_str:
            raise ValueError(f"No schema found for ID {schema_id}")

        return json.loads(schema_str)

    except requests.RequestException as e:
        raise ValueError(f"Error fetching schema ID {schema_id}: {e}") from e


def detect_message_format(message: bytes) -> str:
    """
    Auto-detect message format (JSON or AVRO).

    AVRO messages start with magic byte 0x00.
    JSON messages typically start with '{' (0x7B) or '[' (0x5B).

    Args:
        message: Message bytes

    Returns:
        "AVRO" or "JSON"

    Example:
        >>> avro_msg = b'\\x00\\x00\\x00\\x00\\x01...'
        >>> detect_message_format(avro_msg)
        'AVRO'
        >>> json_msg = b'{"name": "Alice"}'
        >>> detect_message_format(json_msg)
        'JSON'
    """
    if len(message) == 0:
        return "JSON"  # Default to JSON for empty messages

    # AVRO messages start with magic byte 0x00
    if message[0] == 0x00:
        return "AVRO"

    # JSON messages start with '{' or '['
    if message[0] in (0x7B, 0x5B):  # '{' or '['
        return "JSON"

    # Default to JSON for unknown formats
    return "JSON"


def avro_schema_to_pydantic(
    avro_schema: dict, model_name: Optional[str] = None
) -> Type[BaseModel]:
    """
    Convert AVRO schema to Pydantic model (reverse of pydantic_to_avro_schema).

    This is useful for dynamically creating Pydantic models from AVRO schemas
    fetched from the Schema Registry.

    Args:
        avro_schema: AVRO schema dict
        model_name: Optional name for the Pydantic model (defaults to schema name)

    Returns:
        Dynamically created Pydantic model class

    Example:
        >>> avro_schema = {
        ...     "type": "record",
        ...     "name": "User",
        ...     "fields": [
        ...         {"name": "name", "type": "string"},
        ...         {"name": "age", "type": "long"}
        ...     ]
        ... }
        >>> UserModel = avro_schema_to_pydantic(avro_schema)
        >>> user = UserModel(name="Alice", age=30)
    """
    from pydantic import create_model

    if avro_schema.get("type") != "record":
        raise ValueError(
            f"Only AVRO record types are supported, got: {avro_schema.get('type')}"
        )

    name = model_name or avro_schema.get("name", "DynamicModel")
    fields = {}

    for field in avro_schema.get("fields", []):
        field_name = field["name"]
        field_type = _avro_type_to_python(field["type"])
        default = field.get("default", ...)

        fields[field_name] = (field_type, default)

    return create_model(name, **fields)


def _avro_type_to_python(avro_type: Union[str, dict, list]) -> Any:
    """
    Convert AVRO type to Python type annotation.

    Args:
        avro_type: AVRO type (string, dict, or list)

    Returns:
        Python type annotation
    """
    # Handle union types (list)
    if isinstance(avro_type, list):
        # Check if it's an optional type (union with null)
        if "null" in avro_type:
            non_null_types = [t for t in avro_type if t != "null"]
            if len(non_null_types) == 1:
                inner_type = _avro_type_to_python(non_null_types[0])
                return Optional[inner_type]
            # Multiple non-null types
            types = [_avro_type_to_python(t) for t in non_null_types]
            return Union[tuple(types)]
        # Union without null
        types = [_avro_type_to_python(t) for t in avro_type]
        return Union[tuple(types)]

    # Handle complex types (dict)
    if isinstance(avro_type, dict):
        type_name = avro_type.get("type")

        if type_name == "array":
            item_type = _avro_type_to_python(avro_type.get("items", "string"))
            return List[item_type]

        if type_name == "map":
            value_type = _avro_type_to_python(avro_type.get("values", "string"))
            return Dict[str, value_type]

        if type_name == "record":
            # Nested record - recursively convert
            return avro_schema_to_pydantic(avro_type)

        # Unknown complex type
        return Any

    # Handle primitive types (string)
    type_mapping = {
        "string": str,
        "long": int,
        "int": int,
        "double": float,
        "float": float,
        "boolean": bool,
        "bytes": bytes,
        "null": type(None),
    }

    return type_mapping.get(avro_type, Any)


# Made with Bob
