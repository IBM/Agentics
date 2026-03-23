"""
AG Operators - Unified Semantic Mapping UDFs for Flink SQL

Provides ag.map operator that works with both:
- Registered schemas from Schema Registry
- Dynamic on-the-fly type generation

Usage:
    -- Dynamic mode (no schema registry needed)
    SELECT T.sentiment
    FROM pr, LATERAL TABLE(ag.map(customer_review, 'sentiment')) AS T(sentiment);

    -- Registry mode (uses pre-registered schema)
    SELECT T.sentiment_label, T.sentiment_score
    FROM pr, LATERAL TABLE(ag.map(customer_review, 'Sentiment', True)) AS T(sentiment_label, sentiment_score);
"""

import asyncio
import json
import os
import sys
import threading
from typing import Dict, Optional

import requests
from dotenv import load_dotenv
from pydantic import BaseModel, Field, create_model
from pyflink.table import DataTypes
from pyflink.table.udf import udtf

from agentics import AG

# Load environment variables
for env_path in ["/opt/flink/.env", "../../../.env", ".env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"✓ Loaded .env from {env_path}", file=sys.stderr)
        break


#######################
### Helper Functions ###
#######################


def get_schema_registry_url():
    """Get the Schema Registry URL from environment or use default."""
    url = os.getenv("SCHEMA_REGISTRY_URL")
    if url:
        return url

    # Try Docker network hostname (from Flink container)
    try:
        response = requests.get(
            "http://karapace-schema-registry:8081/subjects", timeout=2
        )
        if response.status_code == 200:
            return "http://karapace-schema-registry:8081"
    except:
        pass

    # Fall back to localhost (from host machine)
    return "http://localhost:8081"


def fetch_schema_fields(type_name: str) -> Dict[str, type]:
    """
    Fetch field definitions from Schema Registry.

    Returns:
        Dictionary mapping field names to Python types
    """
    try:
        registry_url = get_schema_registry_url()
        print(f"Fetching schema for '{type_name}' from {registry_url}", file=sys.stderr)

        # Try different subject name variations
        attempts = [
            f"{type_name}-value",  # Standard Kafka naming
            type_name,  # Direct name
            f"{type_name}-key",  # Key schema
        ]

        response = None
        subject_used = None

        for subject in attempts:
            url = f"{registry_url}/subjects/{subject}/versions/latest"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                subject_used = subject
                break

        if not subject_used:
            return {}

        response.raise_for_status()

        schema_data = response.json()
        schema = json.loads(schema_data["schema"])

        # Extract fields
        fields = {}
        if "fields" in schema:
            for field in schema["fields"]:
                field_name = field["name"]
                field_type = field["type"]

                # Simplify complex types
                if isinstance(field_type, dict):
                    if "type" in field_type:
                        field_type = field_type["type"]
                    else:
                        field_type = "string"
                elif isinstance(field_type, list):
                    # Handle union types (e.g., ["null", "string"])
                    non_null_types = [t for t in field_type if t != "null"]
                    field_type = non_null_types[0] if non_null_types else "string"

                # Map Avro types to Python types
                type_mapping = {
                    "string": str,
                    "int": int,
                    "long": int,
                    "float": float,
                    "double": float,
                    "boolean": bool,
                }
                fields[field_name] = type_mapping.get(field_type, str)

        return fields

    except Exception as e:
        print(f"Error fetching schema for {type_name}: {e}", file=sys.stderr)
        return {}


def create_dynamic_model(type_name: str, fields: Dict[str, type]) -> type:
    """Create a Pydantic model dynamically from field definitions."""
    field_definitions = {}
    for field_name, field_type in fields.items():
        # Make all fields optional with None default
        field_definitions[field_name] = (field_type | None, Field(default=None))

    return create_model(type_name, **field_definitions)


def run_async(coro, timeout=30):
    """Run async coroutine in sync context with timeout, handling existing event loops"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            # Add timeout wrapper
            return loop.run_until_complete(asyncio.wait_for(coro, timeout=timeout))
        finally:
            loop.close()
    except asyncio.TimeoutError:
        print(f"Error: LLM call timed out after {timeout} seconds", file=sys.stderr)
        raise TimeoutError(f"LLM call timed out after {timeout} seconds")
    except Exception as e:
        print(f"Error running async: {e}", file=sys.stderr)
        raise


# Thread-local storage for AG instances
_thread_local = threading.local()


def get_ag_for_type(target_type: str, model_class: type):
    """Get or create AG instance for a specific type using thread-local storage."""
    if not hasattr(_thread_local, "ag_cache"):
        _thread_local.ag_cache = {}

    if target_type in _thread_local.ag_cache:
        return _thread_local.ag_cache[target_type]

    # Force reload .env
    for env_path in ["/opt/flink/.env", "../../../.env", ".env"]:
        if os.path.exists(env_path):
            load_dotenv(env_path, override=True)
            break

    # Check API keys
    api_key = os.getenv("OPENAI_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError(
            "No API key found. Please set OPENAI_API_KEY or ANTHROPIC_API_KEY"
        )

    # Create and cache AG instance
    ag_instance = AG(atype=model_class)
    _thread_local.ag_cache[target_type] = ag_instance

    return ag_instance


#######################
### Main UDF: ag.map ###
#######################


@udtf(result_types=[DataTypes.STRING()])
def agmap(input_data, column_name, field_type="str", description=None):
    """
    Unified agmap operator for Flink SQL.

    Supports two modes based on field_type parameter:
    1. Dynamic mode: field_type = 'str', 'int', 'float', 'bool' (generates types on-the-fly)
    2. Registry mode: field_type = 'registry' (uses pre-registered schemas from Schema Registry)

    Usage:
        -- Dynamic mode (default - no schema registry needed)
        SELECT T.sentiment
        FROM pr, LATERAL TABLE(ag.map(customer_review, 'sentiment')) AS T(sentiment);

        -- Dynamic with type
        SELECT T.rating
        FROM pr, LATERAL TABLE(ag.map(customer_review, 'rating', 'float')) AS T(rating);

        -- Dynamic with description
        SELECT T.emotion
        FROM pr, LATERAL TABLE(ag.map(customer_review, 'emotion', 'str',
                               'Primary emotion: joy, anger, sadness, fear, neutral')) AS T(emotion);

        -- Registry mode (multiple fields from registered schema)
        SELECT T.sentiment_label, T.sentiment_score
        FROM pr, LATERAL TABLE(ag.map(customer_review, 'Sentiment', 'registry')) AS T(sentiment_label, sentiment_score);

    Args:
        input_data: Input data to process (FIRST - required)
        column_name: Column name for dynamic mode OR schema name for registry mode (SECOND - required)
        field_type: Type specification (THIRD - optional, default: 'str')
            - 'str', 'string': String type (dynamic mode)
            - 'int', 'integer': Integer type (dynamic mode)
            - 'float', 'double': Float type (dynamic mode)
            - 'bool', 'boolean': Boolean type (dynamic mode)
            - 'registry': Use Schema Registry (registry mode)
        description: Description for dynamic mode to guide LLM (FOURTH - optional, ignored in registry mode)

    Yields:
        Tuple containing the generated field value(s)
    """
    if not input_data or not column_name:
        yield ("error",)
        return

    try:
        # REGISTRY MODE: Use pre-registered schema
        if field_type and field_type.lower() == "registry":
            # Fetch schema fields from registry
            fields = fetch_schema_fields(column_name)

            if not fields:
                yield (json.dumps({"error": "schema_not_found"}),)
                return

            # Create dynamic model from schema
            model_class = create_dynamic_model(column_name, fields)

            # Get AG instance
            ag = get_ag_for_type(column_name, model_class)

            # Perform semantic transformation
            ag_result = run_async(ag << input_data)

            # Extract the actual model instance
            if hasattr(ag_result, "states") and ag_result.states:
                last_state = ag_result.states[-1]
                result = last_state if last_state else ag_result
            else:
                result = ag_result

            # Convert result to dictionary
            if hasattr(result, "model_dump"):
                result_dict = result.model_dump()
            elif hasattr(result, "dict"):
                result_dict = result.dict()
            elif isinstance(result, dict):
                result_dict = result
            else:
                result_dict = {"result": str(result), "error": None}

            # For registry mode, return JSON string with all fields
            yield (json.dumps(result_dict),)

        # DYNAMIC MODE: Generate type on-the-fly
        else:
            # Map string type names to Python types
            type_mapping = {
                "str": str,
                "string": str,
                "int": int,
                "integer": int,
                "float": float,
                "double": float,
                "bool": bool,
                "boolean": bool,
            }

            python_type = type_mapping.get(
                field_type.lower() if field_type else "str", str
            )

            # Create dynamic Pydantic model with single field
            field_definitions = {}
            if description:
                field_definitions[column_name] = (
                    python_type | None,
                    Field(default=None, description=description),
                )
            else:
                field_definitions[column_name] = (
                    python_type | None,
                    Field(default=None),
                )

            # Generate unique model name
            model_name = f"Dynamic_{column_name}_{field_type or 'str'}"
            dynamic_model = create_model(model_name, **field_definitions)

            print(
                f"DEBUG ag.map: Created model {model_name} with field {column_name}:{python_type}",
                file=sys.stderr,
            )

            # Get or create AG instance for this dynamic type
            ag = get_ag_for_type(model_name, dynamic_model)

            # Add instructions based on description
            if description:
                ag.instructions = f"Extract or generate the '{column_name}' field from the input data. {description}"
            else:
                ag.instructions = f"Extract or generate the '{column_name}' field from the input data."

            # Perform semantic transformation
            ag_result = run_async(ag << input_data)

            # Extract the actual model instance
            if hasattr(ag_result, "states") and ag_result.states:
                last_state = ag_result.states[-1]
                result = last_state if last_state else ag_result
            else:
                result = ag_result

            # Convert result to dictionary
            if hasattr(result, "model_dump"):
                result_dict = result.model_dump()
            elif hasattr(result, "dict"):
                result_dict = result.dict()
            elif isinstance(result, dict):
                result_dict = result
            else:
                result_dict = {column_name: str(result)}

            # Extract the field value
            field_value = result_dict.get(column_name)

            # Handle None values and type conversion
            if field_value is None:
                if python_type == str:
                    field_value = "unknown"
                elif python_type in (int, float):
                    field_value = 0
                elif python_type == bool:
                    field_value = False

            # Convert to string for output (Flink UDTF returns STRING)
            yield (str(field_value),)

    except Exception as e:
        print(f"Error in ag.map: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        yield ("error",)


# Made with Bob
