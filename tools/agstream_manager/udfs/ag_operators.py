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


#######################
### agmap_table: Generic Typed Table Output ###
#######################

# Global cache for schema-specific UDTFs
_agmap_table_cache = {}


@udtf(result_types=[DataTypes.STRING()])
def agmap_table(input_data, schema_name, mode="registry"):
    """
    Generic agmap that returns typed table columns instead of JSON.

    This UDF works exactly like agmap() but returns typed columns directly,
    eliminating the need for JSON_VALUE() parsing. It dynamically inspects
    the schema and returns the appropriate typed columns.

    **IMPORTANT**: Due to Flink limitations, this returns a JSON string that
    contains all fields with their proper types. You still need to parse it,
    BUT the companion function agmap_table_for() creates schema-specific UDTFs
    that return true typed columns.

    Usage:
        -- Generic (returns JSON with typed values):
        SELECT
            JSON_VALUE(T.result, '$.sentiment_label') as sentiment_label,
            CAST(JSON_VALUE(T.result, '$.sentiment_score') AS DOUBLE) as sentiment_score
        FROM pr,
        LATERAL TABLE(agmap_table(customer_review, 'Sentiment', 'registry')) AS T(result);

        -- Better: Use schema-specific function (returns typed columns):
        SELECT T.sentiment_label, T.sentiment_score
        FROM pr,
        LATERAL TABLE(agmap_table_sentiment(customer_review)) AS T(sentiment_label, sentiment_score);

    Args:
        input_data: Input data to process
        schema_name: Name of the schema in Schema Registry
        mode: 'registry' to fetch from Schema Registry (default)

    Yields:
        JSON string containing all fields with proper types
    """
    if not input_data or not schema_name:
        yield (json.dumps({"error": "missing_input"}),)
        return

    try:
        if mode == "registry":
            # Fetch schema fields from registry
            fields = fetch_schema_fields(schema_name)

            if not fields:
                yield (json.dumps({"error": "schema_not_found"}),)
                return

            # Create dynamic model from schema
            model_class = create_dynamic_model(schema_name, fields)

            # Get AG instance
            ag = get_ag_for_type(schema_name, model_class)

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

            # Return JSON with all fields
            yield (json.dumps(result_dict),)

        else:
            yield (json.dumps({"error": "unsupported_mode"}),)

    except Exception as e:
        print(f"Error in agmap_table: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        yield (json.dumps({"error": str(e)}),)


def agmap_table_for(schema_name: str):
    """
    Factory function that creates a schema-specific UDTF returning typed columns.

    This creates a UDTF tailored for a specific schema that returns true typed
    columns (not JSON), eliminating ALL parsing overhead.

    Usage:
        # In Python, create schema-specific UDF:
        sentiment_udtf = agmap_table_for('Sentiment')

        # Register in Flink SQL:
        CREATE TEMPORARY SYSTEM FUNCTION agmap_table_sentiment
        AS 'ag_operators.agmap_table_sentiment' LANGUAGE PYTHON;

        # Use with typed columns (no JSON parsing!):
        SELECT T.sentiment_label, T.sentiment_score
        FROM pr, LATERAL TABLE(agmap_table_sentiment(customer_review))
        AS T(sentiment_label, sentiment_score);

    Args:
        schema_name: Name of the schema in Schema Registry

    Returns:
        A UDTF function that returns typed columns matching the schema
    """
    # Check cache
    if schema_name in _agmap_table_cache:
        return _agmap_table_cache[schema_name]

    # Fetch schema fields from registry
    fields = fetch_schema_fields(schema_name)

    if not fields:
        raise ValueError(f"Schema '{schema_name}' not found in registry")

    print(
        f"Creating agmap_table_for('{schema_name}') with fields: {list(fields.keys())}",
        file=sys.stderr,
    )

    # Create dynamic model
    model_class = create_dynamic_model(schema_name, fields)

    # Determine result types for Flink
    result_types = []
    for field_name, field_type in fields.items():
        if field_type == int:
            result_types.append(DataTypes.BIGINT())
        elif field_type == float:
            result_types.append(DataTypes.DOUBLE())
        elif field_type == bool:
            result_types.append(DataTypes.BOOLEAN())
        else:
            result_types.append(DataTypes.STRING())

    # Create the UDTF with typed output
    @udtf(result_types=result_types)
    def schema_specific_udtf(input_data):
        """UDTF for {schema_name} - returns typed columns: {field_list}"""
        if not input_data:
            # Return error values for all fields
            error_values = []
            for field_type in fields.values():
                if field_type == str:
                    error_values.append("error")
                elif field_type in (int, float):
                    error_values.append(0)
                elif field_type == bool:
                    error_values.append(False)
                else:
                    error_values.append("error")
            yield tuple(error_values)
            return

        try:
            # Get AG instance
            ag = get_ag_for_type(schema_name, model_class)

            # Perform semantic transformation
            ag_result = run_async(ag << input_data)

            # Extract result
            if hasattr(ag_result, "states") and ag_result.states:
                result = ag_result.states[-1] if ag_result.states[-1] else ag_result
            else:
                result = ag_result

            # Convert to dictionary
            if hasattr(result, "model_dump"):
                result_dict = result.model_dump()
            elif hasattr(result, "dict"):
                result_dict = result.dict()
            elif isinstance(result, dict):
                result_dict = result
            else:
                result_dict = {field: None for field in fields.keys()}

            # Extract field values in order, maintaining proper types
            values = []
            for field_name, field_type in fields.items():
                value = result_dict.get(field_name)

                # Handle None values with appropriate defaults
                if value is None:
                    if field_type == str:
                        value = "unknown"
                    elif field_type in (int, float):
                        value = 0
                    elif field_type == bool:
                        value = False

                values.append(value)

            yield tuple(values)

        except Exception as e:
            print(f"Error in agmap_table_for({schema_name}): {e}", file=sys.stderr)
            import traceback

            traceback.print_exc(file=sys.stderr)
            # Return error values
            error_values = []
            for field_type in fields.values():
                if field_type == str:
                    error_values.append("error")
                elif field_type in (int, float):
                    error_values.append(0)
                elif field_type == bool:
                    error_values.append(False)
                else:
                    error_values.append("error")
            yield tuple(error_values)

    # Update docstring
    field_list = ", ".join(fields.keys())
    schema_specific_udtf.__doc__ = schema_specific_udtf.__doc__.format(
        schema_name=schema_name, field_list=field_list
    )

    # Cache it
    _agmap_table_cache[schema_name] = schema_specific_udtf

    return schema_specific_udtf


# Pre-create common schema-specific UDTFs
def agmap_table_sentiment():
    """Schema-specific UDTF for Sentiment - returns typed columns"""
    return agmap_table_for("Sentiment")


def agmap_table_productreview():
    """Schema-specific UDTF for ProductReview - returns typed columns"""
    return agmap_table_for("ProductReview")


#######################
### ag_sentiment: Clean Signature UDTF ###
#######################


@udtf(result_types=[DataTypes.STRING(), DataTypes.DOUBLE()])
def ag_sentiment(text):
    """
    Specialized UDTF for sentiment analysis with clean signature.

    Returns typed columns directly: sentiment_label (STRING), sentiment_score (DOUBLE)

    Usage:
        SELECT T.sentiment_label, T.sentiment_score
        FROM pr,
        LATERAL TABLE(ag_sentiment(customer_review)) AS T(sentiment_label, sentiment_score);

    Args:
        text: Input text to analyze

    Yields:
        Tuple of (sentiment_label, sentiment_score)
    """
    if not text:
        yield ("unknown", 0.0)
        return

    try:
        # Fetch schema from registry
        fields = fetch_schema_fields("Sentiment")

        if not fields:
            yield ("error", 0.0)
            return

        # Create model and get AG instance
        model_class = create_dynamic_model("Sentiment", fields)
        ag = get_ag_for_type("Sentiment", model_class)

        # Perform transformation
        ag_result = run_async(ag << text)

        # Extract result
        if hasattr(ag_result, "states") and ag_result.states:
            result = ag_result.states[-1] if ag_result.states[-1] else ag_result
        else:
            result = ag_result

        # Convert to dict
        if hasattr(result, "model_dump"):
            result_dict = result.model_dump()
        elif hasattr(result, "dict"):
            result_dict = result.dict()
        elif isinstance(result, dict):
            result_dict = result
        else:
            result_dict = {"sentiment_label": "unknown", "sentiment_score": 0.0}

        # Extract values
        sentiment_label = result_dict.get("sentiment_label", "unknown")
        sentiment_score = result_dict.get("sentiment_score", 0.0)

        yield (sentiment_label, sentiment_score)

    except Exception as e:
        print(f"Error in ag_sentiment: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        yield ("error", 0.0)


# Made with Bob
