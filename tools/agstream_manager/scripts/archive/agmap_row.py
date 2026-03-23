"""
AGmap Row - Table-Valued Semantic Mapping UDF

Returns structured ROW type instead of JSON string for cleaner SQL queries.
This is a companion to agmap.py that provides tabular output.

Usage:
    SELECT
        customer_review,
        agmap_row('Sentiment', customer_review).sentiment_label as label,
        agmap_row('Sentiment', customer_review).sentiment_score as score
    FROM pr;
"""

import asyncio
import json
import os
import sys
import threading
from typing import Dict

import requests
from dotenv import load_dotenv
from pydantic import BaseModel, Field, create_model
from pyflink.table import DataTypes, Row
from pyflink.table.udf import udf, udtf

from agentics import AG

# Load environment variables
for env_path in ["/opt/flink/.env", "../../../.env", ".env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"✓ Loaded .env from {env_path}", file=sys.stderr)
        break


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


def run_async(coro):
    """Run async coroutine in sync context, handling existing event loops"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
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


def create_row_type_from_fields(fields: Dict[str, type]) -> DataTypes:
    """Create Flink ROW DataType from field definitions."""
    flink_fields = []
    for field_name, field_type in fields.items():
        # Map Python types to Flink DataTypes
        if field_type == str:
            flink_type = DataTypes.STRING()
        elif field_type == int:
            flink_type = DataTypes.BIGINT()
        elif field_type == float:
            flink_type = DataTypes.DOUBLE()
        elif field_type == bool:
            flink_type = DataTypes.BOOLEAN()
        else:
            flink_type = DataTypes.STRING()

        flink_fields.append(DataTypes.FIELD(field_name, flink_type))

    return DataTypes.ROW(flink_fields)


# Cache for ROW types by target_type
_row_type_cache = {}


def get_row_type_for_target(target_type: str) -> DataTypes:
    """Get or create ROW type for a target schema."""
    if target_type in _row_type_cache:
        return _row_type_cache[target_type]

    fields = fetch_schema_fields(target_type)
    if not fields:
        # Default to generic structure
        fields = {"result": str, "error": str}

    row_type = create_row_type_from_fields(fields)
    _row_type_cache[target_type] = row_type
    return row_type


@udf(
    result_type=DataTypes.ROW(
        [
            DataTypes.FIELD("sentiment_label", DataTypes.STRING()),
            DataTypes.FIELD("sentiment_score", DataTypes.DOUBLE()),
        ]
    )
)
def agmap_row(target_type, input_data):
    """
    Semantic mapping UDF that returns structured ROW type.

    Usage:
        SELECT
            agmap_row('Sentiment', customer_review).sentiment_label as label,
            agmap_row('Sentiment', customer_review).sentiment_score as score
        FROM pr;

    Args:
        target_type: Name of the target type in Schema Registry
        input_data: Input data (text or JSON string)

    Returns:
        Row object with structured fields
    """
    if not target_type or not input_data:
        return Row(sentiment_label="error", sentiment_score=0.0)

    try:
        # Fetch schema fields
        fields = fetch_schema_fields(target_type)

        if not fields:
            return Row(sentiment_label="schema_not_found", sentiment_score=0.0)

        # Create dynamic model
        model_class = create_dynamic_model(target_type, fields)

        # Get AG instance
        ag = get_ag_for_type(target_type, model_class)

        # Perform semantic transformation
        ag_result = run_async(ag << input_data)

        # Extract the actual model instance from AG's states
        # The AG object stores results in states[-1]
        if hasattr(ag_result, "states") and ag_result.states:
            # Get the last state which contains the result
            last_state = ag_result.states[-1]
            # The actual model instance should be in the state
            if last_state:
                result = last_state
            else:
                print(
                    f"DEBUG agmap_row: Empty state, using ag_result directly",
                    file=sys.stderr,
                )
                result = ag_result
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

        # Debug: Print the result
        print(
            f"DEBUG agmap_row: target_type={target_type}, result_dict={result_dict}",
            file=sys.stderr,
        )

        # Create Row with field values
        # Map fields: the LLM generates 'label' and 'confidence', but we need 'sentiment_label' and 'sentiment_score'
        sentiment_label = result_dict.get("sentiment_label") or result_dict.get(
            "label", "unknown"
        )
        sentiment_score = result_dict.get("sentiment_score") or result_dict.get(
            "confidence", 0.0
        )

        # Handle None values
        if sentiment_label is None:
            sentiment_label = "unknown"
        if sentiment_score is None:
            sentiment_score = 0.0
        else:
            sentiment_score = float(sentiment_score)

        print(
            f"DEBUG agmap_row: Returning label={sentiment_label}, score={sentiment_score}",
            file=sys.stderr,
        )

        return Row(sentiment_label=sentiment_label, sentiment_score=sentiment_score)

    except Exception as e:
        print(f"Error in agmap_row: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        return Row(sentiment_label="error", sentiment_score=0.0)


@udtf(result_types=[DataTypes.STRING(), DataTypes.DOUBLE()])
def agmap_table(target_type, input_data):
    """
    Table-valued function that returns multiple columns directly.

    Usage:
        SELECT T.sentiment_label, T.sentiment_score
        FROM pr, LATERAL TABLE(agmap_table('Sentiment', customer_review)) AS T(sentiment_label, sentiment_score)
        LIMIT 5;

    Or with original columns:
        SELECT customer_review, T.sentiment_label, T.sentiment_score
        FROM pr, LATERAL TABLE(agmap_table('Sentiment', customer_review)) AS T(sentiment_label, sentiment_score)
        LIMIT 5;

    Args:
        target_type: Name of the target type in Schema Registry
        input_data: Input data (text or JSON string)

    Yields:
        Tuple of (sentiment_label, sentiment_score)
    """
    if not target_type or not input_data:
        yield ("error", 0.0)
        return

    try:
        # Fetch schema fields
        fields = fetch_schema_fields(target_type)

        if not fields:
            yield ("schema_not_found", 0.0)
            return

        # Create dynamic model
        model_class = create_dynamic_model(target_type, fields)

        # Get AG instance
        ag = get_ag_for_type(target_type, model_class)

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

        # Extract fields
        sentiment_label = result_dict.get("sentiment_label") or result_dict.get(
            "label", "unknown"
        )
        sentiment_score = result_dict.get("sentiment_score") or result_dict.get(
            "confidence", 0.0
        )

        if sentiment_label is None:
            sentiment_label = "unknown"
        if sentiment_score is None:
            sentiment_score = 0.0
        else:
            sentiment_score = float(sentiment_score)

        yield (sentiment_label, sentiment_score)

    except Exception as e:
        print(f"Error in agmap_table: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        yield ("error", 0.0)


@udtf(result_types=[DataTypes.STRING()])
def agmap_table_dynamic(column_name, input_data, field_type="str", description=None):
    """
    Dynamic table-valued function that generates a type on-the-fly with a single field.

    This UDF creates a Pydantic model dynamically based on the provided column name and type,
    then performs a semantic transduction from the input data to this generated type.

    Usage:
        -- Basic usage with default string type
        SELECT T.category
        FROM pr, LATERAL TABLE(agmap_table_dynamic('category', customer_review)) AS T(category)
        LIMIT 5;

        -- With explicit type
        SELECT T.rating
        FROM pr, LATERAL TABLE(agmap_table_dynamic('rating', customer_review, 'float')) AS T(rating)
        LIMIT 5;

        -- With description for better LLM guidance
        SELECT T.emotion
        FROM pr, LATERAL TABLE(agmap_table_dynamic('emotion', customer_review, 'str',
                               'The primary emotion expressed in the text')) AS T(emotion)
        LIMIT 5;

    Args:
        column_name: Name of the output column/field to generate
        input_data: Input data (text or JSON string) to transduce
        field_type: Type of the field - 'str', 'int', 'float', 'bool' (default: 'str')
        description: Optional description to guide the LLM transduction (default: None)

    Yields:
        Tuple containing the single generated field value
    """
    if not column_name or not input_data:
        yield ("error",)
        return

    try:
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

        python_type = type_mapping.get(field_type.lower() if field_type else "str", str)

        # Create dynamic Pydantic model with single field
        field_definitions = {}
        if description:
            field_definitions[column_name] = (
                python_type | None,
                Field(default=None, description=description),
            )
        else:
            field_definitions[column_name] = (python_type | None, Field(default=None))

        # Generate unique model name
        model_name = f"Dynamic_{column_name}_{field_type or 'str'}"
        dynamic_model = create_model(model_name, **field_definitions)

        print(
            f"DEBUG agmap_table_dynamic: Created model {model_name} with field {column_name}:{python_type}",
            file=sys.stderr,
        )

        # Get or create AG instance for this dynamic type
        ag = get_ag_for_type(model_name, dynamic_model)

        # Add instructions based on description
        if description:
            ag.instructions = f"Extract or generate the '{column_name}' field from the input data. {description}"
        else:
            ag.instructions = (
                f"Extract or generate the '{column_name}' field from the input data."
            )

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
        print(f"Error in agmap_table_dynamic: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        yield ("error",)


# Made with Bob
