"""
AGmap - Generic Semantic Mapping UDF

A flexible UDF that performs semantic transformations using schemas from the Schema Registry.
Returns JSON string with all transformed fields.
"""

import asyncio
import json
import os
import sys
from typing import Dict

import requests
from dotenv import load_dotenv
from pydantic import BaseModel, Field, create_model
from pyflink.table import DataTypes
from pyflink.table.udf import udf

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
            print(f"  Trying subject: {subject}", file=sys.stderr)
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                subject_used = subject
                print(f"  ✓ Found schema at: {subject}", file=sys.stderr)
                break
            else:
                print(f"  ✗ Not found (status {response.status_code})", file=sys.stderr)

        if not subject_used:
            # List available subjects for debugging
            list_url = f"{registry_url}/subjects"
            list_response = requests.get(list_url, timeout=5)
            if list_response.status_code == 200:
                available = list_response.json()
                print(f"  Available subjects: {available}", file=sys.stderr)
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

        print(f"  Extracted fields: {fields}", file=sys.stderr)
        return fields

    except Exception as e:
        print(f"Error fetching schema for {type_name}: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        return {}


def create_dynamic_model(type_name: str, fields: Dict[str, type]) -> type:
    """
    Create a Pydantic model dynamically from field definitions.

    Args:
        type_name: Name of the model
        fields: Dictionary mapping field names to Python types

    Returns:
        Dynamically created Pydantic model class
    """
    field_definitions = {}
    for field_name, field_type in fields.items():
        # Make all fields optional with None default
        field_definitions[field_name] = (field_type | None, Field(default=None))

    return create_model(type_name, **field_definitions)


def run_async(coro):
    """Run async coroutine in sync context, handling existing event loops"""
    try:
        # Try to create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    except Exception as e:
        print(f"Error running async: {e}", file=sys.stderr)
        raise


# Global cache for AG instances per type
_ag_cache = {}


def get_ag_for_type(target_type: str, model_class: type):
    """Get or create AG instance for a specific type"""
    global _ag_cache

    if target_type not in _ag_cache:
        # Force reload .env to ensure variables are available
        for env_path in ["/opt/flink/.env", "../../../.env", ".env"]:
            if os.path.exists(env_path):
                load_dotenv(env_path, override=True)
                print(f"✓ Reloaded .env from {env_path}", file=sys.stderr)
                break

        # Check if we have API keys
        api_key = os.getenv("OPENAI_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
        print(
            f"DEBUG: OPENAI_API_KEY = {os.getenv('OPENAI_API_KEY')[:20] if os.getenv('OPENAI_API_KEY') else 'None'}...",
            file=sys.stderr,
        )
        print(
            f"DEBUG: ANTHROPIC_API_KEY = {os.getenv('ANTHROPIC_API_KEY')[:20] if os.getenv('ANTHROPIC_API_KEY') else 'None'}...",
            file=sys.stderr,
        )

        if not api_key:
            raise ValueError(
                "No API key found in environment. Please set OPENAI_API_KEY or ANTHROPIC_API_KEY"
            )

        # AG will use environment variables for LLM configuration
        _ag_cache[target_type] = AG(atype=model_class)
        print(
            f"✓ Created AG instance for {target_type} with API key: {api_key[:10]}...",
            file=sys.stderr,
        )

    return _ag_cache[target_type]


@udf(result_type=DataTypes.STRING())
def agmap(target_type, input_data):
    """
    Generic semantic mapping UDF.

    Fetches target schema from Schema Registry and performs semantic transformation.
    Returns JSON string with all fields.

    Usage:
        SELECT agmap('Answer', text) FROM Q LIMIT 1;
        SELECT agmap('Question', 'What is AI?') FROM Q LIMIT 1;

    Args:
        target_type: Name of the target type in Schema Registry
        input_data: Input data (text or JSON string)

    Returns:
        JSON string with transformed data
    """
    if not target_type or not input_data:
        return json.dumps({"error": "Missing parameters"})

    try:
        # Fetch schema fields
        fields = fetch_schema_fields(target_type)

        if not fields:
            return json.dumps({"error": f"Schema not found for {target_type}"})

        # Create dynamic Pydantic model
        model_class = create_dynamic_model(target_type, fields)

        # Get or create AG instance
        ag = get_ag_for_type(target_type, model_class)

        # Execute async operation synchronously
        ag_result = run_async(ag << input_data)

        # Debug: Log what we received
        print(f"DEBUG: Received AG result type: {type(ag_result)}", file=sys.stderr)

        # Extract states from AG object (same pattern as generate_sentiment)
        if hasattr(ag_result, "states") and ag_result.states:
            states = ag_result.states
            print(f"DEBUG: Found states: {states}", file=sys.stderr)

            if isinstance(states, list) and len(states) > 0:
                result_obj = states[0]
                print(f"DEBUG: First state: {result_obj}", file=sys.stderr)
            else:
                result_obj = states
        else:
            result_obj = ag_result

        # Convert to dict
        result_dict = {}
        for field_name in fields.keys():
            value = getattr(result_obj, field_name, None)
            result_dict[field_name] = value
            print(f"DEBUG: Extracted {field_name} = {value}", file=sys.stderr)

        return json.dumps(result_dict)

    except Exception as e:
        # Log error with details
        import traceback

        error_msg = f"Error in agmap: {e}\n{traceback.format_exc()}"
        print(error_msg, file=sys.stderr)
        return json.dumps({"error": f"ERROR: {str(e)[:100]}"})


# Test function
def test_agmap():
    """Test the AGmap UDF."""
    print("=" * 60)
    print("Testing AGmap UDF")
    print("=" * 60)

    print("\nTo test in Flink SQL:")
    print("1. Register the function:")
    print("   CREATE TEMPORARY SYSTEM FUNCTION agmap")
    print("   AS 'agmap.agmap' LANGUAGE PYTHON;")
    print("\n2. Use it with existing schemas:")
    print("   SELECT agmap('Answer', 'What is the capital of France?') FROM Q LIMIT 1;")
    print("   SELECT agmap('Question', 'Tell me about AI') FROM Q LIMIT 1;")
    print("\n3. Extract fields from JSON:")
    print("   SELECT ")
    print("     text,")
    print("     JSON_VALUE(agmap('Answer', text), '$.text') as answer_text")
    print("   FROM Q LIMIT 10;")


if __name__ == "__main__":
    test_agmap()

# Made with Bob
