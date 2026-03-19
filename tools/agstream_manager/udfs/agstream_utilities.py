"""
AGStream Utilities UDFs

Utility functions for interacting with the AGStream environment,
including querying registered types and schemas from the Schema Registry.
"""

import json
import os
import sys
from typing import Iterator, Optional

import requests
from dotenv import load_dotenv
from pyflink.table import DataTypes
from pyflink.table.udf import TableFunction, udf, udtf

# Load environment variables
for env_path in ["/opt/flink/.env", "../../../.env", ".env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"✓ Loaded .env from {env_path}", file=sys.stderr)
        break


# Get Schema Registry URL from environment
# Try multiple possible URLs for different environments
def get_schema_registry_url():
    """Get the Schema Registry URL from environment or use default."""
    # First check environment variable
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


@udf(result_type=DataTypes.STRING())
def list_registered_types():
    """
    List all registered types (subjects) in the Schema Registry.

    Returns:
        JSON string containing array of registered type names

    Example:
        SELECT list_registered_types();
        -- Returns: ["Question", "Answer", "SentimentOutput", ...]
    """
    try:
        url = f"{get_schema_registry_url()}/subjects"
        response = requests.get(url, timeout=5)
        response.raise_for_status()

        subjects = response.json()

        # Filter out -key and -value suffixes to get unique type names
        type_names = set()
        for subject in subjects:
            # Remove -key or -value suffix if present
            if subject.endswith("-key"):
                type_name = subject[:-4]
            elif subject.endswith("-value"):
                type_name = subject[:-6]
            else:
                type_name = subject
            type_names.add(type_name)

        return json.dumps(sorted(list(type_names)))

    except requests.exceptions.RequestException as e:
        return json.dumps({"error": f"Failed to connect to Schema Registry: {str(e)}"})
    except Exception as e:
        return json.dumps({"error": f"Error listing types: {str(e)}"})


@udf(result_type=DataTypes.STRING())
def get_type_schema(type_name: str):
    """
    Get the schema for a specific registered type.

    Args:
        type_name: Name of the type to retrieve schema for

    Returns:
        JSON string containing the schema definition

    Example:
        SELECT get_type_schema('Question');
        -- Returns: {"type":"record","name":"Question","fields":[...]}
    """
    if type_name is None:
        return json.dumps({"error": "Type name cannot be null"})

    try:
        # Try with -value suffix first (most common for Kafka topics)
        subject = f"{type_name}-value"
        url = f"{get_schema_registry_url()}/subjects/{subject}/versions/latest"

        response = requests.get(url, timeout=5)

        # If -value doesn't exist, try without suffix
        if response.status_code == 404:
            subject = type_name
            url = f"{get_schema_registry_url()}/subjects/{subject}/versions/latest"
            response = requests.get(url, timeout=5)

        # If still not found, try -key suffix
        if response.status_code == 404:
            subject = f"{type_name}-key"
            url = f"{get_schema_registry_url()}/subjects/{subject}/versions/latest"
            response = requests.get(url, timeout=5)

        response.raise_for_status()

        schema_data = response.json()

        # Parse the schema string to JSON
        schema = json.loads(schema_data["schema"])

        # Return formatted schema with metadata
        result = {
            "subject": subject,
            "version": schema_data["version"],
            "id": schema_data["id"],
            "schema": schema,
        }

        return json.dumps(result, indent=2)

    except requests.exceptions.RequestException as e:
        return json.dumps(
            {"error": f"Failed to retrieve schema for '{type_name}': {str(e)}"}
        )
    except json.JSONDecodeError as e:
        return json.dumps({"error": f"Failed to parse schema JSON: {str(e)}"})
    except Exception as e:
        return json.dumps({"error": f"Error retrieving schema: {str(e)}"})


@udf(result_type=DataTypes.STRING())
def get_type_fields(type_name: str):
    """
    Get just the field names and types for a specific registered type.

    Args:
        type_name: Name of the type to retrieve fields for

    Returns:
        JSON string containing field definitions

    Example:
        SELECT get_type_fields('Question');
        -- Returns: {"text": "string", "timestamp": "long"}
    """
    if type_name is None:
        return json.dumps({"error": "Type name cannot be null"})

    try:
        # Fetch schema directly (don't call get_type_schema to avoid pickling issues)
        subject = f"{type_name}-value"
        url = f"{get_schema_registry_url()}/subjects/{subject}/versions/latest"

        response = requests.get(url, timeout=5)

        # If -value doesn't exist, try without suffix
        if response.status_code == 404:
            subject = type_name
            url = f"{get_schema_registry_url()}/subjects/{subject}/versions/latest"
            response = requests.get(url, timeout=5)

        # If still not found, try -key suffix
        if response.status_code == 404:
            subject = f"{type_name}-key"
            url = f"{get_schema_registry_url()}/subjects/{subject}/versions/latest"
            response = requests.get(url, timeout=5)

        response.raise_for_status()

        schema_data = response.json()
        schema = json.loads(schema_data["schema"])

        # Extract just the fields
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
                        field_type = str(field_type)
                elif isinstance(field_type, list):
                    # Handle union types (e.g., ["null", "string"])
                    non_null_types = [t for t in field_type if t != "null"]
                    field_type = non_null_types[0] if non_null_types else "null"

                fields[field_name] = field_type

        return json.dumps(fields, indent=2)

    except requests.exceptions.RequestException as e:
        return json.dumps(
            {"error": f"Failed to retrieve schema for '{type_name}': {str(e)}"}
        )
    except json.JSONDecodeError as e:
        return json.dumps({"error": f"Failed to parse schema JSON: {str(e)}"})
    except Exception as e:
        return json.dumps({"error": f"Error extracting fields: {str(e)}"})


@udf(result_type=DataTypes.STRING())
def schema_registry_info():
    """
    Get information about the Schema Registry connection.

    Returns:
        JSON string containing Schema Registry URL and status

    Example:
        SELECT schema_registry_info();
        -- Returns: {"url": "http://localhost:8081", "status": "connected", "subjects_count": 5}
    """
    try:
        url = get_schema_registry_url()
        subjects_url = f"{url}/subjects"

        response = requests.get(subjects_url, timeout=5)
        response.raise_for_status()

        subjects = response.json()

        return json.dumps(
            {
                "url": url,
                "status": "connected",
                "subjects_count": len(subjects),
                "subjects": subjects[:10],  # Show first 10
            },
            indent=2,
        )

    except requests.exceptions.RequestException as e:
        return json.dumps(
            {"url": get_schema_registry_url(), "status": "error", "error": str(e)}
        )
    except Exception as e:
        return json.dumps(
            {
                "url": get_schema_registry_url(),
                "status": "error",
                "error": f"Unexpected error: {str(e)}",
            }
        )


@udf(result_type=DataTypes.BOOLEAN())
def type_exists(type_name: str):
    """
    Check if a type is registered in the Schema Registry.

    Args:
        type_name: Name of the type to check

    Returns:
        Boolean indicating if the type exists

    Example:
        SELECT type_exists('Question');
        -- Returns: true or false
    """
    if type_name is None:
        return False

    try:
        # Fetch types directly (don't call list_registered_types to avoid pickling issues)
        url = f"{get_schema_registry_url()}/subjects"
        response = requests.get(url, timeout=5)
        response.raise_for_status()

        subjects = response.json()

        # Filter out -key and -value suffixes to get unique type names
        type_names = set()
        for subject in subjects:
            # Remove -key or -value suffix if present
            if subject.endswith("-key"):
                type_name_clean = subject[:-4]
            elif subject.endswith("-value"):
                type_name_clean = subject[:-6]
            else:
                type_name_clean = subject
            type_names.add(type_name_clean)

        return type_name in type_names

    except Exception:
        return False


# Helper function for testing (not a UDF)
def test_utilities():
    """Test the utility functions."""
    print("=" * 60)
    print("Testing AGStream Utilities UDFs")
    print("=" * 60)

    print("\n=== Test 1: Schema Registry Info ===")
    info = schema_registry_info()
    print(info)

    print("\n=== Test 2: List Registered Types ===")
    types = list_registered_types()
    print(types)

    print("\n=== Test 3: Get Type Schema ===")
    # Try to get schema for first type if any exist
    types_list = json.loads(types)
    if isinstance(types_list, list) and len(types_list) > 0:
        first_type = types_list[0]
        print(f"Getting schema for: {first_type}")
        schema = get_type_schema(first_type)
        print(schema)

        print(f"\n=== Test 4: Get Type Fields for {first_type} ===")
        fields = get_type_fields(first_type)
        print(fields)

        print(f"\n=== Test 5: Check if {first_type} exists ===")
        exists = type_exists(first_type)
        print(f"Type exists: {exists}")

    print("\n=== Test 6: Check non-existent type ===")
    exists = type_exists("NonExistentType")
    print(f"NonExistentType exists: {exists}")

    print("\n" + "=" * 60)
    print("All tests completed!")
    print("=" * 60)


# UDTF for listing types as rows
class ListRegisteredTypesUDTF(TableFunction):
    """
    Table function that returns each registered type as a separate row.

    Usage:
        SELECT * FROM TABLE(list_types_table());

    Returns:
        Multiple rows, each containing one type name
    """

    def eval(self):
        """Yield each type as a separate row."""
        try:
            url = f"{get_schema_registry_url()}/subjects"
            response = requests.get(url, timeout=5)
            response.raise_for_status()

            subjects = response.json()

            # Filter out -key and -value suffixes to get unique type names
            type_names = set()
            for subject in subjects:
                # Remove -key or -value suffix if present
                if subject.endswith("-key"):
                    type_name = subject[:-4]
                elif subject.endswith("-value"):
                    type_name = subject[:-6]
                else:
                    type_name = subject
                type_names.add(type_name)

            # Yield each type as a separate row
            for type_name in sorted(type_names):
                yield (type_name,)

        except Exception as e:
            # Yield error as a row
            yield (f"ERROR: {str(e)}",)


# Create the UDTF instance
list_types_table = udtf(ListRegisteredTypesUDTF(), result_types=[DataTypes.STRING()])


if __name__ == "__main__":
    test_utilities()

# Made with Bob
