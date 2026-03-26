"""
aggenerate UDF - Generate prototypical instances of a given type

This UDF generates instances of types using LLM-powered generation.
It supports both single-field dynamic types and registry-based types.

Usage:
    -- Dynamic mode (single field)
    SELECT T.generated_text
    FROM (VALUES (1)) AS dummy(x),
    LATERAL TABLE(aggenerate('5', 'Creative tech product names')) AS T(generated_text)
    LIMIT 5;

    -- Registry mode (multi-field from schema registry)
    SELECT T.generated_text
    FROM (VALUES (1)) AS dummy(x),
    LATERAL TABLE(aggenerate('5', 'ProductReview', '', 'registry')) AS T(generated_text)
    LIMIT 5;
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
from pyflink.table import DataTypes
from pyflink.table.udf import udtf

from agentics.core.transducible_functions import generate_prototypical_instances

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
        print(
            f"DEBUG aggenerate: Fetching schema for '{type_name}' from {registry_url}",
            file=sys.stderr,
        )

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
            print(
                f"DEBUG aggenerate: Schema '{type_name}' not found in registry",
                file=sys.stderr,
            )
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

        print(
            f"DEBUG aggenerate: Found {len(fields)} fields in schema: {list(fields.keys())}",
            file=sys.stderr,
        )
        return fields

    except Exception as e:
        print(
            f"DEBUG aggenerate: Error fetching schema for {type_name}: {e}",
            file=sys.stderr,
        )
        import traceback

        traceback.print_exc(file=sys.stderr)
        return {}


def create_dynamic_model(type_name: str, fields: Dict[str, type]) -> type:
    """Create a Pydantic model dynamically from field definitions."""
    field_definitions = {}
    for field_name, field_type in fields.items():
        # Convert string type names to actual types
        if field_type == "str" or field_type == "string":
            python_type = str
        elif field_type == "int" or field_type == "integer":
            python_type = int
        elif field_type == "float" or field_type == "double":
            python_type = float
        elif field_type == "bool" or field_type == "boolean":
            python_type = bool
        else:
            python_type = str

        field_definitions[field_name] = (python_type | None, Field(default=None))

    return create_model(type_name, **field_definitions)


def run_async(coro, timeout=180):
    """Run async coroutine in sync context with timeout."""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(asyncio.wait_for(coro, timeout=timeout))
        finally:
            loop.close()
    except asyncio.TimeoutError:
        print(f"Error running async: Timeout after {timeout}s", file=sys.stderr)
        raise
    except Exception as e:
        print(f"Error running async: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        raise


def ensure_llm_configured():
    """Ensure at least one LLM API key is configured."""
    # Force reload .env
    for env_path in ["/opt/flink/.env", "../../../.env", ".env"]:
        if os.path.exists(env_path):
            load_dotenv(env_path, override=True)
            break

    # Check API keys
    api_key = (
        os.getenv("OPENAI_API_KEY")
        or os.getenv("ANTHROPIC_API_KEY")
        or os.getenv("GOOGLE_API_KEY")
    )
    if not api_key:
        raise ValueError(
            "No LLM API key found. Please set OPENAI_API_KEY, ANTHROPIC_API_KEY, or GOOGLE_API_KEY in your .env file"
        )


async def generate_instances_for_type(
    type_name: str,
    field_name: str,
    field_type: str,
    n_instances: int,
    instructions: str = "",
) -> list[BaseModel]:
    """
    Generate instances of a dynamically created type.

    Args:
        type_name: Name for the generated type
        field_name: Name of the field
        field_type: Type of the field (str, int, float, bool)
        n_instances: Number of instances to generate
        instructions: Additional instructions for generation

    Returns:
        List of generated Pydantic model instances
    """
    # Create dynamic model using create_model (no LLM needed for type creation)
    model_class = create_dynamic_model(type_name, {field_name: field_type})

    print(
        f"DEBUG aggenerate: Created dynamic model {type_name} with field {field_name}:{field_type}",
        file=sys.stderr,
    )
    print(
        f"DEBUG aggenerate: Model schema: {model_class.model_json_schema()}",
        file=sys.stderr,
    )

    # Get LLM provider - use get_llm_provider() which respects SELECTED_LLM env var
    from agentics.core.llm_connections import get_llm_provider

    llm = get_llm_provider()

    if llm is None:
        raise ValueError(
            "No LLM provider available. Please configure OPENAI_API_KEY, ANTHROPIC_API_KEY, or WATSONX credentials"
        )

    print(f"DEBUG aggenerate: Selected LLM: {type(llm).__name__}", file=sys.stderr)
    if hasattr(llm, "model"):
        print(f"DEBUG aggenerate: LLM model: {llm.model}", file=sys.stderr)

    # Generate instances using the agentics function
    full_instructions = (
        instructions if instructions else f"Generate realistic values for {field_name}"
    )

    print(
        f"DEBUG aggenerate: Calling generate_prototypical_instances with n_instances={n_instances}",
        file=sys.stderr,
    )
    print(f"DEBUG aggenerate: Instructions: {full_instructions}", file=sys.stderr)

    try:
        instances = await generate_prototypical_instances(
            type=model_class,
            n_instances=n_instances,
            llm=llm,
            instructions=full_instructions,
        )

        print(
            f"DEBUG aggenerate: generate_prototypical_instances returned: {type(instances)}",
            file=sys.stderr,
        )

        if instances is None:
            print(f"DEBUG aggenerate: instances is None", file=sys.stderr)
            return []

        if not isinstance(instances, list):
            print(
                f"DEBUG aggenerate: instances is not a list, type={type(instances)}",
                file=sys.stderr,
            )
            return []

        print(
            f"DEBUG aggenerate: Generated {len(instances)} instances", file=sys.stderr
        )
        return instances

    except Exception as e:
        print(
            f"DEBUG aggenerate: Exception in generate_prototypical_instances: {e}",
            file=sys.stderr,
        )
        import traceback

        traceback.print_exc(file=sys.stderr)
        return []


#######################
### Main UDF: aggenerate ###
#######################


@udtf(result_types=[DataTypes.STRING()])
def aggenerate(
    n_instances, description, field_name="", field_type="str", instructions=""
):
    """
    Generate prototypical instances of a given type.

    Args:
        n_instances: Number of instances to generate (REQUIRED - must be a string like '5' or '10')
        description: Description to guide generation OR field definitions in format "field1:type1,field2:type2"
        field_name: Name of the field to generate (optional, default: "")
        field_type: Type of the field: 'str', 'int', 'float', 'bool', 'json', 'registry' (optional, default: "str")
        instructions: Additional instructions for generation (optional, default: "")

    Yields:
        Exactly n_instances generated values (one per row)

    Examples:
        -- Generate 5 product names (simple mode)
        SELECT T.generated_text
        FROM (VALUES (1)) AS dummy(x),
        LATERAL TABLE(aggenerate('5', 'Creative tech product names')) AS T(generated_text)
        LIMIT 5;

        -- Generate JSON with multiple fields (json mode)
        SELECT T.generated_text
        FROM (VALUES (1)) AS dummy(x),
        LATERAL TABLE(aggenerate('5', 'Product_name:str,customer_review:str', '', 'json', 'Generate product reviews')) AS T(generated_text)
        LIMIT 5;

        -- Extract fields from JSON output
        SELECT
            JSON_VALUE(T.generated_text, '$.Product_name') AS Product_name,
            JSON_VALUE(T.generated_text, '$.customer_review') AS customer_review
        FROM (VALUES (1)) AS dummy(x),
        LATERAL TABLE(aggenerate('5', 'Product_name:str,customer_review:str', '', 'json')) AS T(generated_text)
        LIMIT 5;

        -- Generate from registry (requires Schema Registry connection)
        SELECT T.generated_text
        FROM (VALUES (1)) AS dummy(x),
        LATERAL TABLE(aggenerate('5', 'ProductReview', '', 'registry')) AS T(generated_text)
        LIMIT 5;
    """
    try:
        # Convert and validate n_instances
        try:
            n_instances_int = (
                int(n_instances) if n_instances and n_instances != "" else 10
            )
            if n_instances_int < 1:
                yield ("error: n_instances must be >= 1",)
                return
            n_instances_int = min(n_instances_int, 100)  # Cap at 100
        except (ValueError, TypeError):
            yield ("error: n_instances must be a number",)
            return

        # Handle required and optional parameters
        description_str = description  # Required parameter
        field_name_str = field_name if field_name and field_name != "" else "value"
        field_type_str = field_type if field_type and field_type != "" else "str"
        instructions_str = instructions if instructions and instructions != "" else ""

        # Ensure LLM is configured
        ensure_llm_configured()

        print(
            f"DEBUG aggenerate: Generating {n_instances_int} instances of '{field_name_str}' (type: {field_type_str})",
            file=sys.stderr,
        )
        if description_str:
            print(f"DEBUG aggenerate: Description: {description_str}", file=sys.stderr)
        if instructions_str:
            print(
                f"DEBUG aggenerate: Instructions: {instructions_str}", file=sys.stderr
            )

        # Check if using json mode (multi-field without registry)
        if field_type_str.lower() == "json":
            print(
                f"DEBUG aggenerate: Using JSON mode for multi-field generation",
                file=sys.stderr,
            )

            # Parse field definitions from description: "field1:type1,field2:type2"
            try:
                fields = {}
                for field_def in description_str.split(","):
                    field_def = field_def.strip()
                    if ":" in field_def:
                        fname, ftype = field_def.split(":", 1)
                        fname = fname.strip()
                        ftype = ftype.strip().lower()

                        # Map type names to Python types
                        type_map = {
                            "str": str,
                            "string": str,
                            "int": int,
                            "integer": int,
                            "float": float,
                            "double": float,
                            "bool": bool,
                            "boolean": bool,
                        }
                        fields[fname] = type_map.get(ftype, str)
                    else:
                        # Default to string type if no type specified
                        fields[field_def] = str

                if not fields:
                    yield (
                        "error: no fields specified in json mode. Use format 'field1:type1,field2:type2'",
                    )
                    return

                print(
                    f"DEBUG aggenerate: Parsed {len(fields)} fields: {list(fields.keys())}",
                    file=sys.stderr,
                )

            except Exception as e:
                yield (f"error: failed to parse field definitions: {str(e)}",)
                return

            # Create dynamic model from parsed fields
            type_name = "GeneratedType"
            model_class = create_dynamic_model(type_name, fields)

            # Get LLM provider
            from agentics.core.llm_connections import get_llm_provider

            llm = get_llm_provider()
            if llm is None:
                yield ("error: No LLM provider available",)
                return

            print(
                f"DEBUG aggenerate: Selected LLM: {type(llm).__name__}", file=sys.stderr
            )

            # Generate instances
            full_instructions = (
                instructions_str
                if instructions_str
                else f"Generate realistic data with fields: {', '.join(fields.keys())}"
            )

            print(
                f"DEBUG aggenerate: Calling generate_prototypical_instances for json mode",
                file=sys.stderr,
            )

            try:
                instances = run_async(
                    generate_prototypical_instances(
                        type=model_class,
                        n_instances=n_instances_int,
                        llm=llm,
                        instructions=full_instructions,
                    ),
                    timeout=180,
                )

                if not instances or len(instances) == 0:
                    print(
                        "DEBUG aggenerate: No instances generated (empty list)",
                        file=sys.stderr,
                    )
                    yield ("error: no instances generated",)
                    return

                print(
                    f"DEBUG aggenerate: Generated {len(instances)} instances in json mode",
                    file=sys.stderr,
                )

                # Yield each instance as JSON (multi-field)
                for instance in instances:
                    if isinstance(instance, BaseModel):
                        yield (instance.model_dump_json(),)
                    else:
                        yield (str(instance),)

            except Exception as e:
                print(
                    f"DEBUG aggenerate: Error generating instances: {e}",
                    file=sys.stderr,
                )
                import traceback

                traceback.print_exc(file=sys.stderr)
                yield (f"error: {str(e)}",)

            return

        # Check if using registry mode
        if field_type_str.lower() == "registry":
            print(
                f"DEBUG aggenerate: Using REGISTRY mode for type '{description_str}'",
                file=sys.stderr,
            )

            # In registry mode, description is the type name
            type_name = description_str

            # Fetch schema from registry
            try:
                fields = fetch_schema_fields(type_name)
                if not fields:
                    yield (
                        f"error: schema '{type_name}' not found in registry or registry not accessible",
                    )
                    return
            except Exception as e:
                yield (f"error: failed to fetch schema from registry: {str(e)}",)
                return

            # Create dynamic model from registry schema
            model_class = create_dynamic_model(type_name, fields)

            print(
                f"DEBUG aggenerate: Created model from registry with {len(fields)} fields: {list(fields.keys())}",
                file=sys.stderr,
            )

            # Get LLM provider
            from agentics.core.llm_connections import get_llm_provider

            llm = get_llm_provider()
            if llm is None:
                yield ("error: No LLM provider available",)
                return

            print(
                f"DEBUG aggenerate: Selected LLM: {type(llm).__name__}", file=sys.stderr
            )

            # Generate instances
            full_instructions = (
                instructions_str
                if instructions_str
                else f"Generate realistic instances of {type_name}"
            )

            print(
                f"DEBUG aggenerate: Calling generate_prototypical_instances for registry type",
                file=sys.stderr,
            )

            try:
                instances = run_async(
                    generate_prototypical_instances(
                        type=model_class,
                        n_instances=n_instances_int,
                        llm=llm,
                        instructions=full_instructions,
                    ),
                    timeout=180,
                )

                if not instances or len(instances) == 0:
                    print(
                        "DEBUG aggenerate: No instances generated (empty list)",
                        file=sys.stderr,
                    )
                    yield ("error: no instances generated",)
                    return

                print(
                    f"DEBUG aggenerate: Generated {len(instances)} instances from registry",
                    file=sys.stderr,
                )

                # Yield each instance as JSON (multi-field)
                for instance in instances:
                    if isinstance(instance, BaseModel):
                        yield (instance.model_dump_json(),)
                    else:
                        yield (str(instance),)

            except Exception as e:
                print(
                    f"DEBUG aggenerate: Error generating instances: {e}",
                    file=sys.stderr,
                )
                import traceback

                traceback.print_exc(file=sys.stderr)
                yield (f"error: {str(e)}",)

            return

        # Dynamic mode (single field)
        # Validate field type
        valid_types = [
            "str",
            "string",
            "int",
            "integer",
            "float",
            "double",
            "bool",
            "boolean",
        ]
        if field_type_str.lower() not in valid_types:
            yield (
                f"error: invalid field_type '{field_type_str}'. Must be one of: {', '.join(valid_types)}, 'json', or 'registry'",
            )
            return

        # Create a type name from the description
        type_name = description_str.replace(" ", "").replace("-", "")[:50] + "Type"

        # Combine description and instructions
        full_instructions = description_str
        if instructions_str:
            full_instructions += f". {instructions_str}"

        # Generate instances
        instances = run_async(
            generate_instances_for_type(
                type_name=type_name,
                field_name=field_name_str,
                field_type=field_type_str,
                n_instances=n_instances_int,
                instructions=full_instructions,
            ),
            timeout=180,
        )

        if not instances or len(instances) == 0:
            print(
                "DEBUG aggenerate: No instances generated (empty list)", file=sys.stderr
            )
            yield ("error: no instances generated",)
            return

        print(
            f"DEBUG aggenerate: Generated {len(instances)} instances", file=sys.stderr
        )

        # Yield each generated instance as a simple string
        for instance in instances:
            if isinstance(instance, BaseModel):
                # Get the field value from the Pydantic model
                field_value = getattr(instance, field_name_str, None)
                if field_value is not None:
                    yield (str(field_value),)
                else:
                    # Fallback: try to get any field
                    fields = list(instance.model_fields.keys())
                    if fields:
                        field_value = getattr(instance, fields[0])
                        yield (str(field_value),)
                    else:
                        yield (instance.model_dump_json(),)
            else:
                yield (str(instance),)

    except Exception as e:
        print(f"Error in aggenerate: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        yield (f"error: {str(e)}",)


# Made with Bob
