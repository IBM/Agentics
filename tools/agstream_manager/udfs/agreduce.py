"""
AGreduce - Aggregate UDF for Semantic Reduction

Performs agentic reduce (areduce) on all table rows at once, aggregating
them into a single result of the target type. This is the streaming SQL
equivalent of the sem_agg operator.

Usage:
    -- Aggregate all reviews into a summary
    SELECT agreduce('summary', 'str', 'Summarize the overall sentiment') as summary
    FROM pr;

    -- Aggregate with registry schema
    SELECT agreduce('ReviewSummary', 'registry') as summary
    FROM pr;
"""

import asyncio
import json
import os
import sys
import threading
from typing import Dict, List

import requests
from dotenv import load_dotenv
from pydantic import BaseModel, Field, create_model
from pyflink.table import DataTypes
from pyflink.table.udf import AggregateFunction, udaf

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


def get_ag_for_type(target_type: str, model_class: type, instructions: str = None):
    """Get or create AG instance for a specific type using thread-local storage."""
    cache_key = f"{target_type}_{instructions or 'default'}"

    if not hasattr(_thread_local, "ag_cache"):
        _thread_local.ag_cache = {}

    if cache_key in _thread_local.ag_cache:
        return _thread_local.ag_cache[cache_key]

    # Force reload .env
    for env_path in ["/opt/flink/.env", "../../../.env", ".env"]:
        if os.path.exists(env_path):
            load_dotenv(env_path, override=True)
            break

    # Create and cache AG instance with areduce transduction
    # Will use default LLM if no API keys are set
    ag_instance = AG(
        atype=model_class, instructions=instructions, transduction_type="areduce"
    )
    _thread_local.ag_cache[cache_key] = ag_instance

    return ag_instance


#######################
### Accumulator Class ###
#######################


class ReduceAccumulator:
    """Accumulator for collecting all rows before reduction."""

    def __init__(self):
        self.rows: List[str] = []
        self.column_name: str = None
        self.field_type: str = None
        self.description: str = None


#######################
### Aggregate Function ###
#######################


class AgReduceFunction(AggregateFunction):
    """
    Aggregate function that collects all rows and performs areduce.

    This is a UDAF (User-Defined Aggregate Function) that:
    1. Accumulates all input rows
    2. On get_value(), performs areduce on all accumulated data
    3. Returns the aggregated result
    """

    def create_accumulator(self):
        """Create a new accumulator."""
        return ReduceAccumulator()

    def accumulate(
        self,
        acc: ReduceAccumulator,
        input_data: str,
        column_name: str,
        field_type: str = "str",
        description: str = None,
    ):
        """
        Accumulate a row.

        Args:
            acc: The accumulator
            input_data: The input data from this row
            column_name: Column name (or schema name for registry mode)
            field_type: Type specification ('str', 'int', 'float', 'bool', 'registry')
            description: Optional description for dynamic mode
        """
        if input_data:
            acc.rows.append(input_data)

        # Store parameters (they should be the same for all rows)
        if acc.column_name is None:
            acc.column_name = column_name
            acc.field_type = field_type or "str"
            acc.description = description

    def get_value(self, acc: ReduceAccumulator) -> str:
        """
        Compute the final aggregated result using areduce.

        Returns:
            JSON string containing the reduced result
        """
        if not acc.rows or not acc.column_name:
            return json.dumps({"error": "no_data"})

        try:
            print(f"DEBUG agreduce: Processing {len(acc.rows)} rows", file=sys.stderr)

            # REGISTRY MODE: Use pre-registered schema
            if acc.field_type and acc.field_type.lower() == "registry":
                # Fetch schema fields from registry
                fields = fetch_schema_fields(acc.column_name)

                if not fields:
                    return json.dumps({"error": "schema_not_found"})

                # Create dynamic model from schema
                model_class = create_dynamic_model(acc.column_name, fields)

                # Get AG instance with areduce
                ag = get_ag_for_type(acc.column_name, model_class, acc.description)

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
                    acc.field_type.lower() if acc.field_type else "str", str
                )

                # Create dynamic Pydantic model with single field
                field_definitions = {}
                if acc.description:
                    field_definitions[acc.column_name] = (
                        python_type | None,
                        Field(default=None, description=acc.description),
                    )
                else:
                    field_definitions[acc.column_name] = (
                        python_type | None,
                        Field(default=None),
                    )

                # Generate unique model name
                model_name = f"Reduce_{acc.column_name}_{acc.field_type or 'str'}"
                dynamic_model = create_model(model_name, **field_definitions)

                print(
                    f"DEBUG agreduce: Created model {model_name} with field {acc.column_name}:{python_type}",
                    file=sys.stderr,
                )

                # Build instructions
                instructions = (
                    acc.description
                    or f"Aggregate all inputs to produce the '{acc.column_name}' field."
                )

                # Get or create AG instance with areduce
                ag = get_ag_for_type(model_name, dynamic_model, instructions)

            # For areduce, we need to create a source AG with the input data
            # Create a simple wrapper model for the input strings
            from pydantic import BaseModel as PydanticBaseModel

            class InputWrapper(PydanticBaseModel):
                text: str

            # Create source AG with input data
            source_ag = AG(atype=InputWrapper)
            for row_data in acc.rows:
                source_ag.append(InputWrapper(text=row_data))

            print(
                f"DEBUG agreduce: Created source AG with {len(source_ag.states)} states",
                file=sys.stderr,
            )

            # Perform areduce - pass the source AG
            ag_result = run_async(ag << source_ag)

            print(
                f"DEBUG agreduce: Got result type: {type(ag_result)}", file=sys.stderr
            )
            print(f"DEBUG agreduce: ag_result = {ag_result}", file=sys.stderr)

            # Extract the result - check if states were generated
            if (
                hasattr(ag_result, "states")
                and ag_result.states
                and len(ag_result.states) > 0
            ):
                # Get the last state (the reduced result)
                result = ag_result.states[-1]
                print(
                    f"DEBUG agreduce: Extracted state from ag_result.states, type: {type(result)}",
                    file=sys.stderr,
                )
            else:
                # No states generated - this shouldn't happen with areduce but handle it
                print(
                    f"DEBUG agreduce: WARNING - No states generated, using ag_result directly",
                    file=sys.stderr,
                )
                result = ag_result

            # Convert result to dictionary
            if hasattr(result, "model_dump"):
                result_dict = result.model_dump()
                print(
                    f"DEBUG agreduce: Used model_dump(), result_dict = {result_dict}",
                    file=sys.stderr,
                )
            elif hasattr(result, "dict"):
                result_dict = result.dict()
                print(
                    f"DEBUG agreduce: Used dict(), result_dict = {result_dict}",
                    file=sys.stderr,
                )
            elif isinstance(result, dict):
                result_dict = result
                print(
                    f"DEBUG agreduce: Result was already dict, result_dict = {result_dict}",
                    file=sys.stderr,
                )
            else:
                # Fallback - wrap in field name
                result_dict = {acc.column_name: str(result)}
                print(
                    f"DEBUG agreduce: Wrapped result in field name, result_dict = {result_dict}",
                    file=sys.stderr,
                )

            print(f"DEBUG agreduce: Final result_dict = {result_dict}", file=sys.stderr)

            # Return as JSON string
            return json.dumps(result_dict)

        except Exception as e:
            print(f"Error in agreduce.get_value: {e}", file=sys.stderr)
            import traceback

            traceback.print_exc(file=sys.stderr)
            return json.dumps({"error": str(e)})

    def merge(self, acc1: ReduceAccumulator, acc2: ReduceAccumulator):
        """
        Merge two accumulators (for parallel processing).

        Args:
            acc1: First accumulator (will be modified)
            acc2: Second accumulator (will be merged into acc1)
        """
        acc1.rows.extend(acc2.rows)
        if acc1.column_name is None:
            acc1.column_name = acc2.column_name
            acc1.field_type = acc2.field_type
            acc1.description = acc2.description

    def get_result_type(self):
        """Return the result type (STRING for JSON output)."""
        return DataTypes.STRING()

    def get_accumulator_type(self):
        """Return the accumulator type."""
        return DataTypes.ROW(
            [
                DataTypes.FIELD("rows", DataTypes.ARRAY(DataTypes.STRING())),
                DataTypes.FIELD("column_name", DataTypes.STRING()),
                DataTypes.FIELD("field_type", DataTypes.STRING()),
                DataTypes.FIELD("description", DataTypes.STRING()),
            ]
        )


# Register the UDAF
agreduce = udaf(
    f=AgReduceFunction(),
    result_type=DataTypes.STRING(),
    accumulator_type=DataTypes.ROW(
        [
            DataTypes.FIELD("rows", DataTypes.ARRAY(DataTypes.STRING())),
            DataTypes.FIELD("column_name", DataTypes.STRING()),
            DataTypes.FIELD("field_type", DataTypes.STRING()),
            DataTypes.FIELD("description", DataTypes.STRING()),
        ]
    ),
    name="agreduce",
)


# Made with Bob
