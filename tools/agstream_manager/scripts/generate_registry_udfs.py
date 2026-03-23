#!/usr/bin/env python3
"""
Generate Registry UDFs Script

This script scans the Schema Registry and auto-generates UDFs for all registered schemas.
Each schema gets its own UDTF (agmap) and UDAF (agreduce) function with the correct return types.

Usage:
    python generate_registry_udfs.py

Output:
    Creates/updates:
    - tools/agstream_manager/udfs/agmap_registry.py (UDTFs for row-by-row mapping)
    - tools/agstream_manager/udfs/agreduce_registry.py (UDAFs for aggregation)
"""

import json
import os
import sys
from typing import Dict, List

import requests
from dotenv import load_dotenv

# Load environment variables
for env_path in ["../.env", ".env", "../../../.env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break


def get_schema_registry_url():
    """Get the Schema Registry URL from environment or use default."""
    url = os.getenv("SCHEMA_REGISTRY_URL")
    if url:
        return url

    # Try Docker network hostname
    try:
        response = requests.get(
            "http://karapace-schema-registry:8081/subjects", timeout=2
        )
        if response.status_code == 200:
            return "http://karapace-schema-registry:8081"
    except:
        pass

    # Fall back to localhost
    return "http://localhost:8081"


def fetch_all_schemas() -> Dict[str, Dict[str, type]]:
    """
    Fetch all schemas from Schema Registry.

    Returns:
        Dictionary mapping schema names to their field definitions
    """
    try:
        registry_url = get_schema_registry_url()
        print(f"📡 Connecting to Schema Registry: {registry_url}")

        # Get all subjects
        response = requests.get(f"{registry_url}/subjects", timeout=5)
        response.raise_for_status()
        subjects = response.json()

        print(f"📋 Found {len(subjects)} subjects")

        schemas = {}

        for subject in subjects:
            # Skip key schemas, only process value schemas
            if not subject.endswith("-value"):
                continue

            # Extract schema name (remove -value suffix)
            schema_name = subject[:-6]

            try:
                # Get latest version of schema
                url = f"{registry_url}/subjects/{subject}/versions/latest"
                response = requests.get(url, timeout=5)
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
                            # Handle union types
                            non_null_types = [t for t in field_type if t != "null"]
                            field_type = (
                                non_null_types[0] if non_null_types else "string"
                            )

                        # Map Avro types to Python types
                        type_mapping = {
                            "string": "str",
                            "int": "int",
                            "long": "int",
                            "float": "float",
                            "double": "float",
                            "boolean": "bool",
                        }
                        fields[field_name] = type_mapping.get(field_type, "str")

                if fields:
                    schemas[schema_name] = fields
                    print(f"  ✓ {schema_name}: {list(fields.keys())}")

            except Exception as e:
                print(f"  ⚠ Skipping {subject}: {e}")
                continue

        return schemas

    except Exception as e:
        print(f"❌ Error fetching schemas: {e}")
        return {}


def generate_udtf_code(schema_name: str, fields: Dict[str, str]) -> str:
    """
    Generate UDTF code for a specific schema.

    Args:
        schema_name: Name of the schema
        fields: Dictionary mapping field names to Python type names

    Returns:
        Python code string for the UDTF
    """
    # Generate result_types list
    result_types = []
    for field_name, field_type in fields.items():
        if field_type == "str":
            result_types.append("DataTypes.STRING()")
        elif field_type == "int":
            result_types.append("DataTypes.BIGINT()")
        elif field_type == "float":
            result_types.append("DataTypes.DOUBLE()")
        elif field_type == "bool":
            result_types.append("DataTypes.BOOLEAN()")
        else:
            result_types.append("DataTypes.STRING()")

    result_types_str = ", ".join(result_types)

    # Generate function name (lowercase, replace spaces/special chars with underscore)
    func_name = schema_name.lower().replace(" ", "_").replace("-", "_")
    func_name = f"agmap_{func_name}"

    # Generate field extraction code
    field_extraction = []
    for field_name, field_type in fields.items():
        default_value = {
            "str": '"unknown"',
            "int": "0",
            "float": "0.0",
            "bool": "False",
        }.get(field_type, '"unknown"')

        field_extraction.append(
            f"""
        {field_name} = result_dict.get('{field_name}')
        if {field_name} is None:
            {field_name} = {default_value}
        elif not isinstance({field_name}, {field_type}):
            try:
                {field_name} = {field_type}({field_name})
            except:
                {field_name} = {default_value}"""
        )

    field_extraction_str = "".join(field_extraction)

    # Generate yield statement
    field_names = ", ".join(fields.keys())

    # Generate default values for error case
    error_values = []
    for field_type in fields.values():
        if field_type == "str":
            error_values.append('"error"')
        elif field_type in ("int", "float"):
            error_values.append("0")
        elif field_type == "bool":
            error_values.append("False")
        else:
            error_values.append('"error"')
    error_values_str = ", ".join(error_values)

    code = f'''
@udtf(result_types=[{result_types_str}])
def {func_name}(input_data):
    """
    Auto-generated UDTF for {schema_name} schema.

    Fields: {", ".join(fields.keys())}

    Usage:
        SELECT T.{", T.".join(fields.keys())}
        FROM source_table, LATERAL TABLE({func_name}(input_column)) AS T({", ".join(fields.keys())});
    """
    if not input_data:
        yield ({error_values_str})
        return

    try:
        # Create dynamic model
        model_class = create_dynamic_model('{schema_name}', {fields})

        # Get AG instance
        ag = get_ag_for_type('{schema_name}', model_class)

        # Perform semantic transformation
        ag_result = run_async(ag << input_data)

        # Extract result
        if hasattr(ag_result, 'states') and ag_result.states:
            result = ag_result.states[-1] if ag_result.states[-1] else ag_result
        else:
            result = ag_result

        # Convert to dictionary
        if hasattr(result, 'model_dump'):
            result_dict = result.model_dump()
        elif hasattr(result, 'dict'):
            result_dict = result.dict()
        elif isinstance(result, dict):
            result_dict = result
        else:
            result_dict = {{}}

        # Extract and validate fields{field_extraction_str}

        yield ({field_names})

    except Exception as e:
        print(f"Error in {func_name}: {{e}}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        yield ({error_values_str})
'''

    return code


def generate_udaf_code(schema_name: str, fields: Dict[str, str]) -> str:
    """
    Generate UDAF code for a specific schema (agreduce function).

    Args:
        schema_name: Name of the schema
        fields: Dictionary mapping field names to Python type names

    Returns:
        Python code string for the UDAF
    """
    # Generate function name (lowercase, replace spaces/special chars with underscore)
    func_name = schema_name.lower().replace(" ", "_").replace("-", "_")
    func_name = f"agreduce_{func_name}"

    # Generate field names for JSON output
    field_names = list(fields.keys())
    field_names_str = ", ".join([f'"{name}"' for name in field_names])

    code = f'''
class {func_name}(AggregateFunction):
    """
    Auto-generated UDAF for {schema_name} schema.

    Performs areduce aggregation on all rows to produce a single {schema_name} result.

    Fields: {", ".join(fields.keys())}

    Usage:
        SELECT {func_name}(input_column) as result
        FROM source_table;

        -- Extract fields using JSON_VALUE:
        SELECT
            JSON_VALUE(result, '$.{field_names[0]}') as {field_names[0]},
            JSON_VALUE(result, '$.{field_names[1] if len(field_names) > 1 else field_names[0]}') as {field_names[1] if len(field_names) > 1 else field_names[0]}
        FROM (
            SELECT {func_name}(input_column) as result
            FROM source_table
        );
    """

    def create_accumulator(self):
        return {{'rows': [], 'schema_name': '{schema_name}', 'fields': {fields}}}

    def accumulate(self, acc, input_data):
        if input_data:
            acc['rows'].append(str(input_data))

    def get_value(self, acc):
        if not acc['rows']:
            # Create default dict with all fields set to "unknown"
            default_dict = {{{', '.join([f'"{name}": "unknown"' for name in field_names])}}}
            return json.dumps(default_dict)

        try:
            # Create dynamic model
            model_class = create_dynamic_model(acc['schema_name'], acc['fields'])

            # Create wrapper for inputs
            class InputWrapper(BaseModel):
                text: str

            # Create source AG with all rows
            source_ag = AG(atype=InputWrapper)
            for row_data in acc['rows']:
                source_ag.append(InputWrapper(text=row_data))

            # Create target AG for reduction
            target_ag = AG(atype=model_class)

            # Perform areduce
            ag_result = run_async(target_ag << source_ag)

            # Extract result
            if hasattr(ag_result, 'states') and ag_result.states:
                result = ag_result.states[-1] if ag_result.states[-1] else ag_result
            else:
                result = ag_result

            # Convert to dictionary
            if hasattr(result, 'model_dump'):
                result_dict = result.model_dump()
            elif hasattr(result, 'dict'):
                result_dict = result.dict()
            elif isinstance(result, dict):
                result_dict = result
            else:
                result_dict = {{}}

            # Return as JSON string
            return json.dumps(result_dict)

        except Exception as e:
            print(f"Error in {func_name}: {{e}}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            # Create error dict with all fields set to "error"
            error_dict = {{{', '.join([f'"{name}": "error"' for name in field_names])}}}
            return json.dumps(error_dict)

    def merge(self, acc1, acc2):
        acc1['rows'].extend(acc2['rows'])
        return acc1

    def get_result_type(self):
        return DataTypes.STRING()

    def get_accumulator_type(self):
        return DataTypes.ROW([
            DataTypes.FIELD('rows', DataTypes.ARRAY(DataTypes.STRING())),
            DataTypes.FIELD('schema_name', DataTypes.STRING()),
            DataTypes.FIELD('fields', DataTypes.STRING())
        ])


# Register the UDAF
{func_name} = udaf({func_name}(), result_type=DataTypes.STRING())
'''

    return code


def generate_agmap_registry_file(schemas: Dict[str, Dict[str, str]]) -> str:
    """
    Generate the complete agmap_registry.py file.

    Args:
        schemas: Dictionary mapping schema names to their fields

    Returns:
        Complete Python file content
    """
    # Generate UDTF code for each schema
    udtf_codes = []
    for schema_name, fields in schemas.items():
        udtf_code = generate_udtf_code(schema_name, fields)
        udtf_codes.append(udtf_code)

    udtfs_str = "\n".join(udtf_codes)

    # Generate schema info for documentation
    schema_list = []
    for schema_name, fields in schemas.items():
        func_name = schema_name.lower().replace(" ", "_").replace("-", "_")
        func_name = f"agmap_{func_name}"
        field_list = ", ".join(fields.keys())
        schema_list.append(f"#   - {func_name}: {field_list}")

    schemas_doc = "\n".join(schema_list)

    file_content = f'''"""
Auto-Generated Registry UDTFs

This file is automatically generated by generate_registry_udfs.py
DO NOT EDIT MANUALLY - your changes will be overwritten!

Generated UDTFs:
{schemas_doc}

Usage:
    1. Register in Flink SQL:
       CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_sentiment
       AS 'agmap_registry.agmap_sentiment' LANGUAGE PYTHON;

    2. Use in queries:
       SELECT T.sentiment_label, T.sentiment_score
       FROM pr, LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label, sentiment_score);
"""

import asyncio
import json
import os
import sys
import threading
from typing import Dict

import requests
from dotenv import load_dotenv
from pydantic import Field, create_model
from pyflink.table import DataTypes
from pyflink.table.udf import udtf

from agentics import AG

# Load environment variables
for env_path in ["/opt/flink/.env", "../../../.env", ".env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break


#######################
### Helper Functions ###
#######################

def create_dynamic_model(type_name: str, fields: Dict[str, type]) -> type:
    """Create a Pydantic model dynamically from field definitions."""
    field_definitions = {{}}
    for field_name, field_type in fields.items():
        # Convert string type names to actual types
        if field_type == "str":
            python_type = str
        elif field_type == "int":
            python_type = int
        elif field_type == "float":
            python_type = float
        elif field_type == "bool":
            python_type = bool
        else:
            python_type = str

        field_definitions[field_name] = (python_type | None, Field(default=None))

    return create_model(type_name, **field_definitions)


def run_async(coro):
    """Run async coroutine in sync context."""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    except Exception as e:
        print(f"Error running async: {{e}}", file=sys.stderr)
        raise


# Thread-local storage for AG instances
_thread_local = threading.local()


def get_ag_for_type(target_type: str, model_class: type):
    """Get or create AG instance for a specific type."""
    if not hasattr(_thread_local, 'ag_cache'):
        _thread_local.ag_cache = {{}}

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
        raise ValueError("No API key found")

    # Create and cache AG instance
    ag_instance = AG(atype=model_class)
    _thread_local.ag_cache[target_type] = ag_instance

    return ag_instance


#######################
### Auto-Generated UDTFs ###
#######################
{udtfs_str}

# Made with Bob - Auto-generated on {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
'''

    return file_content


def generate_agreduce_registry_file(schemas: Dict[str, Dict[str, str]]) -> str:
    """
    Generate the complete agreduce_registry.py file.

    Args:
        schemas: Dictionary mapping schema names to their fields

    Returns:
        Complete Python file content
    """
    # Generate UDAF code for each schema
    udaf_codes = []
    for schema_name, fields in schemas.items():
        udaf_code = generate_udaf_code(schema_name, fields)
        udaf_codes.append(udaf_code)

    udafs_str = "\n".join(udaf_codes)

    # Generate schema info for documentation
    schema_list = []
    for schema_name, fields in schemas.items():
        func_name = schema_name.lower().replace(" ", "_").replace("-", "_")
        func_name = f"agreduce_{func_name}"
        field_list = ", ".join(fields.keys())
        schema_list.append(f"#   - {func_name}: {field_list}")

    schemas_doc = "\n".join(schema_list)

    file_content = f'''"""
Auto-Generated Registry UDAFs

This file is automatically generated by generate_registry_udfs.py
DO NOT EDIT MANUALLY - your changes will be overwritten!

Generated UDAFs (Aggregate Functions):
{schemas_doc}

Usage:
    1. Register in Flink SQL:
       CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agreduce_sentiment
       AS 'agreduce_registry.agreduce_sentiment' LANGUAGE PYTHON;

    2. Use in queries:
       SELECT agreduce_sentiment(customer_review) as result
       FROM pr;

    3. Extract fields using JSON_VALUE:
       SELECT
           JSON_VALUE(result, '$.sentiment_label') as sentiment_label,
           JSON_VALUE(result, '$.sentiment_score') as sentiment_score
       FROM (
           SELECT agreduce_sentiment(customer_review) as result
           FROM pr
       );
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
from pyflink.table.udf import AggregateFunction, udaf

from agentics import AG

# Load environment variables
for env_path in ["/opt/flink/.env", "../../../.env", ".env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break


#######################
### Helper Functions ###
#######################

def create_dynamic_model(type_name: str, fields: Dict[str, type]) -> type:
    """Create a Pydantic model dynamically from field definitions."""
    field_definitions = {{}}
    for field_name, field_type in fields.items():
        # Convert string type names to actual types
        if field_type == "str":
            python_type = str
        elif field_type == "int":
            python_type = int
        elif field_type == "float":
            python_type = float
        elif field_type == "bool":
            python_type = bool
        else:
            python_type = str

        field_definitions[field_name] = (python_type | None, Field(default=None))

    return create_model(type_name, **field_definitions)


def run_async(coro):
    """Run async coroutine in sync context."""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    except Exception as e:
        print(f"Error running async: {{e}}", file=sys.stderr)
        raise


#######################
### Auto-Generated UDAFs ###
#######################
{udafs_str}

# Made with Bob - Auto-generated on {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
'''

    return file_content


def main():
    """Main function to generate UDFs."""
    print("=" * 80)
    print("🔧 Registry UDF Generator")
    print("=" * 80)
    print()

    # Fetch all schemas
    schemas = fetch_all_schemas()

    if not schemas:
        print("\n❌ No schemas found in Schema Registry")
        print("   Make sure Schema Registry is running and has registered schemas")
        return 1

    print(f"\n✅ Found {len(schemas)} schemas")
    print()

    # Generate agmap_registry.py
    print("📝 Generating agmap_registry.py...")
    agmap_content = generate_agmap_registry_file(schemas)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    udfs_dir = os.path.join(script_dir, "..", "udfs")
    os.makedirs(udfs_dir, exist_ok=True)

    agmap_file = os.path.join(udfs_dir, "agmap_registry.py")
    with open(agmap_file, "w") as f:
        f.write(agmap_content)
    print(f"✅ Generated: {agmap_file}")

    # Generate agreduce_registry.py
    print("📝 Generating agreduce_registry.py...")
    agreduce_content = generate_agreduce_registry_file(schemas)

    agreduce_file = os.path.join(udfs_dir, "agreduce_registry.py")
    with open(agreduce_file, "w") as f:
        f.write(agreduce_content)
    print(f"✅ Generated: {agreduce_file}")

    print()
    print("=" * 80)
    print("📋 Next Steps:")
    print("=" * 80)
    print("1. Rebuild Flink image:")
    print("   ./scripts/rebuild_flink_image.sh")
    print()
    print("2. Restart services:")
    print("   ./manage_services_full.sh restart_services")
    print()
    print("3. Register UDFs in Flink SQL:")
    print()
    print("   -- UDTFs (row-by-row mapping):")
    for schema_name in schemas.keys():
        func_name = schema_name.lower().replace(" ", "_").replace("-", "_")
        func_name = f"agmap_{func_name}"
        print(f"   CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS {func_name}")
        print(f"   AS 'agmap_registry.{func_name}' LANGUAGE PYTHON;")
        print()

    print("   -- UDAFs (aggregation):")
    for schema_name in schemas.keys():
        func_name = schema_name.lower().replace(" ", "_").replace("-", "_")
        func_name = f"agreduce_{func_name}"
        print(f"   CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS {func_name}")
        print(f"   AS 'agreduce_registry.{func_name}' LANGUAGE PYTHON;")
        print()
    print("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
