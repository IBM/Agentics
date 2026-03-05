"""
AGstream Manager Service - Unified Flask API for managing schemas and transductions

This consolidated service provides a REST API for:
- Creating/managing Pydantic types (Schema Manager functionality)
- Creating/managing transducible functions (Transduction Manager functionality)
- Starting/stopping listeners as subprocess
- Monitoring listener logs
- Producing/consuming Kafka messages
"""

import json
import os
import subprocess
import tempfile
import traceback
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from pydantic import BaseModel, Field, create_model

from agentics.core.streaming_utils import (
    get_atype_from_registry,
    list_schema_versions,
    register_atype_schema,
)

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend access

# Configuration
SCHEMA_REGISTRY_URL = os.getenv(
    "AGSTREAM_BACKENDS_SCHEMA_REGISTRY_URL", "http://localhost:8081"
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "AGSTREAM_BACKENDS_KAFKA_BOOTSTRAP", "localhost:9092"
)

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent
AGSTREAM_DIR = SCRIPT_DIR.parent

# Persistence files - store in AGSTREAM_BACKENDS directory if set, otherwise in AGStream_Manager directory
AGSTREAM_BACKENDS = os.getenv("AGSTREAM_BACKENDS")
if AGSTREAM_BACKENDS:
    PERSISTENCE_DIR = Path(AGSTREAM_BACKENDS) / "agstream-manager"
    PERSISTENCE_DIR.mkdir(parents=True, exist_ok=True)
else:
    PERSISTENCE_DIR = AGSTREAM_DIR

FUNCTIONS_STORE_FILE = PERSISTENCE_DIR / "agstream_functions.json"
LISTENERS_STORE_FILE = PERSISTENCE_DIR / "agstream_listeners.json"

# In-memory storage
functions_store: Dict[str, Dict[str, Any]] = {}
listeners_store: Dict[str, Dict[str, Any]] = (
    {}
)  # {listener_id: {process, function_name, ...}}
listener_logs: Dict[str, deque] = {}  # {listener_id: deque of log lines}


# ============================================================================
# Persistence Functions
# ============================================================================


def save_functions_store():
    """Save functions store to disk"""
    try:
        with open(FUNCTIONS_STORE_FILE, "w") as f:
            json.dump(functions_store, f, indent=2)
        print(f"✓ Saved {len(functions_store)} functions to {FUNCTIONS_STORE_FILE}")
    except Exception as e:
        print(f"✗ Error saving functions store: {e}")


def load_functions_store():
    """Load functions store from disk"""
    global functions_store
    try:
        if FUNCTIONS_STORE_FILE.exists():
            with open(FUNCTIONS_STORE_FILE, "r") as f:
                functions_store = json.load(f)
            print(
                f"✓ Loaded {len(functions_store)} functions from {FUNCTIONS_STORE_FILE}"
            )
        else:
            print(f"ℹ No existing functions store found at {FUNCTIONS_STORE_FILE}")
    except Exception as e:
        print(f"✗ Error loading functions store: {e}")
        functions_store = {}


def save_listeners_config():
    """Save listener configurations (not processes) to disk"""
    try:
        listeners_config = {}
        for listener_id, listener_data in listeners_store.items():
            listeners_config[listener_id] = {
                "listener_name": listener_data.get("listener_name", listener_id),
                "function_name": listener_data.get("function_name"),
                "input_topic": listener_data.get("input_topic"),
                "output_topic": listener_data.get("output_topic"),
                "lookback_messages": listener_data.get("lookback_messages"),
                "created_at": listener_data.get("created_at"),
                "status": listener_data.get("status", "stopped"),
                "stopped_at": listener_data.get("stopped_at"),
            }

        with open(LISTENERS_STORE_FILE, "w") as f:
            json.dump(listeners_config, f, indent=2)
        print(
            f"✓ Saved {len(listeners_config)} listener configs to {LISTENERS_STORE_FILE}"
        )
    except Exception as e:
        print(f"✗ Error saving listeners config: {e}")


def load_listeners_config():
    """Load listener configurations from disk"""
    try:
        if LISTENERS_STORE_FILE.exists():
            with open(LISTENERS_STORE_FILE, "r") as f:
                listeners_config = json.load(f)
            print(
                f"✓ Loaded {len(listeners_config)} listener configs from {LISTENERS_STORE_FILE}"
            )
            print(
                f"ℹ Note: Listeners are not auto-started. Use the UI to restart them."
            )
            return listeners_config
        else:
            print(f"ℹ No existing listeners config found at {LISTENERS_STORE_FILE}")
            return {}
    except Exception as e:
        print(f"✗ Error loading listeners config: {e}")
        return {}


# ============================================================================
# Schema Helper Functions
# ============================================================================


def get_all_subjects() -> List[str]:
    """Get all subjects from schema registry"""
    try:
        response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
        if response.status_code == 200:
            return response.json()
        return []
    except Exception as e:
        print(f"Error getting subjects: {e}")
        return []


def get_schema_by_subject(subject: str, version: str = "latest") -> Optional[Dict]:
    """Get schema definition for a subject"""
    try:
        response = requests.get(
            f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/{version}"
        )
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        print(f"Error getting schema: {e}")
        return None


def delete_subject(subject: str) -> bool:
    """Delete a subject from schema registry"""
    try:
        response = requests.delete(f"{SCHEMA_REGISTRY_URL}/subjects/{subject}")
        return response.status_code in [200, 204]
    except Exception as e:
        print(f"Error deleting subject: {e}")
        return False


def create_pydantic_from_fields(
    type_name: str, fields: List[Dict[str, Any]]
) -> type[BaseModel]:
    """Create a Pydantic model from field definitions"""
    type_mapping = {
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "list": list,
        "dict": dict,
        "List[str]": List[str],
        "List[int]": List[int],
        "Dict[str, str]": Dict[str, str],
    }

    field_definitions = {}
    for field in fields:
        field_name = field["name"]
        field_type_str = field.get("type", "str")
        field_type = type_mapping.get(field_type_str, str)
        description = field.get("description", "")
        required = field.get("required", False)
        default_value = field.get("default")

        if required:
            field_definitions[field_name] = (
                field_type,
                Field(description=description),
            )
        else:
            if default_value is not None:
                field_definitions[field_name] = (
                    Optional[field_type],
                    Field(default=default_value, description=description),
                )
            else:
                field_definitions[field_name] = (
                    Optional[field_type],
                    Field(default=None, description=description),
                )

    return create_model(type_name, **field_definitions)


def create_pydantic_from_code(code: str) -> type[BaseModel]:
    """Execute Python code to create a Pydantic model"""
    namespace = {
        "BaseModel": BaseModel,
        "Field": Field,
        "Optional": Optional,
        "List": List,
        "Dict": Dict,
    }

    exec(code, namespace)

    for name, obj in namespace.items():
        if (
            isinstance(obj, type)
            and issubclass(obj, BaseModel)
            and obj is not BaseModel
        ):
            return obj

    raise ValueError("No Pydantic model found in code")


def pydantic_to_field_list(model: type[BaseModel]) -> List[Dict[str, Any]]:
    """Convert a Pydantic model to a list of field definitions"""
    fields = []
    for field_name, field_info in model.model_fields.items():
        field_type = str(field_info.annotation)
        field_type = (
            field_type.replace("typing.", "").replace("<class '", "").replace("'>", "")
        )

        is_optional = "Optional" in field_type or "None" in field_type
        if is_optional:
            field_type = (
                field_type.replace("Optional[", "")
                .replace("]", "")
                .replace(" | None", "")
            )

        fields.append(
            {
                "name": field_name,
                "type": field_type,
                "description": field_info.description or "",
                "required": field_info.is_required(),
                "default": (
                    field_info.default if field_info.default is not None else None
                ),
            }
        )
    return fields


def pydantic_to_code(model: type[BaseModel]) -> str:
    """Convert a Pydantic model to Python code"""
    lines = [
        "from pydantic import BaseModel, Field",
        "from typing import Optional, List, Dict",
        "",
        "",
        f"class {model.__name__}(BaseModel):",
    ]

    if not model.model_fields:
        lines.append("    pass")
    else:
        for field_name, field_info in model.model_fields.items():
            field_type = str(field_info.annotation)
            field_type = field_type.replace("typing.", "")

            is_optional = "Optional" in field_type or " | None" in field_type

            description = field_info.description or ""
            default = field_info.default

            if is_optional and (default is None or default is ...):
                if description:
                    lines.append(
                        f'    {field_name}: {field_type} = Field(default=None, description="{description}")'
                    )
                else:
                    lines.append(f"    {field_name}: {field_type} = None")
            elif default is not None and default is not ...:
                if isinstance(default, str):
                    default_str = f'"{default}"'
                else:
                    default_str = str(default)

                if description:
                    lines.append(
                        f'    {field_name}: {field_type} = Field(default={default_str}, description="{description}")'
                    )
                else:
                    lines.append(f"    {field_name}: {field_type} = {default_str}")
            else:
                if description:
                    lines.append(
                        f'    {field_name}: {field_type} = Field(description="{description}")'
                    )
                else:
                    lines.append(f"    {field_name}: {field_type}")

    return "\n".join(lines)


# ============================================================================
# Transduction Helper Functions
# ============================================================================


def generate_listener_code(
    function_data: Dict[str, Any],
    input_topic: str = "input-topic",
    output_topic: str = "output-topic",
    lookback: int = 0,
) -> str:
    """Generate Python code to run a listener for this function"""
    name = function_data["name"]
    source_type = function_data["source_type"]
    target_type = function_data["target_type"]
    instructions = function_data["instructions"]

    # Generate a stable consumer group ID based on function name and topics
    # This ensures the same listener configuration always uses the same group
    consumer_group = f"agstream-{name.replace(' ', '-').lower()}-listener"

    code = f'''"""
Generated listener code for function: {name}
Source: {source_type} -> Target: {target_type}
"""

import os
from agentics.core.streaming import AGStream
from agentics.core.streaming_utils import get_atype_from_registry, register_atype_schema
from agentics.core.transducible_functions import make_transducible_function

# Configuration
SCHEMA_REGISTRY_URL = "{SCHEMA_REGISTRY_URL}"
INPUT_TOPIC = "{input_topic}"
OUTPUT_TOPIC = "{output_topic}"
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "{consumer_group}")

# Get types from registry
{source_type}_type = get_atype_from_registry(
    atype_name="{source_type}",
    schema_registry_url=SCHEMA_REGISTRY_URL
)
{target_type}_type = get_atype_from_registry(
    atype_name="{target_type}",
    schema_registry_url=SCHEMA_REGISTRY_URL
)

# Ensure schemas are registered
register_atype_schema({source_type}_type, schema_registry_url=SCHEMA_REGISTRY_URL, compatibility="NONE")
register_atype_schema({target_type}_type, schema_registry_url=SCHEMA_REGISTRY_URL, compatibility="NONE")

# Create transducible function
fn = make_transducible_function(
    InputModel={source_type}_type,
    OutputModel={target_type}_type,
    instructions="""{instructions}"""
)

# Create AGStream
target = AGStream(
    target_atype_name="{target_type}",
    input_topic=INPUT_TOPIC,
    output_topic=OUTPUT_TOPIC
)

# Start listener with persistent consumer group
print("Starting listener for {name}...")
print(f"Input: {{INPUT_TOPIC}} -> Output: {{OUTPUT_TOPIC}}")
print(f"Source: {source_type} -> Target: {target_type}")
print(f"Consumer Group: {{CONSUMER_GROUP}}")
print("This listener will track processed messages and not reprocess them on restart.")
print("Press Ctrl+C to stop\\n")

# With lookback_messages=0 and a consumer group, the listener will:
# - On FIRST start: Only process NEW messages (scan_mode='latest-offset')
# - On RESTART: Continue from last committed offset (only new messages)
# This ensures messages are never reprocessed across restarts
target.transducible_function_listener(
    fn=fn,
    group_id=CONSUMER_GROUP,  # Persistent consumer group to track processed messages
    parallelism=10,
    lookback_messages=0,  # 0 = use latest-offset (only future messages)
    stop_after_lookback=False,
    verbose=True,
    max_empty_polls=999999
)

print("\\nListener stopped.")
'''
    return code


# ============================================================================
# API Endpoints - Main
# ============================================================================


@app.route("/", methods=["GET"])
@app.route("/agstream_manager.html", methods=["GET"])
def serve_frontend():
    """Serve the AGstream Manager frontend"""
    response = send_from_directory(str(SCRIPT_DIR), "agstream_manager.html")
    # Disable caching to ensure latest version is always served
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/test_listener_creation.html", methods=["GET"])
def serve_test_page():
    """Serve the test listener creation page"""
    response = send_from_directory(str(SCRIPT_DIR), "test_listener_creation.html")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify(
        {
            "status": "healthy",
            "service": "agstream-manager",
            "schema_registry": SCHEMA_REGISTRY_URL,
            "kafka_bootstrap": KAFKA_BOOTSTRAP_SERVERS,
        }
    )


# ============================================================================
# API Endpoints - Schema Management
# ============================================================================


@app.route("/api/schemas/types", methods=["GET"])
def list_schema_types():
    """List all registered Pydantic types (unique schema types only)"""
    try:
        # Get all subjects from schema registry
        subjects = get_all_subjects()
        value_subjects = [s for s in subjects if s.endswith("-value")]

        # Get actual Kafka topics
        from kafka.admin import KafkaAdminClient

        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        kafka_topics = set(admin_client.list_topics())
        admin_client.close()

        # Track unique types by their schema ID
        types_map = {}
        # Track which topics are associated with each type
        type_topics_map = {}

        for subject in value_subjects:
            topic_name = subject[:-6]
            schema_data = get_schema_by_subject(subject)
            if schema_data:
                try:
                    schema = json.loads(schema_data.get("schema", "{}"))
                    schema_type = schema.get("title", topic_name)
                    schema_id = schema_data.get("id")

                    # Only track topics that actually exist in Kafka
                    if topic_name in kafka_topics:
                        if schema_type not in type_topics_map:
                            type_topics_map[schema_type] = []
                        type_topics_map[schema_type].append(topic_name)

                    # Only keep the first occurrence of each unique schema type
                    if schema_type not in types_map:
                        types_map[schema_type] = {
                            "name": schema_type,
                            "subject": f"{schema_type}-value",
                            "version": schema_data.get("version"),
                            "id": schema_id,
                        }
                except Exception as e:
                    print(f"Error processing schema for {subject}: {e}")
                    continue

        # Add associated_topics to each type (only real Kafka topics)
        for schema_type, type_info in types_map.items():
            type_info["associated_topics"] = type_topics_map.get(schema_type, [])

        return jsonify({"types": list(types_map.values())})
    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/schemas/types/<type_name>", methods=["GET"])
def get_schema_type(type_name: str):
    """Get a specific type definition with associated topics"""
    try:
        atype = get_atype_from_registry(
            atype_name=type_name, schema_registry_url=SCHEMA_REGISTRY_URL
        )

        if atype:
            # Get list of actual Kafka topics
            from kafka.admin import KafkaAdminClient

            try:
                admin_client = KafkaAdminClient(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    client_id="agstream-manager",
                )
                actual_kafka_topics = set(admin_client.list_topics())
                admin_client.close()
            except Exception as e:
                print(f"Warning: Could not connect to Kafka to verify topics: {e}")
                actual_kafka_topics = set()  # If Kafka is down, show no topics

            # Find all topics that use this schema type
            subjects = get_all_subjects()
            value_subjects = [s for s in subjects if s.endswith("-value")]
            associated_topics = []

            for subject in value_subjects:
                topic_name = subject[:-6]

                # Only check topics that actually exist in Kafka
                if topic_name not in actual_kafka_topics:
                    continue

                schema_data = get_schema_by_subject(subject)
                if schema_data:
                    try:
                        schema = json.loads(schema_data.get("schema", "{}"))
                        # Check both "name" (Avro) and "title" (JSON Schema) fields
                        schema_type = schema.get("name") or schema.get("title")
                        if schema_type == type_name:
                            associated_topics.append(topic_name)
                    except Exception as e:
                        print(f"Error checking schema for {subject}: {e}")
                        continue

            return jsonify(
                {
                    "name": type_name,
                    "fields": pydantic_to_field_list(atype),
                    "code": pydantic_to_code(atype),
                    "schema": atype.model_json_schema(),
                    "associated_topics": associated_topics,
                }
            )
        else:
            return jsonify({"error": f"Type '{type_name}' not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/schemas/types", methods=["POST"])
def create_schema_type():
    """Create a new Pydantic type"""
    try:
        data = request.json
        type_name = data.get("name")

        if not type_name:
            return jsonify({"error": "Type name is required"}), 400

        if "fields" in data:
            fields = data["fields"]
            model = create_pydantic_from_fields(type_name, fields)
        elif "code" in data:
            code = data["code"]
            model = create_pydantic_from_code(code)
            if model.__name__ != type_name:
                field_defs = {
                    name: (field.annotation, field)
                    for name, field in model.model_fields.items()
                }
                model = create_model(type_name, **field_defs)
        else:
            return jsonify({"error": "Either 'fields' or 'code' must be provided"}), 400

        schema_id = register_atype_schema(
            atype=model, schema_registry_url=SCHEMA_REGISTRY_URL, compatibility="NONE"
        )

        if schema_id:
            return (
                jsonify(
                    {
                        "success": True,
                        "name": type_name,
                        "schema_id": schema_id,
                        "fields": pydantic_to_field_list(model),
                        "code": pydantic_to_code(model),
                    }
                ),
                201,
            )
        else:
            return jsonify({"error": "Failed to register schema"}), 500

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/schemas/types/<type_name>", methods=["PUT"])
def update_schema_type(type_name: str):
    """Update an existing type (creates a new version)"""
    try:
        data = request.json

        if "fields" in data:
            fields = data["fields"]
            model = create_pydantic_from_fields(type_name, fields)
        elif "code" in data:
            code = data["code"]
            model = create_pydantic_from_code(code)
        else:
            return jsonify({"error": "Either 'fields' or 'code' must be provided"}), 400

        schema_id = register_atype_schema(
            atype=model, schema_registry_url=SCHEMA_REGISTRY_URL, compatibility="NONE"
        )

        if schema_id:
            return jsonify(
                {
                    "success": True,
                    "name": type_name,
                    "schema_id": schema_id,
                    "fields": pydantic_to_field_list(model),
                    "code": pydantic_to_code(model),
                }
            )
        else:
            return jsonify({"error": "Failed to register new version"}), 500

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/schemas/types/<type_name>", methods=["DELETE"])
def delete_schema_type(type_name: str):
    """Delete a type from the registry - removes all associated topic subjects"""
    try:
        # First, get all subjects that use this schema type
        subjects = get_all_subjects()
        value_subjects = [s for s in subjects if s.endswith("-value")]

        subjects_to_delete = []

        # Find all subjects that have this schema type
        for subject in value_subjects:
            schema_data = get_schema_by_subject(subject)
            if schema_data:
                try:
                    schema = json.loads(schema_data.get("schema", "{}"))
                    schema_type = schema.get("title", "")

                    if schema_type == type_name:
                        subjects_to_delete.append(subject)
                except Exception as e:
                    print(f"Error checking schema for {subject}: {e}")
                    continue

        # Delete all subjects that use this type
        deleted_count = 0
        failed_subjects = []

        for subject in subjects_to_delete:
            if delete_subject(subject):
                deleted_count += 1
            else:
                failed_subjects.append(subject)

        if deleted_count > 0:
            message = f"Type '{type_name}' deleted from {deleted_count} topic(s)"
            if failed_subjects:
                message += f". Failed to delete from: {', '.join(failed_subjects)}"
            return jsonify(
                {
                    "success": True,
                    "message": message,
                    "deleted_subjects": subjects_to_delete,
                    "deleted_count": deleted_count,
                }
            )
        else:
            return (
                jsonify({"error": f"Type '{type_name}' not found or failed to delete"}),
                404,
            )

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/schemas/types/<type_name>/versions", methods=["GET"])
def list_schema_type_versions(type_name: str):
    """List all versions of a type"""
    try:
        versions = list_schema_versions(
            atype_name=type_name, schema_registry_url=SCHEMA_REGISTRY_URL
        )

        if versions:
            return jsonify({"type_name": type_name, "versions": versions})
        else:
            return jsonify({"error": f"No versions found for '{type_name}'"}), 404

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/schemas/validate", methods=["POST"])
def validate_schema_code():
    """Validate Python code for a Pydantic model"""
    try:
        data = request.json
        code = data.get("code", "")

        if not code:
            return jsonify({"valid": False, "error": "No code provided"}), 400

        model = create_pydantic_from_code(code)

        return jsonify(
            {
                "valid": True,
                "name": model.__name__,
                "fields": pydantic_to_field_list(model),
                "schema": model.model_json_schema(),
            }
        )

    except Exception as e:
        return jsonify({"valid": False, "error": str(e)}), 400


# ============================================================================
# API Endpoints - Transduction Management
# ============================================================================


@app.route("/api/transductions/types", methods=["GET"])
def list_transduction_types():
    """List all registered types from Schema Registry for transductions"""
    try:
        subjects = get_all_subjects()
        value_subjects = [s for s in subjects if s.endswith("-value")]

        types_info = []
        seen_types = set()  # Track unique type names to avoid duplicates

        for subject in value_subjects:
            try:
                # Fetch the schema to get the actual type name
                schema_response = get_schema_by_subject(subject)
                if schema_response and "schema" in schema_response:
                    import json as _json

                    # The schema is stored as a JSON string in the "schema" field
                    schema_dict = _json.loads(schema_response["schema"])

                    # Extract the type name from the schema
                    # Try multiple fields: "name" (Avro), "title" (JSON Schema), or fallback to topic name
                    type_name = schema_dict.get("name") or schema_dict.get("title")

                    if not type_name:
                        # Debug: print the schema structure to understand what's available
                        print(
                            f"DEBUG: Schema for {subject} has keys: {list(schema_dict.keys())}"
                        )
                        print(f"DEBUG: Schema dict: {schema_dict}")
                        # Last resort: use topic name but mark it clearly
                        type_name = subject[:-6]
                        print(
                            f"WARNING: Using topic name '{type_name}' as fallback for {subject}"
                        )

                    # Only add if we haven't seen this type name before
                    if type_name and type_name not in seen_types:
                        seen_types.add(type_name)
                        types_info.append(
                            {
                                "name": type_name,
                                "subject": subject,
                            }
                        )
            except Exception as e:
                # Log the error with more details
                print(f"ERROR: Could not fetch schema for {subject}: {e}")
                import traceback

                traceback.print_exc()

        # Sort by type name for better UX
        types_info.sort(key=lambda x: x["name"])

        return jsonify({"types": types_info})
    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/transductions/topics", methods=["GET"])
def list_kafka_topics():
    """List all Kafka topics"""
    try:
        from kafka.admin import KafkaAdminClient

        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, client_id="agstream-manager"
        )

        try:
            topics = list(admin_client.list_topics())
            topics = [t for t in topics if not t.startswith("_")]
            return jsonify({"topics": sorted(topics)})
        finally:
            admin_client.close()

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/transductions/functions", methods=["GET"])
def list_transduction_functions():
    """List all registered transducible functions"""
    functions = [
        {
            "name": data["name"],
            "source_type": data["source_type"],
            "target_type": data["target_type"],
            "instructions": data["instructions"],
            "created_at": data["created_at"],
        }
        for data in functions_store.values()
    ]
    return jsonify({"functions": functions})


@app.route("/api/transductions/functions/<function_name>", methods=["GET"])
def get_transduction_function(function_name: str):
    """Get a specific function"""
    if function_name not in functions_store:
        return jsonify({"error": f"Function '{function_name}' not found"}), 404

    data = functions_store[function_name]
    return jsonify(
        {
            "name": data["name"],
            "source_type": data["source_type"],
            "target_type": data["target_type"],
            "instructions": data["instructions"],
            "created_at": data["created_at"],
        }
    )


@app.route("/api/transductions/functions", methods=["POST"])
def create_transduction_function():
    """Create a new transducible function"""
    try:
        data = request.json

        if not data.get("name"):
            return jsonify({"error": "Function name is required"}), 400
        if not data.get("source_type"):
            return jsonify({"error": "Source type is required"}), 400
        if not data.get("target_type"):
            return jsonify({"error": "Target type is required"}), 400

        source_atype = get_atype_from_registry(
            atype_name=data["source_type"], schema_registry_url=SCHEMA_REGISTRY_URL
        )
        target_atype = get_atype_from_registry(
            atype_name=data["target_type"], schema_registry_url=SCHEMA_REGISTRY_URL
        )

        if not source_atype:
            return (
                jsonify(
                    {
                        "error": f"Source type '{data['source_type']}' not found in registry"
                    }
                ),
                400,
            )
        if not target_atype:
            return (
                jsonify(
                    {
                        "error": f"Target type '{data['target_type']}' not found in registry"
                    }
                ),
                400,
            )

        function_data = {
            "name": data["name"],
            "source_type": data["source_type"],
            "target_type": data["target_type"],
            "instructions": data.get("instructions", ""),
            "created_at": datetime.now().isoformat(),
        }

        functions_store[data["name"]] = function_data
        save_functions_store()

        return (
            jsonify(
                {
                    "success": True,
                    "function": function_data,
                }
            ),
            201,
        )

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/transductions/functions/<function_name>", methods=["DELETE"])
def delete_transduction_function(function_name: str):
    """Delete a function"""
    if function_name not in functions_store:
        return jsonify({"error": f"Function '{function_name}' not found"}), 404

    del functions_store[function_name]
    save_functions_store()
    return jsonify({"success": True, "message": f"Function '{function_name}' deleted"})


@app.route("/api/transductions/functions/<function_name>/code", methods=["GET"])
def get_transduction_function_code(function_name: str):
    """Get generated listener code for a function"""
    if function_name not in functions_store:
        return jsonify({"error": f"Function '{function_name}' not found"}), 404

    function_data = functions_store[function_name]
    input_topic = request.args.get("input_topic", "input-topic")
    output_topic = request.args.get("output_topic", "output-topic")
    lookback = int(request.args.get("lookback", 0))

    code = generate_listener_code(function_data, input_topic, output_topic, lookback)

    return jsonify({"name": function_name, "code": code})


@app.route("/api/transductions/listeners", methods=["GET"])
def list_transduction_listeners():
    """List all listeners"""
    listeners = []
    for listener_id, data in listeners_store.items():
        process = data.get("process")
        if process:
            status = "running" if process.poll() is None else "stopped"
        else:
            status = data.get("status", "stopped")

        listeners.append(
            {
                "listener_id": listener_id,
                "listener_name": data.get("listener_name", listener_id),
                "function_name": data["function_name"],
                "input_topic": data["input_topic"],
                "output_topic": data["output_topic"],
                "status": status,
                "created_at": data.get("created_at", data.get("started_at", "")),
                "lookback_messages": data.get("lookback_messages", 0),
            }
        )
    return jsonify({"listeners": listeners})


@app.route("/api/transductions/listeners", methods=["POST"])
def start_transduction_listener():
    """Start a new listener as a subprocess"""
    try:
        data = request.json
        listener_name = data.get("listener_name", "").strip()
        function_name = data.get("function_name")
        input_topic = data.get("input_topic", "input-topic")
        output_topic = data.get("output_topic", "output-topic")
        lookback = data.get("lookback", 0)

        if not listener_name:
            return jsonify({"error": "Listener name is required"}), 400

        # Check if listener name already exists
        for existing_id, existing_data in listeners_store.items():
            if existing_data.get("listener_name") == listener_name:
                return (
                    jsonify(
                        {"error": f"Listener name '{listener_name}' already exists"}
                    ),
                    400,
                )

        if not function_name or function_name not in functions_store:
            return jsonify({"error": "Invalid function name"}), 400

        function_data = functions_store[function_name]
        source_type = function_data.get("source_type")
        target_type = function_data.get("target_type")

        from agentics.core.streaming_utils import create_kafka_topic, kafka_topic_exists

        # Create input topic and register schema if needed
        input_topic_created = False
        if not kafka_topic_exists(input_topic, KAFKA_BOOTSTRAP_SERVERS):
            print(f"Creating input topic: {input_topic}")
            if not create_kafka_topic(
                input_topic,
                KAFKA_BOOTSTRAP_SERVERS,
                num_partitions=1,
                replication_factor=1,
            ):
                return (
                    jsonify({"error": f"Failed to create input topic '{input_topic}'"}),
                    500,
                )
            input_topic_created = True

        # Create output topic and register schema if needed
        output_topic_created = False
        if not kafka_topic_exists(output_topic, KAFKA_BOOTSTRAP_SERVERS):
            print(f"Creating output topic: {output_topic}")
            if not create_kafka_topic(
                output_topic,
                KAFKA_BOOTSTRAP_SERVERS,
                num_partitions=1,
                replication_factor=1,
            ):
                return (
                    jsonify(
                        {"error": f"Failed to create output topic '{output_topic}'"}
                    ),
                    500,
                )
            output_topic_created = True

        # Register schemas for newly created topics
        if input_topic_created and source_type:
            try:
                # Get the schema for the source type from schema registry
                source_schema_response = requests.get(
                    f"{SCHEMA_REGISTRY_URL}/subjects/{source_type}-value/versions/latest"
                )
                if source_schema_response.status_code == 200:
                    source_schema_data = source_schema_response.json()
                    # Register the same schema for the input topic
                    register_response = requests.post(
                        f"{SCHEMA_REGISTRY_URL}/subjects/{input_topic}-value/versions",
                        headers={
                            "Content-Type": "application/vnd.schemaregistry.v1+json"
                        },
                        json={"schema": source_schema_data["schema"]},
                    )
                    if register_response.status_code in [200, 201]:
                        print(
                            f"✓ Registered schema '{source_type}' for topic '{input_topic}'"
                        )
                    else:
                        print(
                            f"Warning: Could not register schema for input topic: {register_response.text}"
                        )
            except Exception as e:
                print(f"Warning: Could not register schema for input topic: {e}")

        if output_topic_created and target_type:
            try:
                # Get the schema for the target type from schema registry
                target_schema_response = requests.get(
                    f"{SCHEMA_REGISTRY_URL}/subjects/{target_type}-value/versions/latest"
                )
                if target_schema_response.status_code == 200:
                    target_schema_data = target_schema_response.json()
                    # Register the same schema for the output topic
                    register_response = requests.post(
                        f"{SCHEMA_REGISTRY_URL}/subjects/{output_topic}-value/versions",
                        headers={
                            "Content-Type": "application/vnd.schemaregistry.v1+json"
                        },
                        json={"schema": target_schema_data["schema"]},
                    )
                    if register_response.status_code in [200, 201]:
                        print(
                            f"✓ Registered schema '{target_type}' for topic '{output_topic}'"
                        )
                    else:
                        print(
                            f"Warning: Could not register schema for output topic: {register_response.text}"
                        )
            except Exception as e:
                print(f"Warning: Could not register schema for output topic: {e}")
        code = generate_listener_code(
            function_data, input_topic, output_topic, lookback
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(code)
            temp_file = f.name

        process = subprocess.Popen(
            ["python", temp_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        # Use listener_name as the ID (sanitized)
        listener_id = listener_name.replace(" ", "-").lower()

        listeners_store[listener_id] = {
            "listener_id": listener_id,
            "listener_name": listener_name,
            "function_name": function_name,
            "input_topic": input_topic,
            "output_topic": output_topic,
            "lookback_messages": lookback,
            "process": process,
            "temp_file": temp_file,
            "created_at": datetime.now().isoformat(),
            "status": "running",
        }
        listener_logs[listener_id] = deque(maxlen=1000)
        save_listeners_config()

        import threading

        def read_logs():
            if process.stdout:
                for line in process.stdout:
                    listener_logs[listener_id].append(line)

        threading.Thread(target=read_logs, daemon=True).start()

        return (
            jsonify(
                {
                    "success": True,
                    "listener_id": listener_id,
                    "message": f"Listener started",
                }
            ),
            201,
        )

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/transductions/listeners/<listener_id>", methods=["DELETE"])
def stop_transduction_listener(listener_id: str):
    """Stop or delete a listener. Use ?permanent=true to delete permanently."""
    if listener_id not in listeners_store:
        return jsonify({"error": "Listener not found"}), 404

    permanent = request.args.get("permanent", "false").lower() == "true"

    data = listeners_store[listener_id]
    process = data.get("process")

    if process:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()

    try:
        if "temp_file" in data:
            os.unlink(data["temp_file"])
    except:
        pass

    if permanent:
        # Permanently delete the listener
        del listeners_store[listener_id]
        if listener_id in listener_logs:
            del listener_logs[listener_id]
        save_listeners_config()
        return jsonify({"success": True, "message": "Listener deleted permanently"})
    else:
        # Keep the listener in the store but mark it as stopped
        data["status"] = "stopped"
        data["process"] = None
        data["stopped_at"] = datetime.now().isoformat()
        save_listeners_config()
        return jsonify({"success": True, "message": "Listener stopped"})


@app.route("/api/transductions/listeners/<listener_id>/restart", methods=["POST"])
def restart_transduction_listener(listener_id: str):
    """Restart a stopped listener with the same configuration"""
    if listener_id not in listeners_store:
        return jsonify({"error": "Listener not found"}), 404

    data = listeners_store[listener_id]

    # Check if already running
    process = data.get("process")
    if process and process.poll() is None:
        return jsonify({"error": "Listener is already running"}), 400

    function_name = data["function_name"]
    input_topic = data["input_topic"]
    output_topic = data["output_topic"]
    lookback = data.get("lookback_messages", 0)

    if function_name not in functions_store:
        return jsonify({"error": f"Function '{function_name}' not found"}), 400

    try:
        from agentics.core.streaming_utils import create_kafka_topic, kafka_topic_exists

        # Ensure topics exist
        if not kafka_topic_exists(input_topic, KAFKA_BOOTSTRAP_SERVERS):
            if not create_kafka_topic(
                input_topic,
                KAFKA_BOOTSTRAP_SERVERS,
                num_partitions=1,
                replication_factor=1,
            ):
                return (
                    jsonify({"error": f"Failed to create input topic '{input_topic}'"}),
                    500,
                )

        if not kafka_topic_exists(output_topic, KAFKA_BOOTSTRAP_SERVERS):
            if not create_kafka_topic(
                output_topic,
                KAFKA_BOOTSTRAP_SERVERS,
                num_partitions=1,
                replication_factor=1,
            ):
                return (
                    jsonify(
                        {"error": f"Failed to create output topic '{output_topic}'"}
                    ),
                    500,
                )

        # Generate and run listener code
        function_data = functions_store[function_name]
        code = generate_listener_code(
            function_data, input_topic, output_topic, lookback
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(code)
            temp_file = f.name

        process = subprocess.Popen(
            ["python", temp_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        # Update listener data
        data["process"] = process
        data["temp_file"] = temp_file
        data["status"] = "running"
        data["restarted_at"] = datetime.now().isoformat()

        listener_logs[listener_id] = deque(maxlen=1000)
        save_listeners_config()

        import threading

        def read_logs():
            if process.stdout:
                for line in process.stdout:
                    listener_logs[listener_id].append(line)

        threading.Thread(target=read_logs, daemon=True).start()

        return (
            jsonify(
                {
                    "success": True,
                    "listener_id": listener_id,
                    "message": "Listener restarted successfully",
                }
            ),
            200,
        )

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/transductions/listeners/<listener_id>/logs", methods=["GET"])
def get_transduction_listener_logs(listener_id: str):
    """Get logs for a listener"""
    if listener_id not in listener_logs:
        return jsonify({"error": "Listener not found"}), 404

    logs = list(listener_logs[listener_id])
    return jsonify({"logs": logs})


# ============================================================================
# API Endpoints - Kafka Operations
# ============================================================================


@app.route("/api/kafka/produce", methods=["POST"])
def produce_kafka_message():
    """Produce a message to a Kafka topic using AGStream"""
    try:
        data = request.json
        topic = data.get("topic")
        type_name = data.get("type_name")
        message_data = data.get("message")

        if not topic or not type_name or not message_data:
            return (
                jsonify(
                    {"error": "Missing required fields: topic, type_name, message"}
                ),
                400,
            )

        from agentics.core.streaming import AGStream
        from agentics.core.streaming_utils import (
            get_atype_from_registry,
            register_atype_schema,
        )

        atype = get_atype_from_registry(type_name, SCHEMA_REGISTRY_URL)
        if not atype:
            return jsonify({"error": f"Type '{type_name}' not found"}), 404

        register_atype_schema(
            atype, schema_registry_url=SCHEMA_REGISTRY_URL, compatibility="NONE"
        )

        instance = atype(**message_data)

        stream = AGStream(atype=atype, states=[instance], output_topic=topic)

        message_ids = stream.produce(register_if_missing=True)
        message_key = message_ids[0] if message_ids else "unknown"

        return jsonify(
            {
                "success": True,
                "message": f"Message produced to topic '{topic}'",
                "key": message_key,
                "message_id": message_key,
            }
        )

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/kafka/collect-messages", methods=["POST"])
def collect_kafka_messages():
    """Collect messages from a Kafka topic with optional filtering"""
    try:
        import re

        from kafka import KafkaConsumer

        data = request.json
        topic = data.get("topic")
        max_messages = data.get("max_messages", 100)
        keys = data.get("keys", [])
        content_search = data.get("content_search")
        offset_mode = data.get("offset_mode", "earliest")
        timeout = data.get("timeout", 10) * 1000

        if not topic:
            return jsonify({"error": "Missing required field: topic"}), 400

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset=offset_mode,
            consumer_timeout_ms=timeout,
            enable_auto_commit=False,
            value_deserializer=lambda v: (
                v.decode("utf-8", errors="ignore") if v else None
            ),
            key_deserializer=lambda k: (
                k.decode("utf-8", errors="ignore") if k else None
            ),
        )

        messages = []
        content_pattern = re.compile(content_search) if content_search else None

        try:
            for message in consumer:
                if len(messages) >= max_messages:
                    break

                if keys and message.key not in keys:
                    continue

                if content_pattern and message.value:
                    if not content_pattern.search(message.value):
                        continue

                messages.append(
                    {
                        "key": message.key,
                        "value": message.value,
                        "partition": message.partition,
                        "offset": message.offset,
                        "timestamp": message.timestamp,
                    }
                )

        finally:
            consumer.close()

        return jsonify(
            {
                "success": True,
                "messages": messages,
                "count": len(messages),
                "topic": topic,
            }
        )

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


# ============================================================================
# Topic Management API (using AGstream utilities)
# ============================================================================


@app.route("/api/topics", methods=["GET"])
def get_topics():
    """List all Kafka topics (alias for /api/transductions/topics)"""
    return list_kafka_topics()


@app.route("/api/topics/<topic_name>", methods=["GET"])
def get_topic_details(topic_name: str):
    """Get details about a specific Kafka topic with auto-detected schema type"""
    try:
        from agentics.core.streaming_utils import (
            get_topic_partition_count,
            kafka_topic_exists,
        )

        # Check if topic exists
        if not kafka_topic_exists(topic_name, KAFKA_BOOTSTRAP_SERVERS):
            return jsonify({"error": f"Topic '{topic_name}' not found"}), 404

        # Get partition count
        partitions = get_topic_partition_count(topic_name, KAFKA_BOOTSTRAP_SERVERS)

        # Auto-detect associated type from multiple sources
        associated_type = None
        try:
            # Method 1: Check schema registry for exact match
            subject = f"{topic_name}-value"
            schema_response = requests.get(
                f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
            )
            if schema_response.status_code == 200:
                schema_data = schema_response.json()
                schema = json.loads(schema_data.get("schema", "{}"))
                associated_type = schema.get("title")

            # Method 2: If not in registry, check listeners/functions
            if not associated_type:
                # Load persisted data if in-memory stores are empty
                all_listeners = dict(listeners_store)
                all_functions = dict(functions_store)

                if not all_listeners:
                    all_listeners = load_listeners_config()
                if not all_functions:
                    all_functions = load_functions_store()

                # Check if this topic is used by any listener
                for listener_id, listener_data in all_listeners.items():
                    input_topic = listener_data.get("input_topic")
                    output_topic = listener_data.get("output_topic")
                    function_name = listener_data.get("function_name")

                    # If this topic matches a listener's input or output
                    if topic_name == input_topic or topic_name == output_topic:
                        # Get the function to find the type
                        if function_name and function_name in all_functions:
                            func_data = all_functions[function_name]
                            if topic_name == input_topic:
                                associated_type = func_data.get("source_type")
                            elif topic_name == output_topic:
                                associated_type = func_data.get("target_type")
                            if associated_type:
                                break

            # Method 3: Fuzzy match in schema registry (fallback)
            if not associated_type:
                subjects_response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
                if subjects_response.status_code == 200:
                    all_subjects = subjects_response.json()
                    topic_lower = topic_name.lower()
                    for subj in all_subjects:
                        if subj.endswith("-value"):
                            subj_base = subj[:-6].lower()
                            if (
                                subj_base == topic_lower
                                or subj_base == topic_lower.rstrip("s")
                                or subj_base + "s" == topic_lower
                            ):
                                schema_resp = requests.get(
                                    f"{SCHEMA_REGISTRY_URL}/subjects/{subj}/versions/latest"
                                )
                                if schema_resp.status_code == 200:
                                    schema_data = schema_resp.json()
                                    schema = json.loads(schema_data.get("schema", "{}"))
                                    associated_type = schema.get("title")
                                    break
        except Exception as e:
            print(f"Could not auto-detect type for topic '{topic_name}': {e}")

        return jsonify(
            {
                "name": topic_name,
                "partitions": partitions if partitions else 1,
                "replication_factor": 1,  # Default, actual value would need admin API
                "associated_type": associated_type,
            }
        )

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/topics", methods=["POST"])
def create_topic():
    """Create a new Kafka topic and optionally register its schema"""
    try:
        from agentics.core.streaming_utils import create_kafka_topic, kafka_topic_exists

        data = request.json
        topic_name = data.get("name")
        partitions = data.get("partitions", 1)
        replication_factor = data.get("replication_factor", 1)
        associated_type = data.get("associated_type")  # Optional type to associate

        if not topic_name:
            return jsonify({"error": "Topic name is required"}), 400

        # Check if topic already exists
        if kafka_topic_exists(topic_name, KAFKA_BOOTSTRAP_SERVERS):
            return jsonify({"error": f"Topic '{topic_name}' already exists"}), 409

        # Create the topic using AGstream utility
        success = create_kafka_topic(
            topic_name=topic_name,
            kafka_server=KAFKA_BOOTSTRAP_SERVERS,
            num_partitions=partitions,
            replication_factor=replication_factor,
        )

        if not success:
            return jsonify({"error": f"Failed to create topic '{topic_name}'"}), 500

        # If associated_type is provided, register the schema for this topic
        if associated_type:
            try:
                # Get the schema for this type from the registry
                type_subject = f"{associated_type}-value"
                schema_response = requests.get(
                    f"{SCHEMA_REGISTRY_URL}/subjects/{type_subject}/versions/latest"
                )

                if schema_response.status_code == 200:
                    # Get the schema
                    schema_data = schema_response.json()
                    schema_str = schema_data.get("schema")

                    # Register it for the new topic
                    topic_subject = f"{topic_name}-value"
                    register_payload = {"schema": schema_str, "schemaType": "JSON"}
                    register_response = requests.post(
                        f"{SCHEMA_REGISTRY_URL}/subjects/{topic_subject}/versions",
                        json=register_payload,
                        headers={
                            "Content-Type": "application/vnd.schemaregistry.v1+json"
                        },
                    )

                    if register_response.status_code not in [200, 201]:
                        print(
                            f"Warning: Failed to register schema for topic '{topic_name}': {register_response.text}"
                        )
                else:
                    print(
                        f"Warning: Type '{associated_type}' not found in schema registry"
                    )
            except Exception as e:
                print(
                    f"Warning: Could not register schema for topic '{topic_name}': {e}"
                )

        return (
            jsonify(
                {
                    "message": f"Topic '{topic_name}' created successfully",
                    "name": topic_name,
                    "partitions": partitions,
                    "replication_factor": replication_factor,
                    "associated_type": associated_type,
                }
            ),
            201,
        )

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/topics/<topic_name>", methods=["DELETE"])
def delete_topic(topic_name: str):
    """Delete a Kafka topic and its associated schema"""
    try:
        import time

        from kafka.admin import KafkaAdminClient

        from agentics.core.streaming_utils import kafka_topic_exists

        # Check if topic exists
        if not kafka_topic_exists(topic_name, KAFKA_BOOTSTRAP_SERVERS):
            return jsonify({"error": f"Topic '{topic_name}' not found"}), 404

        # First, try to delete the associated schema from schema registry
        schema_subject = f"{topic_name}-value"
        schema_deleted = False
        schema_error = None

        try:
            schema_deleted = delete_subject(schema_subject)
            if not schema_deleted:
                schema_error = f"Schema '{schema_subject}' not found or already deleted"
        except Exception as e:
            schema_error = f"Error deleting schema: {str(e)}"

        # Then delete the Kafka topic
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, client_id="agstream-manager"
        )

        try:
            # Delete the topic - this is synchronous in kafka-python
            admin_client.delete_topics([topic_name])

            # Give Kafka a moment to process the deletion
            time.sleep(1.0)

            # Verify deletion
            still_exists = kafka_topic_exists(topic_name, KAFKA_BOOTSTRAP_SERVERS)

            result = {
                "message": f"Topic '{topic_name}' {'deletion initiated' if still_exists else 'deleted successfully'}",
                "topic_deleted": not still_exists,
                "schema_deleted": schema_deleted,
            }

            if still_exists:
                result["note"] = (
                    "Topic deletion is asynchronous and may take a few seconds to complete"
                )

            if schema_error:
                result["schema_note"] = schema_error

            return jsonify(result), 200

        finally:
            admin_client.close()

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 AGstream Manager Service Starting")
    print("=" * 60)
    print(f"Schema Registry URL: {SCHEMA_REGISTRY_URL}")
    print(f"Kafka Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Service URL: http://localhost:5003")
    print(f"Functions Store: {FUNCTIONS_STORE_FILE}")
    print(f"Listeners Store: {LISTENERS_STORE_FILE}")
    print("=" * 60)
    print()

    print("📂 Loading persisted data...")
    load_functions_store()
    saved_listeners = load_listeners_config()

    if saved_listeners:
        # Load saved listeners into the listeners_store
        listeners_store.update(saved_listeners)
        print(f"ℹ  Found {len(saved_listeners)} saved listener configurations")
        print("   Listeners loaded (status: stopped). Use the UI to restart them.")

    print()
    print("This unified service manages schemas, functions, and listeners.")
    print("All data is persisted across restarts.")
    print("=" * 60)

    app.run(host="0.0.0.0", port=5003, debug=True)

# Made with Bob
