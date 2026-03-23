"""
AGstream Manager Service - Unified Flask API for managing schemas and transductions

This consolidated service provides a REST API for:
- Creating/managing Pydantic types (Schema Manager functionality)
- Creating/managing transducible functions (Transduction Manager functionality)
- Starting/stopping listeners as subprocess or Flink cluster
- Monitoring listener logs
- Producing/consuming Kafka messages
"""

import fcntl
import json
import os
import pty
import select
import struct
import subprocess
import sys
import tempfile
import termios
import threading
import traceback
import tty
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import psutil
import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from flask_sock import Sock
from pydantic import BaseModel, Field, create_model

from agentics.core.streaming.flink_listener_manager import FlinkListenerManager
from agentics.core.streaming.streaming_utils import (
    get_atype_from_registry,
    list_schema_versions,
    register_atype_schema,
)

# Load environment variables from root .env file
# Find the agentics root by looking for pyproject.toml
current_dir = Path(__file__).resolve().parent
while current_dir != current_dir.parent:
    if (current_dir / "pyproject.toml").exists():
        ROOT_DIR = current_dir
        break
    current_dir = current_dir.parent
else:
    # Fallback: assume we're in tools/agstream_manager/backend
    ROOT_DIR = Path(__file__).resolve().parent.parent.parent

ENV_FILE = ROOT_DIR / ".env"
if ENV_FILE.exists():
    load_dotenv(ENV_FILE, override=True)
    print(f"✓ Loaded environment from {ENV_FILE}")
    # Verify critical env vars are loaded
    if os.getenv("OPENAI_API_KEY"):
        api_key = os.getenv("OPENAI_API_KEY")
        print(f"✓ OPENAI_API_KEY loaded (length: {len(api_key) if api_key else 0})")
else:
    print(f"⚠ Warning: .env file not found at {ENV_FILE}")

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend access
sock = Sock(app)  # Enable WebSocket support

# Configuration
SCHEMA_REGISTRY_URL = os.getenv(
    "AGSTREAM_BACKENDS_SCHEMA_REGISTRY_URL", "http://localhost:8081"
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "AGSTREAM_BACKENDS_KAFKA_BOOTSTRAP", "localhost:9092"
)
FLINK_JOBMANAGER_URL = os.getenv("FLINK_JOBMANAGER_URL", "http://localhost:8085")
# Execution mode: "local" (PyFlink embedded) or "cluster" (Flink cluster via REST API)
LISTENER_EXECUTION_MODE = os.getenv("LISTENER_EXECUTION_MODE", "local")
# Max instances that can be generated in a single request
MAX_GENERATE_INSTANCES = int(os.getenv("MAX_GENERATE_INSTANCES", "100"))

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
LOGS_DIR = PERSISTENCE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# In-memory storage
functions_store: Dict[str, Dict[str, Any]] = {}
listeners_store: Dict[str, Dict[str, Any]] = (
    {}
)  # {listener_id: {process, function_name, ...}}
listener_logs: Dict[str, deque] = {}  # {listener_id: deque of log lines}


def get_log_file_path(listener_id: str) -> Path:
    """Get the path to the log file for a listener."""
    return LOGS_DIR / f"{listener_id}.log"


def append_log(listener_id: str, message: str):
    """Append a log message to both memory and disk."""
    # Add to memory
    if listener_id not in listener_logs:
        listener_logs[listener_id] = deque(maxlen=1000)
    listener_logs[listener_id].append(message)

    # Append to disk
    try:
        log_file = get_log_file_path(listener_id)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(message)
            if not message.endswith("\n"):
                f.write("\n")
    except Exception as e:
        print(f"Warning: Failed to write log to disk: {e}")


def load_logs_from_disk(listener_id: str, max_lines: int = 1000) -> List[str]:
    """Load logs from disk for a listener."""
    log_file = get_log_file_path(listener_id)
    if not log_file.exists():
        return []

    try:
        with open(log_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            # Return last max_lines
            return lines[-max_lines:] if len(lines) > max_lines else lines
    except Exception as e:
        print(f"Warning: Failed to read log file: {e}")
        return []


# Flink listener manager for cluster mode
flink_manager: Optional[FlinkListenerManager] = None


def get_flink_manager() -> FlinkListenerManager:
    """Get or create the FlinkListenerManager instance."""
    global flink_manager
    if flink_manager is None:
        flink_manager = FlinkListenerManager(flink_jobmanager_url=FLINK_JOBMANAGER_URL)
    return flink_manager


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


def cleanup_orphaned_listeners():
    """
    Automatically cleanup orphaned listener processes and zombie entries on startup.

    This function:
    1. Finds running Python processes executing listener scripts from /tmp/
    2. Identifies orphaned processes (running but not tracked in UI)
    3. Identifies zombie entries (tracked in UI but process not running)
    4. Kills orphaned processes
    5. Marks zombie entries as stopped
    """
    print()
    print("🧹 Checking for orphaned listener processes...")

    # Find all running listener processes
    listener_processes = []
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            if proc.info["name"] == "python" or proc.info["name"].startswith("python"):
                cmdline = proc.info["cmdline"]
                if cmdline and len(cmdline) >= 2:
                    script_path = cmdline[1]
                    # Check if it's a listener script in /tmp/
                    if script_path.startswith("/tmp/") and script_path.endswith(".py"):
                        listener_processes.append((proc.info["pid"], script_path))
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

    if not listener_processes:
        print("   ✓ No listener processes found")
        return

    print(f"   Found {len(listener_processes)} running listener processes")

    # Get tracked PIDs and temp files
    tracked_pids = set()
    tracked_temp_files = set()
    for listener_data in listeners_store.values():
        process = listener_data.get("process")
        if process and isinstance(process, dict):
            pid = process.get("pid")
            if pid:
                tracked_pids.add(pid)
        temp_file = listener_data.get("temp_file")
        if temp_file:
            tracked_temp_files.add(temp_file)

    # Find orphaned processes
    orphaned = []
    for pid, script_path in listener_processes:
        if pid not in tracked_pids and script_path not in tracked_temp_files:
            orphaned.append((pid, script_path))

    # Find zombie entries
    zombies = []
    for listener_id, listener_data in listeners_store.items():
        process = listener_data.get("process")
        if process and isinstance(process, dict):
            pid = process.get("pid")
            if pid:
                try:
                    proc = psutil.Process(pid)
                    if not proc.is_running():
                        zombies.append(listener_id)
                except psutil.NoSuchProcess:
                    zombies.append(listener_id)
        elif listener_data.get("status") == "running":
            zombies.append(listener_id)

    # Kill orphaned processes
    if orphaned:
        print(f"   ⚠️  Found {len(orphaned)} orphaned processes - cleaning up...")
        for pid, script_path in orphaned:
            try:
                proc = psutil.Process(pid)
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except psutil.TimeoutExpired:
                    proc.kill()
                print(f"      ✓ Killed orphaned process {pid}: {script_path}")
            except Exception as e:
                print(f"      ✗ Failed to kill process {pid}: {e}")

    # Clean zombie entries
    if zombies:
        print(f"   ⚠️  Found {len(zombies)} zombie entries - marking as stopped...")
        for listener_id in zombies:
            listeners_store[listener_id]["status"] = "stopped"
            listeners_store[listener_id]["process"] = None
            print(f"      ✓ Marked '{listener_id}' as stopped")
        save_listeners_config()

    if not orphaned and not zombies:
        print("   ✓ All listeners properly tracked - no cleanup needed")
    else:
        print(
            f"   ✓ Cleanup complete: {len(orphaned)} orphans killed, {len(zombies)} zombies cleaned"
        )
    print()


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
    """Delete a subject from schema registry (permanently)"""
    try:
        # First soft delete
        response = requests.delete(f"{SCHEMA_REGISTRY_URL}/subjects/{subject}")
        if response.status_code not in [200, 204]:
            print(f"Soft delete failed for {subject}: {response.status_code}")
            return False

        # Then permanent delete
        response = requests.delete(
            f"{SCHEMA_REGISTRY_URL}/subjects/{subject}?permanent=true"
        )
        success = response.status_code in [200, 204]
        if success:
            print(f"✓ Permanently deleted subject: {subject}")
        else:
            print(f"✗ Permanent delete failed for {subject}: {response.status_code}")
        return success
    except Exception as e:
        print(f"Error deleting subject {subject}: {e}")
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
        description = field.get("description", "").strip()
        required = field.get("required", False)
        default_value = field.get("default")

        # Only include description in Field if it's not empty
        if required:
            if description:
                field_definitions[field_name] = (
                    field_type,
                    Field(description=description),
                )
            else:
                field_definitions[field_name] = (
                    field_type,
                    Field(),
                )
        else:
            if default_value is not None:
                if description:
                    field_definitions[field_name] = (
                        Optional[field_type],
                        Field(default=default_value, description=description),
                    )
                else:
                    field_definitions[field_name] = (
                        Optional[field_type],
                        Field(default=default_value),
                    )
            else:
                if description:
                    field_definitions[field_name] = (
                        Optional[field_type],
                        Field(default=None, description=description),
                    )
                else:
                    field_definitions[field_name] = (
                        Optional[field_type],
                        Field(default=None),
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
    from pydantic_core import PydanticUndefined

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

        # Handle default value - check for PydanticUndefined
        default_value = field_info.default
        if (
            default_value is None
            or default_value is PydanticUndefined
            or default_value is ...
        ):
            default_value = None

        fields.append(
            {
                "name": field_name,
                "type": field_type,
                "description": field_info.description or "",
                "required": field_info.is_required(),
                "default": default_value,
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


def reset_consumer_group_offset(
    consumer_group: str,
    topic: str,
    broker: Optional[str] = None,
    reset_to: str = "earliest",
) -> bool:
    """
    Reset Kafka consumer group offset to fix listener consumption issues.

    This addresses the common problem where listeners don't consume messages
    because the consumer group offset is already at the end of the topic.

    Args:
        consumer_group: Consumer group ID to reset
        topic: Topic name to reset offsets for
        broker: Kafka broker address (defaults to KAFKA_BOOTSTRAP_SERVERS)
        reset_to: Position to reset to ("earliest" or "latest")

    Returns:
        True if reset was successful, False otherwise
    """
    if broker is None:
        broker = KAFKA_BOOTSTRAP_SERVERS

    try:
        # Build the kafka-consumer-groups command
        cmd = [
            "kafka-consumer-groups",
            "--bootstrap-server",
            broker,
            "--group",
            consumer_group,
            "--reset-offsets",
            f"--to-{reset_to}",
            "--topic",
            topic,
            "--execute",
        ]

        print(
            f"🔄 Resetting consumer group '{consumer_group}' for topic '{topic}' to {reset_to}"
        )

        # Run the command
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

        if result.returncode == 0:
            print(f"✓ Successfully reset offsets for consumer group '{consumer_group}'")
            return True
        else:
            print(f"⚠️ Warning: Could not reset offsets: {result.stderr}")
            # Don't fail the restart if offset reset fails - it might not be needed
            return False

    except FileNotFoundError:
        print(
            "⚠️ Warning: kafka-consumer-groups command not found - skipping offset reset"
        )
        return False
    except subprocess.TimeoutExpired:
        print("⚠️ Warning: Timeout resetting consumer group offsets")
        return False
    except Exception as e:
        print(f"⚠️ Warning: Error resetting consumer group offsets: {e}")
        return False


# ============================================================================


def generate_listener_code(
    function_data: Dict[str, Any],
    input_topic: str = "input-topic",
    output_topic: str = "output-topic",
    lookback: int = 0,
    parallelism: int = 1,
    use_avro: bool = False,
) -> str:
    """Generate Python code to run a listener for this function"""
    name = function_data["name"]
    source_type = function_data["source_type"]
    target_type = function_data["target_type"]
    instructions = function_data["instructions"]

    # Generate a stable consumer group ID based on function name and topics
    # This ensures the same listener configuration always uses the same group
    consumer_group = f"agstream-{name.replace(' ', '-').lower()}-listener"

    # ALWAYS use Avro - hardcoded for Flink SQL compatibility
    format_type = "Avro (Flink SQL Compatible)"
    stream_class = "AGStreamSQL"

    code = f'''"""
Generated listener code for function: {name}
Source: {source_type} -> Target: {target_type}
Parallelism: {parallelism} thread(s)
Format: {format_type}
"""

import os
from agentics.core.streaming.streaming_utils import get_atype_from_registry, register_atype_schema
from agentics.core.transducible_functions import make_transducible_function
'''

    # ALWAYS use Avro format - hardcoded
    code += f'''from agentics.core.streaming.agstream_sql import AGStreamSQL
import asyncio

# Configuration
SCHEMA_REGISTRY_URL = "{SCHEMA_REGISTRY_URL}"
INPUT_TOPIC = "{input_topic}"
OUTPUT_TOPIC = "{output_topic}"
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "{consumer_group}")
PARALLELISM = {parallelism}
USE_AVRO = True

# Get types from registry
{source_type}_type = get_atype_from_registry(
    atype_name="{source_type}",
    schema_registry_url=SCHEMA_REGISTRY_URL
)
{target_type}_type = get_atype_from_registry(
    atype_name="{target_type}",
    schema_registry_url=SCHEMA_REGISTRY_URL
)

# Ensure schemas are registered as Avro
register_atype_schema({source_type}_type, schema_registry_url=SCHEMA_REGISTRY_URL, compatibility="NONE")
register_atype_schema({target_type}_type, schema_registry_url=SCHEMA_REGISTRY_URL, compatibility="NONE")

# Create transducible function
fn = make_transducible_function(
    InputModel={source_type}_type,
    OutputModel={target_type}_type,
    instructions="""{instructions}"""
)

# Create AGStreamSQL for consuming (Avro input) with persistent consumer group
input_stream = AGStreamSQL(
    atype={source_type}_type,
    topic=INPUT_TOPIC,
    kafka_server="localhost:9092",
    schema_registry_url=SCHEMA_REGISTRY_URL,
    consumer_group=CONSUMER_GROUP  # Use persistent group to track offsets
)

# Create AGStreamSQL for producing (Avro output)
output_stream = AGStreamSQL(
    atype={target_type}_type,
    topic=OUTPUT_TOPIC,
    kafka_server="localhost:9092",
    schema_registry_url=SCHEMA_REGISTRY_URL,
    auto_create_topic=True
)

# Start listener with persistent consumer group
print("Starting Avro listener for {name}...")
print(f"Input: {{INPUT_TOPIC}} -> Output: {{OUTPUT_TOPIC}}")
print(f"Source: {source_type} -> Target: {target_type}")
print(f"Consumer Group: {{CONSUMER_GROUP}}")
print(f"Parallelism: {{PARALLELISM}} thread(s)")
print(f"Format: Avro (Flink SQL Compatible)")
print("This listener consumes and produces Avro format for full Flink SQL compatibility.")
print("Press Ctrl+C to stop\\n")

# Define transduction wrapper for listen() method
def transduce(input_obj):
    \"\"\"Apply transduction function to input and return output\"\"\"
    # Apply transduction
    output_obj = asyncio.run(fn(input_obj))

    # Handle TransductionResult - extract the value for production
    from agentics.core.transducible_functions import TransductionResult
    if isinstance(output_obj, TransductionResult):
        return output_obj.value
    return output_obj

# Use the NEW AGStreamSQL.listen() method (high-level API)
print(f"✓ Starting listener using AGStreamSQL.listen() method...")
print(f"   This uses the new high-level API for cleaner code\\n")

input_stream.listen(
    transduction_fn=transduce,
    output_stream=output_stream,
    timeout_ms=1000,
    max_iterations=None,  # Run forever
    verbose=True,
    auto_offset_reset="earliest"
)

print("\\nListener stopped.")
'''
    # Removed else block - ALWAYS use Avro format

    return code


# ============================================================================
# API Endpoints - Main
# ============================================================================


@app.route("/", methods=["GET"])
@app.route("/agstream_manager.html", methods=["GET"])
def serve_frontend():
    """Serve the AGstream Manager frontend"""
    frontend_dir = SCRIPT_DIR.parent / "frontend"
    response = send_from_directory(str(frontend_dir), "agstream_manager.html")
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
                    # Check both "title" (JSON Schema) and "name" (Avro schema), fallback to topic_name
                    schema_type = (
                        schema.get("title") or schema.get("name") or topic_name
                    )
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
            actual_kafka_topics = set()

        # Find all topics that use this schema type and get the schema
        subjects = get_all_subjects()
        value_subjects = [s for s in subjects if s.endswith("-value")]
        associated_topics = []
        avro_schema = None
        schema_subject = None

        for subject in value_subjects:
            topic_name = subject[:-6]
            schema_data = get_schema_by_subject(subject)
            if schema_data:
                try:
                    schema = json.loads(schema_data.get("schema", "{}"))
                    schema_type = schema.get("name") or schema.get("title")
                    if schema_type == type_name:
                        if not avro_schema:
                            avro_schema = schema
                            schema_subject = subject
                        if topic_name in actual_kafka_topics:
                            associated_topics.append(topic_name)
                except Exception as e:
                    print(f"Error checking schema for {subject}: {e}")
                    continue

        if not avro_schema:
            return jsonify({"error": f"Type '{type_name}' not found"}), 404

        # Convert Avro schema to frontend format
        fields = []
        properties = {}
        required_fields = []

        if avro_schema.get("type") == "record" and "fields" in avro_schema:
            for field in avro_schema["fields"]:
                field_name = field["name"]
                field_type = field["type"]

                # Handle union types (e.g., ["null", "string"])
                is_optional = False
                if isinstance(field_type, list):
                    is_optional = "null" in field_type
                    # Get the non-null type
                    field_type = next((t for t in field_type if t != "null"), "string")

                # Map Avro types to simple types
                type_map = {
                    "string": "str",
                    "long": "int",
                    "int": "int",
                    "float": "float",
                    "double": "float",
                    "boolean": "bool",
                }
                simple_type = type_map.get(field_type, "str")

                # Build field info
                field_info = {
                    "name": field_name,
                    "type": simple_type,
                    "required": not is_optional and "default" not in field,
                    "description": field.get("doc", ""),
                }
                if "default" in field:
                    field_info["default"] = field["default"]

                fields.append(field_info)

                # Build JSON Schema properties
                properties[field_name] = {
                    "type": simple_type,
                    "description": field.get("doc", ""),
                }
                if field_info["required"]:
                    required_fields.append(field_name)

        # Build JSON Schema
        json_schema = {"type": "object", "title": type_name, "properties": properties}
        if required_fields:
            json_schema["required"] = required_fields

        return jsonify(
            {
                "name": type_name,
                "fields": fields,
                "code": f"# Avro schema from {schema_subject}\n# Fields: {', '.join(f['name'] for f in fields)}",
                "schema": json_schema,
                "associated_topics": associated_topics,
            }
        )
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
                    # Check both "title" (JSON Schema) and "name" (Avro schema)
                    schema_type = schema.get("title") or schema.get("name", "")

                    if schema_type == type_name:
                        subjects_to_delete.append(subject)
                        print(
                            f"Found subject to delete: {subject} (type: {schema_type})"
                        )
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
                "parallelism": data.get("parallelism", 1),
                "use_avro": data.get("use_avro", False),
            }
        )
    return jsonify({"listeners": listeners})


@app.route("/api/transductions/listeners", methods=["POST"])
def start_transduction_listener():
    """Start a new listener as a subprocess with configurable parallelism and format"""
    try:
        data = request.json
        listener_name = data.get("listener_name", "").strip()
        function_name = data.get("function_name")
        input_topic = data.get("input_topic", "input-topic")
        output_topic = data.get("output_topic", "output-topic")
        lookback = data.get("lookback", 0)
        # Per-listener parallelism (1 = single-threaded, >1 = multi-threaded PyFlink)
        parallelism = data.get("parallelism", 1)
        # ALWAYS use Avro format for Flink SQL compatibility (hardcoded)
        use_avro = True  # Hardcoded - always use Avro
        print(f"DEBUG: use_avro set to {use_avro} at line 1299")

        if not listener_name:
            return jsonify({"error": "Listener name is required"}), 400

        if not isinstance(parallelism, int) or parallelism < 1 or parallelism > 100:
            return (
                jsonify({"error": "parallelism must be an integer between 1 and 100"}),
                400,
            )

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

        from agentics.core.streaming.streaming_utils import (
            create_kafka_topic,
            kafka_topic_exists,
        )

        # Create input topic and register schema if needed
        # Use parallelism value for partition count to enable parallel processing
        input_topic_created = False
        if not kafka_topic_exists(input_topic, KAFKA_BOOTSTRAP_SERVERS):
            print(f"Creating input topic: {input_topic} with {parallelism} partitions")
            if not create_kafka_topic(
                input_topic,
                KAFKA_BOOTSTRAP_SERVERS,
                num_partitions=parallelism,
                replication_factor=1,
            ):
                return (
                    jsonify({"error": f"Failed to create input topic '{input_topic}'"}),
                    500,
                )
            input_topic_created = True

        # Create output topic and register schema if needed
        # Use parallelism value for partition count to enable parallel processing
        output_topic_created = False
        if not kafka_topic_exists(output_topic, KAFKA_BOOTSTRAP_SERVERS):
            print(
                f"Creating output topic: {output_topic} with {parallelism} partitions"
            )
            if not create_kafka_topic(
                output_topic,
                KAFKA_BOOTSTRAP_SERVERS,
                num_partitions=parallelism,
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
        # Use listener_name as the ID (sanitized)
        listener_id = listener_name.replace(" ", "-").lower()

        # Reset consumer group offset to earliest to fix consumption issues
        # This addresses the common problem where listeners don't consume messages
        # because the consumer group offset is already at the end of the topic
        consumer_group = f"agstream-{function_name.replace(' ', '-').lower()}-listener"

        print(f"🔄 Resetting consumer group '{consumer_group}' offset to earliest...")
        reset_success = reset_consumer_group_offset(
            consumer_group=consumer_group,
            topic=input_topic,
            broker=KAFKA_BOOTSTRAP_SERVERS,
            reset_to="earliest",
        )

        if reset_success:
            print(f"✓ Consumer group offset reset successfully")
        else:
            print(f"⚠️ Could not reset consumer group offset (may not be needed)")

        # Start listener with specified parallelism and format
        parallelism_label = (
            "single-threaded" if parallelism == 1 else f"{parallelism} threads"
        )
        format_label = "Avro (Flink SQL)" if use_avro else "JSON"
        print(
            f"⚡ Starting listener with parallelism={parallelism} ({parallelism_label}), format={format_label}"
        )
        print(f"   Listener: {listener_name}")
        print(f"   Input: {input_topic} → Output: {output_topic}")

        # Generate the listener code with specified parallelism and format
        code = generate_listener_code(
            function_data, input_topic, output_topic, lookback, parallelism, use_avro
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(code)
            temp_file = f.name

        process = subprocess.Popen(
            ["python", "-u", temp_file],  # -u for unbuffered output
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        listeners_store[listener_id] = {
            "listener_id": listener_id,
            "listener_name": listener_name,
            "function_name": function_name,
            "input_topic": input_topic,
            "output_topic": output_topic,
            "lookback_messages": lookback,
            "parallelism": parallelism,
            "use_avro": use_avro,
            "process": process,
            "temp_file": temp_file,
            "created_at": datetime.now().isoformat(),
            "status": "running",
        }
        listener_logs[listener_id] = deque(maxlen=1000)
        append_log(
            listener_id,
            f"[{datetime.now().isoformat()}] ⚡ Starting listener with parallelism={parallelism}, format={format_label}",
        )
        append_log(
            listener_id, f"[{datetime.now().isoformat()}] Job script: {temp_file}"
        )
        append_log(
            listener_id, f"[{datetime.now().isoformat()}] Input topic: {input_topic}"
        )
        append_log(
            listener_id, f"[{datetime.now().isoformat()}] Output topic: {output_topic}"
        )

        save_listeners_config()

        import threading

        def read_logs():
            if process.stdout:
                for line in process.stdout:
                    append_log(listener_id, line)

        threading.Thread(target=read_logs, daemon=True).start()

        return (
            jsonify(
                {
                    "success": True,
                    "listener_id": listener_id,
                    "message": f"Listener started with parallelism={parallelism} ({parallelism_label})",
                    "parallelism": parallelism,
                }
            ),
            201,
        )

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/transductions/listeners/create", methods=["POST"])
def create_transduction_listener_stopped():
    """Create a new listener configuration without starting it"""
    try:
        data = request.json
        listener_name = data.get("listener_name", "").strip()
        function_name = data.get("function_name")
        input_topic = data.get("input_topic", "input-topic")
        output_topic = data.get("output_topic", "output-topic")
        lookback = data.get("lookback", 0)
        parallelism = data.get("parallelism", 1)
        use_avro = True  # Hardcoded - always use Avro

        if not listener_name:
            return jsonify({"error": "Listener name is required"}), 400

        if not isinstance(parallelism, int) or parallelism < 1 or parallelism > 100:
            return (
                jsonify({"error": "parallelism must be an integer between 1 and 100"}),
                400,
            )

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

        from agentics.core.streaming.streaming_utils import (
            create_kafka_topic,
            kafka_topic_exists,
        )

        # Create topics if needed (same logic as start endpoint)
        if not kafka_topic_exists(input_topic, KAFKA_BOOTSTRAP_SERVERS):
            print(f"Creating input topic: {input_topic} with {parallelism} partitions")
            if not create_kafka_topic(
                input_topic,
                KAFKA_BOOTSTRAP_SERVERS,
                num_partitions=parallelism,
                replication_factor=1,
            ):
                return (
                    jsonify({"error": f"Failed to create input topic '{input_topic}'"}),
                    500,
                )

        if not kafka_topic_exists(output_topic, KAFKA_BOOTSTRAP_SERVERS):
            print(
                f"Creating output topic: {output_topic} with {parallelism} partitions"
            )
            if not create_kafka_topic(
                output_topic,
                KAFKA_BOOTSTRAP_SERVERS,
                num_partitions=parallelism,
                replication_factor=1,
            ):
                return (
                    jsonify(
                        {"error": f"Failed to create output topic '{output_topic}'"}
                    ),
                    500,
                )

        # Generate listener code and save it
        listener_id = listener_name.replace(" ", "-").lower()
        code = generate_listener_code(
            function_data, input_topic, output_topic, lookback, parallelism, use_avro
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(code)
            temp_file = f.name

        # Store listener config WITHOUT starting the process
        format_label = "Avro (Flink SQL)" if use_avro else "JSON"
        listeners_store[listener_id] = {
            "listener_id": listener_id,
            "listener_name": listener_name,
            "function_name": function_name,
            "input_topic": input_topic,
            "output_topic": output_topic,
            "lookback_messages": lookback,
            "parallelism": parallelism,
            "use_avro": use_avro,
            "process": None,  # No process yet
            "temp_file": temp_file,
            "created_at": datetime.now().isoformat(),
            "status": "stopped",
        }

        listener_logs[listener_id] = deque(maxlen=1000)
        append_log(
            listener_id, f"[{datetime.now().isoformat()}] 📝 Listener created (stopped)"
        )
        append_log(
            listener_id, f"[{datetime.now().isoformat()}] Format: {format_label}"
        )
        append_log(
            listener_id, f"[{datetime.now().isoformat()}] Parallelism: {parallelism}"
        )
        append_log(
            listener_id, f"[{datetime.now().isoformat()}] Input topic: {input_topic}"
        )
        append_log(
            listener_id, f"[{datetime.now().isoformat()}] Output topic: {output_topic}"
        )
        append_log(
            listener_id, f"[{datetime.now().isoformat()}] Job script: {temp_file}"
        )
        append_log(
            listener_id,
            f"[{datetime.now().isoformat()}] ℹ️  Use 'Start' button to begin processing",
        )

        save_listeners_config()

        return (
            jsonify(
                {
                    "success": True,
                    "listener_id": listener_id,
                    "message": f"Listener created (stopped). Use 'Start' to begin processing.",
                    "status": "stopped",
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
        from agentics.core.streaming.streaming_utils import (
            create_kafka_topic,
            kafka_topic_exists,
        )

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

        # Reset consumer group offset to earliest to fix consumption issues
        # This addresses the common problem where listeners don't consume messages
        # because the consumer group offset is already at the end of the topic
        function_data = functions_store[function_name]
        consumer_group = f"agstream-{function_name.replace(' ', '-').lower()}-listener"

        append_log(
            listener_id,
            f"[{datetime.now().isoformat()}] 🔄 Resetting consumer group offset to earliest...",
        )
        reset_success = reset_consumer_group_offset(
            consumer_group=consumer_group,
            topic=input_topic,
            broker=KAFKA_BOOTSTRAP_SERVERS,
            reset_to="earliest",
        )

        if reset_success:
            append_log(
                listener_id,
                f"[{datetime.now().isoformat()}] ✓ Consumer group offset reset successfully",
            )
        else:
            append_log(
                listener_id,
                f"[{datetime.now().isoformat()}] ⚠️ Could not reset consumer group offset (may not be needed)",
            )

        # Generate and run listener code
        code = generate_listener_code(
            function_data, input_topic, output_topic, lookback
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(code)
            temp_file = f.name

        process = subprocess.Popen(
            ["python", "-u", temp_file],  # -u for unbuffered output
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
        append_log(listener_id, f"[{datetime.now().isoformat()}] 🔄 Listener restarted")
        save_listeners_config()

        import threading

        def read_logs():
            if process.stdout:
                for line in process.stdout:
                    append_log(listener_id, line)

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
    # Check if listener exists in the store
    if listener_id not in listeners_store:
        # Try loading from persistence
        all_listeners = load_listeners_config()
        if listener_id not in all_listeners:
            return jsonify({"error": "Listener not found"}), 404

    # Try to get logs from memory first (for active listeners)
    logs = list(listener_logs.get(listener_id, []))

    # If no logs in memory, try loading from disk
    if not logs:
        logs = load_logs_from_disk(listener_id)

    # Return logs or a helpful message
    if not logs:
        logs = [
            f"[{datetime.now().isoformat()}] No logs available yet. Logs will appear here once the listener starts processing messages."
        ]

    return jsonify({"logs": logs})


@app.route("/api/transductions/listeners/<listener_id>/messages", methods=["GET"])
def get_listener_messages(listener_id: str):
    """Get matched input/output messages for a listener"""
    import traceback

    from kafka import KafkaConsumer

    from agentics.core.streaming.avro_utils import avro_deserialize

    # Check if listener exists
    if listener_id not in listeners_store:
        all_listeners = load_listeners_config()
        if listener_id not in all_listeners:
            return jsonify({"error": "Listener not found", "message_pairs": []}), 404
        listener = all_listeners[listener_id]
    else:
        listener = listeners_store[listener_id]

    input_topic = listener.get("input_topic")
    output_topic = listener.get("output_topic")

    if not input_topic or not output_topic:
        return (
            jsonify({"error": "Listener topics not configured", "message_pairs": []}),
            400,
        )

    try:
        # Read input messages
        input_consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=3000,
        )

        input_messages = {}
        schema_cache = {}

        for msg in input_consumer:
            key = msg.key.decode("utf-8") if msg.key else None
            if key:
                try:
                    # Try Avro deserialization
                    data, schema_id = avro_deserialize(
                        msg.value, SCHEMA_REGISTRY_URL, schema_cache
                    )
                    value = data
                except Exception as e:
                    # Fall back to JSON
                    try:
                        value = json.loads(msg.value.decode("utf-8"))
                    except:
                        value = {
                            "raw": msg.value.decode("utf-8", errors="ignore"),
                            "error": str(e),
                        }

                input_messages[key] = {
                    "key": key,
                    "value": value,
                    "timestamp": msg.timestamp,
                    "partition": msg.partition,
                    "offset": msg.offset,
                }

        input_consumer.close()

        # Read output messages
        output_consumer = KafkaConsumer(
            output_topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=3000,
        )

        output_messages = {}

        for msg in output_consumer:
            key = msg.key.decode("utf-8") if msg.key else None
            if key:
                try:
                    # Try Avro deserialization
                    data, schema_id = avro_deserialize(
                        msg.value, SCHEMA_REGISTRY_URL, schema_cache
                    )
                    value = data
                except Exception as e:
                    # Fall back to JSON
                    try:
                        value = json.loads(msg.value.decode("utf-8"))
                    except:
                        value = {
                            "raw": msg.value.decode("utf-8", errors="ignore"),
                            "error": str(e),
                        }

                output_messages[key] = {
                    "key": key,
                    "value": value,
                    "timestamp": msg.timestamp,
                    "partition": msg.partition,
                    "offset": msg.offset,
                }

        output_consumer.close()

        # Match messages by key
        message_pairs = []
        for key in input_messages.keys():
            if key in output_messages:
                message_pairs.append(
                    {
                        "key": key,
                        "input": input_messages[key],
                        "output": output_messages[key],
                    }
                )

        # Sort by input timestamp
        message_pairs.sort(key=lambda x: x["input"]["timestamp"])

        return jsonify(
            {
                "message_pairs": message_pairs,
                "total_input": len(input_messages),
                "total_output": len(output_messages),
                "matched": len(message_pairs),
            }
        )

    except Exception as e:
        error_msg = str(e)
        error_trace = traceback.format_exc()
        print(f"Error in get_listener_messages: {error_msg}")
        print(error_trace)
        return (
            jsonify(
                {"error": error_msg, "traceback": error_trace, "message_pairs": []}
            ),
            500,
        )


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

        from agentics.core.streaming.agstream_sql import AGStreamSQL
        from agentics.core.streaming.streaming_utils import (
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

        # Use AGStreamSQL for Avro format (Flink SQL compatible)
        stream = AGStreamSQL(
            atype=atype,
            topic=topic,
            kafka_server=KAFKA_BOOTSTRAP_SERVERS,
            schema_registry_url=SCHEMA_REGISTRY_URL,
            auto_create_topic=True,
        )

        message_ids = stream.produce([instance])
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


@app.route("/api/kafka/generate_and_produce", methods=["POST"])
def generate_and_produce_messages():
    """Generate N fake instances using generate_prototypical_instances and produce them to a Kafka topic"""
    try:
        data = request.json
        topic = data.get("topic")
        type_name = data.get("type_name")
        n_instances = data.get("n_instances", 10)
        instructions = data.get("instructions", None)

        if not topic or not type_name:
            return (
                jsonify({"error": "Missing required fields: topic, type_name"}),
                400,
            )

        if (
            not isinstance(n_instances, int)
            or n_instances < 1
            or n_instances > MAX_GENERATE_INSTANCES
        ):
            return (
                jsonify(
                    {
                        "error": f"n_instances must be an integer between 1 and {MAX_GENERATE_INSTANCES}"
                    }
                ),
                400,
            )

        import asyncio

        from agentics.core.streaming.agstream_sql import AGStreamSQL
        from agentics.core.streaming.streaming_utils import (
            get_atype_from_registry,
            register_atype_schema,
        )
        from agentics.core.transducible_functions import generate_prototypical_instances

        # Get the type from registry
        atype = get_atype_from_registry(type_name, SCHEMA_REGISTRY_URL)
        if not atype:
            return jsonify({"error": f"Type '{type_name}' not found"}), 404

        # Register schema if needed
        register_atype_schema(
            atype, schema_registry_url=SCHEMA_REGISTRY_URL, compatibility="NONE"
        )

        # Generate fake instances
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            instances = loop.run_until_complete(
                generate_prototypical_instances(
                    type=atype, n_instances=n_instances, instructions=instructions
                )
            )
        finally:
            loop.close()

        if not instances or len(instances) == 0:
            return jsonify({"error": "Failed to generate instances"}), 500

        # Produce all instances to the topic using Avro format for Flink SQL compatibility
        stream = AGStreamSQL(
            atype=atype,
            topic=topic,
            kafka_server=KAFKA_BOOTSTRAP_SERVERS,
            schema_registry_url=SCHEMA_REGISTRY_URL,
            auto_create_topic=True,
        )
        message_ids = stream.produce(instances)

        return jsonify(
            {
                "success": True,
                "message": f"Generated and produced {len(instances)} messages to topic '{topic}'",
                "count": len(instances),
                "message_ids": (
                    message_ids[:10] if len(message_ids) > 10 else message_ids
                ),  # Return first 10 IDs
            }
        )

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/kafka/collect-messages", methods=["POST"])
def collect_kafka_messages():
    """Collect messages from a Kafka topic with optional filtering"""
    try:
        import re

        from kafka import KafkaConsumer

        from agentics.core.streaming.avro_utils import (
            avro_deserialize,
            detect_message_format,
        )

        data = request.json
        topic = data.get("topic")
        max_messages = data.get("max_messages", 100)
        keys = data.get("keys", [])
        content_search = data.get("content_search")
        offset_mode = data.get(
            "offset_mode", "latest"
        )  # Changed default from "earliest" to "latest"
        timeout = data.get("timeout", 10) * 1000

        if not topic:
            return jsonify({"error": "Missing required field: topic"}), 400

        # Don't deserialize in the consumer - we'll handle it manually to support both JSON and Avro
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset=offset_mode,
            consumer_timeout_ms=timeout,
            enable_auto_commit=False,
            key_deserializer=lambda k: (
                k.decode("utf-8", errors="ignore") if k else None
            ),
        )

        messages = []
        content_pattern = re.compile(content_search) if content_search else None
        schema_cache = {}  # Cache schemas for performance

        try:
            for message in consumer:
                if len(messages) >= max_messages:
                    break

                if keys and message.key not in keys:
                    continue

                # Deserialize value based on format (Avro or JSON)
                value_str = None
                if message.value:
                    msg_format = detect_message_format(message.value)

                    if msg_format == "AVRO":
                        try:
                            # Deserialize Avro message
                            deserialized_data, schema_id = avro_deserialize(
                                message.value,
                                SCHEMA_REGISTRY_URL,
                                schema_cache=schema_cache,
                            )
                            # Convert to JSON string for display
                            value_str = json.dumps(deserialized_data)
                        except Exception as e:
                            # If Avro deserialization fails, fall back to raw display
                            value_str = f"[Avro decode error: {str(e)}]"
                    else:
                        # JSON format - decode as UTF-8
                        value_str = message.value.decode("utf-8", errors="ignore")

                if content_pattern and value_str:
                    if not content_pattern.search(value_str):
                        continue

                messages.append(
                    {
                        "key": message.key,
                        "value": value_str,
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
        from agentics.core.streaming.streaming_utils import (
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
                # Check both "title" (JSON Schema) and "name" (Avro schema)
                associated_type = schema.get("title") or schema.get("name")

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
                                    # Check both "title" (JSON Schema) and "name" (Avro schema)
                                    associated_type = schema.get("title") or schema.get(
                                        "name"
                                    )
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
        from agentics.core.streaming.streaming_utils import (
            create_kafka_topic,
            kafka_topic_exists,
        )

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
                    # Get the schema and detect its type
                    schema_data = schema_response.json()
                    schema_str = schema_data.get("schema")

                    # Auto-detect schema type from content
                    # Karapace doesn't always return schemaType in the response
                    schema_type = schema_data.get("schemaType")
                    if not schema_type:
                        # Parse schema to detect type
                        try:
                            schema_obj = json.loads(schema_str)
                            # Avro schemas have "type": "record" and "fields"
                            if (
                                isinstance(schema_obj, dict)
                                and schema_obj.get("type") == "record"
                                and "fields" in schema_obj
                            ):
                                schema_type = "AVRO"
                            else:
                                schema_type = "JSON"
                        except:
                            schema_type = "JSON"  # Default fallback

                    app.logger.info(
                        f"Detected schema type: {schema_type} for {associated_type}"
                    )

                    # Register it for the new topic with the detected schema type
                    topic_subject = f"{topic_name}-value"
                    register_payload = {"schema": schema_str, "schemaType": schema_type}
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
                            f"✓ Registered schema '{associated_type}' (type: {schema_type}) for topic '{topic_name}'"
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

        from agentics.core.streaming.streaming_utils import kafka_topic_exists

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
# API Endpoints - Configuration
# ============================================================================


@app.route("/api/config/execution-mode", methods=["GET"])
def get_execution_mode():
    """Get the current listener execution mode"""
    return jsonify(
        {
            "mode": LISTENER_EXECUTION_MODE,
            "flink_url": (
                FLINK_JOBMANAGER_URL if LISTENER_EXECUTION_MODE == "cluster" else None
            ),
            "available_modes": ["local", "cluster"],
            "description": {
                "local": "PyFlink embedded (runs in local process with parallel task slots)",
                "cluster": "Flink cluster (distributed execution via JobManager)",
            },
        }
    )


@app.route("/api/config/execution-mode", methods=["POST"])
def set_execution_mode():
    """Set the listener execution mode"""
    global LISTENER_EXECUTION_MODE

    data = request.json
    mode = data.get("mode")

    if mode not in ["local", "cluster"]:
        return jsonify({"error": "Invalid mode. Must be 'local' or 'cluster'"}), 400

    LISTENER_EXECUTION_MODE = mode

    return jsonify(
        {
            "success": True,
            "mode": LISTENER_EXECUTION_MODE,
            "message": f"Execution mode set to '{mode}'. New listeners will use this mode.",
        }
    )


@app.route("/api/channels", methods=["GET"])
def get_all_channels():
    """
    Get all registered channels with their schemas for PyFlink SQL auto-connection.

    Returns a list of channels with:
    - topic: Kafka topic name
    - atype_name: Pydantic type name
    - schema: Field definitions with types
    - schema_id: Schema registry ID
    """
    try:
        from kafka.admin import KafkaAdminClient

        # Get actual Kafka topics
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        kafka_topics = set(admin_client.list_topics())
        admin_client.close()

        # Get all subjects from schema registry
        subjects = get_all_subjects()
        value_subjects = [s for s in subjects if s.endswith("-value")]

        channels = []

        for subject in value_subjects:
            topic_name = subject[:-6]

            # Only include topics that actually exist in Kafka
            if topic_name not in kafka_topics:
                continue

            schema_data = get_schema_by_subject(subject)
            if schema_data:
                try:
                    schema_json = json.loads(schema_data.get("schema", "{}"))

                    # Handle both Avro and JSON Schema formats
                    atype_name = schema_json.get("name") or schema_json.get(
                        "title", topic_name
                    )

                    # Extract field definitions
                    fields = {}

                    # Avro format
                    if "fields" in schema_json:
                        for field in schema_json["fields"]:
                            field_name = field["name"]
                            field_type = field["type"]

                            # Handle union types (e.g., ["null", "string"])
                            if isinstance(field_type, list):
                                # Get non-null type
                                non_null_types = [t for t in field_type if t != "null"]
                                field_type = (
                                    non_null_types[0] if non_null_types else "string"
                                )

                            # Map Avro types to Python types
                            type_mapping = {
                                "string": "str",
                                "long": "int",
                                "int": "int",
                                "double": "float",
                                "float": "float",
                                "boolean": "bool",
                            }
                            fields[field_name] = type_mapping.get(
                                field_type, field_type
                            )

                    # JSON Schema format
                    elif "properties" in schema_json:
                        for field_name, field_info in schema_json["properties"].items():
                            field_type = field_info.get("type", "string")
                            type_mapping = {
                                "string": "str",
                                "integer": "int",
                                "number": "float",
                                "boolean": "bool",
                                "array": "list",
                                "object": "dict",
                            }
                            fields[field_name] = type_mapping.get(
                                field_type, field_type
                            )

                    channels.append(
                        {
                            "topic": topic_name,
                            "atype_name": atype_name,
                            "schema": fields,
                            "schema_id": schema_data.get("id"),
                            "schema_version": schema_data.get("version"),
                        }
                    )

                except Exception as e:
                    print(f"Error processing schema for {subject}: {e}")
                    continue

        return jsonify({"channels": channels})

    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


# ============================================================================
# Flink SQL Shell API
# ============================================================================


@app.route("/api/flink/status", methods=["GET"])
def check_flink_status():
    """Check if Flink JobManager is running"""
    try:
        # Check if Flink container is running
        result = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                "name=flink-jobmanager",
                "--format",
                "{{.Names}}",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )

        is_running = "flink-jobmanager" in result.stdout

        return jsonify(
            {
                "running": is_running,
                "message": (
                    "Flink JobManager is running"
                    if is_running
                    else "Flink JobManager is not running"
                ),
            }
        )
    except Exception as e:
        return jsonify({"running": False, "error": str(e)}), 500


@app.route("/api/flink/restart", methods=["POST"])
def restart_flink():
    """Restart Flink containers to clean up stuck jobs"""
    try:
        import os

        # Get the path to the restart script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        agstream_dir = os.path.join(script_dir, "..")
        restart_script = os.path.join(agstream_dir, "scripts", "restart_flink.sh")

        # Check if script exists
        if not os.path.exists(restart_script):
            return (
                jsonify(
                    {
                        "success": False,
                        "error": f"Restart script not found at {restart_script}",
                    }
                ),
                404,
            )

        # Execute the restart script
        result = subprocess.run(
            ["bash", restart_script],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=agstream_dir,
        )

        if result.returncode == 0:
            return jsonify(
                {
                    "success": True,
                    "message": "Flink containers restarted successfully",
                    "output": result.stdout,
                }
            )
        else:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "Failed to restart Flink",
                        "output": result.stderr,
                    }
                ),
                500,
            )

    except subprocess.TimeoutExpired:
        return jsonify({"success": False, "error": "Restart operation timed out"}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/flink/rebuild-udfs", methods=["POST"])
def rebuild_udfs():
    """Rebuild UDFs from Schema Registry types and restart Flink (runs in background)"""
    try:
        import os
        import threading

        # Get the path to the scripts
        script_dir = os.path.dirname(os.path.abspath(__file__))
        agstream_dir = os.path.join(script_dir, "..")
        generate_script = os.path.join(
            agstream_dir, "scripts", "generate_registry_udfs.py"
        )
        rebuild_script = os.path.join(agstream_dir, "scripts", "rebuild_flink_image.sh")
        restart_script = os.path.join(agstream_dir, "scripts", "restart_flink.sh")

        # Check if scripts exist
        if not os.path.exists(generate_script):
            return (
                jsonify(
                    {
                        "success": False,
                        "error": f"Generate script not found at {generate_script}",
                    }
                ),
                404,
            )

        if not os.path.exists(rebuild_script):
            return (
                jsonify(
                    {
                        "success": False,
                        "error": f"Rebuild script not found at {rebuild_script}",
                    }
                ),
                404,
            )

        if not os.path.exists(restart_script):
            return (
                jsonify(
                    {
                        "success": False,
                        "error": f"Restart script not found at {restart_script}",
                    }
                ),
                404,
            )

        # Run the rebuild process in a background thread
        def run_rebuild():
            try:
                # Step 1: Generate UDFs
                subprocess.run(
                    ["python", generate_script],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    cwd=agstream_dir,
                )

                # Step 2: Rebuild image
                subprocess.run(
                    ["bash", rebuild_script],
                    capture_output=True,
                    text=True,
                    timeout=300,
                    cwd=agstream_dir,
                )

                # Step 3: Restart Flink
                subprocess.run(
                    ["bash", restart_script],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    cwd=agstream_dir,
                )
            except Exception as e:
                import sys as _sys

                print(f"Error in background rebuild: {e}", file=_sys.stderr)

        # Start background thread
        thread = threading.Thread(target=run_rebuild, daemon=True)
        thread.start()

        # Return immediately
        return jsonify(
            {
                "success": True,
                "message": "UDF rebuild started in background. Flink will restart automatically when complete.",
            }
        )

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/flink/register-udfs", methods=["POST"])
def register_udfs():
    """Auto-register all AGMap UDFs in the current Flink SQL session"""
    try:
        import os

        # Get the path to the auto-register script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        agstream_dir = os.path.join(script_dir, "..")
        register_script = os.path.join(agstream_dir, "scripts", "auto_register_udfs.py")

        # Check if script exists
        if not os.path.exists(register_script):
            return (
                jsonify(
                    {
                        "success": False,
                        "error": f"Registration script not found at {register_script}",
                    }
                ),
                404,
            )

        # Run the script to generate SQL
        result = subprocess.run(
            ["python", register_script],
            capture_output=True,
            text=True,
            timeout=10,
            cwd=agstream_dir,
        )

        if result.returncode != 0:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "Failed to generate registration SQL",
                        "output": result.stderr,
                    }
                ),
                500,
            )

        # Extract SQL statements (stdout contains the SQL)
        sql_statements = result.stdout

        return jsonify(
            {
                "success": True,
                "message": "UDF registration SQL generated successfully",
                "sql": sql_statements,
                "summary": result.stderr,  # Summary goes to stderr
            }
        )

    except subprocess.TimeoutExpired:
        return (
            jsonify({"success": False, "error": "Registration script timed out"}),
            500,
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/flink/launch-shell", methods=["POST"])
def launch_flink_shell():
    """Launch the Flink SQL shell in a new terminal"""
    try:
        # Get the path to the flink script
        flink_script = AGSTREAM_DIR / "scripts" / "flink"

        if not flink_script.exists():
            return (
                jsonify(
                    {
                        "success": False,
                        "error": f"Flink script not found at {flink_script}",
                    }
                ),
                404,
            )

        # Make sure the script is executable
        os.chmod(flink_script, 0o755)

        # Launch in a new terminal window based on OS
        if os.uname().sysname == "Darwin":  # macOS
            # Use osascript to open a new Terminal window with bash -c to ensure script runs
            subprocess.Popen(
                [
                    "osascript",
                    "-e",
                    f'tell application "Terminal" to do script "cd {AGSTREAM_DIR.parent.parent} && bash {flink_script}"',
                ]
            )
        elif os.uname().sysname == "Linux":
            # Try common Linux terminal emulators
            terminals = ["gnome-terminal", "konsole", "xterm"]
            for term in terminals:
                try:
                    if term == "gnome-terminal":
                        subprocess.Popen(
                            [
                                term,
                                "--",
                                "bash",
                                "-c",
                                f"cd {AGSTREAM_DIR.parent.parent} && {flink_script}; exec bash",
                            ]
                        )
                    else:
                        subprocess.Popen(
                            [
                                term,
                                "-e",
                                f"bash -c 'cd {AGSTREAM_DIR.parent.parent} && {flink_script}; exec bash'",
                            ]
                        )
                    break
                except FileNotFoundError:
                    continue
        else:
            return (
                jsonify({"success": False, "error": "Unsupported operating system"}),
                400,
            )

        return jsonify(
            {"success": True, "message": "Flink SQL shell launched in new terminal"}
        )

    except Exception as e:
        return (
            jsonify(
                {"success": False, "error": str(e), "traceback": traceback.format_exc()}
            ),
            500,
        )


@sock.route("/api/flink/terminal")
def flink_terminal(ws):
    """WebSocket endpoint for Flink SQL terminal - Direct docker exec approach"""
    try:
        # Generate SQL init file first
        init_script = str(AGSTREAM_DIR / "scripts" / "init_flink_tables.py")
        sql_file = "/tmp/flink_init_tables.sql"

        # Generate the SQL file
        result = subprocess.run(
            [init_script],
            capture_output=True,
            text=True,
            cwd=str(AGSTREAM_DIR.parent.parent),
        )

        if result.returncode == 0:
            # Filter out comments and empty lines
            sql_content = "\n".join(
                [
                    line
                    for line in result.stdout.split("\n")
                    if line.strip() and not line.strip().startswith("--")
                ]
            )

            with open(sql_file, "w") as f:
                f.write(sql_content)

            # Copy to container
            subprocess.run(
                ["docker", "cp", sql_file, "flink-jobmanager:/tmp/init_tables.sql"],
                check=False,
            )

        # Start docker exec with PTY
        master_fd, slave_fd = pty.openpty()

        # Set terminal size
        winsize = struct.pack("HHHH", 25, 80, 0, 0)
        fcntl.ioctl(slave_fd, termios.TIOCSWINSZ, winsize)

        # Start the docker exec process with full TTY support
        # Using script command to ensure proper TTY allocation
        process = subprocess.Popen(
            [
                "docker",
                "exec",
                "-it",
                "flink-jobmanager",
                "bash",
                "-c",
                "cd /opt/flink && ./bin/sql-client.sh -i /tmp/init_tables.sql",
            ],
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            preexec_fn=os.setsid,
            env={**os.environ, "TERM": "xterm-256color"},
        )

        # Close the slave fd in the parent process
        os.close(slave_fd)

        def read_output():
            """Read output from the process and send to WebSocket"""
            while True:
                try:
                    # Use select to check if there's data to read
                    r, _, _ = select.select([master_fd], [], [], 0.1)
                    if r:
                        try:
                            data = os.read(master_fd, 4096)
                            if data:
                                ws.send(
                                    json.dumps(
                                        {
                                            "type": "output",
                                            "data": data.decode(
                                                "utf-8", errors="replace"
                                            ),
                                        }
                                    )
                                )
                            else:
                                break
                        except OSError:
                            break
                except Exception as e:
                    print(f"Error reading output: {e}")
                    break

        # Start output reading thread
        output_thread = threading.Thread(target=read_output, daemon=True)
        output_thread.start()

        # Handle input from WebSocket
        while True:
            try:
                message = ws.receive()
                if message is None:
                    break

                data = json.loads(message)
                if data.get("type") == "input":
                    input_data = data.get("data", "")
                    os.write(master_fd, input_data.encode("utf-8"))
            except Exception as e:
                print(f"WebSocket error: {e}")
                break

        # Cleanup
        try:
            process.terminate()
            process.wait(timeout=2)
        except:
            process.kill()

        try:
            os.close(master_fd)
        except:
            pass

        # Cleanup temp file
        try:
            os.remove(sql_file)
        except:
            pass

    except Exception as e:
        print(f"Terminal error: {e}")
        import traceback

        traceback.print_exc()


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

    # Automatically cleanup orphaned processes and zombie entries
    cleanup_orphaned_listeners()

    print()
    print("This unified service manages schemas, functions, and listeners.")
    print("All data is persisted across restarts.")
    print("=" * 60)

    app.run(host="0.0.0.0", port=5003, debug=True)

# Made with Bob
