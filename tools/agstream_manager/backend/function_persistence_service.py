#!/usr/bin/env python3
"""
function_persistence_service.py
===============================
Background service for managing persistent transducible functions.

This service:
1. Initializes a PersistentFunctionStore on startup
2. Provides a REST API for managing functions
3. Auto-loads all saved functions
4. Integrates with schema registry

Usage:
------
    python function_persistence_service.py

Environment Variables:
----------------------
    STORAGE_DIR: Directory for function metadata (default: ./saved_functions)
    SCHEMA_REGISTRY_URL: Schema registry URL (default: http://localhost:8081)
    SERVICE_PORT: Port for REST API (default: 8082)
"""

import os
import sys
from pathlib import Path

from flask import Flask, jsonify, request
from flask_cors import CORS

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from persistent_function_store import PersistentFunctionStore

from src.agentics.core.streaming_utils import get_atype_from_registry
from src.agentics.core.transducible_functions import make_transducible_function

# Configuration
AGSTREAM_BACKENDS = os.getenv("AGSTREAM_BACKENDS", "./agstream-backends")
STORAGE_DIR = os.path.join(AGSTREAM_BACKENDS, "saved_functions")
SCHEMA_REGISTRY_URL = os.getenv(
    "AGSTREAM_BACKENDS_SCHEMA_REGISTRY_URL", "http://localhost:8081"
)
SERVICE_PORT = int(os.getenv("AGSTREAM_BACKENDS_SERVICE_PORT", "8083"))

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Initialize persistent store
print(f"🔧 Initializing PersistentFunctionStore...")
print(f"   Storage: {STORAGE_DIR}")
print(f"   Schema Registry: {SCHEMA_REGISTRY_URL}")

store = PersistentFunctionStore(
    storage_dir=STORAGE_DIR, schema_registry_url=SCHEMA_REGISTRY_URL
)

print(f"✅ Loaded {len(store.list_functions())} functions from disk")
print()


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify(
        {
            "status": "healthy",
            "storage_dir": STORAGE_DIR,
            "schema_registry_url": SCHEMA_REGISTRY_URL,
            "functions_loaded": len(store.list_functions()),
            "functions_saved": len(store.list_saved_functions()),
        }
    )


@app.route("/functions", methods=["GET"])
def list_functions():
    """List all functions"""
    return jsonify(
        {
            "functions": store.list_functions(),
            "saved": store.list_saved_functions(),
            "summary": store.summary_with_persistence(),
        }
    )


@app.route("/functions/<name>", methods=["GET"])
def get_function(name):
    """Get function metadata"""
    metadata = store.function_metadata(name)
    if metadata is None:
        return jsonify({"error": f"Function '{name}' not found"}), 404

    return jsonify(metadata)


@app.route("/functions/<name>", methods=["DELETE"])
def delete_function(name):
    """Delete a function"""
    success = store.delete_function(name, remove_from_disk=True)
    if not success:
        return jsonify({"error": f"Function '{name}' not found"}), 404

    return jsonify({"message": f"Function '{name}' deleted"})


@app.route("/functions", methods=["POST"])
def create_function():
    """
    Create and save a new function.

    Request body:
    {
        "name": "my_function",
        "description": "Function description",
        "input_model_name": "InputType",
        "target_model_name": "OutputType",
        "instructions": "LLM instructions",
        "tools": ["tool1", "tool2"]
    }
    """
    data = request.json

    # Validate required fields
    required = ["name", "input_model_name", "target_model_name"]
    for field in required:
        if field not in data:
            return jsonify({"error": f"Missing required field: {field}"}), 400

    try:
        # Get types from schema registry
        input_model = get_atype_from_registry(
            atype_name=data["input_model_name"], schema_registry_url=SCHEMA_REGISTRY_URL
        )
        target_model = get_atype_from_registry(
            atype_name=data["target_model_name"],
            schema_registry_url=SCHEMA_REGISTRY_URL,
        )

        # Create function
        fn = make_transducible_function(
            input_model=input_model,
            target_model=target_model,
            instructions=data.get("instructions", ""),
            tools=data.get("tools", []),
        )

        # Save function
        path = store.save_function(
            name=data["name"],
            fn=fn,
            description=data.get("description", ""),
            auto_register_schemas=False,  # Types already in registry
        )

        return (
            jsonify({"message": f"Function '{data['name']}' created", "path": path}),
            201,
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/functions/<name>/export", methods=["GET"])
def export_function(name):
    """Export function metadata"""
    metadata = store.function_metadata(name)
    if metadata is None:
        return jsonify({"error": f"Function '{name}' not found"}), 404

    return jsonify(metadata)


@app.route("/functions/import", methods=["POST"])
def import_function():
    """
    Import a function from metadata.

    Request body: function metadata JSON
    """
    try:
        metadata = request.json

        # Save metadata to temp file
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            import json

            json.dump(metadata, f)
            temp_path = f.name

        # Import function
        name = store.import_function(temp_path)

        # Clean up temp file
        os.unlink(temp_path)

        if name is None:
            return jsonify({"error": "Failed to import function"}), 400

        return jsonify({"message": f"Function '{name}' imported", "name": name}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/", methods=["GET"])
def index():
    """Service info"""
    return jsonify(
        {
            "service": "Function Persistence Service",
            "version": "1.0.0",
            "endpoints": {
                "health": "/health",
                "list_functions": "/functions",
                "get_function": "/functions/<name>",
                "create_function": "POST /functions",
                "delete_function": "DELETE /functions/<name>",
                "export_function": "/functions/<name>/export",
                "import_function": "POST /functions/import",
            },
            "storage_dir": STORAGE_DIR,
            "schema_registry_url": SCHEMA_REGISTRY_URL,
        }
    )


if __name__ == "__main__":
    print(f"🚀 Starting Function Persistence Service on port {SERVICE_PORT}")
    print(f"   API: http://localhost:{SERVICE_PORT}")
    print(f"   Health: http://localhost:{SERVICE_PORT}/health")
    print(f"   Functions: http://localhost:{SERVICE_PORT}/functions")
    print()

    app.run(host="0.0.0.0", port=SERVICE_PORT, debug=False)

# Made with Bob
