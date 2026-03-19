"""
persistent_function_store.py
=============================
Persistent storage for TransducibleFunction objects with full serialization
to disk and schema registry integration.

Features:
- Save/load transducible functions to/from disk
- Automatic schema registry integration for input/output types
- Support for custom instructions and tools
- JSON-based metadata storage
- Automatic reconstruction of functions from metadata

Usage:
------
::

    from persistent_function_store import PersistentFunctionStore
    from agentics.core.transducible_functions import make_transducible_function

    store = PersistentFunctionStore(
        storage_dir="./saved_functions",
        schema_registry_url="http://localhost:8081"
    )

    # Create and save a function
    fn = make_transducible_function(
        input_model=Review,
        target_model=Summary,
        instructions="Summarize the review in 2 sentences",
        tools=["web_search"]
    )
    store.save_function("summarize_review", fn)

    # Load it later (even in a new session)
    loaded_fn = store.load_function("summarize_review")

    # List all saved functions
    print(store.list_saved_functions())
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from function_store import FunctionStore

from agentics.core.streaming.streaming_utils import (
    get_atype_from_registry,
    register_atype_schema,
)
from agentics.core.transducible_functions import make_transducible_function


class PersistentFunctionStore(FunctionStore):
    """
    Extends FunctionStore with full disk persistence and schema registry integration.

    Functions are stored as JSON metadata files containing:
    - Function name
    - Input/output type names (registered in schema registry)
    - Instructions
    - Tools
    - Custom function code (if available)
    """

    def __init__(
        self,
        storage_dir: str = "./saved_functions",
        schema_registry_url: str = "http://localhost:8081",
    ):
        """
        Initialize persistent function store.

        Args:
            storage_dir: Directory to store function metadata files
            schema_registry_url: URL of the schema registry for type persistence
        """
        super().__init__()
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.schema_registry_url = schema_registry_url

        # Auto-load all saved functions on initialization
        self._auto_load_all()

    def save_function(
        self,
        name: str,
        fn: Any,
        description: str = "",
        auto_register_schemas: bool = True,
    ) -> str:
        """
        Save a transducible function to disk with full metadata.

        Args:
            name: Unique name for the function
            fn: TransducibleFunction instance
            description: Optional description
            auto_register_schemas: If True, automatically register input/output
                types in schema registry

        Returns:
            Path to the saved metadata file
        """
        # Add to in-memory store
        self.add_function(name, fn)

        # Extract function metadata
        input_model = getattr(fn, "input_model", None)
        target_model = getattr(fn, "target_model", None)

        if input_model is None or target_model is None:
            raise ValueError(
                f"Function '{name}' must have input_model and target_model attributes"
            )

        # Register schemas in registry if requested
        if auto_register_schemas:
            try:
                register_atype_schema(
                    atype=input_model,
                    schema_registry_url=self.schema_registry_url,
                    compatibility="NONE",
                )
                register_atype_schema(
                    atype=target_model,
                    schema_registry_url=self.schema_registry_url,
                    compatibility="NONE",
                )
            except Exception as e:
                print(f"Warning: Could not register schemas: {e}")

        # Extract function source code if available
        function_code = None
        try:
            if hasattr(fn, "__original_fn__"):
                function_code = inspect.getsource(fn.__original_fn__)
            elif hasattr(fn, "__wrapped__"):
                function_code = inspect.getsource(fn.__wrapped__)
        except (TypeError, OSError):
            # Function was dynamically created, no source available
            pass

        # Build metadata
        metadata = {
            "name": name,
            "description": description,
            "input_model_name": input_model.__name__,
            "target_model_name": target_model.__name__,
            "instructions": getattr(fn, "instructions", ""),
            "tools": getattr(fn, "tools", []),
            "function_code": function_code,
            "schema_registry_url": self.schema_registry_url,
        }

        # Save to disk
        path = self.storage_dir / f"{name}.json"
        with open(path, "w") as f:
            json.dump(metadata, f, indent=2)

        return str(path)

    def load_function(self, name: str) -> Optional[Any]:
        """
        Load a transducible function from disk.

        Args:
            name: Name of the function to load

        Returns:
            Reconstructed TransducibleFunction or None if not found
        """
        path = self.storage_dir / f"{name}.json"
        if not path.exists():
            return None

        # Load metadata
        with open(path) as f:
            metadata = json.load(f)

        # Retrieve types from schema registry
        try:
            input_model = get_atype_from_registry(
                atype_name=metadata["input_model_name"],
                schema_registry_url=self.schema_registry_url,
            )
            target_model = get_atype_from_registry(
                atype_name=metadata["target_model_name"],
                schema_registry_url=self.schema_registry_url,
            )
        except Exception as e:
            print(f"Error loading types from registry: {e}")
            return None

        # Reconstruct function
        fn = make_transducible_function(
            input_model=input_model,
            target_model=target_model,
            instructions=metadata.get("instructions", ""),
            tools=metadata.get("tools", []),
        )

        # Store function code as attribute if available
        if metadata.get("function_code"):
            fn._original_code = metadata["function_code"]

        # Add to in-memory store
        self.add_function(name, fn)

        return fn

    def delete_function(self, name: str, remove_from_disk: bool = True) -> bool:
        """
        Delete a function from memory and optionally from disk.

        Args:
            name: Name of the function to delete
            remove_from_disk: If True, also delete the metadata file

        Returns:
            True if function was deleted, False if not found
        """
        # Remove from memory
        removed = self.remove_function(name)

        # Remove from disk
        if remove_from_disk:
            path = self.storage_dir / f"{name}.json"
            if path.exists():
                path.unlink()
                removed = True

        return removed

    def list_saved_functions(self) -> List[str]:
        """
        List all functions saved to disk.

        Returns:
            Sorted list of function names
        """
        return sorted([p.stem for p in self.storage_dir.glob("*.json")])

    def function_metadata(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get full metadata for a saved function without loading it.

        Args:
            name: Name of the function

        Returns:
            Metadata dict or None if not found
        """
        path = self.storage_dir / f"{name}.json"
        if not path.exists():
            return None

        with open(path) as f:
            return json.load(f)

    def export_function(self, name: str, output_path: str) -> bool:
        """
        Export a function's metadata to a specific file.

        Args:
            name: Name of the function to export
            output_path: Path to export to

        Returns:
            True if successful, False otherwise
        """
        metadata = self.function_metadata(name)
        if metadata is None:
            return False

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        with open(output, "w") as f:
            json.dump(metadata, f, indent=2)

        return True

    def import_function(self, input_path: str) -> Optional[str]:
        """
        Import a function from an exported metadata file.

        Args:
            input_path: Path to the metadata file

        Returns:
            Name of the imported function or None if failed
        """
        input_file = Path(input_path)
        if not input_file.exists():
            return None

        with open(input_file) as f:
            metadata = json.load(f)

        name = metadata.get("name")
        if not name:
            return None

        # Copy to storage directory
        dest = self.storage_dir / f"{name}.json"
        with open(dest, "w") as f:
            json.dump(metadata, f, indent=2)

        # Load into memory
        self.load_function(name)

        return name

    def _auto_load_all(self) -> int:
        """
        Automatically load all saved functions on initialization.

        Returns:
            Number of functions loaded
        """
        count = 0
        for name in self.list_saved_functions():
            try:
                self.load_function(name)
                count += 1
            except Exception as e:
                print(f"Warning: Could not load function '{name}': {e}")

        return count

    def summary_with_persistence(self) -> List[Dict[str, Any]]:
        """
        Get a detailed summary of all functions including persistence status.

        Returns:
            List of dicts with function details
        """
        result = []

        # In-memory functions
        for name in self.list_functions():
            fn = self.get_function(name)
            info = self.function_info(name)
            saved = (self.storage_dir / f"{name}.json").exists()

            result.append(
                {
                    "type": "function",
                    "name": name,
                    "input_model": info["input_model"],
                    "target_model": info["target_model"],
                    "saved_to_disk": saved,
                    "has_instructions": bool(getattr(fn, "instructions", "")),
                    "has_tools": bool(getattr(fn, "tools", [])),
                }
            )

        # Saved but not loaded
        for name in self.list_saved_functions():
            if name not in self._functions:
                metadata = self.function_metadata(name)
                result.append(
                    {
                        "type": "function",
                        "name": name,
                        "input_model": metadata["input_model_name"],
                        "target_model": metadata["target_model_name"],
                        "saved_to_disk": True,
                        "loaded_in_memory": False,
                    }
                )

        return result

    def __repr__(self) -> str:
        saved_count = len(self.list_saved_functions())
        return (
            f"PersistentFunctionStore("
            f"storage_dir={self.storage_dir}, "
            f"in_memory={len(self._functions)}, "
            f"saved={saved_count})"
        )


# Example usage
if __name__ == "__main__":
    from pydantic import BaseModel, Field

    # Define example types
    class Review(BaseModel):
        text: str = Field(..., description="Review text")
        rating: int = Field(..., description="Rating 1-5")

    class Summary(BaseModel):
        summary: str = Field(..., description="Brief summary")
        sentiment: str = Field(..., description="positive/negative/neutral")

    # Create store
    store = PersistentFunctionStore(
        storage_dir="./test_functions", schema_registry_url="http://localhost:8081"
    )

    # Create and save a function
    fn = make_transducible_function(
        input_model=Review,
        target_model=Summary,
        instructions="Summarize the review and determine sentiment",
        tools=[],
    )

    path = store.save_function(
        "summarize_review", fn, description="Summarizes product reviews"
    )
    print(f"Saved to: {path}")

    # List saved functions
    print(f"Saved functions: {store.list_saved_functions()}")

    # Load it back
    loaded_fn = store.load_function("summarize_review")
    print(f"Loaded: {loaded_fn}")

    # Get summary
    print("\nSummary:")
    for item in store.summary_with_persistence():
        print(f"  {item}")

# Made with Bob
