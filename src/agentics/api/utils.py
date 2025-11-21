import importlib.util
import inspect
import sys
from pathlib import Path
from typing import Any, Dict, Type, Optional
from pydantic import BaseModel, create_model


def load_pydantic_model_from_path(file_path: Path) -> Optional[Type[BaseModel]]:
    """
    Loads a Pydantic model from a .py file.
    Assumes the file contains exactly one class inheriting from BaseModel.
    """
    if not file_path.exists():
        return None

    module_name = file_path.stem
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    for name, obj in inspect.getmembers(module):
        if inspect.isclass(obj) and issubclass(obj, BaseModel) and obj is not BaseModel:
            # Avoid importing imported classes from other modules
            if obj.__module__ == module_name:
                return obj
    return None


def create_dynamic_model(
    model_name: str, field_definitions: Dict[str, Any]
) -> Type[BaseModel]:
    """
    Creates a Pydantic model from a dictionary definition (JSON Schema-like).
    Simple implementation for Phase 2 MVP.
    """
    # This is a simplified version. Real implementation would parse full JSON Schema.
    # For now, we assume the input config passes a target_model_name or we use a default.
    # This placeholder allows us to extend later.
    pass
