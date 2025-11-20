import os
import glob
from pathlib import Path
from typing import List, Optional

# Base paths
BASE_DIR = Path("/code/api")  # Matches Dockerfile WORKDIR structure
GLOBAL_TYPES_PATH = os.getenv("ATYPES_PATH", str(BASE_DIR / "predefined_types"))
APPS_DIR = BASE_DIR / "app" / "applications"


def get_storage_path(app_id: Optional[str] = None) -> Path:
    """
    Resolves the directory where types should be stored/loaded.
    - If app_id is None: returns global predefined_types.
    - If app_id is set: returns api/app/applications/{app_id}/predefined_types.
    """
    if not app_id or app_id == "global":
        path = Path(GLOBAL_TYPES_PATH)
    else:
        # Security check: prevent directory traversal (e.g., "../../../etc")
        safe_app_id = os.path.basename(app_id)
        path = APPS_DIR / safe_app_id / "predefined_types"

    # Ensure directory exists (lazy creation)
    os.makedirs(path, exist_ok=True)
    return path


def list_available_atypes(app_id: Optional[str] = None) -> List[str]:
    """Lists all .py files in the target scope."""
    target_dir = get_storage_path(app_id)
    files = glob.glob(str(target_dir / "*.py"))
    # Return filenames without extension
    return [
        os.path.basename(f).replace(".py", "")
        for f in files
        if not f.endswith("__init__.py")
    ]


def load_atype_code(type_name: str, app_id: Optional[str] = None) -> Optional[str]:
    """Reads the raw Python code for a specific type."""
    target_dir = get_storage_path(app_id)
    file_path = target_dir / f"{type_name}.py"

    if not file_path.exists():
        return None

    with open(file_path, "r") as f:
        return f.read()


def save_atype_code(type_name: str, code: str, app_id: Optional[str] = None) -> str:
    """Saves the Python code to the filesystem."""
    target_dir = get_storage_path(app_id)
    file_path = target_dir / f"{type_name}.py"

    with open(file_path, "w") as f:
        f.write(code)

    return str(file_path)


def delete_atype(type_name: str, app_id: Optional[str] = None) -> bool:
    """Deletes a type file."""
    target_dir = get_storage_path(app_id)
    file_path = target_dir / f"{type_name}.py"

    if file_path.exists():
        os.remove(file_path)
        return True
    return False
