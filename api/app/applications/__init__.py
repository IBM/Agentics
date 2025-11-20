import importlib
import pkgutil
import os
from pathlib import Path
from typing import List, Dict
from fastapi import FastAPI, APIRouter

# This router will serve the metadata endpoint (GET /v1/apps)
router = APIRouter(tags=["Applications"])

REGISTERED_APPS: List[Dict] = []


def register_applications(app: FastAPI):
    """
    Dynamically discovers and mounts routers from subdirectories.
    Expected structure: api/app/applications/{app_name}/router.py
    """
    global REGISTERED_APPS
    REGISTERED_APPS = []  # Reset on reload

    # Get the directory of this __init__.py file
    apps_dir = Path(__file__).parent

    # Base python path for imports
    base_package = "api.app.applications"

    print(f"🔍 Scanning for applications in: {apps_dir}")

    # Iterate over all subdirectories in api/app/applications
    for item in os.listdir(apps_dir):
        app_path = apps_dir / item

        # Skip files, __pycache__, etc.
        if not app_path.is_dir() or item.startswith("__"):
            continue

        try:
            # Attempt to import the 'router' module from the app folder
            # e.g., api.app.applications.dynamic_information_extraction.router
            module_name = f"{base_package}.{item}.router"
            module = importlib.import_module(module_name)

            # Check if the module has a 'router' object (APIRouter)
            if hasattr(module, "router"):
                app_router = module.router

                # Check for metadata (optional, defaults provided)
                meta = getattr(
                    module,
                    "APP_METADATA",
                    {
                        "name": item.replace("_", " ").title(),
                        "description": "No description provided.",
                    },
                )

                # Construct the URL prefix
                prefix = f"/v1/apps/{item}"

                # Mount the router
                app.include_router(app_router, prefix=prefix)

                # Add to registry for the frontend
                REGISTERED_APPS.append(
                    {
                        "id": item,
                        "name": meta.get("name"),
                        "description": meta.get("description"),
                        "path": prefix,
                        "files_needed": meta.get("files_needed", False),
                    }
                )

                print(f"✅ Registered App: {meta['name']} at {prefix}")
            else:
                print(f"⚠️  Skipping {item}: No 'router' object found in router.py")

        except ImportError as e:
            # If router.py doesn't exist, we just skip it (it might be a scaffold folder)
            pass
        except Exception as e:
            print(f"❌ Error loading app {item}: {e}")

    # Register the metadata endpoint
    app.include_router(router, prefix="/v1")


@router.get("/apps")
def list_applications():
    """Returns the list of available applications for the UI."""
    return REGISTERED_APPS
