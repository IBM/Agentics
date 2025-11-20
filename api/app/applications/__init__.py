import importlib
import os
import traceback
from pathlib import Path
from typing import List, Dict
from fastapi import FastAPI, APIRouter

router = APIRouter(tags=["Applications"])

REGISTERED_APPS: List[Dict] = []


def register_applications(app: FastAPI):
    global REGISTERED_APPS
    REGISTERED_APPS = []

    apps_dir = Path(__file__).parent
    base_package = "api.app.applications"

    print(f"🔍 Scanning for applications in: {apps_dir}")

    for item in os.listdir(apps_dir):
        app_path = apps_dir / item

        if not app_path.is_dir() or item.startswith("__"):
            continue

        try:
            module_name = f"{base_package}.{item}.router"
            module = importlib.import_module(module_name)

            if hasattr(module, "router"):
                app_router = module.router
                meta = getattr(
                    module,
                    "APP_METADATA",
                    {
                        "name": item.replace("_", " ").title(),
                        "description": "No description provided.",
                    },
                )
                prefix = f"/v1/apps/{item}"
                app.include_router(app_router, prefix=prefix)

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

        except Exception as e:
            print(f"❌ FAILED to register '{item}': {e}")
            traceback.print_exc()

    app.include_router(router, prefix="/v1")


@router.get("/apps")
def list_applications():
    return REGISTERED_APPS
