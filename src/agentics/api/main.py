import json
from typing import Annotated, List, Optional

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError

from agentics.api.core.registry import registry
from agentics.api.models import AppMetadata, AppSchema, ErrorResponse

# --- Initialize App ---
app = FastAPI(
    title="Agentics API",
    description="Unified API for Agentics Logic Transduction Applications",
    version="0.1.0",
)

# --- CORS ---
# Allow all for development ease; restrict in production if needed.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Endpoints ---


@app.get("/apps", response_model=List[AppMetadata])
async def list_applications():
    """List all available agentic applications."""
    apps = registry.list_apps()
    return [
        AppMetadata(slug=a.slug, name=a.name, description=a.description) for a in apps
    ]


@app.get("/apps/{slug}/schema", response_model=AppSchema)
async def get_app_schema(slug: str):
    """
    Get the input/output schema and dynamic options for an app.
    Used by the frontend to build the UI dynamically.
    """
    agent_app = registry.get(slug)
    if not agent_app:
        raise HTTPException(status_code=404, detail="Application not found")

    input_model = agent_app.get_input_model()
    output_model = agent_app.get_output_model()
    options = await agent_app.get_options()

    return AppSchema(
        input_schema=input_model.model_json_schema(),
        output_schema=output_model.model_json_schema(),
        options=options,
    )


@app.post("/apps/{slug}/run")
async def run_app(
    slug: str,
    config: Annotated[
        str, Form(description="JSON string matching the app's Input Schema")
    ],
    files: List[UploadFile] = File(default=[]),
):
    """
    Execute an application.
    Accepts `multipart/form-data`.
    - `config`: JSON string defining the typed input.
    - `files`: Optional list of file uploads.
    """
    agent_app = registry.get(slug)
    if not agent_app:
        raise HTTPException(status_code=404, detail="Application not found")

    # 1. Parse Configuration
    try:
        config_dict = json.loads(config)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON in 'config' field")

    # 2. Validate against App's Input Model
    InputModel = agent_app.get_input_model()
    try:
        validated_input = InputModel(**config_dict)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors())

    # 3. Organize Files
    # We pass a simple dict mapping filename -> file object for the adapter to handle
    file_map = {f.filename: f.file for f in files}

    # 4. Execution
    try:
        result = await agent_app.run(validated_input, file_map)
    except Exception as e:
        # Log error here in production
        raise HTTPException(status_code=500, detail=str(e))

    return result


# --- Health Check ---
@app.get("/health")
async def health_check():
    return {"status": "ok"}
