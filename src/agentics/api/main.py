import json
from typing import Annotated, List, Dict, Any

from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError

# Import registry and the applications package to trigger registration
from agentics.api.core.registry import registry
import agentics.api.applications

from agentics.api.core.models import AppMetadata, AppSchema

app = FastAPI(
    title="Agentics API",
    version="0.1.0",
    description="Unified API for Agentic Applications",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/apps", response_model=List[AppMetadata])
async def list_applications():
    return [
        AppMetadata(slug=a.slug, name=a.name, description=a.description)
        for a in registry.list_apps()
    ]


@app.get("/apps/{slug}/config", response_model=AppSchema)
async def get_app_config(slug: str):
    agent = registry.get(slug)
    if not agent:
        raise HTTPException(404, "App not found")

    return AppSchema(
        input_schema=agent.get_input_model().model_json_schema(),
        output_schema=agent.get_output_model().model_json_schema(),
        options=await agent.get_options(),
    )


@app.post("/apps/{slug}/run")
async def run_app(
    slug: str,
    config: Annotated[str, Form()],
    files: List[UploadFile] = File(default=[]),
):
    agent = registry.get(slug)
    if not agent:
        raise HTTPException(404, "App not found")

    # Parse Config
    try:
        config_data = json.loads(config)
        validated_input = agent.get_input_model()(**config_data)
    except (json.JSONDecodeError, ValidationError) as e:
        raise HTTPException(422, detail=str(e))

    # Map Files
    file_map = {f.filename: f.file for f in files}

    # Execute
    try:
        return await agent.run(validated_input, file_map)
    except Exception as e:
        raise HTTPException(500, detail=str(e))
