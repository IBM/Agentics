import traceback
import shutil
import os
from typing import Dict, Any

from fastapi import (
    APIRouter,
    HTTPException,
    Path,
    Body,
    UploadFile,
    File,
    Depends,
    Request,
)

from agentics.api.services.app_registry import app_registry
from agentics.api.services.session_manager import session_manager
from agentics.api.models import SessionResponse, SessionStateResponse
from agentics.api.dependencies import limiter, get_execution_token
from agentics.api.config import settings

router = APIRouter()


@router.post("/apps/{app_id}/session", response_model=SessionResponse)
@limiter.limit("10/minute")  # Strict limit on creating sessions
async def create_session(
    request: Request, app_id: str = Path(...)  # Required for limiter
):
    app = app_registry.get_app(app_id)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")

    session_id = session_manager.create_session(app_id)
    return SessionResponse(
        session_id=session_id, app_id=app_id, message="Session initialized"
    )


@router.delete("/apps/{app_id}/session/{session_id}")
async def delete_session(app_id: str = Path(...), session_id: str = Path(...)):
    """Explicitly close a session and clean up resources."""
    session_manager.delete_session(session_id)
    return {"status": "closed"}


@router.get("/apps/{app_id}/options")
async def get_app_options(app_id: str = Path(...)):
    app = app_registry.get_app(app_id)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    return app.get_options()


@router.get("/apps/{app_id}/schema")
async def get_app_schema(app_id: str = Path(...)):
    """
    Returns the JSON Schema (Pydantic) for the application's input form.
    Used by the UI to dynamically generate forms.
    """
    app = app_registry.get_app(app_id)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    return app.get_input_schema()


@router.post("/apps/{app_id}/session/{session_id}/execute")
@limiter.limit(settings.DEFAULT_RATE_LIMIT)
async def execute_app(
    request: Request,
    app_id: str = Path(...),
    session_id: str = Path(...),
    payload: dict = Body(...),
    # Wait for a token (semaphore) before running logic
    _token: Any = Depends(get_execution_token),
):
    app = app_registry.get_app(app_id)
    session = session_manager.get_session(session_id)

    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    if session.app_id != app_id:
        raise HTTPException(status_code=400, detail="Session mismatch")

    try:
        result = await app.execute(session_id, payload)
        return result
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/apps/{app_id}/session/{session_id}/files")
def upload_file(
    app_id: str = Path(...), session_id: str = Path(...), file: UploadFile = File(...)
):
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    safe_filename = f"{session_id}_{os.path.basename(file.filename)}"

    try:
        # Now safe to run synchronous boto3/shutil calls
        file_ref = session_manager.storage.upload(file.file, safe_filename)
        session.files[file.filename] = file_ref
        return {"filename": file.filename, "status": "uploaded"}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, "File upload failed")


@router.delete("/apps/{app_id}/session/{session_id}/files/{filename}")
def delete_file(
    app_id: str = Path(...), session_id: str = Path(...), filename: str = Path(...)
):
    """
    Remove a file from the session context.
    """
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    # 1. Remove from Session Memory
    if filename in session.files:
        del session.files[filename]
    else:
        raise HTTPException(404, "File not found in session")

    session_manager.storage.delete(filename)

    return {"status": "deleted", "filename": filename}


@router.post("/apps/{app_id}/session/{session_id}/action/{action_name}")
@limiter.limit(settings.DEFAULT_RATE_LIMIT)
async def execute_action(
    request: Request,
    app_id: str = Path(...),
    session_id: str = Path(...),
    action_name: str = Path(...),
    payload: Dict[str, Any] = Body(...),
):
    app = app_registry.get_app(app_id)
    session = session_manager.get_session(session_id)

    if not app or not session:
        raise HTTPException(404, "Not found")

    try:
        return await app.perform_action(session_id, action_name, payload)
    except NotImplementedError:
        raise HTTPException(400, f"Action {action_name} not supported")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, detail=str(e))


@router.get("/apps/{app_id}/session/{session_id}", response_model=SessionStateResponse)
async def get_session_details(app_id: str = Path(...), session_id: str = Path(...)):
    """Retrieve current session state, including uploaded files."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # Return keys from the files dict (filenames)
    return SessionStateResponse(
        session_id=session_id, app_id=session.app_id, files=list(session.files.keys())
    )
