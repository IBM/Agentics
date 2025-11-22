import shutil
import os
from typing import Dict, Any
from fastapi import APIRouter, HTTPException, Path, Body, UploadFile, File
import traceback
from agentics.api.services.app_registry import app_registry
from agentics.api.services.session_manager import session_manager
from agentics.api.models import SessionResponse

router = APIRouter()


@router.post("/apps/{app_id}/session", response_model=SessionResponse)
async def create_session(app_id: str = Path(...)):
    app = app_registry.get_app(app_id)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")

    session_id = session_manager.create_session(app_id)
    return SessionResponse(
        session_id=session_id, app_id=app_id, message="Session initialized"
    )


@router.get("/apps/{app_id}/options")
async def get_app_options(app_id: str = Path(...)):
    app = app_registry.get_app(app_id)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    return app.get_options()


# Note: The execute endpoint will be generic here, but validation
# happens inside the app logic or via dynamic Pydantic model validation
@router.post("/apps/{app_id}/session/{session_id}/execute")
async def execute_app(
    app_id: str = Path(...), session_id: str = Path(...), payload: dict = Body(...)
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
async def upload_file(
    app_id: str = Path(...), session_id: str = Path(...), file: UploadFile = File(...)
):
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    # Save file locally
    # TODO
    # In production, use S3/Blob storage
    safe_filename = os.path.basename(file.filename)
    file_path = f"src/agentics/api/temp_files/{session_id}_{safe_filename}"

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    session.files[file.filename] = file_path
    return {"filename": file.filename, "status": "uploaded"}


@router.post("/apps/{app_id}/session/{session_id}/action/{action_name}")
async def execute_action(
    app_id: str = Path(...),
    session_id: str = Path(...),
    action_name: str = Path(...),
    payload: Dict[str, Any] = Body({}),
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
        raise HTTPException(500, str(e))
