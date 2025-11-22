from fastapi import APIRouter, HTTPException, Path, Body
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
        session_id=session_id,
        app_id=app_id,
        message="Session initialized"
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
    app_id: str = Path(...),
    session_id: str = Path(...),
    payload: dict = Body(...)
):
    app = app_registry.get_app(app_id)
    session = session_manager.get_session(session_id)

    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    if session.app_id != app_id:
        raise HTTPException(status_code=400, detail="Session mismatch")

    # Validate input schema (Simplistic validation for Phase 1)
    # In Phase 2 we can reconstruct the Pydantic model dynamically
    try:
        result = await app.execute(session_id, payload)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))