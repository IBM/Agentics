from fastapi import APIRouter, HTTPException, Depends
from ...core import ag_registry as reg
from ...core.auth import verify_api_key
from ...core import llm_tracker

router = APIRouter(prefix="/session", tags=["session"])


@router.post("/", status_code=201, summary="Create a new session")
def new_session(api_key: str = Depends(verify_api_key)):
    """
    Generate a unique session ID and reserve a slot in the registry.

    Each session can hold one AG instance. Pass the returned `session_id` in the
    `X-Session` header for all subsequent agent operations.

    Sessions expire after 24 hours of inactivity (configurable TTL).
    """
    sid = reg.create_session()
    return {"session_id": sid}


@router.delete("/{sid}", status_code=204, summary="Delete a session")
def delete_session(sid: str, api_key: str = Depends(verify_api_key)):
    """
    Remove the session and any associated agent from the registry.

    Returns 204 on success; 404 if the session does not exist.
    """
    try:
        reg.drop_session(sid)
        llm_tracker.reset_usage(sid)
    except KeyError:
        raise HTTPException(status_code=404, detail="Session not found")


@router.get("/{sid}/usage", summary="Get LLM usage for session")
def get_usage(sid: str, api_key: str = Depends(verify_api_key)):
    """
    Retrieve token usage statistics for this session.

    Returns used, limit, and remaining token counts.
    """
    return llm_tracker.get_usage(sid)
