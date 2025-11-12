from fastapi import APIRouter, HTTPException
from ...core import ag_registry as reg

router = APIRouter(prefix="/session", tags=["session"])


@router.post("/", status_code=201, summary="Create a new session")
def new_session():
    """
    Generate a unique session ID and reserve a slot in the registry.

    Each session can hold one AG instance. Pass the returned `session_id` in the
    `X-Session` header for all subsequent agent operations.

    Sessions expire after 24 hours of inactivity (configurable TTL).
    """
    sid = reg.create_session()
    return {"session_id": sid}


@router.delete("/{sid}", status_code=204, summary="Delete a session")
def delete_session(sid: str):
    """
    Remove the session and any associated agent from the registry.

    Returns 204 on success; 404 if the session does not exist.
    """
    try:
        reg.drop_session(sid)
    except KeyError:
        raise HTTPException(status_code=404, detail="Session not found")
