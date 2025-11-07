from fastapi import APIRouter, HTTPException
from ...core import ag_registry as reg

router = APIRouter(prefix="/session", tags=["session"])


@router.post("/", status_code=201)
def new_session():
    sid = reg.create_session()
    return {"session_id": sid}


@router.delete("/{sid}", status_code=204)
def delete_session(sid: str):
    try:
        reg.drop_session(sid)
    except KeyError:
        raise HTTPException(status_code=404, detail="Session not found")
