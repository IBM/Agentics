from fastapi import APIRouter, HTTPException, Query, Body
from typing import List, Optional
from pydantic import BaseModel

# Import the updated store logic
from api.app.core.atype_store import (
    list_available_atypes,
    load_atype_code,
    save_atype_code,
    delete_atype,
)

# Import core agentics tool to validate code syntax
from agentics.core.atype import import_pydantic_from_code

router = APIRouter(tags=["Types"])


class TypeCreateRequest(BaseModel):
    name: str
    code: str


@router.get("/types", response_model=List[str])
def get_types(
    app_id: Optional[str] = Query(
        None, description="Filter by App ID. Leave empty for global types."
    )
):
    """List all available Pydantic types for the given scope."""
    return list_available_atypes(app_id)


@router.get("/types/{name}", response_model=dict)
def get_type_code(name: str, app_id: Optional[str] = Query(None)):
    """Get the source code for a specific type."""
    code = load_atype_code(name, app_id)
    if not code:
        raise HTTPException(
            status_code=404,
            detail=f"Type '{name}' not found in scope '{app_id or 'global'}'",
        )
    return {"name": name, "code": code}


@router.post("/types", response_model=dict)
def create_type(payload: TypeCreateRequest, app_id: Optional[str] = Query(None)):
    """Create or update a Pydantic type."""
    # Validate that the code actually compiles to a Pydantic model
    try:
        model = import_pydantic_from_code(payload.code)
        if not model:
            raise ValueError("Code did not return a valid Pydantic class")
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Invalid Python/Pydantic code: {str(e)}"
        )

    path = save_atype_code(payload.name, payload.code, app_id)
    return {"status": "saved", "path": path, "scope": app_id or "global"}


@router.delete("/types/{name}")
def remove_type(name: str, app_id: Optional[str] = Query(None)):
    """Delete a type."""
    success = delete_atype(name, app_id)
    if not success:
        raise HTTPException(status_code=404, detail="Type not found")
    return {"status": "deleted"}
