from fastapi import APIRouter, HTTPException, Depends
from ...core import atype_store as store, llm
from ...core import models as m
from ...core.utils import make_class_code
from ...core.auth import verify_api_key

router = APIRouter(prefix="/atypes", tags=["atypes"])


@router.get("/", response_model=list[str], summary="List all saved atypes")
def list_atypes(api_key: str = Depends(verify_api_key)):
    """
    Retrieve names of all Pydantic atypes saved in the predefined_types directory.

    Returns a simple array of type names (e.g., ["Answer", "Person"]).
    """
    return store.list_names()


@router.post("/", response_model=m.AtypeInfo, summary="Create a new atype")
async def create(req: m.AtypeCreate, api_key: str = Depends(verify_api_key)):
    """
    Create a new Pydantic type from either:
    - **field_spec** mode: provide a list of fields with types and metadata
    - **natural_language** mode: describe the type in plain English; an LLM generates the code

    Optionally saves the generated .py file to disk (default: save=true).

    Returns the generated Python code and JSON schema.
    """
    if req.mode == m.Mode.field_spec:
        if not req.fields:
            raise HTTPException(400, "fields list required for field_spec mode")
        code = make_class_code([f.model_dump() for f in req.fields], req.name)
    else:
        if not req.description:
            raise HTTPException(400, "description required for natural_language mode")
        code, _ = await llm.nl_to_atype(req.name, req.description)
    if req.save:
        store.save_code(req.name, code)
    atype = store.code_to_type(code)
    return m.AtypeInfo(name=req.name, code=code, json_schema=atype.model_json_schema())


@router.get("/{name}", response_model=m.AtypeInfo, summary="Fetch atype details")
def fetch(name: str, api_key: str = Depends(verify_api_key)):
    """
    Retrieve the Python code and JSON schema for a saved atype.

    Useful for inspecting existing types before creating an agent.
    """
    code = store.load_code(name)
    atype = store.code_to_type(code)
    return m.AtypeInfo(name=name, code=code, json_schema=atype.model_json_schema())


@router.delete("/{name}", status_code=204, summary="Delete an atype")
def delete(name: str, api_key: str = Depends(verify_api_key)):
    """
    Remove the .py file for the specified atype from predefined_types.

    Returns 204 on success; 404 if the type does not exist.
    """
    store.delete(name)


@router.post("/{name}/validate", summary="Validate JSON against atype")
def validate(name: str, payload: dict, api_key: str = Depends(verify_api_key)):
    """
    Check whether an arbitrary JSON payload conforms to the specified atype schema.

    Returns `{"valid": true}` on success, or 422 with validation errors.
    """
    code = store.load_code(name)
    atype = store.code_to_type(code)
    try:
        atype.model_validate(payload)
        return {"valid": True}
    except Exception as e:
        raise HTTPException(422, str(e))
