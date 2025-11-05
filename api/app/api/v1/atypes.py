from fastapi import APIRouter, HTTPException
from ...core import atype_store as store, llm
from ...core import models as m
from ...core.utils import make_class_code

router = APIRouter(prefix="/atypes", tags=["atypes"])


@router.get("/", response_model=list[str])
def list_atypes():
    return store.list_names()


@router.post("/", response_model=m.AtypeInfo)
async def create(req: m.AtypeCreate):
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


@router.get("/{name}", response_model=m.AtypeInfo)
def fetch(name: str):
    code = store.load_code(name)
    atype = store.code_to_type(code)
    return m.AtypeInfo(name=name, code=code, json_schema=atype.model_json_schema())


@router.delete("/{name}", status_code=204)
def delete(name: str):
    store.delete(name)


@router.post("/{name}/validate")
def validate(name: str, payload: dict):
    code = store.load_code(name)
    atype = store.code_to_type(code)
    try:
        atype.model_validate(payload)
        return {"valid": True}
    except Exception as e:
        raise HTTPException(422, str(e))
