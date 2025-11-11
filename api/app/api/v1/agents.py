from fastapi import APIRouter, Header, HTTPException, Depends
from ...core import models as m
from ...core import atype_store as store
from ...core import ag_registry as reg
from agentics import AG
from ...core.models import StatesUpdate, TransduceRequest
from ...core.models import AmapRequest, AreduceRequest
from ...core import function_registry as fr

router = APIRouter(prefix="/agents", tags=["agents"])


def get_session_id(x_session: str = Header(...)):
    return x_session  # validate header presence


@router.post("/", response_model=m.AgentMeta, status_code=201)
def create_agent(req: m.AgentCreate, sid: str = Depends(get_session_id)):
    try:
        # choose atype
        if req.atype_name:
            code = store.load_code(req.atype_name)
            atype = store.code_to_type(code)
        elif req.atype_code:
            atype = store.code_to_type(req.atype_code)
        else:
            raise HTTPException(400, "atype_name or atype_code required")
        ag = AG(atype=atype)
        if req.states:
            ag.states = [atype(**d) for d in req.states]
        reg.attach_agent_once(sid, ag)
        return m.AgentMeta(
            session_id=sid,
            atype_name=atype.__name__,
            n_states=len(ag.states),
        )
    except KeyError:
        raise HTTPException(404, "session not found")
    except ValueError as e:
        raise HTTPException(409, str(e))


@router.get("/{sid}", response_model=m.AgentMeta)
def get_agent_meta(sid: str):
    try:
        ag = reg.get_agent(sid)
        if ag is None:
            raise HTTPException(404, "agent not created yet")
        return m.AgentMeta(
            session_id=sid,
            atype_name=ag.atype.__name__,
            n_states=len(ag.states),
        )
    except KeyError:
        raise HTTPException(404, "session not found")


@router.delete("/{sid}", status_code=204)
def drop_agent(sid: str):
    reg.drop_session(sid)  # simple; removes agent + session


# --- helper to fetch AG with safety ----------------
def _require_agent(sid: str):
    try:
        ag = reg.get_agent(sid)
        if ag is None:
            raise HTTPException(404, "agent not created yet")
        return ag
    except KeyError:
        raise HTTPException(404, "session not found")


# ----------  States  ----------
@router.post("/{sid}/states")
def update_states(sid: str, req: StatesUpdate):
    ag = _require_agent(sid)
    if req.mode == "replace":
        ag.states = [ag.atype(**d) for d in req.states]
    else:  # append
        ag.states.extend(ag.atype(**d) for d in req.states)
    reg.touch(sid)
    return {"n_states": len(ag.states)}


# ----------  Transduce  ----------
@router.post("/{sid}/transduce")
async def transduce(sid: str, req: TransduceRequest):
    ag = _require_agent(sid)
    ag.transduction_type = req.transduction_type
    if req.areduce_batch_size:
        ag.areduce_batch_size = req.areduce_batch_size
    # Accept str or list[str]
    other = req.other if isinstance(req.other, list) else [req.other]
    result_ag = await (ag << other)
    reg.touch(sid)
    return {"states": [s.model_dump() for s in result_ag.states]}


# ----------  amap ----------
@router.post("/{sid}/amap")
async def amap_endpoint(sid: str, req: AmapRequest):
    ag = _require_agent(sid)
    try:
        fn = fr.get(req.function_name)
    except KeyError:
        raise HTTPException(404, "function not registered")
    await ag.amap(fn, timeout=req.timeout)
    reg.touch(sid)
    return {"states": [s.model_dump() for s in ag.states]}


# ----------  areduce ----------
@router.post("/{sid}/areduce")
async def areduce_endpoint(sid: str, req: AreduceRequest):
    ag = _require_agent(sid)
    try:
        fn = fr.get(req.function_name)
    except KeyError:
        raise HTTPException(404, "function not registered")
    await ag.areduce(fn)
    reg.touch(sid)
    return {"states": [s.model_dump() for s in ag.states]}
