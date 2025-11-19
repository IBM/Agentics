from fastapi import APIRouter, Header, HTTPException, Depends, Request
from ...core import models as m
from ...core import atype_store as store
from ...core import ag_registry as reg
from ...core.auth import verify_api_key
from ...core.rate_limit import limiter
from ...core import llm_tracker
from agentics import AG
from ...core.models import StatesUpdate, TransduceRequest
from ...core.models import AmapRequest, AreduceRequest
from ...core import function_registry as fr

router = APIRouter(prefix="/agents", tags=["agents"])


def get_session_id(x_session: str = Header(...)):
    return x_session


@router.post(
    "/", response_model=m.AgentMeta, status_code=201, summary="Create an agent"
)
@limiter.limit("60/minute")
def create_agent(
    request: Request,
    req: m.AgentCreate,
    sid: str = Depends(get_session_id),
    api_key: str = Depends(verify_api_key),
):
    """
    Instantiate a new Agentics (AG) object for this session.

    Provide either:
    - `atype_name`: reference a saved type from /atypes
    - `atype_code`: supply raw Python code for a Pydantic class

    Optionally include initial `states` (list of dicts matching the atype schema).

    Only one agent per session is allowed; subsequent calls return 409.
    """
    try:
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


@router.get("/{sid}", response_model=m.AgentMeta, summary="Get agent metadata")
@limiter.limit("100/minute")
def get_agent_meta(request: Request, sid: str, api_key: str = Depends(verify_api_key)):
    """
    Retrieve high-level info about the agent: atype name and current state count.

    Does not return the full state objects; use /states endpoint or operation results for that.
    """
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


@router.delete("/{sid}", status_code=204, summary="Delete agent and session")
@limiter.limit("60/minute")
def drop_agent(request: Request, sid: str, api_key: str = Depends(verify_api_key)):
    """
    Remove the agent and its session from the registry.

    Returns 204 on success. Idempotent; calling multiple times is safe.
    """
    reg.drop_session(sid)
    llm_tracker.reset_usage(sid)


def _require_agent(sid: str):
    try:
        ag = reg.get_agent(sid)
        if ag is None:
            raise HTTPException(404, "agent not created yet")
        return ag
    except KeyError:
        raise HTTPException(404, "session not found")


@router.post("/{sid}/states", summary="Append or replace agent states")
@limiter.limit("60/minute")
def update_states(
    request: Request,
    sid: str,
    req: StatesUpdate,
    api_key: str = Depends(verify_api_key),
):
    """
    Modify the agent's internal state list.

    - **append** (default): add new states to the end
    - **replace**: overwrite all existing states

    Each state must be a dict matching the agent's atype schema.

    Returns the updated state count.
    """
    ag = _require_agent(sid)
    if req.mode == "replace":
        ag.states = [ag.atype(**d) for d in req.states]
    else:
        ag.states.extend(ag.atype(**d) for d in req.states)
    reg.touch(sid)
    return {"n_states": len(ag.states)}


@router.post("/{sid}/transduce", summary="Run logical transduction (<<)")
@limiter.limit("10/minute")
async def transduce(
    request: Request,
    sid: str,
    req: TransduceRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Execute the Agentics transduction operator (`<<`) on the agent.

    Accepts:
    - `other`: a string or list of strings (prompts / source data)
    - `transduction_type`: "amap" (parallel) or "areduce" (batched reduction)
    - `areduce_batch_size`: optional batch size for areduce mode

    Returns the resulting states after LLM-based transformation.

    Example: transform ["Who is the US president?"] into structured Answer objects.

    Rate limited to 10 requests per minute due to LLM cost.
    Subject to per-session token quota (50k tokens).
    """
    ok, used, remaining = llm_tracker.check_quota(sid)
    if not ok:
        raise HTTPException(
            status_code=429,
            detail=f"Token quota exceeded. Used: {used}, Limit: {llm_tracker.MAX_TOKENS_PER_SESSION}",
        )

    ag = _require_agent(sid)
    ag.transduction_type = req.transduction_type
    if req.areduce_batch_size:
        ag.areduce_batch_size = req.areduce_batch_size

    other = req.other if isinstance(req.other, list) else [req.other]
    llm_tracker.track_usage(sid, other)

    result_ag = await (ag << other)
    reg.touch(sid)
    return {"states": [s.model_dump() for s in result_ag.states]}


@router.post("/{sid}/amap", summary="Apply async function to each state")
@limiter.limit("30/minute")
async def amap_endpoint(
    request: Request, sid: str, req: AmapRequest, api_key: str = Depends(verify_api_key)
):
    """
    Run an async map operation over all states in the agent.

    Provide:
    - `function_name`: name of a server-registered callable (e.g., "identity")
    - `timeout`: optional per-state timeout in seconds

    The function is applied in parallel; failed states are left unchanged.

    Returns the updated state list.
    """
    ag = _require_agent(sid)
    try:
        fn = fr.get(req.function_name)
    except KeyError:
        raise HTTPException(404, "function not registered")
    await ag.amap(fn, timeout=req.timeout)
    reg.touch(sid)
    return {"states": [s.model_dump() for s in ag.states]}


@router.post("/{sid}/areduce", summary="Reduce states to one via async function")
@limiter.limit("30/minute")
async def areduce_endpoint(
    request: Request,
    sid: str,
    req: AreduceRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Run an async reduce operation that collapses all states into a single result.

    Provide:
    - `function_name`: name of a server-registered reducer (e.g., "concat_reduce")

    The reducer receives the full state list and returns one state (or a new list).

    Returns the final state(s).
    """
    ag = _require_agent(sid)
    try:
        fn = fr.get(req.function_name)
    except KeyError:
        raise HTTPException(404, "function not registered")
    await ag.areduce(fn)
    reg.touch(sid)
    return {"states": [s.model_dump() for s in ag.states]}
