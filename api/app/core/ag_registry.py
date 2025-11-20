from __future__ import annotations
import time
import uuid
from typing import Dict, Tuple, Optional, Any

# One AG per session; value is (agent_or_none, last_touch_ts)
_REGISTRY: Dict[str, Tuple[Optional[Any], float]] = {}
_TTL_SEC = 24 * 3600


def _sweep_expired() -> None:
    now = time.time()
    expired = [sid for sid, (_, ts) in _REGISTRY.items() if now - ts > _TTL_SEC]
    for sid in expired:
        _REGISTRY.pop(sid, None)


def create_session() -> str:
    """Return new session_id and register an empty slot for its AG."""
    _sweep_expired()
    sid = str(uuid.uuid4())
    _REGISTRY[sid] = (None, time.time())
    return sid


def drop_session(sid: str) -> None:
    _REGISTRY.pop(sid, None)


def attach_agent(sid: str, agent) -> None:
    if sid not in _REGISTRY:
        raise KeyError("unknown session")
    _REGISTRY[sid] = (agent, time.time())


def get_agent(sid: str):
    if sid not in _REGISTRY:
        raise KeyError("unknown session")
    agent, _ = _REGISTRY[sid]
    return agent


def attach_agent_once(sid: str, agent):
    if sid not in _REGISTRY:
        raise KeyError("unknown session")
    if _REGISTRY[sid][0] is not None:
        raise ValueError("session already has an agent")
    _REGISTRY[sid] = (agent, time.time())


def touch(sid: str):
    ag, _ = _REGISTRY[sid]
    _REGISTRY[sid] = (ag, time.time())
