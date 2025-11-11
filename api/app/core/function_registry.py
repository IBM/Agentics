from typing import Callable, Dict
from agentics import AG
from pydantic import BaseModel

# Very small registry; can extend / hot-register later.
_REG: Dict[str, Callable] = {}


def register(name: str, fn: Callable):
    _REG[name] = fn


def get(name: str) -> Callable:
    if name not in _REG:
        raise KeyError(name)
    return _REG[name]


# ------------------------------------------------------------------
# sample functions useful for tests / demos
# ------------------------------------------------------------------
async def identity(state):
    return state


async def concat_reduce(states: list[BaseModel]) -> BaseModel:
    """Merge list of states by taking union of their dicts (last wins)."""
    base = states[0].model_dump()
    for st in states[1:]:
        base |= st.model_dump()
    return states[0].__class__(**base)


register("identity", identity)
register("concat_reduce", concat_reduce)
