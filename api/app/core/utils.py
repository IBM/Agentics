from __future__ import annotations
from typing import Any, get_args, get_origin
from pydantic import Field

SIMPLE_TYPES = {"str": str, "int": int, "float": float, "bool": bool, "Any": Any}


def _type_to_str(t: Any) -> str:
    origin = get_origin(t)
    if origin is list:
        args = get_args(t)
        inner = _type_to_str(args[0]) if args else "Any"
        return f"list[{inner}]"
    if t in (str, int, float, bool, Any):
        return t.__name__ if t is not Any else "Any"
    return getattr(t, "__name__", repr(t))


def make_class_code(fields_spec: list[dict], model_name: str) -> str:
    lines = [
        "from __future__ import annotations",
        "from pydantic import BaseModel, Field",
        "from typing import Any",
        "",
        f"class {model_name}(BaseModel):",
    ]
    if not fields_spec:
        lines.append("    pass")
        return "\n".join(lines)
    for fs in fields_spec:
        ann = _type_to_str(SIMPLE_TYPES.get(fs["type_label"], Any))
        rhs = "Field(...)"  # keep simple; add default later if needed
        lines.append(f"    {fs['name']}: {ann} = {rhs}")
    return "\n".join(lines)
