from __future__ import annotations
from enum import Enum
from typing import Any, List, Dict, Optional, Literal
from pydantic import BaseModel, Field


class Mode(str, Enum):
    field_spec = "field_spec"
    natural_language = "natural_language"


class FieldSpec(BaseModel):
    name: str
    type_label: str
    optional: bool = True
    use_default: bool = False
    default_value: Optional[str] = None
    description: Optional[str] = None


class AtypeCreate(BaseModel):
    mode: Mode
    name: str
    fields: Optional[List[FieldSpec]] = None
    description: Optional[str] = None
    save: bool = True


class AtypeInfo(BaseModel):
    name: str
    code: str
    json_schema: Dict[str, Any]


class AgentCreate(BaseModel):
    atype_name: str | None = None  # reference an existing type
    atype_code: str | None = None  # or supply raw code
    states: list[dict] | None = None  # optional initial states


class AgentMeta(BaseModel):
    session_id: str
    atype_name: str
    n_states: int


class StatesUpdate(BaseModel):
    mode: Literal["append", "replace"] = "append"
    states: list[dict]


class TransduceRequest(BaseModel):
    other: str | list[str]  # accept simple prompt(s) for MVP
    transduction_type: Literal["amap", "areduce"] = "amap"
    areduce_batch_size: int | None = None
