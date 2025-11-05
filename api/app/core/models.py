from __future__ import annotations
from enum import Enum
from typing import Any, List, Dict, Optional
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
