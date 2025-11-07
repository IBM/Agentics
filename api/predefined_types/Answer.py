from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Any

class Answer(BaseModel):
    answer: str = Field(...)