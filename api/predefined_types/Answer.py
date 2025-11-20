from __future__ import annotations
from pydantic import BaseModel, Field


class Answer(BaseModel):
    answer: str = Field(..., description="The answer to the user's question")
    confidence: float | None = Field(
        None, description="Confidence score between 0 and 1"
    )
    reasoning: str | None = Field(
        None, description="Explanation of how the answer was derived"
    )
