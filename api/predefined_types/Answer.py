from __future__ import annotations
from pydantic import BaseModel, Field


class Answer(BaseModel):
    short_answer: str | None = None
    answer_report: str | None = Field(
        None,
        description="A detailed Markdown Document reporting evidence for the above answer",
    )
    evidence: list[str] | None = Field(
        None,
        description="list of evidence sources used to support the answer",
    )
    confidence: float | None = None
