from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PersonWithHobby(BaseModel):
    policy_name: str | None = Field(
        default=None, description="Name or description of the government policy"
    )
    policy_type: str | None = Field(
        default=None,
        description="Type of policy (e.g., regulation, tax, ban, endorsement)",
    )
