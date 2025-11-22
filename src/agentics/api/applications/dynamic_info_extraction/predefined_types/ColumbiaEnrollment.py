from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ColumbiaEnrollment(BaseModel):
    policy_name: str | None = Field(
        default=None, description="Name or description of the government policy"
    )
    policy_type: str | None = Field(
        default=None,
        description="Type of policy (e.g., regulation, tax, ban, endorsement)",
    )
    jurisdiction: str | None = Field(
        default=None, description="Country or region where the policy is applied"
    )
    overall_assessment: str | None = Field(
        default=None,
        description="Overall assessment summarizing the combined impact on market and consumer sentiment.",
    )
    term: str | None = Field(
        default=None, description="Academic term of the enrollment (e.g., Fall 2025)"
    )
    enrollment_status: str | None = Field(
        default=None,
        description="Current status of the enrollment (e.g., enrolled, waitlisted, dropped)",
    )
    enrollment_date: str | None = Field(
        default=None,
        description="Date when the student enrolled in the class (ISO format)",
    )
    credits: int | None = Field(
        default=None, description="Number of credits for the class"
    )
