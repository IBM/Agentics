from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class OilPriceImpactOnMarkets(BaseModel):
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
    sector_affected: str | None = Field(
        default=None,
        description="Key market sector most affected by oil price changes (e.g., transportation, manufacturing).",
    )
