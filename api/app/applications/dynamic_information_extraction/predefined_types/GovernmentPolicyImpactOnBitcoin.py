from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class GovernmentPolicyImpactOnBitcoin(BaseModel):
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
    bitcoin_price_change_percentage: float | None = Field(
        default=None,
        description="Percentage change in Bitcoin price associated with the policy",
    )
    bitcoin_price_change_amount: float | None = Field(
        default=None,
        description="Absolute change in Bitcoin price (e.g., USD) linked to the policy",
    )
    impact_direction: str | None = Field(
        default=None,
        description="Overall direction of impact (e.g., increase, decrease, neutral)",
    )
    effective_date: str | None = Field(
        default=None, description="Date when the policy became effective"
    )
    source_reference: str | None = Field(
        default=None,
        description="Reference or source of the information about the policy impact",
    )
