from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class BitcoinPoliticsImpactInfo(BaseModel):
    attack_type: str | None = Field(
        default=None, description="Type or nature of the terrorist attack."
    )
    attack_location: str | None = Field(
        default=None, description="Geographic location where the attack occurred."
    )
    attack_year: int | None = Field(
        default=None, description="Year when the attack took place."
    )
    economic_impact: float | None = Field(
        default=None,
        description="Quantitative measure of the economic impact, e.g., estimated loss in USD.",
    )
    impact_description: str | None = Field(
        default=None,
        description="Narrative description of how the attack affected the economy.",
    )
    date_retrieved: str | None = Field(
        default=None,
        description="The date when the information was retrieved or generated.",
    )
