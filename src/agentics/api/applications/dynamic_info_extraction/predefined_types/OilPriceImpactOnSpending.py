from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class OilPriceImpactOnSpending(BaseModel):
    oil_price_change: float | None = Field(
        default=None,
        description="The percentage or absolute change in oil price being considered.",
    )
    market_impact: str | None = Field(
        default=None,
        description="A summary description of how oil price changes affect overall market conditions.",
    )
    affected_sectors: str | None = Field(
        default=None,
        description="Comma‑separated list of market sectors most influenced by oil price movements (e.g., energy, transportation, manufacturing).",
    )
    economic_indicators: str | None = Field(
        default=None,
        description="Key economic indicators that are impacted, such as inflation, GDP growth, or consumer price index.",
    )
    timeframe: str | None = Field(
        default=None,
        description="The time period over which the impact is evaluated (e.g., short‑term, long‑term, specific dates).",
    )
    region: str | None = Field(
        default=None,
        description="Geographic region of the analysis; default is United States.",
    )
