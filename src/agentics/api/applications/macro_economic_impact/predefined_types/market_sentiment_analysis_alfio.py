from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class market_sentiment_analysis_alfio(BaseModel):
    date: str | None = Field(
        default=None,
        description="The date of the oil price change event in YYYY-MM-DD format.",
    )
    price_change_percentage: float | None = Field(
        default=None, description="The percentage change in oil prices on that date."
    )
    impact_on_market: str | None = Field(
        default=None,
        description="A brief description of the impact of the oil price change on the stock market.",
    )
    market_sentiment: float | None = Field(
        default=None, description="positive, negative, neutral"
    )
