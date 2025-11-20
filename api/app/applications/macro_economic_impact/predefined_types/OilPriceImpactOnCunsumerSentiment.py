from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class OilPriceImpactOnCunsumerSentiment(BaseModel):
    oil_price_variation: float | None = Field(
        default=None,
        description="This is the variation on the price of oil observed in the given period",
    )
    customer_sentiment_in_news: str | None = Field(
        default=None,
        description="Report the variation of consumer sentiment with respect to the oil price change",
    )
    impact_on_consumer_price_index: float | None = Field(
        default=None,
        description="Report the variation of consumer price index in the given period",
    )
    explanation: str | None = Field(
        default=None,
        description="The reason why there was a variation in sentiment as reported in news",
    )
