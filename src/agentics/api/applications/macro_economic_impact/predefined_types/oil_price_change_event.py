from typing import Optional

from pydantic import BaseModel, Field


class OilPriceChangeEvent(BaseModel):
    """Represents an event of oil price change."""

    date: Optional[str] = Field(
        None, description="The date of the oil price change event in YYYY-MM-DD format."
    )
    price_change_percentage: Optional[float] = Field(
        None, description="The percentage change in oil prices on that date."
    )
    impact_on_market: Optional[str] = Field(
        None,
        description="A brief description of the impact of the oil price change on the stock market.",
    )
