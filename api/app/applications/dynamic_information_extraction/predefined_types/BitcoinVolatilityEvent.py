from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class BitcoinVolatilityEvent(BaseModel):
    company_name: str | None = Field(
        default=None, description="Name of the company that filed for bankruptcy."
    )
    bankruptcy_date: str | None = Field(
        default=None,
        description="Date when the bankruptcy was declared (ISO 8601 format).",
    )
    sector: str | None = Field(
        default=None, description="Industry sector of the bankrupt company."
    )
    stock_price_change_percent: float | None = Field(
        default=None,
        description="Percentage change in the company's stock price around the bankruptcy event.",
    )
    global_market_index_change: float | None = Field(
        default=None,
        description="Percentage change in major global market indices (e.g., S&P 500, FTSE 100) attributed to the bankruptcy.",
    )
    source: str | None = Field(
        default=None,
        description="Reference or source of the information about the event.",
    )
