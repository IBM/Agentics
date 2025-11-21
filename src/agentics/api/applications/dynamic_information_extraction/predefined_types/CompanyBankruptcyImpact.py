from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CompanyBankruptcyImpact(BaseModel):
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
    affected_regions: str | None = Field(
        default=None, description="Geographic regions most affected by the bankruptcy."
    )
    impact_summary: str | None = Field(
        default=None,
        description="Brief summary of the overall impact on global markets.",
    )
    news_headline: str | None = Field(
        default=None, description="Representative news headline describing the event."
    )
    confidence_level: float | None = Field(
        default=None, description="Confidence level (0-1) of the impact assessment."
    )
