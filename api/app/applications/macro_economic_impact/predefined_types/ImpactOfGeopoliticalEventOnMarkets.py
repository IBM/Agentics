from __future__ import annotations

import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class ImpactOfGeopoliticalEventOnMarkets(BaseModel):
    event_type: str | None = Field(
        default=None,
        description="General category for the event, e.g. social unrest, hearthquake",
    )
    event_date: datetime | None = Field(
        default=None,
        description="The date when the event happened. If spans multiple days, use the starting date",
    )
    location_country: str | None = Field(
        default=None, description="Where the event happened, e.g. US , Venezuela"
    )
    economic_impact: Literal | None = Field(
        default=None,
        description="Use the following grades:\nA: High\nB: Medium\nC: Low",
    )
    explanation: str | None = Field(
        default=None,
        description="Explain the reasons for your assessment of the impact of the event on the market",
    )
