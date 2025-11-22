from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PersonDemographics(BaseModel):
    first_name: str | None = Field(default=None, description="The person's given name.")
    last_name: str | None = Field(default=None, description="The person's family name.")
    age: int | None = Field(default=None, description="The person's age in years.")
    gender: str | None = Field(
        default=None, description="The person's gender identity."
    )
    ethnicity: str | None = Field(
        default=None, description="The person's ethnic background."
    )
    nationality: str | None = Field(
        default=None, description="The country of citizenship."
    )
    date_of_birth: str | None = Field(
        default=None, description="The person's birth date in ISO format (YYYY-MM-DD)."
    )
    address: str | None = Field(
        default=None, description="The person's residential address."
    )
