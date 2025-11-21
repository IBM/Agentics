from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class AppMetadata(BaseModel):
    """Summary metadata for listing available applications."""

    slug: str
    name: str
    description: str


class AppSchema(BaseModel):
    """
    The blueprint for the UI.
    - input_schema: JSON Schema for the form configuration.
    - output_schema: JSON Schema for the results.
    - options: Dynamic values for dropdowns (e.g., available DBs, dates).
    """

    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]
    options: Optional[Dict[str, Any]] = None


class ErrorResponse(BaseModel):
    detail: str
