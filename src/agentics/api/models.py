from typing import Any, Dict, List, Optional, Literal, Union
from pydantic import BaseModel


class UIOption(BaseModel):
    """
    Defines how a dropdown or input should behave in the UI.
    - static: A simple list of values.
    - dependent: Values change based on another field's value.
    - date_range: Renders a date picker constrained between min/max.
    """

    type: Literal["static", "dependent", "date_range"]
    values: Optional[List[Any]] = None
    depends_on: Optional[str] = None
    mapping: Optional[Dict[str, List[Any]]] = None

    # New fields for date_range
    min: Optional[str] = None
    max: Optional[str] = None


class ActionMetadata(BaseModel):
    name: str
    label: str
    icon: Optional[str] = None
    input_source_field: str
    output_target_field: str


class AppMetadata(BaseModel):
    id: str
    name: str
    description: str
    icon: str | None = "🤖"
    ui_view: Literal["generic_form", "custom_spreadsheet"] = "generic_form"
    actions: List[ActionMetadata] = []

    # New Field: Markdown-supported guide for the user
    usage_guide: Optional[str] = None


class SessionResponse(BaseModel):
    session_id: str
    app_id: str
    message: str


class SessionStateResponse(BaseModel):
    session_id: str
    app_id: str
    files: List[str]


class ErrorResponse(BaseModel):
    detail: str
