from typing import Any, Dict, List, Optional, Union, Literal
from pydantic import BaseModel


class UIOption(BaseModel):
    """
    Defines how a dropdown should behave in the UI.
    - static: A simple list of values.
    - dependent: Values change based on another field's value.
    """

    type: Literal["static", "dependent"]
    values: Optional[List[Any]] = None
    depends_on: Optional[str] = None
    # Mapping key (value of depends_on field) -> List of available options
    mapping: Optional[Dict[str, List[Any]]] = None


class ActionMetadata(BaseModel):
    """
    Defines a 'Side Action' button in the UI (e.g., 'Draft Schema').
    """

    name: str  # The action_name slug to send to API
    label: str  # Button text
    icon: Optional[str] = None
    # Which field in the form contains the input data for this action?
    input_source_field: str
    # Which field in the form should be populated with the result?
    output_target_field: str


class AppMetadata(BaseModel):
    id: str
    name: str
    description: str
    icon: str | None = "🤖"
    # Helper to tell UI if it should use the generic form or a custom view
    ui_view: Literal["generic_form", "custom_spreadsheet"] = "generic_form"
    actions: List[ActionMetadata] = []


class SessionResponse(BaseModel):
    session_id: str
    app_id: str
    message: str


class ErrorResponse(BaseModel):
    detail: str
