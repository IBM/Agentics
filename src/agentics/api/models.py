from typing import Any, Dict, Optional
from pydantic import BaseModel, Field

class AppMetadata(BaseModel):
    id: str
    name: str
    description: str
    icon: str | None = "🤖"

class SessionResponse(BaseModel):
    session_id: str
    app_id: str
    message: str

class ErrorResponse(BaseModel):
    detail: str