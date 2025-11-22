from pydantic import BaseModel
from typing import Dict, Any
from agentics.api.applications.base import AgenticsApp
from agentics.api.models import AppMetadata


class DummyInput(BaseModel):
    echo: str


class DummyApp(AgenticsApp):
    metadata = AppMetadata(
        id="dummy",
        name="Dummy Application",
        description="A test application to verify API infrastructure.",
        icon="🧪",
    )

    def get_input_schema(self) -> Dict[str, Any]:
        return DummyInput.model_json_schema()

    async def execute(self, session_id: str, input_data: DummyInput) -> Dict[str, Any]:
        return {"result": f"Echoing: {input_data['echo']}", "session": session_id}
