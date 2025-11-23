from typing import Dict, Any, List, Tuple
from pydantic import BaseModel
from agentics import AG
from agentics.api.applications.base import AgenticsApp
from agentics.api.models import AppMetadata
from agentics.api.services.session_manager import session_manager
from agentics.core.atype import create_pydantic_model, make_all_fields_optional


class SchemaUpdate(BaseModel):
    # List of [name, type, description, required]
    fields: List[Tuple[str, str, str, bool]]
    model_name: str = "DynamicModel"


class TransductionRequest(BaseModel):
    source_fields: List[str]
    target_fields: List[str]
    instructions: str = None


class SmartSpreadsheetApp(AgenticsApp):
    metadata = AppMetadata(
        id="smart_spreadsheet",
        name="Smart Spreadsheet",
        description="Interactive data transformation workbench.",
        icon="📊",
    )

    def get_input_schema(self) -> Dict[str, Any]:
        return TransductionRequest.model_json_schema()

    async def perform_action(self, session_id: str, action: str, payload: dict) -> Any:
        session = session_manager.get_session(session_id)

        # Action: Initialize/Update Data from File
        if action == "load_file":
            filename = payload.get("filename")
            if filename not in session.files:
                raise ValueError("File not found")

            path = session_manager.get_file_path(session_id, filename)
            # Load CSV and store AG in session
            # Defaulting to all optional fields for flexibility
            ag = AG.from_csv(path)
            session.ag_instance = ag
            return self._serialize_state(ag)

        # Action: Update Schema (Rebind)
        if action == "update_schema":
            data = SchemaUpdate(**payload)
            if not session.ag_instance:
                # Create empty AG if none exists
                session.ag_instance = AG()

            new_atype = create_pydantic_model(data.fields, name=data.model_name)

            # Rebind existing data to new schema
            session.ag_instance = session.ag_instance.rebind_atype(new_atype)
            return self._serialize_state(session.ag_instance)

        # Action: Get Current State
        if action == "get_state":
            return self._serialize_state(session.ag_instance)

        raise NotImplementedError

    async def execute(
        self, session_id: str, input_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        # This app uses execute() for the "Perform Transduction" button
        req = TransductionRequest(**input_data)
        session = session_manager.get_session(session_id)

        if not session.ag_instance:
            raise ValueError("No active spreadsheet data")

        ag = session.ag_instance

        # Configure instructions if provided
        if req.instructions:
            ag.instructions = req.instructions

        # Run Self-Transduction
        # This updates the AG state in place (or returns new one)
        # The self_transduction method returns a NEW AG instance
        result_ag = await ag.self_transduction(
            source_fields=req.source_fields, target_fields=req.target_fields
        )

        # Update session state
        session.ag_instance = result_ag

        return self._serialize_state(result_ag)

    def _serialize_state(self, ag: AG) -> Dict[str, Any]:
        if not ag:
            return {"schema": None, "data": []}

        # Return schema definition and row data
        return {
            "schema": ag.atype.model_json_schema(),
            "data": [state.model_dump() for state in ag.states],
            "fields": list(ag.atype.model_fields.keys()),
        }
