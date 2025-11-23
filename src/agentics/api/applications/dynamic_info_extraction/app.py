from typing import Dict, Any, Optional
from pydantic import BaseModel, create_model
from agentics import AG
from agentics.api.applications.base import AgenticsApp
from agentics.api.models import AppMetadata
from agentics.api.services.session_manager import session_manager

from agentics.api.applications.utils import load_predefined_type


class DynamicInput(BaseModel):
    filename: str
    analysis_question: str
    predefined_model_name: Optional[str] = None
    custom_model_schema: Optional[Dict[str, Any]] = None
    start_index: int = 0
    end_index: Optional[int] = None
    batch_size: int = 10


class DynamicExtractionApp(AgenticsApp):
    metadata = AppMetadata(
        id="dynamic_extraction",
        name="Dynamic Extraction",
        description="Extract insights from files using generated types.",
        icon="🔍",
    )

    def get_input_schema(self) -> Dict[str, Any]:
        return DynamicInput.model_json_schema()

    def _json_schema_to_pydantic(self, schema: Dict[str, Any]) -> type[BaseModel]:
        """
        Reconstruct a Pydantic model from a simplified JSON Schema.
        Note: This is a basic implementation supporting top-level fields.
        """
        model_name = schema.get("title", "CustomModel")
        properties = schema.get("properties", {})

        field_definitions = {}
        for field_name, props in properties.items():
            # Determine type
            dtype = str
            if "type" in props:
                t = props["type"]
                if t == "integer":
                    dtype = int
                elif t == "number":
                    dtype = float
                elif t == "boolean":
                    dtype = bool

            # Allow optional by default for flexibility
            field_definitions[field_name] = (Optional[dtype], None)

        return create_model(model_name, **field_definitions)

    async def perform_action(self, session_id: str, action: str, payload: dict) -> Any:
        if action == "draft_schema":
            description = payload.get("description")
            if not description:
                raise ValueError("Description required")

            gen_ag = AG()
            await gen_ag.generate_atype(description)

            if gen_ag.atype:
                return {
                    "schema": gen_ag.atype.model_json_schema(),
                    "code": gen_ag.atype_code,
                }
            return {"error": "Failed to generate type"}

        raise NotImplementedError

    async def execute(
        self, session_id: str, input_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        data = DynamicInput(**input_data)

        # 1. Load File
        file_path = session_manager.get_file_path(session_id, data.filename)

        if str(file_path).endswith(".csv"):
            dataset = AG.from_csv(file_path)
        else:
            dataset = AG.from_jsonl(file_path)

        # 2. Resolve Type
        atype = None

        # Priority 1: Custom Schema provided by frontend
        if data.custom_model_schema:
            atype = self._json_schema_to_pydantic(data.custom_model_schema)

        # Priority 2: Predefined Type
        elif data.predefined_model_name:
            # We assume the predefined types are in the macro_economic folder structure
            # or we need to copy them to a shared location.
            # For now, let's reuse the loader logic or try dynamic import.
            try:
                atype = load_predefined_type(data.predefined_model_name)
            except Exception:
                pass  # Fallback to generation

        # Priority 3: Generate from Question
        if not atype:
            temp_ag = AG()
            await temp_ag.generate_atype(data.analysis_question)
            atype = temp_ag.atype

        # 3. Filter
        end = data.end_index if data.end_index is not None else len(dataset)
        filtered_ag = dataset.filter_states(start=data.start_index, end=end)

        # 4. Transduce
        processing_ag = AG(
            atype=atype, transduction_type="areduce", areduce_batch_size=data.batch_size
        )

        if data.analysis_question:
            processing_ag.instructions = data.analysis_question

        sentiment_ag = await (processing_ag << filtered_ag)

        return {
            "results": [s.model_dump() for s in sentiment_ag.states],
            "atype_schema": atype.model_json_schema(),
        }
