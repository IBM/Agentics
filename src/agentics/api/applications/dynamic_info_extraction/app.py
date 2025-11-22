from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
from agentics import AG
from agentics.api.applications.base import AgenticsApp
from agentics.api.models import AppMetadata
from agentics.api.services.session_manager import session_manager
from agentics.core.atype import import_pydantic_from_code


class DynamicInput(BaseModel):
    filename: str
    analysis_question: str
    predefined_model_name: Optional[str] = None
    # We can add custom_schema dictionary support later
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

    async def perform_action(self, session_id: str, action: str, payload: dict) -> Any:
        """
        Action: 'draft_schema'
        Input: { "description": "Extract person names and ages" }
        Output: { "schema": {...}, "code": "class..." }
        """
        if action == "draft_schema":
            description = payload.get("description")
            if not description:
                raise ValueError("Description required")

            gen_ag = AG()
            # AG.generate_atype returns (ag_instance) - we await it
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
        session = session_manager.get_session(session_id)

        # 1. Load File
        if data.filename not in session.files:
            raise ValueError(f"File {data.filename} not found in session")

        file_path = session.files[data.filename]
        if file_path.endswith(".csv"):
            dataset = AG.from_csv(file_path)
        else:
            dataset = AG.from_jsonl(file_path)

        # 2. Resolve Type
        # Simplification: If no model provided, generate from question
        atype = None
        if data.predefined_model_name:
            # In a real scenario, reuse the loader from macro_app or a shared util
            pass

        if not atype:
            # Generate on the fly
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

        # Note: AG needs an LLM configured.
        # The existing AG code pulls from env vars (get_llm_provider).
        sentiment_ag = await (processing_ag << filtered_ag)

        return {
            "results": [s.model_dump() for s in sentiment_ag.states],
            "atype_schema": atype.model_json_schema(),
        }
