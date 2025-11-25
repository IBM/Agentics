import sys
from pathlib import Path
from typing import Dict, Any, Optional, Type
from pydantic import BaseModel, create_model, Field
import importlib.util
from agentics import AG
from agentics.api.applications.base import AgenticsApp
from agentics.api.models import AppMetadata, ActionMetadata, UIOption
from agentics.api.services.session_manager import session_manager


class DynamicInput(BaseModel):
    filename: str = Field(..., json_schema_extra={"ui:widget": "file_select"})
    analysis_question: str = Field(
        ...,
        json_schema_extra={
            "ui:widget": "textarea",
            "ui:placeholder": "What would you like to extract?",
        },
    )
    # UPDATED: Changed to Select widget
    predefined_model_name: Optional[str] = Field(
        None,
        json_schema_extra={
            "ui:widget": "select",
            "ui:label": "Predefined Model (Optional)",
        },
    )

    custom_model_schema: Optional[Dict[str, Any]] = Field(
        None,
        json_schema_extra={"ui:widget": "json_editor", "ui:label": "Extraction Schema"},
    )

    start_index: int = Field(0, json_schema_extra={"ui:widget": "number"})
    end_index: Optional[int] = Field(None, json_schema_extra={"ui:widget": "number"})
    batch_size: int = Field(10, json_schema_extra={"ui:widget": "number"})


class DynamicExtractionApp(AgenticsApp):
    metadata = AppMetadata(
        id="dynamic_extraction",
        name="Dynamic Extraction",
        description="Extract insights from files using generated types.",
        icon="🔍",
        usage_guide="""
### Dynamic Extraction
Extract structured data from unstructured text files (CSV logs, JSONL).

1. **Upload a File**: Use the sidebar to upload a `.csv` or `.jsonl` file.
2. **Define Schema**:
   - Option A: Type a question and click the **Wand Icon** to auto-draft a schema.
   - Option B: Select a **Predefined Model** (e.g. `sentiment_analysis`).
   - Option C: Manually edit the JSON schema.
3. **Execute**: The agent will process the file row-by-row based on your schema.
        """,
        actions=[
            ActionMetadata(
                name="draft_schema",
                label="Auto-Draft Schema",
                icon="✨",
                input_source_field="analysis_question",
                output_target_field="custom_model_schema",
            )
        ],
    )

    def __init__(self):
        # Scan for predefined types (reusing logic from MacroEcon)
        # Assuming they live in a shared 'predefined_types' folder or local to this app
        # Adjust path as necessary for your project structure
        self.types_path = Path(__file__).parent / "predefined_types"
        self.available_types = []
        if self.types_path.exists():
            self.available_types = [
                f.stem
                for f in self.types_path.glob("*.py")
                if not f.name.startswith("__")
            ]

    def get_input_schema(self) -> Dict[str, Any]:
        return DynamicInput.model_json_schema()

    def get_options(self) -> Dict[str, UIOption]:
        # UPDATED: Return options for the dropdown
        return {
            "predefined_model_name": UIOption(
                type="static", values=self.available_types
            )
        }

    def _json_schema_to_pydantic(self, schema: Dict[str, Any]) -> type[BaseModel]:
        model_name = schema.get("title", "CustomModel")
        properties = schema.get("properties", {})
        field_definitions = {}
        for field_name, props in properties.items():
            dtype = str
            if "type" in props:
                t = props["type"]
                if t == "integer":
                    dtype = int
                elif t == "number":
                    dtype = float
                elif t == "boolean":
                    dtype = bool
            field_definitions[field_name] = (Optional[dtype], None)
        return create_model(model_name, **field_definitions)

    async def perform_action(self, session_id: str, action: str, payload: dict) -> Any:
        if action == "draft_schema":
            description = payload.get("analysis_question") or payload.get("description")
            if not description:
                raise ValueError("Analysis question is required to draft schema")

            gen_ag = AG()
            await gen_ag.generate_atype(description)

            if gen_ag.atype:
                return gen_ag.atype.model_json_schema()
            return {"error": "Failed to generate type"}

        raise NotImplementedError

    def _load_local_type(self, type_name: str) -> Type[BaseModel]:
        """
        Dynamically load a Pydantic model from the local 'predefined_types' folder.
        """
        file_path = self.types_path / f"{type_name}.py"

        if not file_path.exists():
            raise ValueError(f"Type definition file not found: {file_path}")

        # Dynamic import from file path
        spec = importlib.util.spec_from_file_location(type_name, file_path)
        if not spec or not spec.loader:
            raise ImportError(f"Could not load spec for {type_name}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Inspect module to find the Pydantic model
        # We assume the model class name matches the filename OR is the only Pydantic model
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, BaseModel)
                and attr is not BaseModel
            ):
                # Found it!
                return attr

        raise ValueError(f"No Pydantic model found in {file_path}")

    async def execute(
        self, session_id: str, input_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        data = DynamicInput(**input_data)
        file_path = session_manager.get_file_path(session_id, data.filename)

        if str(file_path).endswith(".csv"):
            dataset = AG.from_csv(file_path)
        else:
            dataset = AG.from_jsonl(file_path)

        atype = None
        if data.custom_model_schema:
            atype = self._json_schema_to_pydantic(data.custom_model_schema)
        elif data.predefined_model_name:
            try:
                atype = self._load_local_type(data.predefined_model_name)
            except Exception as e:
                # Fallback for debugging
                print(f"Local load failed: {e}")
                raise ValueError(
                    f"Could not load target type: {data.predefined_model_name}"
                )

        if not atype:
            temp_ag = AG()
            await temp_ag.generate_atype(data.analysis_question)
            atype = temp_ag.atype

        end = data.end_index if data.end_index is not None else len(dataset)
        filtered_ag = dataset.filter_states(start=data.start_index, end=end)
        processing_ag = AG(
            atype=atype, transduction_type="areduce", areduce_batch_size=data.batch_size
        )

        if data.analysis_question:
            processing_ag.instructions = data.analysis_question

        sentiment_ag = await (processing_ag << filtered_ag)

        return {
            "results": [s.model_dump() for s in sentiment_ag.states],
            "atype_schema": atype.model_json_schema() if atype else {},
        }
