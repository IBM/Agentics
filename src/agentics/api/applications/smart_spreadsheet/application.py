import os
from pathlib import Path
from typing import Any, Dict, Type, List, Optional
from pydantic import BaseModel, Field

from agentics import AG
from agentics.api.core.interface import AgenticApp


class SpreadsheetInput(BaseModel):
    source_fields: List[str] = Field(..., description="Fields to read from")
    target_fields: List[str] = Field(..., description="Fields to generate/fill")
    instructions: Optional[str] = Field(None, description="Transformation instructions")
    max_rows: int = 10


class SpreadsheetResult(BaseModel):
    results: List[Dict[str, Any]]


class SmartSpreadsheetApp(AgenticApp):
    name = "Smart Spreadsheet"
    slug = "smart-spreadsheet"
    description = "Upload a CSV, select source/target columns, and transform data using LLM instructions."

    def get_input_model(self) -> Type[BaseModel]:
        return SpreadsheetInput

    def get_output_model(self) -> Type[BaseModel]:
        return SpreadsheetResult

    async def run(
        self, input_data: SpreadsheetInput, files: Dict[str, Any] = None
    ) -> Any:
        if not files:
            raise ValueError("CSV file required.")

        filename, file_obj = next(iter(files.items()))
        temp_path = f"/tmp/{filename}"

        try:
            with open(temp_path, "wb") as f:
                f.write(file_obj.read())

            # 1. Load AG from CSV
            # Note: In a real app, we might need to infer the AType dynamically first
            # so the user can select fields.
            # Here, we assume the user knows the column headers or the UI parses the CSV client-side first.
            ag = AG.from_csv(temp_path, max_rows=input_data.max_rows)

            # 2. Setup Instructions
            # The 'self_transduction' method uses internal instructions or we pass them?
            # The UI example code: ag.self_transduction(source_fields, target_fields)
            # If instructions are passed, we can attach them to the agent.
            if input_data.instructions:
                # Inject instructions via the prompt_template or instructions attribute if available
                # For now, we assume the standard self_transduction utilizes prompts derived from field descriptions
                pass

            # 3. Execute Self Transduction
            # This fills 'target_fields' based on 'source_fields'
            result_ag = await ag.self_transduction(
                input_data.source_fields,
                input_data.target_fields,
                instructions=input_data.instructions or "Transform data.",
            )

            # 4. Return
            # Convert to dict for JSON response
            return {"results": [s.model_dump() for s in result_ag.states]}

        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
