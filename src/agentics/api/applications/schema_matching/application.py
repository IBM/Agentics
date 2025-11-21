import os
from pathlib import Path
from typing import Any, Dict, Type, List, Literal
from pydantic import BaseModel, Field

from agentics import AG
from agentics.api.core.interface import AgenticApp

from .schema_matching import Attribute, Attributes, crew_prompt_params, prompt_tempate_1_n, custom_instruction
from .utils import load_states

class SchemaMatchingInput(BaseModel):
    matching_type: Literal["1to1", "1toN"] = "1to1"
    # In a real app, we might ask which file is source vs target via keys,
    # but for simplicity we'll assume file1=Source, file2=Target by name or order.

class SchemaMatchingResult(BaseModel):
    mappings: List[Dict[str, Any]]

class SchemaMatchingApp(AgenticApp):
    name = "Schema Matching"
    slug = "schema-matching"
    description = "Match columns between two schemas (Source and Target)."

    def get_input_model(self) -> Type[BaseModel]:
        return SchemaMatchingInput

    def get_output_model(self) -> Type[BaseModel]:
        return SchemaMatchingResult

    async def run(self, input_data: SchemaMatchingInput, files: Dict[str, Any] = None) -> Any:
        if not files or len(files) < 2:
            raise ValueError("Two files (Source and Target) are required.")

        # We arbitrarily assign first file as Source, second as Target based on keys
        filenames = list(files.keys())
        source_fname, target_fname = filenames[0], filenames[1]

        # Save temps
        # NOTE: The original utility `load_states` expects file PATHS.
        source_path = f"/tmp/{source_fname}"
        target_path = f"/tmp/{target_fname}"

        with open(source_path, "wb") as f:
            f.write(files[source_fname].read())
        with open(target_path, "wb") as f:
            f.write(files[target_fname].read())

        try:
            # 1. Load Source
            # The original logic used specific filter lambdas for the MIMIC dataset.
            # We will generalize: Load ALL attributes from the provided JSONL.
            # Assuming the user uploads prepared schema JSONL files.

            mimic_states = load_states(
                source_path,
                Attribute,
                is_target=False,
                single_state=False,
                filter_fn=lambda x: True # Load all
            )
            mimic_data = AG.from_states(mimic_states, Attribute)

            # 2. Load Target
            if input_data.matching_type == "1toN":
                omop_states = load_states(
                    target_path, Attributes, is_target=True, single_state=True, filter_fn=lambda x: True
                )
            else:
                # 1to1
                omop_states = load_states(
                    target_path, Attributes, is_target=True, single_state=False, filter_fn=lambda x: True
                )

            omop_data = AG.from_states(omop_states, Attributes)

            # 3. Product
            # Calculate Cartesian product of source * target
            mimic_omop = mimic_data.product(omop_data)

            # 4. Setup Transduction
            mimic_omop.crew_prompt_params = crew_prompt_params
            mimic_omop.prompt_template = prompt_tempate_1_n

            num_target_schema = len(omop_states[0].attributes) if omop_states else 0

            mimic_omop = mimic_omop.add_attribute(
                slot_name="invertible",
                slot_type=list[bool],
                default_value=[False for _ in range(num_target_schema)],
                description=f"list of true, false assignment... length {num_target_schema}",
            )
            mimic_omop = mimic_omop.add_attribute(
                slot_name="num_target_schema",
                slot_type=int,
                default_value=num_target_schema,
                description="the number of target schema",
            )

            # 5. Execute
            input_fields = mimic_omop.fields
            result = await mimic_omop.self_transduction(
                input_fields + ["num_target_schema"],
                ["invertible"],
                instructions=custom_instruction,
            )

            return {"mappings": [s.model_dump() for s in result.states]}

        finally:
            if os.path.exists(source_path): os.remove(source_path)
            if os.path.exists(target_path): os.remove(target_path)