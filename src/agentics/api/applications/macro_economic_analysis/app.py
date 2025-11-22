import os
import sys
import pandas as pd
from typing import Dict, Any
from pydantic import BaseModel, Field
from pathlib import Path

# Add current dir to sys path to allow dynamic imports if needed
sys.path.append(str(Path(__file__).parent))

from agentics import AG
from agentics.api.applications.base import AgenticsApp
from agentics.api.models import AppMetadata
from agentics.core.atype import import_pydantic_from_code


# Helper to load types
def load_predefined_type(name: str):
    path = Path(__file__).parent / "predefined_types" / f"{name}.py"
    if not path.exists():
        raise ValueError(f"Type {name} not found")
    with open(path, "r") as f:
        code = f.read()
    return import_pydantic_from_code(code)


class MacroInput(BaseModel):
    start_date: str
    end_date: str
    target_model_name: str
    batch_size: int = Field(10, ge=2, le=50)


class MacroEconApp(AgenticsApp):
    metadata = AppMetadata(
        id="macro_econ",
        name="Macro Economic Analysis",
        description="Analyze market trends using structured map-reduce.",
    )

    def __init__(self):
        # Load dataset once
        self.data_path = Path(__file__).parent / "data" / "market_factors_new.csv"
        if not self.data_path.exists():
            raise FileNotFoundError("Dataset not found")

        self.df = pd.read_csv(self.data_path)
        self.date_index = {str(row["Date"]): i for i, row in self.df.iterrows()}

        # Load types options
        self.types_path = Path(__file__).parent / "predefined_types"
        self.available_types = [
            f.stem for f in self.types_path.glob("*.py") if not f.name.startswith("__")
        ]

    def get_input_schema(self) -> Dict[str, Any]:
        return MacroInput.model_json_schema()

    def get_options(self) -> Dict[str, Any]:
        return {
            "dates": sorted(list(self.date_index.keys())),
            "target_models": self.available_types,
        }

    async def execute(
        self, session_id: str, input_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        # Validate input
        data = MacroInput(**input_data)

        # 1. Load Dataset AG
        ag_dataset = AG.from_dataframe(self.df)

        # 2. Resolve Type
        atype = load_predefined_type(data.target_model_name)
        if not atype:
            raise ValueError("Could not load target type")

        # 3. Filter Logic
        start_idx = self.date_index.get(data.start_date, 0)
        end_idx = self.date_index.get(data.end_date, len(self.df))

        filtered_ag = ag_dataset.filter_states(start=start_idx, end=end_idx + 1)

        # 4. Transduce (Reduce)
        reducer_ag = AG(
            atype=atype, transduction_type="areduce", areduce_batch_size=data.batch_size
        )

        result = await (reducer_ag << filtered_ag)

        return {
            "summary": result.states[0].model_dump() if result.states else None,
            "batches": [b.model_dump() for b in result.areduce_batches],
        }
