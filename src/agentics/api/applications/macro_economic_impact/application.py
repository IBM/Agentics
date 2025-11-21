import os
from pathlib import Path
from datetime import date
from typing import Any, Dict, Type, List
from pydantic import BaseModel, Field

from agentics import AG
from agentics.api.core.interface import AgenticApp
from agentics.api.utils import load_pydantic_model_from_path


class MacroInput(BaseModel):
    target_model: str
    start_date: date
    end_date: date
    batch_size: int = 10


class MacroEconomicImpactApp(AgenticApp):
    name = "Macro Economic Impact Analysis"
    slug = "macro-economic-impact"
    description = "Analyze market factors using time-series data."

    BASE_DIR = Path(__file__).parent
    TYPES_DIR = BASE_DIR / "predefined_types"
    DATA_PATH = BASE_DIR / "data" / "market_factors_new.csv"

    def get_input_model(self) -> Type[BaseModel]:
        return MacroInput

    def get_output_model(self) -> Type[BaseModel]:
        class Output(BaseModel):
            results: List[Dict[str, Any]]

        return Output

    async def get_options(self) -> Dict[str, Any]:
        # In a real scenario, read min/max date from self.DATA_PATH
        models = [
            f.stem for f in self.TYPES_DIR.glob("*.py") if not f.name.startswith("__")
        ]
        return {
            "target_model": sorted(models),
            "start_date": {"min": "2008-01-01", "max": "2025-12-31"},
            "end_date": {"min": "2008-01-01", "max": "2025-12-31"},
        }

    async def run(self, input_data: MacroInput, files: Dict[str, Any] = None) -> Any:
        # Use bundled data
        if not self.DATA_PATH.exists():
            raise FileNotFoundError(f"Data file not found at {self.DATA_PATH}")

        ag_data = AG.from_csv(self.DATA_PATH)

        # Filter States (Naive string comparison matching the demo)
        s_date, e_date = str(input_data.start_date), str(input_data.end_date)
        filtered = AG(atype=ag_data.atype)
        filtered.states = [
            s for s in ag_data.states if s_date <= str(getattr(s, "Date", "")) <= e_date
        ]

        # Load Model
        TargetModel = load_pydantic_model_from_path(
            self.TYPES_DIR / f"{input_data.target_model}.py"
        )

        # Execute
        agent = AG(
            atype=TargetModel,
            transduction_type="areduce",
            areduce_batch_size=input_data.batch_size,
        )
        result = await (agent << filtered)

        return {"results": [s.model_dump() for s in result.states]}
