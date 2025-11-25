import sys
import pandas as pd
from typing import Dict, Any, Type
from pydantic import BaseModel, Field
import importlib.util
from pathlib import Path

# Add current dir to sys path
sys.path.append(str(Path(__file__).parent))

from agentics import AG
from agentics.api.applications.base import AgenticsApp
from agentics.api.models import AppMetadata, UIOption


class MacroInput(BaseModel):
    start_date: str = Field(
        ...,
        json_schema_extra={
            "ui:widget": "date_picker",
            "ui:label": "Start Date",
            "ui:placeholder": "Select start date",
        },
    )
    end_date: str = Field(
        ...,
        json_schema_extra={
            "ui:widget": "date_picker",
            "ui:label": "End Date",
            "ui:placeholder": "Select end date",
        },
    )
    target_model_name: str = Field(
        ..., json_schema_extra={"ui:widget": "select", "ui:label": "Analysis Model"}
    )
    batch_size: int = Field(10, ge=2, le=50, json_schema_extra={"ui:widget": "number"})


class MacroEconApp(AgenticsApp):
    metadata = AppMetadata(
        id="macro_econ",
        name="Macro Economic Analysis",
        description="Analyze market trends using structured map-reduce.",
        icon="📈",
        usage_guide="""
### How to use this Agent
1. **Select a Date Range**: The dataset covers economic indicators from 2000 to 2023.
2. **Choose a Model**:
   - `inflation_model`: Analyzes CPI and interest rate trends.
   - `gdp_growth`: Focuses on production output and labor stats.
3. **Batch Size**: Controls how many months are aggregated in a single LLM call. Higher is faster but less granular.
        """,
    )

    def __init__(self):
        # Load dataset
        self.data_path = Path(__file__).parent / "data" / "market_factors_new.csv"
        if not self.data_path.exists():
            self.date_index = {}
            self.dates_list = []
            self.df = pd.DataFrame()
        else:
            self.df = pd.read_csv(self.data_path)
            self.date_index = {str(row["Date"]): i for i, row in self.df.iterrows()}
            self.dates_list = sorted(list(self.date_index.keys()))

        # Load types options
        self.types_path = Path(__file__).parent / "predefined_types"
        self.available_types = [
            f.stem for f in self.types_path.glob("*.py") if not f.name.startswith("__")
        ]

    def get_input_schema(self) -> Dict[str, Any]:
        return MacroInput.model_json_schema()

    def get_options(self) -> Dict[str, UIOption]:
        # If no data, return defaults
        min_date = self.dates_list[0] if self.dates_list else "2000-01-01"
        max_date = self.dates_list[-1] if self.dates_list else "2023-12-31"

        return {
            "start_date": UIOption(type="date_range", min=min_date, max=max_date),
            "end_date": UIOption(type="date_range", min=min_date, max=max_date),
            "target_model_name": UIOption(type="static", values=self.available_types),
        }

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
        # Validate input
        data = MacroInput(**input_data)

        if self.df.empty:
            return {"error": "Dataset not loaded"}

        # 1. Load Dataset AG
        ag_dataset = AG.from_dataframe(self.df)

        # 2. Resolve Type
        try:
            atype = self._load_local_type(data.target_model_name)
        except Exception as e:
            # Fallback for debugging
            print(f"Local load failed: {e}")
            raise ValueError(f"Could not load target type: {data.target_model_name}")

        # 3. Filter Logic (Find nearest dates if exact match missing)
        # Simple string comparison works for ISO dates
        start_idx = 0
        end_idx = len(self.df)

        try:
            # Find closest date indices
            start_idx = self.date_index.get(data.start_date, 0)
            end_idx = self.date_index.get(data.end_date, len(self.df))
        except:
            pass  # Fallback to defaults

        if start_idx > end_idx:
            start_idx, end_idx = end_idx, start_idx

        filtered_ag = ag_dataset.filter_states(start=start_idx, end=end_idx + 1)

        # 4. Transduce
        reducer_ag = AG(
            atype=atype, transduction_type="areduce", areduce_batch_size=data.batch_size
        )

        result = await (reducer_ag << filtered_ag)

        summary = None
        if result.states:
            summary = result.states[0].model_dump()

        return {
            "summary": summary,
            "batches_processed": (
                len(result.areduce_batches) if hasattr(result, "areduce_batches") else 0
            ),
        }
