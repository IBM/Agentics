import os
from pathlib import Path
from typing import Any, Dict, Type, List
from pydantic import BaseModel, Field

from agentics import AG
from agentics.api.core.interface import AgenticApp
from agentics.api.utils import load_pydantic_model_from_path


# Input/Output Models
class DynamicInformationExtractionInput(BaseModel):
    model_name: str = Field(..., description="The predefined type to extract")
    batch_size: int = Field(10, ge=1, le=50)


class DynamicInformationExtractionResult(BaseModel):
    results: List[Dict[str, Any]]


class DynamicInformationExtractionApp(AgenticApp):
    name = "Dynamic Information Extraction"
    slug = "dynamic-information-extraction"
    description = "Extract structured data using predefined Pydantic models."

    # Relative path to the types folder *inside* this app's directory
    BASE_DIR = Path(__file__).parent
    TYPES_DIR = BASE_DIR / "predefined_types"

    def get_input_model(self) -> Type[BaseModel]:
        return DynamicInformationExtractionInput

    def get_output_model(self) -> Type[BaseModel]:
        return DynamicInformationExtractionResult

    async def get_options(self) -> Dict[str, Any]:
        models = []
        if self.TYPES_DIR.exists():
            models = [
                f.stem
                for f in self.TYPES_DIR.glob("*.py")
                if not f.name.startswith("__")
            ]
        return {"model_name": sorted(models)}

    async def run(
        self,
        input_data: DynamicInformationExtractionInput,
        files: Dict[str, Any] = None,
    ) -> Any:
        if not files:
            raise ValueError("File required")

        # 1. Load Target Model
        model_path = self.TYPES_DIR / f"{input_data.model_name}.py"
        TargetModel = load_pydantic_model_from_path(model_path)
        if not TargetModel:
            raise ValueError(f"Model {input_data.model_name} not found")

        # 2. Handle File
        filename, file_obj = next(iter(files.items()))
        temp_path = f"/tmp/{filename}"
        with open(temp_path, "wb") as f:
            f.write(file_obj.read())

        try:
            # 3. Init AG
            if filename.endswith(".csv"):
                ag = AG.from_csv(temp_path)
            else:
                ag = AG.from_jsonl(temp_path)

            # 4. Transduce
            extraction_agent = AG(
                atype=TargetModel,
                transduction_type="areduce",
                areduce_batch_size=input_data.batch_size,
            )

            result = await (extraction_agent << ag)
            return {"results": [s.model_dump() for s in result.states]}

        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
