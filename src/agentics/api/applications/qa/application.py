import os
import json
from pathlib import Path
from typing import Any, Dict, Type, List
from pydantic import BaseModel, Field

from agentics import AG
from agentics.api.core.interface import AgenticApp

from .table_bench import qa_baseline


class QAInput(BaseModel):
    # Optional instructions could go here
    pass


class QAResult(BaseModel):
    results: List[Dict[str, Any]]


class QAApp(AgenticApp):
    name = "Table QA"
    slug = "qa"
    description = (
        "Answer questions based on tabular data provided in CSV or JSONL files."
    )

    def get_input_model(self) -> Type[BaseModel]:
        return QAInput

    def get_output_model(self) -> Type[BaseModel]:
        return QAResult

    async def run(self, input_data: QAInput, files: Dict[str, Any] = None) -> Any:
        if not files:
            raise ValueError(
                "Please upload a file containing tables and questions (JSONL or CSV)."
            )

        filename, file_obj = next(iter(files.items()))
        temp_path = f"/tmp/{filename}"

        try:
            with open(temp_path, "wb") as f:
                f.write(file_obj.read())

            # 1. Load Data
            if filename.endswith(".csv"):
                ag = AG.from_csv(temp_path)
            else:
                ag = AG.from_jsonl(temp_path)

            # 2. Setup AG - FIX HERE
            # Use add_attribute and assign the result back to ag.
            # This forces the creation of a new AType that includes 'answer'.
            ag = ag.add_attribute("answer", default_value=None)

            # 3. Run Transduction (Map)
            result_ag = await ag.amap(qa_baseline)

            # 4. Return with defensive serialization
            serialized_results = []
            for s in result_ag.states:
                if hasattr(s, "model_dump"):
                    serialized_results.append(s.model_dump())
                elif isinstance(s, dict):
                    serialized_results.append(s)
                elif hasattr(s, "_asdict"):  # NamedTuple
                    serialized_results.append(s._asdict())
                else:
                    # Fallback: try to cast to dict or string representation
                    try:
                        serialized_results.append(dict(s))
                    except:
                        serialized_results.append({"raw": str(s)})

            return {"results": serialized_results}

        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
