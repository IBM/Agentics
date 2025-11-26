import os
import sys
import traceback
from pathlib import Path
from typing import Dict, Any
from pydantic import BaseModel, Field

from agentics import AG
from agentics.api.applications.base import AgenticsApp
from agentics.api.models import AppMetadata, UIOption
from agentics.api.config import settings

# --- Setup Paths ---
BENCHMARKS_DIR = Path(settings.SQL_BENCHMARKS_FOLDER)
DB_DIR = Path(settings.SQL_DB_PATH)

# Set env vars for the legacy logic modules
os.environ["SQL_BENCHMARKS_FOLDER"] = str(BENCHMARKS_DIR) + "/"
os.environ["SQL_DB_PATH"] = str(DB_DIR)

# Ensure we can import local modules
CURRENT_DIR = Path(__file__).parent
sys.path.append(str(CURRENT_DIR))

from agentics.api.applications.text2sql.text2sql_logic import (
    Text2sqlQuestion,
    execute_questions,
)
from agentics.api.applications.text2sql.utils import get_schema_from_file


class Text2SqlInput(BaseModel):
    question: str = Field(
        ...,
        json_schema_extra={
            "ui:widget": "textarea",
            "ui:placeholder": "Ask a question about your data...",
        },
    )
    benchmark_id: str = Field(
        ..., json_schema_extra={"ui:widget": "select", "ui:label": "Benchmark Dataset"}
    )
    db_id: str = Field(
        ..., json_schema_extra={"ui:widget": "select", "ui:label": "Database"}
    )
    use_enrichment: bool = Field(False, json_schema_extra={"ui:widget": "switch"})
    use_validation: bool = Field(False, json_schema_extra={"ui:widget": "switch"})


class Text2SqlApp(AgenticsApp):
    metadata = AppMetadata(
        id="text2sql",
        name="Text to SQL",
        description="Convert natural language to SQL queries.",
        icon="💾",
        usage_guide="""
### Text-to-SQL Assistant
This agent translates natural language questions into SQL queries using the BIRD/Spider benchmarks.

1. **Select a Benchmark**: Choose the dataset context (e.g., `bird` or `retail_bench`).
2. **Select a Database**: Choose the specific database file (e.g., `retail_db`).
3. **Ask a Question**: Type your question naturally.
   - Example: *"List all employees who joined after 2020"*
   - Example: *"What is the total revenue?"*
        """,
    )

    def get_input_schema(self) -> Dict[str, Any]:
        return Text2SqlInput.model_json_schema()

    def get_options(self) -> Dict[str, UIOption]:
        """
        Returns structured options.
        'db_id' is dependent on 'benchmark_id'.
        """
        options = {}
        benchmarks = []
        db_mapping = {}

        if BENCHMARKS_DIR.exists():
            files = [
                f.replace(".json", "")
                for f in os.listdir(BENCHMARKS_DIR)
                if f.endswith(".json") and not f.endswith("-schema.json")
            ]
            benchmarks = files

            for b_id in files:
                try:
                    schemas = get_schema_from_file(b_id)
                    if schemas:
                        db_mapping[b_id] = list(schemas.keys())
                except Exception:
                    db_mapping[b_id] = []

        options["benchmark_id"] = UIOption(type="static", values=benchmarks)
        options["db_id"] = UIOption(
            type="dependent", depends_on="benchmark_id", mapping=db_mapping
        )

        return options

    async def execute(
        self, session_id: str, input_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        data = Text2SqlInput(**input_data)

        # 1. Construct State
        # Ensure we are passing strict kwargs to the model constructor
        question_state = Text2sqlQuestion(
            question=data.question, db_id=data.db_id, benchmark_id=data.benchmark_id
        )

        ag = AG(atype=Text2sqlQuestion, states=[question_state])

        try:
            # 2. Execute Logic
            result_ag, _ = await execute_questions(
                ag,
                answer_validation=data.use_validation,
                enrichments=data.use_enrichment,
                multiple_runs=1,
            )

            # Check if we got states back
            if not result_ag.states:
                return {"error": "No result returned from agent."}

            result = result_ag.states[0]

            # Handle case where result might be a tuple due to library bug
            if isinstance(result, tuple):
                return {"error": "Agent returned malformed state (tuple)."}

            return {
                "sql": getattr(
                    result, "generated_query", "SELECT 'Error: No Query Generated'"
                ),
                "result_json": getattr(result, "system_output_df", []),
                "ground_truth_sql": getattr(result, "sql", ""),
                "ground_truth_json": getattr(result, "gt_output_df", []),
            }

        except AttributeError as e:
            traceback.print_exc()
            return {
                "error": "Agent logic error (AttributeError).",
                "details": str(e),
                "suggestion": "Check if benchmark data files match the schema expected by Agentics.",
            }
        except Exception as e:
            traceback.print_exc()
            return {"error": str(e)}
