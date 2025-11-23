import os
import sys
from pathlib import Path
from typing import Dict, Any
from pydantic import BaseModel

from agentics import AG
from agentics.api.applications.base import AgenticsApp
from agentics.api.models import AppMetadata
from agentics.api.config import settings

# --- Setup Paths ---
# Use paths from Settings (which can be overridden by Env Vars)
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
    question: str
    benchmark_id: str
    db_id: str
    use_enrichment: bool = False
    use_validation: bool = False


class Text2SqlApp(AgenticsApp):
    metadata = AppMetadata(
        id="text2sql",
        name="Text to SQL",
        description="Convert natural language to SQL queries.",
        icon="💾",
    )

    def get_input_schema(self) -> Dict[str, Any]:
        return Text2SqlInput.model_json_schema()

    def get_options(self) -> Dict[str, Any]:
        """Return available benchmarks and their associated DBs from local data folder."""
        options = {"benchmarks": [], "dbs": {}}
        print(BENCHMARKS_DIR)

        if BENCHMARKS_DIR.exists():
            # Filter for .json files that aren't schemas
            files = [
                f.replace(".json", "")
                for f in os.listdir(BENCHMARKS_DIR)
                if f.endswith(".json") and not f.endswith("-schema.json")
            ]
            options["benchmarks"] = files

            for b_id in files:
                try:
                    # Pass benchmark_id, relies on the env var we set above
                    schemas = get_schema_from_file(b_id)
                    if schemas:
                        options["dbs"][b_id] = list(schemas.keys())
                except Exception:
                    options["dbs"][b_id] = []

        return options

    async def execute(
        self, session_id: str, input_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        data = Text2SqlInput(**input_data)

        # 1. Construct State
        question_state = Text2sqlQuestion(
            question=data.question, db_id=data.db_id, benchmark_id=data.benchmark_id
        )

        ag = AG(atype=Text2sqlQuestion, states=[question_state])

        # 2. Execute Logic
        # The logic files will read os.getenv("SQL_DB_PATH") which we set globally
        # for this process.
        result_ag, _ = await execute_questions(
            ag,
            answer_validation=data.use_validation,
            enrichments=data.use_enrichment,
            multiple_runs=1,
        )

        result = result_ag.states[0]

        return {
            "sql": result.generated_query,
            "result_json": result.system_output_df,
            "ground_truth_sql": result.sql,
            "ground_truth_json": result.gt_output_df,
        }
