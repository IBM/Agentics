import os
from pathlib import Path
from typing import Any, Dict, Type, List
from pydantic import BaseModel, Field

from agentics import AG
from agentics.api.core.interface import AgenticApp

from .text2sql import Text2sqlQuestion, execute_questions

class Text2SQLInput(BaseModel):
    question: str = Field(..., description="Natural language question to ask the database.")
    db_id: str = Field(..., description="The target database identifier.")
    benchmark_id: str = Field("dev", description="Dataset split/benchmark ID (folder name in SQL_DB_PATH).")

class Text2SQLResult(BaseModel):
    generated_query: str
    sql_result: List[Dict[str, Any]] | str
    gt_query: str | None = None

class Text2SQLApp(AgenticApp):
    name = "Text to SQL"
    slug = "text2sql"
    description = "Convert natural language questions into SQL and execute them against a database."

    def get_input_model(self) -> Type[BaseModel]:
        return Text2SQLInput

    def get_output_model(self) -> Type[BaseModel]:
        return Text2SQLResult

    async def get_options(self) -> Dict[str, Any]:
        """
        Scans the SQL_DB_PATH to find available databases.
        """
        db_path = os.getenv("SQL_DB_PATH")
        options = {"db_id": [], "benchmark_id": ["dev", "train"]} # Defaults

        if db_path and os.path.exists(db_path):
            # Logic depends on folder structure of BIRD dataset.
            # Usually SQL_DB_PATH/{benchmark_id}/{db_id}/{db_id}.sqlite
            # We will try to list top-level folders as databases if structure varies.
            try:
                # Simple scan: list directories in root of SQL_DB_PATH
                dbs = [d.name for d in Path(db_path).iterdir() if d.is_dir()]
                options["db_id"] = sorted(dbs)
            except Exception:
                pass

        return options

    async def run(self, input_data: Text2SQLInput, files: Dict[str, Any] = None) -> Any:
        # 1. Construct the State object
        question_state = Text2sqlQuestion(
            question=input_data.question,
            db_id=input_data.db_id,
            benchmark_id=input_data.benchmark_id
        )

        # 2. Wrap in AG
        ag_obj = AG(atype=Text2sqlQuestion, states=[question_state])

        # 3. Execute
        # execute_questions returns (AG, accuracy_float)
        result_ag, _ = await execute_questions(
            ag_obj,
            answer_validation=False, # Keep fast for API
            enrichments=False        # Skip slow enrichment for single-shot API
        )

        result_state = result_ag[0]

        # 4. Parse Result
        # system_output_df is a JSON string of the dataframe or an error string
        import json
        output_data = result_state.system_output_df
        try:
            if output_data and not output_data.startswith("Error"):
                output_data = json.loads(output_data)
        except:
            pass # Keep as string if parse fails

        return {
            "generated_query": result_state.generated_query,
            "sql_result": output_data,
            "gt_query": result_state.query or result_state.sql
        }