import asyncio
import sqlite3
import time
from typing import Any, Dict, List, Optional, Union

import aiosqlite
from loguru import logger
from pydantic import BaseModel, Field
from utils import execute_sql_on_endpoint, map_type, quote_ident

from agentics import AG


class Target(BaseModel):
    """Represent the results of the execution of a text2sql task, where the target is the ground truth"""

    id: Optional[str] = None
    target: Optional[str] = None


from dotenv import load_dotenv

load_dotenv()
import json
import os
from abc import ABC, abstractmethod

import pandas as pd


class Column(BaseModel):
    table: Optional[str] = None
    column_name: Optional[str] = None
    type: Optional[str] = "str"
    sample_values: Optional[list[Any]] = []
    description: Optional[str] = Field(
        None,
        description="A generated one sentence description of the column given the context of the DB in which it is located",
    )


class Table(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    columns: Optional[Dict[str, Column]] = {}


class DBSchema(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    tables: Optional[Dict[str, Table]] = {}

    # def generate_ddl(self, dialect: str = "sqlite", include_drop: bool = False)->str:
    #     """
    #     Generate DDL for the given DBSchema.
    #     dialect: 'sqlite' | 'postgres' | 'mysql'
    #     """
    #     if self is None or not self.tables:
    #         return ""

    #     lines = []

    #     # Optional database comment/annotation
    #     if self.name:
    #         db_title = f"-- Schema: {self.name}"
    #         lines.append(db_title)
    #     if self.description:
    #         lines.append(f"-- {self.description}")

    #     for tname, table in (self.tables or {}).items():
    #         if table is None:
    #             continue
    #         table_name = table.name or tname
    #         q_table = quote_ident(table_name, dialect)

    #         if include_drop:
    #             if dialect == "sqlite":
    #                 lines.append(f'DROP TABLE IF EXISTS {q_table};')
    #             elif dialect == "postgres":
    #                 lines.append(f'DROP TABLE IF EXISTS {q_table} CASCADE;')
    #             elif dialect == "mysql":
    #                 lines.append(f'DROP TABLE IF EXISTS {q_table};')

    #         # Build column definitions
    #         cols = []
    #         col_items = (table.columns or {}).items()
    #         # Ensure deterministic order
    #         for cname, col in sorted(col_items, key=lambda kv: kv[0]):
    #             if col is None:
    #                 continue
    #             col_name = col.column_name or cname
    #             q_col = quote_ident(col_name, dialect)
    #             sql_type = map_type(col.type, dialect)

    #             if dialect == "mysql" and col.description:
    #                 cols.append(f"{q_col} {sql_type} COMMENT {repr(col.description)}")
    #             else:
    #                 cols.append(f"{q_col} {sql_type}")

    #         # Fallback if no columns
    #         if not cols:
    #             cols = ["-- (no columns specified)"]

    #         create_stmt = f"CREATE TABLE {q_table} (\n  " + ",\n  ".join(cols) + "\n);"
    #         lines.append(create_stmt)

    #         # Table/column comments where supported
    #         if table.description:
    #             if dialect == "postgres":
    #                 lines.append(f"COMMENT ON TABLE {q_table} IS {repr(table.description)};")
    #             elif dialect == "mysql":
    #                 # MySQL supports table comment in CREATE TABLE; add an ALTER as a fallback
    #                 lines.append(f"ALTER TABLE {q_table} COMMENT = {repr(table.description)};")
    #             else:
    #                 lines.append(f"-- {table_name}: {table.description}")

    #         if dialect == "postgres":
    #             for cname, col in sorted((table.columns or {}).items(), key=lambda kv: kv[0]):
    #                 if col and col.description:
    #                     q_col = quote_ident(col.column_name or cname, dialect)
    #                     lines.append(f"COMMENT ON COLUMN {q_table}.{q_col} IS {repr(col.description)};")
    #         elif dialect == "sqlite":
    #             # SQLite: use comments as inline '--' annotations
    #             for cname, col in sorted((table.columns or {}).items(), key=lambda kv: kv[0]):
    #                 if col and col.description:
    #                     lines.append(f"-- {table_name}.{col.column_name or cname}: {col.description}")
    #     return "\n".join(lines)
    def generate_ddl(self):
        return self.model_dump_json()


# class QueryExecution(BaseModel):
#     question: Optional[str] = ""
#     sql_query: Optional[str] = ""
#     output_dataframe: Optional[Union[List[Dict[str, Any]], Dict[str, Any]]] = None
#     error_message: Optional[str] = None
#     answer_quality_assessment: Optional[str] = Field(
#         None,
#         description="A verbal judgment on the quality of the output dataframe for the provided answer",
#     )
#     answer_quality_score: Optional[float] = Field(0)
#     db_name: Optional[str] = None

# class QuestionSQLPair(BaseModel):
#     question: Optional[str] = None
#     sql_query: Optional[str] = None


class DB(BaseModel, ABC):
    db_name: Optional[str] = None
    db_type: Optional[str] = "sqllite"
    database_schema_description: Optional[str] = Field(
        None,
        description="""A paragraph long description of the schema of the database""",
    )
    keywords: Optional[list[str]] = Field(
        None,
        description="""A list of keywords describing the content of the database.
Produce Keywords that are:
Domain-Relevant: Reflects the thematic area (e.g., education, healthcare, finance).
Purpose-Oriented: Indicates the type of insights the database supports (e.g., performance tracking, demographic analysis).
Unambiguous: Avoids generic or overly broad terms.
Interoperable: Aligns with standard taxonomies when possible (e.g., DataCite or UNSDG classification).
Examples of Strong Keywords: student_outcomes, climate_metrics, financial_forecasting, public_health_indicators, supply_chain_kpis""",
    )
    business_description: Optional[str] = Field(
        None,
        description="""A Description of the business purpose of the db, what use cases it is good for how what type of information it contain""",
    )
    db_schema: Optional[DBSchema] = DBSchema()

    datasource_id: Optional[str] = None
    enrichment_path: Optional[str] = None
    # sample_queries: Optional[list[QueryExecution]] = None
    # natural_language_questions: Optional[list[str]] = Field(
    #     [],
    #     description="""A list of natural language questions that could be answered from the provided DDL.""",
    # )
    # sql_queries: Optional[list[str]] = Field(
    #     [],
    #     description="SQL queries that can be used to answer the questions above. Introduce where clasuses based on the sample values in the",
    # )
    # few_shots: Optional[list[QuestionSQLPair]] = Field(
    #     [], description="A selection of the generated"
    # )
    ddl: Optional[Union[str, list[str]]] = None
    # tables: Optional[Dict[str, str]] = {}
    endpoint_id: Optional[str] = None
    db_path: Optional[str] = None
    benchmark_id: Optional[str] = None
    db_id: Optional[str] = None

    # @abstractmethod
    # async def async_execute_sql(self, sql_query:str) -> DataFrame:

    # def __setattr__(self, name, value):
    #     if name == "natural_language_questions" or name == "sql_queries" :
    #         existing = getattr(self, name, [])
    #         super().__setattr__(name, existing + (value or []))
    #     else:
    #         super().__setattr__(name, value)

    # @classmethod
    # def load_db(cls, db_type, db_name=None, selected_db_path=None, datasource_id=None):
    #     db = DB()

    async def async_execute_sql(self, sql_query: str) -> str:
        """DB id could be a path or a Endpoint connection string"""
        if self.endpoint_id:
            return await execute_sql_on_endpoint(sql_query, self.endpoint_id)

        elif self.db_path:
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    async with db.execute(sql_query) as cursor:
                        columns = [description[0] for description in cursor.description]
                        rows = await asyncio.wait_for(cursor.fetchall(), timeout=10)
                        df = pd.DataFrame(rows, columns=columns)
                        return df.to_json()
            except Exception as e:
                pass

            try:
                async with aiosqlite.connect(self.db_path) as db:
                    async with db.execute(sql_query.replace('"', "'")) as cursor:
                        columns = [description[0] for description in cursor.description]
                        rows = await asyncio.wait_for(cursor.fetchall(), timeout=10)
                        df = pd.DataFrame(rows, columns=columns)
                        return df.to_json()
            except Exception as e:
                return f"Error: {str(e)}"

    def get_schema_from_file(self):
        with open(
            os.path.join(
                os.getenv("SQL_BENCHMARKS_FOLDER"), self.benchmark_id + "-schema.json"
            )
        ) as f:
            all_dbs = json.load(f)

        return all_dbs.get(self.db_id) if self.db_id else all_dbs

    def get_schema_from_sqllite(self, add_sample_values: int = 5):
        if not self.db_path:
            self.db_path = os.path.join(
                os.getenv("SQL_DB_PATH"),
                self.benchmark_id,
                self.db_id,
                self.db_id + ".sqlite",
            )

        self.db_schema = DBSchema()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        schema_json = {}
        for table in tables:
            db_table = Table(name=table[0])

            table_name = table[0]

            # --- get schema info ---
            cursor.execute(f"PRAGMA table_info({table_name});")
            schema = cursor.fetchall()
            schema_json[table[0]] = {
                col[1]: {
                    "type": col[2],
                    "notnull": col[3],
                    "dflt_value": col[4],
                }
                for col in schema
            }

            for col in schema:
                db_table.columns[col[1]] = Column(
                    column_name=col[1], type=col[2], table=table[0]
                )

            columns = [col[1] for col in schema]  # column names
            if add_sample_values:
                # --- get sample data ---
                cursor.execute(f"SELECT * FROM {table_name} LIMIT {add_sample_values};")
                sample_data = cursor.fetchall()

                # attach samples per column
                for row in sample_data:
                    for col_name, value in zip(columns, row):
                        if "sample_values" not in schema_json[table_name][col_name]:
                            schema_json[table_name][col_name]["sample_values"] = []
                        schema_json[table_name][col_name]["sample_values"].append(value)
                        db_table.columns[col_name].sample_values.append(value)
            self.db_schema.tables[db_table.name] = db_table

        self.ddl = self.db_schema.generate_ddl()
        return self

    async def get_schema_enrichments(self):
        if self.db_schema:
            columns = AG(atype=Column)
            tables = AG(atype=Table)
            for table in self.db_schema.tables.keys():

                for column in self.db_schema.tables[table].columns.keys():
                    columns.states.append(self.db_schema.tables[table].columns[column])
                tables.states.append(self.db_schema.tables[table])

            tables = await tables.self_transduction(
                ["name", "columns"],
                ["description"],
                instructions=f"You are analyzing a this db: {self.db_name}Read the following ddl: {self.ddl} \n\nGenerate a one paragraph description for the input table",
            )

            for table in tables:
                self.db_schema.tables[table.name].description = table.description

            columns = await columns.self_transduction(
                ["table", "column_name", "type", "sample_values"],
                ["description"],
                instructions=f"You are analyzing a this db: {self.db_name}Read the following ddl: {self.ddl} \n\nGenerate a one sentence description for the input column",
            )
            for column in columns:
                self.db_schema.tables[column.table].columns[
                    column.column_name
                ].description = column.description
        return self

    async def generate_db_description(self):
        dbs = AG(atype=DB, states=[self])

        dbs = await dbs.self_transduction(
            ["db_name", "ddl"],
            ["database_schema_description", "business_description", "keywords"],
            instructions=f"""Generate the required description of the DB from the input ddl.""",
        )

        self = dbs[0]
        return self

    async def load_db(self, db_path: str = None, enrichments=False):
        if db_path:
            self.db_path = db_path
        self = self.get_schema_from_sqllite()
        if enrichments:
            if not self.enrichment_path:
                self.enrichment_path = os.path.join(
                    os.getenv("SQL_DB_PATH"),
                    self.benchmark_id,
                    self.db_id,
                    self.db_id + "_enriched.json",
                )
            self = await self.load_enrichments()
        return self

    async def generate_enrichments(self):
        self.ddl = self.db_schema.generate_ddl()
        self = await self.generate_db_description()
        self = await self.get_schema_enrichments()
        with open(self.enrichment_path, "w") as f:
            f.write(self.model_dump_json())
        return self

    async def load_enrichments(self):

        if not self.enrichment_path:
            self.enrichment_path = os.path.join(
                os.getenv("SQL_DB_PATH"),
                self.benchmark_id,
                self.db_id,
                self.db_id + "_enriched.json",
            )

        try:
            with open(self.enrichment_path, "r", encoding="utf-8") as f:
                db_dict = json.load(f)
                self = DB(**db_dict)
        except:
            logger.error(
                f"Failed to load enrichments from file {self.enrichment_path}. Generating new Enrichments"
            )
            self = await self.generate_enrichments()
        return self

    # async def generate_questions(self, n_questions: int = 10):
    #     dbs = AG(atype=DB, states=[self])

    #     dbs = await dbs.self_transduction(
    #         ["db_name", "ddl", "tables"],
    #         ["database_schema_description", "business_description", "keywords"],
    #         instructions=f"""Generate the required description of the DB from the input ddl.""",
    #     )

    #     natural_language_questions = []
    #     sql_queries = []
    #     for keyword in dbs[0].keywords:
    #         dbs = await dbs.self_transduction(
    #             [
    #                 "db_name",
    #                 "ddl",
    #                 "tables",
    #                 "database_schema_description",
    #                 "business_description",
    #             ],
    #             ["natural_language_questions", "sql_queries"],
    #             instructions=f"""Generate {n_questions} natural language questions that can be answered
    #             by issuing SQL queries to the provided database about the keyword {keyword}.""",
    #         )
    #         natural_language_questions += dbs[0].natural_language_questions
    #         sql_queries += dbs[0].sql_queries
    #     dbs[0].natural_language_questions = natural_language_questions
    #     dbs[0].sql_queries = sql_queries
    #     questions = dbs[0].natural_language_questions

    #     for i, sql_query in enumerate(dbs[0].sql_queries):
    #         query_execution = await self.async_execute_sql(sql_query)
    #         if type(query_execution) == DataFrame:
    #             if len(query_execution.notnull().values) and len(questions) > i:
    #                 dbs[0].few_shots.append(
    #                     QuestionSQLPair(question=questions[i], sql_query=sql_query)
    #                 )
    #     return dbs[0]


async def main():
    db = DB(benchmark_id="archer_en_dev", db_id="world_1")
    return await db.load_db(enrichments=True)


if __name__ == "__main__":
    print(asyncio.run(main()))
