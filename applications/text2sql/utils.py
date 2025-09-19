import asyncio
import json
import os
import re
import sqlite3
from typing import Literal, Set

import aiosqlite
import pandas as pd
from dotenv import load_dotenv
from httpx import AsyncClient
from loguru import logger

load_dotenv()


async def _endpoint_call(call: Literal["GET", "POST"], endpoint: str, **kwargs):
    api_key = os.getenv("ENDPOINT_API_KEY")
    url = f"{os.getenv('ENDPOINT_URL')}{endpoint}"
    async with AsyncClient(verify=False) as client:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "content-type": "application/json",
        }
        match call:
            case "GET":
                response = await client.get(url, headers=headers, **kwargs)
            case "POST":
                response = await client.post(
                    url, headers=headers, timeout=None, **kwargs
                )
        return response.json()


async def execute_sql_on_endpoint(sql: str, db_id: str) -> str:
    payload = {
        "sql": sql,
        "dataSourceId": db_id,
        "includeCount": True,
        "timeout": 10,
        "verify": False,
    }
    endpoint = "/sql"
    try:
        sample = await _endpoint_call("POST", endpoint, json=payload)
        if "error" in sample and sample["error"]:
            return str(sample)
        return json.dumps(sample.get("results"))
    except Exception as e:
        logger.debug(
            f"Error executing the payload {payload} {e.__class__.__name__, str(e)}, retrying..."
        )
        return None


def fix_double_quoted_literals(sql: str) -> str:
    """
    Convert double-quoted *literals* to single-quoted strings.
    Keep double-quoted *identifiers* like "MyTable" as-is.

    Heuristic: if the content is a simple identifier ([A-Za-z_][A-Za-z0-9_]*),
    we keep the double quotes; otherwise we treat it as a literal and convert.
    """
    ident_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def repl(m):
        body = m.group(1).replace('""', '"')  # unescape doubled quotes inside ""
        if ident_re.fullmatch(body):
            # looks like an identifier → leave as "Identifier"
            return f'"{m.group(1)}"'
        # looks like a literal → convert to '...'
        return "'" + body.replace("'", "''") + "'"

    # Match " ... " allowing doubled "" inside
    return re.sub(r'"((?:[^"]|"")*)"', repl, sql)


def get_schema_from_file(benchmark_id, db_id: str = None):
    with open(
        os.path.join(os.getenv("SQL_BENCHMARKS_FOLDER"), benchmark_id + "-schema.json")
    ) as f:
        all_dbs = json.load(f)

    return all_dbs.get(db_id) if db_id else all_dbs


def get_schema_from_sqllite(db_path, add_sample_values: int = 5):

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    schema_json = {}
    for table in tables:

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

    return schema_json

    # # build column descriptions
    # schema_json[table_name] = {
    #     col[1]: {
    #         "type": col[2],
    #         "notnull": col[3],
    #         "dflt_value": col[4],
    #         "samples": []   # will fill below
    #     }
    #     for col in schema
    # }

    # # --- get sample data ---
    # cursor.execute(f"SELECT * FROM {table_name} LIMIT 3;")
    # sample_data = cursor.fetchall()

    # # attach samples per column
    # for row in sample_data:
    #     for col_name, value in zip(columns, row):
    #         schema_json[table_name][col_name]["samples"].append(value)


def remove_duplicate_col_df(df):
    return df.loc[:, ~df.columns.duplicated()]


def convert_df_to_set(df, row_invariant=True) -> Set:
    # remove duplicate columns
    df = remove_duplicate_col_df(df)

    if row_invariant:
        return set(
            [
                tuple(sorted(df[c].to_list(), key=lambda x: (x is None, str(x))))
                for c in df.columns.values
            ]
        )
    else:
        return set([tuple(df[c].to_list()) for c in df.columns.values])


def compare_df(gt, predicted, row_invariant=False) -> bool:
    # 1: gt_df is subset of predicted_df
    # 2: df1 == df2
    # 0: otherwise
    if predicted.startswith("Error:") or gt.startswith("Error:"):
        return 0
    gt_df = pd.read_json(gt)
    predicted_df = pd.read_json(predicted)
    gt_df = gt_df.map(lambda x: float(f"{x:.5f}") if isinstance(x, float) else x)
    predicted_df = predicted_df.map(
        lambda x: float(f"{x:.5f}") if isinstance(x, float) else x
    )

    gt_set = convert_df_to_set(gt_df, row_invariant=row_invariant)
    predicted_set = convert_df_to_set(predicted_df, row_invariant=row_invariant)

    intersec = gt_set & predicted_set
    return (
        1
        if (intersec == gt_set)
        else 1 if (predicted_set == gt_set) else 1 if (intersec == predicted_set) else 0
    )


async def async_execute_sql(
    sql_query: str, db_path: str = None, endpoint_id: str = None
) -> str:
    """DB id could be a path or a Endpoint connection string"""
    if endpoint_id:

        return await execute_sql_on_endpoint(sql_query, endpoint_id)

    elif db_path:
        try:
            async with aiosqlite.connect(db_path) as db:
                async with db.execute(sql_query.replace('"', "'")) as cursor:
                    columns = [description[0] for description in cursor.description]
                    rows = await asyncio.wait_for(cursor.fetchall(), timeout=10)
                    df = pd.DataFrame(rows, columns=columns)
                    return df.to_json()
        except Exception as e:
            return f"Error: {str(e)}"


def evaluate_execution_accuracy(test):
    total = 0
    for question in test:
        total += compare_df(question.system_output_df, question.gt_output_df)
    execution_accuracy = total / len(test.states)
    print(f"Test size: {len(test.states)}\nExecution Accuracy: {execution_accuracy}")
    return execution_accuracy


def load_benchmark(benchmark_id: str = None, path=None):
    with open(os.getenv("SQL_BENCHMARKS_FOLDER") + ".json") as f:
        benchmarks = json.loads(f.read())
        print()
        for benchmark in benchmarks:
            if os.getenv("ENDPOINT_METADATA") in benchmarks[benchmark]:
                benchmarks[benchmark]["datasource_url"] = benchmarks[benchmark][
                    os.getenv("ENDPOINT_METADATA")
                ]
                benchmarks[benchmark].pop(os.getenv("ENDPOINT_METADATA"))
        return benchmarks.get(benchmark_id) or benchmarks
