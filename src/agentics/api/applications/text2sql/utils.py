import asyncio
import json
import os
import re
import sqlite3
from typing import Literal, Optional, Set, Union

import aiosqlite
import pandas as pd
from dotenv import load_dotenv
from httpx import AsyncClient
from loguru import logger

load_dotenv()


# ----- DDL generator -----
def quote_ident(name: str, dialect: str) -> str:
    if name is None:
        raise ValueError("Identifier cannot be None")
    return {
        "sqlite": f'"{name}"',
        "postgres": f'"{name}"',
        "mysql": f"`{name}`",
    }.get(dialect, f'"{name}"')


def map_type(gen_type: Optional[str], dialect: str) -> str:
    t = (gen_type or "str").lower()
    # Generic -> SQL type mapping
    if dialect == "sqlite":
        mapping = {
            "str": "TEXT",
            "text": "TEXT",
            "int": "INTEGER",
            "integer": "INTEGER",
            "float": "REAL",
            "double": "REAL",
            "bool": "INTEGER",  # SQLite has no native BOOL; 0/1
            "boolean": "INTEGER",
            "date": "TEXT",  # or NUMERIC with check/format
            "datetime": "TEXT",
            "timestamp": "TEXT",
            "json": "TEXT",  # use JSON1 functions if enabled
        }
    elif dialect == "postgres":
        mapping = {
            "str": "VARCHAR",
            "text": "TEXT",
            "int": "INTEGER",
            "integer": "INTEGER",
            "float": "DOUBLE PRECISION",
            "double": "DOUBLE PRECISION",
            "bool": "BOOLEAN",
            "boolean": "BOOLEAN",
            "date": "DATE",
            "datetime": "TIMESTAMP",
            "timestamp": "TIMESTAMP",
            "json": "JSONB",
        }
    elif dialect == "mysql":
        mapping = {
            "str": "VARCHAR(255)",
            "text": "TEXT",
            "int": "INT",
            "integer": "INT",
            "float": "DOUBLE",
            "double": "DOUBLE",
            "bool": "TINYINT(1)",
            "boolean": "TINYINT(1)",
            "date": "DATE",
            "datetime": "DATETIME",
            "timestamp": "TIMESTAMP",
            "json": "JSON",
        }
    else:
        raise ValueError(f"Unsupported dialect: {dialect}")
    return mapping.get(t, mapping["str"])


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


from io import StringIO


def compare_df(gt: str, predicted: str, row_invariant=False) -> int:
    # 1: gt_df is subset of predicted_df
    # 2: df1 == df2
    # 0: otherwise
    try:
        gt_df = pd.read_json(StringIO(gt))
    except:
        gt_df = pd.DataFrame()
    try:
        predicted_df = pd.read_json(StringIO(predicted))
    except:
        predicted_df = pd.DataFrame()
    # gt_df = gt_df.map(lambda x: float(f"{x:.5f}") if isinstance(x, float) else x)
    # predicted_df = predicted_df.map(
    #     lambda x: float(f"{x:.5f}") if isinstance(x, float) else x
    # )

    gt_set = convert_df_to_set(gt_df, row_invariant=row_invariant)
    predicted_set = convert_df_to_set(predicted_df, row_invariant=row_invariant)

    intersec = gt_set & predicted_set
    if gt_set in [{()}]:
        return -1

    return (
        1
        if (intersec == gt_set)
        # else 1 if (predicted_set == gt_set) else 1 if (intersec == predicted_set) else 0
        else 0
    )


def compare_df2(gt, predicted, use_df=True) -> bool:
    # compare the exeuction match on set
    if use_df:
        gt = convert_df_to_set(gt, row_invariant=False)
        predicted = convert_df_to_set(predicted, row_invariant=False)
    return gt == predicted


async def async_execute_sql(
    sql_query: str, db_path: str = None, endpoint_id: str = None
) -> str:
    """DB id could be a path or a Endpoint connection string"""
    if endpoint_id:
        return await execute_sql_on_endpoint(sql_query, endpoint_id)

    elif db_path:
        try:
            async with aiosqlite.connect(db_path) as db:
                async with db.execute(sql_query) as cursor:
                    columns = [description[0] for description in cursor.description]
                    rows = await asyncio.wait_for(cursor.fetchall(), timeout=10)
                    df = pd.DataFrame(rows, columns=columns)
                    return df.to_json()
        except Exception as e:
            pass

        try:
            async with aiosqlite.connect(db_path) as db:
                async with db.execute(sql_query.replace('"', "'")) as cursor:
                    columns = [description[0] for description in cursor.description]
                    rows = await asyncio.wait_for(cursor.fetchall(), timeout=10)
                    df = pd.DataFrame(rows, columns=columns)
                    return df.to_json()
        except Exception as e:
            return f"Error: {str(e)}"


def safe_read_df(raw):
    raw = str(raw).strip()

    # skip known garbage
    if raw in ["{}", "{()}", "()", "[]", ""]:
        return None

    # try JSON decode first
    try:
        data = json.loads(raw)
        # skip if it's clearly an error object
        if isinstance(data, dict) and "error" in data:
            return None
        df = pd.DataFrame(data)
    except Exception:
        # fallback to pandas JSON reader
        try:
            df = pd.read_json(raw)
        except Exception:
            return None

    # validate DataFrame content
    if df.empty or df.isna().all().all():
        return None

    # 3. dataframe only has "empty" placeholders like {} () [] ""
    if all(df.map(lambda v: str(v).strip() in {"{}", "{()}", "()", "[]", ""}).all()):
        return None

    return df


def read_tuple(raw):
    raw = str(raw).strip()
    try:
        data = json.loads(raw)
        return set(data)
    except:
        return None


def evaluate_execution_accuracy(test, use_df=True):
    total = 0
    total_non_empty = 0
    total_gt_non_empty = 0
    correct = 0
    correct_non_empty = 0
    count_gt_read_failure = 0  # gt none
    count_response_read_failure = 0  # response none

    for ind, question in enumerate(test, 1):
        print("####")

        gt_df = (
            safe_read_df(question.gt_output_df)
            if use_df
            else read_tuple(question.gt_output_df)
        )
        response_df = (
            safe_read_df(question.system_output_df)
            if use_df
            else read_tuple(question.system_output_df)
        )
        if gt_df is None:
            count_gt_read_failure += 1
        print("####")
        print(f"gt:{ind}")
        print(question.question)
        print(question.query)
        print(question.gt_output_df)
        print(gt_df)
        if response_df is None:
            count_response_read_failure += 1
        print("####")
        print(f"response:{ind}")
        print(question.question)
        print(question.generated_query)
        print(question.system_output_df)
        print(response_df)

        total += 1
        if gt_df is None:
            # declare it is correct or exclude from evaluatoin
            print("####")
            print(f"gt is None, correct:{correct}")
            correct += 1
        elif response_df is None:
            # gt is not None and response is None then wrong
            print("####")
            print(f"response is None, correct:{correct}")
            total_gt_non_empty += 1
        else:
            total_non_empty += 1
            total_gt_non_empty += 1
            res = compare_df2(gt_df, response_df, use_df)
            if res:
                correct += 1
                correct_non_empty += 1
            print("####")
            print(f"res match:{res}, correct:{correct}")

    exec_accu = correct / total  # include gt None case
    if total_non_empty == 0:
        exec_accu_non_empty = 0
    else:
        exec_accu_non_empty = (
            correct_non_empty / total_non_empty
        )  # exclude gt None case

    out = f"""
### DataSet Evaluation

| Metric                        | Value |
|-------------------------------|-------|
| execution_match               | {exec_accu} |
| execution_match_non_empty     | {exec_accu_non_empty} |
| total                         | {total} |
| total_non_empty               | {total_non_empty} |
| total_gt_non_empty            | {total_gt_non_empty} |
| correct                       | {correct} |
| correct_non_empty             | {correct_non_empty} |
| count_gt_read_failure         | {count_gt_read_failure} |
| count_response_read_failure   | {count_response_read_failure} |
"""

    print(out)
    return exec_accu, out


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
