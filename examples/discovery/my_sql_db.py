import asyncio
import json
import os
import sqlite3
from pathlib import Path
from typing import IO, Any, Dict, List, Literal, Optional, Union

import aiosqlite
import pandas as pd
from httpx import AsyncClient
from loguru import logger
from pydantic import BaseModel, ConfigDict, PrivateAttr


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


class SqliteDB(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    name: Optional[str] = None
    _conn: sqlite3.Connection = PrivateAttr(default=None)
    dataset_description: Optional[str] = None
    name: Optional[str] = None
    sqlite_schema: Optional[Any] = None
    db_path: Optional[str] = None
    columns: Optional[list] = None
    metadata: Optional[dict] = None

    async def _get_sqlite_schema(self, db_path: str = None):
        """
        Asynchronously return a simple schema description:
        {
            "table1": [
                {"name": "...", "type": "...", "nullable": True/False,
                "default": "...", "pk": 0/1},
                ...
            ]
        }
        """
        self.db_path = db_path or self.db_path

        schema = {}

        async with aiosqlite.connect(self.db_path) as db:

            # Get user tables
            async with db.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%';
            """
            ) as cursor:
                tables = [row[0] async for row in cursor]

            # For each table, read PRAGMA info
            for table in tables:
                async with db.execute(f"PRAGMA table_info({table});") as cursor:
                    columns_info = [row async for row in cursor]

                columns = []
                for cid, name, col_type, notnull, default, pk in columns_info:
                    columns.append(
                        {
                            "name": name,
                            "type": col_type,
                            "nullable": (notnull == 0),
                            "default": default,
                            "pk": pk,
                        }
                    )

                schema[table] = columns
        self.sqlite_schema = schema

        return schema

    async def import_db(self, db_path: str = None):
        self.db_path = db_path or self.db_path
        self._conn = sqlite3.connect(self.db_path)
        self.sqlite_schema = await self._get_sqlite_schema()
        return self.sqlite_schema

    async def async_execute_sql(self, sql_query: str) -> str:
        """DB id could be a path or a Endpoint connection string"""

        if self.db_path:
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


#     def get_description(self) -> str:
#         return f"""===============================
# Dataset Path: {self.path}
# Dataset Description: {self.dataset_description}
# Columns: {self.columns}
# """
