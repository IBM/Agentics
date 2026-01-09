from dotenv import load_dotenv

load_dotenv()

import csv
import json
import os
import sqlite3
from pathlib import Path
from typing import IO, Optional, Union

import pandas as pd
from pydantic import BaseModel, ConfigDict, PrivateAttr


def read_table_smart(path: str) -> pd.DataFrame:
    # read a small sample (first line or two)
    with open(path, "r", encoding="utf-8") as f:
        sample = f.readline()

    # count potential delimiters
    commas = sample.count(",")
    tabs = sample.count("\t")
    semicolons = sample.count(";")

    # choose the most frequent one
    if tabs > commas and tabs > semicolons:
        sep = "\t"
    elif semicolons > commas:
        sep = ";"
    else:
        sep = ","

    # print(f"Detected delimiter: {repr(sep)}")
    return pd.read_csv(path, sep=sep, engine="python")


class AgenticDB(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    name: Optional[str] = None
    # _conn: sqlite3.Connection = PrivateAttr(default=None)
    dataset_description: Optional[str] = None
    name: Optional[str] = None
    df: Optional[dict] = None
    path: Optional[str] = None
    columns: Optional[list] = None
    metadata: Optional[dict] = None

    @classmethod
    def import_from_discovery_bench_metadata(cls, metadata_path: str):
        output_dbs = []
        metadata = json.load(open(metadata_path, "r"))

        datadaset_csvs = [
            metadata["datasets"][i]["name"]
            for i in range(len(metadata["datasets"]))
            if metadata["datasets"][i]["name"].endswith("csv")
        ]
        datadaset_txts = [
            metadata["datasets"][i]["name"]
            for i in range(len(metadata["datasets"]))
            if metadata["datasets"][i]["name"].endswith("txt")
        ]
        datadaset_descriptions = [
            metadata["datasets"][i]["description"]
            for i in range(len(metadata["datasets"]))
        ]
        columns = [
            metadata["datasets"][i]["columns"]["raw"]
            for i in range(len(metadata["datasets"]))
        ]
        for dataset_csv, dataset_description, column in zip(
            datadaset_csvs, datadaset_descriptions, columns
        ):
            data_path = Path(os.path.dirname(metadata_path)) / dataset_csv
            database = AgenticDB(
                dataset_description=dataset_description,
                df=read_table_smart(data_path).to_dict(),
                path=str(data_path),
                name=dataset_csv,
                columns=column,
            )
            # database.import_db_from_csv(data_path)
            output_dbs.append(database)
        return output_dbs

    def import_db_from_csv(self, source: Union[str, IO[bytes]] = None):
        """
        Import a CSV file into an in-memory or file-based SQLite database.
        Supports both:
        - path to a CSV file (string)
        - file-like buffer (from Streamlit uploader or similar)
        """
        # Handle file path or buffer
        if isinstance(source, str):
            self.path = source
            self.df = pd.read_csv(source)
        elif source is not None:
            self.path = ":memory:"  # store in memory if no path
            self.df = pd.read_csv(source)
        elif self.path:
            self.df = pd.read_csv(self.path)
        else:
            raise ValueError(
                "Either a CSV file path or a file-like object must be provided."
            )

        # Connect to SQLite (memory or file)
        # self._conn = sqlite3.connect(self.path)

        # Write dataframe to DB
        # self.df.to_sql("data", self._conn, index=False, if_exists="replace")

        return self.df

    def query_db(self, sql: str) -> pd.DataFrame:
        """Execute an SQL query and return the result as a pandas DataFrame."""
        try:
            df = pd.read_sql_query(sql, self._conn)
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()

    def get_description(self) -> str:
        return f"""===============================
Dataset Path: {self.path}
Dataset Description: {self.dataset_description}
Columns: {self.columns}
"""
