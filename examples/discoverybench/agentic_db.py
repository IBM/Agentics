from dotenv import load_dotenv
load_dotenv()

import os
import json
from pathlib import Path
import sqlite3
import pandas as pd
from typing import Optional
from pydantic import BaseModel, ConfigDict, PrivateAttr
import csv

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
    _conn: sqlite3.Connection = PrivateAttr(default=None)
    dataset_description :Optional[str] = None
    df: Optional[dict] = None
    path: Optional[str] = None
    columns: Optional[list]=None
    metadata: Optional[dict] = None

    
    @classmethod
    def import_from_discovery_bench_metadata(cls, metadata_path: str):
        output_dbs = []
        metadata = json.load(open(metadata_path, 'r'))

        datadaset_csvs = [metadata["datasets"][i]["name"] for i in range(len(metadata["datasets"])) if metadata["datasets"][i]["name"].endswith("csv")]
        datadaset_txts = [metadata["datasets"][i]["name"] for i in range(len(metadata["datasets"])) if metadata["datasets"][i]["name"].endswith("txt")]
        datadaset_descriptions = [metadata["datasets"][i]["description"] for i in range(len(metadata["datasets"]))]
        columns = [metadata["datasets"][i]["columns"]["raw"] for i in range(len(metadata["datasets"]))]
        for dataset_csv, dataset_description, column in zip(datadaset_csvs,datadaset_descriptions, columns):  
            data_path = Path(os.path.dirname(metadata_path)) /dataset_csv
            database= AgenticDB(dataset_description=dataset_description, 
                                df=read_table_smart(data_path).to_dict(),
                                path=str(data_path),
                                columns=column)
            #database.import_db_from_csv(data_path)
            output_dbs.append(database)
        return output_dbs
    
    def import_db_from_csv(self,path: str = None):
        self.path = path or self.path
        if self.path is None:
            raise ValueError("Path to CSV file must be provided")
        self._conn=sqlite3.connect(self.path)
        df = pd.read_csv(path)
        df.to_sql("data", self._conn, index=False, if_exists="replace")


    def query_db(self, sql: str) -> pd.DataFrame:
        """Execute an SQL query and return the result as a pandas DataFrame."""
        try:
            df = pd.read_sql_query(sql, self._conn)
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()
        
    def get_description(self)-> str:
        return  f"""===============================
Dataset Path: {self.path}
Dataset Description: {self.dataset_description}
Columns: {self.columns}
"""
