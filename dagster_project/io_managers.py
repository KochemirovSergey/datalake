"""
IO Managers для Medallion 2.0.

BronzeDuckDBIOManager — сохраняет pd.DataFrame в схему bronze DuckDB.
  handle_output: CREATE OR REPLACE TABLE bronze.<asset_name>
  load_input:    SELECT * FROM bronze.<asset_name>
"""

import os

import duckdb
import pandas as pd
from dagster import InputContext, IOManager, OutputContext

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DUCKDB_PATH = os.path.join(BASE_DIR, "datalake.duckdb")


class BronzeDuckDBIOManager(IOManager):
    def __init__(self, duckdb_path: str = DEFAULT_DUCKDB_PATH) -> None:
        self._path = duckdb_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        if obj is None or (isinstance(obj, pd.DataFrame) and obj.empty):
            context.log.warning("Пустой DataFrame для %s — запись пропущена", context.asset_key)
            return

        asset_name = context.asset_key.path[-1]
        conn = duckdb.connect(self._path)
        try:
            conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            conn.register("_payload", obj)
            conn.execute(f'CREATE OR REPLACE TABLE bronze."1_{asset_name}" AS SELECT * FROM _payload')
            context.log.info(
                "bronze.1_%s: записано %d строк, %d колонок",
                asset_name, len(obj), len(obj.columns),
            )
        finally:
            conn.close()

    def load_input(self, context: InputContext) -> pd.DataFrame:
        asset_name = context.asset_key.path[-1]
        conn = duckdb.connect(self._path, read_only=True)
        try:
            return conn.execute(f'SELECT * FROM bronze."1_{asset_name}"').df()
        finally:
            conn.close()
