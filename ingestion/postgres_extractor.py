"""
SQLite → pd.DataFrame extractor.
Только читает данные. Не пишет в БД.

Добавляет lineage-колонки к каждой строке:
  _etl_loaded_at — timestamp загрузки (UTC)
  _source_file   — имя таблицы-источника
  _sheet_name    — None (не применимо для SQLite)
  _row_number    — порядковый номер строки в выгрузке

Все значения из SQLite приводятся к строкам (str).
Несколько исходных таблиц для одной сущности объединяются через pd.concat.
"""

import os
import sqlite3
from datetime import datetime, timezone

import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SQLITE_PATH = os.getenv("SQLITE_PATH", os.path.join(BASE_DIR, "etl_db.sqlite"))

# Маппинг логических ассетов к таблицам SQLite
ASSET_TABLES: dict[str, list[str]] = {
    "obuch_oo": [
        "oo_1_2_7_2_211_v2",
        "oo_1_2_7_1_209_v2",
        "oo_1_2_14_2_1_151_v2",
        "oo_1_2_14_2_2_152_v2",
        "oo_1_2_14_2_3_153_v2",
        "oo_1_2_14_1_1_147_v2",
        "oo_1_2_14_1_2_148_v2",
        "oo_1_2_14_1_3_149_v2",
        "discipuli",
    ],
    "obuch_vpo":  ["впо_1_р2_13_54"],
    "obuch_spo":  ["спо_1_р2_101_43"],
    "obuch_pk":   ["пк_1_2_4_180"],
    "obshagi_vpo": ["впо_2_р1_3_8", "впо_2_р1_4_10"],
    "ped_oo":     ["oo_1_3_4_230", "oo_1_3_1_218", "oo_1_3_2_221"],
}


def _read_sqlite_table(table: str, loaded_at: datetime) -> pd.DataFrame:
    """Читает одну таблицу из SQLite."""
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(f'SELECT * FROM "{table}"')
        rows = cur.fetchall()

        if not rows:
            return pd.DataFrame()

        records = []
        for row_number, row in enumerate(rows):
            record: dict = {
                "_etl_loaded_at": loaded_at,
                "_source_file":   table,
                "_sheet_name":    None,
                "_row_number":    row_number,
            }
            for col in row.keys():
                val = row[col]
                record[col] = str(val) if val is not None else None
            records.append(record)

        return pd.DataFrame(records)
    finally:
        conn.close()


def extract_asset(asset_name: str) -> pd.DataFrame:
    """
    Извлекает данные для конкретного ассета из всех его SQLite-таблиц.
    Таблицы объединяются через pd.concat; колонка _source_file различает источники.
    """
    tables = ASSET_TABLES.get(asset_name)
    if not tables:
        raise ValueError(f"Неизвестный ассет: {asset_name!r}. "
                         f"Доступные: {list(ASSET_TABLES)}")

    loaded_at = datetime.now(tz=timezone.utc)
    dfs = [_read_sqlite_table(t, loaded_at) for t in tables]
    dfs = [df for df in dfs if not df.empty]

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


# Именованные функции для каждого ассета (для явного импорта)

def read_obuch_oo() -> pd.DataFrame:
    return extract_asset("obuch_oo")

def read_obuch_vpo() -> pd.DataFrame:
    return extract_asset("obuch_vpo")

def read_obuch_spo() -> pd.DataFrame:
    return extract_asset("obuch_spo")

def read_obuch_pk() -> pd.DataFrame:
    return extract_asset("obuch_pk")

def read_obshagi_vpo() -> pd.DataFrame:
    return extract_asset("obshagi_vpo")

def read_ped_oo() -> pd.DataFrame:
    return extract_asset("ped_oo")
