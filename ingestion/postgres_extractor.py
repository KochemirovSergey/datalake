"""
PostgreSQL → pd.DataFrame extractor.
Только читает данные. Не пишет в БД.

Добавляет lineage-колонки к каждой строке:
  _etl_loaded_at — timestamp загрузки (UTC)
  _source_file   — имя таблицы-источника (schema.table)
  _sheet_name    — None (не применимо для PostgreSQL)
  _row_number    — порядковый номер строки в выгрузке

Все значения из PostgreSQL приводятся к строкам (str).
Несколько исходных таблиц для одной сущности объединяются через pd.concat.
"""

import os
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import psycopg2.extras

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SRC_DB = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "database": os.getenv("PG_DATABASE", "etl_db"),
    "user":     os.getenv("PG_USER", "etl_user"),
    "password": os.getenv("PG_PASSWORD", "etl_password"),
}

# Маппинг логических ассетов к таблицам PostgreSQL
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


def _read_pg_table(pg_table: str, loaded_at: datetime) -> pd.DataFrame:
    """Читает одну таблицу из схемы public PostgreSQL."""
    source_file = f"public.{pg_table}"
    conn = psycopg2.connect(**SRC_DB)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f'SELECT * FROM "public"."{pg_table}"')
            rows = cur.fetchall()

        if not rows:
            return pd.DataFrame()

        records = []
        for row_number, row in enumerate(rows):
            record: dict = {
                "_etl_loaded_at": loaded_at,
                "_source_file":   source_file,
                "_sheet_name":    None,
                "_row_number":    row_number,
            }
            for col, val in row.items():
                record[col] = str(val) if val is not None else None
            records.append(record)

        return pd.DataFrame(records)
    finally:
        conn.close()


def extract_asset(asset_name: str) -> pd.DataFrame:
    """
    Извлекает данные для конкретного ассета из всех его PostgreSQL-таблиц.
    Таблицы объединяются через pd.concat; колонка _source_file различает источники.
    """
    tables = ASSET_TABLES.get(asset_name)
    if not tables:
        raise ValueError(f"Неизвестный ассет: {asset_name!r}. "
                         f"Доступные: {list(ASSET_TABLES)}")

    loaded_at = datetime.now(tz=timezone.utc)
    dfs = [_read_pg_table(t, loaded_at) for t in tables]
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
