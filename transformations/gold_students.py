"""
Silver → Gold: таблица обучающихся по регионам и возрастам.

Что делает:
  - Читает silver.education_population_wide_annual
  - Убирает колонку region_name_raw
  - Приводит age к INTEGER (нормальный возраст 0–80 лет)
  - Пишет в gold.students

Схема результата:
  region_code, year, age, population_total,
  level_1_1, level_1_2, level_1_3, level_1_4,
  level_2_5_1, level_2_5_2, level_2_6, level_2_7, level_2_8,
  level_4_8b_1, level_4_8b_2,
  education_total, education_share
"""

import logging
import os

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")


def _get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )


def run() -> int:
    """Выполнить трансформацию Silver → Gold для students."""
    cat = _get_catalog()

    log.info("Загружаем silver.education_population_wide_annual...")
    df = cat.load_table("silver.education_population_wide_annual").scan().to_pandas()
    log.info("  Строк в source: %d", len(df))

    # Приводим age к INTEGER (int32 для соответствия схеме Iceberg)
    df["age"] = pd.to_numeric(df["age"], errors="coerce").fillna(0).astype("int32")

    # Убираем region_name_raw
    df = df.drop(columns=["region_name_raw"])

    # Сортируем для консистентности
    df = df.sort_values(["region_code", "year", "age"]).reset_index(drop=True)

    # Все числовые колонки — FLOAT
    numeric_cols = [
        "population_total",
        "level_1_1", "level_1_2", "level_1_3", "level_1_4",
        "level_2_5_1", "level_2_5_2", "level_2_6", "level_2_7", "level_2_8",
        "level_4_8b_1", "level_4_8b_2",
        "education_total", "education_share",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

    # Пишем в gold.students
    table = pa.Table.from_pandas(df)
    tbl = cat.load_table("gold.students")
    tbl.overwrite(table)

    log.info("  Строк в gold.students: %d", len(df))
    return len(df)


def _table_exists(cat: SqlCatalog, table_name: str) -> bool:
    """Проверяет, существует ли таблица."""
    try:
        cat.load_table(table_name)
        return True
    except Exception:
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()
    print(f"✓ gold.students: {count} rows")
