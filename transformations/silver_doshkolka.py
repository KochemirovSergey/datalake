"""
Bronze → Silver: таблица дошкольников по регионам.

Что делает:
  - Читает bronze_normalized.region (source_id=doshkolka) — нормализованный регион
  - Читает bronze_normalized.year  (source_id=doshkolka) — валидированный год
  - Берёт пересечение по row_id → только строки, где оба признака распознаны
  - Джойнит с bronze.excel_tables для получения числовых значений по колонкам
  - Unpivot: одна строка = регион × год × территория × возраст
  - Пишет в silver.doshkolka

Схема результата:
  region_code     — ISO-код (RU-MOW и т.д.)
  region_name_raw — исходное название из Excel
  year            — год
  territory_type  — total / urban / rural
  age_group       — total / age_0 / age_1 / ... / age_7plus
  value           — число детей
"""

import logging
import os

import pyarrow as pa
import pandas as pd
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

SOURCE_ID = "doshkolka"

# ── Конфигурация ───────────────────────────────────────────────────────────────

# 2018-2021: subtotal 0-2 на col_5, subtotal 3+ на col_11 → пропускаем
COL_MAP_2018_2021 = {
    1: "total",
    2: "age_0",
    3: "age_1",
    4: "age_2",
    # 5: skip (subtotal 0-2)
    6: "age_3",
    7: "age_4",
    8: "age_5",
    9: "age_6",
    10: "age_7plus",
    # 11: skip (subtotal 3+)
}

# 2022-2024: нет subtotals, col_5 сразу = age_3
COL_MAP_2022_2024 = {
    1: "total",
    2: "age_0",
    3: "age_1",
    4: "age_2",
    5: "age_3",
    6: "age_4",
    7: "age_5",
    8: "age_6",
    9: "age_7plus",
}

def get_col_map(year: int) -> dict[int, str]:
    return COL_MAP_2018_2021 if year <= 2021 else COL_MAP_2022_2024

# Маппинг листа → тип территории
SHEET_TERRITORY = {
    "6": "total",  "T_0": "total",
    "1": "urban",  "T_1": "urban",
    "2": "rural",  "T_2": "rural",
}


# ── Основная трансформация ─────────────────────────────────────────────────────

def transform(cat: SqlCatalog) -> list[dict]:
    # Нормализованные регионы для doshkolka
    log.info("Загружаем bronze_normalized.region...")
    norm_region = cat.load_table("bronze_normalized.region").scan().to_pandas()
    norm_region = norm_region[norm_region["source_id"] == SOURCE_ID].copy()
    log.info("bronze_normalized.region (doshkolka): %d строк", len(norm_region))

    # Валидированные годы для doshkolka — берём только row_id
    log.info("Загружаем bronze_normalized.year...")
    norm_year = cat.load_table("bronze_normalized.year").scan().to_pandas()
    norm_year = norm_year[norm_year["source_id"] == SOURCE_ID][["row_id"]].drop_duplicates()
    log.info("bronze_normalized.year (doshkolka): %d строк", len(norm_year))

    # Пересечение: только строки с валидным регионом И валидным годом
    valid = norm_region.merge(norm_year, on="row_id", how="inner")
    log.info("Строк с валидным регионом и годом: %d", len(valid))

    if valid.empty:
        log.warning("Нет валидных строк — bronze_normalized пуст или не запущен")
        return []

    # Сырые значения из bronze
    log.info("Загружаем bronze.excel_tables...")
    bronze = cat.load_table("bronze.excel_tables").scan().to_pandas()
    bronze = bronze[bronze["source_file"].str.contains("Дошколка", na=False)].copy()
    log.info("bronze.excel_tables (doshkolka): %d строк", len(bronze))

    # JOIN: valid содержит source_file, sheet_name, row_num, year — уникальный ключ строки
    merged = valid.merge(
        bronze,
        on=["source_file", "sheet_name", "row_num", "year"],
        how="inner",
    )
    log.info("После JOIN с bronze: %d строк", len(merged))

    records = []
    for _, row in merged.iterrows():
        sheet_name = str(row["sheet_name"])
        territory = SHEET_TERRITORY.get(sheet_name)
        if territory is None:
            log.warning("Unknown sheet '%s', skipping", sheet_name)
            continue

        year = int(row["year"])
        col_map = get_col_map(year)

        for col_idx, age_group in col_map.items():
            raw_val = row.get(f"col_{col_idx}")
            if raw_val is None or str(raw_val).strip() in ("nan", "None", "", "-"):
                value = None
            else:
                try:
                    value = int(float(str(raw_val).strip()))
                except (ValueError, TypeError):
                    value = None

            records.append({
                "region_code":     row["region_code"],
                "region_name_raw": row["region_raw"],
                "year":            year,
                "territory_type":  territory,
                "age_group":       age_group,
                "value":           value,
            })

    log.info("Сформировано %d записей", len(records))
    return records


# ── Запись в Iceberg ───────────────────────────────────────────────────────────

def run() -> int:
    cat = SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )

    tbl = cat.load_table("silver.doshkolka")

    # Идемпотентность
    existing = tbl.scan().to_arrow()
    if len(existing) > 0:
        log.info("silver.doshkolka уже содержит %d строк, пропускаю", len(existing))
        return 0

    records = transform(cat)
    if not records:
        log.warning("Нет данных для записи")
        return 0

    pa_schema = pa.schema([
        pa.field("region_code",     pa.string(), nullable=False),
        pa.field("region_name_raw", pa.string(), nullable=False),
        pa.field("year",            pa.int32(),  nullable=False),
        pa.field("territory_type",  pa.string(), nullable=False),
        pa.field("age_group",       pa.string(), nullable=False),
        pa.field("value",           pa.int64(),  nullable=True),
    ])

    arrow_tbl = pa.table(
        {
            "region_code":     pa.array([r["region_code"]     for r in records], pa.string()),
            "region_name_raw": pa.array([r["region_name_raw"] for r in records], pa.string()),
            "year":            pa.array([r["year"]            for r in records], pa.int32()),
            "territory_type":  pa.array([r["territory_type"]  for r in records], pa.string()),
            "age_group":       pa.array([r["age_group"]       for r in records], pa.string()),
            "value":           pa.array([r["value"]           for r in records], pa.int64()),
        },
        schema=pa_schema,
    )

    tbl.append(arrow_tbl)
    log.info("Записано %d строк в silver.doshkolka", len(records))
    return len(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()
    if count > 0:
        print(f"\nГотово: {count} строк")
