"""
Bronze → Silver: таблица дополнительного профессионального образования (ДПО).

Источник: bronze.пк_1_2_4_180
Агрегация: не требуется, данные уже в целевом виде.

Фильтры:
  - row_name из двух значений программ ДПО (обеспечивается через education_level)
  - column_name — возрастные корзины

Схема silver.dpo:
  region_code, region_name_raw, year, age_band, level_code, program_code, program_label, value
"""

import logging
import os

import pyarrow as pa
import pandas as pd
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

SOURCE_ID = "пк_1_2_4_180"
LEVEL_CODE = "4.8b"

# Возрастные корзины ДПО
AGE_BANDS = [
    "моложе 25",
    "25-29",
    "30-34",
    "35-39",
    "40-44",
    "45-49",
    "50-54",
    "55-59",
    "60-64",
    "65 и более",
]


def _to_int(val) -> int | None:
    """Преобразует строковое значение в int или None."""
    if val is None:
        return None
    s = str(val).strip().replace(" ", "").replace("\u00a0", "")
    if s in ("", "-", "nan", "None", "NULL"):
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def _normalize_age_band(column_name: str) -> str | None:
    """Нормализует возрастную корзину из column_name."""
    if not column_name:
        return None
    s = str(column_name).strip().lower().replace("–", "-")
    
    for band in AGE_BANDS:
        if band.lower() in s:
            return band
    return None


def transform(cat: SqlCatalog) -> list[dict]:
    """Основная трансформация для ДПО."""
    records = []

    # Загружаем нормализованные измерения
    log.info("Загружаем bronze_normalized...")
    norm_region = cat.load_table("bronze_normalized.region").scan().to_pandas()
    norm_year = cat.load_table("bronze_normalized.year").scan().to_pandas()
    norm_edu = cat.load_table("bronze_normalized.education_level").scan().to_pandas()

    # Фильтруем по source_id
    src_regions = norm_region[norm_region["source_id"] == SOURCE_ID].copy()
    src_years = norm_year[norm_year["source_id"] == SOURCE_ID].copy()
    src_edu = norm_edu[norm_edu["source_id"] == SOURCE_ID].copy()

    if src_regions.empty:
        log.warning("Нет нормализованных регионов для %s", SOURCE_ID)
        return []

    # Загружаем данные из bronze
    try:
        bronze_tbl = cat.load_table(f"bronze.{SOURCE_ID}")
        bronze_df = bronze_tbl.scan().to_pandas()
    except Exception as e:
        log.error("Не удалось загрузить bronze.%s: %s", SOURCE_ID, e)
        return []

    if bronze_df.empty:
        log.warning("Таблица bronze.%s пуста", SOURCE_ID)
        return []

    # JOIN с нормализованными измерениями
    bronze_df["row_id"] = bronze_df.apply(
        lambda r: f"{SOURCE_ID}||{r['row_num']}", axis=1
    )

    # Проверяем gate
    try:
        gate = cat.load_table("bronze_normalized.row_gate").scan().to_pandas()
        gate_ok = gate[gate["ready_for_silver"] == True][["row_id"]].drop_duplicates()
    except Exception:
        gate_ok = None

    # JOIN
    merged = bronze_df.merge(
        src_regions[["row_id", "region_code", "region_raw"]],
        on="row_id",
        how="inner"
    )

    if not src_years.empty and "year" in src_years.columns:
        merged = merged.merge(
            src_years[["row_id", "year"]],
            on="row_id",
            how="inner"
        )
    else:
        log.warning("Год не нормализован для %s", SOURCE_TABLE)
        return []

    if not src_edu.empty:
        merged = merged.merge(
            src_edu[["row_id", "program_code", "program_label"]],
            on="row_id",
            how="inner"
        )
    else:
        log.warning("Education level не нормализован для %s", SOURCE_TABLE)
        return []

    # Фильтруем по gate
    if gate_ok is not None:
        merged = merged[merged["row_id"].isin(gate_ok["row_id"])]

    log.info("После JOIN: %d строк", len(merged))

    # Обрабатываем строки
    for _, row in merged.iterrows():
        # Нормализуем возрастную корзину
        age_band = _normalize_age_band(row.get("column_name"))
        if age_band is None:
            continue

        # Получаем значение
        value = _to_int(row.get("значение"))
        if value is None:
            value = _to_int(row.get("col_0"))  # fallback

        program_code = str(row.get("program_code", ""))
        if not program_code:
            continue

        records.append({
            "region_code": row["region_code"],
            "region_name_raw": row["region_raw"],
            "year": int(row["year"]),
            "age_band": age_band,
            "level_code": LEVEL_CODE,
            "program_code": program_code,
            "program_label": str(row.get("program_label", "")),
            "value": value,
        })

    log.info("Сформировано %d записей", len(records))
    return records


def run() -> int:
    """Запускает трансформацию и записывает в silver.dpo"""
    cat = SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )

    try:
        tbl = cat.load_table("silver.dpo")
    except Exception as e:
        log.error("Таблица silver.dpo не найдена: %s", e)
        return 0

    records = transform(cat)
    if not records:
        log.warning("Нет данных для записи")
        return 0

    pa_schema = pa.schema([
        pa.field("region_code", pa.string(), nullable=False),
        pa.field("region_name_raw", pa.string(), nullable=False),
        pa.field("year", pa.int32(), nullable=False),
        pa.field("age_band", pa.string(), nullable=False),
        pa.field("level_code", pa.string(), nullable=False),
        pa.field("program_code", pa.string(), nullable=False),
        pa.field("program_label", pa.string(), nullable=True),
        pa.field("value", pa.int64(), nullable=True),
    ])

    arrow_tbl = pa.table(
        {
            "region_code": [r["region_code"] for r in records],
            "region_name_raw": [r["region_name_raw"] for r in records],
            "year": [r["year"] for r in records],
            "age_band": [r["age_band"] for r in records],
            "level_code": [r["level_code"] for r in records],
            "program_code": [r["program_code"] for r in records],
            "program_label": [r["program_label"] for r in records],
            "value": [r["value"] for r in records],
        },
        schema=pa_schema,
    )

    try:
        existing = tbl.scan(selected_fields=("region_code",)).to_arrow()
        if len(existing) > 0:
            log.info("Удаляем %d старых строк из silver.dpo", len(existing))
            tbl.delete("region_code is not null")
    except Exception as e:
        log.warning("Не удалось удалить старые данные: %s", e)

    tbl.append(arrow_tbl)
    log.info("Записано %d строк в silver.dpo", len(records))
    return len(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()
    if count > 0:
        print(f"\nГотово: {count} строк")
