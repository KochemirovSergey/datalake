"""
Bronze → Silver: таблица высшего образования (ВПО).

Источник: bronze.впо_1_р2_13_54
Агрегация:
  - Государственные + Негосударственные
  - Очная + Очно-заочная + Заочная + Аттестация экстернов

Ключ агрегации: region_code, year, age, level_code

Фильтры:
  - row_name из списка возрастных значений ВПО

Схема silver.vpo:
  region_code, region_name_raw, year, age, level_code, level_label, value
"""

import logging
import os

import pyarrow as pa
import pandas as pd
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

SOURCE_TABLE = "впо_1_р2_13_54"

# Возрастные значения для ВПО
AGE_PATTERNS = [
    "всего",
    "18 лет и моложе",
    "19 лет",
    "20 лет",
    "21-24 лет",
    "25-29 лет",
    "30-34 лет",
    "35 лет и старше",
    "возраст неизвестен",
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


def _normalize_age(row_name: str) -> str | None:
    """Нормализует возраст из row_name."""
    if not row_name:
        return None
    s = str(row_name).strip().lower()
    
    for age in AGE_PATTERNS:
        if age.lower() in s:
            return age
    return None


def transform(cat: SqlCatalog) -> list[dict]:
    """Основная трансформация для ВПО."""
    records = []

    # Загружаем нормализованные измерения
    log.info("Загружаем bronze_normalized...")
    norm_region = cat.load_table("bronze_normalized.region").scan().to_pandas()
    norm_year = cat.load_table("bronze_normalized.year").scan().to_pandas()
    norm_edu = cat.load_table("bronze_normalized.education_level").scan().to_pandas()

    # Фильтруем по source_table
    src_regions = norm_region[norm_region["source_table"] == SOURCE_TABLE].copy()
    src_years = norm_year[norm_year["source_table"] == SOURCE_TABLE].copy()
    src_edu = norm_edu[norm_edu["source_table"] == SOURCE_TABLE].copy()

    if src_regions.empty:
        log.warning("Нет нормализованных регионов для %s", SOURCE_TABLE)
        return []

    # Загружаем данные из bronze
    try:
        bronze_tbl = cat.load_table(f"bronze.{SOURCE_TABLE}")
        bronze_df = bronze_tbl.scan().to_pandas()
    except Exception as e:
        log.error("Не удалось загрузить bronze.%s: %s", SOURCE_TABLE, e)
        return []

    if bronze_df.empty:
        log.warning("Таблица bronze.%s пуста", SOURCE_TABLE)
        return []

    # JOIN с нормализованными измерениями
    bronze_df["row_id"] = bronze_df.apply(
        lambda r: f"{SOURCE_TABLE}||{r['row_num']}", axis=1
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
            src_edu[["row_id", "level_code", "level_label"]],
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
        # Нормализуем возраст
        age = _normalize_age(row.get("row_name"))
        if age is None:
            continue

        # Получаем значение
        value = _to_int(row.get("value"))

        level_code = str(row.get("level_code", ""))
        level_label = str(row.get("level_label", ""))
        if not level_code:
            continue

        records.append({
            "region_code": row["region_code"],
            "region_name_raw": row["region_raw"],
            "year": int(row["year"]),
            "age": age,
            "level_code": level_code,
            "level_label": level_label or level_code,
            "value": value,
        })

    if not records:
        log.warning("Нет записей для агрегации")
        return []

    # Агрегация
    df = pd.DataFrame(records)
    log.info("До агрегации: %d строк", len(df))

    grouped = df.groupby(
        ["region_code", "region_name_raw", "year", "age", "level_code", "level_label"],
        as_index=False
    )["value"].sum()

    log.info("После агрегации: %d строк", len(grouped))

    return grouped.to_dict(orient="records")


def run() -> int:
    """Запускает трансформацию и записывает в silver.vpo"""
    cat = SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )

    try:
        tbl = cat.load_table("silver.vpo")
    except Exception as e:
        log.error("Таблица silver.vpo не найдена: %s", e)
        return 0

    # Идемпотентность
    try:
        existing = tbl.scan().to_arrow()
        if len(existing) > 0:
            log.info("silver.vpo уже содержит %d строк, пропускаю", len(existing))
            return 0
    except Exception:
        pass

    records = transform(cat)
    if not records:
        log.warning("Нет данных для записи")
        return 0

    pa_schema = pa.schema([
        pa.field("region_code", pa.string(), nullable=False),
        pa.field("region_name_raw", pa.string(), nullable=False),
        pa.field("year", pa.int32(), nullable=False),
        pa.field("age", pa.string(), nullable=False),
        pa.field("level_code", pa.string(), nullable=False),
        pa.field("level_label", pa.string(), nullable=False),
        pa.field("value", pa.int64(), nullable=True),
    ])

    arrow_tbl = pa.table(
        {
            "region_code": [r["region_code"] for r in records],
            "region_name_raw": [r["region_name_raw"] for r in records],
            "year": [r["year"] for r in records],
            "age": [r["age"] for r in records],
            "level_code": [r["level_code"] for r in records],
            "level_label": [r["level_label"] for r in records],
            "value": [r["value"] for r in records],
        },
        schema=pa_schema,
    )

    tbl.append(arrow_tbl)
    log.info("Записано %d строк в silver.vpo", len(records))
    return len(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()
    if count > 0:
        print(f"\nГотово: {count} строк")
