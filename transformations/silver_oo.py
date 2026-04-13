"""
Bronze → Silver: таблица общеобразовательных программ (ОО).

Источники: bronze.oo_1_2_7_1_209 + bronze.oo_1_2_7_2_211
Агрегация:
  - Государственные + Негосударственные (схлопываем через SUM)
  - Городские + Сельские (схлопываем через SUM)
  - Строки из обеих исходных таблиц (объединяем через UNION)

Ключ агрегации: region_code, year, age, level_code

Схема silver.oo:
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

SOURCE_IDS = ["oo_1_2_7_1_209", "oo_1_2_7_2_211"]

# Возрастные значения из ОО (нормализованные)
AGE_PATTERNS = [
    "всего",
    "7 лет",
    "8 лет",
    "9 лет",
    "10 лет",
    "11 лет",
    "12 лет",
    "13 лет",
    "14 лет",
    "15 лет",
    "16 лет",
    "17 лет",
    "18 лет и старше",
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
    
    # Ищем паттерны
    for age in AGE_PATTERNS:
        if age.lower() in s:
            return age
    return None


def transform(cat: SqlCatalog) -> list[dict]:
    """Основная трансформация для ОО."""
    records = []

    # Загружаем нормализованные измерения
    log.info("Загружаем bronze_normalized.region...")
    norm_region = cat.load_table("bronze_normalized.region").scan().to_pandas()
    
    log.info("Загружаем bronze_normalized.year...")
    norm_year = cat.load_table("bronze_normalized.year").scan().to_pandas()
    
    log.info("Загружаем bronze_normalized.education_level...")
    norm_edu = cat.load_table("bronze_normalized.education_level").scan().to_pandas()

    # Загружаем справочник уровней
    try:
        edu_lookup = cat.load_table("bronze.education_level_lookup").scan().to_pandas()
        edu_labels = {
            str(row["code"]): str(row["label_ru"]) 
            for _, row in edu_lookup.iterrows()
        }
    except Exception:
        edu_labels = {
            "1.2": "Начальное общее образование",
            "1.3": "Основное общее образование",
            "1.4": "Среднее общее образование",
        }

    for source_id in SOURCE_IDS:
        log.info("Обработка %s...", source_id)
        
        # Фильтруем измерения по source_id
        src_regions = norm_region[norm_region["source_id"] == source_id].copy()
        src_years = norm_year[norm_year["source_id"] == source_id].copy()
        src_edu = norm_edu[norm_edu["source_id"] == source_id].copy()
        
        if src_regions.empty:
            log.warning("Нет нормализованных регионов для %s", source_id)
            continue

        # Загружаем данные из bronze
        try:
            bronze_tbl = cat.load_table(f"bronze.{source_id}")
            bronze_df = bronze_tbl.scan().to_pandas()
        except Exception as e:
            log.warning("Не удалось загрузить bronze.%s: %s", source_id, e)
            continue

        if bronze_df.empty:
            log.warning("Таблица bronze.%s пуста", source_id)
            continue

        # JOIN с нормализованными измерениями
        # row_id в PostgreSQL-таблицах: "table_name||row_num"
        bronze_df["row_id"] = bronze_df.apply(
            lambda r: f"{source_id}||{r['row_num']}", axis=1
        )

        # Проверяем gate
        try:
            gate = cat.load_table("bronze_normalized.row_gate").scan().to_pandas()
            gate_ok = gate[gate["ready_for_silver"] == True][["row_id"]].drop_duplicates()
        except Exception:
            gate_ok = None

        # JOIN region
        merged = bronze_df.merge(
            src_regions[["row_id", "region_code", "region_raw"]],
            on="row_id",
            how="inner"
        )

        # JOIN year
        if not src_years.empty and "year" in src_years.columns:
            merged = merged.merge(
                src_years[["row_id", "year"]],
                on="row_id",
                how="inner"
            )
        else:
            # Если год не нормализован, пробуем извлечь из данных
            log.warning("Год не нормализован для %s, пропускаем", source_id)
            continue

        # JOIN education_level
        if not src_edu.empty:
            merged = merged.merge(
                src_edu[["row_id", "level_code"]],
                on="row_id",
                how="inner"
            )
        else:
            log.warning("Education level не нормализован для %s", source_id)
            continue

        # Фильтруем по gate если есть
        if gate_ok is not None:
            merged = merged[merged["row_id"].isin(gate_ok["row_id"])]

        log.info("После JOIN: %d строк для %s", len(merged), source_id)

        # Обрабатываем строки
        for _, row in merged.iterrows():
            # Нормализуем возраст из row_name
            age = _normalize_age(row.get("row_name"))
            if age is None:
                continue

            # Получаем значение
            value = _to_int(row.get("значение"))
            if value is None:
                value = _to_int(row.get("col_0"))  # fallback

            level_code = str(row.get("level_code", ""))
            if not level_code:
                continue

            records.append({
                "region_code": row["region_code"],
                "region_name_raw": row["region_raw"],
                "year": int(row["year"]),
                "age": age,
                "level_code": level_code,
                "level_label": edu_labels.get(level_code, level_code),
                "value": value,
            })

    if not records:
        log.warning("Нет записей для агрегации")
        return []

    # Агрегация по ключу
    df = pd.DataFrame(records)
    log.info("До агрегации: %d строк", len(df))

    # Группировка и суммирование
    grouped = df.groupby(
        ["region_code", "region_name_raw", "year", "age", "level_code", "level_label"],
        as_index=False
    )["value"].sum()

    log.info("После агрегации: %d строк", len(grouped))

    return grouped.to_dict(orient="records")


def run() -> int:
    """Запускает трансформацию и записывает в silver.oo"""
    cat = SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )

    try:
        tbl = cat.load_table("silver.oo")
    except Exception as e:
        log.error("Таблица silver.oo не найдена: %s", e)
        return 0

    # Идемпотентность
    try:
        existing = tbl.scan().to_arrow()
        if len(existing) > 0:
            log.info("silver.oo уже содержит %d строк, пропускаю", len(existing))
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
    log.info("Записано %d строк в silver.oo", len(records))
    return len(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()
    if count > 0:
        print(f"\nГотово: {count} строк")
