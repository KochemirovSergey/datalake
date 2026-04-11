"""
Bronze → Silver: таблица дошкольников по регионам.

Что делает:
  - Читает bronze.excel_tables (сырые строки Excel)
  - Определяет строки данных (после заголовка)
  - Распознаёт колонки возрастов по году
  - Нормализует название региона → region_code через bronze.region_lookup
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

import re
import logging
import os

import pyarrow as pa
import pandas as pd
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

# ── Конфигурация ───────────────────────────────────────────────────────────────

# Маппинг: номер колонки → название age_group
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


# ── Нормализация названий регионов ─────────────────────────────────────────────

def _normalize_region_name(name: str) -> str:
    """
    Приводит название региона к виду для сравнения с region_lookup.
    """
    if not name:
        return ""
    s = name.strip()

    # Латинская H → кириллическая Н (артефакт в 2018: "Hижегородская")
    s = s.replace("H", "Н").replace("h", "н")

    s = s.lower()

    # "Город Санкт-Петербург город федерального значения" → "санкт-петербург"
    s = re.sub(r"город федерального значения\s*", "", s)
    s = re.sub(r"^город\s+", "", s)

    # "г. Санкт-Петербург" / "г Санкт-Петербург" → убрать префикс г.
    s = re.sub(r"^г\.\s*", "", s)
    s = re.sub(r"^г\s+", "", s)

    # Скобки: контент СОХРАНЯЕМ (Якутия нужна), только убираем скобки
    # "(Якутия)" → " Якутия", "(кроме ...)" → тоже оставим, потом отфильтруем
    s = re.sub(r"\(([^)]+)\)", r" \1", s)

    # Тире → пробел (Кабардино-Балкарская, Северная Осетия-Алания, Ямало-Ненецкий)
    s = s.replace("-", " ").replace("–", " ")

    # "авт." → "автономный " (Чукотский авт.округ → авт.округ → автономный округ)
    s = s.replace("авт.", "автономный ")

    # Убрать двойные пробелы
    s = re.sub(r"\s+", " ", s).strip()

    return s


def build_region_index(cat: SqlCatalog) -> dict[str, str]:
    """
    Возвращает словарь: нормализованное_название → region_code.
    Включает и канонические имена, и алиасы.
    """
    df = cat.load_table("bronze.region_lookup").scan().to_pandas()
    index: dict[str, str] = {}
    for _, row in df.iterrows():
        key = _normalize_region_name(row["name_variant"])
        index[key] = row["region_code"]

    # Дополнительные алиасы для вариантов без уточнений в названии
    extras = {
        # "Ханты-Мансийский автономный округ" (без «Югра») = тот же регион
        "ханты мансийский автономный округ": "RU-KHM",
    }
    for key, code in extras.items():
        if key not in index:
            index[key] = code

    return index


def lookup_region(name: str, index: dict[str, str]) -> str | None:
    """
    Ищет region_code по названию. Если точного совпадения нет —
    пробует последовательно убирать последнее слово (до 4 попыток).
    Это покрывает случаи типа:
      "кемеровская область кузбасс"        → "кемеровская область"
      "чувашская республика чувашия"       → "чувашская республика"
      "ханты мансийский ... тюменская обл" → "ханты мансийский ... югра"
    """
    key = _normalize_region_name(name)
    if key in index:
        return index[key]

    # Убираем по одному слову с конца
    parts = key.split()
    for trim in range(1, min(4, len(parts))):
        shorter = " ".join(parts[: len(parts) - trim])
        if shorter in index:
            return index[shorter]

    return None


# ── Определение начала данных ──────────────────────────────────────────────────

def is_numbering_row(row: pd.Series) -> bool:
    """True если это строка-нумератор колонок (A/1/2/3... или 1.0/2.0/3.0...)."""
    c0 = str(row.get("col_0", "") or "").strip()
    c1 = str(row.get("col_1", "") or "").strip()
    if c0 == "A" and c1 in ("1", "1.0"):
        return True
    try:
        if float(c0) == 1.0 and float(c1) == 2.0:
            return True
    except (ValueError, TypeError):
        pass
    return False


def find_data_start(sheet_df: pd.DataFrame) -> int:
    """Возвращает row_num первой строки данных (после нумератора)."""
    sorted_df = sheet_df.sort_values("row_num")
    for _, row in sorted_df.iterrows():
        if is_numbering_row(row):
            return int(row["row_num"]) + 1
    return 0


# ── Основная трансформация ─────────────────────────────────────────────────────

def transform(cat: SqlCatalog) -> list[dict]:
    log.info("Loading bronze data...")
    bronze = cat.load_table("bronze.excel_tables").scan().to_pandas()

    region_index = build_region_index(cat)
    log.info("Region index: %d entries", len(region_index))

    records = []
    skipped_regions: set[str] = set()

    for (year, sheet_name), group in bronze.groupby(["year", "sheet_name"]):
        territory = SHEET_TERRITORY.get(str(sheet_name))
        if territory is None:
            log.warning("Unknown sheet '%s', skipping", sheet_name)
            continue

        col_map = get_col_map(int(year))
        data_start = find_data_start(group)
        data_rows = group[group["row_num"] >= data_start].sort_values("row_num")

        for _, row in data_rows.iterrows():
            region_raw = str(row.get("col_0") or "").strip()
            if not region_raw or region_raw in ("nan", "None", ""):
                continue

            region_code = lookup_region(region_raw, region_index)
            if region_code is None:
                skipped_regions.add(region_raw)
                continue

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
                    "region_code":     region_code,
                    "region_name_raw": region_raw,
                    "year":            int(year),
                    "territory_type":  territory,
                    "age_group":       age_group,
                    "value":           value,
                })

    if skipped_regions:
        log.info("Regions not matched (%d):", len(skipped_regions))
        for r in sorted(skipped_regions):
            log.info("  SKIP: %s", r)

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
