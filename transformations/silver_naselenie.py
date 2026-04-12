"""
Bronze → Silver: численность населения по регионам, полу и возрасту.

Что делает:
  - Читает bronze_normalized.region (source_id=naselenie) — нормализованный регион
  - Читает bronze_normalized.year  (source_id=naselenie) — валидированный год
  - Берёт пересечение по row_id → только строки, где оба признака распознаны
  - Джойнит с bronze.excel_tables для получения числовых значений по колонкам
  - Фильтрует строки данных по возрасту (0–79, 80+)
  - Пишет в silver.naselenie

Форматы по годам:
  2018-2020: листы «Таб.2.X.Y», смещение +1 (col_1 = возраст, col_2..col_10 = данные)
  2021-2024: листы «2.X.Y.»,    без смещения (col_0 = возраст, col_1..col_9  = данные)

Схема результата silver.naselenie:
  region_code     — ISO-код субъекта (RU-MOW и т.д.)
  region_name_raw — исходное название из Excel
  year            — год данных
  age             — возраст: «0»–«79», «80+»
  total_both      — всё население, оба пола
  total_male      — всё население, мужчины
  total_female    — всё население, женщины
  urban_both / urban_male / urban_female
  rural_both / rural_male / rural_female
"""

import re
import logging
import os

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

SOURCE_ID = "naselenie"


# ── Формат по году ─────────────────────────────────────────────────────────────

def _is_old_format(year: int) -> bool:
    """2018-2020: старый формат со смещением столбцов."""
    return year <= 2020


def _col(year: int, offset: int) -> str:
    base = 1 if _is_old_format(year) else 0
    return f"col_{base + offset}"


def _age_col(year: int) -> str:
    return _col(year, 0)


# ── Фильтрация возрастов ───────────────────────────────────────────────────────

_AGE_80_PLUS_VARIANTS = {"80 и старше", "80 и более", "80 лет и старше"}


def _parse_age(raw: str) -> str | None:
    """
    Возвращает нормализованный возраст или None если строку нужно пропустить.
    Одиночные: «0»–«79» → «0»–«79».
    «80 и старше» и варианты → «80+».
    Всё остальное (диапазоны, «Итого», ...) → None.
    """
    if not raw:
        return None
    s = raw.strip()
    if s in _AGE_80_PLUS_VARIANTS:
        return "80+"
    if re.fullmatch(r"\d+", s):
        age = int(s)
        if age <= 79:
            return s
    return None


# ── Вспомогательные ────────────────────────────────────────────────────────────

def _to_int(val) -> int | None:
    if val is None:
        return None
    s = str(val).strip()
    if s in ("", "None", "nan", "-"):
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


# ── Основная трансформация ─────────────────────────────────────────────────────

def transform(cat: SqlCatalog) -> list[dict]:
    # Нормализованные регионы для naselenie
    log.info("Загружаем bronze_normalized.region...")
    norm_region = cat.load_table("bronze_normalized.region").scan().to_pandas()
    norm_region = norm_region[norm_region["source_id"] == SOURCE_ID].copy()
    log.info("bronze_normalized.region (naselenie): %d строк", len(norm_region))

    # Валидированные годы для naselenie — берём только row_id
    log.info("Загружаем bronze_normalized.year...")
    norm_year = cat.load_table("bronze_normalized.year").scan().to_pandas()
    norm_year = norm_year[norm_year["source_id"] == SOURCE_ID][["row_id"]].drop_duplicates()
    log.info("bronze_normalized.year (naselenie): %d строк", len(norm_year))

    # Пересечение: только строки с валидным регионом И валидным годом
    valid = norm_region.merge(norm_year, on="row_id", how="inner")
    log.info("Строк с валидным регионом и годом: %d", len(valid))

    if valid.empty:
        log.warning("Нет валидных строк — bronze_normalized пуст или не запущен")
        return []

    # Сырые значения из bronze
    log.info("Загружаем bronze.excel_tables...")
    bronze = cat.load_table("bronze.excel_tables").scan().to_pandas()
    bronze = bronze[bronze["source_file"].str.contains("Население", na=False)].copy()
    log.info("bronze.excel_tables (naselenie): %d строк", len(bronze))

    # JOIN: valid содержит source_file, sheet_name, row_num, year — уникальный ключ строки
    merged = valid.merge(
        bronze,
        on=["source_file", "sheet_name", "row_num", "year"],
        how="inner",
    )
    log.info("После JOIN с bronze: %d строк", len(merged))

    records = []
    for _, row in merged.iterrows():
        year = int(row["year"])
        age_col = _age_col(year)

        raw_age = row.get(age_col)
        if not raw_age or str(raw_age).strip() in ("", "None", "nan"):
            continue

        age = _parse_age(str(raw_age).strip())
        if age is None:
            continue

        records.append({
            "region_code":     row["region_code"],
            "region_name_raw": row["region_raw"],
            "year":            year,
            "age":             age,
            "total_both":      _to_int(row.get(_col(year, 1))),
            "total_male":      _to_int(row.get(_col(year, 2))),
            "total_female":    _to_int(row.get(_col(year, 3))),
            "urban_both":      _to_int(row.get(_col(year, 4))),
            "urban_male":      _to_int(row.get(_col(year, 5))),
            "urban_female":    _to_int(row.get(_col(year, 6))),
            "rural_both":      _to_int(row.get(_col(year, 7))),
            "rural_male":      _to_int(row.get(_col(year, 8))),
            "rural_female":    _to_int(row.get(_col(year, 9))),
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

    tbl = cat.load_table("silver.naselenie")

    existing = tbl.scan().to_arrow()
    if len(existing) > 0:
        log.info("silver.naselenie уже содержит %d строк, пропускаю", len(existing))
        return 0

    records = transform(cat)
    if not records:
        log.warning("Нет данных для записи")
        return 0

    pa_schema = pa.schema([
        pa.field("region_code",     pa.string(), nullable=False),
        pa.field("region_name_raw", pa.string(), nullable=False),
        pa.field("year",            pa.int32(),  nullable=False),
        pa.field("age",             pa.string(), nullable=False),
        pa.field("total_both",      pa.int64(),  nullable=True),
        pa.field("total_male",      pa.int64(),  nullable=True),
        pa.field("total_female",    pa.int64(),  nullable=True),
        pa.field("urban_both",      pa.int64(),  nullable=True),
        pa.field("urban_male",      pa.int64(),  nullable=True),
        pa.field("urban_female",    pa.int64(),  nullable=True),
        pa.field("rural_both",      pa.int64(),  nullable=True),
        pa.field("rural_male",      pa.int64(),  nullable=True),
        pa.field("rural_female",    pa.int64(),  nullable=True),
    ])

    cols = {
        f.name: pa.array([r[f.name] for r in records], f.type)
        for f in pa_schema
    }
    arrow_tbl = pa.table(cols, schema=pa_schema)

    tbl.append(arrow_tbl)
    log.info("Записано %d строк в silver.naselenie", len(records))
    return len(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()
    if count > 0:
        print(f"\nГотово: {count} строк")
