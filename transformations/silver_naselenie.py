"""
Bronze → Silver: численность населения по регионам, полу и возрасту.

Источник: bronze.excel_tables, записи из data/Население/Бюллетень_YYYY.xlsx
Листы раздела 2 с двумя уровнями (2.X.Y) — данные по каждому субъекту РФ.
Листы раздела 2 с одним уровнем (2.X) — федеральные округа, пропускаем.

Форматы по годам:
  2018-2020: листы «Таб.2.X.Y», смещение +1 (col_1 = возраст, col_2..col_10 = данные)
  2021-2024: листы «2.X.Y.»,    без смещения (col_0 = возраст, col_1..col_9  = данные)

Строки данных — только одиночные возрасты (0, 1, ..., 79, 80+).
Диапазоны («0–2», «0–4» ...) и итоговые строки («Итого», «моложе трудоспособного» ...) — пропускаем.

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

# Фильтр: source_file должен содержать "Население"
SOURCE_FILTER = "Население"


# ── Идентификация листов ───────────────────────────────────────────────────────

def _normalize_sheet_name(sheet_name: str) -> str:
    """Убирает «Таб.» и лишние точки: «Таб.2.1.1» → «2.1.1»."""
    s = sheet_name.strip()
    if s.startswith("Таб."):
        s = s[4:]
    s = s.strip(".")
    return s


def _is_region_sheet(sheet_name: str) -> bool:
    """
    True если лист содержит данные по субъекту РФ.
    Паттерн: 2.X.Y — три уровня числовые.
    Пропускаем: 2.X (федеральные округа).
    """
    norm = _normalize_sheet_name(sheet_name)
    return bool(re.fullmatch(r"2\.\d+\.\d+", norm))


# ── Формат по году ─────────────────────────────────────────────────────────────

def _is_old_format(year: int) -> bool:
    """2018-2020: старый формат со смещением столбцов."""
    return year <= 2020


def _col(year: int, offset: int) -> str:
    """
    Имя колонки в Bronze с учётом смещения.
    Старый формат (2018-2020): данные начинаются с col_1.
    Новый формат (2021-2024): данные начинаются с col_0.
    """
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


# ── Нормализация названий регионов (из silver_doshkolka) ──────────────────────

def _normalize_region_name(name: str) -> str:
    if not name:
        return ""
    s = name.strip()
    # Латинские буквы → кириллические (артефакты OCR/копипасты)
    s = s.replace("H", "Н").replace("h", "н")
    s = s.replace("O", "О").replace("o", "о")  # лат. O → кир. О (Oмская)
    s = s.lower()
    s = re.sub(r"город федерального значения\s*", "", s)
    s = re.sub(r"^город\s+", "", s)
    s = re.sub(r"^г\.\s*", "", s)
    s = re.sub(r"^г\s+", "", s)
    s = re.sub(r"\(([^)]+)\)", r" \1", s)
    s = s.replace("-", " ").replace("–", " ")
    s = s.replace("авт.", "автономный ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def build_region_index(cat: SqlCatalog) -> dict[str, str]:
    df = cat.load_table("bronze.region_lookup").scan().to_pandas()
    index: dict[str, str] = {}
    for _, row in df.iterrows():
        key = _normalize_region_name(row["name_variant"])
        index[key] = row["region_code"]
    extras = {
        "ханты мансийский автономный округ": "RU-KHM",
        # 2018-2020: регионы отчитывались вместе с входящими АО
        "архангельская область включая ненецкий автономный округ": "RU-ARK",
        "тюменская область включая ханты мансийский автономный округ югра и ямало ненецкий автономный округ": "RU-TYU",
    }
    for key, code in extras.items():
        if key not in index:
            index[key] = code
    return index


def lookup_region(name: str, index: dict[str, str]) -> str | None:
    key = _normalize_region_name(name)
    if key in index:
        return index[key]
    parts = key.split()
    for trim in range(1, min(4, len(parts))):
        shorter = " ".join(parts[: len(parts) - trim])
        if shorter in index:
            return index[shorter]
    return None


# ── Извлечение названия региона из листа ──────────────────────────────────────

def _extract_region_name(sheet_rows: list[dict], year: int) -> str | None:
    """
    Извлекает название субъекта из заголовочных строк листа.
    2018-2020: строка с row_num=3, col_1
    2021-2024: строка с row_num=1, col_0
    """
    target_row = 3 if _is_old_format(year) else 1
    col = "col_1" if _is_old_format(year) else "col_0"

    for r in sheet_rows:
        if int(r["row_num"]) == target_row:
            val = r.get(col)
            if val and str(val).strip() not in ("", "None", "nan"):
                return str(val).strip()
    return None


# ── Основная трансформация ─────────────────────────────────────────────────────

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


def transform(cat: SqlCatalog) -> list[dict]:
    log.info("Загружаем bronze.excel_tables (Население)...")
    bronze = cat.load_table("bronze.excel_tables").scan().to_pandas()

    # Оставляем только строки из файлов Население
    mask = bronze["source_file"].str.contains(SOURCE_FILTER, na=False)
    bronze = bronze[mask].copy()
    log.info("Строк из Население в Bronze: %d", len(bronze))

    if bronze.empty:
        log.warning("Нет данных Население в Bronze. Сначала выполни population_bronze.")
        return []

    region_index = build_region_index(cat)
    log.info("Индекс регионов: %d записей", len(region_index))

    records = []
    skipped_regions: set[str] = set()
    skipped_sheets: set[str] = set()

    for (source_file, sheet_name, year), group in bronze.groupby(
        ["source_file", "sheet_name", "year"]
    ):
        year = int(year)

        if not _is_region_sheet(str(sheet_name)):
            continue

        sheet_rows = group.to_dict(orient="records")

        region_raw = _extract_region_name(sheet_rows, year)
        if not region_raw:
            skipped_sheets.add(f"{year}/{sheet_name}: нет названия региона")
            continue

        region_code = lookup_region(region_raw, region_index)
        if region_code is None:
            skipped_regions.add(f"{region_raw!r} ({year})")
            continue

        age_col = _age_col(year)

        for r in sorted(sheet_rows, key=lambda x: int(x["row_num"])):
            raw_age = r.get(age_col)
            if not raw_age or str(raw_age).strip() in ("", "None", "nan"):
                continue

            age = _parse_age(str(raw_age).strip())
            if age is None:
                continue

            records.append({
                "region_code":     region_code,
                "region_name_raw": region_raw,
                "year":            year,
                "age":             age,
                "total_both":      _to_int(r.get(_col(year, 1))),
                "total_male":      _to_int(r.get(_col(year, 2))),
                "total_female":    _to_int(r.get(_col(year, 3))),
                "urban_both":      _to_int(r.get(_col(year, 4))),
                "urban_male":      _to_int(r.get(_col(year, 5))),
                "urban_female":    _to_int(r.get(_col(year, 6))),
                "rural_both":      _to_int(r.get(_col(year, 7))),
                "rural_male":      _to_int(r.get(_col(year, 8))),
                "rural_female":    _to_int(r.get(_col(year, 9))),
            })

    if skipped_regions:
        log.info("Регионы без совпадения (%d):", len(skipped_regions))
        for r in sorted(skipped_regions):
            log.info("  SKIP: %s", r)

    if skipped_sheets:
        log.info("Листы без названия региона (%d):", len(skipped_sheets))
        for s in sorted(skipped_sheets):
            log.info("  SKIP: %s", s)

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
