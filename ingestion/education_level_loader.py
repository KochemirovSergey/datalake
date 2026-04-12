"""
Загрузчик справочника education_level_lookup.csv → bronze.education_level_lookup.

Идемпотентен: повторный запуск не дублирует данные.

Публичный API:
  run(csv_path?) → int  — количество загруженных записей (0 если уже загружено)
"""

import csv
import logging
import os

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH  = os.path.join(BASE_DIR, "education_level_lookup.csv")
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

_SCHEMA = pa.schema([
    pa.field("code",               pa.string(), nullable=False),
    pa.field("label_ru",           pa.string(), nullable=False),
    pa.field("kind_code",          pa.string(), nullable=True),
    pa.field("kind_label",         pa.string(), nullable=True),
    pa.field("level_code",         pa.string(), nullable=True),
    pa.field("level_label",        pa.string(), nullable=True),
    pa.field("program_code",       pa.string(), nullable=True),
    pa.field("program_label",      pa.string(), nullable=True),
    pa.field("granularity_level",  pa.string(), nullable=False),
])


def _get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )


def run(csv_path: str = CSV_PATH) -> int:
    """
    Загружает CSV-справочник в bronze.education_level_lookup.
    Возвращает кол-во загруженных записей (0 если уже загружено).
    """
    catalog = _get_catalog()
    tbl = catalog.load_table("bronze.education_level_lookup")

    # Идемпотентность
    arrow = tbl.scan(selected_fields=("code",)).to_arrow()
    if len(arrow) > 0:
        log.info(
            "education_level_lookup уже загружен (%d записей), пропускаю", len(arrow)
        )
        return 0

    records: list[dict] = []
    with open(csv_path, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            code = (row.get("code") or "").strip()
            if not code:
                continue
            records.append({
                "code":               code,
                "label_ru":           (row.get("label_ru")          or "").strip() or None,
                "kind_code":          (row.get("kind_code")         or "").strip() or None,
                "kind_label":         (row.get("kind_label")        or "").strip() or None,
                "level_code":         (row.get("level_code")        or "").strip() or None,
                "level_label":        (row.get("level_label")       or "").strip() or None,
                "program_code":       (row.get("program_code")      or "").strip() or None,
                "program_label":      (row.get("program_label")     or "").strip() or None,
                "granularity_level":  (row.get("granularity_level") or "").strip() or None,
            })

    if not records:
        log.warning("Нет записей в %s", csv_path)
        return 0

    # Обязательные поля — замена None на пустую строку для not-null
    for r in records:
        if r["label_ru"] is None:
            r["label_ru"] = ""
        if r["granularity_level"] is None:
            r["granularity_level"] = ""

    cols = {
        field.name: pa.array([r[field.name] for r in records], field.type)
        for field in _SCHEMA
    }
    tbl.append(pa.table(cols, schema=_SCHEMA))

    log.info("Загружено %d записей education_level_lookup", len(records))
    return len(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    n = run()
    print(f"Загружено: {n}")
