"""
Загружает data/regions.json в bronze.region_lookup.

Структура результирующей таблицы:
  name_variant   — вариант названия (каноническое или алиас/опечатка)
  canonical_name — стандартное название (ключ из regions{})
  region_code    — ISO-код региона (RU-MOW и т.д.)
  is_alias       — True если name_variant это алиас/опечатка, False если каноническое
"""

import json
import os

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")
REGIONS_FILE = os.path.join(BASE_DIR, "data", "regions.json")


def run() -> int:
    with open(REGIONS_FILE, encoding="utf-8") as f:
        data = json.load(f)

    regions: dict[str, str] = data["regions"]       # canonical_name → code
    typo_fixes: dict[str, str] = data["typoFixes"]  # slug_alias → canonical_name
    # quarantine: нормализованное_название → описание причины.
    # Эти варианты — агрегаты нескольких субъектов РФ; в данных они дублируют
    # информацию, которая уже учтена через отдельные субъекты.
    quarantine: dict[str, str] = data.get("quarantine", {})

    # Инвертируем typoFixes: canonical_name → [slug_aliases]
    aliases_by_canonical: dict[str, list[str]] = {}
    for slug, canonical in typo_fixes.items():
        aliases_by_canonical.setdefault(canonical, []).append(
            slug.replace("_", " ")  # slug → читаемый вид
        )

    records = []

    # Канонические записи (is_alias=False)
    for canonical_name, code in regions.items():
        records.append({
            "name_variant":   canonical_name,
            "canonical_name": canonical_name,
            "region_code":    code,
            "is_alias":       False,
        })

    # Алиасы/опечатки (is_alias=True)
    for canonical_name, aliases in aliases_by_canonical.items():
        code = regions.get(canonical_name)
        if not code:
            print(f"  Предупреждение: нет кода для '{canonical_name}', пропускаю алиасы")
            continue
        for alias in aliases:
            records.append({
                "name_variant":   alias,
                "canonical_name": canonical_name,
                "region_code":    code,
                "is_alias":       True,
            })

    # Quarantine-записи — агрегаты нескольких субъектов (is_alias=True, region_code="SKIP")
    for variant, reason in quarantine.items():
        print(f"  quarantine: {variant!r} → SKIP ({reason})")
        records.append({
            "name_variant":   variant,
            "canonical_name": variant,
            "region_code":    "SKIP",
            "is_alias":       True,
        })

    # Запись в Iceberg
    cat = SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )

    tbl = cat.load_table("bronze.region_lookup")

    # Идемпотентность: если данные уже есть — не дублируем
    existing = tbl.scan().to_arrow()
    if len(existing) > 0:
        print(f"Уже загружено {len(existing)} записей, пропускаю")
        return 0

    pa_schema = pa.schema([
        pa.field("name_variant",   pa.string(), nullable=False),
        pa.field("canonical_name", pa.string(), nullable=False),
        pa.field("region_code",    pa.string(), nullable=False),
        pa.field("is_alias",       pa.bool_(),  nullable=False),
    ])

    arrow_tbl = pa.table(
        {
            "name_variant":   pa.array([r["name_variant"]   for r in records], pa.string()),
            "canonical_name": pa.array([r["canonical_name"] for r in records], pa.string()),
            "region_code":    pa.array([r["region_code"]    for r in records], pa.string()),
            "is_alias":       pa.array([r["is_alias"]       for r in records], pa.bool_()),
        },
        schema=pa_schema,
    )

    tbl.append(arrow_tbl)
    return len(records)


if __name__ == "__main__":
    count = run()
    print(f"Загружено {count} записей в bronze.region_lookup")
