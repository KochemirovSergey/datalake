"""
JSON / CSV → pd.DataFrame extractor.
Только читает данные. Не пишет в БД.

Добавляет lineage-колонки:
  _etl_loaded_at — timestamp загрузки (UTC)
  _source_file   — относительный путь к файлу
  _sheet_name    — None (не применимо)
  _row_number    — порядковый номер строки в источнике
"""

import json
import os
from datetime import datetime, timezone

import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REGIONS_FILE = os.path.join(BASE_DIR, "data", "regions.json")
CODE_PROGRAMM_FILE = os.path.join(BASE_DIR, "data", "programma_educationis_codicem.csv")


def read_regions() -> pd.DataFrame:
    """
    Читает data/regions.json.
    Возвращает DataFrame: canonical_name, region_code, name_variant, is_alias.
    """
    loaded_at = datetime.now(tz=timezone.utc)

    with open(REGIONS_FILE, encoding="utf-8") as f:
        data = json.load(f)

    regions: dict[str, str] = data["regions"]
    typo_fixes: dict[str, str] = data["typoFixes"]
    quarantine: dict[str, str] = data.get("quarantine", {})

    aliases_by_canonical: dict[str, list[str]] = {}
    for slug, canonical in typo_fixes.items():
        aliases_by_canonical.setdefault(canonical, []).append(slug.replace("_", " "))

    records: list[dict] = []
    row_number = 0

    for canonical_name, code in regions.items():
        records.append({
            "_etl_loaded_at": loaded_at,
            "_source_file":   "data/regions.json",
            "_sheet_name":    None,
            "_row_number":    row_number,
            "name_variant":   canonical_name,
            "canonical_name": canonical_name,
            "region_code":    code,
            "is_alias":       False,
        })
        row_number += 1

    for canonical_name, aliases in aliases_by_canonical.items():
        code = regions.get(canonical_name)
        if not code:
            continue
        for alias in aliases:
            records.append({
                "_etl_loaded_at": loaded_at,
                "_source_file":   "data/regions.json",
                "_sheet_name":    None,
                "_row_number":    row_number,
                "name_variant":   alias,
                "canonical_name": canonical_name,
                "region_code":    code,
                "is_alias":       True,
            })
            row_number += 1

    for variant, _reason in quarantine.items():
        records.append({
            "_etl_loaded_at": loaded_at,
            "_source_file":   "data/regions.json",
            "_sheet_name":    None,
            "_row_number":    row_number,
            "name_variant":   variant,
            "canonical_name": variant,
            "region_code":    "SKIP",
            "is_alias":       True,
        })
        row_number += 1

    return pd.DataFrame(records)


def read_code_programm() -> pd.DataFrame:
    """
    Читает data/programma_educationis_codicem.csv.
    Возвращает DataFrame: code, name.
    """
    loaded_at = datetime.now(tz=timezone.utc)

    df = pd.read_csv(
        CODE_PROGRAMM_FILE,
        sep=";",
        header=None,
        names=["code", "name"],
        encoding="utf-8",
        dtype=str,
    )

    df.insert(0, "_etl_loaded_at", loaded_at)
    df.insert(1, "_source_file", "data/programma_educationis_codicem.csv")
    df.insert(2, "_sheet_name", None)
    df.insert(3, "_row_number", range(len(df)))

    return df
