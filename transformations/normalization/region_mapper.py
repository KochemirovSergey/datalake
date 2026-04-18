"""
Маппинг названий регионов на region_code.

Читает data/regions.json напрямую (статический справочник, не Dagster-ассет).
Результат кешируется на уровне модуля — файл читается один раз за процесс.
"""

import json
import os

from transformations.bronze_normalized.region_normalizer import (
    normalize_region_name,
    lookup_region,
)

_REGIONS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "data", "regions.json",
)

_LOOKUP_CACHE: dict[str, str] | None = None


def build_lookup() -> dict[str, str]:
    """
    Строит индекс normalized_name → region_code из data/regions.json.
    Результат кешируется — повторные вызовы не перечитывают файл.
    """
    global _LOOKUP_CACHE
    if _LOOKUP_CACHE is not None:
        return _LOOKUP_CACHE

    with open(_REGIONS_FILE, encoding="utf-8") as f:
        data = json.load(f)

    regions: dict[str, str] = data["regions"]
    typo_fixes: dict[str, str] = data["typoFixes"]
    quarantine: dict[str, str] = data.get("quarantine", {})

    index: dict[str, str] = {}

    for canonical_name, code in regions.items():
        key = normalize_region_name(canonical_name)
        if key:
            index[key] = code

    for slug, canonical_name in typo_fixes.items():
        variant = slug.replace("_", " ")
        key = normalize_region_name(variant)
        code = regions.get(canonical_name)
        if key and code and key not in index:
            index[key] = code

    for variant in quarantine:
        key = normalize_region_name(variant)
        if key:
            index[key] = "SKIP"

    _LOOKUP_CACHE = index
    return index


def map_series(
    names: "pd.Series",
    lookup: dict[str, str],
) -> tuple[list[str | None], list[str]]:
    """
    Маппирует серию строк-имён регионов на коды.

    Возвращает:
      codes     — список region_code (None = нераспознан, SKIP = агрегат)
      unmatched — список нераспознанных имён (для логирования)
    """
    codes: list[str | None] = []
    unmatched: list[str] = []

    for name in names:
        raw = str(name) if name and str(name) not in ("nan", "None") else ""
        code = lookup_region(raw, lookup) if raw else None
        codes.append(code)
        if code is None and raw:
            unmatched.append(raw)

    return codes, unmatched
