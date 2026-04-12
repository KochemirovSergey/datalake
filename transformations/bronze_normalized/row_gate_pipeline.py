"""
Row gate: агрегирует три измерения и принимает решение ready_for_silver.

Читает:
  bronze_normalized.region        (ok)
  bronze_normalized.region_error  (errors)
  bronze_normalized.year          (ok)
  bronze_normalized.year_error    (errors)
  bronze_normalized.education_level        (ok)
  bronze_normalized.education_level_error  (errors)

Пишет:
  bronze_normalized.row_gate

Логика:
  - Базовое множество строк = union row_ids из region (ok + error)
  - Для каждой строки определяются статусы трёх измерений
  - education_level_required определяется из normalization_config.yaml
  - all_required_ok = True если все required-измерения имеют status="ok"
  - ready_for_silver = all_required_ok

Идемпотентность: row_gate всегда перестраивается заново (overwrite).

Публичный API:
  run(cat, config_path?) → {total, ready, not_ready}
"""

from __future__ import annotations

import logging

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

from transformations.bronze_normalized.config_loader import load_config, DEFAULT_CONFIG_PATH
from transformations.bronze_normalized.common import DimensionStatus, write_to_iceberg

log = logging.getLogger(__name__)

_GATE_SCHEMA = pa.schema([
    pa.field("row_id",                  pa.string(), nullable=False),
    pa.field("source_id",               pa.string(), nullable=False),
    pa.field("region_status",           pa.string(), nullable=False),
    pa.field("year_status",             pa.string(), nullable=False),
    pa.field("education_level_status",  pa.string(), nullable=False),
    pa.field("all_required_ok",         pa.bool_(),  nullable=False),
    pa.field("ready_for_silver",        pa.bool_(),  nullable=False),
    pa.field("rejection_reason",        pa.string(), nullable=False),
])


def _load_df(cat: SqlCatalog, table_name: str):
    """Загружает таблицу в pandas. Возвращает пустой DataFrame при ошибке."""
    import pandas as pd
    try:
        return cat.load_table(table_name).scan().to_pandas()
    except Exception as e:
        log.warning("Не удалось загрузить %s: %s", table_name, e)
        return pd.DataFrame()


def _overwrite_gate(cat: SqlCatalog, records: list[dict]) -> None:
    """Перезаписывает все данные в bronze_normalized.row_gate."""
    tbl = cat.load_table("bronze_normalized.row_gate")

    if records:
        cols = {
            field.name: pa.array([r[field.name] for r in records], field.type)
            for field in _GATE_SCHEMA
        }
        arrow_tbl = pa.table(cols, schema=_GATE_SCHEMA)
        # overwrite — заменяет все снапшоты единым новым файлом
        tbl.overwrite(arrow_tbl)
    else:
        log.warning("row_gate: нет записей для записи")


def run(cat: SqlCatalog, config_path: str = DEFAULT_CONFIG_PATH) -> dict:
    """
    Строит (или перестраивает) bronze_normalized.row_gate.

    Возвращает {total, ready, not_ready}.
    """
    sources = load_config(config_path)

    # Какие источники требуют education_level
    edu_required: dict[str, bool] = {
        s["source_id"]: s.get("education_level", {}).get("required", False)
        for s in sources
    }

    # ── Загрузка всех таблиц измерений ────────────────────────────────────────
    region_ok_df  = _load_df(cat, "bronze_normalized.region")
    region_err_df = _load_df(cat, "bronze_normalized.region_error")
    year_ok_df    = _load_df(cat, "bronze_normalized.year")
    year_err_df   = _load_df(cat, "bronze_normalized.year_error")
    edu_ok_df     = _load_df(cat, "bronze_normalized.education_level")
    edu_err_df    = _load_df(cat, "bronze_normalized.education_level_error")

    log.info(
        "Загружено: region ok=%d err=%d | year ok=%d err=%d | edu ok=%d err=%d",
        len(region_ok_df), len(region_err_df),
        len(year_ok_df),   len(year_err_df),
        len(edu_ok_df),    len(edu_err_df),
    )

    # ── Строим индексы row_id → status ────────────────────────────────────────

    # Region: row_id → source_id (из обеих таблиц)
    row_source: dict[str, str] = {}
    region_ok_set: set[str] = set()
    region_err_map: dict[str, str] = {}

    if not region_ok_df.empty:
        for _, r in region_ok_df.iterrows():
            row_source[r["row_id"]] = r["source_id"]
            region_ok_set.add(r["row_id"])

    if not region_err_df.empty:
        for _, r in region_err_df.iterrows():
            row_source.setdefault(r["row_id"], r["source_id"])
            region_err_map[r["row_id"]] = r["error_type"]

    # Year
    year_ok_set:  set[str]        = set()
    year_err_map: dict[str, str]  = {}
    if not year_ok_df.empty:
        year_ok_set = set(year_ok_df["row_id"].tolist())
    if not year_err_df.empty:
        for _, r in year_err_df.iterrows():
            year_err_map[r["row_id"]] = r["error_type"]

    # Education level
    edu_ok_set:  set[str]        = set()
    edu_err_map: dict[str, str]  = {}
    if not edu_ok_df.empty:
        edu_ok_set = set(edu_ok_df["row_id"].tolist())
    if not edu_err_df.empty:
        for _, r in edu_err_df.iterrows():
            edu_err_map[r["row_id"]] = r["error_type"]

    # ── Полное множество строк = union region ok + error ──────────────────────
    all_row_ids = region_ok_set | set(region_err_map.keys())
    log.info("Всего уникальных row_id для gate: %d", len(all_row_ids))

    if not all_row_ids:
        log.warning("row_gate: нет данных в region-таблицах. Пропускаю.")
        return {"total": 0, "ready": 0, "not_ready": 0}

    # ── Формируем gate-записи ─────────────────────────────────────────────────
    records: list[dict] = []

    for row_id in all_row_ids:
        source_id = row_source.get(row_id, "unknown")

        # Region status
        if row_id in region_ok_set:
            region_status = DimensionStatus.OK
        else:
            region_status = region_err_map.get(row_id, DimensionStatus.MISSING)

        # Year status
        if row_id in year_ok_set:
            year_status = DimensionStatus.OK
        elif row_id in year_err_map:
            year_status = year_err_map[row_id]
        else:
            year_status = DimensionStatus.MISSING

        # Education level status
        is_edu_required = edu_required.get(source_id, False)
        if not is_edu_required:
            edu_status = DimensionStatus.NOT_APPLICABLE
        elif row_id in edu_ok_set:
            edu_status = DimensionStatus.OK
        elif row_id in edu_err_map:
            edu_status = edu_err_map[row_id]
        else:
            edu_status = DimensionStatus.MISSING

        # all_required_ok
        required_statuses = [region_status, year_status]
        if is_edu_required:
            required_statuses.append(edu_status)

        all_ok = all(s == DimensionStatus.OK for s in required_statuses)

        # rejection_reason — первые провалы
        failures: list[str] = []
        if region_status != DimensionStatus.OK:
            failures.append(f"region={region_status}")
        if year_status != DimensionStatus.OK:
            failures.append(f"year={year_status}")
        if is_edu_required and edu_status != DimensionStatus.OK:
            failures.append(f"education_level={edu_status}")

        records.append({
            "row_id":                 row_id,
            "source_id":              source_id,
            "region_status":          region_status,
            "year_status":            year_status,
            "education_level_status": edu_status,
            "all_required_ok":        all_ok,
            "ready_for_silver":       all_ok,
            "rejection_reason":       "; ".join(failures),
        })

    _overwrite_gate(cat, records)

    ready     = sum(1 for r in records if r["ready_for_silver"])
    not_ready = len(records) - ready

    log.info(
        "row_gate: total=%d ready=%d not_ready=%d (%.1f%% готово к Silver)",
        len(records), ready, not_ready,
        100 * ready / len(records) if records else 0,
    )
    return {"total": len(records), "ready": ready, "not_ready": not_ready}
