"""
Нормализация education_level: → bronze_normalized.education_level / education_level_error.

Три режима (определяются полем education_level.location в конфиге):

  «source»          — фиксированный код для всего источника (doshkolka → 1.1)
  «column_metadata» — код из значений указанных колонок (СПО: column_metadata_2)
  «stub»            — заглушка: ОО и ВО, документация не получена → not_found

Источники данных:
  Excel (table_type=excel | не указан) — читает из bronze.excel_tables
  PostgreSQL (table_type=postgres)     — читает из bronze.<table_name>

Публичный API:
  run(cat, config_path?) → dict{source_id → stats}
"""

from __future__ import annotations

import logging
from typing import Any

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

from transformations.bronze_normalized.config_loader import load_config, DEFAULT_CONFIG_PATH
from transformations.bronze_normalized.common import (
    DimensionStatus,
    build_edu_lookup,
    find_data_start,
    is_numbering_row,
    make_postgres_row_id,
    make_row_id,
    write_to_iceberg,
)

log = logging.getLogger(__name__)

# ── Схемы PyArrow ─────────────────────────────────────────────────────────────

_OK_SCHEMA = pa.schema([
    pa.field("row_id",           pa.string(), nullable=False),
    pa.field("source_id",        pa.string(), nullable=False),
    pa.field("raw_signal",       pa.string(), nullable=True),
    pa.field("resolved_code",    pa.string(), nullable=False),
    pa.field("resolved_label",   pa.string(), nullable=False),
    pa.field("rule_id",          pa.string(), nullable=False),
    pa.field("resolution_scope", pa.string(), nullable=False),
])

_ERR_SCHEMA = pa.schema([
    pa.field("row_id",           pa.string(), nullable=False),
    pa.field("source_id",        pa.string(), nullable=False),
    pa.field("raw_signal",       pa.string(), nullable=True),
    pa.field("rule_id",          pa.string(), nullable=False),
    pa.field("resolution_scope", pa.string(), nullable=False),
    pa.field("error_type",       pa.string(), nullable=False),
    pa.field("error_reason",     pa.string(), nullable=True),
])


# ── Утилиты ───────────────────────────────────────────────────────────────────

def _already_processed(cat: SqlCatalog, source_id: str) -> bool:
    """True если bronze_normalized.education_level уже содержит данные для source_id."""
    try:
        tbl = cat.load_table("bronze_normalized.education_level")
        arrow = tbl.scan(selected_fields=("source_id",)).to_arrow()
        if len(arrow) == 0:
            return False
        return source_id in arrow["source_id"].to_pylist()
    except Exception:
        return False


def _make_ok(row_id: str, source_id: str, raw_signal: str | None,
             resolved_code: str, label: str, rule_id: str, scope: str) -> dict:
    return {
        "row_id":           row_id,
        "source_id":        source_id,
        "raw_signal":       raw_signal,
        "resolved_code":    resolved_code,
        "resolved_label":   label,
        "rule_id":          rule_id,
        "resolution_scope": scope,
    }


def _make_err(row_id: str, source_id: str, raw_signal: str | None,
              rule_id: str, scope: str, error_type: str, error_reason: str | None = None) -> dict:
    return {
        "row_id":           row_id,
        "source_id":        source_id,
        "raw_signal":       raw_signal,
        "rule_id":          rule_id,
        "resolution_scope": scope,
        "error_type":       error_type,
        "error_reason":     error_reason,
    }


def _resolve_column_metadata(
    row: dict,
    edu_cfg: dict,
) -> tuple[str | None, str | None, str]:
    """
    Извлекает education_level из колонок строки по правилу column_metadata.

    Возвращает (resolved_code, raw_signal, error_type).
    error_type = None при успехе.
    """
    axes: list[dict] = edu_cfg.get("metadata_axes", [])
    fallback_code: str | None = edu_cfg.get("fallback_code")

    for axis in axes:
        col = axis["column"]
        mapping: dict[str, str] = axis.get("mapping", {})
        raw = str(row.get(col) or "").strip()
        if raw and raw in mapping:
            return mapping[raw], raw, None

    # Нет совпадения
    if fallback_code:
        return fallback_code, None, None

    return None, None, DimensionStatus.NOT_FOUND


# ── Пайплайн A: фиксированный код по источнику (location=source, Excel) ────────

def _run_source_pipeline(
    df,                        # pandas DataFrame из bronze.excel_tables
    source_id: str,
    source_filter: str,
    edu_cfg: dict,
    edu_lookup: dict[str, dict],
) -> tuple[list[dict], list[dict]]:
    """Все data-строки источника получают фиксированный resolved_code."""
    ok_records: list[dict] = []

    resolved_code: str = edu_cfg["resolved_code"]
    rule_id: str = edu_cfg.get("rule_id", f"R_source_{source_id}")
    label = edu_lookup.get(resolved_code, {}).get("label_ru", resolved_code)

    mask = df["source_file"].str.contains(source_filter, na=False)
    src_df = df[mask]

    if src_df.empty:
        log.warning("[%s] Нет данных в bronze.excel_tables для фильтра %r", source_id, source_filter)
        return [], []

    for (source_file, sheet_name, year), group in src_df.groupby(
        ["source_file", "sheet_name", "year"]
    ):
        sheet_rows = group.to_dict(orient="records")
        data_start = find_data_start(sheet_rows)
        for row in sheet_rows:
            if int(row["row_num"]) < data_start:
                continue
            col_0 = str(row.get("col_0") or "").strip()
            if not col_0 or col_0 in ("nan", "None"):
                continue
            row_id = make_row_id(str(source_file), str(sheet_name), int(row["row_num"]))
            ok_records.append(
                _make_ok(row_id, source_id, None, resolved_code, label, rule_id, "source")
            )

    return ok_records, []


# ── Пайплайн B: column_metadata из Excel ──────────────────────────────────────

def _run_col_meta_excel_pipeline(
    df,
    source_id: str,
    source_filter: str,
    edu_cfg: dict,
    edu_lookup: dict[str, dict],
) -> tuple[list[dict], list[dict]]:
    """Извлекает education_level из значений колонок Excel-строк."""
    ok_records:  list[dict] = []
    err_records: list[dict] = []

    mask = df["source_file"].str.contains(source_filter, na=False)
    src_df = df[mask]

    if src_df.empty:
        log.warning("[%s] Нет данных в bronze.excel_tables", source_id)
        return [], []

    for (source_file, sheet_name, year), group in src_df.groupby(
        ["source_file", "sheet_name", "year"]
    ):
        sheet_rows = group.to_dict(orient="records")
        data_start = find_data_start(sheet_rows)
        for row in sheet_rows:
            if int(row["row_num"]) < data_start:
                continue
            col_0 = str(row.get("col_0") or "").strip()
            if not col_0 or col_0 in ("nan", "None"):
                continue
            row_id = make_row_id(str(source_file), str(sheet_name), int(row["row_num"]))
            resolved_code, raw_signal, error_type = _resolve_column_metadata(row, edu_cfg)
            if error_type is None and resolved_code:
                label = edu_lookup.get(resolved_code, {}).get("label_ru", resolved_code)
                ok_records.append(
                    _make_ok(row_id, source_id, raw_signal, resolved_code, label,
                             "R4_column_metadata", "column_metadata")
                )
            else:
                err_records.append(
                    _make_err(row_id, source_id, raw_signal,
                              "R4_column_metadata", "column_metadata",
                              error_type or DimensionStatus.NOT_FOUND)
                )

    return ok_records, err_records


# ── Пайплайн C: column_metadata из PostgreSQL-таблицы ─────────────────────────

def _run_col_meta_postgres_pipeline(
    cat: SqlCatalog,
    source_id: str,
    table_name: str,
    edu_cfg: dict,
    edu_lookup: dict[str, dict],
) -> tuple[list[dict], list[dict]]:
    """Читает bronze.<table_name> и извлекает education_level из колонок."""
    ok_records:  list[dict] = []
    err_records: list[dict] = []
    iceberg_name = f"bronze.{table_name}"

    try:
        tbl = cat.load_table(iceberg_name)
    except Exception as e:
        log.warning("[%s] Таблица %s не найдена: %s", source_id, iceberg_name, e)
        return [], []

    df = tbl.scan().to_pandas()
    if df.empty:
        log.warning("[%s] Таблица %s пуста", source_id, iceberg_name)
        return [], []

    for _, row in df.iterrows():
        row_num = int(row.get("row_num", 0))
        row_id = make_postgres_row_id(table_name, row_num)
        row_dict = row.to_dict()

        resolved_code, raw_signal, error_type = _resolve_column_metadata(row_dict, edu_cfg)
        if error_type is None and resolved_code:
            label = edu_lookup.get(resolved_code, {}).get("label_ru", resolved_code)
            ok_records.append(
                _make_ok(row_id, source_id, raw_signal, resolved_code, label,
                         "R4_column_metadata", "column_metadata")
            )
        else:
            err_records.append(
                _make_err(row_id, source_id, raw_signal,
                          "R4_column_metadata", "column_metadata",
                          error_type or DimensionStatus.NOT_FOUND)
            )

    return ok_records, err_records


# ── Пайплайн D: заглушка для ОО/ВО ────────────────────────────────────────────

def _run_stub_pipeline(
    cat: SqlCatalog,
    source_id: str,
    table_name: str,
    edu_cfg: dict,
) -> tuple[list[dict], list[dict]]:
    """Помечает все строки как not_found (документация не получена)."""
    err_records: list[dict] = []
    iceberg_name = f"bronze.{table_name}"

    note: str = edu_cfg.get("note", "stub: документация не получена")

    try:
        tbl = cat.load_table(iceberg_name)
        df = tbl.scan(selected_fields=("row_num",)).to_arrow().to_pydict()
        row_nums = df.get("row_num", [])
    except Exception as e:
        log.warning("[%s] Таблица %s недоступна: %s", source_id, iceberg_name, e)
        return [], []

    for row_num in row_nums:
        row_id = make_postgres_row_id(table_name, int(row_num))
        err_records.append(
            _make_err(row_id, source_id, None, "R_stub", "stub",
                      DimensionStatus.NOT_FOUND, note)
        )

    return [], err_records


# ── Точка входа ────────────────────────────────────────────────────────────────

def run(cat: SqlCatalog, config_path: str = DEFAULT_CONFIG_PATH) -> dict:
    """
    Запускает нормализацию education_level для всех источников с education_level конфигом.
    Идемпотентна: пропускает источники, которые уже обработаны.

    Возвращает dict: source_id → {total, ok_count, error_count} | {skipped: True}.
    """
    sources = load_config(config_path)
    edu_lookup = build_edu_lookup(cat)
    log.info("Справочник education_level: %d кодов", len(edu_lookup))

    # Excel-данные загружаем один раз
    import pandas as pd
    try:
        bronze_excel = cat.load_table("bronze.excel_tables").scan().to_pandas()
        log.info("bronze.excel_tables: %d строк", len(bronze_excel))
    except Exception as e:
        log.warning("Не удалось загрузить bronze.excel_tables: %s", e)
        bronze_excel = pd.DataFrame()

    stats: dict[str, dict] = {}

    for src in sources:
        source_id  = src["source_id"]
        edu_cfg    = src.get("education_level")
        table_type = src.get("table_type", "excel")

        # Нет конфига education_level или required=False — пропускаем
        if not edu_cfg:
            log.info("[%s] education_level не сконфигурирован, пропускаю", source_id)
            continue

        if not edu_cfg.get("required", False):
            log.info("[%s] education_level.required=false, пропускаю", source_id)
            continue

        if _already_processed(cat, source_id):
            log.info("[%s] education_level уже обработан, пропускаю", source_id)
            stats[source_id] = {"skipped": True}
            continue

        location     = edu_cfg.get("location", "stub")
        source_filter = src.get("source_filter", "")

        ok_records:  list[dict] = []
        err_records: list[dict] = []

        if location == "source" and table_type == "excel":
            ok_records, err_records = _run_source_pipeline(
                bronze_excel, source_id, source_filter, edu_cfg, edu_lookup
            )

        elif location == "column_metadata" and table_type == "excel":
            ok_records, err_records = _run_col_meta_excel_pipeline(
                bronze_excel, source_id, source_filter, edu_cfg, edu_lookup
            )

        elif location == "column_metadata" and table_type == "postgres":
            table_name = src.get("table_name", "")
            if not table_name:
                log.error("[%s] table_type=postgres, но table_name не задан", source_id)
                continue
            ok_records, err_records = _run_col_meta_postgres_pipeline(
                cat, source_id, table_name, edu_cfg, edu_lookup
            )

        elif location == "stub":
            table_name = src.get("table_name", "")
            if table_type == "postgres" and table_name:
                ok_records, err_records = _run_stub_pipeline(
                    cat, source_id, table_name, edu_cfg
                )
            else:
                log.info("[%s] stub location, table_name не задан или не postgres — пропускаю", source_id)
                continue

        else:
            log.warning("[%s] Неизвестная комбинация location=%r table_type=%r — пропускаю",
                        source_id, location, table_type)
            continue

        write_to_iceberg(cat, "bronze_normalized.education_level",  ok_records,  _OK_SCHEMA)
        write_to_iceberg(cat, "bronze_normalized.education_level_error", err_records, _ERR_SCHEMA)

        total = len(ok_records) + len(err_records)
        log.info(
            "[%s] education_level: ok=%d error=%d (%.1f%% покрыто)",
            source_id, len(ok_records), len(err_records),
            100 * len(ok_records) / total if total else 0,
        )
        stats[source_id] = {
            "total":       total,
            "ok_count":    len(ok_records),
            "error_count": len(err_records),
        }

    return stats
