"""
Нормализация education_level: → bronze_normalized.education_level / education_level_error.

Поддерживаемые location (определяются полем education_level.location в конфиге):

  «source»            — фиксированный код для всего источника (doshkolka → 1.1)
  «column_name»       — код из значения column_name (ОО)
  «row_name»          — код из значения row_name (ДПО)
  «column_metadata_1» — код из значения column_metadata_1 (ВПО)
  «column_metadata_2» — код из значения column_metadata_2 (СПО)

Источники данных:
  Excel (table_type=excel | не указан) — читает из bronze.excel_tables
  PostgreSQL (table_type=postgres)     — читает из bronze.<table_name>

Публичный API:
  run(cat, config_path?) → dict{source_id → stats}
"""

from __future__ import annotations

import csv
import logging
import os
from typing import Any

import pyarrow as pa
import pandas as pd
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

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MAPPING_CSV_PATH = os.path.join(BASE_DIR, "data", "education_level_mapping.csv")

# ── Схемы PyArrow (обновлённые per ТЗ) ─────────────────────────────────────────

_OK_SCHEMA = pa.schema([
    pa.field("row_id",           pa.string(), nullable=False),
    pa.field("source_id",        pa.string(), nullable=False),
    pa.field("source_file",      pa.string(), nullable=False),
    pa.field("level_code",       pa.string(), nullable=False),
    pa.field("level_label",      pa.string(), nullable=False),
    pa.field("program_code",     pa.string(), nullable=True),
    pa.field("program_label",    pa.string(), nullable=True),
    pa.field("match_field",      pa.string(), nullable=False),
    pa.field("match_value",      pa.string(), nullable=False),
    pa.field("status",           pa.string(), nullable=False),
])

_ERR_SCHEMA = pa.schema([
    pa.field("row_id",           pa.string(), nullable=False),
    pa.field("source_id",        pa.string(), nullable=False),
    pa.field("source_file",      pa.string(), nullable=False),
    pa.field("match_field",      pa.string(), nullable=True),
    pa.field("match_value",      pa.string(), nullable=True),
    pa.field("error_type",       pa.string(), nullable=False),
    pa.field("error_details",    pa.string(), nullable=True),
])


# ── Утилиты ───────────────────────────────────────────────────────────────────

def _already_processed(cat: SqlCatalog, source_table: str) -> bool:
    """True если bronze_normalized.education_level уже содержит данные для source_table."""
    try:
        tbl = cat.load_table("bronze_normalized.education_level")
        arrow = tbl.scan(selected_fields=("source_id",)).to_arrow()
        if len(arrow) == 0:
            return False
        return source_table in arrow["source_id"].to_pylist()
    except Exception:
        return False


def _load_mapping_csv(path: str = MAPPING_CSV_PATH) -> dict[tuple[str, str, str], dict]:
    """
    Загружает CSV-справочник education_level_mapping.csv.
    Возвращает dict: (source_table, match_field, match_value) → row_dict
    """
    mapping: dict[tuple[str, str, str], dict] = {}
    if not os.path.exists(path):
        log.warning("Mapping CSV не найден: %s", path)
        return mapping

    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["source_table"], row["match_field"], row["match_value"])
            mapping[key] = row
    log.info("Загружен mapping: %d записей", len(mapping))
    return mapping


def _make_ok(row_id: str, source_id: str, source_file: str, level_code: str, level_label: str,
             program_code: str | None, program_label: str | None,
             match_field: str, match_value: str) -> dict:
    return {
        "row_id":        row_id,
        "source_id":     source_id,
        "source_file":   source_file,
        "level_code":    level_code,
        "level_label":   level_label,
        "program_code":  program_code,
        "program_label": program_label,
        "match_field":   match_field,
        "match_value":   match_value,
        "status":        "ok",
    }


def _make_err(row_id: str, source_id: str, source_file: str, match_field: str | None,
              match_value: str | None, error_type: str, error_details: str | None = None) -> dict:
    return {
        "row_id":        row_id,
        "source_id":     source_id,
        "source_file":   source_file,
        "match_field":   match_field,
        "match_value":   match_value,
        "error_type":    error_type,
        "error_details": error_details,
    }


def _check_filters(row: dict, filters: dict | None) -> tuple[bool, str | None]:
    """
    Проверяет фильтры из конфига.
    Возвращает (passes, error_details).
    """
    if not filters:
        return True, None

    for field, allowed_values in filters.items():
        row_value = str(row.get(field) or "").strip()
        if row_value not in allowed_values:
            return False, f"filter_not_matched: {field}={row_value!r} not in {allowed_values}"
    return True, None


def _lookup_education_level(
    source_table: str,
    match_field: str,
    match_value: str,
    mapping: dict[tuple[str, str, str], dict],
) -> dict | None:
    """Ищет запись в mapping по (source_table, match_field, match_value)."""
    key = (source_table, match_field, match_value)
    return mapping.get(key)


# ── Пайплайн: фиксированный код по источнику (location=source, Excel) ─────────

def _run_source_pipeline(
    df: pd.DataFrame,
    source_id: str,
    source_filter: str,
    edu_cfg: dict,
    edu_lookup: dict[str, dict],
) -> tuple[list[dict], list[dict]]:
    """Все data-строки источника получают фиксированный resolved_code."""
    ok_records: list[dict] = []

    resolved_code: str = edu_cfg["resolved_code"]
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
                _make_ok(row_id, source_id, str(source_file), resolved_code, label, None, None, "source", "")
            )

    return ok_records, []


# ── Пайплайн: lookup из PostgreSQL (новая логика per ТЗ) ──────────────────────

def _run_lookup_postgres_pipeline(
    cat: SqlCatalog,
    source_id: str,
    table_name: str,
    edu_cfg: dict,
    mapping: dict[tuple[str, str, str], dict],
) -> tuple[list[dict], list[dict]]:
    """
    Пайплайн для PostgreSQL-источников с lookup из CSV.
    Поддерживает location: column_name, row_name, column_metadata_1, column_metadata_2
    """
    ok_records:  list[dict] = []
    err_records: list[dict] = []
    iceberg_name = f"bronze.{table_name}"

    match_field: str = edu_cfg.get("match_field", "")
    filters: dict | None = edu_cfg.get("filters")

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

        # 1. Проверка unsupported_source — конфиг уже есть, так что пропускаем
        # (проверка была бы раньше, если source_id не найден)

        # 2. Применяем фильтры
        passes, filter_error = _check_filters(row_dict, filters)
        if not passes:
            err_records.append(
                _make_err(row_id, source_id, table_name, match_field, None,
                          "filter_not_matched", filter_error)
            )
            continue

        # 3. Проверяем наличие match_field
        if match_field not in row_dict:
            err_records.append(
                _make_err(row_id, source_id, table_name, match_field, None,
                          "match_field_missing", f"Column {match_field} not found in bronze table")
            )
            continue

        # 4. Проверяем значение match_field
        match_value = str(row_dict.get(match_field) or "").strip()
        if not match_value:
            err_records.append(
                _make_err(row_id, source_id, table_name, match_field, match_value,
                          "match_value_empty", "Match field value is empty or null")
            )
            continue

        # 5. Ищем в справочнике
        lookup_result = _lookup_education_level(table_name, match_field, match_value, mapping)
        if lookup_result is None:
            err_records.append(
                _make_err(row_id, source_id, table_name, match_field, match_value,
                          "lookup_not_found",
                          f"No mapping found for {table_name}/{match_field}={match_value!r}")
            )
            continue

        # 6. Успех — записываем в ok
        ok_records.append(
            _make_ok(
                row_id, source_id, table_name,
                lookup_result["level_code"],
                lookup_result["level_label"],
                lookup_result.get("program_code") or None,
                lookup_result.get("program_label") or None,
                match_field,
                match_value,
            )
        )

    return ok_records, err_records


# ── Точка входа ────────────────────────────────────────────────────────────────

def run(cat: SqlCatalog, config_path: str = DEFAULT_CONFIG_PATH) -> dict:
    """
    Запускает нормализацию education_level для всех источников с education_level конфигом.
    Идемпотентна: пропускает источники, которые уже обработаны.

    Возвращает dict: source_id → {total, ok_count, error_count} | {skipped: True}.
    """
    sources = load_config(config_path)
    edu_lookup = build_edu_lookup(cat)
    mapping = _load_mapping_csv()
    log.info("Справочник education_level: %d кодов, mapping: %d записей",
             len(edu_lookup), len(mapping))

    # Excel-данные загружаем один раз
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
        table_name = src.get("table_name", "")

        # Нет конфига education_level — пропускаем
        if not edu_cfg:
            log.info("[%s] education_level не сконфигурирован, пропускаю", source_id)
            continue

        # required=False — пропускаем
        if not edu_cfg.get("required", False):
            log.info("[%s] education_level.required=false, пропускаю", source_id)
            continue

        # Определяем source_table для проверки уже обработанного
        source_table_for_check = table_name if table_type == "postgres" else source_id
        if _already_processed(cat, source_table_for_check):
            log.info("[%s] education_level уже обработан, пропускаю", source_id)
            stats[source_id] = {"skipped": True}
            continue

        location = edu_cfg.get("location", "")
        source_filter = src.get("source_filter", "")

        ok_records:  list[dict] = []
        err_records: list[dict] = []

        if location == "source" and table_type == "excel":
            ok_records, err_records = _run_source_pipeline(
                bronze_excel, source_id, source_filter, edu_cfg, edu_lookup
            )

        elif location in ("column_name", "row_name", "column_metadata_1", "column_metadata_2") \
                and table_type == "postgres":
            if not table_name:
                log.error("[%s] table_type=postgres, но table_name не задан", source_id)
                continue
            ok_records, err_records = _run_lookup_postgres_pipeline(
                cat, source_id, table_name, edu_cfg, mapping
            )

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
