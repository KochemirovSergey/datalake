"""
Нормализация региона: Bronze → bronze_normalized.region / region_error.

Поддерживает два режима, определяемых полем region.location в конфиге:

  «row»        — регион в колонке каждой строки данных (doshkolka).
  «meta_sheet» — регион в заголовке листа, наследуется всеми строками (naselenie).

Публичный API:
  run(cat, config_path) → (total, ok_count, error_count)
"""

import logging
import re

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

from transformations.bronze_normalized.config_loader import load_config, DEFAULT_CONFIG_PATH
from transformations.bronze_normalized.region_normalizer import (
    SKIP,
    build_region_index,
    normalize_region_name,
    lookup_region,
)

log = logging.getLogger(__name__)

# ── Схемы PyArrow ─────────────────────────────────────────────────────────────

_OK_SCHEMA = pa.schema([
    pa.field("row_id",           pa.string(), nullable=False),
    pa.field("source_id",        pa.string(), nullable=False),
    pa.field("source_file",      pa.string(), nullable=False),
    pa.field("sheet_name",       pa.string(), nullable=False),
    pa.field("year",             pa.int32(),  nullable=False),
    pa.field("row_num",          pa.int32(),  nullable=False),
    pa.field("region_raw",       pa.string(), nullable=False),
    pa.field("region_code",      pa.string(), nullable=False),
    pa.field("resolution_scope", pa.string(), nullable=False),
    pa.field("normalized_key",   pa.string(), nullable=False),
])

_ERR_SCHEMA = pa.schema([
    pa.field("row_id",           pa.string(), nullable=False),
    pa.field("source_id",        pa.string(), nullable=False),
    pa.field("source_file",      pa.string(), nullable=False),
    pa.field("sheet_name",       pa.string(), nullable=False),
    pa.field("year",             pa.int32(),  nullable=False),
    pa.field("row_num",          pa.int32(),  nullable=False),
    pa.field("region_raw",       pa.string(), nullable=False),
    pa.field("error_type",       pa.string(), nullable=False),
    pa.field("resolution_scope", pa.string(), nullable=False),
    pa.field("normalized_key",   pa.string(), nullable=False),
])


# ── Вспомогательные функции ────────────────────────────────────────────────────

def _make_row_id(source_file: str, sheet_name: str, row_num: int) -> str:
    return f"{source_file}||{sheet_name}||{row_num}"


def _is_numbering_row(row: dict) -> bool:
    """True если строка — нумератор колонок (A/1/2/3... или 1.0/2.0...)."""
    c0 = str(row.get("col_0") or "").strip()
    c1 = str(row.get("col_1") or "").strip()
    if c0 == "A" and c1 in ("1", "1.0"):
        return True
    try:
        if float(c0) == 1.0 and float(c1) == 2.0:
            return True
    except (ValueError, TypeError):
        pass
    return False


def _find_data_start(sheet_rows: list[dict]) -> int:
    """Номер первой строки данных (следующая после нумератора)."""
    for r in sorted(sheet_rows, key=lambda x: int(x["row_num"])):
        if _is_numbering_row(r):
            return int(r["row_num"]) + 1
    return 0


def _normalize_sheet_name(sheet_name: str) -> str:
    """Убирает «Таб.» и лишние точки: «Таб.2.1.1» → «2.1.1»."""
    s = sheet_name.strip()
    if s.startswith("Таб."):
        s = s[4:]
    return s.strip(".")


def _is_region_sheet(sheet_name: str) -> bool:
    """True если лист содержит данные по субъекту РФ (паттерн 2.X.Y)."""
    return bool(re.fullmatch(r"2\.\d+\.\d+", _normalize_sheet_name(sheet_name)))


def _extract_region_from_header(sheet_rows: list[dict], year: int) -> str | None:
    """
    Извлекает название субъекта из заголовочных строк листа (для befolkning-источников).
    2018-2020: row_num=3, col_1
    2021-2024: row_num=1, col_0
    """
    if year <= 2020:
        target_row, col = 3, "col_1"
    else:
        target_row, col = 1, "col_0"

    for r in sheet_rows:
        if int(r["row_num"]) == target_row:
            val = r.get(col)
            if val and str(val).strip() not in ("", "None", "nan"):
                return str(val).strip()
    return None


# ── Пайплайн A: регион в строке (location=row) ────────────────────────────────

def _run_row_pipeline(
    df: pd.DataFrame,
    source_id: str,
    row_column: str,
    region_index: dict,
) -> tuple[list[dict], list[dict]]:
    ok_records: list[dict] = []
    error_records: list[dict] = []

    for (source_file, sheet_name, year), group in df.groupby(
        ["source_file", "sheet_name", "year"]
    ):
        sheet_rows = group.to_dict(orient="records")
        data_start = _find_data_start(sheet_rows)
        data_rows = [r for r in sheet_rows if int(r["row_num"]) >= data_start]

        for row in sorted(data_rows, key=lambda x: int(x["row_num"])):
            region_raw = str(row.get(row_column) or "").strip()
            if not region_raw or region_raw in ("nan", "None"):
                # Пустые строки пропускаем — они не несут данных о регионе
                continue

            row_num = int(row["row_num"])
            row_id = _make_row_id(source_file, sheet_name, row_num)
            normalized_key = normalize_region_name(region_raw)
            region_code = lookup_region(region_raw, region_index)

            base = {
                "row_id":      row_id,
                "source_id":   source_id,
                "source_file": str(source_file),
                "sheet_name":  str(sheet_name),
                "year":        int(year),
                "row_num":     row_num,
                "region_raw":  region_raw,
            }

            if region_code and region_code != SKIP:
                ok_records.append({
                    **base,
                    "region_code":      region_code,
                    "resolution_scope": "row",
                    "normalized_key":   normalized_key,
                })
            else:
                error_records.append({
                    **base,
                    "error_type":       "aggregate_scope" if region_code == SKIP else "unmatched",
                    "resolution_scope": "row",
                    "normalized_key":   normalized_key,
                })

    return ok_records, error_records


# ── Пайплайн B: регион на уровне листа (location=meta_sheet) ──────────────────

def _run_meta_sheet_pipeline(
    df: pd.DataFrame,
    source_id: str,
    region_index: dict,
    region_cfg: dict,
) -> tuple[list[dict], list[dict]]:
    ok_records: list[dict] = []
    error_records: list[dict] = []

    for (source_file, sheet_name, year), group in df.groupby(
        ["source_file", "sheet_name", "year"]
    ):
        year = int(year)

        # Обрабатываем только листы с данными по субъектам РФ (2.X.Y)
        if not _is_region_sheet(str(sheet_name)):
            continue

        sheet_rows = group.to_dict(orient="records")
        sorted_rows = sorted(sheet_rows, key=lambda x: int(x["row_num"]))

        # Проверяем quarantine_sheets — листы которые карантинируются по имени,
        # даже если название региона в заголовке выглядит как обычный субъект.
        quarantine_sheets: dict[str, str] = region_cfg.get("quarantine_sheets", {})
        normalized_sheet = _normalize_sheet_name(str(sheet_name))
        if normalized_sheet in quarantine_sheets:
            reason = quarantine_sheets[normalized_sheet]
            for row in sorted_rows:
                row_num = int(row["row_num"])
                row_id = _make_row_id(source_file, sheet_name, row_num)
                error_records.append({
                    "row_id":           row_id,
                    "source_id":        source_id,
                    "source_file":      str(source_file),
                    "sheet_name":       str(sheet_name),
                    "year":             year,
                    "row_num":          row_num,
                    "region_raw":       reason,
                    "error_type":       "aggregate_scope",
                    "resolution_scope": "meta_sheet",
                    "normalized_key":   normalized_sheet,
                })
            continue

        region_raw = _extract_region_from_header(sheet_rows, year)

        if not region_raw:
            for row in sorted_rows:
                row_num = int(row["row_num"])
                row_id = _make_row_id(source_file, sheet_name, row_num)
                error_records.append({
                    "row_id":           row_id,
                    "source_id":        source_id,
                    "source_file":      str(source_file),
                    "sheet_name":       str(sheet_name),
                    "year":             year,
                    "row_num":          row_num,
                    "region_raw":       "",
                    "error_type":       "empty",
                    "resolution_scope": "meta_sheet",
                    "normalized_key":   "",
                })
            continue

        normalized_key = normalize_region_name(region_raw)
        region_code = lookup_region(region_raw, region_index)

        if region_code and region_code != SKIP:
            for row in sorted_rows:
                row_num = int(row["row_num"])
                row_id = _make_row_id(source_file, sheet_name, row_num)
                ok_records.append({
                    "row_id":           row_id,
                    "source_id":        source_id,
                    "source_file":      str(source_file),
                    "sheet_name":       str(sheet_name),
                    "year":             year,
                    "row_num":          row_num,
                    "region_raw":       region_raw,
                    "region_code":      region_code,
                    "resolution_scope": "meta_sheet",
                    "normalized_key":   normalized_key,
                })
        else:
            err_type = "aggregate_scope" if region_code == SKIP else "doc_unmatched"
            for row in sorted_rows:
                row_num = int(row["row_num"])
                row_id = _make_row_id(source_file, sheet_name, row_num)
                error_records.append({
                    "row_id":           row_id,
                    "source_id":        source_id,
                    "source_file":      str(source_file),
                    "sheet_name":       str(sheet_name),
                    "year":             year,
                    "row_num":          row_num,
                    "region_raw":       region_raw,
                    "error_type":       err_type,
                    "resolution_scope": "meta_sheet",
                    "normalized_key":   normalized_key,
                })

    return ok_records, error_records


# ── Запись в Iceberg ───────────────────────────────────────────────────────────

def _write_to_iceberg(cat: SqlCatalog, table_name: str, records: list[dict], schema: pa.Schema) -> None:
    if not records:
        return
    cols = {field.name: pa.array([r[field.name] for r in records], field.type) for field in schema}
    arrow_tbl = pa.table(cols, schema=schema)
    cat.load_table(table_name).append(arrow_tbl)


def _already_processed(cat: SqlCatalog, source_id: str) -> bool:
    """True если в bronze_normalized.region уже есть записи для данного source_id."""
    try:
        tbl = cat.load_table("bronze_normalized.region")
        arrow = tbl.scan(selected_fields=("source_id",)).to_arrow()
        if len(arrow) == 0:
            return False
        source_ids = arrow["source_id"].to_pylist()
        return source_id in source_ids
    except Exception:
        return False


# ── Точка входа ────────────────────────────────────────────────────────────────

def run(cat: SqlCatalog, config_path: str = DEFAULT_CONFIG_PATH) -> dict:
    """
    Запускает нормализацию региона для всех источников из конфига.
    Идемпотентна: пропускает источники, которые уже обработаны.

    Возвращает dict: source_id → {total, ok_count, error_count}.
    """
    sources = load_config(config_path)
    region_index = build_region_index(cat)
    log.info("Индекс регионов: %d записей", len(region_index))

    log.info("Загружаем bronze.excel_tables...")
    bronze = cat.load_table("bronze.excel_tables").scan().to_pandas()
    log.info("Всего строк в bronze: %d", len(bronze))

    stats: dict[str, dict] = {}

    for src in sources:
        source_id = src["source_id"]

        # PostgreSQL-источники не имеют region-конфига и source_filter — пропускаем
        if "source_filter" not in src or "region" not in src:
            log.info("[%s] Нет source_filter/region в конфиге, пропускаю", source_id)
            continue

        source_filter = src["source_filter"]
        region_cfg = src["region"]
        location = region_cfg["location"]

        if _already_processed(cat, source_id):
            log.info("[%s] Уже обработан, пропускаю", source_id)
            stats[source_id] = {"skipped": True}
            continue

        # Фильтруем строки по источнику
        mask = bronze["source_file"].str.contains(source_filter, na=False)
        src_df = bronze[mask].copy()
        log.info("[%s] Строк в bronze: %d", source_id, len(src_df))

        if src_df.empty:
            log.warning("[%s] Нет данных в bronze. Пропускаю.", source_id)
            continue

        # Запускаем соответствующий пайплайн
        if location == "row":
            row_column = region_cfg["row_column"]
            ok_records, error_records = _run_row_pipeline(src_df, source_id, row_column, region_index)
        elif location == "meta_sheet":
            ok_records, error_records = _run_meta_sheet_pipeline(src_df, source_id, region_index, region_cfg)
        else:
            raise ValueError(f"[{source_id}] Неизвестный location: {location!r}")

        # Запись результатов
        _write_to_iceberg(cat, "bronze_normalized.region", ok_records, _OK_SCHEMA)
        _write_to_iceberg(cat, "bronze_normalized.region_error", error_records, _ERR_SCHEMA)

        total = len(ok_records) + len(error_records)
        log.info(
            "[%s] ok=%d error=%d (%.1f%% покрыто)",
            source_id, len(ok_records), len(error_records),
            100 * len(ok_records) / total if total else 0,
        )

        # Топ нераспознанных для диагностики
        if error_records:
            from collections import Counter
            top = Counter(r["region_raw"] for r in error_records if r["region_raw"]).most_common(10)
            log.info("[%s] Топ нераспознанных region_raw:", source_id)
            for raw, cnt in top:
                log.info("  %4d  %r", cnt, raw)

        stats[source_id] = {
            "total":       total,
            "ok_count":    len(ok_records),
            "error_count": len(error_records),
        }

    return stats
