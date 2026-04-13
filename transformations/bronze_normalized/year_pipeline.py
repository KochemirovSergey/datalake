"""
Нормализация года: Bronze → bronze_normalized.year / year_error.

Для текущих источников год уже содержится в поле bronze.excel_tables.year
(проставляется excel_loader при загрузке). Данный пайплайн:
  1. Читает это поле для каждой строки
  2. Валидирует диапазон (2000–2030)
  3. Записывает валидные строки в bronze_normalized.year,
     невалидные — в bronze_normalized.year_error

Публичный API:
  run(cat, config_path) → dict{source_id → stats}
"""

import logging

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

from transformations.bronze_normalized.config_loader import load_config, DEFAULT_CONFIG_PATH
from transformations.bronze_normalized.year_normalizer import normalize_year_from_field

log = logging.getLogger(__name__)

# ── Схемы PyArrow ─────────────────────────────────────────────────────────────

_OK_SCHEMA = pa.schema([
    pa.field("row_id",           pa.string(), nullable=False),
    pa.field("source_id",        pa.string(), nullable=False),
    pa.field("source_file",      pa.string(), nullable=False),
    pa.field("sheet_name",       pa.string(), nullable=False),
    pa.field("row_num",          pa.int32(),  nullable=False),
    pa.field("year",             pa.int32(),  nullable=False),
    pa.field("year_type",        pa.string(), nullable=False),
    pa.field("date_raw",         pa.string(), nullable=True),
    pa.field("year_raw",         pa.string(), nullable=True),
    pa.field("resolution_scope", pa.string(), nullable=False),
])

_ERR_SCHEMA = pa.schema([
    pa.field("row_id",           pa.string(), nullable=False),
    pa.field("source_id",        pa.string(), nullable=False),
    pa.field("source_file",      pa.string(), nullable=False),
    pa.field("sheet_name",       pa.string(), nullable=False),
    pa.field("row_num",          pa.int32(),  nullable=False),
    pa.field("year_raw",         pa.string(), nullable=False),
    pa.field("error_type",       pa.string(), nullable=False),
    pa.field("resolution_scope", pa.string(), nullable=False),
])


# ── Вспомогательные функции ────────────────────────────────────────────────────

def _make_postgres_row_id(table_name: str, row_num: int) -> str:
    return f"{table_name}||{row_num}"


def _make_row_id(source_file: str, sheet_name: str, row_num: int) -> str:
    return f"{source_file}||{sheet_name}||{row_num}"


def _write_to_iceberg(cat: SqlCatalog, table_name: str, records: list[dict], schema: pa.Schema) -> None:
    if not records:
        return
    cols = {field.name: pa.array([r[field.name] for r in records], field.type) for field in schema}
    arrow_tbl = pa.table(cols, schema=schema)
    cat.load_table(table_name).append(arrow_tbl)


def _already_processed(cat: SqlCatalog, source_id: str) -> bool:
    """True если в bronze_normalized.year уже есть записи для данного source_id."""
    try:
        tbl = cat.load_table("bronze_normalized.year")
        arrow = tbl.scan(selected_fields=("source_id",)).to_arrow()
        if len(arrow) == 0:
            return False
        return source_id in arrow["source_id"].to_pylist()
    except Exception:
        return False


# ── Точка входа ────────────────────────────────────────────────────────────────

def run(cat: SqlCatalog, config_path: str = DEFAULT_CONFIG_PATH) -> dict:
    """
    Запускает нормализацию года для всех источников из конфига.
    Идемпотентна: пропускает источники, которые уже обработаны.

    Возвращает dict: source_id → {total, ok_count, error_count}.
    """
    sources = load_config(config_path)

    log.info("Загружаем bronze.excel_tables...")
    import pandas as pd
    bronze = cat.load_table("bronze.excel_tables").scan().to_pandas()
    log.info("Всего строк в bronze: %d", len(bronze))

    stats: dict[str, dict] = {}

    for src in sources:
        source_id = src["source_id"]

        # Пытаемся получить конфиг года
        year_cfg = src.get("year")
        if not year_cfg:
            log.info("[%s] Нет конфига year, пропускаю", source_id)
            continue
        year_type = year_cfg["year_type"]
        location = year_cfg["location"]

        if _already_processed(cat, source_id):
            log.info("[%s] Год уже нормализован, пропускаю", source_id)
            stats[source_id] = {"skipped": True}
            continue

        # Определяем источник данных
        if src.get("table_type") == "postgres":
            table_name = src["table_name"]
            log.info("[%s] Загружаем bronze.%s...", source_id, table_name)
            try:
                src_df = cat.load_table(f"bronze.{table_name}").scan().to_pandas()
            except Exception as e:
                log.warning("[%s] Ошибка загрузки таблицы %s: %s", source_id, table_name, e)
                continue
        else:
            if "source_filter" not in src:
                log.info("[%s] Нет source_filter, пропускаю", source_id)
                continue
            source_filter = src["source_filter"]
            mask = bronze["source_file"].str.contains(source_filter, na=False)
            src_df = bronze[mask].copy()

        log.info("[%s] Строк для обработки: %d", source_id, len(src_df))

        if src_df.empty:
            log.warning("[%s] Нет данных. Пропускаю.", source_id)
            continue

        ok_records: list[dict] = []
        error_records: list[dict] = []

        is_postgres = src.get("table_type") == "postgres"

        for _, row in src_df.iterrows():
            row_num = int(row["row_num"])
            if is_postgres:
                row_id = _make_postgres_row_id(src["table_name"], row_num)
                source_file = src["table_name"]
                sheet_name = "postgres"
                # В Postgres-таблицах год в колонке "год" (или что в конфиге)
                row_column = year_cfg.get("row_column", "год")
                raw_year = row.get(row_column)
            else:
                row_id = _make_row_id(
                    str(row["source_file"]),
                    str(row["sheet_name"]),
                    row_num,
                )
                source_file = str(row["source_file"])
                sheet_name = str(row["sheet_name"])
                raw_year = row.get("year")

            year_raw_str = str(raw_year) if raw_year is not None else ""
            year_int, error_type = normalize_year_from_field(raw_year, year_type)

            if year_int is not None:
                ok_records.append({
                    "row_id":           row_id,
                    "source_id":        source_id,
                    "source_file":      source_file,
                    "sheet_name":       sheet_name,
                    "row_num":          row_num,
                    "year":             year_int,
                    "year_type":        year_type,
                    "date_raw":         None,
                    "year_raw":         year_raw_str,
                    "resolution_scope": location,
                })
            else:
                error_records.append({
                    "row_id":           row_id,
                    "source_id":        source_id,
                    "source_file":      source_file,
                    "sheet_name":       sheet_name,
                    "row_num":          row_num,
                    "year_raw":         year_raw_str,
                    "error_type":       error_type,
                    "resolution_scope": location,
                })

        _write_to_iceberg(cat, "bronze_normalized.year", ok_records, _OK_SCHEMA)
        _write_to_iceberg(cat, "bronze_normalized.year_error", error_records, _ERR_SCHEMA)

        total = len(ok_records) + len(error_records)
        log.info(
            "[%s] ok=%d error=%d",
            source_id, len(ok_records), len(error_records),
        )

        stats[source_id] = {
            "total":       total,
            "ok_count":    len(ok_records),
            "error_count": len(error_records),
        }

    return stats
