"""
Общие утилиты и интерфейс DimensionProcessor для слоя bronze_normalized.

Этап 1 ТЗ: единый интерфейс конвейера измерения.
  extract → normalize → resolve → validate → publish → gate

DimensionProcessor — Protocol (структурная типизация): любой класс с методом run()
автоматически является его реализацией. Регион и год пока не мигрировали на него,
но следуют тому же контракту.
"""

from __future__ import annotations

import re
from typing import Protocol

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog


# ── DimensionProcessor ─────────────────────────────────────────────────────────

class DimensionProcessor(Protocol):
    """
    Единый интерфейс обработки измерения bronze_normalized.

    Конвейер:
      extract → normalize → resolve → validate → publish → gate

    Каждая реализация (RegionPipeline, YearPipeline, EducationLevelPipeline)
    предоставляет метод run(), возвращающий статистику по источникам.
    """

    def run(self, cat: SqlCatalog, config_path: str = ...) -> dict:
        """
        Запускает полный пайплайн нормализации.

        Идемпотентен: пропускает источники, которые уже обработаны.
        Возвращает dict: source_id → {total, ok_count, error_count} | {skipped: True}.
        """
        ...


# ── Допустимые статусы измерений ───────────────────────────────────────────────

class DimensionStatus:
    OK              = "ok"
    EMPTY           = "empty"
    UNPARSEABLE     = "unparseable"
    OUT_OF_RANGE    = "out_of_range"
    NOT_FOUND       = "not_found"
    UNMATCHED       = "unmatched"
    AMBIGUOUS       = "ambiguous"
    AGGREGATE_SCOPE = "aggregate_scope"
    NOT_APPLICABLE  = "not_applicable"   # измерение не требуется для источника
    MISSING         = "missing"          # строка не попала ни в ok, ни в error


# ── Вспомогательные функции (shared across all dimension pipelines) ─────────────

def make_row_id(source_file: str, sheet_name: str, row_num: int) -> str:
    """row_id для строк из bronze.excel_tables."""
    return f"{source_file}||{sheet_name}||{row_num}"


def make_postgres_row_id(table_name: str, row_num: int) -> str:
    """row_id для строк из PostgreSQL-bronze таблиц."""
    return f"{table_name}||{row_num}"


def is_numbering_row(row: dict) -> bool:
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


def find_data_start(sheet_rows: list[dict]) -> int:
    """Номер первой строки данных (следующая после нумератора)."""
    for r in sorted(sheet_rows, key=lambda x: int(x["row_num"])):
        if is_numbering_row(r):
            return int(r["row_num"]) + 1
    return 0


def write_to_iceberg(
    cat: SqlCatalog,
    table_name: str,
    records: list[dict],
    schema: pa.Schema,
) -> None:
    """Записывает список словарей в Iceberg-таблицу."""
    if not records:
        return
    cols = {
        field.name: pa.array([r[field.name] for r in records], field.type)
        for field in schema
    }
    cat.load_table(table_name).append(pa.table(cols, schema=schema))


def build_edu_lookup(cat: SqlCatalog) -> dict[str, dict]:
    """
    Строит индекс code → row_dict из bronze.education_level_lookup.
    Используется для проставления resolved_label.
    """
    try:
        df = cat.load_table("bronze.education_level_lookup").scan().to_pandas()
        return {str(row["code"]): row.to_dict() for _, row in df.iterrows()}
    except Exception:
        return {}
