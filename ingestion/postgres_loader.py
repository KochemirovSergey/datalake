"""
PostgreSQL → Bronze loader.

Загружает таблицы из PostgreSQL в bronze-слой Iceberg.
Принцип: максимально сырые данные — все значения хранятся как строки.

Метаданные на каждую строку:
  load_id      — UUID загрузки
  loaded_at    — время загрузки (UTC)
  source_table — исходная таблица (schema.table)
  row_num      — порядковый номер строки в выгрузке

Данные: все колонки исходной таблицы (значения приведены к строкам).

Идемпотентность: повторный запуск не дублирует данные.
Каждая PostgreSQL-таблица хранится в отдельной Iceberg-таблице bronze.<table_name>.
"""

import logging
import os
import uuid
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

SRC_DB = {
    "host":     "localhost",
    "port":     5432,
    "database": "etl_db",
    "user":     "etl_user",
    "password": "etl_password",
}

# Таблицы для загрузки: (pg_schema, pg_table)
TABLES = [
    ("public", "oo_1_2_7_2_211_v2"),
    ("public", "oo_1_2_7_1_209_v2"),
    ("public", "oo_1_2_14_2_1_151_v2"),
    ("public", "oo_1_2_14_2_2_152_v2"),
    ("public", "oo_1_2_14_2_3_153_v2"),
    ("public", "oo_1_2_14_1_1_147_v2"),
    ("public", "oo_1_2_14_1_2_148_v2"),
    ("public", "oo_1_2_14_1_3_149_v2"),
    ("public", "oo_1_2_14_1_1_147_v2"),
    ("public", "спо_1_р2_101_43"),
    ("public", "впо_1_р2_13_54"),
    ("public", "пк_1_2_4_180"),
]


# ── Каталог ────────────────────────────────────────────────────────────────────

def get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )


# ── Introspection ──────────────────────────────────────────────────────────────

def _get_pg_columns(conn, pg_schema: str, pg_table: str) -> list[str]:
    """Возвращает имена колонок таблицы в порядке ordinal_position."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (pg_schema, pg_table),
        )
        return [row[0] for row in cur.fetchall()]


# ── Schema + создание таблицы ──────────────────────────────────────────────────

def _ensure_iceberg_table(catalog: SqlCatalog, iceberg_name: str, columns: list[str]) -> None:
    """Создаёт Iceberg-таблицу если её ещё нет. Схема: метаданные + все колонки как string."""
    meta_fields = [
        NestedField(1, "load_id",      StringType(),      required=True),
        NestedField(2, "loaded_at",    TimestamptzType(), required=True),
        NestedField(3, "source_table", StringType(),      required=True),
        NestedField(4, "row_num",      IntegerType(),     required=True),
    ]
    data_fields = [
        NestedField(5 + i, col, StringType(), required=False)
        for i, col in enumerate(columns)
    ]
    schema = Schema(*(meta_fields + data_fields))
    try:
        catalog.create_table(iceberg_name, schema=schema)
        log.info("Создана таблица: %s", iceberg_name)
    except Exception:
        log.info("Таблица уже существует: %s", iceberg_name)


# ── Идемпотентность ────────────────────────────────────────────────────────────

def _is_already_loaded(catalog: SqlCatalog, iceberg_name: str) -> bool:
    """True если таблица уже содержит данные."""
    try:
        tbl = catalog.load_table(iceberg_name)
        arrow = tbl.scan(selected_fields=("row_num",)).to_arrow()
        return len(arrow) > 0
    except Exception:
        return False


# ── Загрузка одной таблицы ─────────────────────────────────────────────────────

def load_table(pg_schema: str, pg_table: str) -> int:
    """
    Загружает одну PostgreSQL-таблицу в bronze Iceberg.
    Возвращает количество загруженных строк (0 если уже загружено).
    """
    source_table = f"{pg_schema}.{pg_table}"
    iceberg_name = f"bronze.{pg_table}"

    catalog = get_catalog()
    conn = psycopg2.connect(**SRC_DB)
    try:
        columns = _get_pg_columns(conn, pg_schema, pg_table)
        if not columns:
            log.warning("Таблица не найдена или схема пуста: %s", source_table)
            return 0

        _ensure_iceberg_table(catalog, iceberg_name, columns)

        if _is_already_loaded(catalog, iceberg_name):
            log.info("Уже загружено: %s, пропускаю", source_table)
            return 0

        load_id = str(uuid.uuid4())
        loaded_at = datetime.now(tz=timezone.utc)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f'SELECT * FROM "{pg_schema}"."{pg_table}"')
            rows = cur.fetchall()

        if not rows:
            log.info("Нет строк в %s", source_table)
            return 0

        records = []
        for row_num, row in enumerate(rows):
            record: dict = {
                "load_id":      load_id,
                "loaded_at":    loaded_at,
                "source_table": source_table,
                "row_num":      row_num,
            }
            for col in columns:
                val = row.get(col)
                record[col] = str(val) if val is not None else None
            records.append(record)

        # Строим PyArrow таблицу по актуальной схеме Iceberg
        tbl = catalog.load_table(iceberg_name)
        pa_fields = []
        for field in tbl.schema().fields:
            if field.name == "loaded_at":
                pa_fields.append(pa.field(field.name, pa.timestamp("us", tz="UTC"), nullable=False))
            elif field.name == "row_num":
                pa_fields.append(pa.field(field.name, pa.int32(), nullable=False))
            elif field.name in ("load_id", "source_table"):
                pa_fields.append(pa.field(field.name, pa.string(), nullable=False))
            else:
                pa_fields.append(pa.field(field.name, pa.string(), nullable=True))

        pa_schema = pa.schema(pa_fields)
        col_names = [f.name for f in tbl.schema().fields]
        arrays = {
            col: pa.array([r.get(col) for r in records], type=pa_schema.field(col).type)
            for col in col_names
        }
        arrow_table = pa.table(arrays, schema=pa_schema)
        tbl.append(arrow_table)

        log.info(
            "Загружено %d строк: %s → %s",
            len(records), source_table, iceberg_name,
        )
        return len(records)

    finally:
        conn.close()


# ── Точка входа ────────────────────────────────────────────────────────────────

def run() -> dict[str, int]:
    """Загружает все таблицы из TABLES. Возвращает dict: source_table → кол-во строк."""
    results: dict[str, int] = {}
    for pg_schema, pg_table in TABLES:
        count = load_table(pg_schema, pg_table)
        results[f"{pg_schema}.{pg_table}"] = count
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stats = run()
    print("\n── Итог ──────────────────────────")
    for table, count in stats.items():
        status = f"{count} строк" if count > 0 else "пропущено (уже загружено)"
        print(f"  {table}  →  {status}")
