"""
Обновляет datalake.duckdb: пересоздаёт вьюхи на основе актуальных
Iceberg-снапшотов. Запускать после каждой новой загрузки данных.
"""

import os
import sys

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DUCKDB_PATH = os.path.join(BASE_DIR, "datalake.duckdb")
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

catalog = SqlCatalog(
    "datalake",
    **{
        "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
        "warehouse": f"file://{CATALOG_DIR}/warehouse",
    },
)

con = duckdb.connect(DUCKDB_PATH)

tables = [
    ("bronze",            "excel_tables"),
    ("bronze",            "region_lookup"),
    ("bronze_normalized", "region"),
    ("bronze_normalized", "region_error"),
    ("bronze_normalized", "year"),
    ("bronze_normalized", "year_error"),
    ("silver",            "doshkolka"),
    ("silver",            "naselenie"),
]

for namespace, table_name in tables:
    full_name = f"{namespace}.{table_name}"
    view_name = f"{namespace}_{table_name}"
    try:
        tbl = catalog.load_table(full_name)
        files = tbl.inspect.files().to_pydict()["file_path"]
        # Конвертируем в абсолютные пути:
        # file:///abs/path → /abs/path
        # file://rel/path → BASE_DIR/rel/path
        local_files = []
        for f in files:
            path = f.replace("file://", "")
            if not os.path.isabs(path):
                path = os.path.join(BASE_DIR, path)
            local_files.append(path)

        if not local_files:
            print(f"  {full_name}: нет файлов, пропускаю")
            continue

        files_sql = ", ".join(f"'{f}'" for f in local_files)
        con.execute(f"DROP VIEW IF EXISTS {view_name}")
        con.execute(
            f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet([{files_sql}], union_by_name=true)"
        )
        count = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
        print(f"  {view_name}: {count} строк ({len(local_files)} файлов)")
    except Exception as e:
        print(f"  {full_name}: ошибка — {e}", file=sys.stderr)

# Удобные вьюхи-фильтры поверх bronze_excel_tables
filtered_views = [
    (
        "bronze_doshkolka",
        "SELECT * FROM bronze_excel_tables WHERE source_file LIKE '%Дошколка%'",
    ),
    (
        "bronze_naselenie",
        "SELECT * FROM bronze_excel_tables WHERE source_file LIKE '%Население%'",
    ),
]

for view_name, query in filtered_views:
    try:
        con.execute(f"DROP VIEW IF EXISTS {view_name}")
        con.execute(f"CREATE VIEW {view_name} AS {query}")
        count = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
        print(f"  {view_name}: {count} строк")
    except Exception as e:
        print(f"  {view_name}: ошибка — {e}", file=sys.stderr)

# ── Валидационные вьюхи: bronze + нормализация ────────────────────────────────
# Объединяют все строки bronze с результатами нормализации региона и года.
# norm_status:
#   ok              — регион и год распознаны
#   region_error:X  — регион не распознан (X = тип ошибки)
#   unprocessed     — строка не дошла до нормализации региона (заголовки, агрегаты)

_VALIDATION_BASE_SQL = """
    SELECT
        -- Нормализация (первые колонки для удобства в DBeaver)
        COALESCE(r.source_id, e.source_id, y.source_id)    AS source_id,
        r.region_code,
        COALESCE(r.region_raw, e.region_raw)               AS region_raw,
        CASE
            WHEN r.region_code IS NOT NULL THEN 'ok'
            WHEN e.error_type  IS NOT NULL THEN 'region_error:' || e.error_type
            ELSE 'unprocessed'
        END                                                 AS norm_status,
        e.error_type                                        AS region_error_type,
        COALESCE(r.resolution_scope, e.resolution_scope)   AS region_scope,
        y.year                                              AS year_normalized,
        -- Исходные данные из bronze
        b.*
    FROM bronze_excel_tables b
    LEFT JOIN bronze_normalized_region r
        ON  r.source_file = b.source_file
        AND r.sheet_name  = b.sheet_name
        AND r.row_num     = b.row_num
    LEFT JOIN bronze_normalized_region_error e
        ON  e.source_file = b.source_file
        AND e.sheet_name  = b.sheet_name
        AND e.row_num     = b.row_num
    LEFT JOIN bronze_normalized_year y
        ON  y.source_file = b.source_file
        AND y.sheet_name  = b.sheet_name
        AND y.row_num     = b.row_num
"""

validation_views = [
    (
        "validation_normalized",
        _VALIDATION_BASE_SQL,
    ),
    (
        "validation_doshkolka",
        _VALIDATION_BASE_SQL + "WHERE b.source_file LIKE '%Дошколка%'",
    ),
    (
        "validation_naselenie",
        _VALIDATION_BASE_SQL + "WHERE b.source_file LIKE '%Население%'",
    ),
]

print("\nВалидационные вьюхи:")
for view_name, query in validation_views:
    try:
        con.execute(f"DROP VIEW IF EXISTS {view_name}")
        con.execute(f"CREATE VIEW {view_name} AS {query}")
        count = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
        ok_count = con.execute(
            f"SELECT COUNT(*) FROM {view_name} WHERE norm_status = 'ok'"
        ).fetchone()[0]
        print(f"  {view_name}: {count} строк, из них ok={ok_count}")
    except Exception as e:
        print(f"  {view_name}: ошибка — {e}", file=sys.stderr)

con.close()
print(f"\nГотово: {DUCKDB_PATH}")
