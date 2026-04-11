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
    ("bronze", "excel_tables"),
    ("bronze", "region_lookup"),
    ("silver", "doshkolka"),
    ("silver", "naselenie"),
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

con.close()
print(f"\nГотово: {DUCKDB_PATH}")
