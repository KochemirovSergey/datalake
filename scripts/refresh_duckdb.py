"""
Обновляет datalake.duckdb: пересоздаёт вьюхи на основе актуальных
Iceberg-снапшотов. Запускать после каждой новой загрузки данных.

Можно вызывать как скрипт (python scripts/refresh_duckdb.py)
или импортировать и вызывать run() из Dagster-активов.
"""

import logging
import os
import sys

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DUCKDB_PATH = os.path.join(BASE_DIR, "datalake.duckdb")
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

_TABLES = [
    ("bronze",            "excel_tables",    "bronze_raw"),
    ("bronze",            "region_lookup",   "bronze_region_lookup"),
    ("bronze",            "oo_1_2_7_2_211",  "bronze_oo_1_2_7_2_211"),
    ("bronze",            "oo_1_2_7_1_209",  "bronze_oo_1_2_7_1_209"),
    ("bronze",            "спо_1_р2_101_43", "bronze_спо_1_р2_101_43"),
    ("bronze",            "впо_1_р2_13_54",  "bronze_впо_1_р2_13_54"),
    ("bronze",            "пк_1_2_4_180",    "bronze_пк_1_2_4_180"),
    ("bronze",            "education_level_lookup",    "bronze_education_level_lookup"),
    ("bronze_normalized", "region",                     "bronze_normalized_region"),
    ("bronze_normalized", "region_error",               "bronze_normalized_region_error"),
    ("bronze_normalized", "year",                       "bronze_normalized_year"),
    ("bronze_normalized", "year_error",                 "bronze_normalized_year_error"),
    ("bronze_normalized", "education_level",            "bronze_normalized_education_level"),
    ("bronze_normalized", "education_level_error",      "bronze_normalized_education_level_error"),
    ("bronze_normalized", "row_gate",                   "bronze_normalized_row_gate"),
    ("silver",            "doshkolka",     "silver_doshkolka"),
    ("silver",            "naselenie",     "silver_naselenie"),
    ("silver",            "oo",            "silver_oo"),
    ("silver",            "spo",           "silver_spo"),
    ("silver",            "vpo",           "silver_vpo"),
    ("silver",            "dpo",           "silver_dpo"),
]

_FILTERED_VIEWS = [
    (
        "bronze_doshkolka",
        "SELECT * FROM bronze_raw WHERE source_file LIKE '%Дошколка%'",
    ),
    (
        "bronze_naselenie",
        "SELECT * FROM bronze_raw WHERE source_file LIKE '%Население%'",
    ),
]

_OBSOLETE_VIEWS = [
    "bronze_excel_tables",
    "validation_normalized",
    "validation_doshkolka",
    "validation_naselenie",
]


def run() -> None:
    """
    Пересоздаёт все DuckDB-вьюхи из актуальных Iceberg-снапшотов.
    При блокировке файла DBeaver'ом — логирует предупреждение и возвращает управление.
    """
    catalog = SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )

    try:
        con = duckdb.connect(DUCKDB_PATH)
    except Exception as e:
        log.warning("DuckDB заблокирован (возможно, открыт в DBeaver): %s", e)
        return

    try:
        # Удаляем устаревшие вьюхи
        for obsolete in _OBSOLETE_VIEWS:
            con.execute(f"DROP VIEW IF EXISTS {obsolete}")

        # Основные вьюхи из Iceberg-таблиц
        for namespace, table_name, view_name in _TABLES:
            full_name = f"{namespace}.{table_name}"
            try:
                tbl = catalog.load_table(full_name)
                files = tbl.inspect.files().to_pydict()["file_path"]

                local_files = []
                for f in files:
                    path = f.replace("file://", "")
                    if not os.path.isabs(path):
                        path = os.path.join(BASE_DIR, path)
                    local_files.append(path)

                if not local_files:
                    con.execute(f"DROP VIEW IF EXISTS {view_name}")
                    log.debug("%s: нет файлов, пропускаю", full_name)
                    continue

                files_sql = ", ".join(f"'{f}'" for f in local_files)
                con.execute(f"DROP VIEW IF EXISTS {view_name}")
                con.execute(
                    f"CREATE VIEW {view_name} AS "
                    f"SELECT * FROM read_parquet([{files_sql}], union_by_name=true)"
                )
                count = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
                log.debug("%s: %d строк (%d файлов)", view_name, count, len(local_files))

            except Exception as e:
                log.warning("%s: ошибка — %s", full_name, e)

        # Фильтрованные вьюхи поверх bronze_raw
        for view_name, query in _FILTERED_VIEWS:
            try:
                con.execute(f"DROP VIEW IF EXISTS {view_name}")
                con.execute(f"CREATE VIEW {view_name} AS {query}")
            except Exception as e:
                log.warning("%s: ошибка — %s", view_name, e)

    finally:
        con.close()

    log.info("DuckDB-вьюхи обновлены: %s", DUCKDB_PATH)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(message)s")
    run()
