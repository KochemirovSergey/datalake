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
    ("bronze",            "oo_1_2_7_2_211_v2",       "bronze_oo_1_2_7_2_211_v2"),
    ("bronze",            "oo_1_2_7_1_209_v2",       "bronze_oo_1_2_7_1_209_v2"),
    ("bronze",            "oo_1_2_14_2_1_151_v2",    "bronze_oo_1_2_14_2_1_151_v2"),
    ("bronze",            "oo_1_2_14_2_2_152_v2",    "bronze_oo_1_2_14_2_2_152_v2"),
    ("bronze",            "oo_1_2_14_2_3_153_v2",    "bronze_oo_1_2_14_2_3_153_v2"),
    ("bronze",            "oo_1_2_14_1_1_147_v2",    "bronze_oo_1_2_14_1_1_147_v2"),
    ("bronze",            "oo_1_2_14_1_2_148_v2",    "bronze_oo_1_2_14_1_2_148_v2"),
    ("bronze",            "oo_1_2_14_1_3_149_v2",    "bronze_oo_1_2_14_1_3_149_v2"),
    ("bronze",            "спо_1_р2_101_43", "bronze_спо_1_р2_101_43"),
    ("bronze",            "впо_1_р2_13_54",  "bronze_впо_1_р2_13_54"),
    ("bronze",            "пк_1_2_4_180",    "bronze_пк_1_2_4_180"),
    ("bronze",            "education_level_lookup",    "bronze_education_level_lookup"),
    # Дашборд: дефицит кадров
    ("bronze",            "discipuli",       "bronze_discipuli"),
    ("bronze",            "oo_1_3_4_230",    "bronze_oo_1_3_4_230"),
    ("bronze",            "oo_1_3_1_218",    "bronze_oo_1_3_1_218"),
    ("bronze",            "oo_1_3_2_221",    "bronze_oo_1_3_2_221"),
    # Дашборд: общежития
    ("bronze",            "впо_2_р1_3_8",    "bronze_впо_2_р1_3_8"),
    ("bronze",            "впо_2_р1_4_10",   "bronze_впо_2_р1_4_10"),
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
    ("silver",            "education_population_wide",         "silver_education_population_wide"),
    ("silver",            "education_population_wide_annual",  "silver_education_population_wide_annual"),
    # Аналитические линии дашбордов
    ("silver",            "staff_shortage_triggers",           "silver_staff_shortage_triggers"),
    ("silver",            "dormitory_infrastructure",          "silver_dormitory_infrastructure"),
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

        # Аналитические вьюхи: анализ охвата образованием
        try:
            con.execute("DROP VIEW IF EXISTS coverage_wide")
            con.execute("DROP VIEW IF EXISTS coverage_tidy")

            # Длинный формат: регион × возраст → охват, отклонение, лет данных
            con.execute("""
                CREATE VIEW coverage_tidy AS
                SELECT
                    COALESCE(r.canonical_name, w.region_code) AS регион,
                    w.region_code AS код,
                    w.age AS возраст,
                    nat.national_share_pct,
                    AVG(CASE WHEN w.population_total > 0 AND w.education_total IS NOT NULL
                             THEN (w.education_total / w.population_total) * 100
                             ELSE NULL END) AS рег_охват_pct,
                    AVG(CASE WHEN w.population_total > 0 AND w.education_total IS NOT NULL
                             THEN (w.education_total / w.population_total) * 100
                             ELSE NULL END) -
                    nat.national_share_pct AS отклонение_пп,
                    COUNT(DISTINCT w.year) AS лет_данных
                FROM silver_education_population_wide_annual w
                LEFT JOIN bronze_region_lookup r
                    ON r.region_code = w.region_code AND r.is_alias = false
                JOIN (
                    SELECT age,
                           SUM(education_total) / SUM(population_total) * 100 AS national_share_pct
                    FROM silver_education_population_wide_annual
                    WHERE population_total > 0 AND education_total IS NOT NULL
                    GROUP BY age
                ) nat ON nat.age = w.age
                WHERE w.population_total > 0 AND w.education_total IS NOT NULL
                GROUP BY w.region_code, r.canonical_name, w.age, nat.national_share_pct
                ORDER BY w.region_code, w.age
            """)
            log.debug("coverage_tidy: создана")

            # Пивот: получи список возрастов и создай PIVOT с явным IN (...)
            ages_result = con.execute(
                "SELECT DISTINCT age FROM silver_education_population_wide_annual "
                "WHERE age IS NOT NULL ORDER BY TRY_CAST(age AS INTEGER)"
            ).fetchall()
            if ages_result:
                ages_list = ", ".join(str(int(row[0])) for row in ages_result)
                pivot_query = f"""
                    CREATE VIEW coverage_wide AS
                    PIVOT coverage_tidy
                    ON возраст IN ({ages_list})
                    USING FIRST(рег_охват_pct)
                    GROUP BY регион, код
                """
                con.execute(pivot_query)
                log.debug("coverage_wide: создана с %d возрастов", len(ages_result))
            else:
                log.warning("coverage_wide: нет данных для пивота")

        except Exception as e:
            log.warning("coverage_*: ошибка — %s", e)

    finally:
        con.close()

    log.info("DuckDB-вьюхи обновлены: %s", DUCKDB_PATH)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(message)s")
    run()
