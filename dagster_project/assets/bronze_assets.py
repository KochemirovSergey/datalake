import logging
import os
import sys

from dagster import AssetExecutionContext, MetadataValue, asset

log = logging.getLogger(__name__)

# Корень проекта в sys.path — нужен для импорта ingestion/
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def _refresh_views(context: AssetExecutionContext) -> None:
    from scripts.refresh_duckdb import run
    run()
    context.log.info("DuckDB views refreshed")


@asset(
    group_name="bronze_extraction",
    description="Загружает все Excel-файлы из data/Дошколка/ в Bronze слой (Iceberg).",
)
def doshkolka_bronze(context: AssetExecutionContext) -> None:
    from ingestion.excel_loader import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stats = run()

    total_rows = sum(stats.values())
    files_loaded = len({(year, fname) for year, fname, _ in stats})
    sheets_loaded = len(stats)

    context.add_output_metadata(
        {
            "total_rows": MetadataValue.int(total_rows),
            "files_loaded": MetadataValue.int(files_loaded),
            "sheets_loaded": MetadataValue.int(sheets_loaded),
            "breakdown": MetadataValue.md(
                "\n".join(
                    f"- **{year}** `{fname}` [{sheet}] → {count} строк"
                    for (year, fname, sheet), count in sorted(stats.items())
                )
            ),
        }
    )
    context.log.info(
        "Bronze loaded: %d rows across %d sheets from %d files",
        total_rows,
        sheets_loaded,
        files_loaded,
    )
    _refresh_views(context)


@asset(
    group_name="bronze_extraction",
    description="Загружает все Excel-файлы из data/Население/ в Bronze слой (Iceberg).",
)
def naselenie_bronze(context: AssetExecutionContext) -> None:
    from ingestion.excel_loader import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    data_dir = os.path.join(_project_root, "data", "Население")
    stats = run(data_dir=data_dir, flat=True)

    total_rows = sum(stats.values())
    new_sheets = sum(1 for c in stats.values() if c > 0)

    context.add_output_metadata(
        {
            "total_rows": MetadataValue.int(total_rows),
            "new_sheets": MetadataValue.int(new_sheets),
        }
    )
    context.log.info("Population Bronze loaded: %d rows across %d new sheets", total_rows, new_sheets)
    _refresh_views(context)


@asset(
    group_name="bronze_extraction",
    description="Загружает data/regions.json в Bronze слой (bronze.region_lookup).",
)
def regions_bronze(context: AssetExecutionContext) -> None:
    from ingestion.json_loader import run

    count = run()

    context.add_output_metadata(
        {
            "total_rows": MetadataValue.int(count),
        }
    )
    context.log.info("Regions loaded: %d rows", count)
    _refresh_views(context)


@asset(
    group_name="bronze_extraction",
    description=(
        "Загружает таблицы из PostgreSQL (etl_db) в Bronze слой Iceberg: "
        "oo_1_2_7_2_211, oo_1_2_7_1_209, спо_1_р2_101_43, впо_1_р2_13_54, пк_1_2_4_180."
    ),
)
def postgres_bronze(context: AssetExecutionContext) -> None:
    from ingestion.postgres_loader import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stats = run()

    total_rows = sum(stats.values())
    context.add_output_metadata(
        {
            "total_rows": MetadataValue.int(total_rows),
            "breakdown": MetadataValue.md(
                "\n".join(
                    f"- `{table}` → {count} строк" if count > 0
                    else f"- `{table}` → пропущено (уже загружено)"
                    for table, count in stats.items()
                )
            ),
        }
    )
    context.log.info("PostgreSQL Bronze loaded: %d rows total", total_rows)
    _refresh_views(context)
