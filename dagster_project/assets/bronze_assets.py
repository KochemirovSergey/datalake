import logging
import os
import sys

from dagster import AssetExecutionContext, MetadataValue, asset

log = logging.getLogger(__name__)

# Корень проекта в sys.path — нужен для импорта ingestion/
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


@asset(
    group_name="bronze",
    description="Загружает все Excel-файлы из data/Дошколка/ в Bronze слой (Iceberg).",
)
def excel_bronze(context: AssetExecutionContext) -> None:
    """
    Обнаруживает .xls/.xlsx файлы по папкам-годам,
    извлекает строки данных в wide-формате и пишет в bronze.excel_tables.
    """
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


@asset(
    group_name="bronze",
    description="Загружает все Excel-файлы из data/Население/ в Bronze слой (Iceberg).",
)
def population_bronze(context: AssetExecutionContext) -> None:
    """
    Обнаруживает Бюллетень_YYYY.xlsx файлы из data/Население/,
    извлекает строки всех листов и пишет в bronze.excel_tables.
    """
    import os
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


@asset(
    group_name="bronze",
    description="Загружает data/regions.json в Bronze слой (bronze.region_lookup).",
)
def regions_bronze(context: AssetExecutionContext) -> None:
    """
    Читает regions.json, строит плоскую таблицу:
    name_variant → canonical_name → region_code.
    Включает как канонические названия, так и алиасы/опечатки (is_alias=True).
    """
    from ingestion.regions_loader import run

    count = run()

    context.add_output_metadata(
        {
            "total_rows": MetadataValue.int(count),
        }
    )
    context.log.info("Regions loaded: %d rows", count)
