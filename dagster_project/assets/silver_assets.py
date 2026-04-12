import logging
import os
import sys

from dagster import AssetExecutionContext, MetadataValue, asset

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def _refresh_views(context: AssetExecutionContext) -> None:
    from scripts.refresh_duckdb import run
    run()
    context.log.info("DuckDB views refreshed")


@asset(
    group_name="silver",
    deps=["normalized_row_gate"],
    description="Bronze → Silver: дошкольники по регионам, годам, возрастам и типу территории.",
)
def doshkolka_silver(context: AssetExecutionContext) -> None:
    from transformations.silver_doshkolka import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()

    context.add_output_metadata({"total_rows": MetadataValue.int(count)})
    context.log.info("Silver doshkolka: %d rows written", count)
    _refresh_views(context)


@asset(
    group_name="silver",
    deps=["normalized_row_gate"],
    description="Bronze → Silver: численность населения по субъектам РФ, полу и возрасту.",
)
def naselenie_silver(context: AssetExecutionContext) -> None:
    from transformations.silver_naselenie import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()

    context.add_output_metadata({"total_rows": MetadataValue.int(count)})
    context.log.info("Silver naselenie: %d rows written", count)
    _refresh_views(context)


@asset(
    group_name="silver",
    deps=["doshkolka_silver", "naselenie_silver"],
    description=(
        "Валидация всего Silver-слоя: покрытие по регионам и годам "
        "для doshkolka и naselenie. Сохраняет Markdown-отчёт в reports/."
    ),
)
def silver_validation(context: AssetExecutionContext) -> None:
    from pyiceberg.catalog.sql import SqlCatalog
    from validation.validate_silver import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    catalog_dir = os.path.join(_project_root, "catalog")
    cat = SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{catalog_dir}/catalog.db",
            "warehouse": f"file://{catalog_dir}/warehouse",
        },
    )

    report_path = run(cat)

    context.add_output_metadata({
        "report_path": MetadataValue.path(report_path),
    })
    context.log.info("Silver validation report saved: %s", report_path)
    _refresh_views(context)
