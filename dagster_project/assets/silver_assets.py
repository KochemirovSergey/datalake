import logging
import os
import sys

from dagster import AssetExecutionContext, MetadataValue, asset

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


@asset(
    group_name="silver",
    deps=["excel_bronze", "regions_bronze"],
    description="Bronze → Silver: дошкольники по регионам, годам, возрастам и типу территории.",
)
def doshkolka_silver(context: AssetExecutionContext) -> None:
    from transformations.silver_doshkolka import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()

    context.add_output_metadata({"total_rows": MetadataValue.int(count)})
    context.log.info("Silver doshkolka: %d rows written", count)


@asset(
    group_name="silver",
    deps=["population_bronze", "regions_bronze"],
    description="Bronze → Silver: численность населения по субъектам РФ, полу и возрасту.",
)
def naselenie_silver(context: AssetExecutionContext) -> None:
    from transformations.silver_naselenie import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()

    context.add_output_metadata({"total_rows": MetadataValue.int(count)})
    context.log.info("Silver naselenie: %d rows written", count)
