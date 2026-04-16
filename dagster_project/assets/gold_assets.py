"""
Dagster assets для Gold-слоя: students, staff_load, dormitory.
"""

import logging
import os
import sys

from dagster import AssetExecutionContext, MetadataValue, asset

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def _refresh_views(context: AssetExecutionContext) -> None:
    """Пересоздать DuckDB-вьюхи после записи в Iceberg."""
    from scripts.refresh_duckdb import run
    run()
    context.log.info("DuckDB views refreshed")


@asset(
    group_name="gold",
    deps=["education_population_wide_annual_silver"],
    description="Silver → Gold: обучающиеся по регионам, годам и возрастам.",
)
def students_gold(context: AssetExecutionContext) -> None:
    """Создать gold.students из silver.education_population_wide_annual."""
    from transformations.gold_students import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()

    context.add_output_metadata({"total_rows": MetadataValue.int(count)})
    context.log.info("Gold students: %d rows written", count)
    _refresh_views(context)


@asset(
    group_name="gold",
    deps=["staff_shortage_silver"],
    description="Silver + Bronze → Gold: педагогическая нагрузка по уровням образования.",
)
def staff_load_gold(context: AssetExecutionContext) -> None:
    """Создать gold.staff_load из silver.staff_shortage_triggers и bronze-таблиц."""
    from transformations.gold_staff_load import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()

    context.add_output_metadata({"total_rows": MetadataValue.int(count)})
    context.log.info("Gold staff_load: %d rows written", count)
    _refresh_views(context)


@asset(
    group_name="gold",
    deps=["dormitory_silver"],
    description="Silver → Gold: метрики общежитий по регионам и годам.",
)
def dormitory_gold(context: AssetExecutionContext) -> None:
    """Создать gold.dormitory из silver.dormitory_infrastructure."""
    from transformations.gold_dormitory import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()

    context.add_output_metadata({"total_rows": MetadataValue.int(count)})
    context.log.info("Gold dormitory: %d rows written", count)
    _refresh_views(context)
