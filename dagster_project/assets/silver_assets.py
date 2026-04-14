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


def _get_catalog():
    from pyiceberg.catalog.sql import SqlCatalog
    catalog_dir = os.path.join(_project_root, "catalog")
    return SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{catalog_dir}/catalog.db",
            "warehouse": f"file://{catalog_dir}/warehouse",
        },
    )


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


# ── Образовательные ассеты ──────────────────────────────────────────────────────

@asset(
    group_name="silver",
    deps=["normalized_row_gate"],
    description="Bronze → Silver: общеобразовательные программы (начальное, основное, среднее).",
)
def oo_silver(context: AssetExecutionContext) -> None:
    from transformations.silver_oo import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()

    context.add_output_metadata({"total_rows": MetadataValue.int(count)})
    context.log.info("Silver oo: %d rows written", count)
    _refresh_views(context)


@asset(
    group_name="silver",
    deps=["normalized_row_gate"],
    description="Bronze → Silver: среднее профессиональное образование.",
)
def spo_silver(context: AssetExecutionContext) -> None:
    from transformations.silver_spo import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()

    context.add_output_metadata({"total_rows": MetadataValue.int(count)})
    context.log.info("Silver spo: %d rows written", count)
    _refresh_views(context)


@asset(
    group_name="silver",
    deps=["normalized_row_gate"],
    description="Bronze → Silver: высшее образование (бакалавриат, специалитет, магистратура).",
)
def vpo_silver(context: AssetExecutionContext) -> None:
    from transformations.silver_vpo import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()

    context.add_output_metadata({"total_rows": MetadataValue.int(count)})
    context.log.info("Silver vpo: %d rows written", count)
    _refresh_views(context)


@asset(
    group_name="silver",
    deps=["normalized_row_gate"],
    description="Bronze → Silver: дополнительное профессиональное образование.",
)
def dpo_silver(context: AssetExecutionContext) -> None:
    from transformations.silver_dpo import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()

    context.add_output_metadata({"total_rows": MetadataValue.int(count)})
    context.log.info("Silver dpo: %d rows written", count)
    _refresh_views(context)


@asset(
    group_name="silver",
    deps=["oo_silver", "spo_silver", "vpo_silver", "dpo_silver", "doshkolka_silver", "naselenie_silver"],
    description="Сборная таблица silver.education_population_wide из всех источников.",
)
def education_population_wide_silver(context: AssetExecutionContext) -> None:
    from transformations.silver_education_population_wide import run
    from validation.validate_silver_education_population_wide import run as run_validation

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()
    
    cat = _get_catalog()
    report_path = run_validation(cat)

    context.add_output_metadata({
        "total_rows": MetadataValue.int(count),
        "report_path": MetadataValue.path(report_path),
    })
    context.log.info("Silver education_population_wide: %d rows written", count)
    context.log.info("Validation report saved to: %s", report_path)
    _refresh_views(context)


@asset(
    group_name="silver",
    deps=["education_population_wide_silver"],
    description=(
        "Годовая детализация silver.education_population_wide: "
        "возрастные диапазоны развёртываются в отдельные годы (0–80) "
        "с убывающим линейным распределением для открытых диапазонов. "
        "Генерирует анализ охвата образованием по возрастам."
    ),
)
def education_population_wide_annual_silver(context: AssetExecutionContext) -> None:
    from transformations.silver_education_population_wide_annual import run
    from validation.validate_silver_education_population_wide_annual import run as run_validation
    from validation.validate_coverage_analysis import run as run_coverage_analysis

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()

    cat = _get_catalog()
    report_path = run_validation(cat)
    coverage_report_path = run_coverage_analysis(cat)

    context.add_output_metadata({
        "total_rows": MetadataValue.int(count),
        "report_path": MetadataValue.path(report_path),
        "coverage_report_path": MetadataValue.path(coverage_report_path),
    })
    context.log.info("Silver education_population_wide_annual: %d rows written", count)
    context.log.info("Validation report saved to: %s", report_path)
    context.log.info("Coverage analysis report saved to: %s", coverage_report_path)
    _refresh_views(context)
