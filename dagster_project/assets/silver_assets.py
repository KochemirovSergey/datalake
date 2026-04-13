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


@asset(
    group_name="silver",
    deps=["doshkolka_silver", "naselenie_silver"],
    description=(
        "Валидация всего Silver-слоя: покрытие по регионам и годам "
        "для doshkolka и naselenie. Сохраняет Markdown-отчёт в reports/."
    ),
)
def silver_validation(context: AssetExecutionContext) -> None:
    from validation.validate_silver import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cat = _get_catalog()

    report_path = run(cat)

    context.add_output_metadata({
        "report_path": MetadataValue.path(report_path),
    })
    context.log.info("Silver validation report saved: %s", report_path)
    _refresh_views(context)


# ── Новые образовательные ассеты (ТЗ: silver education) ─────────────────────────

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
    deps=["oo_silver", "spo_silver", "vpo_silver", "dpo_silver"],
    description=(
        "Валидация образовательных витрин Silver-слоя: покрытие по уровням, "
        "возрастам, регионам и годам. Сохраняет Markdown-отчёт в reports/."
    ),
)
def education_silver_validation(context: AssetExecutionContext) -> None:
    from validation.validate_silver_education import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cat = _get_catalog()

    report_path = run(cat)

    # Собираем статистику по уровням для UI
    per_level = {}
    try:
        import pandas as pd
        for tbl_name, level_code in [
            ("silver.oo", None),
            ("silver.spo", None),
            ("silver.vpo", None),
            ("silver.dpo", None),
        ]:
            try:
                df = cat.load_table(tbl_name).scan().to_pandas()
                if level_code:
                    per_level[level_code] = {
                        "regions": df["region_code"].nunique() if not df.empty else 0,
                        "rows": len(df),
                    }
                else:
                    for lvl in df["level_code"].unique() if not df.empty else []:
                        sub = df[df["level_code"] == lvl]
                        per_level[lvl] = {
                            "regions": sub["region_code"].nunique(),
                            "rows": len(sub),
                        }
            except Exception as e:
                context.log.warning("Ошибка загрузки %s: %s", tbl_name, e)
    except Exception as e:
        context.log.warning("Ошибка сбора статистики: %s", e)

    context.add_output_metadata({
        "report_path": MetadataValue.path(report_path),
        "per_level": MetadataValue.json(per_level),
    })
    context.log.info("Education silver validation report saved: %s", report_path)
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
