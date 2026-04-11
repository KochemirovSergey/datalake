"""
Dagster assets для слоя bronze_normalized.

  normalized_region     — нормализует регион для всех источников
  normalized_year       — нормализует год для всех источников
  normalized_validation — генерирует отчёт о покрытии
"""

import logging
import os
import sys

from dagster import AssetExecutionContext, MetadataValue, asset

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

log = logging.getLogger(__name__)


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
    group_name="bronze_normalized",
    deps=["excel_bronze", "population_bronze", "regions_bronze"],
    description=(
        "Нормализует регион для всех источников (doshkolka, naselenie). "
        "Записывает результаты в bronze_normalized.region и bronze_normalized.region_error."
    ),
)
def normalized_region(context: AssetExecutionContext) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from transformations.bronze_normalized.region_pipeline import run

    cat = _get_catalog()
    stats = run(cat)

    total_ok = sum(s.get("ok_count", 0) for s in stats.values() if not s.get("skipped"))
    total_err = sum(s.get("error_count", 0) for s in stats.values() if not s.get("skipped"))
    total = total_ok + total_err
    coverage = f"{100 * total_ok / total:.1f}%" if total else "n/a"

    breakdown_lines = []
    for src_id, s in stats.items():
        if s.get("skipped"):
            breakdown_lines.append(f"- **{src_id}**: пропущено (уже обработано)")
        else:
            breakdown_lines.append(
                f"- **{src_id}**: ok={s['ok_count']} error={s['error_count']}"
            )

    context.add_output_metadata({
        "total_rows":   MetadataValue.int(total),
        "ok_count":     MetadataValue.int(total_ok),
        "error_count":  MetadataValue.int(total_err),
        "coverage":     MetadataValue.text(coverage),
        "breakdown":    MetadataValue.md("\n".join(breakdown_lines)),
    })
    context.log.info(
        "Region normalization: total=%d ok=%d error=%d coverage=%s",
        total, total_ok, total_err, coverage,
    )


@asset(
    group_name="bronze_normalized",
    deps=["excel_bronze", "population_bronze", "normalized_region"],
    description=(
        "Нормализует год для всех источников (doshkolka, naselenie). "
        "Записывает результаты в bronze_normalized.year и bronze_normalized.year_error."
    ),
)
def normalized_year(context: AssetExecutionContext) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from transformations.bronze_normalized.year_pipeline import run

    cat = _get_catalog()
    stats = run(cat)

    total_ok = sum(s.get("ok_count", 0) for s in stats.values() if not s.get("skipped"))
    total_err = sum(s.get("error_count", 0) for s in stats.values() if not s.get("skipped"))

    breakdown_lines = []
    for src_id, s in stats.items():
        if s.get("skipped"):
            breakdown_lines.append(f"- **{src_id}**: пропущено (уже обработано)")
        else:
            breakdown_lines.append(
                f"- **{src_id}**: ok={s['ok_count']} error={s['error_count']}"
            )

    context.add_output_metadata({
        "ok_count":    MetadataValue.int(total_ok),
        "error_count": MetadataValue.int(total_err),
        "breakdown":   MetadataValue.md("\n".join(breakdown_lines)),
    })
    context.log.info(
        "Year normalization: ok=%d error=%d", total_ok, total_err,
    )


@asset(
    group_name="bronze_normalized",
    deps=["normalized_region", "normalized_year"],
    description=(
        "Генерирует Markdown-отчёт о покрытии нормализации: "
        "регионы × годы, error summary. Сохраняет в reports/."
    ),
)
def normalized_validation(context: AssetExecutionContext) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from validation.validate_bronze_normalized import run

    cat = _get_catalog()
    report_path = run(cat)

    context.add_output_metadata({
        "report_path": MetadataValue.path(report_path),
    })
    context.log.info("Validation report saved: %s", report_path)
