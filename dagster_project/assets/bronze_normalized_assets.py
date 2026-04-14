"""
Dagster assets для слоя bronze_normalized.

  normalized_region     — нормализует регион для всех источников
  normalized_year       — нормализует год для всех источников
  normalized_education  — нормализует education_level для источников с конфигом
  normalized_row_gate   — gate: агрегирует три измерения, проставляет ready_for_silver
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


def _refresh_views(context: AssetExecutionContext) -> None:
    from scripts.refresh_duckdb import run
    run()
    context.log.info("DuckDB views refreshed")


@asset(
    group_name="bronze_normalized",
    deps=["doshkolka_bronze", "naselenie_bronze", "postgres_bronze"],
    description=(
        "Нормализует регион для всех источников (doshkolka, naselenie, postgres OO/СПО/ВПО/ДПО). "
        "Читает справочник из bronze.region_lookup (загружается отдельно: regions_bronze). "
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
    _refresh_views(context)


@asset(
    group_name="bronze_normalized",
    deps=["doshkolka_bronze", "naselenie_bronze", "postgres_bronze"],
    description=(
        "Нормализует год для всех источников (doshkolka, naselenie, postgres OO/СПО/ВПО/ДПО). "
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
    _refresh_views(context)


@asset(
    group_name="bronze_normalized",
    deps=["doshkolka_bronze", "postgres_bronze"],
    description=(
        "Нормализует education_level для источников с education_level-конфигом. "
        "Читает справочник из bronze.education_level_lookup (загружается отдельно: education_level_lookup_bronze). "
        "Записывает результаты в bronze_normalized.education_level "
        "и bronze_normalized.education_level_error."
    ),
)
def normalized_education(context: AssetExecutionContext) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from transformations.bronze_normalized.education_level_pipeline import run

    cat = _get_catalog()
    stats = run(cat)

    total_ok  = sum(s.get("ok_count", 0)    for s in stats.values() if not s.get("skipped"))
    total_err = sum(s.get("error_count", 0) for s in stats.values() if not s.get("skipped"))
    total     = total_ok + total_err
    coverage  = f"{100 * total_ok / total:.1f}%" if total else "n/a"

    breakdown_lines = []
    for src_id, s in stats.items():
        if s.get("skipped"):
            breakdown_lines.append(f"- **{src_id}**: пропущено (уже обработано)")
        else:
            breakdown_lines.append(
                f"- **{src_id}**: ok={s['ok_count']} error={s['error_count']}"
            )

    context.add_output_metadata({
        "total_rows":  MetadataValue.int(total),
        "ok_count":    MetadataValue.int(total_ok),
        "error_count": MetadataValue.int(total_err),
        "coverage":    MetadataValue.text(coverage),
        "breakdown":   MetadataValue.md("\n".join(breakdown_lines)),
    })
    context.log.info(
        "Education level normalization: total=%d ok=%d error=%d coverage=%s",
        total, total_ok, total_err, coverage,
    )
    _refresh_views(context)


@asset(
    group_name="bronze_normalized",
    deps=["normalized_region", "normalized_year", "normalized_education"],
    description=(
        "Gate: агрегирует три измерения (region, year, education_level) по row_id. "
        "Проставляет ready_for_silver. Перестраивается заново при каждом запуске."
    ),
)
def normalized_row_gate(context: AssetExecutionContext) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from transformations.bronze_normalized.row_gate_pipeline import run

    cat = _get_catalog()
    result = run(cat)

    total     = result["total"]
    ready     = result["ready"]
    not_ready = result["not_ready"]
    pct       = f"{100 * ready / total:.1f}%" if total else "n/a"

    context.add_output_metadata({
        "total_rows":  MetadataValue.int(total),
        "ready":       MetadataValue.int(ready),
        "not_ready":   MetadataValue.int(not_ready),
        "ready_pct":   MetadataValue.text(pct),
    })
    context.log.info(
        "Row gate: total=%d ready=%d not_ready=%d (%s готово к Silver)",
        total, ready, not_ready, pct,
    )
    _refresh_views(context)


