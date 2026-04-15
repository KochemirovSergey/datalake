"""
Dagster assets линии «Дефицит кадров»:
  discipuli_bronze
  staff_shortage_sources_bronze
  staff_shortage_silver
  staff_shortage_dashboard
"""

import logging
import os
import sys
import tempfile
from datetime import date
from pathlib import Path

from dagster import AssetExecutionContext, MetadataValue, asset
from dagster_project.resources import S3Config

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

_regions_json = Path(_project_root) / "data" / "regions_final.json"


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


# ── Bronze extraction ──────────────────────────────────────────────────────────

@asset(
    group_name="bronze_extraction",
    description="PostgreSQL → Bronze: таблица discipuli (численность обучающихся по регионам).",
)
def discipuli_bronze(context: AssetExecutionContext) -> None:
    from ingestion.postgres_loader import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stats = run(tables=["discipuli"])

    total = sum(stats.values())
    breakdown = "\n".join(
        f"- **{t}**: {n} строк" if n > 0 else f"- **{t}**: пропущено (уже загружено)"
        for t, n in stats.items()
    )
    context.add_output_metadata({
        "total_rows": MetadataValue.int(total),
        "breakdown":  MetadataValue.md(breakdown),
    })
    context.log.info("discipuli_bronze: %d строк загружено/обновлено", total)
    _refresh_views(context)


@asset(
    group_name="bronze_extraction",
    description=(
        "PostgreSQL → Bronze: источники для расчёта дефицита кадров "
        "(oo_1_3_4_230, oo_1_3_1_218, oo_1_3_2_221)."
    ),
)
def staff_shortage_sources_bronze(context: AssetExecutionContext) -> None:
    from ingestion.postgres_loader import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stats = run(tables=["oo_1_3_4_230", "oo_1_3_1_218", "oo_1_3_2_221"])

    total = sum(stats.values())
    breakdown = "\n".join(
        f"- **{t}**: {n} строк" if n > 0 else f"- **{t}**: пропущено (уже загружено)"
        for t, n in stats.items()
    )
    context.add_output_metadata({
        "total_rows": MetadataValue.int(total),
        "breakdown":  MetadataValue.md(breakdown),
    })
    context.log.info("staff_shortage_sources_bronze: %d строк загружено/обновлено", total)
    _refresh_views(context)


# ── Silver ─────────────────────────────────────────────────────────────────────

@asset(
    group_name="staff_shortage",
    deps=["discipuli_bronze", "staff_shortage_sources_bronze"],
    description=(
        "Bronze → Silver: расчёт триггеров и балльной оценки дефицита кадров "
        "по регионам, годам и уровням образования."
    ),
)
def staff_shortage_silver(context: AssetExecutionContext) -> None:
    from transformations.staff_shortage_pipeline import run as run_pipeline
    from validation.validate_staff_shortage import run as run_validation

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cat   = _get_catalog()
    count = run_pipeline(cat)

    report_path = run_validation(cat)

    # Статистика из Silver
    import pandas as pd
    df = cat.load_table("silver.staff_shortage_triggers").scan().to_pandas()
    n_regions = df["region_code"].nunique() if not df.empty else 0
    n_years   = df["year"].nunique() if not df.empty else 0
    levels    = ", ".join(sorted(df["level"].unique())) if not df.empty else "—"

    context.add_output_metadata({
        "total_rows":  MetadataValue.int(count if count > 0 else len(df)),
        "n_regions":   MetadataValue.int(n_regions),
        "n_years":     MetadataValue.int(n_years),
        "levels":      MetadataValue.text(levels),
        "report_path": MetadataValue.path(report_path),
    })
    context.log.info("staff_shortage_silver: записано %d строк", count)
    context.log.info("Validation report: %s", report_path)
    _refresh_views(context)


# ── Dashboard ──────────────────────────────────────────────────────────────────

@asset(
    group_name="staff_shortage",
    deps=["staff_shortage_silver"],
    description=(
        "Silver → S3: генерация HTML-дашборда «Нехватка кадров» "
        "и загрузка в S3."
    ),
)
def staff_shortage_dashboard(
    context: AssetExecutionContext,
    s3_config: S3Config,
) -> None:
    import pandas as pd
    from dashboards.staff_shortage_dashboard import build_dashboard
    from utils.s3_utils import get_s3_client, upload_html

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cat = _get_catalog()

    df = cat.load_table("silver.staff_shortage_triggers").scan().to_pandas()
    if df.empty:
        context.log.warning("silver.staff_shortage_triggers пуста — дашборд не строится")
        return

    for col in ["trig1_val", "trig2_val", "bonus_score", "score"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["student_count"] = pd.to_numeric(df["student_count"], errors="coerce").fillna(0).astype(int)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype(int)

    # DataFrame совместим с generate_html: region=region_code, level, year, student_count, trig*, score
    df = df.rename(columns={"region_code": "region"})

    last_year  = int(df["year"].max())
    n_regions  = df["region"].nunique()
    run_id     = context.run_id[:8]
    date_str   = date.today().isoformat()
    html_name  = f"staff-shortage_{date_str}_{run_id}.html"
    regions_p  = _regions_json if _regions_json.exists() else None

    with tempfile.TemporaryDirectory() as tmpdir:
        local_html = os.path.join(tmpdir, html_name)
        build_dashboard(df, last_year, regions_p, local_html)
        context.log.info("Dashboard HTML built: %s", local_html)

        s3 = get_s3_client(
            s3_config.endpoint_url,
            s3_config.access_key,
            s3_config.secret_key,
        )
        s3_key = f"dashboards/staff-shortage/{date_str}/{html_name}"
        dashboard_url = upload_html(
            s3, s3_config.bucket, s3_key, local_html, s3_config.public_base_url,
        )
        context.log.info("Dashboard uploaded: %s", dashboard_url)

    context.add_output_metadata({
        "dashboard_url":   MetadataValue.url(dashboard_url),
        "dashboard_s3_key": MetadataValue.text(s3_key),
        "n_regions":       MetadataValue.int(n_regions),
        "last_year":       MetadataValue.int(last_year),
    })
    _refresh_views(context)
