"""
Dagster assets линии «Инфраструктура ВПО — общежития»:
  dormitory_area_bronze
  dormitory_dorm_bronze
  dormitory_silver
  dormitory_dashboard
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
    description="PostgreSQL → Bronze: таблица впо_2_р1_3_8 (площади учебных зданий ВПО).",
)
def dormitory_area_bronze(context: AssetExecutionContext) -> None:
    from ingestion.postgres_loader import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stats = run(tables=["впо_2_р1_3_8"])

    total = sum(stats.values())
    breakdown = "\n".join(
        f"- **{t}**: {n} строк" if n > 0 else f"- **{t}**: пропущено (уже загружено)"
        for t, n in stats.items()
    )
    context.add_output_metadata({
        "total_rows": MetadataValue.int(total),
        "breakdown":  MetadataValue.md(breakdown),
    })
    context.log.info("dormitory_area_bronze: %d строк загружено/обновлено", total)
    _refresh_views(context)


@asset(
    group_name="bronze_extraction",
    description=(
        "PostgreSQL → Bronze: таблица впо_2_р1_4_10 "
        "(нуждаемость в общежитиях и фактическое проживание ВПО)."
    ),
)
def dormitory_dorm_bronze(context: AssetExecutionContext) -> None:
    from ingestion.postgres_loader import run

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stats = run(tables=["впо_2_р1_4_10"])

    total = sum(stats.values())
    breakdown = "\n".join(
        f"- **{t}**: {n} строк" if n > 0 else f"- **{t}**: пропущено (уже загружено)"
        for t, n in stats.items()
    )
    context.add_output_metadata({
        "total_rows": MetadataValue.int(total),
        "breakdown":  MetadataValue.md(breakdown),
    })
    context.log.info("dormitory_dorm_bronze: %d строк загружено/обновлено", total)
    _refresh_views(context)


# ── Silver ─────────────────────────────────────────────────────────────────────

@asset(
    group_name="dormitory",
    deps=["dormitory_area_bronze", "dormitory_dorm_bronze", "discipuli_bronze"],
    description=(
        "Bronze → Silver: расчёт метрик инфраструктуры ВПО (общежития, площади), "
        "прогноз на 5 лет, расчёт сигм и флагов аномалий."
    ),
)
def dormitory_silver(context: AssetExecutionContext) -> None:
    from transformations.dormitory_pipeline import run as run_pipeline
    from validation.validate_dormitory import run as run_validation

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cat = _get_catalog()
    count, n_hist, n_fc = run_pipeline(cat)

    report_path = run_validation(cat)

    # Статистика из Silver
    import pandas as pd
    df = cat.load_table("silver.dormitory_infrastructure").scan().to_pandas()

    n_regions = df["region_code"].nunique() if not df.empty else 0

    if not df.empty:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        hist_df = df[~df["is_forecast"]]
        hist_year_min = int(hist_df["year"].min()) if not hist_df.empty else 0
        hist_year_max = int(hist_df["year"].max()) if not hist_df.empty else 0
        fc_df = df[df["is_forecast"]]
        fc_year_max = int(fc_df["year"].max()) if not fc_df.empty else hist_year_max

        last_hist = hist_year_max
        last_df = hist_df[hist_df["year"] == last_hist]
        df["alert_flag"] = pd.to_numeric(df.get("alert_flag", 0), errors="coerce").fillna(0)
        n_alert = int(last_df["alert_flag"].fillna(0).sum())
    else:
        hist_year_min = hist_year_max = fc_year_max = n_alert = 0

    context.add_output_metadata({
        "total_rows":      MetadataValue.int(count if count > 0 else len(df)),
        "n_regions":       MetadataValue.int(n_regions),
        "hist_year_min":   MetadataValue.int(hist_year_min),
        "hist_year_max":   MetadataValue.int(hist_year_max),
        "forecast_year_max": MetadataValue.int(fc_year_max),
        "n_alert_regions": MetadataValue.int(n_alert),
        "report_path":     MetadataValue.path(report_path),
    })
    context.log.info("dormitory_silver: записано %d строк", count)
    context.log.info("Validation report: %s", report_path)
    _refresh_views(context)


# ── Dashboard ──────────────────────────────────────────────────────────────────

@asset(
    group_name="dormitory",
    deps=["dormitory_silver"],
    description=(
        "Silver → S3: генерация HTML-дашборда «Инфраструктура ВПО — общежития» "
        "и загрузка в S3."
    ),
)
def dormitory_dashboard(
    context: AssetExecutionContext,
    s3_config: S3Config,
) -> None:
    import pandas as pd
    from dashboards.dormitory_dashboard import build_dashboard
    from utils.s3_utils import get_s3_client, upload_html

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cat = _get_catalog()

    df = cat.load_table("silver.dormitory_infrastructure").scan().to_pandas()
    if df.empty:
        context.log.warning("silver.dormitory_infrastructure пуста — дашборд не строится")
        return

    # Переименование: region_code → регион (для совместимости с build_dashboard)
    df = df.rename(columns={"region_code": "регион"})
    df["регион"] = df["регион"].astype(str).str.strip().str.lower()

    for col in ["area_total", "area_need_repair", "area_in_repair", "area_emergency",
                "dorm_need", "dorm_live", "dorm_shortage_abs",
                "metric_1", "metric_2", "metric_3", "metric_4",
                "metric_1_mean", "metric_2_mean", "metric_3_mean", "metric_4_mean",
                "metric_1_sigma", "metric_2_sigma", "metric_3_sigma", "metric_4_sigma",
                "metric_1_penalty", "metric_2_penalty", "metric_3_penalty", "metric_4_penalty",
                "sigma_sum"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Веса регионов из bronze.discipuli (последний год, student_count)
    student_weights: dict = {}
    try:
        disc = cat.load_table("bronze.discipuli").scan().to_pandas()
        disc = disc[
            (disc["Определение"] == "Численность обучающихся")
            & (disc["Уровень субъектности"] == "Регион")
            & (disc["гос/негос"] == "сумма")
        ].copy()
        disc["Год"] = pd.to_numeric(disc["Год"], errors="coerce")
        last_year = int(disc["Год"].max())
        disc = disc[disc["Год"] == last_year]
        disc["val"] = pd.to_numeric(disc["Значение"], errors="coerce").fillna(0)
        for _, r in disc.groupby("Объект")["val"].sum().reset_index().iterrows():
            student_weights[str(r["Объект"]).strip().lower()] = float(r["val"])
    except Exception as e:
        context.log.warning("Не удалось загрузить веса регионов из discipuli: %s", e)

    n_regions  = df["регион"].nunique()
    hist_years = sorted(df[~df["is_forecast"]]["year"].unique())
    fc_years   = sorted(df[df["is_forecast"]]["year"].unique())
    run_id     = context.run_id[:8]
    date_str   = date.today().isoformat()
    html_name  = f"dormitory_{date_str}_{run_id}.html"
    regions_p  = _regions_json if _regions_json.exists() else None

    with tempfile.TemporaryDirectory() as tmpdir:
        local_html = os.path.join(tmpdir, html_name)
        build_dashboard(df, student_weights, regions_p, local_html)
        context.log.info("Dashboard HTML built: %s", local_html)

        s3 = get_s3_client(
            s3_config.endpoint_url,
            s3_config.access_key,
            s3_config.secret_key,
        )
        s3_key = f"dashboards/dormitory/{date_str}/{html_name}"
        dashboard_url = upload_html(
            s3, s3_config.bucket, s3_key, local_html, s3_config.public_base_url,
        )
        context.log.info("Dashboard uploaded: %s", dashboard_url)

    context.add_output_metadata({
        "dashboard_url":    MetadataValue.url(dashboard_url),
        "dashboard_s3_key": MetadataValue.text(s3_key),
        "n_regions":        MetadataValue.int(n_regions),
        "n_hist_years":     MetadataValue.int(len(hist_years)),
        "n_fc_years":       MetadataValue.int(len(fc_years)),
    })
    _refresh_views(context)
