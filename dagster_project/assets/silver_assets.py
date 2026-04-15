import logging
import os
import sys
import tempfile
from datetime import date

from dagster import AssetExecutionContext, MetadataValue, asset
from dagster_project.resources import S3Config

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


def _build_dashboard_dataframes(cat):
    """
    Строит wide_df и tidy_df для coverage_dashboard.build_dashboard
    из silver.education_population_wide_annual.

    wide_df: регион × возраст → охват (%), колонки: регион, код, 0..80
    tidy_df: регион × возраст → отклонение от общероссийского среднего (пп)
    """
    import pandas as pd

    try:
        raw = cat.load_table("silver.education_population_wide_annual").scan().to_pandas()
    except Exception as e:
        raise RuntimeError(f"Не удалось загрузить silver.education_population_wide_annual: {e}") from e

    if raw.empty:
        raise RuntimeError("silver.education_population_wide_annual пуста")

    # Справочник регионов
    try:
        lookup = cat.load_table("bronze.region_lookup").scan().to_pandas()
        if "is_alias" in lookup.columns:
            lookup = lookup[lookup["is_alias"] == False]
        name_col = "canonical_name" if "canonical_name" in lookup.columns else "name_variant"
        code_to_name = {str(r["region_code"]): str(r.get(name_col, r["region_code"]))
                        for _, r in lookup.iterrows()}
    except Exception:
        code_to_name = {}

    # Привести типы
    df = raw.copy()
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df = df[df["age"].notna()].copy()
    df["age"] = df["age"].astype(int)

    # Только строки, пригодные для расчёта доли
    df_ratio = df[
        df["education_total"].notna()
        & df["population_total"].notna()
        & (df["population_total"] > 0)
    ].copy()
    df_ratio["row_share"] = df_ratio["education_total"] / df_ratio["population_total"]

    # Региональный средний охват
    regional = (
        df_ratio.groupby(["region_code", "age"], as_index=False)
        .agg(avg_share=("row_share", "mean"), n_years=("year", "nunique"))
    )

    # Общероссийский средний охват по возрасту
    nat_agg = (
        df_ratio.groupby("age", as_index=False)
        .agg(sum_edu=("education_total", "sum"), sum_pop=("population_total", "sum"))
    )
    nat_agg["national_share"] = nat_agg["sum_edu"] / nat_agg["sum_pop"]
    national_map = dict(zip(nat_agg["age"], nat_agg["national_share"]))

    # wide_df: pivot region × age → охват % + столбцы регион / код
    pivot = regional.pivot(index="region_code", columns="age", values="avg_share") * 100
    pivot.columns = [str(c) for c in pivot.columns]
    wide_df = pivot.reset_index()
    wide_df.insert(0, "регион", wide_df["region_code"].map(lambda c: code_to_name.get(c, c)))
    wide_df.insert(1, "код", wide_df["region_code"])
    wide_df = wide_df.drop(columns=["region_code"])

    # tidy_df: регион × возраст → отклонение_пп
    regional["national_share"] = regional["age"].map(national_map)
    regional["отклонение_пп"] = (regional["avg_share"] - regional["national_share"]) * 100
    regional["регион"] = regional["region_code"].map(lambda c: code_to_name.get(c, c))
    tidy_df = regional[["регион", "age", "отклонение_пп"]].rename(columns={"age": "возраст"})

    return wide_df, tidy_df


@asset(
    group_name="silver",
    deps=["education_population_wide_silver"],
    description=(
        "Годовая детализация silver.education_population_wide: "
        "возрастные диапазоны развёртываются в отдельные годы (0–80) "
        "с убывающим линейным распределением для открытых диапазонов. "
        "Генерирует анализ охвата и HTML-дашборд, загружает его в S3."
    ),
)
def education_population_wide_annual_silver(
    context: AssetExecutionContext,
    s3_config: S3Config,
) -> None:
    from transformations.silver_education_population_wide_annual import run
    from validation.validate_silver_education_population_wide_annual import run as run_validation
    from validation.validate_coverage_analysis import run as run_coverage_analysis
    from coverage_dashboard import build_dashboard
    from utils.s3_utils import get_s3_client, upload_html

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()

    cat = _get_catalog()
    report_path = run_validation(cat)
    coverage_report_path = run_coverage_analysis(cat)

    # ── Coverage dashboard ────────────────────────────────────────────────────
    run_id   = context.run_id[:8]
    date_str = date.today().isoformat()
    html_name = f"coverage_{date_str}_{run_id}.html"

    wide_df, tidy_df = _build_dashboard_dataframes(cat)

    with tempfile.TemporaryDirectory() as tmpdir:
        local_html = os.path.join(tmpdir, html_name)
        build_dashboard(wide_df, tidy_df, local_html)
        context.log.info("Dashboard HTML built: %s", local_html)

        s3 = get_s3_client(
            s3_config.endpoint_url,
            s3_config.access_key,
            s3_config.secret_key,
        )
        s3_key = f"dashboards/coverage/{date_str}/{html_name}"
        dashboard_url = upload_html(
            s3, s3_config.bucket, s3_key, local_html, s3_config.public_base_url
        )
        context.log.info("Dashboard uploaded: %s", dashboard_url)

    context.add_output_metadata({
        "total_rows":          MetadataValue.int(count),
        "report_path":         MetadataValue.path(report_path),
        "coverage_report_path": MetadataValue.path(coverage_report_path),
        "dashboard_url":       MetadataValue.url(dashboard_url),
        "dashboard_s3_key":    MetadataValue.text(s3_key),
    })
    context.log.info("Silver education_population_wide_annual: %d rows written", count)
    context.log.info("Validation report saved to: %s", report_path)
    context.log.info("Coverage analysis report saved to: %s", coverage_report_path)
    _refresh_views(context)
