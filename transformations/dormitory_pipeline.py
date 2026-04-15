"""
Расчёт метрик инфраструктуры ВПО (общежития и площади) + прогноз.
Портирован из generate_dormitory_heatmap.py. Читает из Bronze Iceberg,
пишет в silver.dormitory_infrastructure.
"""

import logging
import os
from typing import Optional, List

import numpy as np
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

FORECAST_YEARS      = 5
MIN_HISTORY         = 5
SIGMA_RED_THRESHOLD = 5.0

ABS_SERIES = [
    "area_total", "area_need_repair", "area_in_repair",
    "area_emergency", "dorm_need", "dorm_live",
]

_DORM_COLS = (
    "образовательные программы высшего образования "
    "(программы бакалавриата, программы специалитета, программы магистратуры)",
    "образовательные программы подготовки научно-педагогических кадров "
    "в аспирантуре (адъюнктуре), программы ординатуры "
    "и программы ассистентуры-стажировки",
    "образовательные программы подготовки специалистов среднего звена",
    "дополнительные профессиональ- ные программы",
    "программы профессиональ- ного обучения",
    "образовательные программы подготовки квалифицированных рабочих, служащих",
)

_ROW_NEED = "Численность обучающихся, нуждающихся в общежитиях"
_ROW_LIVE = "в том числе проживает в общежитиях"


def _get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "datalake",
        **{
            "uri":       f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )


def load_area(cat: SqlCatalog) -> pd.DataFrame:
    """Загружает данные площадей из bronze.впо_2_р1_3_8.

    Возвращает DataFrame: регион, year, area_total, area_need_repair, area_in_repair, area_emergency.
    """
    log.info("Загружаем bronze.впо_2_р1_3_8...")
    df = cat.load_table("bronze.впо_2_р1_3_8").scan().to_pandas()
    log.info("  Строк в bronze.впо_2_р1_3_8: %d", len(df))

    df = df[df["row_name"] == "из нее занятая обучающимися"].copy()
    df["_val"] = pd.to_numeric(df["значение"], errors="coerce").fillna(0)
    df["регион"] = df["регион"].astype(str).str.strip().str.lower()
    df["year"] = pd.to_numeric(df["год"], errors="coerce").astype("Int64")
    df = df[df["регион"] != "ru-fed"]
    df = df[df["year"].notna()].copy()
    df["year"] = df["year"].astype(int)

    col_map = {
        "Всего (сумма граф 9-12)":               "area_total",
        "требую-щая капи-тального ремонта":       "area_need_repair",
        "находя-щаяся на капита-льном ремонте":   "area_in_repair",
        "находя-щаяся в аварий-ном состоя-нии":   "area_emergency",
    }
    df = df[df["column_name"].isin(col_map)].copy()
    df["metric"] = df["column_name"].map(col_map)

    pivot = (
        df.groupby(["регион", "year", "metric"])["_val"]
        .sum()
        .unstack("metric", fill_value=0)
        .reset_index()
    )
    for col in ["area_total", "area_need_repair", "area_in_repair", "area_emergency"]:
        if col not in pivot.columns:
            pivot[col] = 0.0
        pivot[col] = pivot[col].astype(float)

    log.info("  Строк area после агрегации: %d", len(pivot))
    return pivot


def load_dorm(cat: SqlCatalog) -> pd.DataFrame:
    """Загружает данные по общежитиям из bronze.впо_2_р1_4_10.

    Возвращает DataFrame: регион, year, dorm_need, dorm_live.
    """
    log.info("Загружаем bronze.впо_2_р1_4_10...")
    df = cat.load_table("bronze.впо_2_р1_4_10").scan().to_pandas()
    log.info("  Строк в bronze.впо_2_р1_4_10: %d", len(df))

    df = df[df["row_name"].isin([_ROW_NEED, _ROW_LIVE])].copy()
    df = df[df["column_name"].isin(_DORM_COLS)].copy()
    df["_val"] = pd.to_numeric(df["значение"], errors="coerce").fillna(0)
    df["регион"] = df["регион"].astype(str).str.strip().str.lower()
    df["year"] = pd.to_numeric(df["год"], errors="coerce").astype("Int64")
    df = df[df["регион"] != "ru-fed"]
    df = df[df["year"].notna()].copy()
    df["year"] = df["year"].astype(int)

    need_df = df[df["row_name"] == _ROW_NEED].groupby(["регион", "year"])["_val"].sum().rename("dorm_need")
    live_df = df[df["row_name"] == _ROW_LIVE].groupby(["регион", "year"])["_val"].sum().rename("dorm_live")

    result = need_df.to_frame().join(live_df, how="outer").reset_index()
    result["dorm_need"] = result["dorm_need"].fillna(0).astype(float)
    result["dorm_live"] = result["dorm_live"].fillna(0).astype(float)

    log.info("  Строк dorm после агрегации: %d", len(result))
    return result


def calc_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Вычисляет 4 относительных метрики и dorm_shortage_abs."""
    df = df.copy()
    at  = df["area_total"]
    anr = df["area_need_repair"]
    air = df["area_in_repair"]
    ae  = df["area_emergency"]
    dn  = df["dorm_need"]
    dl  = df["dorm_live"]

    df["metric_1"] = np.where(at > 0, anr / at, np.nan)
    df["metric_2"] = np.where(anr > 0, np.maximum(anr - air, 0.0) / anr, np.nan)
    df["metric_3"] = np.where(dn > 0, np.maximum(dn - dl, 0.0) / dn, np.nan)
    df["metric_4"] = np.where(at > 0, ae / at, np.nan)
    df["dorm_shortage_abs"] = np.maximum(dn - dl, 0.0)
    return df


def _forecast_series(values: np.ndarray, horizon: int) -> Optional[List[float]]:
    """Линейный тренд на horizon шагов вперёд."""
    if len(values) < MIN_HISTORY:
        return None
    try:
        x = np.arange(len(values), dtype=float)
        slope, intercept = np.polyfit(x, values, deg=1)
        n = len(values)
        return [max(0.0, intercept + slope * (n + i)) for i in range(horizon)]
    except Exception as exc:
        log.debug("Ошибка линейного тренда: %s", exc)
        return None


def build_forecast(hist_df: pd.DataFrame) -> pd.DataFrame:
    """Строит прогноз на FORECAST_YEARS лет для каждого региона."""
    last_year    = int(hist_df["year"].max())
    forecast_yrs = list(range(last_year + 1, last_year + FORECAST_YEARS + 1))
    regions      = hist_df["регион"].unique()
    results      = []

    for reg in regions:
        reg_df = hist_df[hist_df["регион"] == reg].sort_values("year")
        forecasts: dict = {}
        for col in ABS_SERIES:
            series = reg_df[col].values.astype(float)
            forecasts[col] = _forecast_series(series, FORECAST_YEARS)

        if all(v is None for v in forecasts.values()):
            continue

        last_vals = {col: float(reg_df[col].iloc[-1]) for col in ABS_SERIES}
        for i, yr in enumerate(forecast_yrs):
            row: dict = {"регион": reg, "year": yr, "is_forecast": True}
            for col in ABS_SERIES:
                fc_list = forecasts[col]
                row[col] = fc_list[i] if fc_list is not None else max(0.0, last_vals[col])
            results.append(row)

    if not results:
        log.warning("Прогноз не построен ни для одного региона")
        return pd.DataFrame(columns=hist_df.columns.tolist())

    fc_df = pd.DataFrame(results)
    log.info(
        "Прогноз построен: %d строк (%d регионов × %d лет)",
        len(fc_df), len(regions), FORECAST_YEARS,
    )
    return fc_df


def calc_sigmas(df: pd.DataFrame) -> pd.DataFrame:
    """Рассчитывает z-score, штраф (penalty=max(z,0)) и флаг аномалии по каждой метрике."""
    df = df.copy()
    for m in [1, 2, 3, 4]:
        col         = f"metric_{m}"
        mean_col    = f"{col}_mean"
        sigma_col   = f"{col}_sigma"
        penalty_col = f"{col}_penalty"

        df[mean_col] = df.groupby("year")[col].transform("mean")
        std_by_year  = df.groupby("year")[col].transform("std").fillna(0.0)

        raw_z = np.where(
            std_by_year > 0,
            (df[col] - df[mean_col]) / std_by_year,
            0.0,
        )
        df[sigma_col]   = np.where(df[col].isna(), np.nan, raw_z)
        df[penalty_col] = np.where(df[col].isna(), 0.0, np.maximum(raw_z, 0.0))

    df["sigma_sum"] = (
        df["metric_1_penalty"] + df["metric_2_penalty"]
        + df["metric_3_penalty"] + df["metric_4_penalty"]
    )
    df["alert_flag"] = (df["sigma_sum"] >= SIGMA_RED_THRESHOLD).astype(int)
    return df


def run(cat: SqlCatalog | None = None) -> tuple:
    """
    Загружает данные из Bronze, рассчитывает метрики + прогноз + сигмы,
    пишет результат в silver.dormitory_infrastructure.

    Returns:
        (count, n_hist_years, n_fc_years)
    """
    if cat is None:
        cat = _get_catalog()

    # Идемпотентность
    tbl = cat.load_table("silver.dormitory_infrastructure")
    existing = tbl.scan(selected_fields=("region_code",)).to_arrow()
    if len(existing) > 0:
        log.info("silver.dormitory_infrastructure уже содержит %d строк, пропускаю", len(existing))
        return (0, 0, 0)

    # Загрузка и объединение
    area_df = load_area(cat)
    dorm_df = load_dorm(cat)

    hist_df = area_df.merge(dorm_df, on=["регион", "year"], how="outer")
    for col in ABS_SERIES:
        hist_df[col] = hist_df[col].fillna(0.0)
    hist_df["is_forecast"] = False

    n_hist_years = hist_df["year"].nunique()

    # Прогноз
    fc_df = build_forecast(hist_df)
    df = pd.concat([hist_df, fc_df], ignore_index=True)
    n_fc_years = df[df["is_forecast"]]["year"].nunique() if not fc_df.empty else 0

    # Метрики и сигмы
    df = calc_metrics(df)
    df = calc_sigmas(df)
    df = df.sort_values(["year", "регион"]).reset_index(drop=True)

    log.info(
        "Итоговая витрина: %d строк, годы %d–%d",
        len(df), int(df["year"].min()), int(df["year"].max()),
    )

    # PyArrow запись
    def _flt(series):
        vals = pd.to_numeric(series, errors="coerce")
        return pa.array([float(v) if pd.notna(v) else None for v in vals], type=pa.float64())

    def _int32(series):
        vals = pd.to_numeric(series, errors="coerce").fillna(0)
        return pa.array(vals.astype(int).tolist(), type=pa.int32())

    pa_schema = pa.schema([
        pa.field("region_code",       pa.string(),  nullable=False),
        pa.field("year",              pa.int32(),   nullable=False),
        pa.field("is_forecast",       pa.bool_(),   nullable=False),
        pa.field("area_total",        pa.float64(), nullable=True),
        pa.field("area_need_repair",  pa.float64(), nullable=True),
        pa.field("area_in_repair",    pa.float64(), nullable=True),
        pa.field("area_emergency",    pa.float64(), nullable=True),
        pa.field("dorm_need",         pa.float64(), nullable=True),
        pa.field("dorm_live",         pa.float64(), nullable=True),
        pa.field("dorm_shortage_abs", pa.float64(), nullable=True),
        pa.field("metric_1",          pa.float64(), nullable=True),
        pa.field("metric_2",          pa.float64(), nullable=True),
        pa.field("metric_3",          pa.float64(), nullable=True),
        pa.field("metric_4",          pa.float64(), nullable=True),
        pa.field("metric_1_mean",     pa.float64(), nullable=True),
        pa.field("metric_2_mean",     pa.float64(), nullable=True),
        pa.field("metric_3_mean",     pa.float64(), nullable=True),
        pa.field("metric_4_mean",     pa.float64(), nullable=True),
        pa.field("metric_1_sigma",    pa.float64(), nullable=True),
        pa.field("metric_2_sigma",    pa.float64(), nullable=True),
        pa.field("metric_3_sigma",    pa.float64(), nullable=True),
        pa.field("metric_4_sigma",    pa.float64(), nullable=True),
        pa.field("metric_1_penalty",  pa.float64(), nullable=True),
        pa.field("metric_2_penalty",  pa.float64(), nullable=True),
        pa.field("metric_3_penalty",  pa.float64(), nullable=True),
        pa.field("metric_4_penalty",  pa.float64(), nullable=True),
        pa.field("sigma_sum",         pa.float64(), nullable=True),
        pa.field("alert_flag",        pa.int32(),   nullable=True),
    ])

    arrow_tbl = pa.table({
        "region_code":       pa.array(df["регион"].astype(str).tolist(), type=pa.string()),
        "year":              _int32(df["year"]),
        "is_forecast":       pa.array(df["is_forecast"].tolist(), type=pa.bool_()),
        "area_total":        _flt(df["area_total"]),
        "area_need_repair":  _flt(df["area_need_repair"]),
        "area_in_repair":    _flt(df["area_in_repair"]),
        "area_emergency":    _flt(df["area_emergency"]),
        "dorm_need":         _flt(df["dorm_need"]),
        "dorm_live":         _flt(df["dorm_live"]),
        "dorm_shortage_abs": _flt(df["dorm_shortage_abs"]),
        "metric_1":          _flt(df["metric_1"]),
        "metric_2":          _flt(df["metric_2"]),
        "metric_3":          _flt(df["metric_3"]),
        "metric_4":          _flt(df["metric_4"]),
        "metric_1_mean":     _flt(df["metric_1_mean"]),
        "metric_2_mean":     _flt(df["metric_2_mean"]),
        "metric_3_mean":     _flt(df["metric_3_mean"]),
        "metric_4_mean":     _flt(df["metric_4_mean"]),
        "metric_1_sigma":    _flt(df["metric_1_sigma"]),
        "metric_2_sigma":    _flt(df["metric_2_sigma"]),
        "metric_3_sigma":    _flt(df["metric_3_sigma"]),
        "metric_4_sigma":    _flt(df["metric_4_sigma"]),
        "metric_1_penalty":  _flt(df["metric_1_penalty"]),
        "metric_2_penalty":  _flt(df["metric_2_penalty"]),
        "metric_3_penalty":  _flt(df["metric_3_penalty"]),
        "metric_4_penalty":  _flt(df["metric_4_penalty"]),
        "sigma_sum":         _flt(df["sigma_sum"]),
        "alert_flag":        _int32(df["alert_flag"]),
    }, schema=pa_schema)

    tbl.append(arrow_tbl)
    count = len(arrow_tbl)
    log.info("Записано %d строк в silver.dormitory_infrastructure", count)
    return (count, n_hist_years, n_fc_years)
