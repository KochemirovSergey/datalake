"""
Генератор HTML-дашборда «Состояние инфраструктуры ВПО — общежития и площади»
Тепловая карта по регионам × годам: аномалии (сигмы) + прогноз ARIMA на 5 лет.

Источники данных:
  - public.впо_2_р1_3_8  (площади учебных зданий)
  - public.впо_2_р1_4_10 (численность нуждающихся в общежитиях)
"""

import json
import logging
import sys
import warnings
from pathlib import Path
from datetime import datetime
from typing import Optional, List

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------

SRC_DB = {
    "host":     "localhost",
    "port":     5432,
    "database": "etl_db",
    "user":     "etl_user",
    "password": "etl_password",
}

OUTPUT_HTML    = Path(__file__).parent / "dormitory_dashboard.html"
FORECAST_YEARS = 5
MIN_HISTORY    = 5
SIGMA_RED_THRESHOLD = 5.0

ABS_SERIES = [
    "area_total", "area_need_repair", "area_in_repair",
    "area_emergency", "dorm_need", "dorm_live",
]

# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            Path(__file__).parent / "generate_dormitory.log", encoding="utf-8"
        ),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Подключение к БД
# ---------------------------------------------------------------------------

def get_connection():
    try:
        conn = psycopg2.connect(**SRC_DB)
        log.info(
            "Подключение к БД установлено (%s:%s/%s)",
            SRC_DB["host"], SRC_DB["port"], SRC_DB["database"],
        )
        return conn
    except psycopg2.OperationalError as exc:
        log.error("Ошибка подключения к БД: %s", exc)
        raise


def query_df(conn, sql: str, params=None) -> pd.DataFrame:
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        df = pd.DataFrame(rows)
        log.info("Запрос вернул %d строк", len(df))
        return df
    except psycopg2.Error as exc:
        log.error("Ошибка SQL-запроса: %s", exc)
        raise


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

SQL_STUDENT_COUNT = """
SELECT
    "Объект"        AS region_id,
    SUM("Значение") AS student_count
FROM discipuli
WHERE "Определение"             = 'Численность обучающихся'
  AND "Уровень субъектности"    = 'Регион'
  AND "гос/негос"               = 'сумма'
  AND "Год" = (
      SELECT MAX("Год") FROM discipuli
      WHERE "Определение"          = 'Численность обучающихся'
        AND "Уровень субъектности" = 'Регион'
        AND "гос/негос"            = 'сумма'
  )
GROUP BY "Объект";
"""

SQL_AREA = """
SELECT
    регион,
    год::int AS year,
    SUM(CASE WHEN column_name = 'Всего (сумма граф 9-12)'
             THEN значение::numeric ELSE 0 END) AS area_total,
    SUM(CASE WHEN column_name = 'требую-щая капи-тального ремонта'
             THEN значение::numeric ELSE 0 END) AS area_need_repair,
    SUM(CASE WHEN column_name = 'находя-щаяся на капита-льном ремонте'
             THEN значение::numeric ELSE 0 END) AS area_in_repair,
    SUM(CASE WHEN column_name = 'находя-щаяся в аварий-ном состоя-нии'
             THEN значение::numeric ELSE 0 END) AS area_emergency
FROM public.впо_2_р1_3_8
WHERE row_name = 'из нее занятая обучающимися'
  AND column_name IN (
      'Всего (сумма граф 9-12)',
      'требую-щая капи-тального ремонта',
      'находя-щаяся на капита-льном ремонте',
      'находя-щаяся в аварий-ном состоя-нии'
  )
  AND регион IS NOT NULL
GROUP BY регион, год::int;
"""

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

_need_cases = " + ".join(
    f"COALESCE(SUM(CASE WHEN row_name = '{_ROW_NEED}' "
    f"AND column_name = $${c}$$ THEN значение::numeric ELSE 0 END), 0)"
    for c in _DORM_COLS
)
_live_cases = " + ".join(
    f"COALESCE(SUM(CASE WHEN row_name = '{_ROW_LIVE}' "
    f"AND column_name = $${c}$$ THEN значение::numeric ELSE 0 END), 0)"
    for c in _DORM_COLS
)
_col_in_list = ", ".join(f"$${c}$$" for c in _DORM_COLS)

SQL_DORM = f"""
SELECT
    регион,
    год::int AS year,
    {_need_cases} AS dorm_need,
    {_live_cases} AS dorm_live
FROM public.впо_2_р1_4_10
WHERE row_name IN ('{_ROW_NEED}', '{_ROW_LIVE}')
  AND column_name IN ({_col_in_list})
  AND регион IS NOT NULL
GROUP BY регион, год::int;
"""


# ---------------------------------------------------------------------------
# Загрузка и объединение данных
# ---------------------------------------------------------------------------

def load_data(conn) -> tuple:
    log.info("Загружаем численность обучающихся (discipuli)...")
    stud_df = query_df(conn, SQL_STUDENT_COUNT)
    stud_df["student_count"] = pd.to_numeric(stud_df["student_count"], errors="coerce").fillna(0)
    # dict: lowercase iso → student_count
    student_weights = {
        str(r).strip().lower(): float(c)
        for r, c in zip(stud_df["region_id"], stud_df["student_count"])
    }
    log.info("Регионов с численностью обучающихся: %d", len(student_weights))

    log.info("Загружаем данные по площадям (впо_2_р1_3_8)...")
    area_df = query_df(conn, SQL_AREA)

    log.info("Загружаем данные по общежитиям (впо_2_р1_4_10)...")
    dorm_df = query_df(conn, SQL_DORM)

    # Числовые типы
    for col in ["area_total", "area_need_repair", "area_in_repair", "area_emergency"]:
        area_df[col] = pd.to_numeric(area_df[col], errors="coerce").fillna(0.0)
    for col in ["dorm_need", "dorm_live"]:
        dorm_df[col] = pd.to_numeric(dorm_df[col], errors="coerce").fillna(0.0)

    # Нормализация имён регионов
    area_df["регион"] = area_df["регион"].astype(str).str.strip().str.lower()
    dorm_df["регион"] = dorm_df["регион"].astype(str).str.strip().str.lower()
    area_df["year"]   = area_df["year"].astype(int)
    dorm_df["year"]   = dorm_df["year"].astype(int)

    # Исключаем федеральный агрегат
    area_df = area_df[area_df["регион"] != "ru-fed"]
    dorm_df = dorm_df[dorm_df["регион"] != "ru-fed"]

    # Объединяем по региону и году (FULL OUTER JOIN)
    df = area_df.merge(dorm_df, on=["регион", "year"], how="outer")

    for col in ["area_total", "area_need_repair", "area_in_repair",
                "area_emergency", "dorm_need", "dorm_live"]:
        df[col] = df[col].fillna(0.0)

    log.info(
        "Историческая витрина: %d строк, %d регионов, годы %d–%d",
        len(df), df["регион"].nunique(), df["year"].min(), df["year"].max(),
    )
    return df, student_weights


# ---------------------------------------------------------------------------
# Прогноз: линейный тренд (numpy.polyfit degree=1)
# ---------------------------------------------------------------------------

def _forecast_series(values: np.ndarray, horizon: int) -> Optional[List[float]]:
    """
    Линейный тренд по всем историческим точкам.
    x = 0, 1, 2, … n-1; прогноз на x = n, n+1, …, n+horizon-1.
    Отрицательные значения приводятся к 0.
    """
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
    last_year     = int(hist_df["year"].max())
    forecast_yrs  = list(range(last_year + 1, last_year + FORECAST_YEARS + 1))
    regions       = hist_df["регион"].unique()
    results       = []

    for reg in regions:
        reg_df = hist_df[hist_df["регион"] == reg].sort_values("year")

        forecasts: dict = {}
        for col in ABS_SERIES:
            series = reg_df[col].values.astype(float)
            forecasts[col] = _forecast_series(series, FORECAST_YEARS)

        # Если ни одна серия не дала прогноза — пропускаем регион
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


# ---------------------------------------------------------------------------
# Расчёт метрик
# ---------------------------------------------------------------------------

def calc_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Вычисляет 4 относительных показателя и dorm_shortage_abs."""
    df = df.copy()

    at  = df["area_total"]
    anr = df["area_need_repair"]
    air = df["area_in_repair"]
    ae  = df["area_emergency"]
    dn  = df["dorm_need"]
    dl  = df["dorm_live"]

    # metric_1 = area_need_repair / area_total
    df["metric_1"] = np.where(at > 0, anr / at, np.nan)

    # metric_2 = max(area_need_repair - area_in_repair, 0) / area_need_repair
    df["metric_2"] = np.where(
        anr > 0,
        np.maximum(anr - air, 0.0) / anr,
        np.nan,
    )

    # metric_3 = max(dorm_need - dorm_live, 0) / dorm_need
    df["metric_3"] = np.where(dn > 0, np.maximum(dn - dl, 0.0) / dn, np.nan)

    # metric_4 = area_emergency / area_total
    df["metric_4"] = np.where(at > 0, ae / at, np.nan)

    # Абсолютная нехватка мест
    df["dorm_shortage_abs"] = np.maximum(dn - dl, 0.0)

    return df


# ---------------------------------------------------------------------------
# Расчёт сигм
# ---------------------------------------------------------------------------

def calc_sigmas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Для каждого года и каждой метрики рассчитывает:
      - mean по всем регионам
      - z-score (сигма) для каждого региона
      - penalty = max(z, 0)
    Затем sigma_sum = sum(penalties), alert_flag = sigma_sum >= SIGMA_RED_THRESHOLD.
    """
    df = df.copy()

    for m in [1, 2, 3, 4]:
        col = f"metric_{m}"
        mean_col    = f"{col}_mean"
        sigma_col   = f"{col}_sigma"
        penalty_col = f"{col}_penalty"

        # Среднее по году (игнорирует NaN)
        df[mean_col] = df.groupby("year")[col].transform("mean")

        # Стандартное отклонение по году
        std_by_year = df.groupby("year")[col].transform("std").fillna(0.0)

        # z-score: если std=0, z=0; если значение NaN, оставляем NaN/0
        raw_z = np.where(
            std_by_year > 0,
            (df[col] - df[mean_col]) / std_by_year,
            0.0,
        )
        df[sigma_col]   = np.where(df[col].isna(), np.nan,  raw_z)
        df[penalty_col] = np.where(df[col].isna(), 0.0,     np.maximum(raw_z, 0.0))

    df["sigma_sum"] = (
        df["metric_1_penalty"] + df["metric_2_penalty"] +
        df["metric_3_penalty"] + df["metric_4_penalty"]
    )
    df["alert_flag"]    = (df["sigma_sum"] >= SIGMA_RED_THRESHOLD).astype(int)
    df["abs_mode_value"] = df["dorm_shortage_abs"]

    return df


# ---------------------------------------------------------------------------
# Основная обработка
# ---------------------------------------------------------------------------

def process(hist_df: pd.DataFrame) -> pd.DataFrame:
    hist_df = hist_df.copy()
    hist_df["is_forecast"] = False

    fc_df = build_forecast(hist_df)

    df = pd.concat([hist_df, fc_df], ignore_index=True)
    df = calc_metrics(df)
    df = calc_sigmas(df)

    df = df.sort_values(["year", "регион"]).reset_index(drop=True)
    log.info(
        "Итоговая витрина: %d строк, годы %d–%d",
        len(df), df["year"].min(), df["year"].max(),
    )
    return df


# ---------------------------------------------------------------------------
# HTML-шаблон
# ---------------------------------------------------------------------------

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Инфраструктура ВПО — тепловая карта регионов</title>
<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { width: 100%; }
body { font-family: Arial, sans-serif; background: #f8fafc; min-height: 100vh; padding: 16px 12px; }
h1 { font-size: 18px; color: #1F2937; margin-bottom: 4px; text-align: center; }
.subtitle { font-size: 12px; color: #6B7280; margin-bottom: 14px; text-align: center; }

#heatmap-panel {
  width: 100%; background: white;
  border-radius: 14px;
  box-shadow: 0 6px 32px rgba(0,0,0,0.13);
  padding: 22px 8px 28px;
  overflow-x: hidden;
}
.hm-header {
  display: flex; justify-content: space-between; align-items: flex-start;
  margin-bottom: 12px; flex-wrap: wrap; gap: 10px;
}
.hm-title    { font-size: 16px; font-weight: bold; color: #1F2937; }
.hm-subtitle { font-size: 12px; color: #6B7280; margin-top: 3px; }

.mode-switch { display: flex; gap: 6px; align-items: center; }
.mode-btn {
  padding: 5px 14px; border-radius: 20px; border: 1.5px solid #D1D5DB;
  background: white; font-size: 12px; color: #6B7280; cursor: pointer; transition: all .18s;
  font-family: Arial, sans-serif;
}
.mode-btn.active { background: #1D4ED8; color: white; border-color: #1D4ED8; font-weight: 600; }
.mode-btn:hover:not(.active) { border-color: #9CA3AF; color: #374151; }

.hm-legend { display: flex; gap: 14px; align-items: center; margin-bottom: 10px; font-size: 11px; color: #374151; flex-wrap: wrap; min-height: 28px; }
#hm-svg-container { width: 100%; overflow-x: hidden; }

#hm-cell-tooltip {
  position: fixed; background: white; border: 1px solid #E5E7EB;
  border-radius: 8px; padding: 10px 14px; font-size: 12px; color: #1F2937;
  pointer-events: none; box-shadow: 0 4px 18px rgba(0,0,0,0.12);
  min-width: 320px; max-width: 520px; display: none; z-index: 300; line-height: 1.6;
}
</style>
</head>
<body>

<h1>Состояние инфраструктуры ВПО — тепловая карта по регионам и годам</h1>
<p class="subtitle">
  Регионов: <strong>__N_REGIONS__</strong> &nbsp;·&nbsp;
  Фактических лет: <strong>__N_HIST_YEARS__</strong> &nbsp;·&nbsp;
  Прогнозных лет: <strong>__N_FC_YEARS__</strong> &nbsp;·&nbsp;
  Сформировано: __GEN_DT__
</p>

<div id="heatmap-panel">
  <div class="hm-header">
    <div>
      <div class="hm-title" id="hm-main-title">Аномалии по регионам &middot; сумма плохих сигм</div>
      <div class="hm-subtitle">Наведите на ячейку для детализации &middot; прогнозные годы выделены курсивом</div>
    </div>
    <div class="mode-switch">
      <button class="mode-btn active" id="btn-anomaly"  onclick="setMode('anomaly')">Аномалии</button>
      <button class="mode-btn"        id="btn-absolute" onclick="setMode('absolute')">Абсолютные значения</button>
    </div>
  </div>

  <div class="hm-legend" id="hm-legend"></div>
  <div id="hm-svg-container"></div>
</div>
<div id="hm-cell-tooltip"></div>

<script>
// ── Данные ────────────────────────────────────────────────────────────────
const DATA           = __JSON_DATA__;
const REGION_LON     = __REGION_LON__;
const REGION_WEIGHT  = __REGION_WEIGHT__;
const SIGMA_RED      = __SIGMA_RED__;

// ── Вспомогательные функции форматирования ───────────────────────────────
function fmt(v, dec) {
  dec = dec == null ? 2 : dec;
  if (v == null || isNaN(v)) return '—';
  return Number(v).toFixed(dec);
}
function fmtPct(v) {
  if (v == null || isNaN(v)) return '—';
  return (v * 100).toFixed(1) + '%';
}
function fmtInt(v) {
  if (v == null || isNaN(v)) return '—';
  return Math.round(v).toLocaleString('ru');
}

// ── Режим ─────────────────────────────────────────────────────────────────
let currentMode = 'anomaly';

function renderLegend(mode) {
  const leg = document.getElementById('hm-legend');
  if (mode === 'anomaly') {
    leg.innerHTML = `
      <span style="color:#6B7280;white-space:nowrap">Сумма сигм:</span>
      <div style="display:flex;align-items:center;gap:6px">
        <span style="font-size:11px;color:#22C55E">0</span>
        <div style="width:140px;height:12px;border-radius:6px;
                    background:linear-gradient(to right,#22C55E,#FBBF24,#F87171);"></div>
        <span style="font-size:11px;color:#F87171">&ge;${SIGMA_RED}&sigma;</span>
      </div>
      <span style="color:#D1D5DB">&middot;</span>
      <div style="display:flex;align-items:center;gap:5px">
        <div style="width:12px;height:12px;background:#E5E7EB;border-radius:2px;flex-shrink:0"></div>
        <span>нет данных</span>
      </div>
      <div style="display:flex;align-items:center;gap:5px">
        <span style="font-style:italic;color:#7C3AED;font-size:11px;white-space:nowrap">2025 прогноз</span>
        <span style="color:#6B7280">— прогнозный год</span>
      </div>`;
  } else {
    leg.innerHTML = `
      <span style="color:#6B7280;white-space:nowrap">Нехватка мест в общежитии:</span>
      <div style="display:flex;align-items:center;gap:6px">
        <span style="font-size:11px;color:#22C55E">мин</span>
        <div style="width:140px;height:12px;border-radius:6px;
                    background:linear-gradient(to right,#22C55E,#FBBF24,#F87171);"></div>
        <span style="font-size:11px;color:#F87171">макс</span>
      </div>
      <span style="color:#6B7280;font-size:10px">(шкала пересчитывается для каждого года)</span>`;
  }
}

function setMode(mode) {
  currentMode = mode;
  document.getElementById('btn-anomaly').className  = 'mode-btn' + (mode === 'anomaly'  ? ' active' : '');
  document.getElementById('btn-absolute').className = 'mode-btn' + (mode === 'absolute' ? ' active' : '');
  document.getElementById('hm-main-title').textContent = mode === 'anomaly'
    ? 'Аномалии по регионам \u00b7 сумма плохих сигм'
    : 'Абсолютная нехватка мест в общежитиях';
  renderLegend(mode);
  redrawCells();
}

renderLegend('anomaly');

// ── Цвета ─────────────────────────────────────────────────────────────────
const COLOR_NONE = '#E5E7EB';
const _colorScale = d3.scaleLinear()
  .domain([0, 0.5, 1.0])
  .range(['#22C55E', '#FBBF24', '#F87171'])
  .interpolate(d3.interpolateRgb);

function anomalyColor(sigma_sum) {
  if (sigma_sum == null) return COLOR_NONE;
  return _colorScale(Math.min(sigma_sum / SIGMA_RED, 1.0));
}

function absColor(val, minVal, maxVal) {
  if (val == null) return COLOR_NONE;
  if (maxVal === minVal) return _colorScale(0);
  return _colorScale(Math.min(Math.max((val - minVal) / (maxVal - minVal), 0), 1));
}

// ── Геометрия ─────────────────────────────────────────────────────────────
const ROW_H  = 44;
const margin = { top: 148, right: 8, bottom: 44, left: 96 };
const _svgContainer = document.getElementById('hm-svg-container');
const COL_TOTAL = Math.max(400,
  (_svgContainer.offsetWidth || window.innerWidth) - margin.left - margin.right);

// Все регионы и годы
const allRegions = [...new Set(DATA.map(d => d.region))];
const allYears   = [...new Set(DATA.map(d => d.year))].sort((a, b) => a - b);

// Ширина столбцов пропорциональна численности обучающихся (из discipuli, как в основном скрипте)
const regWeight = {};
allRegions.forEach(r => {
  regWeight[r] = REGION_WEIGHT[r] || 1;
});
const totalWeight = Object.values(regWeight).reduce((a, b) => a + b, 0) || 1;

// Сортировка регионов по долготе (запад → восток)
const regionsSorted = allRegions.slice().sort((a, b) => {
  return (REGION_LON[a] ?? 999) - (REGION_LON[b] ?? 999);
});

const colW = regionsSorted.map(r => Math.max(1, regWeight[r] / totalWeight * COL_TOTAL));
const colX = [];
let _cx = 0;
colW.forEach(w => { colX.push(_cx); _cx += w; });

const svgW     = margin.left + COL_TOTAL + margin.right;
const svgTotalH = allYears.length * ROW_H + margin.top + margin.bottom;

const hmSvg = d3.select('#hm-svg-container').append('svg')
  .attr('width', svgW).attr('height', svgTotalH)
  .style('display', 'block');

const gHm = hmSvg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

// Слои SVG
const gCells    = gHm.append('g').attr('class', 'g-cells');
const gDividers = gHm.append('g').attr('class', 'g-dividers').attr('pointer-events', 'none');
const gLabels   = gHm.append('g').attr('class', 'g-labels');
const gRegLabs  = gHm.append('g').attr('class', 'g-reglabs').attr('pointer-events', 'none');

hmSvg.append('text')
  .attr('x', margin.left).attr('y', svgTotalH - margin.bottom + 18)
  .attr('font-size', '9px').attr('fill', '#9CA3AF').attr('font-family', 'Arial,sans-serif')
  .text('\u2194 ширина столбца пропорциональна численности обучающихся в регионе');

// ── Индекс данных ─────────────────────────────────────────────────────────
const dataIndex = {};
DATA.forEach(d => {
  if (!dataIndex[d.year]) dataIndex[d.year] = {};
  dataIndex[d.year][d.region] = d;
});

// Диапазон абс. значений по году для режима «Абсолютные значения»
const absRanges = {};
allYears.forEach(yr => {
  const vals = DATA.filter(d => d.year === yr && d.dorm_shortage_abs != null)
                   .map(d => d.dorm_shortage_abs);
  if (vals.length > 0) {
    absRanges[yr] = { min: Math.min(...vals), max: Math.max(...vals) };
  } else {
    absRanges[yr] = { min: 0, max: 0 };
  }
});

// ── Подписи регионов (вертикальные) ──────────────────────────────────────
regionsSorted.forEach((reg, ri) => {
  const w  = colW[ri];
  const fs = 8.5;
  if (w < fs - 1) return;
  gRegLabs.append('text')
    .attr('transform', `translate(${colX[ri] + w / 2}, -8) rotate(-90)`)
    .attr('text-anchor', 'start').attr('dominant-baseline', 'middle')
    .attr('font-family', 'Arial,sans-serif').attr('font-size', fs + 'px')
    .attr('fill', '#374151')
    .text(reg);
});

// ── Подписи годов (статичные) ─────────────────────────────────────────────
function drawYearLabels() {
  gLabels.selectAll('*').remove();
  allYears.forEach((yr, yi) => {
    const y   = yi * ROW_H + ROW_H / 2;
    const isFc = DATA.some(d => d.year === yr && d.is_forecast);
    const g = gLabels.append('g');

    // Фон для прогнозного года
    if (isFc) {
      g.append('rect')
        .attr('x', -margin.left).attr('y', yi * ROW_H)
        .attr('width', margin.left - 2).attr('height', ROW_H)
        .attr('fill', '#F5F3FF');
    }

    g.append('text')
      .attr('x', -8).attr('y', isFc ? y - 7 : y)
      .attr('text-anchor', 'end').attr('dominant-baseline', 'middle')
      .attr('font-family', 'Arial,sans-serif')
      .attr('font-size', '10px')
      .attr('font-style', isFc ? 'italic' : 'normal')
      .attr('font-weight', isFc ? '600' : 'normal')
      .attr('fill', isFc ? '#7C3AED' : '#374151')
      .text(yr);

    if (isFc) {
      g.append('text')
        .attr('x', -8).attr('y', y + 7)
        .attr('text-anchor', 'end').attr('dominant-baseline', 'middle')
        .attr('font-family', 'Arial,sans-serif').attr('font-size', '7.5px')
        .attr('fill', '#7C3AED').attr('font-style', 'italic')
        .text('прогноз');
    }
  });
}
drawYearLabels();

// ── Разделители строк ─────────────────────────────────────────────────────
for (let yi = 0; yi <= allYears.length; yi++) {
  gDividers.append('line')
    .attr('x1', 0).attr('x2', COL_TOTAL)
    .attr('y1', yi * ROW_H).attr('y2', yi * ROW_H)
    .attr('stroke', 'white').attr('stroke-width', 1.5);
}

// ── Рендер ячеек ──────────────────────────────────────────────────────────
function cellColor(d, yr) {
  if (!d) return COLOR_NONE;
  if (currentMode === 'anomaly') {
    // Все 4 метрики NULL → нейтральный
    if (d.metric_1 == null && d.metric_2 == null &&
        d.metric_3 == null && d.metric_4 == null) return COLOR_NONE;
    return anomalyColor(d.sigma_sum);
  } else {
    const { min, max } = absRanges[yr] || { min: 0, max: 0 };
    return absColor(d.dorm_shortage_abs, min, max);
  }
}

function redrawCells() {
  gCells.selectAll('*').remove();
  allYears.forEach((yr, yi) => {
    const rowData = dataIndex[yr] || {};
    const isFc   = DATA.some(d => d.year === yr && d.is_forecast);
    const rowY   = yi * ROW_H;

    regionsSorted.forEach((reg, ri) => {
      const d = rowData[reg];
      const x = colX[ri];
      const w = Math.max(0.6, colW[ri]);
      const col = cellColor(d, yr);

      const cellG = gCells.append('g')
        .attr('transform', `translate(${x}, ${rowY})`)
        .style('cursor', d ? 'pointer' : 'default');

      cellG.append('rect')
        .attr('width', w - 0.4).attr('height', ROW_H - 0.4)
        .attr('fill', col);

      if (d) {
        cellG
          .on('mouseover', ev => {
            d3.select(ev.currentTarget).select('rect')
              .attr('stroke', '#374151')
              .attr('stroke-width', Math.min(2, w * 0.35));
            showTip(ev, d, reg, yr, isFc);
          })
          .on('mousemove', moveTip)
          .on('mouseout', ev => {
            d3.select(ev.currentTarget).select('rect')
              .attr('stroke', null).attr('stroke-width', null);
            hideTip();
          });
      }
    });
  });
}

redrawCells();

// ── Tooltip ───────────────────────────────────────────────────────────────
const _tip = document.getElementById('hm-cell-tooltip');

// ── Заглушка: таблица проблемных вузов ───────────────────────────────────
// Данных по отдельным вузам нет — показываем макет будущего раздела.
// Строки генерируются псевдослучайно на основе региона и года (детерминировано).
function stubVuzTable(reg, yr) {
  // Детерминированный псевдослучайный генератор (seed = reg+yr)
  let seed = 0;
  for (let i = 0; i < reg.length; i++) seed = (seed * 31 + reg.charCodeAt(i)) & 0xffffffff;
  seed = (seed * 31 + yr) & 0xffffffff;
  function rand() {
    seed = (seed * 1664525 + 1013904223) & 0xffffffff;
    return (seed >>> 0) / 4294967296;
  }

  const templates = [
    'Классический университет', 'Технический университет', 'Педагогический университет',
    'Медицинский университет', 'Аграрный университет', 'Политехнический институт',
    'Гуманитарный институт', 'Экономический университет', 'Архитектурный институт',
    'Юридический институт', 'Транспортный университет', 'Горный институт',
  ];

  const n = 3 + Math.floor(rand() * 3); // 3–5 вузов
  const rows = [];
  for (let i = 0; i < n; i++) {
    const name = templates[Math.floor(rand() * templates.length)];
    const students = Math.round(1000 + rand() * 15000);
    const pRepair  = +(rand() * 0.45).toFixed(2);       // доля треб. кап. ремонта
    const pActive  = +(rand() * pRepair).toFixed(2);    // доля в активном ремонте
    const pEmerg   = +(rand() * 0.12).toFixed(2);       // доля аварийных

    const isProb = pRepair > 0.25 || pEmerg > 0.06;
    const rowBg  = isProb ? '#FEF2F2' : 'white';
    const badge  = isProb
      ? '<span style="background:#FEE2E2;color:#DC2626;border-radius:3px;padding:0 4px;font-size:9px;font-weight:700;margin-left:4px">!</span>'
      : '';

    const tdS = 'padding:3px 6px;border-right:1px solid #F3F4F6;font-size:10px';
    const pct = v => (v * 100).toFixed(0) + '%';
    const colRepair = pRepair > 0.25 ? '#DC2626' : '#374151';
    const colEmerg  = pEmerg  > 0.06 ? '#DC2626' : '#374151';

    rows.push(`<tr style="border-top:1px solid #F3F4F6;background:${rowBg}">
      <td style="${tdS};max-width:140px;white-space:normal;line-height:1.3">${name}${badge}</td>
      <td style="${tdS};text-align:right">${students.toLocaleString('ru')}</td>
      <td style="${tdS};text-align:right;color:${colRepair};font-weight:${pRepair>0.25?'700':'400'}">${pct(pRepair)}</td>
      <td style="${tdS};text-align:right">${pct(pActive)}</td>
      <td style="${tdS};text-align:right;color:${colEmerg};font-weight:${pEmerg>0.06?'700':'400'};border-right:none">${pct(pEmerg)}</td>
    </tr>`);
  }

  return `
    <div style="margin-top:10px;border-top:1px dashed #E5E7EB;padding-top:8px">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:5px">
        <span style="font-size:11px;font-weight:700;color:#374151">Проблемные вузы региона</span>
        <span style="font-size:9px;color:#9CA3AF;background:#F9FAFB;border:1px solid #E5E7EB;
                     border-radius:3px;padding:1px 5px">заглушка · данные в разработке</span>
      </div>
      <table style="border-collapse:collapse;width:100%;font-size:10px;border:1px solid #E5E7EB;border-radius:4px;overflow:hidden">
        <thead style="background:#F3F4F6">
          <tr>
            <td style="padding:3px 6px;font-weight:bold;border-right:1px solid #E5E7EB;font-size:10px">Наименование</td>
            <td style="padding:3px 6px;font-weight:bold;text-align:right;border-right:1px solid #E5E7EB;white-space:nowrap;font-size:10px">Числен.</td>
            <td style="padding:3px 6px;font-weight:bold;text-align:right;border-right:1px solid #E5E7EB;white-space:nowrap;font-size:10px">Треб. кап. рем.</td>
            <td style="padding:3px 6px;font-weight:bold;text-align:right;border-right:1px solid #E5E7EB;white-space:nowrap;font-size:10px">В ремонте</td>
            <td style="padding:3px 6px;font-weight:bold;text-align:right;white-space:nowrap;font-size:10px">Аварийн.</td>
          </tr>
        </thead>
        <tbody>${rows.join('')}</tbody>
      </table>
    </div>`;
}

function metricRow(label, val, mean, sigma, penalty) {
  const sigmaStr = sigma == null ? '—' : (sigma >= 0 ? '+' : '') + sigma.toFixed(2) + '\u03c3';
  const penStr   = (penalty != null && penalty > 0) ? penalty.toFixed(2) : '0';
  const hasBad   = penalty != null && penalty > 0;
  const bg   = hasBad ? '#FEF2F2' : '#F9FAFB';
  const fc   = hasBad ? '#DC2626' : '#374151';
  return `<tr style="border-top:1px solid #E5E7EB">
    <td style="padding:4px 7px;color:#6B7280;border-right:1px solid #E5E7EB;white-space:nowrap;font-size:10.5px">${label}</td>
    <td style="padding:4px 7px;text-align:right;border-right:1px solid #E5E7EB">${fmtPct(val)}</td>
    <td style="padding:4px 7px;text-align:right;border-right:1px solid #E5E7EB">${fmtPct(mean)}</td>
    <td style="padding:4px 7px;text-align:center;border-right:1px solid #E5E7EB;background:${bg};color:${fc};font-weight:700;font-size:10.5px">${sigmaStr}</td>
    <td style="padding:4px 7px;text-align:center;color:${fc};font-weight:700;font-size:10.5px">${penStr}</td>
  </tr>`;
}

function showTip(ev, d, reg, yr, isFc) {
  const typeBadge = isFc
    ? '<span style="background:#EDE9FE;color:#7C3AED;border-radius:4px;padding:1px 6px;font-size:10px;font-style:italic">прогноз</span>'
    : '<span style="background:#F0FDF4;color:#16A34A;border-radius:4px;padding:1px 6px;font-size:10px">факт</span>';

  let html = `<div style="font-weight:bold;font-size:13px;color:#111827;margin-bottom:4px">
    ${reg} &middot; ${yr} ${typeBadge}
  </div>`;

  if (currentMode === 'anomaly') {
    const allNull = d.metric_1 == null && d.metric_2 == null &&
                    d.metric_3 == null && d.metric_4 == null;
    if (allNull) {
      html += '<div style="color:#6B7280;margin-top:6px">Нет данных для расчёта аномалий</div>';
    } else {
      const sumStr = d.sigma_sum != null ? d.sigma_sum.toFixed(2) : '—';
      const alertBg = d.alert_flag ? '#FEE2E2' : '#F0FDF4';
      const alertFc = d.alert_flag ? '#DC2626' : '#16A34A';
      html += `<div style="background:${alertBg};color:${alertFc};border-radius:5px;
                           padding:5px 10px;margin:5px 0;font-weight:700;font-size:12px">
        Сумма сигм: ${sumStr}${d.alert_flag ? ' \u26a0 аномалия' : ''}
      </div>`;
      html += `<table style="border-collapse:collapse;width:100%;font-size:11px;
                              border:1px solid #E5E7EB;border-radius:4px;overflow:hidden">
        <thead style="background:#F3F4F6">
          <tr>
            <td style="padding:4px 7px;font-weight:bold;border-right:1px solid #E5E7EB">Метрика</td>
            <td style="padding:4px 7px;font-weight:bold;text-align:right;border-right:1px solid #E5E7EB">Регион</td>
            <td style="padding:4px 7px;font-weight:bold;text-align:right;border-right:1px solid #E5E7EB">Ср. РФ</td>
            <td style="padding:4px 7px;font-weight:bold;text-align:center;border-right:1px solid #E5E7EB">Сигма</td>
            <td style="padding:4px 7px;font-weight:bold;text-align:center">Штраф</td>
          </tr>
        </thead><tbody>
        ${metricRow('М1: нуждается кап. ремонт / всего', d.metric_1, d.metric_1_mean, d.metric_1_sigma, d.metric_1_penalty)}
        ${metricRow('М2: не в ремонте / под ремонт', d.metric_2, d.metric_2_mean, d.metric_2_sigma, d.metric_2_penalty)}
        ${metricRow('М3: нехватка общежитий, доля', d.metric_3, d.metric_3_mean, d.metric_3_sigma, d.metric_3_penalty)}
        ${metricRow('М4: аварийные площади / всего', d.metric_4, d.metric_4_mean, d.metric_4_sigma, d.metric_4_penalty)}
        </tbody>
      </table>`;
      html += `<div style="font-size:10px;color:#9CA3AF;margin-top:5px">
        Площадь: всего ${fmtInt(d.area_total)}\u00a0м² &middot;
        Потреб. в общежит.: ${fmtInt(d.dorm_need)} &middot;
        Проживают: ${fmtInt(d.dorm_live)} &middot;
        Нехватка: ${fmtInt(d.dorm_shortage_abs)}
      </div>`;
    }
  } else {
    // Режим абсолютных значений
    html += `<div style="margin-top:6px;font-size:12px">
      <div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #F3F4F6">
        <span style="color:#6B7280">Нуждаются в общежитии</span>
        <strong>${fmtInt(d.dorm_need)}</strong>
      </div>
      <div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #F3F4F6">
        <span style="color:#6B7280">Проживают в общежитии</span>
        <strong>${fmtInt(d.dorm_live)}</strong>
      </div>
      <div style="display:flex;justify-content:space-between;
                  padding:5px 8px;background:#FEF2F2;border-radius:4px;margin-top:6px">
        <span style="color:#DC2626;font-weight:600">Не хватает мест</span>
        <strong style="color:#DC2626">${fmtInt(d.dorm_shortage_abs)}</strong>
      </div>
    </div>`;
  }

  html += stubVuzTable(reg, yr);

  _tip.innerHTML = html;
  _tip.style.display = 'block';
  moveTip(ev);
}

function moveTip(ev) {
  const x  = ev.clientX + 16;
  const y  = ev.clientY - 10;
  const ox = x + _tip.offsetWidth - window.innerWidth;
  _tip.style.left = (ox > 0 ? ev.clientX - _tip.offsetWidth - 12 : x) + 'px';
  _tip.style.top  = Math.max(4, y) + 'px';
}

function hideTip() { _tip.style.display = 'none'; }
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Генерация HTML
# ---------------------------------------------------------------------------

def generate_html(df: pd.DataFrame, student_weights: dict) -> str:
    # Строим маппинги из regions_final.json:
    #   iso_lower → русское название (капитализированное)
    #   iso_lower → долгота
    regions_json_path = Path(__file__).parent / "regions_final.json"
    iso_to_name: dict = {}
    iso_to_lon:  dict = {}
    if regions_json_path.exists():
        regions_data = json.loads(regions_json_path.read_text(encoding="utf-8"))
        for ru_name, info in regions_data.get("regions", {}).items():
            iso_key = info["iso"].strip().lower()   # "ru-mow"
            display = ru_name.strip()
            display = display[0].upper() + display[1:]  # "Москва"
            iso_to_name[iso_key] = display
            iso_to_lon[iso_key]  = info["longitude"]

    def to_display(reg_lower: str) -> str:
        return iso_to_name.get(reg_lower, reg_lower[0].upper() + reg_lower[1:])

    # Статистика
    hist_years = sorted(df[~df["is_forecast"]]["year"].unique())
    fc_years   = sorted(df[df["is_forecast"]]["year"].unique())
    n_regions  = df["регион"].nunique()

    metric_cols = [
        "metric_1", "metric_2", "metric_3", "metric_4",
        "metric_1_mean",    "metric_2_mean",    "metric_3_mean",    "metric_4_mean",
        "metric_1_sigma",   "metric_2_sigma",   "metric_3_sigma",   "metric_4_sigma",
        "metric_1_penalty", "metric_2_penalty", "metric_3_penalty", "metric_4_penalty",
    ]

    records = []
    for _, row in df.iterrows():
        reg_lower   = str(row["регион"])
        reg_display = to_display(reg_lower)

        def _f(col, dec=4):
            v = row.get(col)
            return round(float(v), dec) if (v is not None and pd.notna(v)) else None

        rec = {
            "region":            reg_display,
            "year":              int(row["year"]),
            "is_forecast":       bool(row["is_forecast"]),
            "area_total":        _f("area_total",   1),
            "area_need_repair":  _f("area_need_repair", 1),
            "area_in_repair":    _f("area_in_repair",   1),
            "area_emergency":    _f("area_emergency",   1),
            "dorm_need":         _f("dorm_need", 0),
            "dorm_live":         _f("dorm_live", 0),
            "dorm_shortage_abs": _f("dorm_shortage_abs", 0),
            "sigma_sum":         _f("sigma_sum", 3),
            "alert_flag":        int(row["alert_flag"]) if pd.notna(row.get("alert_flag")) else 0,
        }
        for mc in metric_cols:
            rec[mc] = _f(mc, 4)
        records.append(rec)

    # Долготы и веса: ключ — русское отображаемое имя
    region_lon_display: dict    = {}
    region_weight_display: dict = {}
    for reg_lower in df["регион"].unique():
        dn = to_display(reg_lower)
        lon = iso_to_lon.get(reg_lower)
        if lon is not None:
            region_lon_display[dn] = lon
        wt = student_weights.get(reg_lower)
        if wt and wt > 0:
            region_weight_display[dn] = wt

    gen_dt = datetime.now().strftime("%d.%m.%Y %H:%M")

    html = HTML_TEMPLATE
    html = html.replace("__N_REGIONS__",      str(n_regions))
    html = html.replace("__N_HIST_YEARS__",   str(len(hist_years)))
    html = html.replace("__N_FC_YEARS__",     str(len(fc_years)))
    html = html.replace("__GEN_DT__",         gen_dt)
    html = html.replace("__SIGMA_RED__",      str(SIGMA_RED_THRESHOLD))
    html = html.replace("__JSON_DATA__",      json.dumps(records,                ensure_ascii=False))
    html = html.replace("__REGION_LON__",     json.dumps(region_lon_display,     ensure_ascii=False))
    html = html.replace("__REGION_WEIGHT__",  json.dumps(region_weight_display,  ensure_ascii=False))
    return html


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

def main():
    log.info("=== Старт генерации дашборда «Инфраструктура ВПО» ===")
    conn = get_connection()
    try:
        hist_df, student_weights = load_data(conn)
    finally:
        conn.close()

    df   = process(hist_df)
    html = generate_html(df, student_weights)

    OUTPUT_HTML.write_text(html, encoding="utf-8")
    log.info("HTML сохранён: %s", OUTPUT_HTML)
    log.info("=== Готово ===")


if __name__ == "__main__":
    main()
