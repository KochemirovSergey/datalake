"""
Генератор HTML-дашборда «Состояние инфраструктуры ВПО — общежития и площади».
Портирован из generate_dormitory_heatmap.py — логика генерации HTML без подключения к БД.
"""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd


SIGMA_RED_THRESHOLD = 5.0

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

// Ширина столбцов пропорциональна численности обучающихся
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

// Диапазон абс. значений по году
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


def build_dashboard(
    df: pd.DataFrame,
    student_weights: dict,
    regions_json_path,
    output_path: str,
) -> str:
    """
    Генерирует HTML-дашборд «Инфраструктура ВПО — общежития» и сохраняет в output_path.

    Args:
        df: DataFrame из silver.dormitory_infrastructure
            (колонки: регион, year, is_forecast, метрики, сигмы и пр.)
        student_weights: dict {region_lower_iso → student_count}
        regions_json_path: путь к regions_final.json или None
        output_path: куда сохранить HTML

    Returns:
        output_path
    """
    iso_to_name: dict = {}
    iso_to_lon: dict  = {}

    if regions_json_path is not None:
        rp = Path(regions_json_path)
        if rp.exists():
            regions_data = json.loads(rp.read_text(encoding="utf-8"))
            for ru_name, info in regions_data.get("regions", {}).items():
                iso_key = info["iso"].strip().lower()
                display = ru_name.strip()
                display = display[0].upper() + display[1:]
                iso_to_name[iso_key] = display
                iso_to_lon[iso_key]  = info["longitude"]

    def to_display(reg_lower: str) -> str:
        return iso_to_name.get(reg_lower, reg_lower[0].upper() + reg_lower[1:] if reg_lower else reg_lower)

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
            if v is None or (hasattr(v, "__class__") and pd.isna(v)):
                return None
            return round(float(v), dec)

        rec = {
            "region":            reg_display,
            "year":              int(row["year"]),
            "is_forecast":       bool(row["is_forecast"]),
            "area_total":        _f("area_total",       1),
            "area_need_repair":  _f("area_need_repair", 1),
            "area_in_repair":    _f("area_in_repair",   1),
            "area_emergency":    _f("area_emergency",   1),
            "dorm_need":         _f("dorm_need",        0),
            "dorm_live":         _f("dorm_live",        0),
            "dorm_shortage_abs": _f("dorm_shortage_abs", 0),
            "sigma_sum":         _f("sigma_sum",        3),
            "alert_flag":        int(row["alert_flag"]) if pd.notna(row.get("alert_flag")) else 0,
        }
        for mc in metric_cols:
            rec[mc] = _f(mc, 4)
        records.append(rec)

    region_lon_display: dict    = {}
    region_weight_display: dict = {}
    for reg_lower in df["регион"].unique():
        dn  = to_display(reg_lower)
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
    html = html.replace("__JSON_DATA__",      json.dumps(records,               ensure_ascii=False))
    html = html.replace("__REGION_LON__",     json.dumps(region_lon_display,    ensure_ascii=False))
    html = html.replace("__REGION_WEIGHT__",  json.dumps(region_weight_display, ensure_ascii=False))

    Path(output_path).write_text(html, encoding="utf-8")
    return output_path
