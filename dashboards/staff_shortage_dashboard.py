"""
Генератор HTML-дашборда «Нехватка кадров».
Портирован из generate_heatmap.py — логика генерации HTML без подключения к БД.
"""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd


HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Нехватка кадров — тепловая карта</title>
<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { width: 100%; }
  body {
    font-family: Arial, sans-serif;
    background: #f8fafc;
    min-height: 100vh;
    padding: 16px 12px;
  }
  h1 { font-size: 18px; color: #1F2937; margin-bottom: 4px; text-align: center; }
  .subtitle { font-size: 12px; color: #6B7280; margin-bottom: 14px; text-align: center; }

  #heatmap-panel {
    width: 100%;
    background: white;
    border-radius: 14px;
    box-shadow: 0 6px 32px rgba(0,0,0,0.13);
    padding: 22px 8px 28px;
    overflow-x: hidden;
  }
  .hm-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 16px;
  }
  .hm-title    { font-size: 16px; font-weight: bold; color: #1F2937; }
  .hm-subtitle { font-size: 12px; color: #6B7280; margin-top: 3px; }

  /* Легенда */
  .hm-legend {
    display: flex;
    gap: 18px;
    align-items: center;
    margin-bottom: 10px;
    font-size: 11px;
    color: #374151;
  }
  .hm-legend-item { display: flex; align-items: center; gap: 5px; }
  .hm-swatch { width: 14px; height: 14px; border-radius: 2px; flex-shrink: 0; }

  #hm-svg-container { width: 100%; overflow-x: hidden; }

  /* Tooltip */
  #hm-cell-tooltip {
    position: fixed;
    background: white;
    border: 1px solid #E5E7EB;
    border-radius: 8px;
    padding: 10px 14px;
    font-size: 12px;
    color: #1F2937;
    pointer-events: none;
    box-shadow: 0 4px 18px rgba(0,0,0,0.12);
    min-width: 260px;
    max-width: 360px;
    display: none;
    z-index: 300;
    line-height: 1.6;
  }
</style>
</head>
<body>

<h1>Нехватка кадров — распределение по регионам и уровням образования</h1>
<p class="subtitle">
  Год данных: <strong>__YEAR__</strong> &nbsp;·&nbsp;
  Регионов: <strong>__N_REGIONS__</strong> &nbsp;·&nbsp;
  Уровней: <strong>__N_LEVELS__</strong> &nbsp;·&nbsp;
  Сформировано: __GEN_DT__
</p>

<div id="heatmap-panel">
  <div class="hm-header">
    <div>
      <div class="hm-title">Балльная оценка регионов (0–5) · по уровню образования</div>
      <div class="hm-subtitle">Оценка основана на отклонении от среднего по ставке и вакансиям · наведите на ячейку для детализации · кликните на уровень для таблицы по годам</div>
    </div>
  </div>
  <div class="hm-legend">
    <span style="color:#6B7280;white-space:nowrap">Отклонение от среднего:</span>
    <div style="display:flex;align-items:center;gap:6px">
      <span style="font-size:11px;color:#22C55E">0σ</span>
      <div style="width:160px;height:12px;border-radius:6px;
                  background:linear-gradient(to right,#22C55E,#FBBF24,#F87171);"></div>
      <span style="font-size:11px;color:#F87171">3σ</span>
    </div>
    <div style="font-size:10px;color:#6B7280;margin-top:4px;max-width:300px">
      (три стандартных отклонения)
    </div>
  </div>
  <div id="hm-svg-container"></div>
</div>
<div id="hm-cell-tooltip"></div>

<script>
// ── Данные, подставленные Python-скриптом ─────────────────────────────────
const HEATMAP_DATA = __JSON_DATA__;
const HEATMAP_DATA_ALL = __JSON_DATA_ALL__;
const COLOR_RANGES = __COLOR_RANGES__;
const REGION_LON = __REGION_LON__;
// ──────────────────────────────────────────────────────────────────────────

const COLOR_NONE = "#E5E7EB";

// Плавный градиент: зелёный (лучше) → красный (хуже) с мягкими оттенками
const _colorScale = d3.scaleLinear()
  .domain([0, 0.5, 1.0])
  .range(["#22C55E", "#FBBF24", "#F87171"])
  .interpolate(d3.interpolateRgb);

function gradColor(t) { return _colorScale(t); }

// levelScales определяется ниже в блоке данных (по HEATMAP_DATA_ALL)

// ── Геометрия — адаптивная ────────────────────────────────────────────────
const ROW_H  = 50;
const margin = { top: 148, right: 8, bottom: 46, left: 108 };
const _svgContainer = document.getElementById("hm-svg-container");
const COL_TOTAL = Math.max(400, (_svgContainer.offsetWidth || window.innerWidth) - margin.left - margin.right);

// Уровни и регионы
const levels  = [...new Set(HEATMAP_DATA.map(d => d.level))];
// Сортируем регионы по debuf_count убывающий (для первого уровня), иначе по имени
const regSet  = [...new Set(HEATMAP_DATA.map(d => d.region))];

// Суммарный student_count по региону (для ширины колонок)
const regPop  = {};
HEATMAP_DATA.forEach(d => {
  regPop[d.region] = (regPop[d.region] || 0) + d.student_count;
});
const totalPop = Object.values(regPop).reduce((a, b) => a + b, 0) || 1;

// Сортируем регионы по долготе — с запада на восток
const regionsSorted = regSet.slice().sort((a, b) => {
  const lonA = REGION_LON[a] ?? 999;
  const lonB = REGION_LON[b] ?? 999;
  return lonA - lonB;
});

// Ширины и X-позиции колонок пропорционально student_count
const colW = regionsSorted.map(r => Math.max(1, (regPop[r] || 0) / totalPop * COL_TOTAL));
const colX = [];
let _cx = 0;
colW.forEach(w => { colX.push(_cx); _cx += w; });

const svgW = margin.left + COL_TOTAL + margin.right;

const hmSvg = d3.select("#hm-svg-container").append("svg")
  .attr("width", svgW)
  .style("display", "block");

const gHm = hmSvg.append("g")
  .attr("transform", `translate(${margin.left},${margin.top})`);

// Слои SVG в нужном порядке
const gRows    = gHm.append("g").attr("class", "g-rows");
const gDividers= gHm.append("g").attr("class", "g-dividers").attr("pointer-events", "none");
const gLabels  = gHm.append("g").attr("class", "g-labels");
const gRegLabs = gHm.append("g").attr("class", "g-reglabs").attr("pointer-events", "none");

// Легенда снизу — позиция обновляется динамически
const legendText = hmSvg.append("text")
  .attr("class", "legend-text")
  .attr("x", margin.left)
  .attr("font-size", "9px").attr("fill", "#9CA3AF")
  .attr("font-family", "Arial,sans-serif")
  .text("\u2194 ширина колонки пропорциональна численности обучающихся в регионе");

// ── Данные: latestYear, индекс, шкалы, исторические годы ─────────────────
const latestYear = Math.max(...HEATMAP_DATA.map(d => d.year));

const dataIndex = {};
HEATMAP_DATA_ALL.forEach(d => {
  if (!dataIndex[d.level]) dataIndex[d.level] = {};
  if (!dataIndex[d.level][d.year]) dataIndex[d.level][d.year] = {};
  dataIndex[d.level][d.year][d.region] = d;
});

// Нормировка: от минимума (по всем годам уровня) до 5.0
const levelScales = {};
levels.forEach(lev => {
  const allScores = HEATMAP_DATA_ALL
    .filter(d => d.level === lev && d.score !== null)
    .map(d => d.score);
  const minVal = allScores.length > 0 ? Math.min(...allScores) : 0;
  const maxVal = 5.0;
  levelScales[lev] = v => {
    if (v === null || v === undefined) return 0.5;
    return 1 - (v - minVal) / (maxVal - minVal);
  };
});

// Исторические годы для каждого уровня (по убыванию, без latestYear)
const levelHistYears = {};
levels.forEach(lev => {
  levelHistYears[lev] = [...new Set(
    HEATMAP_DATA_ALL.filter(d => d.level === lev).map(d => d.year)
  )].sort((a, b) => b - a).filter(y => y !== latestYear);
});

// Состояние раскрытых уровней и хранилище групп строк
const expandedLevels = new Set();
const rowGMap = {}; // "level__year" → g element

// ── Список строк по текущему состоянию ───────────────────────────────────
function buildRowList() {
  const rows = [];
  levels.forEach(lev => {
    rows.push({ level: lev, year: latestYear, isMain: true });
    if (expandedLevels.has(lev)) {
      levelHistYears[lev].forEach(yr => {
        rows.push({ level: lev, year: yr, isMain: false });
      });
    }
  });
  return rows;
}

// ── Рендер ячеек одной строки в группу g ─────────────────────────────────
function renderRowCells(g, lev, year) {
  const rowData = dataIndex[lev]?.[year] || {};
  regionsSorted.forEach((reg, ri) => {
    const d = rowData[reg];
    const x = colX[ri];
    const w = Math.max(0.6, colW[ri]);
    const col = d && d.score !== null ? gradColor(levelScales[lev](d.score)) : COLOR_NONE;

    const cellG = g.append("g")
      .attr("transform", `translate(${x}, 0)`)
      .style("cursor", d ? "pointer" : "default");

    cellG.append("rect")
      .attr("width", w - 0.4).attr("height", ROW_H - 0.5)
      .attr("fill", col);

    if (d) {
      cellG
        .on("mouseover", ev => {
          d3.select(ev.currentTarget).select("rect")
            .attr("stroke", "#374151").attr("stroke-width", Math.min(2, w * 0.3));
          showTip(ev, d, reg, lev);
        })
        .on("mousemove", moveTip)
        .on("mouseout", ev => {
          d3.select(ev.currentTarget).select("rect")
            .attr("stroke", null).attr("stroke-width", null);
          hideTip();
        });
    }
  });
}

// ── Обновить разделители + подписи уровней/годов ─────────────────────────
function updateLabelsAndDividers(rows) {
  gDividers.selectAll("*").remove();
  gLabels.selectAll("*").remove();

  // Разделители между строками
  for (let ri = 0; ri <= rows.length; ri++) {
    gDividers.append("line")
      .attr("x1", 0).attr("x2", COL_TOTAL)
      .attr("y1", ri * ROW_H).attr("y2", ri * ROW_H)
      .attr("stroke", "white").attr("stroke-width", 1.5);
  }

  rows.forEach((row, ri) => {
    const y = ri * ROW_H + ROW_H / 2;
    if (row.isMain) {
      const isExpanded = expandedLevels.has(row.level);
      const labelG = gLabels.append("g")
        .style("cursor", "pointer")
        .on("click", () => toggleLevel(row.level));

      // Фон-кнопка для удобного клика
      labelG.append("rect")
        .attr("x", -(margin.left)).attr("y", ri * ROW_H)
        .attr("width", margin.left - 5).attr("height", ROW_H)
        .attr("fill", "transparent");

      // Разбиваем название на строки: каждое слово на новой строке
      const arrow = isExpanded ? " \u25b2" : " \u25bc";
      const words = row.level.split(/ +/);
      const textEl = labelG.append("text")
        .attr("x", -10)
        .attr("text-anchor", "end")
        .attr("font-family", "Arial,sans-serif").attr("font-size", "9px")
        .attr("font-weight", "bold").attr("fill", "#374151");

      const lineH = 11; // высота строки в пикселях
      const totalH = words.length * lineH;
      const startY = y - totalH / 2 + lineH / 2;
      words.forEach((word, wi) => {
        const isLast = wi === words.length - 1;
        textEl.append("tspan")
          .attr("x", -10)
          .attr("y", startY + wi * lineH)
          .text(isLast ? word + arrow : word);
      });
    } else {
      // Подпись года для исторической строки
      gLabels.append("text")
        .attr("x", -10).attr("y", y)
        .attr("text-anchor", "end").attr("dominant-baseline", "middle")
        .attr("font-family", "Arial,sans-serif").attr("font-size", "8px")
        .attr("font-weight", "normal").attr("fill", "#9CA3AF")
        .text(row.year);
    }
  });
}

// ── Полный перерендер ─────────────────────────────────────────────────────
function redraw(animate) {
  const rows = buildRowList();
  const totalH = rows.length * ROW_H + margin.top + margin.bottom;

  if (animate) {
    hmSvg.transition().duration(380).attr("height", totalH);
  } else {
    hmSvg.attr("height", totalH);
  }
  legendText.attr("y", totalH - margin.bottom + 16);

  const seenKeys = new Set();
  rows.forEach((row, ri) => {
    const key = row.level + "__" + row.year;
    seenKeys.add(key);
    const targetY = ri * ROW_H;

    if (!rowGMap[key]) {
      const g = gRows.append("g")
        .attr("class", "row-group")
        .attr("transform", `translate(0, ${targetY})`);
      rowGMap[key] = g;
      renderRowCells(g, row.level, row.year);
      if (!row.isMain) {
        g.attr("opacity", 0).transition().duration(300).delay(60).attr("opacity", 1);
      }
    } else {
      if (animate) {
        rowGMap[key].transition().duration(380).attr("transform", `translate(0, ${targetY})`);
      } else {
        rowGMap[key].attr("transform", `translate(0, ${targetY})`);
      }
    }
  });

  // Удаляем строки которых нет в новом состоянии
  Object.keys(rowGMap).forEach(key => {
    if (!seenKeys.has(key)) {
      const g = rowGMap[key];
      g.transition().duration(220).attr("opacity", 0).on("end", () => g.remove());
      delete rowGMap[key];
    }
  });

  updateLabelsAndDividers(rows);
}

// ── Переключение уровня ───────────────────────────────────────────────────
function toggleLevel(lev) {
  hideTip();
  if (expandedLevels.has(lev)) {
    expandedLevels.delete(lev);
  } else {
    expandedLevels.add(lev);
  }
  redraw(true);
}

// ── Подписи регионов — вертикальные сверху (статичные) ───────────────────
const EDGE_REGIONS = new Set(["Калининградская область", "Чукотский автономный округ"]);
const FORCED_LABELS = new Set([
  "Смоленская область",
  "Севастополь",
  "Тульская область",
  "Республика адыгея",
  "Орловская область",
  "Карачаево черкесская республика",
  "Астраханская область",
  "Томская область",
  "Амурская область",
  "Республика саха якутия",
  "Хабаровский край",
]);
regionsSorted.forEach((reg, ri) => {
  const w      = colW[ri];
  const isEdge = EDGE_REGIONS.has(reg);
  const isForced = FORCED_LABELS.has(reg);
  const fs     = isEdge ? 9 : 8.5;
  if (!isEdge && !isForced && w < fs) return;
  const x = colX[ri] + w / 2;
  gRegLabs.append("text")
    .attr("transform", `translate(${x}, -8) rotate(-90)`)
    .attr("text-anchor", "start")
    .attr("dominant-baseline", "middle")
    .attr("font-family", "Arial,sans-serif")
    .attr("font-size", fs + "px")
    .attr("font-weight", isEdge ? "bold" : "normal")
    .attr("fill", "#374151")
    .text(reg);
});

// Первый рендер (без анимации)
redraw(false);



// ── Tooltip ───────────────────────────────────────────────────────────────
const _tip = document.getElementById("hm-cell-tooltip");

function fmtVal(v, dec) {
  if (v == null) return "—";
  return Number(v).toFixed(dec);
}

function showTip(ev, d, reg, lev) {
  if (!d || d.score === null) {
    _tip.innerHTML = `<strong>${reg}</strong><br/>Нет данных`;
    _tip.style.display = "block";
    moveTip(ev); return;
  }

  // Вычисляем статистику для года и уровня (по всем годам)
  const dataForYearLevel = HEATMAP_DATA_ALL.filter(x => x.year === d.year && x.level === lev);

  // Статистика по ставкам (trig1_val)
  const t1vals = dataForYearLevel.map(x => x.trig1_val).filter(v => v != null);
  const mean_t1 = t1vals.reduce((a, b) => a + b, 0) / t1vals.length;
  const std_t1 = Math.sqrt(t1vals.reduce((sum, v) => sum + Math.pow(v - mean_t1, 2), 0) / t1vals.length);
  const z_t1 = std_t1 > 0 ? (d.trig1_val - mean_t1) / std_t1 : 0;
  const sigma_pct_t1 = (z_t1 * 100).toFixed(1);
  const sign_t1 = z_t1 >= 0 ? "+" : "";
  const color_t1 = z_t1 > 0 ? '#F87171' : '#22C55E';
  const bgColor_t1 = z_t1 > 0 ? '#FFEDEB' : '#DCFCE7';

  // Статистика по вакансиям (trig2_val)
  const t2vals = dataForYearLevel.map(x => x.trig2_val).filter(v => v != null);
  const mean_t2 = t2vals.reduce((a, b) => a + b, 0) / t2vals.length;
  const std_t2 = Math.sqrt(t2vals.reduce((sum, v) => sum + Math.pow(v - mean_t2, 2), 0) / t2vals.length);
  const z_t2 = std_t2 > 0 ? (d.trig2_val - mean_t2) / std_t2 : 0;
  const sigma_pct_t2 = (z_t2 * 100).toFixed(1);
  const sign_t2 = z_t2 >= 0 ? "+" : "";
  const color_t2 = z_t2 > 0 ? '#F87171' : '#22C55E';
  const bgColor_t2 = z_t2 > 0 ? '#FFEDEB' : '#DCFCE7';

  _tip.innerHTML = `
    <div style="font-weight:bold;font-size:13px;color:#111827;margin-bottom:2px">
      ${reg} · ${lev}
    </div>
    <div style="color:#6B7280;font-size:11px;margin-bottom:8px">Год: ${d.year} · Обучающихся: <strong style="color:#111827">${d.student_count.toLocaleString("ru")}</strong></div>

    <table style="border-collapse:collapse;width:100%;font-size:11px;border:1px solid #E5E7EB;border-radius:4px;overflow:hidden">
      <thead style="background:#F3F4F6">
        <tr>
          <td style="padding:6px 8px;font-weight:bold;color:#374151;text-align:left;border-right:1px solid #E5E7EB">Показатель</td>
          <td style="padding:6px 8px;font-weight:bold;color:#374151;text-align:right;border-right:1px solid #E5E7EB;min-width:70px">Регион</td>
          <td style="padding:6px 8px;font-weight:bold;color:#374151;text-align:right;border-right:1px solid #E5E7EB;min-width:90px">Среднее РФ</td>
          <td style="padding:6px 8px;font-weight:bold;color:#374151;text-align:center;min-width:80px">Отклон. σ</td>
        </tr>
      </thead>
      <tbody>
        <tr style="border-top:1px solid #E5E7EB">
          <td style="padding:6px 8px;color:#6B7280;border-right:1px solid #E5E7EB">Ставка на 1 учителя</td>
          <td style="padding:6px 8px;color:#111827;font-weight:600;text-align:right;border-right:1px solid #E5E7EB">${d.trig1_val.toFixed(3)}</td>
          <td style="padding:6px 8px;color:#111827;font-weight:600;text-align:right;border-right:1px solid #E5E7EB">${mean_t1.toFixed(3)}</td>
          <td style="padding:6px 8px;background:${bgColor_t1};color:${color_t1};font-weight:700;text-align:center;border-radius:3px">${sign_t1}${sigma_pct_t1}%</td>
        </tr>
        <tr style="border-top:1px solid #E5E7EB">
          <td style="padding:6px 8px;color:#6B7280;border-right:1px solid #E5E7EB">Незаполненные ставки</td>
          <td style="padding:6px 8px;color:#111827;font-weight:600;text-align:right;border-right:1px solid #E5E7EB">${(d.trig2_val * 100).toFixed(1)}%</td>
          <td style="padding:6px 8px;color:#111827;font-weight:600;text-align:right;border-right:1px solid #E5E7EB">${(mean_t2 * 100).toFixed(1)}%</td>
          <td style="padding:6px 8px;background:${bgColor_t2};color:${color_t2};font-weight:700;text-align:center;border-radius:3px">${sign_t2}${sigma_pct_t2}%</td>
        </tr>
      </tbody>
    </table>`;
  _tip.style.display = "block";
  moveTip(ev);
}

function moveTip(ev) {
  const x = ev.clientX + 16;
  const y = ev.clientY - 10;
  const ox = x + _tip.offsetWidth - window.innerWidth;
  _tip.style.left = (ox > 0 ? ev.clientX - _tip.offsetWidth - 12 : x) + "px";
  _tip.style.top  = Math.max(4, y) + "px";
}

function hideTip() { _tip.style.display = "none"; }

</script>
</body>
</html>
"""


def build_dashboard(
    df: pd.DataFrame,
    year: int,
    regions_json_path,
    output_path: str,
) -> str:
    """
    Генерирует HTML-дашборд «Нехватка кадров» и сохраняет в output_path.

    Args:
        df: DataFrame с колонками region, year, level, student_count, trig1_val, trig2_val, score
        year: последний год для отображения (главный слой хитмапа)
        regions_json_path: путь к regions_final.json или None
        output_path: куда сохранить HTML

    Returns:
        output_path
    """
    region_lon: dict = {}
    region_name: dict = {}

    if regions_json_path is not None:
        rp = Path(regions_json_path)
        if rp.exists():
            regions_data = json.loads(rp.read_text(encoding="utf-8"))
            for name, info in regions_data.get("regions", {}).items():
                iso = info["iso"]
                display_name = name[0].upper() + name[1:]
                region_lon[display_name] = info["longitude"]
                region_name[iso] = display_name

    # Данные последнего года для главного слоя
    df_last = df[df["year"] == year].copy()

    # Динамическая шкала цветов
    color_ranges: dict = {}
    for (year_val, level), grp in df.groupby(["year", "level"]):
        scores = grp["score"].dropna()
        if len(scores) > 0:
            key = f"{year_val}_{level}"
            color_ranges[key] = {"min": float(scores.min()), "max": 5.0}

    # Записи для последнего года
    records_latest = []
    for _, row in df_last.iterrows():
        iso = str(row["region"])
        region_display = region_name.get(iso, iso)
        score = float(row["score"]) if pd.notna(row.get("score")) else None
        records_latest.append({
            "region":        region_display,
            "year":          int(row["year"]),
            "level":         str(row["level"]),
            "student_count": int(row["student_count"]) if pd.notna(row.get("student_count")) else 0,
            "trig1_val":     round(float(row["trig1_val"]), 3) if pd.notna(row.get("trig1_val")) else None,
            "trig2_val":     round(float(row["trig2_val"]), 3) if pd.notna(row.get("trig2_val")) else None,
            "score":         round(score, 2) if score is not None else None,
        })

    # Полные исторические записи
    records_all = []
    for _, row in df.iterrows():
        iso = str(row["region"])
        region_display = region_name.get(iso, iso)
        score = float(row["score"]) if pd.notna(row.get("score")) else None
        records_all.append({
            "region":        region_display,
            "year":          int(row["year"]),
            "level":         str(row["level"]),
            "score":         round(score, 2) if score is not None else None,
            "trig1_val":     round(float(row["trig1_val"]), 3) if pd.notna(row.get("trig1_val")) else None,
            "trig2_val":     round(float(row["trig2_val"]), 3) if pd.notna(row.get("trig2_val")) else None,
            "student_count": int(row["student_count"]) if pd.notna(row.get("student_count")) else 0,
        })

    n_regions = df_last["region"].nunique()
    n_levels  = df_last["level"].nunique()
    gen_dt    = datetime.now().strftime("%d.%m.%Y %H:%M")

    html = HTML_TEMPLATE
    html = html.replace("__YEAR__",          str(year))
    html = html.replace("__N_REGIONS__",     str(n_regions))
    html = html.replace("__N_LEVELS__",      str(n_levels))
    html = html.replace("__GEN_DT__",        gen_dt)
    html = html.replace("__JSON_DATA__",     json.dumps(records_latest, ensure_ascii=False))
    html = html.replace("__JSON_DATA_ALL__", json.dumps(records_all,    ensure_ascii=False))
    html = html.replace("__COLOR_RANGES__",  json.dumps(color_ranges,   ensure_ascii=False))
    html = html.replace("__REGION_LON__",    json.dumps(region_lon,     ensure_ascii=False))

    Path(output_path).write_text(html, encoding="utf-8")
    return output_path
