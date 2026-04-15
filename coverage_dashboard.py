#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
coverage_dashboard.py
──────────────────────────────────────────────────────────────────────────────
Генерирует интерактивный HTML-дашборд охвата образованием на Plotly.

Публичный API (для вызова из Dagster):
    build_dashboard(wide_df, tidy_df, output_path) -> str

CLI-запуск:
    python coverage_dashboard.py
    (читает CSV из той же папки, генерирует coverage_dashboard.html)

Зависимости:
    pip install pandas plotly
"""

import os
import sys
import pandas as pd
import plotly.graph_objects as go

# ── Настройки ячеек ──────────────────────────────────────────────────────────
CELL_W     = 18     # px — ширина ячейки
CELL_H     = 22     # px — высота строки

# Охват %: шкала
COV_ZMIN, COV_ZMAX = 0, 120
COV_COLORSCALE = [
    [0.00, "#f7fbff"], [0.10, "#c6dbef"],
    [0.40, "#6baed6"], [0.70, "#2171b5"],
    [0.85, "#084594"], [1.00, "#08306b"],
]

# Дельта пп: симметричная красно-зелёная шкала
DELTA_ABS  = 30     # ±30 пп по умолчанию; расширяется до реального max
DELTA_COLORSCALE = [
    [0.00, "#d73027"], [0.20, "#f46d43"],
    [0.35, "#fdae61"], [0.50, "#ffffbf"],
    [0.65, "#a6d96a"], [0.80, "#66bd63"],
    [1.00, "#1a9641"],
]

MARGIN_L, MARGIN_R, MARGIN_T, MARGIN_B = 280, 130, 30, 60
# ─────────────────────────────────────────────────────────────────────────────


def check(path):
    if not os.path.exists(path):
        sys.exit(f"[ERROR] Файл не найден: {path}")


def _extract_wide(df: pd.DataFrame):
    """Извлекает данные охвата из wide-DataFrame (регион × возраст → %)."""
    age_cols = [c for c in df.columns if c not in ("регион", "код")]
    age_cols = [c for c in age_cols if df[c].notna().any()]
    age_cols = sorted(age_cols, key=lambda x: int(x))
    regions  = df["регион"].tolist()
    z        = df[age_cols].values.tolist()
    return regions, age_cols, z


def _extract_tidy(df: pd.DataFrame):
    """Извлекает данные отклонений из tidy-DataFrame (регион × возраст → пп)."""
    df = df.copy()
    df["возраст"] = df["возраст"].astype(str).str.strip('"').astype(int)
    pivot = df.pivot_table(
        index="регион", columns="возраст",
        values="отклонение_пп", aggfunc="mean"
    )
    pivot = pivot.reindex(columns=sorted(pivot.columns))
    regions  = pivot.index.tolist()
    age_cols = [str(c) for c in pivot.columns.tolist()]
    z        = pivot.values.tolist()
    real_max = float(pd.DataFrame(z).abs().max().max())
    delta_lim = max(DELTA_ABS, round(real_max, 0))
    return regions, age_cols, z, delta_lim


def load_wide(wide_csv: str):
    check(wide_csv)
    df = pd.read_csv(wide_csv, sep=";", encoding="utf-8")
    return _extract_wide(df)


def load_tidy(tidy_csv: str):
    check(tidy_csv)
    df = pd.read_csv(tidy_csv, sep=";", encoding="utf-8")
    return _extract_tidy(df)


def make_heatmap(z, x, y, colorscale, zmin, zmax, hover_suffix):
    return go.Heatmap(
        z=z, x=x, y=y,
        colorscale=colorscale, zmin=zmin, zmax=zmax,
        colorbar=dict(
            title=dict(text=hover_suffix, side="right"),
            thickness=14, len=0.92,
        ),
        hoverongaps=False,
        hovertemplate=(
            "<b>%{y}</b><br>Возраст: %{x}<br>"
            + hover_suffix + ": %{z:.1f}<extra></extra>"
        ),
    )


def fig_to_div(fig, div_id):
    return fig.to_html(
        full_html=False, include_plotlyjs=False,
        div_id=div_id,
        config={"scrollZoom": True, "displayModeBar": True},
    )


def build_fig(regions, age_cols, z, colorscale, zmin, zmax, hover_suffix):
    pw = MARGIN_L + len(age_cols) * CELL_W + MARGIN_R
    ph = MARGIN_T + len(regions)  * CELL_H + MARGIN_B
    fig = go.Figure(make_heatmap(z, age_cols, regions, colorscale, zmin, zmax, hover_suffix))
    fig.update_layout(
        xaxis=dict(title="Возраст", tickangle=0, tickfont=dict(size=11), fixedrange=False),
        yaxis=dict(title="", autorange="reversed", tickfont=dict(size=11), fixedrange=False),
        width=pw, height=ph,
        margin=dict(l=MARGIN_L, r=MARGIN_R, t=MARGIN_T, b=MARGIN_B),
        paper_bgcolor="white", plot_bgcolor="white",
    )
    return fig


def _render_html(w_reg, w_ages, w_z, d_reg, d_ages, d_z, dlim,
                 div_cov, div_delta, source_label: str = "") -> str:
    """Собирает финальный HTML-документ из готовых компонентов."""
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Дашборд охвата регионов</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: #f0f2f5;
  color: #1a1a2e;
  min-height: 100vh;
}}

/* ── шапка ── */
header {{
  background: linear-gradient(135deg, #1b3a6b 0%, #2b6cb0 100%);
  color: #fff;
  padding: 18px 32px;
  display: flex;
  align-items: center;
  gap: 32px;
  flex-wrap: wrap;
  box-shadow: 0 2px 12px rgba(0,0,0,.25);
}}
header h1 {{ font-size: 1.25rem; font-weight: 700; flex: 1 1 200px; line-height: 1.25; }}

.stats {{ display: flex; gap: 16px; flex-wrap: wrap; }}
.stat {{
  background: rgba(255,255,255,.15); border-radius: 8px;
  padding: 8px 16px; text-align: center; min-width: 80px;
}}
.stat-value {{ font-size: 1.4rem; font-weight: 800; line-height: 1; }}
.stat-label {{ font-size: .68rem; opacity: .85; margin-top: 2px;
               text-transform: uppercase; letter-spacing: .05em; }}

/* ── рубильник ── */
.toggle-bar {{
  background: #fff;
  border-bottom: 1px solid #dde1ea;
  padding: 14px 32px;
  display: flex; align-items: center; gap: 20px; flex-wrap: wrap;
}}
.toggle-label {{ font-size: .88rem; font-weight: 600; color: #444; }}

.switch-wrap {{
  display: flex; align-items: center; gap: 12px;
  background: #eef1f7; border-radius: 40px; padding: 4px 6px;
}}
.switch-btn {{
  padding: 7px 22px; border-radius: 36px; border: none;
  font-size: .85rem; font-weight: 600; cursor: pointer;
  transition: background .22s, color .22s, box-shadow .22s;
  color: #666; background: transparent;
}}
.switch-btn.active {{
  background: #fff; color: #1b3a6b;
  box-shadow: 0 2px 8px rgba(0,0,0,.12);
}}
.switch-btn[data-mode="delta"].active {{ color: #c0392b; }}

.legend-strip {{
  display: flex; align-items: center; gap: 6px;
  font-size: .78rem; color: #666; margin-left: auto;
}}
.grad-bar {{
  width: 140px; height: 12px; border-radius: 6px;
  border: 1px solid #ddd;
}}
.grad-cov   {{ background: linear-gradient(to right, #f7fbff, #c6dbef, #6baed6, #2171b5, #08306b); }}
.grad-delta {{ background: linear-gradient(to right, #d73027, #fdae61, #ffffbf, #a6d96a, #1a9641); }}

/* ── фильтр ── */
.toolbar {{
  background: #fff; border-bottom: 1px solid #eaecf0;
  padding: 10px 32px; display: flex; align-items: center; gap: 14px; flex-wrap: wrap;
}}
.toolbar input[type=text] {{
  border: 1px solid #c8cdd8; border-radius: 6px;
  padding: 6px 12px; font-size: .88rem; width: 260px; outline: none;
  transition: border-color .18s;
}}
.toolbar input[type=text]:focus {{ border-color: #2b6cb0; }}
.toolbar .hint {{ font-size: .76rem; color: #999; }}

/* ── карта ── */
main {{ padding: 24px 32px 48px; }}
.chart-card {{
  background: #fff; border-radius: 12px;
  box-shadow: 0 2px 16px rgba(0,0,0,.08);
  overflow-x: auto; overflow-y: visible;
  padding: 16px 8px 8px;
}}
.chart-card::-webkit-scrollbar {{ height: 7px; }}
.chart-card::-webkit-scrollbar-track {{ background: #f0f2f5; border-radius: 4px; }}
.chart-card::-webkit-scrollbar-thumb {{ background: #b0bcd0; border-radius: 4px; }}
.chart-card::-webkit-scrollbar-thumb:hover {{ background: #2b6cb0; }}

.chart-pane {{ display: none; }}
.chart-pane.visible {{ display: block; }}

footer {{
  text-align: center; padding: 16px;
  font-size: .76rem; color: #aaa;
}}
</style>
</head>
<body>

<header>
  <h1>Дашборд охвата<br>по регионам и возрасту</h1>
  <div class="stats">
    <div class="stat">
      <div class="stat-value" id="s-regions">—</div>
      <div class="stat-label">Регионов</div>
    </div>
    <div class="stat">
      <div class="stat-value" id="s-ages">—</div>
      <div class="stat-label">Возрастов</div>
    </div>
    <div class="stat" id="s-scale-wrap">
      <div class="stat-value" id="s-scale">0–120%</div>
      <div class="stat-label">Шкала</div>
    </div>
  </div>
</header>

<div class="toggle-bar">
  <span class="toggle-label">Режим:</span>
  <div class="switch-wrap">
    <button class="switch-btn active" data-mode="cov"   onclick="switchMode('cov')">Охват, %</button>
    <button class="switch-btn"        data-mode="delta" onclick="switchMode('delta')">Дельта, пп</button>
  </div>

  <div class="legend-strip" id="legend-cov">
    <span>0%</span>
    <div class="grad-bar grad-cov"></div>
    <span>120%</span>
  </div>
  <div class="legend-strip" id="legend-delta" style="display:none">
    <span>−{dlim:.0f} пп</span>
    <div class="grad-bar grad-delta"></div>
    <span>+{dlim:.0f} пп</span>
  </div>
</div>

<div class="toolbar">
  <label for="regionFilter">🔍 Регион:</label>
  <input type="text" id="regionFilter" placeholder="Введите название региона…">
  <span class="hint">← прокрутите карту вправо для просмотра всех возрастов</span>
</div>

<main>
  <div class="chart-card">
    <div class="chart-pane visible" id="pane-cov">{div_cov}</div>
    <div class="chart-pane"         id="pane-delta">{div_delta}</div>
  </div>
</main>

<footer>
  {f"Источник: {source_label}" if source_label else ""}
</footer>

<script>
// ── Данные по режимам ────────────────────────────────────────────────────────
const META = {{
  cov:   {{ regions: {len(w_reg)}, ages: {len(w_ages)}, scale: "0 – 120 %" }},
  delta: {{ regions: {len(d_reg)}, ages: {len(d_ages)}, scale: "±{dlim:.0f} пп" }},
}};

let currentMode = 'cov';

// ── Рубильник ────────────────────────────────────────────────────────────────
function switchMode(mode) {{
  if (mode === currentMode) return;
  currentMode = mode;

  document.querySelectorAll('.switch-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.mode === mode));

  document.getElementById('pane-cov').classList.toggle('visible', mode === 'cov');
  document.getElementById('pane-delta').classList.toggle('visible', mode === 'delta');

  document.getElementById('legend-cov').style.display   = mode === 'cov'   ? '' : 'none';
  document.getElementById('legend-delta').style.display = mode === 'delta' ? '' : 'none';

  const m = META[mode];
  document.getElementById('s-regions').textContent = m.regions;
  document.getElementById('s-ages').textContent    = m.ages;
  document.getElementById('s-scale').textContent   = m.scale;

  // Сбрасываем фильтр при переключении
  document.getElementById('regionFilter').value = '';
  applyFilter('');
}}

// Инит статистики
(function() {{
  const m = META[currentMode];
  document.getElementById('s-regions').textContent = m.regions;
  document.getElementById('s-ages').textContent    = m.ages;
  document.getElementById('s-scale').textContent   = m.scale;
}})();

// ── Фильтр регионов ──────────────────────────────────────────────────────────
const CELL_H  = {CELL_H};
const MARGIN_T = {MARGIN_T};
const MARGIN_B = {MARGIN_B};

let origData = {{}};  // {{ divId: {{ y, z }} }}

function waitGd(divId, cb) {{
  const el = document.getElementById(divId);
  if (el && el.data && el.data.length) {{ cb(el); }}
  else {{ setTimeout(() => waitGd(divId, cb), 200); }}
}}

['chart-cov', 'chart-delta'].forEach(id => {{
  waitGd(id, gd => {{
    origData[id] = {{
      y: [...gd.data[0].y],
      z: gd.data[0].z.map(r => [...r]),
    }};
  }});
}});

function applyFilter(q) {{
  ['chart-cov', 'chart-delta'].forEach(id => {{
    const gd = document.getElementById(id);
    if (!gd || !origData[id]) return;
    const {{ y: origY, z: origZ }} = origData[id];
    if (!q) {{
      Plotly.restyle(gd, {{ y: [origY], z: [origZ] }});
      Plotly.relayout(gd, {{ height: MARGIN_T + origY.length * CELL_H + MARGIN_B }});
      return;
    }}
    const idx  = origY.map((r, i) => r.toLowerCase().includes(q) ? i : -1).filter(i => i >= 0);
    const newY = idx.map(i => origY[i]);
    const newZ = idx.map(i => origZ[i]);
    Plotly.restyle(gd, {{ y: [newY], z: [newZ] }});
    Plotly.relayout(gd, {{ height: MARGIN_T + Math.max(newY.length, 1) * CELL_H + MARGIN_B }});
  }});
}}

document.getElementById('regionFilter').addEventListener('input', e =>
  applyFilter(e.target.value.trim().toLowerCase())
);
</script>
</body>
</html>
"""


def build_dashboard(
    wide_df: pd.DataFrame,
    tidy_df: pd.DataFrame,
    output_path: str,
    source_label: str = "silver.education_population_wide_annual",
) -> str:
    """
    Принимает DataFrames вместо CSV-файлов, записывает HTML в output_path,
    возвращает output_path.

    wide_df ожидает колонки: регион, код, <возраст_0>, <возраст_1>, ...
    tidy_df ожидает колонки: регион, возраст (int), отклонение_пп
    """
    w_reg, w_ages, w_z = _extract_wide(wide_df)
    d_reg, d_ages, d_z, dlim = _extract_tidy(tidy_df)

    fig_cov   = build_fig(w_reg, w_ages, w_z, COV_COLORSCALE,   COV_ZMIN, COV_ZMAX, "Охват, %")
    fig_delta = build_fig(d_reg, d_ages, d_z, DELTA_COLORSCALE, -dlim,    +dlim,    "Дельта, пп")

    div_cov   = fig_to_div(fig_cov,   "chart-cov")
    div_delta = fig_to_div(fig_delta, "chart-delta")

    html = _render_html(
        w_reg, w_ages, w_z,
        d_reg, d_ages, d_z, dlim,
        div_cov, div_delta,
        source_label=source_label,
    )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    return output_path


def main():
    WIDE_CSV = "coverage_wide_202604151405.csv"
    TIDY_CSV = "coverage_tidy_202604151412.csv"
    OUTPUT   = "coverage_dashboard.html"

    print("[1/4] Загружаю coverage_wide  …")
    w_reg, w_ages, w_z = load_wide(WIDE_CSV)
    print(f"      {len(w_reg)} регионов, {len(w_ages)} возрастов")

    print("[2/4] Загружаю coverage_tidy  …")
    d_reg, d_ages, d_z, dlim = load_tidy(TIDY_CSV)
    print(f"      {len(d_reg)} регионов, {len(d_ages)} возрастов  |  дельта ±{dlim:.0f} пп")

    print("[3/4] Строю графики …")
    fig_cov   = build_fig(w_reg, w_ages, w_z, COV_COLORSCALE,   COV_ZMIN, COV_ZMAX, "Охват, %")
    fig_delta = build_fig(d_reg, d_ages, d_z, DELTA_COLORSCALE, -dlim,    +dlim,    "Дельта, пп")

    div_cov   = fig_to_div(fig_cov,   "chart-cov")
    div_delta = fig_to_div(fig_delta, "chart-delta")

    print("[4/4] Генерирую HTML …")
    html = _render_html(
        w_reg, w_ages, w_z,
        d_reg, d_ages, d_z, dlim,
        div_cov, div_delta,
        source_label=f"{WIDE_CSV} | {TIDY_CSV}",
    )

    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✅  Готово!  →  {OUTPUT}\n")


if __name__ == "__main__":
    main()
