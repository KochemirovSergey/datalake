"""
Валидация silver.education_population_wide_annual

Проверяет:
  - корректность возрастов (только целые числа 0-80)
  - отсутствие диапазонов в колонке age
  - уникальность ключа region_code + year + age
  - базовое покрытие по регионам и годам

Генерирует Markdown-отчёт в reports/.
"""

import logging
import os
from datetime import date
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

METRICS = [
    "population_total",
    "level_1_1", "level_1_2", "level_1_3", "level_1_4",
    "level_2_5_1", "level_2_5_2",
    "level_2_6", "level_2_7", "level_2_8",
    "level_4_8b_1", "level_4_8b_2",
]

METRIC_LABELS = {
    "population_total": "Население (total)",
    "level_1_1": "1.1 Дошкольное",
    "level_1_2": "1.2 Начальное общее",
    "level_1_3": "1.3 Основное общее",
    "level_1_4": "1.4 Среднее общее",
    "level_2_5_1": "2.5.1 СПО программа 1",
    "level_2_5_2": "2.5.2 СПО программа 2",
    "level_2_6": "2.6 Бакалавриат",
    "level_2_7": "2.7 Специалитет",
    "level_2_8": "2.8 Магистратура",
    "level_4_8b_1": "4.8b.1 ДПО программа 1",
    "level_4_8b_2": "4.8b.2 ДПО программа 2",
}


def run(cat: SqlCatalog) -> str:
    import pandas as pd

    os.makedirs(REPORTS_DIR, exist_ok=True)
    today = date.today().isoformat()
    report_path = os.path.join(REPORTS_DIR, f"silver_education_population_wide_annual_{today}.md")

    try:
        df = cat.load_table("silver.education_population_wide_annual").scan().to_pandas()
    except Exception as e:
        log.warning("Ошибка загрузки таблицы: %s", e)
        df = pd.DataFrame()

    lines = [
        f"# Отчёт: silver.education_population_wide_annual",
        f"Дата: {today}",
        "",
    ]

    if df.empty:
        lines.append("**Таблица пуста — данные не загружены.**")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return report_path

    # ── 1. Базовая статистика ─────────────────────────────────────────────────
    n_rows = len(df)
    n_regions = df["region_code"].nunique()
    years = sorted(df["year"].unique().tolist())
    ages = sorted(df["age"].astype(int).unique().tolist())
    lines += [
        "## 1. Базовая статистика",
        "",
        f"| Показатель | Значение |",
        f"|---|---|",
        f"| Строк итого | {n_rows:,} |",
        f"| Регионов | {n_regions} |",
        f"| Диапазон лет | {min(years)}–{max(years)} |",
        f"| Диапазон возрастов | {min(ages)}–{max(ages)} лет |",
        f"| Уникальных возрастов | {len(ages)} |",
        "",
    ]

    # ── 2. Проверки целостности ───────────────────────────────────────────────
    lines.append("## 2. Проверки целостности")
    lines.append("")

    # нет диапазонов в age
    bad_ages = df[df['age'].str.contains('-|\\+|<|моложе', regex=True, na=False)]['age'].unique().tolist()
    if bad_ages:
        lines.append(f"⚠️ **Найдены нераскрытые диапазоны в `age`:** {bad_ages}")
    else:
        lines.append("✅ Все значения `age` — конкретные числа (без диапазонов).")

    # все возраста — число в диапазоне 0-80
    try:
        df['age_int'] = df['age'].astype(int)
        invalid_ages = df[(df['age_int'] < 0) | (df['age_int'] > 80)]['age'].unique().tolist()
        if invalid_ages:
            lines.append(f"⚠️ **Возраста вне диапазона 0-80:** {invalid_ages}")
        else:
            lines.append("✅ Все возраста в диапазоне 0–80.")
    except Exception:
        lines.append("⚠️ Ошибка при проверке числового формата возраста.")

    # уникальность ключа
    dup = df.duplicated(subset=['region_code', 'year', 'age'], keep=False)
    n_dup = dup.sum()
    if n_dup > 0:
        lines.append(f"⚠️ **Найдено {n_dup} дублирующих строк** по ключу `region_code + year + age`.")
    else:
        lines.append("✅ Ключ `region_code + year + age` уникален.")
    lines.append("")

    # ── 3. Покрытие по годам ─────────────────────────────────────────────────
    lines.append("## 3. Покрытие по годам")
    lines.append("")
    year_stats = df.groupby('year').agg(
        regions=('region_code', 'nunique'),
        rows=('region_code', 'count')
    ).reset_index()
    lines.append("| Год | Регионов | Строк |")
    lines.append("|---|---|---|")
    for _, row in year_stats.iterrows():
        lines.append(f"| {int(row['year'])} | {int(row['regions'])} | {int(row['rows']):,} |")
    lines.append("")

    # ── 4. Покрытие по уровням образования ──────────────────────────────────
    lines.append("## 4. Покрытие по уровням образования (% строк с ненулевым значением)")
    lines.append("")
    lines.append("| Уровень | Строк с данными | % от итого |")
    lines.append("|---|---|---|")
    for m in METRICS:
        if m not in df.columns:
            lines.append(f"| {METRIC_LABELS.get(m, m)} | — | — |")
            continue
        n_filled = df[m].notna().sum()
        pct = n_filled / n_rows * 100 if n_rows > 0 else 0
        lines.append(f"| {METRIC_LABELS.get(m, m)} | {n_filled:,} | {pct:.1f}% |")
    lines.append("")

    # ── 5. Распределение возрастов ─────────────────────────────────────────
    lines.append("## 5. Распределение возрастов по уровням образования")
    lines.append("")
    lines.append("Диапазоны встречающихся возрастов для каждого уровня:")
    lines.append("")
    lines.append("| Уровень | Мин. возраст | Макс. возраст | Уникальных возрастов |")
    lines.append("|---|---|---|---|")
    for m in METRICS:
        if m not in df.columns:
            continue
        sub = df[df[m].notna()]
        if sub.empty:
            continue
        try:
            ages_m = sub['age_int'] if 'age_int' in sub.columns else sub['age'].astype(int)
            lines.append(
                f"| {METRIC_LABELS.get(m, m)} | {ages_m.min()} | {ages_m.max()} | {ages_m.nunique()} |"
            )
        except Exception:
            pass
    lines.append("")

    # ── 6. NULL по колонкам ─────────────────────────────────────────────────
    lines.append("## 6. Количество NULL по колонкам метрик")
    lines.append("")
    lines.append("| Колонка | NULL | Заполнено |")
    lines.append("|---|---|---|")
    for m in METRICS:
        if m not in df.columns:
            continue
        n_null = df[m].isna().sum()
        n_filled = df[m].notna().sum()
        lines.append(f"| {m} | {n_null:,} | {n_filled:,} |")
    lines.append("")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    log.info("Отчёт сохранён: %s", report_path)
    return report_path
