"""
Валидация silver.staff_shortage_triggers.
Генерирует Markdown-отчёт с базовой статистикой.
"""

import os
from datetime import date

import pandas as pd
from pyiceberg.catalog.sql import SqlCatalog

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)


def run(cat: SqlCatalog) -> str:
    """Генерирует Markdown-отчёт по silver.staff_shortage_triggers.

    Returns:
        Путь к сохранённому Markdown-файлу.
    """
    tbl = cat.load_table("silver.staff_shortage_triggers")
    df  = tbl.scan().to_pandas()

    report_date = date.today().isoformat()
    path = os.path.join(REPORTS_DIR, f"staff_shortage_{report_date}.md")

    if df.empty:
        content = f"# Отчёт: silver.staff_shortage_triggers\n\nДата: {report_date}\n\n**Таблица пуста.**\n"
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    for col in ["trig1_val", "trig2_val", "bonus_score", "score"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    n_rows    = len(df)
    n_regions = df["region_code"].nunique()
    n_years   = df["year"].nunique()
    years     = sorted(df["year"].unique())
    levels    = sorted(df["level"].unique())

    score_stats = df["score"].describe()
    n_null_score = df["score"].isna().sum()

    lines = [
        f"# Отчёт: silver.staff_shortage_triggers",
        f"",
        f"Дата: {report_date}",
        f"",
        f"## Общая статистика",
        f"",
        f"| Показатель | Значение |",
        f"|---|---|",
        f"| Всего строк | {n_rows} |",
        f"| Регионов | {n_regions} |",
        f"| Годов | {n_years} ({years[0]}–{years[-1]}) |",
        f"| Уровней | {len(levels)} |",
        f"| Строк без score | {n_null_score} |",
        f"",
        f"## Уровни",
        f"",
    ]
    for lev in levels:
        cnt = len(df[df["level"] == lev])
        lines.append(f"- **{lev}**: {cnt} строк")

    lines += [
        f"",
        f"## Распределение score (0–5)",
        f"",
        f"| Статистика | Значение |",
        f"|---|---|",
        f"| Среднее | {score_stats.get('mean', 0):.3f} |",
        f"| Медиана | {score_stats.get('50%', 0):.3f} |",
        f"| Мин | {score_stats.get('min', 0):.3f} |",
        f"| Макс | {score_stats.get('max', 0):.3f} |",
        f"| Стд. откл. | {score_stats.get('std', 0):.3f} |",
        f"",
        f"## Регионы с наименьшим score (топ-10)",
        f"",
    ]

    worst = (
        df[df["score"].notna()]
        .groupby("region_code")["score"]
        .mean()
        .sort_values()
        .head(10)
    )
    lines.append("| Регион | Средний score |")
    lines.append("|---|---|")
    for reg, sc in worst.items():
        lines.append(f"| {reg} | {sc:.3f} |")

    content = "\n".join(lines) + "\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

    return path
