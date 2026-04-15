"""
Валидация silver.dormitory_infrastructure.
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
    """Генерирует Markdown-отчёт по silver.dormitory_infrastructure.

    Returns:
        Путь к сохранённому Markdown-файлу.
    """
    tbl = cat.load_table("silver.dormitory_infrastructure")
    df  = tbl.scan().to_pandas()

    report_date = date.today().isoformat()
    path = os.path.join(REPORTS_DIR, f"dormitory_{report_date}.md")

    if df.empty:
        content = f"# Отчёт: silver.dormitory_infrastructure\n\nДата: {report_date}\n\n**Таблица пуста.**\n"
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    for col in ["year", "alert_flag"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    hist_df = df[~df["is_forecast"]]
    fc_df   = df[df["is_forecast"]]

    n_rows       = len(df)
    n_regions    = df["region_code"].nunique()
    hist_years   = sorted(hist_df["year"].dropna().astype(int).unique())
    fc_years     = sorted(fc_df["year"].dropna().astype(int).unique())

    # alert-регионы в последнем историческом году
    if hist_years:
        last_hist = hist_years[-1]
        last_hist_df = hist_df[hist_df["year"] == last_hist]
        n_alert = int(last_hist_df["alert_flag"].fillna(0).sum())
    else:
        last_hist = None
        n_alert   = 0

    lines = [
        f"# Отчёт: silver.dormitory_infrastructure",
        f"",
        f"Дата: {report_date}",
        f"",
        f"## Общая статистика",
        f"",
        f"| Показатель | Значение |",
        f"|---|---|",
        f"| Всего строк | {n_rows} |",
        f"| Регионов | {n_regions} |",
        f"| Исторических лет | {len(hist_years)} ({hist_years[0] if hist_years else '—'}–{hist_years[-1] if hist_years else '—'}) |",
        f"| Прогнозных лет | {len(fc_years)} ({fc_years[0] if fc_years else '—'}–{fc_years[-1] if fc_years else '—'}) |",
        f"| Alert-регионов в {last_hist} году | {n_alert} |",
        f"",
    ]

    if n_alert > 0 and last_hist is not None:
        alert_regions = (
            last_hist_df[last_hist_df["alert_flag"] == 1]["region_code"]
            .sort_values()
            .tolist()
        )
        lines.append(f"## Alert-регионы ({last_hist})")
        lines.append("")
        for reg in alert_regions:
            lines.append(f"- {reg}")
        lines.append("")

    # Топ-10 по sigma_sum
    df["sigma_sum"] = pd.to_numeric(df.get("sigma_sum", 0), errors="coerce")
    worst = (
        hist_df[hist_df["sigma_sum"].notna()]
        .groupby("region_code")["sigma_sum"]
        .mean()
        .sort_values(ascending=False)
        .head(10)
    )

    lines += [
        f"## Регионы с наибольшей суммой сигм (топ-10, историч.)",
        f"",
        f"| Регион | Средняя sigma_sum |",
        f"|---|---|",
    ]
    for reg, ss in worst.items():
        lines.append(f"| {reg} | {ss:.3f} |")

    content = "\n".join(lines) + "\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

    return path
