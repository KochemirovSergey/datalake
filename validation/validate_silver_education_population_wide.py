"""
Генерирует Markdown-отчёт по coverage и статистике таблицы silver.education_population_wide.
"""

import os
import datetime
from pyiceberg.catalog.sql import SqlCatalog
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

METRICS = [
    "population_total",
    "level_1_1",
    "level_1_2",
    "level_1_3",
    "level_1_4",
    "level_2_5_1",
    "level_2_5_2",
    "level_2_6",
    "level_2_7",
    "level_2_8",
    "level_4_8b_1",
    "level_4_8b_2"
]

def generate_report(cat: SqlCatalog) -> str:
    try:
        tbl = cat.load_table("silver.education_population_wide")
        df = tbl.scan().to_pandas()
    except Exception as e:
        return f"Ошибка загрузки таблицы: {e}"

    if df.empty:
        return "Таблица silver.education_population_wide пуста."

    total_rows = len(df)
    unique_regions = df["region_code"].nunique()
    min_year = df["year"].min()
    max_year = df["year"].max()

    today = datetime.date.today().isoformat()
    md = [
        f"# Валидация silver.education_population_wide ({today})",
        "",
        "## Общая статистика",
        f"- **Количество строк:** {total_rows}",
        f"- **Количество регионов:** {unique_regions}",
        f"- **Диапазон лет:** {min_year} – {max_year}",
        ""
    ]

    # Количество NULL
    md.append("## Количество пустых значений (NULL)")
    md.append("| Метрика | NULL, кол-во | NULL, % |")
    md.append("|---|---|---|")
    for m in METRICS:
        if m in df.columns:
            null_count = df[m].isna().sum()
            null_pct = (null_count / total_rows) * 100
            md.append(f"| `{m}` | {null_count} | {null_pct:.1f}% |")
        else:
            md.append(f"| `{m}` | N/A | N/A |")
    md.append("")

    # Возрасты и годы по каждому уровню
    md.append("## Покрытие: Доступные Возрасты и Года")
    md.append("| Метрика | Доступные года | Доступные возрасты |")
    md.append("|---|---|---|")
    for m in METRICS:
        if m in df.columns:
            not_null_df = df[df[m].notna()]
            if not_null_df.empty:
                md.append(f"| `{m}` | Нет данных | Нет данных |")
            else:
                years = sorted(not_null_df["year"].unique().tolist())
                ages = sorted(not_null_df["age"].unique().tolist(), key=str)
                # Ограничим вывод возрастов чтоб таблица не разъезжалась
                if len(ages) > 15:
                    ages_str = f"{ages[0]} .. {ages[-1]} ({len(ages)} вариантов)"
                else:
                    ages_str = ", ".join(map(str, ages))
                years_str = ", ".join(map(str, years))
                md.append(f"| `{m}` | {years_str} | {ages_str} |")
    md.append("")

    # Coverage-таблица (%) регионов
    md.append("## Покрытие регионов (%) по годам")
    md.append("Показывает процент субъектов РФ (относительно максимального числа регионов из всех данных = 89 или больше), для которых есть данные хотя бы в одной строке за этот год.")
    
    # Считаем уникальные регионы за все время
    total_region_pool = df["region_code"].nunique()
    
    all_years = sorted(df["year"].dropna().unique())
    header = "| Метрика | " + " | ".join(map(str, all_years)) + " |"
    sep = "|---|" + "|".join(["---"] * len(all_years)) + "|"
    md.append(header)
    md.append(sep)

    for m in METRICS:
        if m not in df.columns:
            continue
        row = [f"`{m}`"]
        for y in all_years:
            sub = df[(df["year"] == y) & (df[m].notna())]
            reg_count = sub["region_code"].nunique()
            if total_region_pool > 0:
                pct = (reg_count / total_region_pool) * 100
            else:
                pct = 0
            row.append(f"{pct:.1f}%")
        md.append("| " + " | ".join(row) + " |")

    return "\n".join(md)

def run(cat: SqlCatalog) -> str:
    from pathlib import Path
    reports_dir = Path(BASE_DIR) / "reports"
    reports_dir.mkdir(exist_ok=True)

    today = datetime.date.today().isoformat()
    report_path = reports_dir / f"silver_education_population_wide_{today}.md"

    md_content = generate_report(cat)
    report_path.write_text(md_content, encoding="utf-8")
    return str(report_path)

if __name__ == "__main__":
    cat = SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{BASE_DIR}/catalog/catalog.db",
            "warehouse": f"file://{BASE_DIR}/catalog/warehouse",
        },
    )
    path = run(cat)
    print(f"Report saved to: {path}")
