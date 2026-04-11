"""
Валидационный отчёт для слоя bronze_normalized.

Генерирует Markdown-файл с четырьмя блоками:
  1. Покрытие по регионам   — сколько регионов из справочника представлено в данных
  2. Покрытие по годам      — какие годы распознаны и сколько строк по каждому
  3. Матрица region × year  — ключевая таблица для анализа полноты
  4. Error summary          — топ нераспознанных значений и распределение по типам

Вызывается из Dagster asset `normalized_validation`.
Также можно запустить как standalone: python validate_bronze_normalized.py
"""

import os
from collections import Counter, defaultdict
from datetime import datetime, timezone

from pyiceberg.catalog.sql import SqlCatalog

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")


def _get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )


def _load_table_df(cat: SqlCatalog, table_name: str):
    import pandas as pd
    try:
        return cat.load_table(table_name).scan().to_pandas()
    except Exception as e:
        print(f"  Предупреждение: не удалось загрузить {table_name}: {e}")
        return pd.DataFrame()


def _build_region_coverage(region_df, lookup_df) -> str:
    """Блок 1: покрытие по регионам."""
    lines = ["## Блок 1 — Покрытие по регионам", ""]

    if region_df.empty:
        lines.append("_Таблица bronze_normalized.region пуста._")
        return "\n".join(lines)

    counts = region_df.groupby("region_code").size().to_dict()

    # Только канонические регионы (is_alias=False, если поле есть; иначе берём уникальные region_code)
    if "is_alias" in lookup_df.columns:
        canonical = lookup_df[lookup_df["is_alias"] == False]["region_code"].unique().tolist()
    else:
        canonical = lookup_df["region_code"].unique().tolist()

    # Берём canonical_name или name_variant для отображения
    name_col = "canonical_name" if "canonical_name" in lookup_df.columns else "name_variant"
    code_to_name: dict[str, str] = {}
    for _, row in lookup_df.iterrows():
        code = row["region_code"]
        if code not in code_to_name:
            code_to_name[code] = str(row.get(name_col, row.get("name_variant", code)))

    represented = [c for c in canonical if counts.get(c, 0) > 0]
    missing = [c for c in canonical if counts.get(c, 0) == 0]

    pct = 100 * len(represented) / len(canonical) if canonical else 0
    lines.append(
        f"**Итого:** {len(represented)} из {len(canonical)} регионов представлены в данных "
        f"(**{pct:.1f}%**)"
    )
    lines.append("")

    lines.append("| region_code | region_name | строк |")
    lines.append("|-------------|-------------|------:|")
    for code in sorted(canonical, key=lambda c: code_to_name.get(c, c)):
        n = counts.get(code, 0)
        name = code_to_name.get(code, code)
        mark = "" if n > 0 else " ⚠ ОТСУТСТВУЕТ"
        lines.append(f"| {code} | {name}{mark} | {n} |")

    lines.append("")
    return "\n".join(lines)


def _build_year_coverage(year_df) -> str:
    """Блок 2: покрытие по годам."""
    lines = ["## Блок 2 — Покрытие по годам", ""]

    if year_df.empty:
        lines.append("_Таблица bronze_normalized.year пуста._")
        return "\n".join(lines)

    total_rows = len(year_df)
    by_year = year_df.groupby("year").size().sort_index()

    if "year_type" in year_df.columns:
        period_count = (year_df["year_type"] == "period").sum()
        exact_count = (year_df["year_type"] == "exact_date").sum()
    else:
        period_count = total_rows
        exact_count = 0

    lines.append(f"**Всего строк с распознанным годом:** {total_rows}")
    lines.append(f"- year_type=period: {period_count}")
    lines.append(f"- year_type=exact_date: {exact_count}")
    lines.append("")
    lines.append("| год | строк |")
    lines.append("|----:|------:|")
    for year, cnt in by_year.items():
        lines.append(f"| {year} | {cnt} |")

    lines.append("")
    return "\n".join(lines)


def _build_region_year_matrix(region_df, year_df, lookup_df) -> str:
    """Блок 3: матрица region × year."""
    lines = ["## Блок 3 — Матрица region × year", ""]

    if region_df.empty or year_df.empty:
        lines.append("_Нет данных для построения матрицы._")
        return "\n".join(lines)

    # JOIN по row_id
    merged = region_df[["row_id", "region_code"]].merge(
        year_df[["row_id", "year"]],
        on="row_id",
        how="inner",
    )

    if merged.empty:
        lines.append("_JOIN по row_id не дал совпадений._")
        return "\n".join(lines)

    # Строим матрицу
    matrix: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for _, row in merged.iterrows():
        matrix[row["region_code"]][int(row["year"])] += 1

    years = sorted(merged["year"].unique().tolist())

    # Имена регионов
    name_col = "canonical_name" if "canonical_name" in lookup_df.columns else "name_variant"
    if "is_alias" in lookup_df.columns:
        canonical_df = lookup_df[lookup_df["is_alias"] == False]
    else:
        canonical_df = lookup_df
    code_to_name: dict[str, str] = {}
    for _, row in canonical_df.iterrows():
        code = row["region_code"]
        if code not in code_to_name:
            code_to_name[code] = str(row.get(name_col, row.get("name_variant", code)))

    all_codes = sorted(matrix.keys(), key=lambda c: code_to_name.get(c, c))

    # Заголовок таблицы
    year_cols = " | ".join(str(y) for y in years)
    lines.append(f"| region_code | region_name | {year_cols} |")
    sep_cols = " | ".join("---:" for _ in years)
    lines.append(f"|-------------|-------------|{sep_cols}|")

    year_totals: dict[int, int] = defaultdict(int)
    absent_regions = []

    for code in all_codes:
        name = code_to_name.get(code, code)
        row_vals = [matrix[code].get(y, 0) for y in years]
        for y, v in zip(years, row_vals):
            year_totals[y] += v
        row_str = " | ".join(str(v) for v in row_vals)
        if all(v == 0 for v in row_vals):
            absent_regions.append(code)
            lines.append(f"| {code} | {name} ⚠ ОТСУТСТВУЕТ | {row_str} |")
        else:
            lines.append(f"| {code} | {name} | {row_str} |")

    # Итоговая строка
    totals_str = " | ".join(str(year_totals[y]) for y in years)
    grand_total = sum(year_totals.values())
    lines.append(f"| **ИТОГО** | | {totals_str} |")
    lines.append("")
    lines.append(f"**Всего строк в матрице:** {grand_total}")

    if absent_regions:
        lines.append(f"**Регионы без данных ({len(absent_regions)}):** {', '.join(absent_regions)}")

    represented_pct = 100 * (len(all_codes) - len(absent_regions)) / len(all_codes) if all_codes else 0
    lines.append(f"**Покрытие регионов:** {represented_pct:.1f}%")
    lines.append("")
    return "\n".join(lines)


def _build_error_summary(region_error_df, year_error_df) -> str:
    """Блок 4: Error summary."""
    lines = ["## Блок 4 — Error summary", ""]

    region_total = len(region_error_df)
    year_total = len(year_error_df)
    grand_total = region_total + year_total

    if grand_total == 0:
        lines.append("_Ошибок нет. Все строки нормализованы._")
        return "\n".join(lines)

    lines.append("| Тип ошибки | строк |")
    lines.append("|-----------|------:|")

    # Region errors by type
    if not region_error_df.empty and "error_type" in region_error_df.columns:
        for err_type, cnt in region_error_df["error_type"].value_counts().items():
            lines.append(f"| region: {err_type} | {cnt} |")

    # Year errors by type
    if not year_error_df.empty and "error_type" in year_error_df.columns:
        for err_type, cnt in year_error_df["error_type"].value_counts().items():
            lines.append(f"| year: {err_type} | {cnt} |")

    lines.append(f"| **Итого в quarantine** | **{grand_total}** |")
    lines.append("")

    # Топ-20 нераспознанных region_raw
    if not region_error_df.empty and "region_raw" in region_error_df.columns:
        non_empty = region_error_df[region_error_df["region_raw"].str.strip() != ""]
        if not non_empty.empty:
            top20 = non_empty["region_raw"].value_counts().head(20)
            lines.append("### Топ-20 нераспознанных region_raw")
            lines.append("")
            lines.append("| # | region_raw | встреч |")
            lines.append("|--:|-----------|------:|")
            for i, (raw, cnt) in enumerate(top20.items(), 1):
                lines.append(f"| {i} | {raw} | {cnt} |")
            lines.append("")

    return "\n".join(lines)


def run(cat: SqlCatalog) -> str:
    """
    Генерирует полный Markdown-отчёт.
    Возвращает путь к сохранённому файлу.
    """
    os.makedirs(REPORTS_DIR, exist_ok=True)

    region_df     = _load_table_df(cat, "bronze_normalized.region")
    region_err_df = _load_table_df(cat, "bronze_normalized.region_error")
    year_df       = _load_table_df(cat, "bronze_normalized.year")
    year_err_df   = _load_table_df(cat, "bronze_normalized.year_error")
    lookup_df     = _load_table_df(cat, "bronze.region_lookup")

    now = datetime.now(tz=timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y-%m-%d %H:%M UTC")

    region_ok_count = len(region_df)
    region_err_count = len(region_err_df)
    year_ok_count = len(year_df)
    year_err_count = len(year_err_df)

    sections = [
        f"# Отчёт валидации bronze_normalized",
        f"",
        f"**Дата:** {ts_str}",
        f"",
        f"| | строк |",
        f"|--|------:|",
        f"| bronze_normalized.region (ok) | {region_ok_count} |",
        f"| bronze_normalized.region_error | {region_err_count} |",
        f"| bronze_normalized.year (ok) | {year_ok_count} |",
        f"| bronze_normalized.year_error | {year_err_count} |",
        f"",
        "---",
        "",
        _build_region_coverage(region_df, lookup_df),
        "---",
        "",
        _build_year_coverage(year_df),
        "---",
        "",
        _build_region_year_matrix(region_df, year_df, lookup_df),
        "---",
        "",
        _build_error_summary(region_err_df, year_err_df),
    ]

    report_content = "\n".join(sections)
    report_path = os.path.join(REPORTS_DIR, f"bronze_normalized_{date_str}.md")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    print(f"Отчёт сохранён: {report_path}")
    return report_path


if __name__ == "__main__":
    cat = _get_catalog()
    run(cat)
