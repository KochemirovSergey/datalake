"""
Валидационный отчёт для слоя bronze_normalized.

Генерирует Markdown-файл с блоками:
  1. Покрытие по регионам
  2. Покрытие по годам
  3. Матрица region × year
  4. Покрытие по education_level
  5. Row gate — готовность к Silver
  6. Error summary (region + year + education_level)

Вызывается из Dagster asset `normalized_validation`.
Также можно запустить как standalone: python validate_bronze_normalized.py
"""

import os
from collections import Counter, defaultdict
from datetime import datetime, timezone

from pyiceberg.catalog.sql import SqlCatalog

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
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


def _load_df(cat: SqlCatalog, table_name: str):
    import pandas as pd
    try:
        return cat.load_table(table_name).scan().to_pandas()
    except Exception as e:
        print(f"  Предупреждение: не удалось загрузить {table_name}: {e}")
        return pd.DataFrame()


# ── Блок 1: покрытие по регионам ──────────────────────────────────────────────

def _build_region_coverage(region_df, lookup_df) -> str:
    lines = ["## Блок 1 — Покрытие по регионам", ""]

    if region_df.empty:
        lines.append("_Таблица bronze_normalized.region пуста._")
        return "\n".join(lines)

    counts = region_df.groupby("region_code").size().to_dict()

    if "is_alias" in lookup_df.columns:
        canonical = lookup_df[lookup_df["is_alias"] == False]["region_code"].unique().tolist()
    else:
        canonical = lookup_df["region_code"].unique().tolist()

    name_col = "canonical_name" if "canonical_name" in lookup_df.columns else "name_variant"
    code_to_name: dict[str, str] = {}
    for _, row in lookup_df.iterrows():
        code = row["region_code"]
        if code not in code_to_name:
            code_to_name[code] = str(row.get(name_col, row.get("name_variant", code)))

    represented = [c for c in canonical if counts.get(c, 0) > 0]
    pct = 100 * len(represented) / len(canonical) if canonical else 0
    lines.append(
        f"**Итого:** {len(represented)} из {len(canonical)} регионов представлены "
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


# ── Блок 2: покрытие по годам ─────────────────────────────────────────────────

def _build_year_coverage(year_df) -> str:
    lines = ["## Блок 2 — Покрытие по годам", ""]

    if year_df.empty:
        lines.append("_Таблица bronze_normalized.year пуста._")
        return "\n".join(lines)

    total_rows = len(year_df)
    by_year = year_df.groupby("year").size().sort_index()

    period_count = (year_df["year_type"] == "period").sum() if "year_type" in year_df.columns else total_rows
    exact_count  = (year_df["year_type"] == "exact_date").sum() if "year_type" in year_df.columns else 0

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


# ── Блок 3: матрица region × year ─────────────────────────────────────────────

def _build_region_year_matrix(region_df, year_df, lookup_df) -> str:
    lines = ["## Блок 3 — Матрица region × year", ""]

    if region_df.empty or year_df.empty:
        lines.append("_Нет данных для построения матрицы._")
        return "\n".join(lines)

    merged = region_df[["row_id", "region_code"]].merge(
        year_df[["row_id", "year"]], on="row_id", how="inner"
    )

    if merged.empty:
        lines.append("_JOIN по row_id не дал совпадений._")
        return "\n".join(lines)

    matrix: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for _, row in merged.iterrows():
        matrix[row["region_code"]][int(row["year"])] += 1

    years = sorted(merged["year"].unique().tolist())

    name_col = "canonical_name" if "canonical_name" in lookup_df.columns else "name_variant"
    canonical_df = lookup_df[lookup_df["is_alias"] == False] if "is_alias" in lookup_df.columns else lookup_df
    code_to_name: dict[str, str] = {}
    for _, row in canonical_df.iterrows():
        code = row["region_code"]
        if code not in code_to_name:
            code_to_name[code] = str(row.get(name_col, row.get("name_variant", code)))

    all_codes = sorted(matrix.keys(), key=lambda c: code_to_name.get(c, c))
    year_cols = " | ".join(str(y) for y in years)
    lines.append(f"| region_code | region_name | {year_cols} |")
    lines.append(f"|-------------|-------------|{'|'.join('---:' for _ in years)}|")

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

    totals_str = " | ".join(str(year_totals[y]) for y in years)
    lines.append(f"| **ИТОГО** | | {totals_str} |")
    lines.append("")
    lines.append(f"**Всего строк в матрице:** {sum(year_totals.values())}")
    if absent_regions:
        lines.append(f"**Регионы без данных ({len(absent_regions)}):** {', '.join(absent_regions)}")
    represented_pct = 100 * (len(all_codes) - len(absent_regions)) / len(all_codes) if all_codes else 0
    lines.append(f"**Покрытие регионов:** {represented_pct:.1f}%")
    lines.append("")
    return "\n".join(lines)


# ── Блок 4: покрытие по education_level ───────────────────────────────────────

def _build_edu_coverage(edu_df, edu_err_df, edu_lookup_df) -> str:
    lines = ["## Блок 4 — Покрытие по education_level", ""]

    total_ok  = len(edu_df)
    total_err = len(edu_err_df)
    total     = total_ok + total_err
    pct       = f"{100 * total_ok / total:.1f}%" if total else "n/a"

    lines.append(f"**Всего строк:** {total} | ok: {total_ok} | error: {total_err} | покрытие: {pct}")
    lines.append("")

    if not edu_df.empty and "resolved_code" in edu_df.columns:
        by_code = edu_df.groupby("resolved_code").size().sort_index()
        code_to_label: dict[str, str] = {}
        if not edu_lookup_df.empty and "code" in edu_lookup_df.columns:
            for _, r in edu_lookup_df.iterrows():
                code_to_label[str(r["code"])] = str(r.get("label_ru", r["code"]))

        lines.append("| resolved_code | label | строк |")
        lines.append("|--------------|-------|------:|")
        for code, cnt in by_code.items():
            label = code_to_label.get(str(code), str(code))
            lines.append(f"| {code} | {label} | {cnt} |")
        lines.append("")

    if not edu_df.empty and "source_id" in edu_df.columns:
        lines.append("**По источникам:**")
        lines.append("")
        lines.append("| source_id | ok | error |")
        lines.append("|-----------|---:|------:|")
        src_ok  = edu_df.groupby("source_id").size().to_dict()
        src_err = edu_err_df.groupby("source_id").size().to_dict() if not edu_err_df.empty and "source_id" in edu_err_df.columns else {}
        all_srcs = sorted(set(list(src_ok.keys()) + list(src_err.keys())))
        for src in all_srcs:
            lines.append(f"| {src} | {src_ok.get(src, 0)} | {src_err.get(src, 0)} |")
        lines.append("")

    return "\n".join(lines)


# ── Блок 5: row gate ──────────────────────────────────────────────────────────

def _build_gate_stats(gate_df) -> str:
    lines = ["## Блок 5 — Row gate (готовность к Silver)", ""]

    if gate_df.empty:
        lines.append("_Таблица bronze_normalized.row_gate пуста._")
        return "\n".join(lines)

    total     = len(gate_df)
    ready     = gate_df["ready_for_silver"].sum() if "ready_for_silver" in gate_df.columns else 0
    not_ready = total - ready
    pct       = f"{100 * ready / total:.1f}%" if total else "n/a"

    lines.append(f"**Итого строк:** {total}")
    lines.append(f"- ready_for_silver=True:  **{ready}** ({pct})")
    lines.append(f"- ready_for_silver=False: {not_ready}")
    lines.append("")

    # Статистика статусов по измерениям
    for dim in ("region_status", "year_status", "education_level_status"):
        if dim not in gate_df.columns:
            continue
        lines.append(f"**{dim}:**")
        lines.append("")
        lines.append("| статус | строк |")
        lines.append("|--------|------:|")
        for status, cnt in gate_df[dim].value_counts().items():
            lines.append(f"| {status} | {cnt} |")
        lines.append("")

    # По источникам
    if "source_id" in gate_df.columns:
        lines.append("**Готовность по источникам:**")
        lines.append("")
        lines.append("| source_id | total | ready | not_ready | % |")
        lines.append("|-----------|------:|------:|----------:|--:|")
        for src, grp in gate_df.groupby("source_id"):
            t = len(grp)
            r = grp["ready_for_silver"].sum()
            p = f"{100 * r / t:.1f}%" if t else "n/a"
            lines.append(f"| {src} | {t} | {r} | {t - r} | {p} |")
        lines.append("")

    # Топ-10 причин отказа
    if "rejection_reason" in gate_df.columns:
        not_ready_df = gate_df[gate_df["ready_for_silver"] == False]
        if not not_ready_df.empty:
            top_reasons = not_ready_df["rejection_reason"].value_counts().head(10)
            lines.append("**Топ-10 причин отказа:**")
            lines.append("")
            lines.append("| # | reason | строк |")
            lines.append("|--:|--------|------:|")
            for i, (reason, cnt) in enumerate(top_reasons.items(), 1):
                lines.append(f"| {i} | {reason} | {cnt} |")
            lines.append("")

    return "\n".join(lines)


# ── Блок 6: error summary ─────────────────────────────────────────────────────

def _build_error_summary(region_err_df, year_err_df, edu_err_df) -> str:
    lines = ["## Блок 6 — Error summary", ""]

    region_total = len(region_err_df)
    year_total   = len(year_err_df)
    edu_total    = len(edu_err_df)
    grand_total  = region_total + year_total + edu_total

    if grand_total == 0:
        lines.append("_Ошибок нет. Все строки нормализованы._")
        return "\n".join(lines)

    lines.append("| Измерение | тип ошибки | строк |")
    lines.append("|-----------|-----------|------:|")

    if not region_err_df.empty and "error_type" in region_err_df.columns:
        for err_type, cnt in region_err_df["error_type"].value_counts().items():
            lines.append(f"| region | {err_type} | {cnt} |")

    if not year_err_df.empty and "error_type" in year_err_df.columns:
        for err_type, cnt in year_err_df["error_type"].value_counts().items():
            lines.append(f"| year | {err_type} | {cnt} |")

    if not edu_err_df.empty and "error_type" in edu_err_df.columns:
        for err_type, cnt in edu_err_df["error_type"].value_counts().items():
            lines.append(f"| education_level | {err_type} | {cnt} |")

    lines.append(f"| **Итого** | | **{grand_total}** |")
    lines.append("")

    # Топ-20 нераспознанных region_raw
    if not region_err_df.empty and "region_raw" in region_err_df.columns:
        non_empty = region_err_df[region_err_df["region_raw"].str.strip() != ""]
        if not non_empty.empty:
            top20 = non_empty["region_raw"].value_counts().head(20)
            lines.append("### Топ-20 нераспознанных region_raw")
            lines.append("")
            lines.append("| # | region_raw | встреч |")
            lines.append("|--:|-----------|------:|")
            for i, (raw, cnt) in enumerate(top20.items(), 1):
                lines.append(f"| {i} | {raw} | {cnt} |")
            lines.append("")

    # Топ нераспознанных raw_signal для education_level
    if not edu_err_df.empty and "raw_signal" in edu_err_df.columns:
        non_empty_edu = edu_err_df[edu_err_df["raw_signal"].notna() & (edu_err_df["raw_signal"].str.strip() != "")]
        if not non_empty_edu.empty:
            top10 = non_empty_edu["raw_signal"].value_counts().head(10)
            lines.append("### Топ-10 нераспознанных education_level signals")
            lines.append("")
            lines.append("| # | raw_signal | встреч |")
            lines.append("|--:|-----------|------:|")
            for i, (raw, cnt) in enumerate(top10.items(), 1):
                lines.append(f"| {i} | {raw} | {cnt} |")
            lines.append("")

    return "\n".join(lines)


# ── Точка входа ────────────────────────────────────────────────────────────────

def run(cat: SqlCatalog) -> str:
    """
    Генерирует полный Markdown-отчёт.
    Возвращает путь к сохранённому файлу.
    """
    os.makedirs(REPORTS_DIR, exist_ok=True)

    region_df     = _load_df(cat, "bronze_normalized.region")
    region_err_df = _load_df(cat, "bronze_normalized.region_error")
    year_df       = _load_df(cat, "bronze_normalized.year")
    year_err_df   = _load_df(cat, "bronze_normalized.year_error")
    edu_df        = _load_df(cat, "bronze_normalized.education_level")
    edu_err_df    = _load_df(cat, "bronze_normalized.education_level_error")
    gate_df       = _load_df(cat, "bronze_normalized.row_gate")
    lookup_df     = _load_df(cat, "bronze.region_lookup")
    edu_lookup_df = _load_df(cat, "bronze.education_level_lookup")

    now     = datetime.now(tz=timezone.utc)
    ts_str  = now.strftime("%Y-%m-%d %H:%M UTC")
    date_str = now.strftime("%Y-%m-%d")

    summary_rows = [
        f"| bronze_normalized.region (ok) | {len(region_df)} |",
        f"| bronze_normalized.region_error | {len(region_err_df)} |",
        f"| bronze_normalized.year (ok) | {len(year_df)} |",
        f"| bronze_normalized.year_error | {len(year_err_df)} |",
        f"| bronze_normalized.education_level (ok) | {len(edu_df)} |",
        f"| bronze_normalized.education_level_error | {len(edu_err_df)} |",
        f"| bronze_normalized.row_gate | {len(gate_df)} |",
    ]
    if not gate_df.empty and "ready_for_silver" in gate_df.columns:
        ready = gate_df["ready_for_silver"].sum()
        summary_rows.append(f"| &nbsp;&nbsp;└ ready_for_silver=True | {ready} |")

    sections = [
        "# Отчёт валидации bronze_normalized",
        "",
        f"**Дата:** {ts_str}",
        "",
        "| таблица | строк |",
        "|---------|------:|",
        *summary_rows,
        "",
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
        _build_edu_coverage(edu_df, edu_err_df, edu_lookup_df),
        "---",
        "",
        _build_gate_stats(gate_df),
        "---",
        "",
        _build_error_summary(region_err_df, year_err_df, edu_err_df),
    ]

    report_content = "\n".join(sections)
    report_path    = os.path.join(REPORTS_DIR, f"bronze_normalized_{date_str}.md")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    print(f"Отчёт сохранён: {report_path}")
    return report_path


if __name__ == "__main__":
    cat = _get_catalog()
    run(cat)
