"""
Валидационный отчёт для всего Silver-слоя.

Генерирует единый Markdown-файл с двумя разделами:
  § 1  silver.doshkolka — покрытие по регионам, годам, матрица region × year
  § 2  silver.naselenie — покрытие по регионам, годам, матрица region × year

Можно запустить как standalone: python validation/validate_silver.py
"""

import os
from collections import defaultdict
from datetime import datetime, timezone

from pyiceberg.catalog.sql import SqlCatalog

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

EXPECTED_YEARS = list(range(2018, 2025))  # 2018–2024


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


def _code_to_name(lookup_df) -> dict[str, str]:
    if "is_alias" in lookup_df.columns:
        lookup_df = lookup_df[lookup_df["is_alias"] == False]
    name_col = "canonical_name" if "canonical_name" in lookup_df.columns else "name_variant"
    result: dict[str, str] = {}
    for _, row in lookup_df.iterrows():
        code = row["region_code"]
        if code not in result:
            result[code] = str(row.get(name_col, code))
    return result


# ── Общие блоки ────────────────────────────────────────────────────────────────

def _region_coverage(df, code_to_name_map: dict, label: str) -> str:
    lines = [f"### Покрытие по регионам — {label}", ""]
    if df.empty:
        lines.append(f"_{label} пуста._\n")
        return "\n".join(lines)

    canonical = sorted(code_to_name_map.keys())
    present = set(df["region_code"].unique())
    represented = [c for c in canonical if c in present]
    missing = [c for c in canonical if c not in present]
    extra = sorted(present - set(canonical))

    pct = 100 * len(represented) / len(canonical) if canonical else 0
    lines.append(
        f"**{len(represented)} из {len(canonical)} регионов** присутствуют "
        f"(**{pct:.1f}%**)"
    )
    if missing:
        lines.append(f"**Отсутствуют ({len(missing)}):** {', '.join(missing)}")
    if extra:
        lines.append(f"**Вне справочника ({len(extra)}):** {', '.join(extra)}")
    lines.append("")
    return "\n".join(lines)


def _year_coverage(df, label: str) -> str:
    lines = [f"### Покрытие по годам — {label}", ""]
    if df.empty:
        lines.append(f"_{label} пуста._\n")
        return "\n".join(lines)

    by_year = df.groupby("year").size().sort_index()
    present_years = set(by_year.index.tolist())
    missing_years = [y for y in EXPECTED_YEARS if y not in present_years]

    lines.append(f"**Ожидаемые годы:** {EXPECTED_YEARS[0]}–{EXPECTED_YEARS[-1]}")
    if missing_years:
        lines.append(f"**Отсутствующие годы:** {missing_years}  ⚠")
    else:
        lines.append("**Все ожидаемые годы присутствуют.**")
    lines.append("")

    lines.append("| год | строк | регионов |")
    lines.append("|----:|------:|---------:|")
    for year in EXPECTED_YEARS:
        sub = df[df["year"] == year]
        mark = " ⚠" if len(sub) == 0 else ""
        lines.append(f"| {year}{mark} | {len(sub)} | {sub['region_code'].nunique()} |")
    lines.append("")
    return "\n".join(lines)


def _matrix(df, code_to_name_map: dict, label: str, value_fn) -> str:
    """
    Матрица region × year.
    value_fn(sub_df) → строка для ячейки по подмножеству строк одного региона × одного года.
    """
    lines = [f"### Матрица region × year — {label}", ""]
    if df.empty:
        lines.append(f"_{label} пуста._\n")
        return "\n".join(lines)

    canonical = sorted(code_to_name_map.keys(), key=lambda c: code_to_name_map.get(c, c))
    years = EXPECTED_YEARS

    year_header = " | ".join(str(y) for y in years)
    sep = " | ".join("---:" for _ in years)
    lines.append(f"| region_code | region_name | {year_header} |")
    lines.append(f"|-------------|-------------|{sep}|")

    gaps = []
    for code in canonical:
        name = code_to_name_map.get(code, code)
        cells = []
        has_gap = False
        for year in years:
            sub = df[(df["region_code"] == code) & (df["year"] == year)]
            if sub.empty:
                cells.append("⚠")
                has_gap = True
            else:
                cells.append(value_fn(sub))
        if has_gap:
            missing_yrs = [y for y, c in zip(years, cells) if c == "⚠"]
            gaps.append((code, missing_yrs))
        lines.append(f"| {code} | {name} | {' | '.join(cells)} |")

    lines.append("")
    if gaps:
        lines.append(f"**Пропуски ({len(gaps)} регионов):**")
        for code, yrs in gaps:
            lines.append(f"- {code} ({code_to_name_map.get(code, code)}): нет данных за {yrs}")
    else:
        lines.append("**Матрица полная — все регионы × все годы заполнены.**")
    lines.append("")
    return "\n".join(lines)


# ── Секция doshkolka ───────────────────────────────────────────────────────────

def _section_doshkolka(cat: SqlCatalog, code_to_name_map: dict) -> str:
    df = _load_df(cat, "silver.doshkolka")

    def _value(sub):
        row = sub[
            (sub["territory_type"] == "total") & (sub["age_group"] == "total")
        ]
        if row.empty:
            return "—"
        val = row.iloc[0]["value"]
        return str(int(val)) if val is not None else "—"

    lines = [
        "## § 1 — silver.doshkolka",
        "",
        f"**Строк:** {len(df)}  |  "
        f"**Регионов:** {df['region_code'].nunique() if not df.empty else 0}  |  "
        f"**Годов:** {df['year'].nunique() if not df.empty else 0}",
        "",
        _region_coverage(df, code_to_name_map, "silver.doshkolka"),
        _year_coverage(df, "silver.doshkolka"),
        "_Значение в ячейке: число детей (territory\\_type=total, age\\_group=total)._",
        "",
        _matrix(df, code_to_name_map, "silver.doshkolka", _value),
    ]
    return "\n".join(lines)


# ── Секция naselenie ───────────────────────────────────────────────────────────

def _section_naselenie(cat: SqlCatalog, code_to_name_map: dict) -> str:
    df = _load_df(cat, "silver.naselenie")

    def _value(sub):
        # Количество уникальных возрастов (ожидается 81: 0–79 + 80+)
        return str(sub["age"].nunique())

    lines = [
        "## § 2 — silver.naselenie",
        "",
        f"**Строк:** {len(df)}  |  "
        f"**Регионов:** {df['region_code'].nunique() if not df.empty else 0}  |  "
        f"**Годов:** {df['year'].nunique() if not df.empty else 0}",
        "",
        _region_coverage(df, code_to_name_map, "silver.naselenie"),
        _year_coverage(df, "silver.naselenie"),
        "_Значение в ячейке: число уникальных возрастов (ожидается 81: 0–79 + 80+)._",
        "",
        _matrix(df, code_to_name_map, "silver.naselenie", _value),
    ]
    return "\n".join(lines)


# ── Точка входа ────────────────────────────────────────────────────────────────

def run(cat: SqlCatalog) -> str:
    os.makedirs(REPORTS_DIR, exist_ok=True)

    lookup_df = _load_df(cat, "bronze.region_lookup")
    code_map = _code_to_name(lookup_df)

    now = datetime.now(tz=timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y-%m-%d %H:%M UTC")

    sections = [
        "# Отчёт валидации Silver-слоя",
        "",
        f"**Дата:** {ts_str}",
        "",
        "---",
        "",
        _section_doshkolka(cat, code_map),
        "---",
        "",
        _section_naselenie(cat, code_map),
    ]

    report_content = "\n".join(sections)
    report_path = os.path.join(REPORTS_DIR, f"silver_{date_str}.md")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    print(f"Отчёт сохранён: {report_path}")
    return report_path


if __name__ == "__main__":
    cat = _get_catalog()
    run(cat)
