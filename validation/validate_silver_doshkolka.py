"""
Валидационный отчёт для silver.doshkolka.

Генерирует Markdown-файл с тремя блоками:
  1. Покрытие по регионам   — сколько регионов из справочника представлено
  2. Покрытие по годам      — какие годы есть и сколько строк по каждому
  3. Матрица region × year  — ячейка = значение total/total (общее число детей);
                              пустая ячейка — данных нет

Можно запустить как standalone: python validate_silver_doshkolka.py
"""

import os
from collections import defaultdict
from datetime import datetime, timezone

from pyiceberg.catalog.sql import SqlCatalog

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

EXPECTED_YEARS = list(range(2018, 2025))   # 2018–2024


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


# ── Блок 1: покрытие по регионам ──────────────────────────────────────────────

def _build_region_coverage(silver_df, lookup_df) -> str:
    lines = ["## Блок 1 — Покрытие по регионам", ""]

    if silver_df.empty:
        lines.append("_silver.doshkolka пуста._")
        return "\n".join(lines)

    # Справочник регионов
    if "is_alias" in lookup_df.columns:
        canonical_df = lookup_df[lookup_df["is_alias"] == False]
    else:
        canonical_df = lookup_df
    name_col = "canonical_name" if "canonical_name" in lookup_df.columns else "name_variant"

    code_to_name: dict[str, str] = {}
    for _, row in canonical_df.iterrows():
        code = row["region_code"]
        if code not in code_to_name:
            code_to_name[code] = str(row.get(name_col, row.get("name_variant", code)))

    canonical = sorted(code_to_name.keys())
    present = set(silver_df["region_code"].unique())
    represented = [c for c in canonical if c in present]
    missing = [c for c in canonical if c not in present]
    extra = sorted(present - set(canonical))

    pct = 100 * len(represented) / len(canonical) if canonical else 0
    lines.append(
        f"**Итого:** {len(represented)} из {len(canonical)} регионов присутствуют "
        f"(**{pct:.1f}%**)"
    )
    if missing:
        lines.append(f"**Отсутствуют ({len(missing)}):** {', '.join(missing)}")
    if extra:
        lines.append(f"**Вне справочника ({len(extra)}):** {', '.join(extra)}")
    lines.append("")

    # Структура данных: сколько лет × территорий × возрастов на регион
    n_years = silver_df["year"].nunique()
    n_territories = silver_df["territory_type"].nunique()
    n_ages = silver_df["age_group"].nunique()
    expected_per_region = n_years * n_territories * n_ages
    lines.append(
        f"_Структура: {n_years} лет × {n_territories} тип территории × {n_ages} возрастных групп = "
        f"{expected_per_region} строк на регион_"
    )
    lines.append("")

    # Счётчики: все строки + разбивка по territory_type для каждого regional
    all_counts = silver_df.groupby("region_code").size().to_dict()
    terr_counts: dict[str, dict[str, int]] = {}
    for code in present:
        sub = silver_df[silver_df["region_code"] == code]
        terr_counts[code] = sub.groupby("territory_type").size().to_dict()

    territories = sorted(silver_df["territory_type"].unique())
    terr_header = " | ".join(territories)
    terr_sep = " | ".join("---:" for _ in territories)

    lines.append(f"| region_code | region_name | строк всего | {terr_header} |")
    lines.append(f"|-------------|-------------|------------:|{terr_sep}|")
    for code in sorted(canonical, key=lambda c: code_to_name.get(c, c)):
        name = code_to_name.get(code, code)
        total = all_counts.get(code, 0)
        mark = " ⚠ ОТСУТСТВУЕТ" if total == 0 else ""
        terr_cells = " | ".join(str(terr_counts.get(code, {}).get(t, 0)) for t in territories)
        lines.append(f"| {code} | {name}{mark} | {total} | {terr_cells} |")

    lines.append("")
    return "\n".join(lines)


# ── Блок 2: покрытие по годам ─────────────────────────────────────────────────

def _build_year_coverage(silver_df) -> str:
    lines = ["## Блок 2 — Покрытие по годам", ""]

    if silver_df.empty:
        lines.append("_silver.doshkolka пуста._")
        return "\n".join(lines)

    by_year = silver_df.groupby("year").size().sort_index()
    present_years = set(by_year.index.tolist())
    missing_years = [y for y in EXPECTED_YEARS if y not in present_years]

    lines.append(f"**Ожидаемые годы:** {EXPECTED_YEARS[0]}–{EXPECTED_YEARS[-1]}")
    if missing_years:
        lines.append(f"**Отсутствующие годы:** {missing_years}  ⚠")
    else:
        lines.append("**Все ожидаемые годы присутствуют.**")
    lines.append("")

    # Уникальные комбинации территория × возраст на год
    age_groups = sorted(silver_df["age_group"].unique())
    territories = sorted(silver_df["territory_type"].unique())

    lines.append(f"**Территории:** {', '.join(territories)}")
    lines.append(f"**Возрастные группы:** {', '.join(age_groups)}")
    lines.append("")

    lines.append("| год | строк всего | регионов | территорий | возрастных групп |")
    lines.append("|----:|------------:|---------:|-----------:|-----------------:|")
    for year in EXPECTED_YEARS:
        sub = silver_df[silver_df["year"] == year]
        all_cnt = len(sub)
        regions_cnt = sub["region_code"].nunique()
        terr_cnt = sub["territory_type"].nunique()
        age_cnt = sub["age_group"].nunique()
        mark = " ⚠" if all_cnt == 0 else ""
        lines.append(f"| {year}{mark} | {all_cnt} | {regions_cnt} | {terr_cnt} | {age_cnt} |")

    lines.append("")

    # Проверяем: у всех ли регионов есть все age_group за каждый год
    lines.append("### Полнота age_group по годам")
    lines.append("")
    lines.append("_Ожидается одинаковый набор возрастных групп по всем регионам в каждом году._")
    lines.append("")
    all_age_groups = set(silver_df["age_group"].unique())
    issues = []
    for year in EXPECTED_YEARS:
        sub = silver_df[silver_df["year"] == year]
        for code in sub["region_code"].unique():
            region_ages = set(sub[sub["region_code"] == code]["age_group"].unique())
            diff = all_age_groups - region_ages
            if diff:
                issues.append(f"- {year} / {code}: отсутствуют age_group {sorted(diff)}")
    if issues:
        lines.append(f"**Найдено {len(issues)} неполных комбинаций регион × год:**")
        for issue in issues[:20]:
            lines.append(issue)
        if len(issues) > 20:
            lines.append(f"_(ещё {len(issues) - 20} не показаны)_")
    else:
        lines.append("**Все регионы × годы содержат полный набор возрастных групп.**")
    lines.append("")

    return "\n".join(lines)


# ── Блок 3: матрица region × year ─────────────────────────────────────────────

def _build_matrix(silver_df, lookup_df) -> str:
    lines = ["## Блок 3 — Матрица region × year", ""]
    lines.append("_Значение в ячейке: число детей (territory\\_type=total, age\\_group=total)._")
    lines.append("_Пустая ячейка — данных нет._")
    lines.append("")

    if silver_df.empty:
        lines.append("_silver.doshkolka пуста._")
        return "\n".join(lines)

    # Берём только total/total — один показатель на регион × год
    total_df = silver_df[
        (silver_df["territory_type"] == "total") & (silver_df["age_group"] == "total")
    ].copy()

    # Имена регионов
    if "is_alias" in lookup_df.columns:
        canonical_df = lookup_df[lookup_df["is_alias"] == False]
    else:
        canonical_df = lookup_df
    name_col = "canonical_name" if "canonical_name" in lookup_df.columns else "name_variant"
    code_to_name: dict[str, str] = {}
    for _, row in canonical_df.iterrows():
        code = row["region_code"]
        if code not in code_to_name:
            code_to_name[code] = str(row.get(name_col, row.get("name_variant", code)))

    canonical = sorted(code_to_name.keys(), key=lambda c: code_to_name.get(c, c))
    years = EXPECTED_YEARS

    # Матрица: region_code → year → value
    matrix: dict[str, dict[int, str]] = defaultdict(dict)
    for _, row in total_df.iterrows():
        code = row["region_code"]
        year = int(row["year"])
        val = row["value"]
        matrix[code][year] = str(int(val)) if val is not None else "—"

    # Заголовок
    year_header = " | ".join(str(y) for y in years)
    lines.append(f"| region_code | region_name | {year_header} |")
    sep = " | ".join("---:" for _ in years)
    lines.append(f"|-------------|-------------|{sep}|")

    gaps = []
    for code in canonical:
        name = code_to_name.get(code, code)
        cells = [matrix[code].get(y, "") for y in years]
        missing_for = [y for y in years if not matrix[code].get(y)]
        if missing_for:
            gaps.append((code, missing_for))
        row_str = " | ".join(c if c else "⚠" for c in cells)
        lines.append(f"| {code} | {name} | {row_str} |")

    lines.append("")
    if gaps:
        lines.append(f"**Пропуски ({len(gaps)} регионов):**")
        for code, yrs in gaps:
            lines.append(f"- {code} ({code_to_name.get(code, code)}): нет данных за {yrs}")
    else:
        lines.append("**Матрица полная — все регионы × все годы заполнены.**")

    lines.append("")
    return "\n".join(lines)


# ── Точка входа ───────────────────────────────────────────────────────────────

def run(cat: SqlCatalog) -> str:
    """
    Генерирует полный Markdown-отчёт.
    Возвращает путь к сохранённому файлу.
    """
    import pandas as pd

    os.makedirs(REPORTS_DIR, exist_ok=True)

    silver_df = _load_table_df(cat, "silver.doshkolka")
    lookup_df = _load_table_df(cat, "bronze.region_lookup")

    now = datetime.now(tz=timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y-%m-%d %H:%M UTC")

    total_rows = len(silver_df)
    regions_count = silver_df["region_code"].nunique() if not silver_df.empty else 0
    years_count = silver_df["year"].nunique() if not silver_df.empty else 0

    sections = [
        "# Отчёт валидации silver.doshkolka",
        "",
        f"**Дата:** {ts_str}",
        "",
        "| | |",
        "|--|--|",
        f"| Всего строк | {total_rows} |",
        f"| Уникальных регионов | {regions_count} |",
        f"| Уникальных годов | {years_count} |",
        "",
        "---",
        "",
        _build_region_coverage(silver_df, lookup_df),
        "---",
        "",
        _build_year_coverage(silver_df),
        "---",
        "",
        _build_matrix(silver_df, lookup_df),
    ]

    report_content = "\n".join(sections)
    report_path = os.path.join(REPORTS_DIR, f"silver_doshkolka_{date_str}.md")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    print(f"Отчёт сохранён: {report_path}")
    return report_path


if __name__ == "__main__":
    cat = _get_catalog()
    run(cat)
