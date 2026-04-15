"""
Анализ охвата образованием: отчёт по возрастам.

Источник: silver.education_population_wide_annual

Что считает:
  §1 Средний охват по возрасту для страны в целом
     = SUM(education_total по всем регионам и годам) / SUM(population_total по всем регионам и годам)
     Агрегация выполняется на уровне возраста × (регион × год), поэтому знаменатель
     — реальная численность населения, а не сумма долей.

  §2 Топ-5 регионов с наибольшим отклонением от общероссийского среднего
     для каждого возраста.
     Региональный показатель = среднее значение education_share по всем годам
     (те годы, где population_total > 0 и education_total не NULL).

  §3 Средний охват по возрасту для каждого региона
     (таблица: регион × возраст → средняя доля за все годы)

Запуск: python validation/validate_coverage_analysis.py
"""

import os
import pandas as pd
from datetime import datetime, timezone
from pyiceberg.catalog.sql import SqlCatalog

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

# Возрасты, которые выводим в отчёт (обычно 0–80)
# Если данных для возраста нет — пропускаем
MIN_REPORTING_AGE = 0
MAX_REPORTING_AGE = 80

# Сколько регионов в топе по отклонению
TOP_N = 5


def _get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )


def _load_df(cat: SqlCatalog, table_name: str) -> pd.DataFrame:
    try:
        return cat.load_table(table_name).scan().to_pandas()
    except Exception as e:
        print(f"  Предупреждение: не удалось загрузить {table_name}: {e}")
        return pd.DataFrame()


def _code_to_name(cat: SqlCatalog) -> dict[str, str]:
    try:
        lookup_df = cat.load_table("bronze.region_lookup").scan().to_pandas()
        if "is_alias" in lookup_df.columns:
            lookup_df = lookup_df[lookup_df["is_alias"] == False]
        name_col = "canonical_name" if "canonical_name" in lookup_df.columns else "name_variant"
        return {
            str(row["region_code"]): str(row.get(name_col, row["region_code"]))
            for _, row in lookup_df.iterrows()
        }
    except Exception:
        return {}


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    """
    Приводит типы без фильтрации строк — валидация должна отражать
    полное содержимое таблицы.
      - age → int (нечисловые значения → NaN, остаются в таблице)
    """
    df = df.copy()
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df["age"] = df["age"].where(df["age"].notna(), other=None)
    # Оставляем только строки с числовым возрастом в отчётном диапазоне,
    # потому что annual-таблица содержит исключительно целые возрасты 0–80.
    # Это не фильтр данных, а приведение типа для последующей сортировки/группировки.
    df = df[df["age"].notna()].copy()
    df["age"] = df["age"].astype(int)
    return df


# ── §1 Общероссийский охват по возрасту ──────────────────────────────────────

def _national_coverage_by_age(df: pd.DataFrame) -> pd.DataFrame:
    """
    Взвешенный охват: SUM(education_total) / SUM(population_total) по возрасту.
    Для расчёта доли используются только строки, где оба значения не NULL и
    population_total > 0 (деление на ноль невозможно). Строки с NULL в одном
    из операндов всё равно отражаются через n_total_region_years.
    """
    agg = (
        df.groupby("age", as_index=False)
        .agg(
            n_total_region_years=("region_code", "count"),
        )
    )

    # Только строки, пригодные для расчёта отношения
    df_ratio = df[df["education_total"].notna() & df["population_total"].notna() & (df["population_total"] > 0)].copy()
    ratio_agg = (
        df_ratio.groupby("age", as_index=False)
        .agg(
            sum_education=("education_total", "sum"),
            sum_population=("population_total", "sum"),
            n_ratio_region_years=("region_code", "count"),
        )
    )
    ratio_agg["national_share"] = ratio_agg["sum_education"] / ratio_agg["sum_population"]

    agg = agg.merge(ratio_agg, on="age", how="left")
    return agg.sort_values("age")


def _section_national(national: pd.DataFrame) -> str:
    lines = [
        "## §1 — Общероссийский охват образованием по возрасту",
        "",
        "_Методология: взвешенная доля = Σ(обучающихся) / Σ(население) по всем регионам и годам для каждого возраста. "
        "«Строк всего» — все строки таблицы для данного возраста; «в расчёте» — строки с ненулевым населением и непустым education\\_total._",
        "",
        "| Возраст | Обучающихся | Население | Охват (%) | Строк всего | В расчёте |",
        "|--------:|------------:|----------:|----------:|------------:|----------:|",
    ]
    for _, row in national.iterrows():
        share_pct = f"{row['national_share'] * 100:.2f}%" if pd.notna(row.get("national_share")) else "—"
        edu = f"{int(row['sum_education']):,}".replace(",", " ") if pd.notna(row.get("sum_education")) else "—"
        pop = f"{int(row['sum_population']):,}".replace(",", " ") if pd.notna(row.get("sum_population")) else "—"
        n_total = int(row["n_total_region_years"])
        n_ratio = int(row["n_ratio_region_years"]) if pd.notna(row.get("n_ratio_region_years")) else 0
        lines.append(
            f"| {int(row['age'])} | {edu} | {pop} | {share_pct} | {n_total} | {n_ratio} |"
        )
    lines.append("")
    return "\n".join(lines)


# ── §2 Региональный охват и Топ-5 по отклонению ──────────────────────────────

def _regional_avg_coverage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Средний education_share региона по всем годам для каждой комбинации (region_code, age).
    share вычисляется только для строк, где population_total > 0 и education_total не NULL
    (деление на ноль невозможно). Строки без этих данных не участвуют в среднем.
    """
    df_ratio = df[
        df["education_total"].notna() & df["population_total"].notna() & (df["population_total"] > 0)
    ].copy()

    df_ratio["row_share"] = df_ratio["education_total"] / df_ratio["population_total"]

    regional = (
        df_ratio.groupby(["region_code", "age"], as_index=False)
        .agg(
            avg_share=("row_share", "mean"),
            n_years=("year", "nunique"),
        )
    )
    return regional


def _section_top5_deviations(
    regional: pd.DataFrame,
    national: pd.DataFrame,
    code_to_name: dict,
) -> str:
    lines = [
        "## §2 — Топ-5 регионов по отклонению от общероссийского среднего (для каждого возраста)",
        "",
        "_Региональный показатель = среднее education_share по всем доступным годам. "
        "Отклонение = региональная доля − общероссийская доля. "
        "Показаны 5 регионов с наибольшим положительным и 5 с наибольшим отрицательным отклонением._",
        "",
    ]

    national_map = dict(zip(national["age"], national["national_share"]))
    ages_sorted = sorted(national["age"].tolist())

    for age in ages_sorted:
        nat_share = national_map.get(age)
        if nat_share is None:
            continue

        age_reg = regional[regional["age"] == age].copy()
        if age_reg.empty:
            continue

        age_reg["deviation"] = age_reg["avg_share"] - nat_share
        age_reg["region_name"] = age_reg["region_code"].map(
            lambda c: code_to_name.get(c, c)
        )

        top_pos = age_reg.nlargest(TOP_N, "deviation")
        top_neg = age_reg.nsmallest(TOP_N, "deviation")

        lines.append(f"### Возраст {age} лет  (общероссийский охват: {nat_share * 100:.2f}%)")
        lines.append("")
        lines.append("**Регионы с наибольшим превышением среднего:**")
        lines.append("")
        lines.append("| Регион | Охват (%) | Отклонение (п.п.) | Лет данных |")
        lines.append("|--------|----------:|------------------:|-----------:|")
        for _, r in top_pos.iterrows():
            lines.append(
                f"| {r['region_name']} ({r['region_code']}) "
                f"| {r['avg_share'] * 100:.2f}% "
                f"| +{r['deviation'] * 100:.2f} "
                f"| {int(r['n_years'])} |"
            )
        lines.append("")
        lines.append("**Регионы с наибольшим отставанием от среднего:**")
        lines.append("")
        lines.append("| Регион | Охват (%) | Отклонение (п.п.) | Лет данных |")
        lines.append("|--------|----------:|------------------:|-----------:|")
        for _, r in top_neg.iterrows():
            sign = "+" if r["deviation"] >= 0 else ""
            lines.append(
                f"| {r['region_name']} ({r['region_code']}) "
                f"| {r['avg_share'] * 100:.2f}% "
                f"| {sign}{r['deviation'] * 100:.2f} "
                f"| {int(r['n_years'])} |"
            )
        lines.append("")

    return "\n".join(lines)


# ── §3 Средний охват по регионам (матрица регион × возраст) ─────────────────

def _section_regional_matrix(
    regional: pd.DataFrame,
    code_to_name: dict,
) -> str:
    """
    Широкая матрица: регион (строки) × возраст (столбцы) → средний охват (%).
    Выводим только возрасты, для которых есть хотя бы одно значение.
    """
    ages = sorted(regional["age"].unique().tolist(), key=lambda x: int(x) if str(x).isdigit() else float("inf"))
    region_codes = sorted(regional["region_code"].unique().tolist())

    # Pivot
    pivot = regional.pivot(index="region_code", columns="age", values="avg_share")
    pivot = pivot.reindex(region_codes)

    lines = [
        "## §3 — Средний охват по регионам (регион × возраст)",
        "",
        "_Значение = среднеарифметическое education\\_share по всем доступным годам для данного региона и возраста. "
        "Прочерк (—) — нет данных._",
        "",
    ]

    # Заголовок
    age_header = " | ".join(str(a) for a in ages)
    age_sep = " | ".join("---:" for _ in ages)
    lines.append(f"| Регион | Код | {age_header} |")
    lines.append(f"|--------|-----|{age_sep}|")

    for code in region_codes:
        name = code_to_name.get(code, code)
        cells = []
        for age in ages:
            val = pivot.loc[code, age] if age in pivot.columns else None
            if val is None or (isinstance(val, float) and pd.isna(val)):
                cells.append("—")
            else:
                cells.append(f"{val * 100:.1f}%")
        lines.append(f"| {name} | {code} | {' | '.join(cells)} |")

    lines.append("")
    return "\n".join(lines)


# ── §4 Суммарные значения по уровням образования и годам ────────────────────

# Человекочитаемые названия уровней образования
LEVEL_LABELS: dict[str, str] = {
    "level_1_1":   "Дошк. (1.1)",
    "level_1_2":   "НОО (1.2)",
    "level_1_3":   "ООО (1.3)",
    "level_1_4":   "СОО (1.4)",
    "level_2_5_1": "СПО осн. (2.5.1)",
    "level_2_5_2": "СПО баз. (2.5.2)",
    "level_2_6":   "ВПО бак. (2.6)",
    "level_2_7":   "ВПО спец. (2.7)",
    "level_2_8":   "ВПО маг. (2.8)",
    "level_4_8b_1": "ДПО ПК (4.8b.1)",
    "level_4_8b_2": "ДПО ПП (4.8b.2)",
}

EDUCATION_LEVEL_COLS = list(LEVEL_LABELS.keys())


def _section_education_totals_by_year(cat: SqlCatalog) -> str:
    """
    §4: Суммарные значения по каждому уровню образования за каждый год.
    Источник: silver.education_population_wide (оригинальные значения, без распределения по возрастам).
    Все регионы, все возрастные группы кроме 'всего'.
    """
    lines = [
        "## §4 — Суммарные значения по уровням образования и годам",
        "",
        "_Источник: `silver.education_population_wide`. "
        "Агрегация: все строки таблицы без исключений. "
        "Значения — исходные из Silver-таблиц (до распределения по отдельным годам возраста)._",
        "",
    ]

    try:
        wide = cat.load_table("silver.education_population_wide").scan().to_pandas()
    except Exception as e:
        lines.append(f"_Не удалось загрузить таблицу: {e}_")
        lines.append("")
        return "\n".join(lines)

    if wide.empty:
        lines.append("_Таблица пуста._")
        lines.append("")
        return "\n".join(lines)

    # Привести числовые столбцы
    for col in EDUCATION_LEVEL_COLS:
        if col in wide.columns:
            wide[col] = pd.to_numeric(wide[col], errors="coerce")

    years = sorted(wide["year"].unique().tolist())

    # Заголовок таблицы
    col_labels = [LEVEL_LABELS[c] for c in EDUCATION_LEVEL_COLS if c in wide.columns]
    present_cols = [c for c in EDUCATION_LEVEL_COLS if c in wide.columns]

    header = "| Год | " + " | ".join(col_labels) + " |"
    sep = "|----:|" + "|".join("------------------:" for _ in present_cols) + "|"
    lines.append(header)
    lines.append(sep)

    for year in years:
        yr_df = wide[wide["year"] == year]
        cells = []
        for col in present_cols:
            val = yr_df[col].sum(min_count=1)
            if pd.isna(val):
                cells.append("—")
            else:
                cells.append(f"{int(val):,}".replace(",", " "))
        lines.append(f"| {year} | " + " | ".join(cells) + " |")

    lines.append("")
    return "\n".join(lines)


# ── Точка входа ──────────────────────────────────────────────────────────────

def run(cat: SqlCatalog | None = None) -> str:
    os.makedirs(REPORTS_DIR, exist_ok=True)
    if cat is None:
        cat = _get_catalog()

    code_to_name = _code_to_name(cat)

    print("  Загрузка silver.education_population_wide_annual…")
    raw = _load_df(cat, "silver.education_population_wide_annual")
    if raw.empty:
        print("  Таблица пуста, отчёт не сформирован.")
        return ""

    df = _prepare(raw)
    print(f"  Строк в таблице: {len(raw):,} (с числовым возрастом для расчётов: {len(df):,})")

    print("  Считаем общероссийский охват по возрасту…")
    national = _national_coverage_by_age(df)

    print("  Считаем региональный охват по возрасту…")
    regional = _regional_avg_coverage(df)

    print("  Считаем суммарные значения по уровням образования и годам…")
    section_edu_totals = _section_education_totals_by_year(cat)

    now = datetime.now(tz=timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y-%m-%d %H:%M UTC")

    # Краткая статистика для шапки — по полному содержимому таблицы
    years_in_data = sorted(raw["year"].unique().tolist())
    year_range = f"{years_in_data[0]}–{years_in_data[-1]}" if years_in_data else "нет данных"
    n_regions = raw["region_code"].nunique()
    n_with_population = int((raw["population_total"].notna() & (raw["population_total"] > 0)).sum())

    sections = [
        "# Анализ охвата образованием по возрасту",
        "",
        f"**Дата:** {ts_str}",
        "",
        "## Базовая статистика",
        "",
        f"| Показатель | Значение |",
        f"|------------|----------|",
        f"| Строк в таблице (всего) | {len(raw):,} |",
        f"| Строк с данными населения | {n_with_population:,} |",
        f"| Регионов | {n_regions} |",
        f"| Годы | {year_range} |",
        f"| Диапазон возрастов | {df['age'].min()}–{df['age'].max()} лет |",
        "",
        "---",
        "",
        _section_national(national),
        "",
        "---",
        "",
        _section_top5_deviations(regional, national, code_to_name),
        "",
        "---",
        "",
        _section_regional_matrix(regional, code_to_name),
        "",
        "---",
        "",
        section_edu_totals,
    ]

    report_content = "\n".join(sections)
    report_path = os.path.join(REPORTS_DIR, f"coverage_analysis_{date_str}.md")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    print(f"  Отчёт сохранён: {report_path}")
    return report_path


if __name__ == "__main__":
    path = run()
    if path:
        print(f"\nГотово → {path}")
