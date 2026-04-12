"""
Валидация образовательных витрин Silver-слоя.

Генерирует Markdown-отчёт с секциями:
  §1 Наличие возрастов по уровням
  §2 Покрытие по регионам
  §3 Матрица покрытия по годам
  §4 Error summary

Можно запустить standalone: python validation/validate_silver_education.py
"""

import os
from collections import defaultdict
from datetime import datetime, timezone

from pyiceberg.catalog.sql import SqlCatalog

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

# Конфигурация витрин
SILVER_TABLES = {
    "silver.oo": {
        "level_codes": ["1.2", "1.3", "1.4"],
        "age_col": "age",
    },
    "silver.spo": {
        "level_codes": ["2.5"],
        "program_codes": ["2.5.1", "2.5.2"],
        "age_col": "age",
    },
    "silver.vpo": {
        "level_codes": ["2.6", "2.7", "2.8"],
        "age_col": "age",
    },
    "silver.dpo": {
        "level_codes": ["4.8b"],
        "program_codes": ["4.8b.1", "4.8b.2"],
        "age_col": "age_band",
    },
}

EXPECTED_YEARS = list(range(2018, 2025))


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
        return None


def _code_to_name(cat: SqlCatalog) -> dict[str, str]:
    """Загружает справочник регионов."""
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


# ── Секция 1: Наличие возрастов по уровням ───────────────────────────────────

def _section_age_coverage(cat: SqlCatalog) -> str:
    lines = ["## §1 — Наличие возрастов по уровням", ""]
    
    for table_name, config in SILVER_TABLES.items():
        df = _load_df(cat, table_name)
        if df is None or df.empty:
            lines.append(f"### {table_name}")
            lines.append("_Таблица пуста или не найдена._")
            lines.append("")
            continue
        
        age_col = config["age_col"]
        if age_col not in df.columns:
            lines.append(f"### {table_name}")
            lines.append(f"_Колонка {age_col} не найдена._")
            lines.append("")
            continue
        
        # Определяем level_col
        level_col = "program_code" if "program_codes" in config else "level_code"
        if level_col not in df.columns:
            level_col = "level_code"
        
        lines.append(f"### {table_name}")
        lines.append("")
        lines.append(f"| {level_col} | {age_col} | записей |")
        lines.append(f"|{'-'*10}|{'-'*15}|{'-'*10}|")
        
        # Группировка по уровню и возрасту
        grouped = df.groupby([level_col, age_col]).size().reset_index(name="count")
        for _, row in grouped.iterrows():
            lines.append(f"| {row[level_col]} | {row[age_col]} | {row['count']} |")
        
        lines.append("")
    
    return "\n".join(lines)


# ── Секция 2: Покрытие по регионам ───────────────────────────────────────────

def _section_region_coverage(cat: SqlCatalog, code_to_name_map: dict) -> str:
    lines = ["## §2 — Покрытие по регионам", ""]
    
    canonical_codes = set(code_to_name_map.keys())
    
    for table_name, config in SILVER_TABLES.items():
        df = _load_df(cat, table_name)
        if df is None or df.empty:
            continue
        
        level_col = "program_code" if "program_codes" in config else "level_code"
        if level_col not in df.columns:
            level_col = "level_code"
        
        age_col = config["age_col"]
        if age_col not in df.columns:
            continue
        
        lines.append(f"### {table_name}")
        lines.append("")
        lines.append(f"| level | age | регионов заполнено | регионов нет |")
        lines.append(f"|{'-'*7}|{'-'*15}|{'-'*20}|{'-'*15}|")
        
        # Для каждого уровня и возраста считаем покрытие
        levels = df[level_col].unique() if level_col in df.columns else ["N/A"]
        ages = df[age_col].unique() if age_col in df.columns else []
        
        for level in sorted(levels):
            level_df = df[df[level_col] == level] if level_col in df.columns else df
            for age in sorted(ages):
                sub = level_df[level_df[age_col] == age] if age_col in level_df.columns else level_df
                present_regions = set(sub["region_code"].unique()) if "region_code" in sub.columns else set()
                
                has_regions = len(present_regions)
                missing_regions = len(canonical_codes - present_regions) if canonical_codes else 0
                
                lines.append(f"| {level} | {age} | {has_regions} | {missing_regions} |")
        
        lines.append("")
    
    return "\n".join(lines)


# ── Секция 3: Матрица покрытия по годам ─────────────────────────────────────

def _section_year_matrix(cat: SqlCatalog, code_to_name_map: dict) -> str:
    lines = ["## §3 — Матрица покрытия по годам", ""]
    
    canonical = sorted(code_to_name_map.keys(), key=lambda c: code_to_name_map.get(c, c))
    years = EXPECTED_YEARS
    
    for table_name, config in SILVER_TABLES.items():
        df = _load_df(cat, table_name)
        if df is None or df.empty:
            continue
        
        level_col = "program_code" if "program_codes" in config else "level_code"
        if level_col not in df.columns:
            level_col = "level_code"
        
        age_col = config["age_col"]
        if age_col not in df.columns:
            continue
        
        # Для каждого уровня делаем отдельную таблицу
        levels = df[level_col].unique() if level_col in df.columns else ["N/A"]
        ages = df[age_col].unique() if age_col in df.columns else []
        
        for level in sorted(levels):
            lines.append(f"### {table_name} — level={level}")
            lines.append("")
            
            # Заголовок с годами
            year_header = " | ".join(str(y) for y in years)
            lines.append(f"| region | {year_header} |")
            sep = " | ".join(["---"] + ["---:" for _ in years])
            lines.append(sep)
            
            level_df = df[df[level_col] == level] if level_col in df.columns else df
            
            for code in canonical:
                region_df = level_df[level_df["region_code"] == code] if "region_code" in level_df.columns else level_df
                
                cells = []
                for year in years:
                    year_df = region_df[region_df["year"] == year] if "year" in region_df.columns else region_df
                    # Проверяем наличие данных для любого возраста
                    has_data = len(year_df) > 0 if age_col in year_df.columns else False
                    cells.append("✓" if has_data else "✗")
                
                region_name = code_to_name_map.get(code, code)
                lines.append(f"| {region_name} ({code}) | {' | '.join(cells)} |")
            
            lines.append("")
    
    return "\n".join(lines)


# ── Секция 4: Error summary ────────────────────────────────────────────────

def _section_error_summary(cat: SqlCatalog) -> str:
    lines = ["## §4 — Error summary", ""]
    
    # Загружаем ошибки из bronze_normalized
    try:
        edu_errors = cat.load_table("bronze_normalized.education_level_error").scan().to_pandas()
        if not edu_errors.empty:
            lines.append("### Ошибки нормализации education_level")
            lines.append("")
            
            # Группировка по source_table и error_type
            grouped = edu_errors.groupby(["source_table", "error_type"]).size().reset_index(name="count")
            lines.append("| source_table | error_type | количество |")
            lines.append("|--------------|------------|------------|")
            for _, row in grouped.iterrows():
                lines.append(f"| {row['source_table']} | {row['error_type']} | {row['count']} |")
            lines.append("")
            
            # Топ-20 регионов с наибольшим количеством пропусков
            if "source_table" in edu_errors.columns:
                by_region = edu_errors.groupby("source_table").size().reset_index(name="count")
                by_region = by_region.sort_values("count", ascending=False).head(20)
                
                lines.append("### Топ-20 источников с наибольшим количеством ошибок")
                lines.append("")
                lines.append("| source_table | ошибок |")
                lines.append("|--------------|--------|")
                for _, row in by_region.iterrows():
                    lines.append(f"| {row['source_table']} | {row['count']} |")
                lines.append("")
        else:
            lines.append("_Ошибок нормализации education_level не найдено._")
            lines.append("")
    except Exception as e:
        lines.append(f"_Не удалось загрузить ошибки: {e}_")
        lines.append("")
    
    return "\n".join(lines)


# ── Точка входа ─────────────────────────────────────────────────────────────

def run(cat: SqlCatalog) -> str:
    """Генерирует отчёт валидации образовательных витрин."""
    os.makedirs(REPORTS_DIR, exist_ok=True)
    
    code_map = _code_to_name(cat)
    
    now = datetime.now(tz=timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y-%m-%d %H:%M UTC")
    
    sections = [
        "# Отчёт валидации образовательных витрин Silver-слоя",
        "",
        f"**Дата:** {ts_str}",
        "",
        "---",
        "",
        _section_age_coverage(cat),
        "",
        "---",
        "",
        _section_region_coverage(cat, code_map),
        "",
        "---",
        "",
        _section_year_matrix(cat, code_map),
        "",
        "---",
        "",
        _section_error_summary(cat),
    ]
    
    report_content = "\n".join(sections)
    report_path = os.path.join(REPORTS_DIR, f"silver_education_{date_str}.md")
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)
    
    print(f"Отчёт сохранён: {report_path}")
    return report_path


if __name__ == "__main__":
    cat = _get_catalog()
    run(cat)
