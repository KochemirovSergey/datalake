"""
Слой 1: Извлечение (Extraction)

Каждый ассет:
  - вызывает чистую функцию-экстрактор из ingestion/
  - добавляет lineage-колонки (_etl_loaded_at, _source_file, _sheet_name, _row_number)
  - возвращает pd.DataFrame (запись в bronze схему — задача IO Manager)

Группы:
  1_excel    — данные из Excel-файлов
  1_postgres — данные из PostgreSQL
  1_json     — справочник регионов
  1_csv      — справочник кодов программ
"""

import os
import sys

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


# ── 1_excel ────────────────────────────────────────────────────────────────────

@asset(
    group_name="1_excel",
    io_manager_key="bronze_io_manager",
    description="Первичка: Excel (Дошколка). Источник: data/Дошколка/",
)
def obuch_doshkolka(context: AssetExecutionContext) -> pd.DataFrame:
    from ingestion.excel_extractor import extract
    data_dir = os.path.join(_project_root, "data", "Дошколка")
    df = extract(data_dir, flat=False)
    context.add_output_metadata({
        "total_rows":   MetadataValue.int(len(df)),
        "total_sheets": MetadataValue.int(df["_sheet_name"].nunique() if not df.empty else 0),
        "years":        MetadataValue.text(
            str(sorted(df["_year"].unique().tolist())) if not df.empty else "[]"
        ),
    })
    return df


@asset(
    group_name="1_excel",
    io_manager_key="bronze_io_manager",
    description="Первичка: Excel (Дошколка, педагоги). Категории: возраст, стаж, образование, ставки. Источник: data/Дошколка/",
)
def ped_doshkolka(context: AssetExecutionContext) -> pd.DataFrame:
    from ingestion.excel_extractor import extract_ped_doshkolka
    data_dir = os.path.join(_project_root, "data", "Дошколка")
    df = extract_ped_doshkolka(data_dir)
    context.add_output_metadata({
        "total_rows":   MetadataValue.int(len(df)),
        "categories":   MetadataValue.text(
            str(sorted(df["_ped_category"].unique().tolist())) if not df.empty else "[]"
        ),
        "years":        MetadataValue.text(
            str(sorted(df["_year"].unique().tolist())) if not df.empty else "[]"
        ),
    })
    return df


@asset(
    group_name="1_excel",
    io_manager_key="bronze_io_manager",
    description="Первичка: Excel (Население). Источник: data/Население/",
)
def naselenie(context: AssetExecutionContext) -> pd.DataFrame:
    from ingestion.excel_extractor import extract
    data_dir = os.path.join(_project_root, "data", "Население")
    df = extract(data_dir, flat=True)
    context.add_output_metadata({
        "total_rows":   MetadataValue.int(len(df)),
        "total_sheets": MetadataValue.int(df["_sheet_name"].nunique() if not df.empty else 0),
        "years":        MetadataValue.text(
            str(sorted(df["_year"].unique().tolist())) if not df.empty else "[]"
        ),
    })
    return df


# ── 1_postgres ─────────────────────────────────────────────────────────────────

@asset(
    group_name="1_postgres",
    io_manager_key="bronze_io_manager",
    description="Первичка: Postgres (Обуч. ОО). Таблицы: oo_1_2_7_*, oo_1_2_14_*, discipuli",
)
def obuch_oo(context: AssetExecutionContext) -> pd.DataFrame:
    from ingestion.postgres_extractor import read_obuch_oo
    df = read_obuch_oo()
    context.add_output_metadata({
        "total_rows":   MetadataValue.int(len(df)),
        "source_tables": MetadataValue.text(
            str(df["_source_file"].unique().tolist()) if not df.empty else "[]"
        ),
    })
    return df


@asset(
    group_name="1_postgres",
    io_manager_key="bronze_io_manager",
    description="Первичка: Postgres (Обуч. ВПО). Таблица: впо_1_р2_13_54",
)
def obuch_vpo(context: AssetExecutionContext) -> pd.DataFrame:
    from ingestion.postgres_extractor import read_obuch_vpo
    df = read_obuch_vpo()
    context.add_output_metadata({"total_rows": MetadataValue.int(len(df))})
    return df


@asset(
    group_name="1_postgres",
    io_manager_key="bronze_io_manager",
    description="Первичка: Postgres (Обуч. СПО). Таблица: спо_1_р2_101_43",
)
def obuch_spo(context: AssetExecutionContext) -> pd.DataFrame:
    from ingestion.postgres_extractor import read_obuch_spo
    df = read_obuch_spo()
    context.add_output_metadata({"total_rows": MetadataValue.int(len(df))})
    return df


@asset(
    group_name="1_postgres",
    io_manager_key="bronze_io_manager",
    description="Первичка: Postgres (Обуч. ПК). Таблица: пк_1_2_4_180",
)
def obuch_pk(context: AssetExecutionContext) -> pd.DataFrame:
    from ingestion.postgres_extractor import read_obuch_pk
    df = read_obuch_pk()
    context.add_output_metadata({"total_rows": MetadataValue.int(len(df))})
    return df


@asset(
    group_name="1_postgres",
    io_manager_key="bronze_io_manager",
    description="Первичка: Postgres (Общаги ВПО). Таблицы: впо_2_р1_3_8, впо_2_р1_4_10",
)
def obshagi_vpo(context: AssetExecutionContext) -> pd.DataFrame:
    from ingestion.postgres_extractor import read_obshagi_vpo
    df = read_obshagi_vpo()
    context.add_output_metadata({
        "total_rows":   MetadataValue.int(len(df)),
        "source_tables": MetadataValue.text(
            str(df["_source_file"].unique().tolist()) if not df.empty else "[]"
        ),
    })
    return df


@asset(
    group_name="1_postgres",
    io_manager_key="bronze_io_manager",
    description="Первичка: Postgres (Педагоги ОО). Таблицы: oo_1_3_4_230, oo_1_3_1_218, oo_1_3_2_221",
)
def ped_oo(context: AssetExecutionContext) -> pd.DataFrame:
    from ingestion.postgres_extractor import read_ped_oo
    df = read_ped_oo()
    context.add_output_metadata({
        "total_rows":   MetadataValue.int(len(df)),
        "source_tables": MetadataValue.text(
            str(df["_source_file"].unique().tolist()) if not df.empty else "[]"
        ),
    })
    return df


# ── 1_json / 1_csv ─────────────────────────────────────────────────────────────

@asset(
    group_name="1_json",
    io_manager_key="bronze_io_manager",
    description="Первичка: Справочник регионов РФ. Источник: data/regions.json",
)
def regions(context: AssetExecutionContext) -> pd.DataFrame:
    from ingestion.json_extractor import read_regions
    df = read_regions()
    context.add_output_metadata({
        "total_rows":    MetadataValue.int(len(df)),
        "canonical_cnt": MetadataValue.int(int((~df["is_alias"]).sum()) if not df.empty else 0),
        "alias_cnt":     MetadataValue.int(int(df["is_alias"].sum()) if not df.empty else 0),
    })
    return df


@asset(
    group_name="1_csv",
    io_manager_key="bronze_io_manager",
    description="Первичка: Справочник кодов программ. Источник: data/programma_educationis_codicem.csv",
)
def code_programm(context: AssetExecutionContext) -> pd.DataFrame:
    from ingestion.json_extractor import read_code_programm
    df = read_code_programm()
    context.add_output_metadata({"total_rows": MetadataValue.int(len(df))})
    return df
