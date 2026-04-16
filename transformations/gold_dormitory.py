"""
Silver → Gold: метрики общежитий по регионам.

Что делает:
  - Читает silver.dormitory_infrastructure
  - Отбирает нужные колонки и переименовывает их согласно ТЗ
  - Пишет в gold.dormitory

Ключи: region_code, year
"""

import logging
import os

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")


def _get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )


def run() -> int:
    """Выполнить трансформацию Silver → Gold для dormitory."""
    cat = _get_catalog()

    log.info("Загружаем silver.dormitory_infrastructure...")
    df = cat.load_table("silver.dormitory_infrastructure").scan().to_pandas()
    log.info("  Строк в source: %d", len(df))

    # Выбираем нужные колонки и переименовываем
    result = pd.DataFrame()
    result["region_code"] = df["region_code"]
    result["year"] = df["year"]
    result["area_total"] = pd.to_numeric(df["area_total"], errors="coerce").fillna(0.0).astype(float)
    result["area_need_repair"] = pd.to_numeric(df["area_need_repair"], errors="coerce").fillna(0.0).astype(float)
    result["repair_share"] = pd.to_numeric(df["metric_1"], errors="coerce").fillna(0.0).astype(float)
    result["area_emergency"] = pd.to_numeric(df["area_emergency"], errors="coerce").fillna(0.0).astype(float)
    result["dorm_shortage_abs"] = pd.to_numeric(df["dorm_shortage_abs"], errors="coerce").fillna(0.0).astype(float)
    result["dorm_shortage_share"] = pd.to_numeric(df["metric_3"], errors="coerce").fillna(0.0).astype(float)
    result["dorm_need"] = pd.to_numeric(df["dorm_need"], errors="coerce").fillna(0.0).astype(float)
    result["dorm_live"] = pd.to_numeric(df["dorm_live"], errors="coerce").fillna(0.0).astype(float)

    # is_forecast — BOOLEAN (required в Iceberg)
    result["is_forecast"] = df["is_forecast"].astype(bool).fillna(False)

    # alert_flag из sigma_sum (если sigma_sum > 0, то alert_flag=1, иначе 0)
    # int32 для соответствия схеме Iceberg
    result["alert_flag"] = (pd.to_numeric(df["sigma_sum"], errors="coerce").fillna(0) > 0).astype("int32")

    # Сортируем
    result = result.sort_values(["region_code", "year"]).reset_index(drop=True)

    # Пишем в gold.dormitory
    table = pa.Table.from_pandas(result)
    tbl = cat.load_table("gold.dormitory")
    tbl.overwrite(table)

    log.info("  Строк в gold.dormitory: %d", len(result))
    return len(result)


def _table_exists(cat: SqlCatalog, table_name: str) -> bool:
    """Проверяет, существует ли таблица."""
    try:
        cat.load_table(table_name)
        return True
    except Exception:
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()
    print(f"✓ gold.dormitory: {count} rows")
