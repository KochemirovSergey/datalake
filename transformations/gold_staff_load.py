"""
Silver + Bronze → Gold: педагогическая нагрузка по уровням образования.

Что делает:
  - Читает silver.staff_shortage_triggers (основной источник тригеров и бонуса)
  - Читает bronze.oo_1_3_4_230, bronze.oo_1_3_1_218, bronze.oo_1_3_2_221
  - Агрегирует Bronze-таблицы по (регион, год, level) с суммированием по всем тегам
  - Джойнит Silver с агрегированными Bronze-данными
  - Пишет в gold.staff_load

Ключи: region_code, year, level
Уровни: 1.2, 1.3+1.4, 2.5_bachelor, 2.5_specialist, 2.5_master
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


def _pivot_table(df: pd.DataFrame, reg_col: str, year_col: str,
                 col_num_col: str, row_num_col: str, val_col: str,
                 row_filter=None, combinations: list | None = None) -> pd.DataFrame:
    """Сводит данные из «длинного» формата в широкий, агрегируя по всем тегам."""
    work = df.copy()
    work["_val"] = pd.to_numeric(work[val_col], errors="coerce").fillna(0)

    if row_filter is not None:
        work = work[work[row_num_col].isin(row_filter)]

    # GROUP BY (регион, год) — суммируем по всем тегам
    grouped = work.groupby([reg_col, year_col])

    result_rows = []
    for (reg, yr), grp in grouped:
        row = {reg_col: reg, year_col: yr}
        if combinations:
            for c_num, r_num, new_name in combinations:
                mask = (grp[col_num_col].astype(str) == str(c_num)) & (grp[row_num_col].astype(str) == str(r_num))
                row[new_name] = grp.loc[mask, "_val"].sum()
        result_rows.append(row)

    return pd.DataFrame(result_rows) if result_rows else pd.DataFrame(
        columns=[reg_col, year_col] + ([c[2] for c in combinations] if combinations else [])
    )


def load_form230(cat: SqlCatalog) -> pd.DataFrame:
    """Загружает ставки и численность из bronze.oo_1_3_4_230.

    Возвращает: регион, год, level, positions_total, staff_headcount.
    """
    log.info("Загружаем bronze.oo_1_3_4_230...")
    df = cat.load_table("bronze.oo_1_3_4_230").scan().to_pandas()

    # Фильтруем по Городским организациям и нужным столбцам
    df = df[
        (df["регион"].notna()) &
        (df["тег_2"] == "Городские организации") &
        (df["column_name"].isin([
            "Число ставок   по штату, единиц",
            "Численность работников на начало отчет- ного учебного года (без совместителей и работающих по договорам гражданско-правового характера), человек"
        ]))
    ].copy()

    df["_val"] = pd.to_numeric(df["значение"], errors="coerce").fillna(0)
    df["год"] = pd.to_numeric(df["год"], errors="coerce").astype("Int64")
    df = df[df["год"].notna()].copy()
    df["год"] = df["год"].astype(int)

    # Агрегируем по (регион, год, column_name)
    agg = df.groupby(["регион", "год", "column_name"])["_val"].sum().reset_index()

    # Пивотируем по column_name
    pivot = agg.pivot_table(
        index=["регион", "год"],
        columns="column_name",
        values="_val",
        fill_value=0
    ).reset_index()

    # После pivot, столбцы находятся в pivot.columns
    # Поищем нужные колонки по префиксам
    positions_col = None
    headcount_col = None

    for col in pivot.columns:
        if "Число ставок" in str(col):
            positions_col = col
        if "Численность работников на начало отчет" in str(col):
            headcount_col = col

    rows = []
    if positions_col is not None and headcount_col is not None:
        for _, r in pivot.iterrows():
            # Для каждого уровня используем одинаковые значения ставок
            # (поделить поровну между 1.2 и 1.3+1.4)
            for level in ["Учитель начальных классов", "Учитель предметник"]:
                rows.append({
                    "region_id": r["регион"],
                    "year": r["год"],
                    "level": level,
                    "positions_total": float(r[positions_col]) / 2 if r[positions_col] else 0,
                    "staff_headcount": float(r[headcount_col]) / 2 if r[headcount_col] else 0,
                })
    else:
        log.warning("Не найдены нужные колонки в form230. Доступные: %s", pivot.columns.tolist())

    result = pd.DataFrame(rows)
    log.info("  Строк form230 после агрегации: %d", len(result))
    return result


def load_form218(cat: SqlCatalog) -> pd.DataFrame:
    """Загружает квалификационные показатели из bronze.oo_1_3_1_218.

    Возвращает: регион, год, level, qual_higher_edu, qual_higher_cat, qual_first_cat,
                qual_ped_higher, qual_spe_mid, qual_ped_mid, qual_candidate, qual_doctor.
    """
    log.info("Загружаем bronze.oo_1_3_1_218...")
    df = cat.load_table("bronze.oo_1_3_1_218").scan().to_pandas()

    # Фильтруем
    df = df[
        (df["регион"].notna()) &
        (df["тег_1"] == "Государственные организации") &
        (df["тег_2"] == "Городские организации") &
        (df["row_number"].isin(["7", "8"]))
    ].copy()

    # Пивотируем: нужны колонки 3-16 для строк 7 и 8
    t218_combos = (
        [("3", "7", "r7_den"), ("3", "8", "r8_den")]
        + [(str(c), "7", f"r7_c{c}") for c in range(4, 17)]
        + [(str(c), "8", f"r8_c{c}") for c in range(4, 17)]
    )
    t218 = _pivot_table(df, "регион", "год", "column_number", "row_number", "значение",
                        combinations=t218_combos)

    t218["год"] = pd.to_numeric(t218["год"], errors="coerce").astype("Int64")
    t218 = t218[t218["год"].notna()].copy()
    t218["год"] = t218["год"].astype(int)

    rows = []
    # Маппинг колонок (из ТЗ):
    # col 4=высшее, col 5=высшая, col 6=первая, col 7=пед.высшее,
    # col 10=сред.проф, col 11=пед.сред.проф, col 12=канд., col 13=доктор
    qual_map = {
        "r8_c4": "qual_higher_edu",         # col 4 - высшее
        "r8_c5": "qual_higher_cat",         # col 5 - высшая категория
        "r8_c6": "qual_first_cat",          # col 6 - первая категория
        "r8_c7": "qual_ped_higher",         # col 7 - пед.высшее
        "r8_c10": "qual_spe_mid",           # col 10 - сред.проф.специалисты
        "r8_c11": "qual_ped_mid",           # col 11 - пед.сред.проф
        "r8_c12": "qual_candidate",         # col 12 - канд.наук
        "r8_c13": "qual_doctor",            # col 13 - доктор.наук
    }

    for _, r in t218.iterrows():
        row_data = {
            "region_id": r["регион"],
            "year": r["год"],
            "level": "Учитель начальных классов",
        }
        for src_col, dst_col in qual_map.items():
            row_data[dst_col] = r.get(src_col, 0)
        rows.append(row_data)

        # Для "Учитель предметник" — разница (row 7 - row 8)
        row_data = {
            "region_id": r["регион"],
            "year": r["год"],
            "level": "Учитель предметник",
        }
        qual_map_pred = {
            "qual_higher_edu": ("r7_c4", "r8_c4"),
            "qual_higher_cat": ("r7_c5", "r8_c5"),
            "qual_first_cat": ("r7_c6", "r8_c6"),
            "qual_ped_higher": ("r7_c7", "r8_c7"),
            "qual_spe_mid": ("r7_c10", "r8_c10"),
            "qual_ped_mid": ("r7_c11", "r8_c11"),
            "qual_candidate": ("r7_c12", "r8_c12"),
            "qual_doctor": ("r7_c13", "r8_c13"),
        }
        for dst_col, (src_col7, src_col8) in qual_map_pred.items():
            row_data[dst_col] = r.get(src_col7, 0) - r.get(src_col8, 0)
        rows.append(row_data)

    result = pd.DataFrame(rows)
    log.info("  Строк form218 после агрегации: %d", len(result))
    return result


def load_form221(cat: SqlCatalog) -> pd.DataFrame:
    """Загружает показатели стажа из bronze.oo_1_3_2_221.

    Возвращает: регион, год, level, exp_total, exp_none, exp_lt3, exp_3_5, exp_5_10,
                exp_10_15, exp_15_20, exp_gt20.
    """
    log.info("Загружаем bronze.oo_1_3_2_221...")
    df = cat.load_table("bronze.oo_1_3_2_221").scan().to_pandas()

    df = df[
        (df["регион"].notna()) &
        (df["row_number"].isin(["7", "8"]))
    ].copy()

    df["_val"] = pd.to_numeric(df["значение"], errors="coerce").fillna(0)
    df["год"] = pd.to_numeric(df["год"], errors="coerce").astype("Int64")
    df = df[df["год"].notna()].copy()
    df["год"] = df["год"].astype(int)

    rows = []
    # Маппинг стажа
    exp_map = {
        "3": "exp_total",
        "4": "exp_none",
        "5": "exp_lt3",
        "6": "exp_3_5",
        "7": "exp_5_10",
        "8": "exp_10_15",
        "9": "exp_15_20",
        "11": "exp_gt20",
    }

    # Группируем по (регион, год, row_number, column_number)
    for (reg, year, row_num), group_df in df.groupby(["регион", "год", "row_number"]):
        row_data = {
            "region_id": reg,
            "year": year,
            "level": "Учитель начальных классов" if row_num == "8" else "Учитель предметник",
        }

        # Для каждого стажа, берём значение из этой строки
        for col_num, exp_field in exp_map.items():
            val = group_df[group_df["column_number"].astype(str) == col_num]["_val"].sum()
            row_data[exp_field] = val

        rows.append(row_data)

    result = pd.DataFrame(rows)
    log.info("  Строк form221 после агрегации: %d", len(result))
    return result


def run() -> int:
    """Выполнить трансформацию Silver + Bronze → Gold для staff_load."""
    cat = _get_catalog()

    # Загружаем Silver как основной источник
    log.info("Загружаем silver.staff_shortage_triggers...")
    silver = cat.load_table("silver.staff_shortage_triggers").scan().to_pandas()
    silver = silver.rename(columns={
        "region_code": "region_id",
        "level": "level",
        "student_count": "student_count",
        "trig1_val": "load_ratio",
        "trig2_val": "vacancy_unfilled_share",
        "bonus_score": "avg_hours_per_teacher",
        "score": "shortage_score",
    })
    log.info("  Строк в silver: %d", len(silver))

    # Загружаем Bronze-таблицы и агрегируем
    form230 = load_form230(cat)
    form218 = load_form218(cat)
    form221 = load_form221(cat)

    # Последовательно джойним: сначала silver с form230
    result = silver.merge(form230, on=["region_id", "year", "level"], how="left")

    # Затем с form218
    result = result.merge(form218, on=["region_id", "year", "level"], how="left")

    # Затем с form221
    result = result.merge(form221, on=["region_id", "year", "level"], how="left")

    # Рассчитываем производные колонки
    result["vacancies_unfilled"] = result["positions_total"] - result["staff_headcount"]
    result["staff_fte"] = 0.0  # TODO: добавить из Bronze, если есть

    # FTE также можно вычислить из form218 col 3

    # Приводим all numeric to FLOAT
    numeric_cols = [col for col in result.columns if col not in ["region_id", "year", "level"]]
    for col in numeric_cols:
        result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0.0).astype(float)

    # Переименовываем в финальные имена
    result = result.rename(columns={"region_id": "region_code"})

    # Сортируем
    result = result.sort_values(["region_code", "year", "level"]).reset_index(drop=True)

    # Пишем в gold.staff_load
    table = pa.Table.from_pandas(result)
    tbl = cat.load_table("gold.staff_load")
    tbl.overwrite(table)

    log.info("  Строк в gold.staff_load: %d", len(result))
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
    print(f"✓ gold.staff_load: {count} rows")
