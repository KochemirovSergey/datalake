"""
Расчёт триггеров и балльной оценки дефицита кадров.
Портирован из generate_heatmap.py. Читает из Bronze Iceberg, пишет в silver.staff_shortage_triggers.
"""

import logging
import os

import numpy as np
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

LEVEL_MAP = {
    "1.2":     "Учитель начальных классов",
    "1.3+1.4": "Учитель предметник",
}

# Веса для расчёта бонуса из таблицы 3.1 (13 колонок: 4-16)
WEIGHTS_31 = [1, 2, 1, 2, 3, 5, 0.5, 0.75, 0.5, 3, 3, 3, 3]
# Веса для расчёта бонуса из таблицы 3.2 (12 колонок: 4-9, 11-16, без 10)
WEIGHTS_32 = [0.5, 0.75, 1, 2, 3, 4, 0.75, 1, 2, 3, 5, 7]


def _get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "datalake",
        **{
            "uri":       f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )


def load_enrollment(cat: SqlCatalog) -> pd.DataFrame:
    """Загружает численность обучающихся из bronze.discipuli.

    Возвращает DataFrame: region_id, year, level_id, student_count.
    """
    log.info("Загружаем bronze.discipuli...")
    df = cat.load_table("bronze.discipuli").scan().to_pandas()
    log.info("  Строк в bronze.discipuli: %d", len(df))

    # Все значения хранятся как строки — фильтруем по строкам
    mask = (
        (df["Определение"] == "Численность обучающихся")
        & (df["Уровень субъектности"] == "Регион")
        & (df["гос/негос"] == "сумма")
        & (df["Вид и уровень образования"].isin(["1.2", "1.3", "1.4"]))
    )
    df = df[mask].copy()

    df["student_count"] = pd.to_numeric(df["Значение"], errors="coerce").fillna(0).astype(int)
    df["year"] = pd.to_numeric(df["Год"], errors="coerce").astype("Int64")
    df = df[df["year"].notna()].copy()
    df["year"] = df["year"].astype(int)

    # Схлопываем 1.3 и 1.4 в один уровень
    df["level_id"] = df["Вид и уровень образования"].apply(
        lambda v: "1.3+1.4" if str(v) in ("1.3", "1.4") else str(v)
    )

    result = (
        df.groupby(["Объект", "year", "level_id"], as_index=False)["student_count"].sum()
        .rename(columns={"Объект": "region_id"})
    )
    log.info("  Строк enrollment после агрегации: %d", len(result))
    return result


def _pivot_table(df: pd.DataFrame, reg_col: str, year_col: str,
                 col_num_col: str, row_num_col: str, val_col: str,
                 col_filter=None, row_filter=None,
                 combinations: list | None = None) -> pd.DataFrame:
    """Сводит данные из «длинного» формата (колонка/строка/значение) в широкий.

    combinations: список (col_number, row_number, new_col_name) — какие пивоты создать.
    """
    work = df.copy()
    work["_val"] = pd.to_numeric(work[val_col], errors="coerce").fillna(0)

    if col_filter is not None:
        work = work[work[col_num_col].isin(col_filter)]
    if row_filter is not None:
        work = work[work[row_num_col].isin(row_filter)]

    grouped = work.groupby([reg_col, year_col])

    result_rows = []
    for (reg, yr), grp in grouped:
        row = {reg_col: reg, year_col: yr}
        if combinations:
            for c_num, r_num, new_name in combinations:
                mask = (grp[col_num_col] == c_num) & (grp[row_num_col] == r_num)
                row[new_name] = grp.loc[mask, "_val"].sum()
        result_rows.append(row)

    return pd.DataFrame(result_rows) if result_rows else pd.DataFrame(
        columns=[reg_col, year_col] + ([c[2] for c in combinations] if combinations else [])
    )


def load_triggers(cat: SqlCatalog) -> pd.DataFrame:
    """Загружает триггеры из bronze.oo_1_3_4_230 и bronze.oo_1_3_1_218.

    Воспроизводит SQL_TRIGGERS_VITRINA_HISTORICAL как pandas-агрегации.
    Возвращает DataFrame: region_id, year, level_code, trig1_val, trig2_val.
    """
    log.info("Загружаем bronze.oo_1_3_4_230...")
    df34 = cat.load_table("bronze.oo_1_3_4_230").scan().to_pandas()
    df34 = df34[
        (df34["тег_2"] == "Городские организации")
        & df34["регион"].notna()
    ].copy()

    log.info("Загружаем bronze.oo_1_3_1_218...")
    df31_den = cat.load_table("bronze.oo_1_3_1_218").scan().to_pandas()
    df31_den = df31_den[
        (df31_den["тег_1"] == "Государственные организации")
        & (df31_den["тег_2"] == "Городские организации")
        & df31_den["регион"].notna()
    ].copy()

    # t34_raw: pivot по (регион, год) с нужными столбцами
    t34_combos = [
        ("4", "7", "r7_c4"), ("3", "7", "r7_c3"), ("5", "7", "r7_c5"),
        ("4", "8", "r8_c4"), ("3", "8", "r8_c3"), ("5", "8", "r8_c5"),
    ]
    t34 = _pivot_table(df34, "регион", "год", "column_number", "row_number", "значение",
                       combinations=t34_combos)

    # t31_den_raw: pivot (знаменатель триггера 1)
    t31_den_combos = [
        ("3", "7", "r7_c3"), ("3", "8", "r8_c3"),
    ]
    t31_den = _pivot_table(df31_den, "регион", "год", "column_number", "row_number", "значение",
                           combinations=t31_den_combos)
    t31_den = t31_den.rename(columns={"r7_c3": "den_r7_c3", "r8_c3": "den_r8_c3"})

    # Объединяем
    base = t34.merge(t31_den, on=["регион", "год"], how="left")
    for col in ["den_r7_c3", "den_r8_c3"]:
        base[col] = base[col].fillna(0)

    rows = []

    # Уровень 1.2 (начальные классы, строка 8)
    for _, r in base.iterrows():
        trig1 = r["r8_c4"] / r["den_r8_c3"] if r["den_r8_c3"] != 0 else None
        trig2 = (r["r8_c3"] - r["r8_c5"]) / r["r8_c3"] if r["r8_c3"] != 0 else None
        rows.append({
            "region_id":  r["регион"],
            "year":       r["год"],
            "level_code": "1.2",
            "trig1_val":  trig1,
            "trig2_val":  trig2,
        })

    # Уровень 1.3+1.4 (предметники, строка 7 - строка 8)
    for _, r in base.iterrows():
        den1 = r.get("den_r7_c3", 0) - r.get("den_r8_c3", 0)
        den2 = r["r7_c3"] - r["r8_c3"]
        trig1 = (r["r7_c4"] - r["r8_c4"]) / den1 if den1 != 0 else None
        trig2 = (den2 - (r["r7_c5"] - r["r8_c5"])) / den2 if den2 != 0 else None
        rows.append({
            "region_id":  r["регион"],
            "year":       r["год"],
            "level_code": "1.3+1.4",
            "trig1_val":  trig1,
            "trig2_val":  trig2,
        })

    result = pd.DataFrame(rows)
    result["year"] = pd.to_numeric(result["year"], errors="coerce").astype("Int64")
    result = result[result["year"].notna()].copy()
    result["year"] = result["year"].astype(int)
    log.info("  Строк триггеров: %d", len(result))
    return result


def calc_bonus(cat: SqlCatalog) -> pd.DataFrame:
    """Вычисляет бонусный балл из bronze.oo_1_3_1_218 и bronze.oo_1_3_2_221.

    Воспроизводит bonus_all из SQL_REBUILD_VITRINA.
    Возвращает DataFrame: region_id, year, level_code, bonus_score.
    """
    log.info("Загружаем bronze.oo_1_3_1_218 для бонуса...")
    df31 = cat.load_table("bronze.oo_1_3_1_218").scan().to_pandas()
    df31 = df31[df31["row_number"].isin(["7", "8"]) & df31["регион"].notna()].copy()

    log.info("Загружаем bronze.oo_1_3_2_221 для бонуса...")
    df32 = cat.load_table("bronze.oo_1_3_2_221").scan().to_pandas()
    df32 = df32[df32["row_number"].isin(["7", "8"]) & df32["регион"].notna()].copy()

    # t31_raw: колонки 3-16 строки 7 и 8
    t31_combos = (
        [("3", "7", "r7_den"), ("3", "8", "r8_den")]
        + [(str(c), "7", f"r7_c{c}") for c in range(4, 17)]
        + [(str(c), "8", f"r8_c{c}") for c in range(4, 17)]
    )
    t31 = _pivot_table(df31, "регион", "год", "column_number", "row_number", "значение",
                       combinations=t31_combos)

    # t32_raw: колонки 3-9, 11-16 строки 7 и 8 (нет col10)
    t32_col_nums = [3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16]
    t32_combos = (
        [("3", "7", "r7_den"), ("3", "8", "r8_den")]
        + [(str(c), "7", f"r7_c{c}") for c in t32_col_nums if c != 3]
        + [(str(c), "8", f"r8_c{c}") for c in t32_col_nums if c != 3]
    )
    t32 = _pivot_table(df32, "регион", "год", "column_number", "row_number", "значение",
                       combinations=t32_combos)

    merged = t31.merge(t32, on=["регион", "год"], how="inner",
                       suffixes=("_31", "_32"))

    rows = []
    # Колонки для расчёта score31 (из t31): 4-16
    cols31 = list(range(4, 17))
    # Колонки для расчёта score32 (из t32): 4-9, 11-16
    cols32 = [4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16]

    def _score31(row_prefix, den_col):
        den = merged[den_col].fillna(0)
        s = pd.Series(0.0, index=merged.index)
        for i, c in enumerate(cols31):
            col = f"{row_prefix}_c{c}_31"
            if col in merged.columns:
                s = s + (merged[col].fillna(0) / den.replace(0, np.nan).fillna(0)) * WEIGHTS_31[i]
        return s.where(den != 0, other=None)

    def _score32(row_prefix, den_col):
        den = merged[den_col].fillna(0)
        s = pd.Series(0.0, index=merged.index)
        for i, c in enumerate(cols32):
            col = f"{row_prefix}_c{c}_32"
            if col in merged.columns:
                s = s + (merged[col].fillna(0) / den.replace(0, np.nan).fillna(0)) * WEIGHTS_32[i]
        return s.where(den != 0, other=None)

    # Начальные классы (строка 8)
    s31_12 = _score31("r8", "r8_den_31")
    s32_12 = _score32("r8", "r8_den_32")
    bonus_12 = s31_12.fillna(0) + s32_12.fillna(0)

    # Предметники (строка 7 - строка 8)
    den31_pred = (merged["r7_den_31"].fillna(0) - merged["r8_den_31"].fillna(0))
    den32_pred = (merged["r7_den_32"].fillna(0) - merged["r8_den_32"].fillna(0))

    s31_pred = pd.Series(0.0, index=merged.index)
    s32_pred = pd.Series(0.0, index=merged.index)
    for i, c in enumerate(cols31):
        c7 = f"r7_c{c}_31"; c8 = f"r8_c{c}_31"
        if c7 in merged.columns and c8 in merged.columns:
            diff = merged[c7].fillna(0) - merged[c8].fillna(0)
            s31_pred = s31_pred + (diff / den31_pred.replace(0, np.nan).fillna(0)) * WEIGHTS_31[i]
    for i, c in enumerate(cols32):
        c7 = f"r7_c{c}_32"; c8 = f"r8_c{c}_32"
        if c7 in merged.columns and c8 in merged.columns:
            diff = merged[c7].fillna(0) - merged[c8].fillna(0)
            s32_pred = s32_pred + (diff / den32_pred.replace(0, np.nan).fillna(0)) * WEIGHTS_32[i]

    s31_pred = s31_pred.where(den31_pred != 0, other=None)
    s32_pred = s32_pred.where(den32_pred != 0, other=None)
    bonus_pred = s31_pred.fillna(0) + s32_pred.fillna(0)

    for idx in merged.index:
        rows.append({
            "region_id":  merged.loc[idx, "регион"],
            "year":       merged.loc[idx, "год"],
            "level_code": "1.2",
            "bonus_score": float(bonus_12[idx]) if pd.notna(bonus_12[idx]) else None,
        })
        rows.append({
            "region_id":  merged.loc[idx, "регион"],
            "year":       merged.loc[idx, "год"],
            "level_code": "1.3+1.4",
            "bonus_score": float(bonus_pred[idx]) if pd.notna(bonus_pred[idx]) else None,
        })

    result = pd.DataFrame(rows)
    result["year"] = pd.to_numeric(result["year"], errors="coerce").astype("Int64")
    result = result[result["year"].notna()].copy()
    result["year"] = result["year"].astype(int)
    log.info("  Строк бонуса: %d", len(result))
    return result


def calc_score(df: pd.DataFrame) -> pd.DataFrame:
    """Рассчитывает балльную оценку 0–5 на основе z-score по trig1_val и trig2_val."""
    result_parts = []

    for (year, level), grp in df.groupby(["year", "level"]):
        grp = grp.copy()

        trig1_vals = grp["trig1_val"].dropna()
        mean_t1 = trig1_vals.mean() if len(trig1_vals) > 0 else 0
        std_t1  = trig1_vals.std()  if len(trig1_vals) > 1 else 1

        trig2_vals = grp["trig2_val"].dropna()
        mean_t2 = trig2_vals.mean() if len(trig2_vals) > 0 else 0
        std_t2  = trig2_vals.std()  if len(trig2_vals) > 1 else 1

        def _region_score(row):
            if pd.isna(row["trig1_val"]) or pd.isna(row["trig2_val"]):
                return None
            z1 = (row["trig1_val"] - mean_t1) / std_t1 if std_t1 > 0 else 0
            z2 = (row["trig2_val"] - mean_t2) / std_t2 if std_t2 > 0 else 0
            penalty = max(0, z1) + max(0, z2)
            return max(0.0, 5 * (1 - penalty / 3))

        grp["score"] = grp.apply(_region_score, axis=1)
        result_parts.append(grp)

    return pd.concat(result_parts, ignore_index=True) if result_parts else df


def run(cat: SqlCatalog | None = None) -> int:
    """
    Загружает данные из Bronze, рассчитывает триггеры и балльную оценку,
    пишет результат в silver.staff_shortage_triggers.

    Returns:
        Количество записанных строк (0 если уже загружено).
    """
    if cat is None:
        cat = _get_catalog()

    # Идемпотентность
    tbl = cat.load_table("silver.staff_shortage_triggers")
    existing = tbl.scan(selected_fields=("region_code",)).to_arrow()
    if len(existing) > 0:
        log.info("silver.staff_shortage_triggers уже содержит %d строк, пропускаю", len(existing))
        return 0

    # Загрузка данных
    enrollment = load_enrollment(cat)
    triggers   = load_triggers(cat)
    bonuses    = calc_bonus(cat)

    # Маппинг level_id → отображаемое имя
    enrollment["level"] = enrollment["level_id"].map(LEVEL_MAP)
    enrollment = enrollment[enrollment["level"].notna()]

    triggers["level"] = triggers["level_code"].map(LEVEL_MAP)
    triggers = triggers[triggers["level"].notna()]

    bonuses["level"] = bonuses["level_code"].map(LEVEL_MAP)
    bonuses = bonuses[bonuses["level"].notna()]

    # Джойн enrollment + triggers
    df = enrollment.merge(
        triggers[["region_id", "year", "level", "trig1_val", "trig2_val"]],
        on=["region_id", "year", "level"],
        how="left",
    )

    # Джойн с бонусом
    df = df.merge(
        bonuses[["region_id", "year", "level", "bonus_score"]],
        on=["region_id", "year", "level"],
        how="left",
    )

    # Расчёт оценки
    df = calc_score(df)

    if df.empty:
        log.warning("После объединения DataFrame пуст")
        return 0

    # Сборка PyArrow таблицы
    pa_schema = pa.schema([
        pa.field("region_code",   pa.string(),  nullable=False),
        pa.field("year",          pa.int32(),   nullable=False),
        pa.field("level",         pa.string(),  nullable=False),
        pa.field("student_count", pa.int32(),   nullable=True),
        pa.field("trig1_val",     pa.float64(), nullable=True),
        pa.field("trig2_val",     pa.float64(), nullable=True),
        pa.field("bonus_score",   pa.float64(), nullable=True),
        pa.field("score",         pa.float64(), nullable=True),
    ])

    def _safe_int32(series):
        return pa.array(
            pd.to_numeric(series, errors="coerce").fillna(0).astype(int).tolist(),
            type=pa.int32(),
        )

    def _safe_float(series):
        vals = pd.to_numeric(series, errors="coerce")
        return pa.array([float(v) if pd.notna(v) else None for v in vals], type=pa.float64())

    arrow_tbl = pa.table({
        "region_code":   pa.array(df["region_id"].astype(str).tolist(), type=pa.string()),
        "year":          _safe_int32(df["year"]),
        "level":         pa.array(df["level"].astype(str).tolist(), type=pa.string()),
        "student_count": _safe_int32(df["student_count"]),
        "trig1_val":     _safe_float(df["trig1_val"]),
        "trig2_val":     _safe_float(df["trig2_val"]),
        "bonus_score":   _safe_float(df.get("bonus_score", pd.Series([None] * len(df)))),
        "score":         _safe_float(df.get("score", pd.Series([None] * len(df)))),
    }, schema=pa_schema)

    tbl.append(arrow_tbl)
    count = len(arrow_tbl)
    log.info("Записано %d строк в silver.staff_shortage_triggers", count)
    return count
