"""
Слой 2: Нормализация (Normalization)  →  схема silver_raw

Структура выхода каждого n_* ассета:
  [region_code, year, edu_level_code, ...остальные колонки источника]

  - region_code     — ISO-код субъекта РФ
  - year            — год (int)
  - edu_level_code  — код уровня образования (None если неприменимо)
  - остальные колонки из источника, без системных ETL-колонок
"""

from __future__ import annotations

import os
import re
import sys
from collections import Counter

import pandas as pd
from dagster import MetadataValue, asset

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from transformations.normalization.ocr_fixes import fix_ocr, fix_ocr_preserve_case
from transformations.normalization.row_gate import is_aggregate, QUARANTINE_CODES
from transformations.normalization.region_mapper import build_lookup, map_series
from transformations.normalization.age_parser import parse_age
from transformations.bronze_normalized.region_normalizer import lookup_region

UNIMPLEMENTED = {"status": "unimplemented", "dagster/compute_kind": "Not_Implemented"}

# ── Вспомогательные функции ────────────────────────────────────────────────────

def _to_int(val) -> int | None:
    if val is None:
        return None
    s = str(val).strip().replace("\u00a0", "").replace(" ", "")
    if s in ("", "-", "nan", "None", "NULL", "none"):
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def _is_numbering_row(col_0: str | None, col_1: str | None) -> bool:
    """True если строка — нумератор колонок (A/1/2/3... или 1.0/2.0...)."""
    c0 = str(col_0 or "").strip()
    c1 = str(col_1 or "").strip()
    if c0 == "A" and c1 in ("1", "1.0"):
        return True
    try:
        if float(c0) == 1.0 and float(c1) == 2.0:
            return True
    except (ValueError, TypeError):
        pass
    return False


def _find_data_start(group_df: pd.DataFrame) -> int:
    """Индекс первой строки данных (следующая после нумератора)."""
    for _, row in group_df.sort_values("_row_number").iterrows():
        if _is_numbering_row(row.get("col_0"), row.get("col_1")):
            return int(row["_row_number"]) + 1
    return 0


# ── Конфиги уровня образования для Postgres-источников ───────────────────────
#
# signal_col    — колонка, из которой извлекается уровень образования
# match         — "exact" (точное совпадение) или "contains" (подстрока)
# mapping       — значение/ключевое слово → edu_level_code
# drop_unmatched— True: строки без уровня удаляются (не агрегаты данных)
#
# BUGFIXES_AUDIT §4.4: нормализуем en-dash → дефис и пробелы перед matching
_ETL_COLS = frozenset({"_etl_loaded_at", "_sheet_name", "_row_number"})

_AGGREGATE_ROW_NAME = "Всего"  # общий агрегат для vpo / spo / oo

# BUGFIXES_AUDIT: obuch_pk — колонки-агрегаты
_PK_SKIP_COLUMN_NAMES = frozenset({"Всего (сумма гр.4 -13)"})

# BUGFIXES_AUDIT: obuch_vpo — колонки-агрегаты, которые не должны попадать во 2-й слой
_VPO_SKIP_COLUMN_NAMES = frozenset({
    "Принято",
    "Выпуск",
    "из них женщины 6",
    "из них женщины 4",
    "из них женщины 8",
    "из них женщины 12",
    "из них женщины 18",
    "из них женщины 16",
    "из них женщины 10",
    "из них женщины 20",
    "из них женщины 14",
})

# BUGFIXES_AUDIT: obshagi_vpo — строки-агрегаты (AND по column_name + row_name)
_OBSHAGI_VPO_SKIP_COLUMN_NAMES = frozenset({
    "оборудо-ванная охранно-пожарной сигнали-зацией",
    "в опера-тивном управлении",
    "другие формы владения",
    "арендо-ванная",
    "на правах собствен-ности",
})
_OBSHAGI_VPO_SKIP_ROW_NAMES = frozenset({
    "из нее площадь по целям использования:      учебно-лабораторных зданий (сумма строк 03, 05, 06, 07)",
    "в том числе:         учебная",
    "подсобная",
    "учебно-вспомогательная",
    "из нее площадь пунктов общественного питания",
    "из нее площадь крытых спортивных сооружений",
    "предназначенная для научно-исследовательских подразделений",
    "Общая площадь зданий (помещений), всего (сумма строк 02, 09,12), кв м",
    "прочих зданий",
    "Общая площадь зданий (помещений) – всего (сумма строк 02, 09,12), кв м",
    "иностранных граждан и лиц без гражданства",
    "Число учебных мест в лабораториях",
    "из них проживает:        в помещениях с повышенными комфортными условиями",
    "из (строки 01):      иногородних граждан",
    "из (строки 02):      иногородних граждан",
    "в общежитиях, арендуемых у сторонних организаций",
    "из них проживает:         в помещениях с повышенными комфортными условиями",
    "Общая площадь земельных участков, всего, га",
    "опытных полей",
    "Число учебных (рабочих) мест в учебно-производственных помещениях (мастерских, полигонах, технодромах, учебных цехах и т.п.): всего",
    "Количество автоматизированных тренажерно-обучающих комплексов (систем)",
    "Общая площадь земельных участков – всего, га",
    "в том числе предоставлено организациями, с которыми      заключены договоры на подготовку кадров",
    "Число учебных (рабочих) мест в учебно-производственных помещениях   (мастерских, полигонах, технодромах, учебных цехах и т.п.):      всего",
    "из нее площадь по целям использования:      учебных полигонов",
    "Количество автоматизированных тренажерно-обучающих комплексов  (систем)",
    "в том числе предоставлено организациями, с которыми заключены        договоры на подготовку кадров",
    "из нее площадь по целям использования:     учебных полигонов",
})

# BUGFIXES_AUDIT: ped_kadry — строки-агрегаты по row_number, которые не должны попадать во 2-й слой
_PED_KADRY_SKIP_ROW_NUMBERS = frozenset({
    "1", "2", "3", "4", "5", "6", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18",
    "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48",
    "49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63",
    "64", "65", "66", "67", "68",
})

# BUGFIXES_AUDIT: obuch_spo — колонки-агрегаты, которые не должны попадать во 2-й слой
_SPO_SKIP_COLUMN_NAMES = frozenset({
    "Выпуск",
    "Принято",
    "из них женщины 14",
    "из них женщины 12",
    "из них женщины 16",
    "из них женщины 4",
    "из них женщины 10",
    "из них женщины 18",
    "из них женщины 28",
    "из них женщины 24",
    "из них женщины 26",
    "из них женщины 30",
})

_EDU_LEVEL_CFG: dict[str, dict] = {
    "obuch_vpo": {
        "signal_col": "column_metadata_1",
        "match": "exact",
        "mapping": {
            "Программы бакалавриата":  "2.6",
            "Программы специалитета":  "2.7",
            "Программы магистратуры":  "2.8",
        },
        "drop_unmatched": True,
    },
    "obuch_spo": {
        "signal_col": "column_metadata_2",
        "match": "exact",
        "mapping": {
            "Программы подготовки специалистов среднего звена":               "2.5.2",
            "Программы подготовки квалифицированных рабочих, служащих":       "2.5.1",
        },
        "drop_unmatched": True,
    },
    "obuch_oo": {
        "signal_col": "column_name",
        "match": "contains",
        "mapping": {
            "начального общего образования": "1.2",
            "основного общего образования":  "1.3",
            "среднего общего образования":   "1.4",
        },
        "drop_unmatched": True,
    },
    "obuch_pk": {
        "signal_col": "row_name",
        "match": "contains",
        "mapping": {
            "повышения квалификации":        "4.8b.1",
            "профессиональной переподготовки": "4.8b.2",
        },
        "drop_unmatched": True,
    },
    # obshagi_vpo, ped_kadry — уровень образования неприменим, строки не фильтруем
}


def _norm_signal(s: str) -> str:
    """Нормализует строку для matching: strip, схлопывает пробелы, en-dash → дефис."""
    return re.sub(r"\s+", " ", str(s).strip().replace("\u2013", "-").replace("\u2014", "-"))


def _resolve_edu_level(raw_val: str, cfg: dict) -> str | None:
    """Возвращает edu_level_code по значению сигнальной колонки и конфигу."""
    norm = _norm_signal(raw_val)
    if cfg["match"] == "exact":
        return cfg["mapping"].get(raw_val) or cfg["mapping"].get(norm)
    # contains: ищем первый ключ, который входит как подстрока (без учёта регистра)
    norm_lower = norm.lower()
    for keyword, code in cfg["mapping"].items():
        if keyword.lower() in norm_lower:
            return code
    return None


def _normalize_postgres_df(
    df: pd.DataFrame,
    context,
    label: str,
    region_col: str = "регион",
    year_col: str = "год",
) -> pd.DataFrame:
    """
    Пайплайн для Postgres-источников.

    1. region_code  — берём из колонки регион (уже ISO-коды), фильтруем агрегаты
    2. year         — парсим год как int
    3. edu_level_code — определяем по _EDU_LEVEL_CFG[label]; если конфига нет — None
    4. Колонки: [region_code, year, edu_level_code, ...остальные] без ETL-метаданных
    """
    if df.empty:
        return df

    result = df.copy()

    # 1. region_code
    result["region_code"] = result[region_col].apply(
        lambda x: str(x).strip() if x and str(x) not in ("nan", "None") else None
    )
    before = len(result)
    result = result[
        result["region_code"].notna() &
        ~result["region_code"].isin(QUARANTINE_CODES)
    ].copy()
    dropped = before - len(result)
    if dropped:
        context.log.info("[%s] Row Gate: удалено %d строк-агрегатов", label, dropped)

    # 2. year
    result["year"] = pd.to_numeric(result[year_col], errors="coerce")
    result = result[result["year"].notna()].copy()
    result["year"] = result["year"].astype(int)

    # 3. edu_level_code
    edu_cfg = _EDU_LEVEL_CFG.get(label)
    if edu_cfg:
        sig_col = edu_cfg["signal_col"]
        result["edu_level_code"] = result[sig_col].apply(
            lambda x: _resolve_edu_level(str(x), edu_cfg) if x and str(x) not in ("nan", "None") else None
        )
        if edu_cfg["drop_unmatched"]:
            before = len(result)
            result = result[result["edu_level_code"].notna()].copy()
            dropped = before - len(result)
            if dropped:
                context.log.info("[%s] edu_level filter: удалено %d строк без уровня", label, dropped)
    else:
        result["edu_level_code"] = None

    # 4. Порядок колонок: [region_code, year, edu_level_code, ...остальные]
    drop_cols = _ETL_COLS | {region_col, year_col}
    rest = [c for c in result.columns if c not in drop_cols | {"region_code", "year", "edu_level_code"}]
    result = result[["region_code", "year", "edu_level_code"] + rest].copy()

    return result


# ── Конфиги для Excel (Дошколка) ──────────────────────────────────────────────

# Маппинг col_index → age_group (без col_1 = "total", т.к. это сумма)
_DOSHK_COL_MAP_2018_2021: dict[int, str] = {
    # 1: "total" — пропускаем (агрегат по возрастам — BUGFIXES_AUDIT §5)
    2: "age_0", 3: "age_1", 4: "age_2",
    # 5: subtotal 0-2 — пропускаем
    6: "age_3", 7: "age_4", 8: "age_5", 9: "age_6", 10: "age_7plus",
    # 11: subtotal 3+ — пропускаем
}
_DOSHK_COL_MAP_2022_2024: dict[int, str] = {
    # 1: "total" — пропускаем
    2: "age_0", 3: "age_1", 4: "age_2",
    5: "age_3", 6: "age_4", 7: "age_5", 8: "age_6", 9: "age_7plus",
}
_DOSHK_SHEET_TERRITORY: dict[str, str] = {
    "6": "total", "T_0": "total",
    "1": "urban", "T_1": "urban",
    "2": "rural", "T_2": "rural",
}


# ── Конфиги для Excel (Население) ─────────────────────────────────────────────

_NAR_QUARANTINE_SHEETS = frozenset({"2.2.3", "2.6.3"})
_NAR_SHEET_PATTERN = re.compile(r"^(?:Таб\.)?2\.\d+\.\d+\.?$")

# Маппинг col_offset → колонка данных (offset от age_col)
# 2018-2020: age в col_1, данные в col_2..col_10 (total_both, total_male, total_female, urban_*, rural_*)
# 2021+:     age в col_0, данные в col_1..col_9
_NAR_DATA_COLS = [
    "total_both", "total_male", "total_female",
    "urban_both", "urban_male", "urban_female",
    "rural_both", "rural_male", "rural_female",
]
_AGE_80_PLUS = frozenset({"80 и старше", "80 и более", "80 лет и старше", "80+"})


def _parse_age_naselenie(raw: str) -> str | None:
    if not raw:
        return None
    s = raw.strip()
    if s in _AGE_80_PLUS:
        return "80+"
    if re.fullmatch(r"\d+", s):
        age = int(s)
        if 0 <= age <= 79:
            return s
    return None


def _normalize_sheet_name(sheet_name: str) -> str:
    s = str(sheet_name).strip()
    if s.startswith("Таб."):
        s = s[4:]
    return s.strip(".")


def _extract_region_from_header(sheet_df: pd.DataFrame, year: int) -> str | None:
    """Извлекает название региона из заголовочных строк листа."""
    if year <= 2020:
        target_row, col = 3, "col_1"
    else:
        target_row, col = 1, "col_0"
    rows = sheet_df[sheet_df["_row_number"] == target_row]
    if rows.empty:
        return None
    val = rows.iloc[0].get(col)
    if val and str(val).strip() not in ("", "None", "nan"):
        return str(val).strip()
    return None


# ── n_* ассеты ─────────────────────────────────────────────────────────────────


@asset(
    group_name="2_normalization",
    io_manager_key="silver_raw_io_manager",
    description="Нормализация: дошкольное образование (Excel)",
)
def n_obuch_doshkolka(
    context,
    obuch_doshkolka: pd.DataFrame,
) -> pd.DataFrame:
    lookup = build_lookup()
    records: list[dict] = []
    unmatched_all: list[str] = []

    for (source_file, sheet_name, year), group in obuch_doshkolka.groupby(
        ["_source_file", "_sheet_name", "_year"]
    ):
        territory = _DOSHK_SHEET_TERRITORY.get(str(sheet_name))
        if territory is None:
            continue  # пропускаем нерелевантные листы

        col_map = _DOSHK_COL_MAP_2018_2021 if int(year) <= 2021 else _DOSHK_COL_MAP_2022_2024
        data_start = _find_data_start(group)
        data_rows = group[group["_row_number"] >= data_start].sort_values("_row_number")

        for _, row in data_rows.iterrows():
            region_raw = str(row.get("col_0") or "").strip()
            if not region_raw or region_raw in ("nan", "None"):
                continue

            region_fixed = fix_ocr(region_raw)
            region_code = lookup_region(region_fixed, lookup)

            if region_code in QUARANTINE_CODES:
                continue
            if region_code is None:
                unmatched_all.append(region_raw)
                continue

            for col_idx, age_group in col_map.items():
                value = _to_int(row.get(f"col_{col_idx}"))
                records.append({
                    "region_code":     region_code,
                    "year":            int(year),
                    "edu_level_code":  "1.1",
                    "territory_type":  territory,
                    "age_group":       age_group,
                    "value":           value,
                    "_source_file":    str(source_file),
                })

    if unmatched_all:
        for name, cnt in Counter(unmatched_all).most_common(5):
            context.log.warning("[doshkolka] Нераспознанный регион (×%d): %r", cnt, name)

    df_out = pd.DataFrame(records)
    context.add_output_metadata({
        "total_rows":   MetadataValue.int(len(df_out)),
        "regions":      MetadataValue.int(df_out["region_code"].nunique() if not df_out.empty else 0),
        "years":        MetadataValue.text(
            str(sorted(df_out["year"].unique().tolist())) if not df_out.empty else "[]"
        ),
        "unmatched_cnt": MetadataValue.int(len(unmatched_all)),
    })
    return df_out


@asset(
    group_name="2_normalization",
    io_manager_key="silver_raw_io_manager",
    description="Нормализация: численность населения (Excel flat)",
)
def n_naselenie(
    context,
    naselenie: pd.DataFrame,
) -> pd.DataFrame:
    lookup = build_lookup()
    records: list[dict] = []
    unmatched_all: list[str] = []

    for (source_file, sheet_name, year), group in naselenie.groupby(
        ["_source_file", "_sheet_name", "_year"]
    ):
        sheet_norm = _normalize_sheet_name(str(sheet_name))

        if not _NAR_SHEET_PATTERN.match(str(sheet_name)):
            continue
        if sheet_norm in _NAR_QUARANTINE_SHEETS:
            context.log.info("[naselenie] Пропускаем карантинный лист %s", sheet_name)
            continue

        yr = int(year)
        region_raw = _extract_region_from_header(group, yr)
        if not region_raw:
            context.log.warning("[naselenie] Нет имени региона в заголовке листа %s/%d", sheet_name, yr)
            continue

        region_fixed = fix_ocr(region_raw)
        region_code = lookup_region(region_fixed, lookup)

        if region_code in QUARANTINE_CODES:
            continue
        if region_code is None:
            unmatched_all.append(region_raw)
            context.log.warning("[naselenie] Нераспознанный регион %r (лист %s)", region_raw, sheet_name)
            continue

        # Определяем позиции колонок по году
        age_col_offset = 1 if yr <= 2020 else 0
        data_col_start = age_col_offset + 1

        data_rows = group[group["_row_number"] >= (4 if yr <= 2020 else 2)].sort_values("_row_number")

        for _, row in data_rows.iterrows():
            age_raw = str(row.get(f"col_{age_col_offset}") or "").strip()
            age = _parse_age_naselenie(age_raw)
            if age is None:
                continue  # не возрастная строка (заголовок, итого, и т.д.)

            row_data: dict = {
                "region_code":    region_code,
                "year":           yr,
                "edu_level_code": None,
                "age":            age,
                "_source_file":   str(source_file),
            }
            for i, col_label in enumerate(_NAR_DATA_COLS):
                raw_val = row.get(f"col_{data_col_start + i}")
                row_data[col_label] = _to_int(raw_val)
            records.append(row_data)

    if unmatched_all:
        for name, cnt in Counter(unmatched_all).most_common(5):
            context.log.warning("[naselenie] Нераспознанный регион (×%d): %r", cnt, name)

    df_out = pd.DataFrame(records)
    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(df_out)),
        "regions":    MetadataValue.int(df_out["region_code"].nunique() if not df_out.empty else 0),
    })
    return df_out


@asset(
    group_name="2_normalization",
    io_manager_key="silver_raw_io_manager",
    description="Нормализация: обучающиеся ОО (Postgres, 9 таблиц)",
)
def n_obuch_oo(
    context,
    obuch_oo: pd.DataFrame,
) -> pd.DataFrame:
    if "row_name" in obuch_oo.columns:
        before = len(obuch_oo)
        obuch_oo = obuch_oo[obuch_oo["row_name"] != _AGGREGATE_ROW_NAME].copy()
        dropped = before - len(obuch_oo)
        if dropped:
            context.log.info("[obuch_oo] Row Gate row_name='Всего': удалено %d строк-агрегатов", dropped)

    df = _normalize_postgres_df(obuch_oo, context, "obuch_oo")
    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(df)),
        "regions":    MetadataValue.int(df["region_code"].nunique() if not df.empty else 0),
        "source_tables": MetadataValue.text(
            str(df["_source_file"].unique().tolist()) if not df.empty else "[]"
        ),
    })
    return df


@asset(
    group_name="2_normalization",
    io_manager_key="silver_raw_io_manager",
    description="Нормализация: обучающиеся ВПО (Postgres)",
)
def n_obuch_vpo(
    context,
    obuch_vpo: pd.DataFrame,
) -> pd.DataFrame:
    before = len(obuch_vpo)
    if "column_name" in obuch_vpo.columns:
        obuch_vpo = obuch_vpo[~obuch_vpo["column_name"].isin(_VPO_SKIP_COLUMN_NAMES)].copy()
    if "row_name" in obuch_vpo.columns:
        obuch_vpo = obuch_vpo[obuch_vpo["row_name"] != _AGGREGATE_ROW_NAME].copy()
    dropped = before - len(obuch_vpo)
    if dropped:
        context.log.info("[obuch_vpo] Row Gate: удалено %d строк-агрегатов", dropped)

    df = _normalize_postgres_df(obuch_vpo, context, "obuch_vpo")
    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(df)),
        "regions":    MetadataValue.int(df["region_code"].nunique() if not df.empty else 0),
    })
    return df


@asset(
    group_name="2_normalization",
    io_manager_key="silver_raw_io_manager",
    description="Нормализация: обучающиеся СПО (Postgres)",
)
def n_obuch_spo(
    context,
    obuch_spo: pd.DataFrame,
) -> pd.DataFrame:
    before = len(obuch_spo)
    if "column_name" in obuch_spo.columns:
        obuch_spo = obuch_spo[~obuch_spo["column_name"].isin(_SPO_SKIP_COLUMN_NAMES)].copy()
    if "row_name" in obuch_spo.columns:
        obuch_spo = obuch_spo[obuch_spo["row_name"] != _AGGREGATE_ROW_NAME].copy()
    dropped = before - len(obuch_spo)
    if dropped:
        context.log.info("[obuch_spo] Row Gate: удалено %d строк-агрегатов", dropped)

    df = _normalize_postgres_df(obuch_spo, context, "obuch_spo")
    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(df)),
        "regions":    MetadataValue.int(df["region_code"].nunique() if not df.empty else 0),
    })
    return df


@asset(
    group_name="2_normalization",
    io_manager_key="silver_raw_io_manager",
    description="Нормализация: повышение квалификации ПК (Postgres)",
)
def n_obuch_pk(
    context,
    obuch_pk: pd.DataFrame,
) -> pd.DataFrame:
    if "column_name" in obuch_pk.columns:
        before = len(obuch_pk)
        obuch_pk = obuch_pk[~obuch_pk["column_name"].isin(_PK_SKIP_COLUMN_NAMES)].copy()
        dropped = before - len(obuch_pk)
        if dropped:
            context.log.info("[obuch_pk] Row Gate column_name: удалено %d строк-агрегатов", dropped)

    df = _normalize_postgres_df(obuch_pk, context, "obuch_pk")
    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(df)),
        "regions":    MetadataValue.int(df["region_code"].nunique() if not df.empty else 0),
    })
    return df


@asset(
    group_name="2_normalization",
    io_manager_key="silver_raw_io_manager",
    description="Нормализация: общежития ВПО (Postgres, 2 таблицы)",
)
def n_obshagi_vpo(
    context,
    obshagi_vpo: pd.DataFrame,
) -> pd.DataFrame:
    if "column_name" in obshagi_vpo.columns and "row_name" in obshagi_vpo.columns:
        before = len(obshagi_vpo)
        mask = (
            obshagi_vpo["column_name"].isin(_OBSHAGI_VPO_SKIP_COLUMN_NAMES) |
            obshagi_vpo["row_name"].isin(_OBSHAGI_VPO_SKIP_ROW_NAMES)
        )
        obshagi_vpo = obshagi_vpo[~mask].copy()
        dropped = before - len(obshagi_vpo)
        if dropped:
            context.log.info("[obshagi_vpo] Row Gate: удалено %d строк-агрегатов", dropped)

    df = _normalize_postgres_df(obshagi_vpo, context, "obshagi_vpo")
    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(df)),
        "regions":    MetadataValue.int(df["region_code"].nunique() if not df.empty else 0),
        "source_tables": MetadataValue.text(
            str(df["_source_file"].unique().tolist()) if not df.empty else "[]"
        ),
    })
    return df


# ── Вспомогательные функции для нормализации возраста ─────────────────────────

_DOSHK_AGE_GROUP_MAP: dict[str, int] = {
    "age_0": 0, "age_1": 1, "age_2": 2, "age_3": 3,
    "age_4": 4, "age_5": 5, "age_6": 6, "age_7plus": 7,
}

_AGE_SKIP_CATEGORIES = frozenset({"total", "unknown"})

# Веса для open_end ("X лет и старше"): 7 строк вперёд, убывающее распределение
_OPEN_END_WEIGHTS = [0.30, 0.20, 0.10, 0.10, 0.10, 0.10, 0.10]

# Колонки, которые НЕ нужно распределять (метаданные и ключи)
_NON_VALUE_COLS = frozenset({
    "region_code", "year", "edu_level_code", "age", "age_category",
    "row_name", "_source_file", "_etl_loaded_at",
    "row_number", "column_number", "column_name", "column_metadata_1", "column_metadata_2",
})


def _parse_float(val) -> float | None:
    """Безопасный парсинг числа из строки."""
    if val is None:
        return None
    s = str(val).strip().replace("\u00a0", "").replace(" ", "")
    if s in ("", "-", "nan", "None", "NULL"):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _detect_value_cols(columns: list[str]) -> list[str]:
    """Определяет колонки-значения (всё, что не в _NON_VALUE_COLS и не тег)."""
    result = []
    for c in columns:
        if c in _NON_VALUE_COLS:
            continue
        # теги (тег_1, тег_2, ...) — категориальные, не распределяем
        if re.match(r"^тег_\d+$", c):
            continue
        result.append(c)
    return result


def _expand_age_row(row: dict, age_min: int | None, age_max: int | None,
                    category: str, value_cols: list[str]) -> list[dict]:
    """
    Раскрывает одну строку по правилам распределения возрастных диапазонов.

    exact      → 1 строка, value без изменений
    open_start → 1 строка (age = age_max), value без изменений
    range      → (age_max − age_min + 1) строк, value делится поровну
    open_end   → 7 строк от age_min, value по весам _OPEN_END_WEIGHTS
    total/unknown → [] (строка пропускается)
    """
    if category in _AGE_SKIP_CATEGORIES:
        return []

    base = {k: v for k, v in row.items() if k != "row_name"}

    orig_values = {c: _parse_float(row.get(c)) for c in value_cols}

    if category == "exact":
        r = {**base, "age": age_min, "age_category": category}
        for c in value_cols:
            r[c] = orig_values[c]
        return [r]

    if category == "open_start":
        r = {**base, "age": age_max, "age_category": category}
        for c in value_cols:
            r[c] = orig_values[c]
        return [r]

    if category == "range" and age_min is not None and age_max is not None:
        ages = list(range(age_min, age_max + 1))
        n = len(ages)
        rows = []
        for age in ages:
            r = {**base, "age": age, "age_category": category}
            for c in value_cols:
                v = orig_values[c]
                r[c] = round(v / n, 4) if v is not None else None
            rows.append(r)
        return rows

    if category == "open_end" and age_min is not None:
        rows = []
        for i, weight in enumerate(_OPEN_END_WEIGHTS):
            r = {**base, "age": age_min + i, "age_category": category}
            for c in value_cols:
                v = orig_values[c]
                r[c] = round(v * weight, 4) if v is not None else None
            rows.append(r)
        return rows

    return []


def _expand_age_from_col(df: pd.DataFrame, context, label: str, src_col: str = "row_name") -> pd.DataFrame:
    """
    Универсальная функция: раскрывает колонку src_col → age (int) + age_category.
    Применяет распределение значений (range / open_end).
    src_col удаляется из выхода; 'age' и 'age_category' добавляются.
    """
    if df.empty or src_col not in df.columns:
        context.log.warning("[%s_age] Колонка '%s' отсутствует, возвращаем пустой DF", label, src_col)
        return pd.DataFrame(columns=list(df.columns) + ["age", "age_category"])

    value_cols = _detect_value_cols([c for c in df.columns if c != src_col])
    records: list[dict] = []
    skipped = 0

    for _, row in df.iterrows():
        raw = str(row.get(src_col) or "").strip()
        if raw in ("nan", "None"):
            raw = ""
        age_min, age_max, category = parse_age(raw)
        row_dict = {k: v for k, v in row.items() if k != src_col}
        expanded = _expand_age_row(row_dict, age_min, age_max, category, value_cols)
        if not expanded:
            skipped += 1
        records.extend(expanded)

    if skipped:
        context.log.info("[%s_age] Пропущено строк-агрегатов (total/unknown): %d", label, skipped)

    if not records:
        return pd.DataFrame(columns=[c for c in df.columns if c != src_col] + ["age", "age_category"])

    result = pd.DataFrame(records)

    priority = ["region_code", "year", "edu_level_code", "age", "age_category"]
    rest = [c for c in result.columns if c not in priority]
    return result[priority + rest].copy()


@asset(
    group_name="2_normalization_age",
    io_manager_key="silver_raw_age_io_manager",
    description="Нормализация возраста: обучающиеся ОО — row_name → age (int)",
)
def n_obuch_oo_age(context, n_obuch_oo: pd.DataFrame) -> pd.DataFrame:
    df = _expand_age_from_col(n_obuch_oo, context, "obuch_oo")
    context.add_output_metadata({
        "total_rows":    MetadataValue.int(len(df)),
        "regions":       MetadataValue.int(df["region_code"].nunique() if not df.empty else 0),
        "age_categories": MetadataValue.text(
            str(sorted(df["age_category"].unique().tolist())) if not df.empty else "[]"
        ),
        "age_range": MetadataValue.text(
            f"{df['age'].min()}–{df['age'].max()}" if not df.empty else "—"
        ),
    })
    return df


@asset(
    group_name="2_normalization_age",
    io_manager_key="silver_raw_age_io_manager",
    description="Нормализация возраста: обучающиеся ВПО — row_name → age (int)",
)
def n_obuch_vpo_age(context, n_obuch_vpo: pd.DataFrame) -> pd.DataFrame:
    df = _expand_age_from_col(n_obuch_vpo, context, "obuch_vpo")
    context.add_output_metadata({
        "total_rows":    MetadataValue.int(len(df)),
        "regions":       MetadataValue.int(df["region_code"].nunique() if not df.empty else 0),
        "age_categories": MetadataValue.text(
            str(sorted(df["age_category"].unique().tolist())) if not df.empty else "[]"
        ),
        "age_range": MetadataValue.text(
            f"{df['age'].min()}–{df['age'].max()}" if not df.empty else "—"
        ),
    })
    return df


@asset(
    group_name="2_normalization_age",
    io_manager_key="silver_raw_age_io_manager",
    description="Нормализация возраста: обучающиеся СПО — row_name → age (int)",
)
def n_obuch_spo_age(context, n_obuch_spo: pd.DataFrame) -> pd.DataFrame:
    df = _expand_age_from_col(n_obuch_spo, context, "obuch_spo")
    context.add_output_metadata({
        "total_rows":    MetadataValue.int(len(df)),
        "regions":       MetadataValue.int(df["region_code"].nunique() if not df.empty else 0),
        "age_categories": MetadataValue.text(
            str(sorted(df["age_category"].unique().tolist())) if not df.empty else "[]"
        ),
        "age_range": MetadataValue.text(
            f"{df['age'].min()}–{df['age'].max()}" if not df.empty else "—"
        ),
    })
    return df


@asset(
    group_name="2_normalization_age",
    io_manager_key="silver_raw_age_io_manager",
    description="Нормализация возраста: дошкольное образование — age_group → age (int)",
)
def n_obuch_doshkolka_age(context, n_obuch_doshkolka: pd.DataFrame) -> pd.DataFrame:
    if n_obuch_doshkolka.empty or "age_group" not in n_obuch_doshkolka.columns:
        context.log.warning("[doshkolka_age] Нет колонки age_group, возвращаем пустой DF")
        return pd.DataFrame(columns=["region_code", "year", "edu_level_code", "territory_type", "age", "value"])

    df = n_obuch_doshkolka.copy()
    before = len(df)
    df["age"] = df["age_group"].map(_DOSHK_AGE_GROUP_MAP)
    df = df[df["age"].notna()].copy()
    df["age"] = df["age"].astype(int)
    dropped = before - len(df)
    if dropped:
        context.log.warning("[doshkolka_age] Нераспознанных age_group: %d строк", dropped)

    df = df.drop(columns=["age_group"], errors="ignore")

    priority = ["region_code", "year", "edu_level_code", "territory_type", "age"]
    rest = [c for c in df.columns if c not in priority]
    df = df[priority + rest].copy()

    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(df)),
        "regions":    MetadataValue.int(df["region_code"].nunique() if not df.empty else 0),
        "age_range":  MetadataValue.text(
            f"{df['age'].min()}–{df['age'].max()}" if not df.empty else "—"
        ),
    })
    return df


@asset(
    group_name="2_normalization_age",
    io_manager_key="silver_raw_age_io_manager",
    description="Нормализация возраста: ПК/ДПО — column_name → age (int)",
)
def n_obuch_pk_age(context, n_obuch_pk: pd.DataFrame) -> pd.DataFrame:
    # Возраст в ПК хранится в column_name ("моложе 25", "25-29", "65 и более" и т.д.)
    df = _expand_age_from_col(n_obuch_pk, context, "obuch_pk", src_col="column_name")
    context.add_output_metadata({
        "total_rows":     MetadataValue.int(len(df)),
        "regions":        MetadataValue.int(df["region_code"].nunique() if not df.empty else 0),
        "age_categories": MetadataValue.text(
            str(sorted(df["age_category"].unique().tolist())) if not df.empty else "[]"
        ),
        "age_range": MetadataValue.text(
            f"{df['age'].min()}–{df['age'].max()}" if not df.empty else "—"
        ),
    })
    return df


@asset(
    group_name="2_normalization_age",
    io_manager_key="silver_raw_age_io_manager",
    description="Нормализация возраста: население — age строка → age (int), 80+ раскрывается в 7 строк",
)
def n_naselenie_age(context, n_naselenie: pd.DataFrame) -> pd.DataFrame:
    # Возраст в населении уже в колонке 'age' как строки: "5", "80+"
    df = _expand_age_from_col(n_naselenie, context, "naselenie", src_col="age")
    context.add_output_metadata({
        "total_rows":     MetadataValue.int(len(df)),
        "regions":        MetadataValue.int(df["region_code"].nunique() if not df.empty else 0),
        "age_categories": MetadataValue.text(
            str(sorted(df["age_category"].unique().tolist())) if not df.empty else "[]"
        ),
        "age_range": MetadataValue.text(
            f"{df['age'].min()}–{df['age'].max()}" if not df.empty else "—"
        ),
    })
    return df


@asset(
    group_name="2_normalization",
    io_manager_key="silver_raw_io_manager",
    tags=UNIMPLEMENTED,
    description="Нормализация: общежития СПО — заглушка (нет данных)",
)
def n_obshagi_spo(context) -> pd.DataFrame:
    context.log.warning("n_obshagi_spo: источник не реализован, возвращаем пустой DataFrame")
    return pd.DataFrame(columns=["region_code", "region_name_raw", "year"])


@asset(
    group_name="2_normalization",
    io_manager_key="silver_raw_io_manager",
    description="Нормализация: педагогические кадры ОО (Postgres, 3 таблицы)",
)
def n_ped_kadry(
    context,
    ped_oo: pd.DataFrame,
) -> pd.DataFrame:
    if "row_number" in ped_oo.columns:
        before = len(ped_oo)
        ped_oo = ped_oo[~ped_oo["row_number"].astype(str).isin(_PED_KADRY_SKIP_ROW_NUMBERS)].copy()
        dropped = before - len(ped_oo)
        if dropped:
            context.log.info("[ped_kadry] Row Gate row_number: удалено %d строк-агрегатов", dropped)

    df = _normalize_postgres_df(ped_oo, context, "ped_kadry")
    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(df)),
        "regions":    MetadataValue.int(df["region_code"].nunique() if not df.empty else 0),
        "source_tables": MetadataValue.text(
            str(df["_source_file"].unique().tolist()) if not df.empty else "[]"
        ),
    })
    return df
