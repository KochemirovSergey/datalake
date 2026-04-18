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
    if "column_name" in obuch_vpo.columns:
        before = len(obuch_vpo)
        obuch_vpo = obuch_vpo[~obuch_vpo["column_name"].isin(_VPO_SKIP_COLUMN_NAMES)].copy()
        dropped = before - len(obuch_vpo)
        if dropped:
            context.log.info("[obuch_vpo] Row Gate column_name: удалено %d строк-агрегатов", dropped)

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
    # Row Gate для SPO: исключаем агрегатные колонки до нормализации
    if "column_name" in obuch_spo.columns:
        before = len(obuch_spo)
        obuch_spo = obuch_spo[~obuch_spo["column_name"].isin(_SPO_SKIP_COLUMN_NAMES)].copy()
        dropped = before - len(obuch_spo)
        if dropped:
            context.log.info("[obuch_spo] Row Gate column_name: удалено %d строк-агрегатов", dropped)

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
