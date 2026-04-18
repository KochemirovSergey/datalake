import pandas as pd
from dagster import MetadataValue, asset

# Общие колонки-метаданные, не имеющие смысла после группировки
_META_COLS = frozenset({
    "age_category",
    "_source_file",
    "_etl_loaded_at",
    "column_metadata_1",
    "column_metadata_2",
    "column_number",
    "row_number",
})

# Ключи для таблиц с возрастом (Слой 2.1 → Слой 3)
_KEY_AGE    = ["region_code", "year", "edu_level_code", "age"]
# column_name — метаданные для age-таблиц (там это имя исходной колонки Postgres)
_DROP_AGE   = _META_COLS | {"column_name"}

# Ключи для obshagi_vpo: column_name = тип метрики (структурная, не метаданные)
_KEY_OBSHAGI = ["region_code", "year", "edu_level_code", "column_name", "row_name"]
_DROP_OBSHAGI = _META_COLS

# Ключи для ped_kadry: row_name сохраняем (там тип педагога), edu_level_code уже проставлен
_KEY_PED    = ["region_code", "year", "edu_level_code", "row_name"]
_DROP_PED   = _META_COLS


def _aggregate_table(
    df: pd.DataFrame,
    key_cols: list[str],
    drop_cols: frozenset,
    label: str,
    context,
) -> pd.DataFrame:
    """
    Универсальная агрегация: drop мусора → groupby(key_cols) → SUM → fillna(0).
    Исторический баг: NULL в числовых фактах роняет расчёт дефицита — fillna(0) обязателен.
    """
    if df.empty:
        context.log.warning("[%s] Входной DataFrame пуст, возвращаем пустой результат", label)
        return pd.DataFrame(columns=key_cols)

    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    missing_keys = [k for k in key_cols if k not in df.columns]
    if missing_keys:
        context.log.error("[%s] Отсутствуют ключевые колонки: %s", label, missing_keys)
        return df

    value_cols = [
        c for c in df.columns
        if c not in key_cols and pd.api.types.is_numeric_dtype(df[c])
    ]

    if not value_cols:
        context.log.warning("[%s] Числовые колонки-значения не найдены", label)
        return df[key_cols].drop_duplicates()

    rows_before = len(df)
    result = df.groupby(key_cols, as_index=False)[value_cols].sum()
    result[value_cols] = result[value_cols].fillna(0)

    context.log.info(
        "[%s] %d строк → %d строк после groupby (схлопнуто %d)",
        label, rows_before, len(result), rows_before - len(result),
    )
    return result


@asset(
    group_name="3_aggregation",
    io_manager_key="silver_agg_io_manager",
    description="Агрегация: обучающиеся ОО по [region_code, year, edu_level_code, age]",
)
def a_obuch_oo(context, n_obuch_oo_age: pd.DataFrame) -> pd.DataFrame:
    result = _aggregate_table(n_obuch_oo_age, _KEY_AGE, _DROP_AGE, "a_obuch_oo", context)
    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(result)),
        "regions":    MetadataValue.int(result["region_code"].nunique() if not result.empty else 0),
        "age_range":  MetadataValue.text(
            f"{int(result['age'].min())}–{int(result['age'].max())}" if not result.empty else "—"
        ),
    })
    return result


@asset(
    group_name="3_aggregation",
    io_manager_key="silver_agg_io_manager",
    description="Агрегация: обучающиеся ВПО по [region_code, year, edu_level_code, age]",
)
def a_obuch_vpo(context, n_obuch_vpo_age: pd.DataFrame) -> pd.DataFrame:
    result = _aggregate_table(n_obuch_vpo_age, _KEY_AGE, _DROP_AGE, "a_obuch_vpo", context)
    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(result)),
        "regions":    MetadataValue.int(result["region_code"].nunique() if not result.empty else 0),
        "age_range":  MetadataValue.text(
            f"{int(result['age'].min())}–{int(result['age'].max())}" if not result.empty else "—"
        ),
    })
    return result


@asset(
    group_name="3_aggregation",
    io_manager_key="silver_agg_io_manager",
    description="Агрегация: обучающиеся СПО по [region_code, year, edu_level_code, age]",
)
def a_obuch_spo(context, n_obuch_spo_age: pd.DataFrame) -> pd.DataFrame:
    result = _aggregate_table(n_obuch_spo_age, _KEY_AGE, _DROP_AGE, "a_obuch_spo", context)
    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(result)),
        "regions":    MetadataValue.int(result["region_code"].nunique() if not result.empty else 0),
        "age_range":  MetadataValue.text(
            f"{int(result['age'].min())}–{int(result['age'].max())}" if not result.empty else "—"
        ),
    })
    return result


@asset(
    group_name="3_aggregation",
    io_manager_key="silver_agg_io_manager",
    description="Агрегация: общежития ВПО по [region_code, year, edu_level_code, column_name, row_name]",
)
def a_obshagi_vpo(context, n_obshagi_vpo: pd.DataFrame) -> pd.DataFrame:
    result = _aggregate_table(n_obshagi_vpo, _KEY_OBSHAGI, _DROP_OBSHAGI, "a_obshagi_vpo", context)
    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(result)),
        "regions":    MetadataValue.int(result["region_code"].nunique() if not result.empty else 0),
    })
    return result


@asset(
    group_name="3_aggregation",
    io_manager_key="silver_agg_io_manager",
    description="Агрегация: педагогические кадры ОО по [region_code, year, edu_level_code, row_name]",
)
def a_ped_kadry(context, n_ped_kadry: pd.DataFrame) -> pd.DataFrame:
    result = _aggregate_table(n_ped_kadry, _KEY_PED, _DROP_PED, "a_ped_kadry", context)
    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(result)),
        "regions":    MetadataValue.int(result["region_code"].nunique() if not result.empty else 0),
    })
    return result
