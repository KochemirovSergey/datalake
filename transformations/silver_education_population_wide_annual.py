"""
Bronze/Silver → Silver: Годовая детализация таблицы silver.education_population_wide

Что делает:
  - Читает silver.education_population_wide
  - Исключает общие агрегаты (age == 'всего')
  - Разворачивает возрастные диапазоны в отдельные годы с учётом
    ограничений min_age / max_age на уровне каждой метрики:
      * ДПО (level_4_8b_*): min=18, max=70
      * ВПО (level_2_6/7/8): min=0, max=70
      * остальные: min=0, max=80
  - Для закрытых диапазонов — равномерное распределение (1/N)
  - Для открытых верхних (X+) — убывающее линейное
  - Для открытых нижних (<X, моложе X) — убывающее линейное (0 → max)
  - Записывает результат в silver.education_population_wide_annual
"""

import logging
import os
import re
import pyarrow as pa
import pandas as pd
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

METRICS = [
    "population_total",
    "level_1_1",
    "level_1_2",
    "level_1_3",
    "level_1_4",
    "level_2_5_1",
    "level_2_5_2",
    "level_2_6",
    "level_2_7",
    "level_2_8",
    "level_4_8b_1",
    "level_4_8b_2",
]

# Колонки уровней образования (без population_total)
EDUCATION_LEVELS = [
    "level_1_1", "level_1_2", "level_1_3", "level_1_4",
    "level_2_5_1", "level_2_5_2",
    "level_2_6", "level_2_7", "level_2_8",
    "level_4_8b_1", "level_4_8b_2",
]

# Ограничения возраста на уровне метрики: (min_age включительно, max_age включительно)
METRIC_BOUNDS: dict[str, tuple[int, int]] = {
    "level_2_5_1":  (0,  70),   # СПО: программа 1
    "level_2_5_2":  (0,  70),   # СПО: программа 2
    "level_2_6":    (0,  70),   # ВПО: бакалавриат
    "level_2_7":    (0,  70),   # ВПО: специалитет
    "level_2_8":    (0,  70),   # ВПО: магистратура
    "level_4_8b_1": (18, 70),   # ДПО (1-ПК): программа 1
    "level_4_8b_2": (18, 70),   # ДПО (1-ПК): программа 2
}
DEFAULT_BOUNDS = (0, 80)


def _clamp(ages_weights: list[tuple[int, float]], min_age: int, max_age: int) -> list[tuple[int, float]]:
    """Фильтрует список (возраст, вес) по границам min/max и перенормирует веса."""
    filtered = [(a, w) for a, w in ages_weights if min_age <= a <= max_age]
    if not filtered:
        return []
    total = sum(w for _, w in filtered)
    if total == 0:
        return []
    return [(a, w / total) for a, w in filtered]


def parse_and_distribute_age(age_str: str, max_age: int = 80, min_age: int = 0) -> list[tuple[int, float]]:
    """
    Возвращает список кортежей (year: int, weight: float) для заданных границ.
    Веса нормированы (сумма = 1.0).
    """
    age_str = str(age_str).strip().lower()

    # Просто число
    if age_str.isdigit():
        age = int(age_str)
        if min_age <= age <= max_age:
            return [(age, 1.0)]
        return []

    # '<X' или 'моложе X'
    match_lt = re.match(r'^(?:<|моложе\s*)(\d+)$', age_str)
    if match_lt:
        upper_bound = int(match_lt.group(1))  # не включительно
        N = upper_bound  # возраста 0..upper_bound-1
        if N <= 0:
            return _clamp([(0, 1.0)], min_age, max_age)
        total_weight = N * (N + 1) / 2.0
        # убывающее от старшего к младшему: age=X-1 получает максимальный вес
        # вес[i] = (i+1) / total → age=0 минимум, age=N-1 максимум
        raw = [(i, (i + 1) / total_weight) for i in range(N)]
        return _clamp(raw, min_age, max_age)

    # 'X-Y' (закрытый диапазон)
    match_range = re.match(r'^(\d+)-(\d+)$', age_str)
    if match_range:
        start_age = int(match_range.group(1))
        end_age = int(match_range.group(2))
        if start_age > end_age:
            start_age, end_age = end_age, start_age
        N = end_age - start_age + 1
        raw = [(y, 1.0 / N) for y in range(start_age, end_age + 1)]
        return _clamp(raw, min_age, max_age)

    # 'X+', 'X лет+' (открытый верхний диапазон)
    match_plus = re.match(r'^(\d+)(?:\s*лет)?\+$', age_str)
    if match_plus:
        start_age = int(match_plus.group(1))
        if start_age >= max_age:
            return _clamp([(max_age, 1.0)], min_age, max_age)
        N = max_age - start_age + 1
        total_weight = N * (N + 1) / 2.0
        # убывающее: start_age получает максимальный вес
        raw = [(start_age + i, (N - i) / total_weight) for i in range(N)]
        return _clamp(raw, min_age, max_age)

    return []


def transform(cat: SqlCatalog) -> list[dict]:
    try:
        df = cat.load_table("silver.education_population_wide").scan().to_pandas()
    except Exception as e:
        log.warning("Could not load silver.education_population_wide: %s", e)
        return []

    if df.empty:
        return []

    # Исключаем 'всего'
    df = df[df['age'] != 'всего'].copy()

    # Обрабатываем каждую метрику независимо → long-формат
    long_rows = []
    for _, row in df.iterrows():
        age_str = str(row['age'])
        for metric in METRICS:
            val = row.get(metric)
            if pd.isna(val) or val is None:
                continue

            min_age, max_age = METRIC_BOUNDS.get(metric, DEFAULT_BOUNDS)
            distribution = parse_and_distribute_age(age_str, max_age=max_age, min_age=min_age)

            if not distribution:
                continue

            for target_age, weight in distribution:
                long_rows.append({
                    'region_code':     row['region_code'],
                    'region_name_raw': row['region_name_raw'],
                    'year':            row['year'],
                    'age':             str(target_age),
                    'metric':          metric,
                    'value':           int(round(float(val) * weight)),
                })

    if not long_rows:
        return []

    long_df = pd.DataFrame(long_rows)

    # Суммируем на случай дублей
    agg = long_df.groupby(
        ['region_code', 'region_name_raw', 'year', 'age', 'metric'],
        as_index=False
    )['value'].sum()

    # Pivot → wide
    wide = agg.pivot(
        index=['region_code', 'region_name_raw', 'year', 'age'],
        columns='metric',
        values='value',
    ).reset_index()
    wide.columns.name = None

    # Гарантируем наличие всех колонок метрик
    for m in METRICS:
        if m not in wide.columns:
            wide[m] = None

    # Сортировка
    wide['age_int'] = wide['age'].astype(int)
    wide = wide.sort_values(['region_code', 'year', 'age_int']).drop(columns=['age_int'])

    records = wide.to_dict('records')
    for r in records:
        for m in METRICS:
            v = r.get(m)
            if v is None or (isinstance(v, float) and pd.isna(v)):
                r[m] = None
            else:
                r[m] = int(v)

        # Сумма всех уровней образования
        level_vals = [r[m] for m in EDUCATION_LEVELS if r.get(m) is not None]
        r['education_total'] = sum(level_vals) if level_vals else None

        # Доля от населения
        pop = r.get('population_total')
        edu = r.get('education_total')
        if pop and pop > 0 and edu is not None:
            r['education_share'] = round(edu / pop, 6)
        else:
            r['education_share'] = None

    return records


def run() -> int:
    cat = SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )

    try:
        tbl = cat.load_table("silver.education_population_wide_annual")
    except Exception as e:
        log.error("Table silver.education_population_wide_annual not found: %s", e)
        return 0

    existing = tbl.scan().to_arrow()
    if len(existing) > 0:
        log.info("Deleting existing records in silver.education_population_wide_annual...")
        tbl.delete("region_code is not null")

    # Schema evolution: добавляем новые колонки, если ещё нет
    existing_fields = {f.name for f in tbl.schema().fields}
    updater = tbl.update_schema()
    changed = False
    if 'education_total' not in existing_fields:
        from pyiceberg.types import LongType
        updater.add_column('education_total', LongType())
        changed = True
    if 'education_share' not in existing_fields:
        from pyiceberg.types import DoubleType
        updater.add_column('education_share', DoubleType())
        changed = True
    if changed:
        updater.commit()
        log.info("Schema evolution: добавлены колонки education_total, education_share")

    records = transform(cat)
    if not records:
        log.warning("Нет данных для записи")
        return 0

    pa_schema = pa.schema([
        pa.field("region_code",      pa.string(),  nullable=False),
        pa.field("region_name_raw",  pa.string(),  nullable=False),
        pa.field("year",             pa.int32(),   nullable=False),
        pa.field("age",              pa.string(),  nullable=False),
        pa.field("population_total", pa.int64(),   nullable=True),
        pa.field("level_1_1",        pa.int64(),   nullable=True),
        pa.field("level_1_2",        pa.int64(),   nullable=True),
        pa.field("level_1_3",        pa.int64(),   nullable=True),
        pa.field("level_1_4",        pa.int64(),   nullable=True),
        pa.field("level_2_5_1",      pa.int64(),   nullable=True),
        pa.field("level_2_5_2",      pa.int64(),   nullable=True),
        pa.field("level_2_6",        pa.int64(),   nullable=True),
        pa.field("level_2_7",        pa.int64(),   nullable=True),
        pa.field("level_2_8",        pa.int64(),   nullable=True),
        pa.field("level_4_8b_1",     pa.int64(),   nullable=True),
        pa.field("level_4_8b_2",     pa.int64(),   nullable=True),
        pa.field("education_total",  pa.int64(),   nullable=True),
        pa.field("education_share",  pa.float64(), nullable=True),
    ])

    arrays = {
        "region_code":     pa.array([r["region_code"]     for r in records], pa.string()),
        "region_name_raw": pa.array([r["region_name_raw"] for r in records], pa.string()),
        "year":            pa.array([r["year"]            for r in records], pa.int32()),
        "age":             pa.array([r["age"]             for r in records], pa.string()),
    }
    for m in METRICS:
        arrays[m] = pa.array([r[m] for r in records], pa.int64())
    arrays["education_total"] = pa.array([r["education_total"] for r in records], pa.int64())
    arrays["education_share"] = pa.array([r["education_share"] for r in records], pa.float64())

    tbl.append(pa.table(arrays, schema=pa_schema))
    log.info("Записано %d строк в silver.education_population_wide_annual", len(records))
    return len(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()
    if count > 0:
        print(f"\nГотово: {count} строк")
