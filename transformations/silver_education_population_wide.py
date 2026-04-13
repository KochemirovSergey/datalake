"""
Bronze/Silver → Silver: Сборная таблица silver.education_population_wide

Что делает:
  - Читает silver.doshkolka, silver.naselenie, silver.oo, silver.spo, silver.vpo, silver.dpo
  - Приводит к единому формату long: region_code, region_name_raw, year, age, metric_code, value
  - Нормализует поле age
  - Выполняет pivot по metric_code
  - Сортирует и возвращает записи для записи в Iceberg
"""

import logging
import os
import pyarrow as pa
import pandas as pd
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")

# Порядок метрик для итоговой таблицы
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
    "level_4_8b_2"
]

def normalize_age(age: str) -> str:
    if pd.isna(age):
        return "unknown"
        
    a = str(age).strip().lower()
    
    if a in ("всего", "total", "nan", "none", "null"):
        return "всего"
    if a == "моложе 25":
        return "<25"
    if a == "65 и более":
        return "65+"
    if a == "age_7plus":
        return "7+"
    if a.startswith("age_"):
        a = a.replace("age_", "")

    if a.startswith("в возрасте моложе "):
        return "<" + a.replace("в возрасте моложе ", "").replace(" лет", "")
    if a.endswith(" и старше"):
        a = a.replace(" и старше", "+")

    # Clean russian words
    for word in (" лет и старше", " лет", " года", " год"):
        if a.endswith(word):
            a = a.replace(word, "")

    # Normalize weird dashes
    a = a.replace("–", "-").strip()
    
    return a

def sort_key(row):
    # Кастомная функция для сортировки age ("0" < "1" < ... < "7+" ... "<25" .. "65+" ... "всего")
    age = row['age']
    
    if age == "всего":
        age_num = 999
    elif age == "<25":
        age_num = -1  # хотя лучше как-то иначе, но пусть будет перед 25
    elif age == "65+":
        age_num = 65.5
    elif age.endswith("+"):
        try:
            age_num = float(age[:-1]) + 0.5
        except:
            age_num = 100
    else:
        try:
            age_num = float(age)
        except:
            age_num = 1000  # нераспознанный наверх
            
    return (row['region_code'], row['year'], age_num, age)

def load_long_format(cat: SqlCatalog) -> pd.DataFrame:
    dfs = []
    
    # 1. naselenie -> population_total
    try:
        nas_df = cat.load_table("silver.naselenie").scan().to_pandas()
        if not nas_df.empty:
            nas_df = nas_df[['region_code', 'region_name_raw', 'year', 'age', 'total_both']].copy()
            nas_df.rename(columns={'total_both': 'value'}, inplace=True)
            nas_df['metric_code'] = 'population_total'
            dfs.append(nas_df)
    except Exception as e:
        log.warning("Could not load silver.naselenie: %s", e)

    # 2. doshkolka -> level_1_1
    try:
        dosh_df = cat.load_table("silver.doshkolka").scan().to_pandas()
        if not dosh_df.empty:
            # Берем только territory_type == 'total'
            dosh_df = dosh_df[dosh_df['territory_type'] == 'total'].copy()
            dosh_df = dosh_df[['region_code', 'region_name_raw', 'year', 'age_group', 'value']].copy()
            dosh_df.rename(columns={'age_group': 'age'}, inplace=True)
            dosh_df['metric_code'] = 'level_1_1'
            dfs.append(dosh_df)
    except Exception as e:
        log.warning("Could not load silver.doshkolka: %s", e)

    # 3. oo -> level_1_2, level_1_3, level_1_4
    try:
        oo_df = cat.load_table("silver.oo").scan().to_pandas()
        if not oo_df.empty:
            oo_df = oo_df[['region_code', 'region_name_raw', 'year', 'age', 'level_code', 'value']].copy()
            oo_df['metric_code'] = oo_df['level_code'].apply(lambda x: f"level_{str(x).replace('.', '_')}")
            oo_df.drop(columns=['level_code'], inplace=True)
            dfs.append(oo_df)
    except Exception as e:
        log.warning("Could not load silver.oo: %s", e)

    # 4. spo -> level_2_5_1, level_2_5_2
    try:
        spo_df = cat.load_table("silver.spo").scan().to_pandas()
        if not spo_df.empty:
            spo_df = spo_df[['region_code', 'region_name_raw', 'year', 'age', 'program_code', 'value']].copy()
            spo_df['metric_code'] = spo_df['program_code'].apply(lambda x: f"level_{str(x).replace('.', '_')}")
            spo_df.drop(columns=['program_code'], inplace=True)
            dfs.append(spo_df)
    except Exception as e:
        log.warning("Could not load silver.spo: %s", e)

    # 5. vpo -> level_2_6, level_2_7, level_2_8
    try:
        vpo_df = cat.load_table("silver.vpo").scan().to_pandas()
        if not vpo_df.empty:
            vpo_df = vpo_df[['region_code', 'region_name_raw', 'year', 'age', 'level_code', 'value']].copy()
            vpo_df['metric_code'] = vpo_df['level_code'].apply(lambda x: f"level_{str(x).replace('.', '_')}")
            vpo_df.drop(columns=['level_code'], inplace=True)
            dfs.append(vpo_df)
    except Exception as e:
        log.warning("Could not load silver.vpo: %s", e)

    # 6. dpo -> level_4_8b_1, level_4_8b_2
    try:
        dpo_df = cat.load_table("silver.dpo").scan().to_pandas()
        if not dpo_df.empty:
            dpo_df = dpo_df[['region_code', 'region_name_raw', 'year', 'age_band', 'program_code', 'value']].copy()
            dpo_df.rename(columns={'age_band': 'age'}, inplace=True)
            dpo_df['metric_code'] = dpo_df['program_code'].apply(lambda x: f"level_{str(x).replace('.', '_')}")
            dpo_df.drop(columns=['program_code'], inplace=True)
            dfs.append(dpo_df)
    except Exception as e:
        log.warning("Could not load silver.dpo: %s", e)
        
    if not dfs:
        return pd.DataFrame()
        
    lon_df = pd.concat(dfs, ignore_index=True)
    return lon_df

def transform(cat: SqlCatalog) -> list[dict]:
    df_long = load_long_format(cat)
    if df_long.empty:
        log.warning("No data found to build wide table")
        return []
        
    log.info("Total long records: %d", len(df_long))
    
    # 1. Normalize age
    df_long['age'] = df_long['age'].apply(normalize_age)
    
    # 1.5. Normalize region_name_raw to avoid multiple rows for the same region_code
    # Select the first seen region_name_raw for each region_code
    reg_map = df_long.drop_duplicates('region_code').set_index('region_code')['region_name_raw']
    df_long['region_name_raw'] = df_long['region_code'].map(reg_map)
    
    # group by just in case there are duplicates with the same keys, though there shouldn't be
    # in some sources, but we sum them securely.
    grouped = df_long.groupby(
        ['region_code', 'region_name_raw', 'year', 'age', 'metric_code'], 
        as_index=False
    )['value'].sum()
    
    # 2. Pivot
    wide_df = grouped.pivot(
        index=['region_code', 'region_name_raw', 'year', 'age'], 
        columns='metric_code', 
        values='value'
    ).reset_index()
    
    # Ensure all metric columns exist
    for m in METRICS:
        if m not in wide_df.columns:
            wide_df[m] = None
            
    # 3. Sort records
    # Custom sort using the sort_key function over records
    records = wide_df.to_dict('records')
    records.sort(key=sort_key)
    
    # 4. Fill NaNs with None for Arrow compatibility
    for r in records:
        for m in METRICS:
            if pd.isna(r[m]):
                r[m] = None
            else:
                r[m] = int(r[m])
                
    return records

def run() -> int:
    cat = SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )

    tbl = cat.load_table("silver.education_population_wide")

    # Идемпотентность: перезаписываем (overwrite) 
    # В Iceberg можно сделать tbl.overwrite(arrow_tbl),
    # Для простоты можно удалить старые данные или просто использовать delete
    
    existing = tbl.scan().to_arrow()
    if len(existing) > 0:
        log.info("Deleting existing records in silver.education_population_wide...")
        tbl.delete("region_code is not null")

    records = transform(cat)
    if not records:
        log.warning("Нет данных для записи")
        return 0

    pa_schema = pa.schema([
        pa.field("region_code", pa.string(), nullable=False),
        pa.field("region_name_raw", pa.string(), nullable=False),
        pa.field("year", pa.int32(), nullable=False),
        pa.field("age", pa.string(), nullable=False),
        pa.field("population_total", pa.int64(), nullable=True),
        pa.field("level_1_1", pa.int64(), nullable=True),
        pa.field("level_1_2", pa.int64(), nullable=True),
        pa.field("level_1_3", pa.int64(), nullable=True),
        pa.field("level_1_4", pa.int64(), nullable=True),
        pa.field("level_2_5_1", pa.int64(), nullable=True),
        pa.field("level_2_5_2", pa.int64(), nullable=True),
        pa.field("level_2_6", pa.int64(), nullable=True),
        pa.field("level_2_7", pa.int64(), nullable=True),
        pa.field("level_2_8", pa.int64(), nullable=True),
        pa.field("level_4_8b_1", pa.int64(), nullable=True),
        pa.field("level_4_8b_2", pa.int64(), nullable=True),
    ])

    arrays = {
        "region_code": pa.array([r["region_code"] for r in records], pa.string()),
        "region_name_raw": pa.array([r["region_name_raw"] for r in records], pa.string()),
        "year": pa.array([r["year"] for r in records], pa.int32()),
        "age": pa.array([r["age"] for r in records], pa.string()),
    }
    
    for m in METRICS:
        arrays[m] = pa.array([r[m] for r in records], pa.int64())

    arrow_tbl = pa.table(arrays, schema=pa_schema)

    tbl.append(arrow_tbl)
    log.info("Записано %d строк в silver.education_population_wide", len(records))
    return len(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    count = run()
    if count > 0:
        print(f"\nГотово: {count} строк")
