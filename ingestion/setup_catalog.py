"""
Инициализация Iceberg каталога и схем Bronze/Silver.
Запускается один раз (или при пересоздании таблиц).
"""

import os
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestamptzType, IntegerType, LongType

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")
os.makedirs(CATALOG_DIR, exist_ok=True)

catalog = SqlCatalog(
    "datalake",
    **{
        "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
        "warehouse": f"file://{CATALOG_DIR}/warehouse",  # абсолютный путь → пути в снапшотах тоже абсолютные
    },
)

for ns in ["bronze", "bronze_normalized", "silver", "gold"]:
    try:
        catalog.create_namespace(ns)
        print(f"Created namespace: {ns}")
    except Exception:
        print(f"Namespace already exists: {ns}")

# Базовая схема bronze.excel_tables
# Принцип: максимально сырые данные, никакой бизнес-логики.
# col_0..col_N добавляются динамически через schema evolution.
base_schema = Schema(
    NestedField(1,  "load_id",           StringType(),      required=True),
    NestedField(2,  "loaded_at",         TimestamptzType(), required=True),
    NestedField(3,  "source_file",       StringType(),      required=True),
    NestedField(4,  "year",              IntegerType(),     required=True),
    NestedField(5,  "sheet_name",        StringType(),      required=True),
    NestedField(6,  "row_num",           IntegerType(),     required=True),
    NestedField(7,  "range_address",     StringType(),      required=False),
    NestedField(8,  "merged_cells_meta", StringType(),      required=False),
)

try:
    table = catalog.create_table("bronze.excel_tables", schema=base_schema)
    print("Created table: bronze.excel_tables")
except Exception:
    table = catalog.load_table("bronze.excel_tables")
    print("Table already exists: bronze.excel_tables")

print(f"\nCurrent schema:\n{table.schema()}")

# Silver: silver.doshkolka
doshkolka_schema = Schema(
    NestedField(1, "region_code",     StringType(), required=True),
    NestedField(2, "region_name_raw", StringType(), required=True),
    NestedField(3, "year",            IntegerType(), required=True),
    NestedField(4, "territory_type",  StringType(), required=True),
    NestedField(5, "age_group",       StringType(), required=True),
    NestedField(6, "value",           LongType(),   required=False),
)
# ── bronze.region_lookup ──────────────────────────────────────────────────────

from pyiceberg.types import BooleanType
region_lookup_schema = Schema(
    NestedField(1, "name_variant",   StringType(), required=True),
    NestedField(2, "canonical_name", StringType(), required=True),
    NestedField(3, "region_code",    StringType(), required=True),
    NestedField(4, "is_alias",       BooleanType(), required=True),
)
try:
    catalog.create_table("bronze.region_lookup", schema=region_lookup_schema)
    print("Created table: bronze.region_lookup")
except Exception:
    print("Table already exists: bronze.region_lookup")

# ── bronze_normalized: 4 таблицы нормализационного подслоя ────────────────────

region_ok_schema = Schema(
    NestedField(1,  "row_id",           StringType(),  required=True),
    NestedField(2,  "source_id",        StringType(),  required=True),
    NestedField(3,  "source_file",      StringType(),  required=True),
    NestedField(4,  "sheet_name",       StringType(),  required=True),
    NestedField(5,  "year",             IntegerType(), required=True),
    NestedField(6,  "row_num",          IntegerType(), required=True),
    NestedField(7,  "region_raw",       StringType(),  required=True),
    NestedField(8,  "region_code",      StringType(),  required=True),
    NestedField(9,  "resolution_scope", StringType(),  required=True),
    NestedField(10, "normalized_key",   StringType(),  required=True),
)
try:
    catalog.create_table("bronze_normalized.region", schema=region_ok_schema)
    print("Created table: bronze_normalized.region")
except Exception:
    print("Table already exists: bronze_normalized.region")

region_error_schema = Schema(
    NestedField(1,  "row_id",           StringType(),  required=True),
    NestedField(2,  "source_id",        StringType(),  required=True),
    NestedField(3,  "source_file",      StringType(),  required=True),
    NestedField(4,  "sheet_name",       StringType(),  required=True),
    NestedField(5,  "year",             IntegerType(), required=True),
    NestedField(6,  "row_num",          IntegerType(), required=True),
    NestedField(7,  "region_raw",       StringType(),  required=True),
    NestedField(8,  "error_type",       StringType(),  required=True),
    NestedField(9,  "resolution_scope", StringType(),  required=True),
    NestedField(10, "normalized_key",   StringType(),  required=True),
)
try:
    catalog.create_table("bronze_normalized.region_error", schema=region_error_schema)
    print("Created table: bronze_normalized.region_error")
except Exception:
    print("Table already exists: bronze_normalized.region_error")

year_ok_schema = Schema(
    NestedField(1,  "row_id",           StringType(),  required=True),
    NestedField(2,  "source_id",        StringType(),  required=True),
    NestedField(3,  "source_file",      StringType(),  required=True),
    NestedField(4,  "sheet_name",       StringType(),  required=True),
    NestedField(5,  "row_num",          IntegerType(), required=True),
    NestedField(6,  "year",             IntegerType(), required=True),
    NestedField(7,  "year_type",        StringType(),  required=True),
    NestedField(8,  "date_raw",         StringType(),  required=False),
    NestedField(9,  "year_raw",         StringType(),  required=False),
    NestedField(10, "resolution_scope", StringType(),  required=True),
)
try:
    catalog.create_table("bronze_normalized.year", schema=year_ok_schema)
    print("Created table: bronze_normalized.year")
except Exception:
    print("Table already exists: bronze_normalized.year")

year_error_schema = Schema(
    NestedField(1, "row_id",           StringType(),  required=True),
    NestedField(2, "source_id",        StringType(),  required=True),
    NestedField(3, "source_file",      StringType(),  required=True),
    NestedField(4, "sheet_name",       StringType(),  required=True),
    NestedField(5, "row_num",          IntegerType(), required=True),
    NestedField(6, "year_raw",         StringType(),  required=True),
    NestedField(7, "error_type",       StringType(),  required=True),
    NestedField(8, "resolution_scope", StringType(),  required=True),
)
try:
    catalog.create_table("bronze_normalized.year_error", schema=year_error_schema)
    print("Created table: bronze_normalized.year_error")
except Exception:
    print("Table already exists: bronze_normalized.year_error")

# ── Silver ─────────────────────────────────────────────────────────────────────

try:
    catalog.create_table("silver.doshkolka", schema=doshkolka_schema)
    print("Created table: silver.doshkolka")
except Exception:
    print("Table already exists: silver.doshkolka")

# Silver: silver.naselenie — население по полу и возрасту по регионам
naselenie_schema = Schema(
    NestedField(1,  "region_code",     StringType(),  required=True),
    NestedField(2,  "region_name_raw", StringType(),  required=True),
    NestedField(3,  "year",            IntegerType(), required=True),
    NestedField(4,  "age",             StringType(),  required=True),
    NestedField(5,  "total_both",      LongType(),    required=False),
    NestedField(6,  "total_male",      LongType(),    required=False),
    NestedField(7,  "total_female",    LongType(),    required=False),
    NestedField(8,  "urban_both",      LongType(),    required=False),
    NestedField(9,  "urban_male",      LongType(),    required=False),
    NestedField(10, "urban_female",    LongType(),    required=False),
    NestedField(11, "rural_both",      LongType(),    required=False),
    NestedField(12, "rural_male",      LongType(),    required=False),
    NestedField(13, "rural_female",    LongType(),    required=False),
)
try:
    catalog.create_table("silver.naselenie", schema=naselenie_schema)
    print("Created table: silver.naselenie")
except Exception:
    print("Table already exists: silver.naselenie")
