"""
Инициализация Iceberg каталога и схем Bronze/Silver.
Запускается один раз (или при пересоздании таблиц).
"""

import os
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestamptzType, IntegerType, LongType, DoubleType

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

# ── bronze.education_level_lookup ─────────────────────────────────────────────

edu_level_lookup_schema = Schema(
    NestedField(1, "code",               StringType(), required=True),
    NestedField(2, "label_ru",           StringType(), required=True),
    NestedField(3, "kind_code",          StringType(), required=False),
    NestedField(4, "kind_label",         StringType(), required=False),
    NestedField(5, "level_code",         StringType(), required=False),
    NestedField(6, "level_label",        StringType(), required=False),
    NestedField(7, "program_code",       StringType(), required=False),
    NestedField(8, "program_label",      StringType(), required=False),
    NestedField(9, "granularity_level",  StringType(), required=True),
)
try:
    catalog.create_table("bronze.education_level_lookup", schema=edu_level_lookup_schema)
    print("Created table: bronze.education_level_lookup")
except Exception:
    print("Table already exists: bronze.education_level_lookup")

# ── bronze_normalized.education_level ─────────────────────────────────────────

edu_level_ok_schema = Schema(
    NestedField(1, "row_id",         StringType(), required=True),
    NestedField(2, "source_id",      StringType(), required=True),
    NestedField(3, "source_file",    StringType(), required=True),
    NestedField(4, "level_code",     StringType(), required=True),
    NestedField(5, "level_label",    StringType(), required=True),
    NestedField(6, "program_code",   StringType(), required=False),
    NestedField(7, "program_label",  StringType(), required=False),
    NestedField(8, "match_field",    StringType(), required=True),
    NestedField(9, "match_value",    StringType(), required=True),
    NestedField(10, "status",        StringType(), required=True),
)
try:
    catalog.create_table("bronze_normalized.education_level", schema=edu_level_ok_schema)
    print("Created table: bronze_normalized.education_level")
except Exception:
    print("Table already exists: bronze_normalized.education_level")

edu_level_err_schema = Schema(
    NestedField(1, "row_id",         StringType(), required=True),
    NestedField(2, "source_id",      StringType(), required=True),
    NestedField(3, "source_file",    StringType(), required=True),
    NestedField(4, "match_field",    StringType(), required=False),
    NestedField(5, "match_value",    StringType(), required=False),
    NestedField(6, "error_type",     StringType(), required=True),
    NestedField(7, "error_details",  StringType(), required=False),
)
try:
    catalog.create_table("bronze_normalized.education_level_error", schema=edu_level_err_schema)
    print("Created table: bronze_normalized.education_level_error")
except Exception:
    print("Table already exists: bronze_normalized.education_level_error")

# ── bronze_normalized.row_gate ─────────────────────────────────────────────────

row_gate_schema = Schema(
    NestedField(1, "row_id",                 StringType(),  required=True),
    NestedField(2, "source_id",              StringType(),  required=True),
    NestedField(3, "region_status",          StringType(),  required=True),
    NestedField(4, "year_status",            StringType(),  required=True),
    NestedField(5, "education_level_status", StringType(),  required=True),
    NestedField(6, "all_required_ok",        BooleanType(), required=True),
    NestedField(7, "ready_for_silver",       BooleanType(), required=True),
    NestedField(8, "rejection_reason",       StringType(),  required=True),
)
try:
    catalog.create_table("bronze_normalized.row_gate", schema=row_gate_schema)
    print("Created table: bronze_normalized.row_gate")
except Exception:
    print("Table already exists: bronze_normalized.row_gate")

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

# ── Новые Silver-таблицы (ТЗ: Образование) ────────────────────────────────────

oo_schema = Schema(
    NestedField(1, "region_code",     StringType(),  required=True),
    NestedField(2, "region_name_raw", StringType(),  required=True),
    NestedField(3, "year",            IntegerType(), required=True),
    NestedField(4, "age",             StringType(),  required=True),
    NestedField(5, "level_code",      StringType(),  required=True),
    NestedField(6, "level_label",     StringType(),  required=True),
    NestedField(7, "value",           LongType(),    required=False),
)

spo_schema = Schema(
    NestedField(1, "region_code",     StringType(),  required=True),
    NestedField(2, "region_name_raw", StringType(),  required=True),
    NestedField(3, "year",            IntegerType(), required=True),
    NestedField(4, "age",             StringType(),  required=True),
    NestedField(5, "level_code",      StringType(),  required=True),
    NestedField(6, "program_code",    StringType(),  required=False),
    NestedField(7, "program_label",   StringType(),  required=False),
    NestedField(8, "value",           LongType(),    required=False),
)

vpo_schema = oo_schema

dpo_schema = Schema(
    NestedField(1, "region_code",     StringType(),  required=True),
    NestedField(2, "region_name_raw", StringType(),  required=True),
    NestedField(3, "year",            IntegerType(), required=True),
    NestedField(4, "age_band",        StringType(),  required=True),
    NestedField(5, "level_code",      StringType(),  required=True),
    NestedField(6, "program_code",    StringType(),  required=False),
    NestedField(7, "program_label",   StringType(),  required=False),
    NestedField(8, "value",           LongType(),    required=False),
)

for table_name, schema in [
    ("silver.oo",  oo_schema),
    ("silver.spo", spo_schema),
    ("silver.vpo", vpo_schema),
    ("silver.dpo", dpo_schema),
]:
    try:
        catalog.create_table(table_name, schema=schema)
        print(f"Created table: {table_name}")
    except Exception:
        print(f"Table already exists: {table_name}")

education_population_wide_schema = Schema(
    NestedField(1, "region_code",     StringType(), required=True),
    NestedField(2, "region_name_raw", StringType(), required=True),
    NestedField(3, "year",            IntegerType(), required=True),
    NestedField(4, "age",             StringType(), required=True),
    NestedField(5, "population_total", LongType(),   required=False),
    NestedField(6, "level_1_1",       LongType(),   required=False),
    NestedField(7, "level_1_2",       LongType(),   required=False),
    NestedField(8, "level_1_3",       LongType(),   required=False),
    NestedField(9, "level_1_4",       LongType(),   required=False),
    NestedField(10, "level_2_5_1",    LongType(),   required=False),
    NestedField(11, "level_2_5_2",    LongType(),   required=False),
    NestedField(12, "level_2_6",      LongType(),   required=False),
    NestedField(13, "level_2_7",      LongType(),   required=False),
    NestedField(14, "level_2_8",      LongType(),   required=False),
    NestedField(15, "level_4_8b_1",   LongType(),   required=False),
    NestedField(16, "level_4_8b_2",   LongType(),   required=False),
)
try:
    catalog.create_table("silver.education_population_wide", schema=education_population_wide_schema)
    print("Created table: silver.education_population_wide")
except Exception:
    print("Table already exists: silver.education_population_wide")

annual_schema = Schema(
    NestedField(1,  "region_code",      StringType(),  required=True),
    NestedField(2,  "region_name_raw",  StringType(),  required=True),
    NestedField(3,  "year",             IntegerType(), required=True),
    NestedField(4,  "age",              StringType(),  required=True),
    NestedField(5,  "population_total", LongType(),    required=False),
    NestedField(6,  "level_1_1",        LongType(),    required=False),
    NestedField(7,  "level_1_2",        LongType(),    required=False),
    NestedField(8,  "level_1_3",        LongType(),    required=False),
    NestedField(9,  "level_1_4",        LongType(),    required=False),
    NestedField(10, "level_2_5_1",      LongType(),    required=False),
    NestedField(11, "level_2_5_2",      LongType(),    required=False),
    NestedField(12, "level_2_6",        LongType(),    required=False),
    NestedField(13, "level_2_7",        LongType(),    required=False),
    NestedField(14, "level_2_8",        LongType(),    required=False),
    NestedField(15, "level_4_8b_1",     LongType(),    required=False),
    NestedField(16, "level_4_8b_2",     LongType(),    required=False),
    NestedField(17, "education_total",  LongType(),    required=False),
    NestedField(18, "education_share",  DoubleType(),  required=False),
)
try:
    catalog.create_table("silver.education_population_wide_annual", schema=annual_schema)
    print("Created table: silver.education_population_wide_annual")
except Exception:
    print("Table already exists: silver.education_population_wide_annual")

# ── Линия 1: Дефицит кадров ────────────────────────────────────────────────────

staff_shortage_schema = Schema(
    NestedField(1, "region_code",   StringType(),  required=True),
    NestedField(2, "year",          IntegerType(), required=True),
    NestedField(3, "level",         StringType(),  required=True),
    NestedField(4, "student_count", IntegerType(), required=False),
    NestedField(5, "trig1_val",     DoubleType(),  required=False),
    NestedField(6, "trig2_val",     DoubleType(),  required=False),
    NestedField(7, "bonus_score",   DoubleType(),  required=False),
    NestedField(8, "score",         DoubleType(),  required=False),
)
try:
    catalog.create_table("silver.staff_shortage_triggers", schema=staff_shortage_schema)
    print("Created table: silver.staff_shortage_triggers")
except Exception:
    print("Table already exists: silver.staff_shortage_triggers")

# ── Линия 2: Общежития ─────────────────────────────────────────────────────────

from pyiceberg.types import BooleanType as _Bool

dormitory_schema = Schema(
    NestedField(1,  "region_code",          StringType(),  required=True),
    NestedField(2,  "year",                 IntegerType(), required=True),
    NestedField(3,  "is_forecast",          _Bool(),       required=True),
    NestedField(4,  "area_total",           DoubleType(),  required=False),
    NestedField(5,  "area_need_repair",     DoubleType(),  required=False),
    NestedField(6,  "area_in_repair",       DoubleType(),  required=False),
    NestedField(7,  "area_emergency",       DoubleType(),  required=False),
    NestedField(8,  "dorm_need",            DoubleType(),  required=False),
    NestedField(9,  "dorm_live",            DoubleType(),  required=False),
    NestedField(10, "dorm_shortage_abs",    DoubleType(),  required=False),
    NestedField(11, "metric_1",             DoubleType(),  required=False),
    NestedField(12, "metric_2",             DoubleType(),  required=False),
    NestedField(13, "metric_3",             DoubleType(),  required=False),
    NestedField(14, "metric_4",             DoubleType(),  required=False),
    NestedField(15, "metric_1_mean",        DoubleType(),  required=False),
    NestedField(16, "metric_2_mean",        DoubleType(),  required=False),
    NestedField(17, "metric_3_mean",        DoubleType(),  required=False),
    NestedField(18, "metric_4_mean",        DoubleType(),  required=False),
    NestedField(19, "metric_1_sigma",       DoubleType(),  required=False),
    NestedField(20, "metric_2_sigma",       DoubleType(),  required=False),
    NestedField(21, "metric_3_sigma",       DoubleType(),  required=False),
    NestedField(22, "metric_4_sigma",       DoubleType(),  required=False),
    NestedField(23, "metric_1_penalty",     DoubleType(),  required=False),
    NestedField(24, "metric_2_penalty",     DoubleType(),  required=False),
    NestedField(25, "metric_3_penalty",     DoubleType(),  required=False),
    NestedField(26, "metric_4_penalty",     DoubleType(),  required=False),
    NestedField(27, "sigma_sum",            DoubleType(),  required=False),
    NestedField(28, "alert_flag",           IntegerType(), required=False),
)
try:
    catalog.create_table("silver.dormitory_infrastructure", schema=dormitory_schema)
    print("Created table: silver.dormitory_infrastructure")
except Exception:
    print("Table already exists: silver.dormitory_infrastructure")

# ── Gold-слой (аналитические агрегаты) ─────────────────────────────────────────

# Gold: gold.students — обучающиеся по возрастам
gold_students_schema = Schema(
    NestedField(1,  "region_code",     StringType(),  required=False),
    NestedField(2,  "year",            IntegerType(), required=False),
    NestedField(3,  "age",             IntegerType(), required=False),
    NestedField(4,  "population_total", DoubleType(),  required=False),
    NestedField(5,  "level_1_1",       DoubleType(),  required=False),
    NestedField(6,  "level_1_2",       DoubleType(),  required=False),
    NestedField(7,  "level_1_3",       DoubleType(),  required=False),
    NestedField(8,  "level_1_4",       DoubleType(),  required=False),
    NestedField(9,  "level_2_5_1",     DoubleType(),  required=False),
    NestedField(10, "level_2_5_2",     DoubleType(),  required=False),
    NestedField(11, "level_2_6",       DoubleType(),  required=False),
    NestedField(12, "level_2_7",       DoubleType(),  required=False),
    NestedField(13, "level_2_8",       DoubleType(),  required=False),
    NestedField(14, "level_4_8b_1",    DoubleType(),  required=False),
    NestedField(15, "level_4_8b_2",    DoubleType(),  required=False),
    NestedField(16, "education_total", DoubleType(),  required=False),
    NestedField(17, "education_share", DoubleType(),  required=False),
)
try:
    catalog.create_table("gold.students", schema=gold_students_schema)
    print("Created table: gold.students")
except Exception:
    print("Table already exists: gold.students")

# Gold: gold.staff_load — педагогическая нагрузка
gold_staff_load_schema = Schema(
    NestedField(1,  "region_code",          StringType(),  required=False),
    NestedField(2,  "year",                 IntegerType(), required=False),
    NestedField(3,  "level",                StringType(),  required=False),
    NestedField(4,  "student_count",        DoubleType(),  required=False),
    NestedField(5,  "positions_total",      DoubleType(),  required=False),
    NestedField(6,  "staff_headcount",      DoubleType(),  required=False),
    NestedField(7,  "vacancies_unfilled",   DoubleType(),  required=False),
    NestedField(8,  "staff_fte",            DoubleType(),  required=False),
    NestedField(9,  "qual_higher_edu",      DoubleType(),  required=False),
    NestedField(10, "qual_higher_cat",      DoubleType(),  required=False),
    NestedField(11, "qual_first_cat",       DoubleType(),  required=False),
    NestedField(12, "qual_ped_higher",      DoubleType(),  required=False),
    NestedField(13, "qual_spe_mid",         DoubleType(),  required=False),
    NestedField(14, "qual_ped_mid",         DoubleType(),  required=False),
    NestedField(15, "qual_candidate",       DoubleType(),  required=False),
    NestedField(16, "qual_doctor",          DoubleType(),  required=False),
    NestedField(17, "exp_total",            DoubleType(),  required=False),
    NestedField(18, "exp_none",             DoubleType(),  required=False),
    NestedField(19, "exp_lt3",              DoubleType(),  required=False),
    NestedField(20, "exp_3_5",              DoubleType(),  required=False),
    NestedField(21, "exp_5_10",             DoubleType(),  required=False),
    NestedField(22, "exp_10_15",            DoubleType(),  required=False),
    NestedField(23, "exp_15_20",            DoubleType(),  required=False),
    NestedField(24, "exp_gt20",             DoubleType(),  required=False),
    NestedField(25, "load_ratio",           DoubleType(),  required=False),
    NestedField(26, "vacancy_unfilled_share", DoubleType(), required=False),
    NestedField(27, "avg_hours_per_teacher", DoubleType(), required=False),
    NestedField(28, "shortage_score",       DoubleType(),  required=False),
)
try:
    catalog.create_table("gold.staff_load", schema=gold_staff_load_schema)
    print("Created table: gold.staff_load")
except Exception:
    print("Table already exists: gold.staff_load")

# Gold: gold.dormitory — общежития
gold_dormitory_schema = Schema(
    NestedField(1,  "region_code",       StringType(),  required=False),
    NestedField(2,  "year",              IntegerType(), required=False),
    NestedField(3,  "area_total",        DoubleType(),  required=False),
    NestedField(4,  "area_need_repair",  DoubleType(),  required=False),
    NestedField(5,  "repair_share",      DoubleType(),  required=False),
    NestedField(6,  "area_emergency",    DoubleType(),  required=False),
    NestedField(7,  "dorm_shortage_abs", DoubleType(),  required=False),
    NestedField(8,  "dorm_shortage_share", DoubleType(), required=False),
    NestedField(9,  "dorm_need",         DoubleType(),  required=False),
    NestedField(10, "dorm_live",         DoubleType(),  required=False),
    NestedField(11, "is_forecast",       _Bool(),       required=False),
    NestedField(12, "alert_flag",        IntegerType(), required=False),
)
try:
    catalog.create_table("gold.dormitory", schema=gold_dormitory_schema)
    print("Created table: gold.dormitory")
except Exception:
    print("Table already exists: gold.dormitory")
