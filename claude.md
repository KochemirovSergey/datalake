# CLAUDE.md — Контекст проекта и инструкции для агента

## Кто ты и что происходит

Ты — AI-агент, работающий над проектом по построению локального ETL-пайплайна
для анализа демографических данных России.

Читай этот файл при каждом запуске. Он описывает архитектуру, текущее состояние
и следующий этап работы.

***

## Текущее состояние (апрель 2026)

### Инфраструктура — развёрнута

- **RustFS** (Docker) — локальное S3-хранилище, порты 9000/9001
- **Apache Iceberg** (SQLite catalog) — `catalog/catalog.db`, warehouse в `catalog/warehouse/`
- **DuckDB** — `datalake.duckdb`, вьюхи пересоздаются через `scripts/refresh_duckdb.py`
- **Dagster** — оркестрация, граф assets, UI на `http://localhost:3000`

### Данные — загружены и обработаны

**Bronze:**
- `bronze.excel_tables` — все листы всех Excel-файлов в сыром виде (col_0..col_163)
  - Источник 1: `data/Дошколка/YYYY/` — дети в дошкольных учреждениях, 7 лет (2018–2024)
  - Источник 2: `data/Население/Бюллетень_YYYY.xlsx` — бюллетени о населении, 7 лет (2018–2024)
- `bronze.region_lookup` — справочник регионов, 107 строк (89 канонических + 18 алиасов)
- **PostgreSQL-источники** (загружаются через `ingestion/postgres_loader.py`):
  - `bronze.oo_1_2_7_2_211_v2` ← `public.oo_1_2_7_2_211_v2`
  - `bronze.oo_1_2_7_1_209_v2` ← `public.oo_1_2_7_1_209_v2`
  - `bronze.спо_1_р2_101_43` ← `public.спо_1_р2_101_43`
  - `bronze.впо_1_р2_13_54` ← `public.впо_1_р2_13_54`
  - `bronze.пк_1_2_4_180` ← `public.пк_1_2_4_180`
  - `bronze.discipuli` ← `public.discipuli` (численность обучающихся, используется в обеих линиях дашбордов)
  - `bronze.oo_1_3_4_230` ← `public.oo_1_3_4_230` (форма 3.4 ОО — наличие ставок учителей)
  - `bronze.oo_1_3_1_218` ← `public.oo_1_3_1_218` (форма 3.1 ОО — триггер дефицита и бонус)
  - `bronze.oo_1_3_2_221` ← `public.oo_1_3_2_221` (форма 3.2 ОО — второй компонент бонуса)
  - `bronze.впо_2_р1_3_8` ← `public.впо_2_р1_3_8` (площади учебных зданий ВПО)
  - `bronze.впо_2_р1_4_10` ← `public.впо_2_р1_4_10` (нуждаемость в общежитиях и фактическое проживание)

**Bronze Normalized — реализован:**
- `bronze_normalized.region` — нормализованный регион по каждой строке
- `bronze_normalized.region_error` — строки, для которых регион не распознан
- `bronze_normalized.year` — нормализованный год по каждой строке
- `bronze_normalized.year_error` — строки, для которых год не распознан
- `bronze_normalized.education_level` — нормализованный education_level (doshkolka=1.1; ОО — заглушки)
- `bronze_normalized.education_level_error` — строки, для которых education_level не определён
- `bronze_normalized.row_gate` — gate по трём измерениям; 97.6% строк ready_for_silver

**Silver — реализован, читает из bronze_normalized:**
- `silver.doshkolka` — 15 993 строки: регион × год × территория × возрастная группа × значение
- `silver.naselenie` — регион × год × возраст (0–79, 80+) × 9 числовых показателей (пол × тип территории)
- `silver.oo` — регион × год × возраст × уровень образования (1.x)
- `silver.spo` — регион × год × возраст × программа (2.5)
- `silver.vpo` — регион × год × возраст × уровень образования (2.6+)
- `silver.dpo` — регион × год × возрастная группа × программа (4.8)
- `silver.staff_shortage_triggers` — регион × год × уровень: тригеры дефицита, бонус, score (0–10) — **линия дашборда «Дефицит кадров»**
- `silver.dormitory_infrastructure` — регион × год × is_forecast: метрики площадей и общежитий + sigma/alert — **линия дашборда «Общежития»**

### Скрипты

- `scripts/refresh_duckdb.py` — пересоздаёт DuckDB-вьюхи из Iceberg снапшотов
- `scripts/reset.py` — очистка таблиц (интерактивное меню или CLI-аргумент)
- `ingestion/setup_catalog.py` — инициализация каталога и схем (при пересоздании)
- `ingestion/excel_loader.py` — загрузчик Excel (`flat=True` для файлов с годом в имени)
- `ingestion/json_loader.py` — загрузчик регионального справочника из JSON (бывший `regions_loader.py`)
- `ingestion/postgres_loader.py` — загрузчик таблиц из PostgreSQL в Bronze (все значения → string)
- `transformations/bronze_normalized/common.py` — общие утилиты + интерфейс DimensionProcessor
- `transformations/bronze_normalized/education_level_pipeline.py` — нормализация education_level
- `transformations/bronze_normalized/row_gate_pipeline.py` — gate по трём измерениям
- `ingestion/education_level_loader.py` — загрузчик справочника education_level_lookup.csv
- `transformations/silver_doshkolka.py` — Bronze → Silver (дошкольники), читает из bronze_normalized
- `transformations/silver_naselenie.py` — Bronze → Silver (население), читает из bronze_normalized
- `transformations/silver_oo.py` — Bronze → Silver (школы)
- `transformations/silver_spo.py` — Bronze → Silver (СПО)
- `transformations/silver_vpo.py` — Bronze → Silver (ВУЗы)
- `transformations/silver_dpo.py` — Bronze → Silver (ДПО)
- `validation/validate_silver.py` — генерирует Markdown-отчёт покрытия для Silver-слоя (doshkolka, naselenie)
- `validation/validate_silver_doshkolka.py` — детальная матрица region × year для doshkolka
- `validation/validate_silver_education.py` — отчёт покрытия для таблиц образования (silver_oo/spo/vpo/dpo)
- `validation/validate_staff_shortage.py` — Markdown-отчёт по `silver.staff_shortage_triggers`
- `validation/validate_dormitory.py` — Markdown-отчёт по `silver.dormitory_infrastructure`
- `transformations/staff_shortage_pipeline.py` — расчёт триггеров дефицита кадров + score (0–10)
- `transformations/dormitory_pipeline.py` — расчёт метрик общежитий + прогноз + sigma/alert
- `dashboards/staff_shortage_dashboard.py` — HTML-генератор дашборда «Дефицит кадров»
- `dashboards/dormitory_dashboard.py` — HTML-генератор дашборда «Общежития»

### Схемы Silver-таблиц

**silver.doshkolka:**
```
region_code, region_name_raw, year, territory_type (total/urban/rural),
age_group (total/age_0/age_1/.../age_7plus), value
```

**silver.naselenie:**
```
region_code, region_name_raw, year, age (0–79, 80+),
total_both, total_male, total_female,
urban_both, urban_male, urban_female,
rural_both, rural_male, rural_female
```

***

## Архитектурные принципы

- **Bronze — иммутабельный слой.** Только append. Никаких обновлений, никакого удаления.
- **Идемпотентность.** Повторный запуск любого лоадера ничего не дублирует.
- **Lineage.** Каждая строка Silver содержит `region_name_raw` — исходное название,
  по которому можно отследить откуда пришла запись.
- **Переигровка всегда возможна.** Bronze хранит все исходные строки включая заголовки.
  Silver можно дропнуть и пересчитать из Bronze без потери данных.
- **Контекст централизован.** Регион и год для каждой строки определяются один раз
  на этапе `bronze_normalized` — все последующие слои используют уже нормализованные поля.

***

## Стек

| Слой | Инструмент |
|------|-----------|
| Физическое хранилище | RustFS (Docker) |
| Формат таблиц | Apache Iceberg (pyiceberg, SQLite catalog) |
| Запись/чтение | PyArrow |
| Трансформации | Python-скрипты (`transformations/`) |
| Аналитические запросы | DuckDB |
| Оркестрация + lineage | Dagster |

***

## Граф Dagster assets — подробное описание

### Группа `bronze_extraction`

#### `doshkolka_bronze`
- **Источник:** `data/Дошколка/YYYY/*.xlsx`
- **Вызывает:** `ingestion/excel_loader.run()`
- **Пишет в:** `bronze.excel_tables`
- **Статистика в UI:**
  - `total_rows` — общее число загруженных строк
  - `files_loaded` — количество файлов
  - `sheets_loaded` — количество листов
  - `breakdown` — таблица: год / файл / лист → кол-во строк

#### `naselenie_bronze`
- **Источник:** `data/Население/Бюллетень_YYYY.xlsx`
- **Вызывает:** `ingestion/excel_loader.run(data_dir=..., flat=True)`
- **Пишет в:** `bronze.excel_tables`
- **Статистика в UI:**
  - `total_rows` — общее число загруженных строк
  - `new_sheets` — количество новых листов

#### `regions_bronze`
- **Источник:** `data/regions.json`
- **Вызывает:** `ingestion/json_loader.run()`
- **Пишет в:** `bronze.region_lookup`
- **Статистика в UI:**
  - `total_rows` — количество записей (0 если уже загружено)

#### `postgres_bronze`
- **Источник:** PostgreSQL `etl_db` (localhost:5432), 5 таблиц:
  `oo_1_2_7_2_211_v2_v2`, `oo_1_2_7_1_209_v2_v2`, `спо_1_р2_101_43`, `впо_1_р2_13_54`, `пк_1_2_4_180`
- **Вызывает:** `ingestion/postgres_loader.run()`
- **Пишет в:** `bronze.<table_name>` (схема создаётся динамически по introspection)
- **Статистика в UI:**
  - `total_rows` — суммарное число строк по всем таблицам
  - `breakdown` — список: `source_table → N строк` (или «пропущено» если уже загружено)

---

### Группа `bronze_normalized`

#### `normalized_region`
- **Зависимости:** `doshkolka_bronze`, `naselenie_bronze`, `regions_bronze`
- **Вызывает:** `transformations/bronze_normalized/region_pipeline.run(cat)`
- **Пишет в:** `bronze_normalized.region` (ok), `bronze_normalized.region_error` (не распознано)
- **Логика:** для каждой строки Excel определяет регион через `region_lookup`;
  строки без совпадения уходят в `region_error`
- **Статистика в UI:**
  - `total_rows` — всего обработано строк
  - `ok_count` — успешно распознано
  - `error_count` — не распознано
  - `coverage` — процент покрытия (`ok / total`)
  - `breakdown` — по источнику: `doshkolka: ok=N error=M`, `naselenie: ok=N error=M`

#### `normalized_year`
- **Зависимости:** `doshkolka_bronze`, `naselenie_bronze`, `normalized_region`
- **Вызывает:** `transformations/bronze_normalized/year_pipeline.run(cat)`
- **Пишет в:** `bronze_normalized.year` (ok), `bronze_normalized.year_error` (не распознано)
- **Логика:** определяет отчётный год из имени файла / папки / заголовка листа
- **Статистика в UI:**
  - `ok_count` — строк с распознанным годом
  - `error_count` — строк без года
  - `breakdown` — по источнику: `doshkolka: ok=N error=M`, `naselenie: ok=N error=M`

#### `normalized_education`
- **Зависимости:** `doshkolka_bronze`, `postgres_bronze`, `education_level_lookup_bronze`
- **Вызывает:** `transformations/bronze_normalized/education_level_pipeline.run(cat)`
- **Пишет в:** `bronze_normalized.education_level` (ok), `bronze_normalized.education_level_error` (ошибки)
- **Логика:** для каждого источника применяет правило из конфига:
  - `location=source` (doshkolka): фиксированный код 1.1 для всех data-строк
  - `location=column_metadata` (spo): извлекает код из колонок таблицы
  - `location=stub` (oo, vo, pk): помечает как not_found до получения документации
  - `required=false` (населenie): пропускает источник
- **Статистика в UI:** ok_count, error_count, coverage, breakdown по источникам

#### `normalized_row_gate`
- **Зависимости:** `normalized_region`, `normalized_year`, `normalized_education`
- **Вызывает:** `transformations/bronze_normalized/row_gate_pipeline.run(cat)`
- **Пишет в:** `bronze_normalized.row_gate` (overwrite при каждом запуске)
- **Логика:** объединяет три измерения по row_id; `ready_for_silver` = True если все required-измерения = ok
- **Статистика в UI:** total, ready, not_ready, ready_pct

---

### Группа `silver`

#### `doshkolka_silver`
- **Зависимости:** `normalized_row_gate`
- **Вызывает:** `transformations/silver_doshkolka.run()`
- **Читает из:** `bronze_normalized.region` + `bronze_normalized.year` (пересечение по `row_id`),
  затем джойнит с `bronze.excel_tables` для получения числовых значений
- **Пишет в:** `silver.doshkolka`
- **Логика:** unpivot — одна строка = регион × год × территория (total/urban/rural) × возраст
- **Статистика в UI:**
  - `total_rows` — количество записей в Silver (≈15 993)

#### `naselenie_silver`
- **Зависимости:** `normalized_row_gate`
- **Вызывает:** `transformations/silver_naselenie.run()`
- **Читает из:** `bronze_normalized.region` + `bronze_normalized.year`,
  джойнит с `bronze.excel_tables`
- **Пишет в:** `silver.naselenie`
- **Логика:** каждая строка = регион × год × возраст (0–79, 80+) × 9 показателей
- **Статистика в UI:**
  - `total_rows` — количество записей в Silver

#### `oo_silver`
- **Зависимости:** `normalized_row_gate`
- **Вызывает:** `transformations/silver_oo.run()`
- **Пишет в:** `silver.oo`
- **Логика:** трансформирует данные PostgreSQL о школах по возрастам и уровням образования

#### `spo_silver`
- **Зависимости:** `normalized_row_gate`
- **Вызывает:** `transformations/silver_spo.run()`
- **Пишет в:** `silver.spo`
- **Логика:** среднее профессиональное образование по возрастам и программам

#### `vpo_silver`
- **Зависимости:** `normalized_row_gate`
- **Вызывает:** `transformations/silver_vpo.run()`
- **Пишет в:** `silver.vpo`
- **Логика:** высшее образование по возрастам и уровням

#### `dpo_silver`
- **Зависимости:** `normalized_row_gate`
- **Вызывает:** `transformations/silver_dpo.run()`
- **Пишет в:** `silver.dpo`
- **Логика:** дополнительное профессиональное образование по возрастным группам и программам

#### `education_population_wide_silver`
- **Зависимости:** `oo_silver`, `spo_silver`, `vpo_silver`, `dpo_silver`, `doshkolka_silver`, `naselenie_silver`
- **Вызывает:** `transformations/silver_education_population_wide.run()`
- **Пишет в:** `silver.education_population_wide`
- **Логика:** объединяет все источники (дошкольное, школы, СПО, ВПО, ДПО, население) в одну сборную витрину
- **Статистика в UI:**
  - `total_rows` — количество записей в сборной таблице
  - `report_path` — путь к отчёту валидации

#### `education_population_wide_annual_silver`
- **Зависимости:** `education_population_wide_silver`
- **Вызывает:** 
  - `transformations/silver_education_population_wide_annual.run()`
  - `validation/validate_silver_education_population_wide_annual.run(cat)`
  - `validation/validate_coverage_analysis.run(cat)`
- **Пишет в:** `silver.education_population_wide_annual`
- **Логика:** развёртывает возрастные диапазоны в отдельные года (0–80) с убывающим линейным распределением для открытых диапазонов
- **Генерирует:** 
  - Отчёт валидации в `reports/education_population_wide_annual_YYYY-MM-DD.md`
  - Анализ охвата образованием по возрастам в `reports/coverage_analysis_YYYY-MM-DD.md`
- **Статистика в UI:**
  - `total_rows` — количество записей в таблице
  - `report_path` — путь к отчёту валидации
  - `coverage_report_path` — путь к отчёту анализа охвата

---

---

### Группа `staff_shortage`

#### `discipuli_bronze`
- **Источник:** PostgreSQL `public.discipuli`
- **Вызывает:** `ingestion/postgres_loader.run(tables=["discipuli"])`
- **Пишет в:** `bronze.discipuli`

#### `staff_shortage_sources_bronze`
- **Вызывает:** `ingestion/postgres_loader.run(tables=["oo_1_3_4_230", "oo_1_3_1_218", "oo_1_3_2_221"])`
- **Пишет в:** `bronze.oo_1_3_4_230`, `bronze.oo_1_3_1_218`, `bronze.oo_1_3_2_221`

#### `staff_shortage_silver`
- **Зависимости:** `discipuli_bronze`, `staff_shortage_sources_bronze`
- **Вызывает:** `transformations/staff_shortage_pipeline.run(cat)`
- **Пишет в:** `silver.staff_shortage_triggers`

#### `staff_shortage_dashboard`
- **Зависимости:** `staff_shortage_silver`
- **Вызывает:** `dashboards/staff_shortage_dashboard.build_dashboard()` → загрузка в S3
- **s3_key:** `dashboards/staff-shortage/<date>/<date>_<run_id[:8]>.html`

---

### Группа `dormitory`

#### `dormitory_area_bronze`
- **Вызывает:** `ingestion/postgres_loader.run(tables=["вpo_2_р1_3_8"])`
- **Пишет в:** `bronze.впо_2_р1_3_8`

#### `dormitory_dorm_bronze`
- **Вызывает:** `ingestion/postgres_loader.run(tables=["вpo_2_р1_4_10"])`
- **Пишет в:** `bronze.впо_2_р1_4_10`

#### `dormitory_silver`
- **Зависимости:** `dormitory_area_bronze`, `dormitory_dorm_bronze`, `discipuli_bronze`
- **Вызывает:** `transformations/dormitory_pipeline.run(cat)`
- **Пишет в:** `silver.dormitory_infrastructure`

#### `dormitory_dashboard`
- **Зависимости:** `dormitory_silver`
- **Вызывает:** `dashboards/dormitory_dashboard.build_dashboard()` → загрузка в S3
- **s3_key:** `dashboards/dormitory/<date>/<date>_<run_id[:8]>.html`

---

### Порядок выполнения (полный пайплайн)

```
doshkolka_bronze  ──┐
                    ├──► normalized_region ──┐
naselenie_bronze  ──┤                        ├──► normalized_row_gate ──┐
                    └──► normalized_year  ───┘                          │
regions_bronze    ──┘                                                   │
                                                                        ▼
postgres_bronze ──► normalized_education ────────────────────────────────┤
                                                                         ▼
                            doshkolka_silver  ──────────────────┐       │
                            naselenie_silver  ──────────────┐   │       │
                            oo_silver        ──────────┐    │   │       │
                            spo_silver       ──────┐   │    │   │       │
                            vpo_silver       ──┐   │   │    │   │       │
                            dpo_silver       ──┤   │   │    │   │       │
                                               ▼   ▼   ▼    ▼   ▼       │
                            education_population_wide_silver ◄─┘        │
                            (читает из всех выше)                       │
                                               ▼                        │
                            education_population_wide_annual_silver ◄───┘
                            (развёртывает возрасты в годы 0-80)
                                               ▼
                            coverage_analysis (анализ охвата)

── Линия: Дефицит кадров ────────────────────────────────────────────────────────
discipuli_bronze ─────────────────────────────┐
                                               ▼
staff_shortage_sources_bronze ──► staff_shortage_silver ──► staff_shortage_dashboard

── Линия: Общежития ──────────────────────────────────────────────────────────────
dormitory_area_bronze ──┐
                         ├──► dormitory_silver ──► dormitory_dashboard
dormitory_dorm_bronze ──┤
                         │
discipuli_bronze ────────┘
```

Каждый asset после записи данных вызывает `scripts/refresh_duckdb.py` —
DuckDB-вьюхи обновляются автоматически, данные сразу доступны в DBeaver/SQL.

***

## Следующий этап: Gold-слой

После построения `education_population_wide_annual_silver` и прохождения анализа охвата `validate_coverage_analysis` строится Gold — аналитические агрегаты:

- Демографический профиль региона: численность населения по возрастным когортам
- Охват по уровням образования (дошкольное, школа, СПО, ВПО, ДПО): доля обучающихся на каждом уровне от населения соответствующего возраста
- Динамика год к году и региональные различия

Детальное ТЗ — отдельный документ.

***

## Документация и ТЗ

| Документ | Путь | Описание |
|----------|------|---------|
| Этот файл | `CLAUDE.md` | Архитектура, состояние, инструкции агенту |
| ТЗ: bronze_normalized | `docs/tz_bronze_normalized.md` | Нормализация региона и года — полное ТЗ |

***

## Источники данных

### Загружены (Excel)
- `data/Дошколка/YYYY/` — Excel, дети в ДОУ по регионам (2018–2024)
- `data/Население/Бюллетень_YYYY.xlsx` — Excel, численность населения (2018–2024)
- `data/regions.json` — справочник субъектов РФ с ISO-кодами и алиасами
- `data/education_level_lookup.csv` — справочник уровней образования
- `data/education_level_mapping.csv` — маппинг колонок PostgreSQL → уровни образования

### Загружены из PostgreSQL (etl_db, localhost:5432)
- `public.oo_1_2_7_2_211_v2`, `public.oo_1_2_7_1_209_v2`
- `public.спо_1_р2_101_43`, `public.впо_1_р2_13_54`, `public.пк_1_2_4_180`
- `public.discipuli` — численность обучающихся (для обоих дашбордов)
- `public.oo_1_3_4_230`, `public.oo_1_3_1_218`, `public.oo_1_3_2_221` — дашборд дефицита кадров
- `public.впо_2_р1_3_8`, `public.впо_2_р1_4_10` — дашборд общежитий
- Соединение: `SRC_DB` в `ingestion/postgres_loader.py`
- Добавить таблицу: дописать в список `TABLES` в `postgres_loader.py`

### Планируется
- Реестр лицензий — CSV или база, проектируется отдельно
- Другие источники — когда дойдём

***

## Структура проекта

```
/datalake
  ├── CLAUDE.md
  ├── docs/
  │   └── tz_bronze_normalized.md
  ├── data/
  │   ├── Дошколка/YYYY/*.xlsx
  │   ├── Население/Бюллетень_YYYY.xlsx
  │   ├── regions.json
  │   ├── education_level_lookup.csv     ← справочник уровней образования
  │   └── education_level_mapping.csv    ← маппинг колонок → уровни
  ├── catalog/
  │   ├── catalog.db                    ← SQLite Iceberg catalog
  │   └── warehouse/                    ← Parquet-файлы таблиц
  ├── ingestion/
  │   ├── setup_catalog.py              ← инициализация всех схем
  │   ├── excel_loader.py
  │   ├── json_loader.py                ← загрузчик regions.json (бывший regions_loader.py)
  │   ├── postgres_loader.py            ← загрузчик PostgreSQL → bronze
  │   └── normalization_config.yaml     ← конфиг источников для bronze_normalized
  ├── transformations/
  │   ├── bronze_normalized/
  │   │   ├── __init__.py
  │   │   ├── config_loader.py
  │   │   ├── region_normalizer.py
  │   │   ├── year_normalizer.py
  │   │   ├── region_pipeline.py
  │   │   └── year_pipeline.py
  │   ├── silver_doshkolka.py           ← читает из bronze_normalized
  │   ├── silver_naselenie.py           ← читает из bronze_normalized
  │   ├── staff_shortage_pipeline.py    ← триггеры дефицита кадров + score
  │   └── dormitory_pipeline.py         ← метрики общежитий + прогноз + sigma
  ├── dashboards/
  │   ├── staff_shortage_dashboard.py   ← HTML-генератор дашборда дефицита кадров
  │   └── dormitory_dashboard.py        ← HTML-генератор дашборда общежитий
  ├── validation/
  │   ├── validate_bronze_normalized.py ← отчёт по покрытию нормализации
  │   ├── validate_silver.py            ← отчёт по покрытию Silver (оба источника)
  │   ├── validate_silver_doshkolka.py  ← детальная матрица region×year для doshkolka
  │   ├── validate_staff_shortage.py    ← отчёт по silver.staff_shortage_triggers
  │   └── validate_dormitory.py         ← отчёт по silver.dormitory_infrastructure
  ├── dagster_project/
  │   ├── assets/
  │   │   ├── bronze_assets.py          ← 4 asset: doshkolka/naselenie/regions/postgres
  │   │   ├── bronze_normalized_assets.py ← 3 asset: region/year/validation
  │   │   ├── silver_assets.py          ← 3 asset: doshkolka/naselenie/validation
  │   │   ├── staff_shortage_assets.py  ← 4 asset: линия дефицита кадров
  │   │   └── dormitory_assets.py       ← 4 asset: линия общежитий
  │   └── definitions.py
  ├── scripts/
  │   ├── refresh_duckdb.py
  │   └── reset.py
  ├── reports/                          ← Markdown-отчёты (bronze_normalized, silver)
  ├── docker-compose.yml
  ├── datalake.duckdb
  └── requirements.txt
```
