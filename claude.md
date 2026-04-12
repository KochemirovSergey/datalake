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
  - `bronze.oo_1_2_7_2_211` ← `public.oo_1_2_7_2_211`
  - `bronze.oo_1_2_7_1_209` ← `public.oo_1_2_7_1_209`
  - `bronze.спо_1_р2_101_43` ← `public.спо_1_р2_101_43`
  - `bronze.впо_1_р2_13_54` ← `public.впо_1_р2_13_54`
  - `bronze.пк_1_2_4_180` ← `public.пк_1_2_4_180`

**Bronze Normalized — реализован:**
- `bronze_normalized.region` — нормализованный регион по каждой строке
- `bronze_normalized.region_error` — строки, для которых регион не распознан
- `bronze_normalized.year` — нормализованный год по каждой строке
- `bronze_normalized.year_error` — строки, для которых год не распознан

**Silver — реализован, читает из bronze_normalized:**
- `silver.doshkolka` — 15 993 строки: регион × год × территория × возрастная группа × значение
- `silver.naselenie` — регион × год × возраст (0–79, 80+) × 9 числовых показателей (пол × тип территории)

### Скрипты

- `scripts/refresh_duckdb.py` — пересоздаёт DuckDB-вьюхи из Iceberg снапшотов
- `scripts/reset.py` — очистка таблиц (интерактивное меню или CLI-аргумент)
- `ingestion/setup_catalog.py` — инициализация каталога и схем (при пересоздании)
- `ingestion/excel_loader.py` — загрузчик Excel (`flat=True` для файлов с годом в имени)
- `ingestion/json_loader.py` — загрузчик регионального справочника из JSON (бывший `regions_loader.py`)
- `ingestion/postgres_loader.py` — загрузчик таблиц из PostgreSQL в Bronze (все значения → string)
- `transformations/silver_doshkolka.py` — Bronze → Silver (дошкольники), читает из bronze_normalized
- `transformations/silver_naselenie.py` — Bronze → Silver (население), читает из bronze_normalized
- `validation/validate_silver.py` — генерирует Markdown-отчёт покрытия для Silver-слоя
- `validation/validate_silver_doshkolka.py` — детальная матрица region × year для doshkolka

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
  `oo_1_2_7_2_211`, `oo_1_2_7_1_209`, `спо_1_р2_101_43`, `впо_1_р2_13_54`, `пк_1_2_4_180`
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

#### `normalized_validation`
- **Зависимости:** `normalized_region`, `normalized_year`
- **Вызывает:** `validation/validate_bronze_normalized.run(cat)`
- **Генерирует:** Markdown-отчёт в `reports/bronze_normalized_YYYY-MM-DD.md`
- **Содержимое отчёта:**
  1. Покрытие по регионам — сколько из 89 канонических регионов представлено, строк на регион
  2. Покрытие по годам — какие годы распознаны, сколько строк по каждому
  3. Матрица `region × year` — ключевая таблица; пустые ячейки = нет данных
  4. Error summary — Top-20 нераспознанных `region_raw`, распределение по типам ошибок
- **Статистика в UI:**
  - `report_path` — путь к сохранённому отчёту
- **Назначение:** человек смотрит матрицу и решает, достаточно ли покрытие для перехода к Silver

---

### Группа `silver`

#### `doshkolka_silver`
- **Зависимости:** `normalized_validation`
- **Вызывает:** `transformations/silver_doshkolka.run()`
- **Читает из:** `bronze_normalized.region` + `bronze_normalized.year` (пересечение по `row_id`),
  затем джойнит с `bronze.excel_tables` для получения числовых значений
- **Пишет в:** `silver.doshkolka`
- **Логика:** unpivot — одна строка = регион × год × территория (total/urban/rural) × возраст
- **Статистика в UI:**
  - `total_rows` — количество записей в Silver (≈15 993)

#### `naselenie_silver`
- **Зависимости:** `normalized_validation`
- **Вызывает:** `transformations/silver_naselenie.run()`
- **Читает из:** `bronze_normalized.region` + `bronze_normalized.year`,
  джойнит с `bronze.excel_tables`
- **Пишет в:** `silver.naselenie`
- **Логика:** каждая строка = регион × год × возраст (0–79, 80+) × 9 показателей
- **Статистика в UI:**
  - `total_rows` — количество записей в Silver

#### `silver_validation`
- **Зависимости:** `doshkolka_silver`, `naselenie_silver`
- **Вызывает:** `validation/validate_silver.run(cat)`
- **Генерирует:** Markdown-отчёт в `reports/silver_YYYY-MM-DD.md`
- **Содержимое отчёта:**
  - §1 `silver.doshkolka` — покрытие регионов, годов, матрица `region × year`
  - §2 `silver.naselenie` — покрытие регионов, годов, матрица `region × year`
- **Статистика в UI:**
  - `report_path` — путь к отчёту
- **Назначение:** финальная проверка перед Gold-слоем

---

### Порядок выполнения (полный пайплайн)

```
doshkolka_bronze  ──┐
                    ├──► normalized_region ──┐
naselenie_bronze  ──┤                        ├──► normalized_validation ──┐
                    └──► normalized_year  ───┘                            │
regions_bronze    ──┘                                                     │
                                                                          ▼
postgres_bronze  (независимо)                              doshkolka_silver ──┐
                                                           naselenie_silver ──┴──► silver_validation
```

Каждый asset после записи данных вызывает `scripts/refresh_duckdb.py` —
DuckDB-вьюхи обновляются автоматически, данные сразу доступны в DBeaver/SQL.

***

## Следующий этап: Gold-слой

После прохождения `silver_validation` без ошибок строится Gold — аналитические агрегаты:

- Демографический профиль региона: численность населения по возрастным когортам
- Охват дошкольным образованием: отношение `silver.doshkolka` к `silver.naselenie`
- Динамика год к году

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

### Загружены из PostgreSQL (etl_db, localhost:5432)
- `public.oo_1_2_7_2_211`, `public.oo_1_2_7_1_209`
- `public.спо_1_р2_101_43`, `public.впо_1_р2_13_54`, `public.пк_1_2_4_180`
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
  │   └── regions.json
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
  │   └── silver_naselenie.py           ← читает из bronze_normalized
  ├── validation/
  │   ├── validate_bronze_normalized.py ← отчёт по покрытию нормализации
  │   ├── validate_silver.py            ← отчёт по покрытию Silver (оба источника)
  │   └── validate_silver_doshkolka.py  ← детальная матрица region×year для doshkolka
  ├── dagster_project/
  │   ├── assets/
  │   │   ├── bronze_assets.py          ← 4 asset: doshkolka/naselenie/regions/postgres
  │   │   ├── bronze_normalized_assets.py ← 3 asset: region/year/validation
  │   │   └── silver_assets.py          ← 3 asset: doshkolka/naselenie/validation
  │   └── definitions.py
  ├── scripts/
  │   ├── refresh_duckdb.py
  │   └── reset.py
  ├── reports/                          ← Markdown-отчёты (bronze_normalized, silver)
  ├── docker-compose.yml
  ├── datalake.duckdb
  └── requirements.txt
```
