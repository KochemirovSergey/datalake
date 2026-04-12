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

### Данные — загружены

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

**Bronze Normalized** ← следующий этап (ТЗ: `docs/tz_bronze_normalized.md`):
- `bronze_normalized.region` — нормализованный регион по каждой строке
- `bronze_normalized.region_error` — строки, для которых регион не распознан
- `bronze_normalized.year` — нормализованный год по каждой строке
- `bronze_normalized.year_error` — строки, для которых год не распознан

**Silver:**
- `silver.doshkolka` — 15 993 строки: регион × год × территория × возрастная группа × значение
- `silver.naselenie` — регион × год × возраст (0–79, 80+) × 9 числовых показателей (пол × тип территории)

### Dagster assets

```
bronze:            excel_bronze            (data/Дошколка/ → bronze.excel_tables)
                   population_bronze       (data/Население/ → bronze.excel_tables)
                   regions_bronze          (data/regions.json → bronze.region_lookup)
                   postgres_bronze         (PostgreSQL etl_db → bronze.oo_*/спо_*/впо_*/пк_*)

bronze_normalized: normalized_region       (deps: excel_bronze, regions_bronze)     ← планируется
                   normalized_year         (deps: excel_bronze, normalized_region)   ← планируется
                   normalized_validation   (deps: normalized_region, normalized_year) ← планируется

silver:            doshkolka_silver        (deps: excel_bronze, regions_bronze)
                   naselenie_silver        (deps: population_bronze, regions_bronze)
```

### Скрипты

- `scripts/refresh_duckdb.py` — пересоздаёт DuckDB-вьюхи из Iceberg снапшотов
- `scripts/reset.py` — очистка таблиц (интерактивное меню или CLI-аргумент)
- `ingestion/setup_catalog.py` — инициализация каталога и схем (при пересоздании)
- `ingestion/excel_loader.py` — загрузчик Excel (`flat=True` для файлов с годом в имени)
- `ingestion/json_loader.py` — загрузчик регионального справочника из JSON (бывший `regions_loader.py`)
- `ingestion/postgres_loader.py` — загрузчик таблиц из PostgreSQL в Bronze (все значения → string)
- `transformations/silver_doshkolka.py` — Bronze → Silver (дошкольники)
- `transformations/silver_naselenie.py` — Bronze → Silver (население)

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

## Следующий этап: bronze_normalized

### Зачем

Логика распознавания региона сейчас встроена локально в `silver_doshkolka.py`.
Это неустойчиво: при добавлении новых источников каждая silver-трансформация
вынуждена повторять аналогичную логику самостоятельно.

Нужен ранний нормализационный подслой внутри bronze, который централизованно
определяет **регион** и **год** для всех входных данных, прежде чем они попадают
в предметные silver-трансформации.

**Полное ТЗ:** `docs/tz_bronze_normalized.md`

### Ключевые решения

- Два независимых Dagster asset: `normalized_region` и `normalized_year`
- Объект нормализации — **гибридный**: регион/год могут быть на уровне строки,
  листа, файла или папки; контекст наследуется от документа к строке
- Эталонный справочник региона — `bronze.region_lookup` (уже в БД)
- Строки, у которых не распознан **хотя бы один** из двух признаков (регион или год),
  уходят в error/quarantine и **не попадают** в silver-слои
- Человек задаёт конфигурацию источников в `ingestion/normalization_config.yaml`

### Два пайплайна нормализации региона

**row-based** — регион в строках таблицы:
1. Читать колонку из конфига
2. Нормализовать название через `normalize_region_name()`
3. Искать в `region_lookup`
4. ok → `bronze_normalized.region`, не найден → `bronze_normalized.region_error`

**document-based** — регион в названии листа / файла / папки:
1. Извлечь название из конфига (паттерн или прямое поле)
2. Нормализовать и найти регион
3. ok → наследовать всем строкам документа
4. не найден → все строки документа → `bronze_normalized.region_error`

Для реестра лицензий: сначала поле `subject_rf`, если пусто — парсить `address`.

### Нормализация года

- `year_type: period` — год как отчётный период (статистика), хранить только `year`
- `year_type: exact_date` — точная дата (реестр лицензий), хранить `date_raw` + `year`
- Источники для поиска: поле строки, заголовок, название листа, имя файла, папка

### Валидация покрытия (выход этапа)

Dagster asset `normalized_validation` генерирует отчёт:

1. **Покрытие по регионам** — сколько регионов из `bronze.region_lookup` представлено в данных,
   сколько строк по каждому региону
2. **Покрытие по годам** — какие годы распознаны, сколько строк по каждому году
3. **Матрица region × year** — ключевая таблица: по каким регионам и годам реально есть данные;
   регионы без данных помечаются явно
4. **Error summary** — Top-20 нераспознанных `region_raw`, распределение по типам ошибок

**Это и есть валидация нормализации.** Человек смотрит отчёт и решает, достаточно ли покрытие
для перехода к следующему слою.

### Порядок реализации

1. Создать `ingestion/normalization_config.yaml` для источников `doshkolka`, `naselenie`
2. Добавить таблицы в `ingestion/setup_catalog.py` (4 новые Iceberg-таблицы)
3. Реализовать `transformations/bronze_normalized/region_normalizer.py`
   (перенести `normalize_region_name()` из `silver_doshkolka.py`)
4. Реализовать `transformations/bronze_normalized/year_normalizer.py`
5. Создать Dagster assets в `dagster_project/assets/bronze_normalized_assets.py`
6. Добавить assets в `dagster_project/definitions.py`
7. Обновить `scripts/refresh_duckdb.py` — добавить вьюхи для новых таблиц
8. Запустить, изучить матрицу покрытия, убедиться в полноте распознавания

***

## Этап после bronze_normalized: Silver → переработка

После того как `bronze_normalized` готов и матрица покрытия выглядит корректно:

- `silver_doshkolka.py` и `silver_naselenie.py` переключаются на чтение
  нормализованных полей из `bronze_normalized` вместо самостоятельного поиска региона/года
- Логика `normalize_region_name()` удаляется из `silver_doshkolka.py` — она уже живёт
  в `region_normalizer.py`
- Это отдельное ТЗ

***

## Этап после Silver: система валидации Silver

После переработки silver-трансформаций:

### Что проверяем

**Блок 1 — Полнота (completeness):**
- Все 85 субъектов РФ присутствуют в каждом году (2018–2024) — для обоих Silver
- Нет NULL в `total_both` (ключевой показатель)

**Блок 2 — Арифметическая консистентность:**
- `total_both = total_male + total_female` (допуск ±1 на округление)
- `total_both ≈ urban_both + rural_both`
- В `silver.doshkolka`: сумма `age_0..age_7plus` ≤ `total`

**Блок 3 — Динамика (trend checks):**
- Год к году изменение по региону > 15% → WARNING
- Значение = 0 там где не должно быть нуля → WARNING

**Блок 4 — Кросс-источниковая согласованность:**
- `silver.doshkolka.value` (age_0..age_6) ≤ `silver.naselenie.total_both` (тот же возраст и регион)
- Нарушение → ERROR

### Dagster asset `silver_validation`

```python
@asset(group_name="validation", deps=["doshkolka_silver", "naselenie_silver"])
def silver_validation(context): ...
```

Если есть ERROR — завершается с исключением, Gold-assets заблокированы.

**Правило:** Gold не строится пока `silver_validation` не прошёл без ERROR.

***

## Документация и ТЗ

| Документ | Путь | Описание |
|----------|------|---------|
| Этот файл | `CLAUDE.md` | Архитектура, состояние, инструкции агенту |
| ТЗ: bronze_normalized | `docs/tz_bronze_normalized.md` | Нормализация региона и года — полное ТЗ |

***

## Источники данных

### Загружены
- `data/Дошколка/YYYY/` — Excel, дети в ДОУ по регионам (2018–2024)
- `data/Население/Бюллетень_YYYY.xlsx` — Excel, численность населения (2018–2024)
- `data/regions.json` — справочник субъектов РФ с ISO-кодами и алиасами

### Загружены из PostgreSQL (etl_db, localhost:5432)
- `public.oo_1_2_7_2_211`, `public.oo_1_2_7_1_209` — первые два источника
- `public.спо_1_р2_101_43`, `public.впо_1_р2_13_54`, `public.пк_1_2_4_180`
- Соединение: `SRC_DB` в `ingestion/postgres_loader.py`

### Планируется
- Реестр лицензий — CSV или база, проектируется отдельно
- Другие источники — когда дойдём

***

## Структура проекта

```
/datalake
  ├── CLAUDE.md
  ├── docs/
  │   └── tz_bronze_normalized.md       ← ТЗ на bronze_normalized
  ├── data/
  │   ├── Дошколка/YYYY/*.xlsx
  │   ├── Население/Бюллетень_YYYY.xlsx
  │   └── regions.json
  ├── catalog/
  │   ├── catalog.db                    ← SQLite Iceberg catalog
  │   └── warehouse/                    ← Parquet-файлы таблиц
  ├── ingestion/
  │   ├── setup_catalog.py
  │   ├── excel_loader.py
  │   ├── json_loader.py                ← загрузчик regions.json (бывший regions_loader.py)
  │   ├── postgres_loader.py            ← загрузчик PostgreSQL → bronze
  │   └── normalization_config.yaml     ← конфиг bronze_normalized (создать)
  ├── transformations/
  │   ├── bronze_normalized/            ← создать в рамках ТЗ
  │   │   ├── __init__.py
  │   │   ├── config_loader.py
  │   │   ├── region_normalizer.py
  │   │   ├── year_normalizer.py
  │   │   ├── region_pipeline.py
  │   │   └── year_pipeline.py
  │   ├── silver_doshkolka.py
  │   └── silver_naselenie.py
  ├── validation/
  │   ├── validate_silver.py            ← этап после Silver
  │   └── report.py
  ├── dagster_project/
  │   ├── assets/
  │   │   ├── bronze_assets.py
  │   │   ├── bronze_normalized_assets.py  ← создать
  │   │   └── silver_assets.py
  │   └── definitions.py
  ├── scripts/
  │   ├── refresh_duckdb.py
  │   └── reset.py
  ├── reports/                          ← выходные отчёты
  ├── docker-compose.yml
  ├── datalake.duckdb
  └── requirements.txt
```