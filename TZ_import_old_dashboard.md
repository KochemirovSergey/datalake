Теперь у меня полная картина. Пишу ТЗ. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/62641069/9a7afca7-92bd-4053-bb74-d344ef4e2ea4/bronze_assets.py?AWSAccessKeyId=ASIA2F3EMEYE37Y3FU3A&Signature=NZz%2FOljdJ3ok9WrZPc%2FW8cGgq4Y%3D&x-amz-security-token=IQoJb3JpZ2luX2VjEOP%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJIMEYCIQCg%2BWr33KFS%2BmOeapB5QttJh8BXkqgrFU9UlqWRfbndRQIhAIEAR0tKgeyhNxKWqpI%2B9hYWaaC1NPPrwsraizsC8vLeKvwECKv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEQARoMNjk5NzUzMzA5NzA1IgwoGm3VbnidXbC5ko0q0AT0cYbkLSoDbCTEb%2BL55s%2FeGaM3s1BV%2FRP9aXsWDGSksLcioOS4toJBRTkw1R4%2FvzHT%2FbNHCZwWmRMBiqI%2BRTxgQhYrRjNZva5OfUI27HA6Ogs79LtBPOIXRrmUdF5X0Cd8N41cFHLngHuKMZlHhnefCqaReRL9rNfDS0zG4C6C339OalDXTyg%2Fz8bsTUtG%2FGtfOtpQ7bTsPxjb1o3UPtd%2FrzMIj9WbyOZRxgHn65AlTLroej4%2BYHI%2B5UVxsXB6bbnN9TxSkZPaY2BcnDZBk7tC1pTSo3I%2FDYInUeK%2FX4pYQaXKrI7A4UAdN1rXAg1FLTMiMrl7ooo6QIk0E5%2F0GaJ9UdQ6l9TIqbV7DYibuf7mWsRpLimDpB%2FgVOFpxaTRsIKGmrFzyRMp1WOaCOE2xVK%2B1L6ZMx2QkUgFlBJtQ2E5BpEzkKX%2BZ6UKwSkdqQfl5LuviLixNobmBMQKkoq9%2F1cKgDqDpnTHwJ5GmTWr6t1eCPC1HRgP09msHBbLPthLj2cDkrveYsJxCV808SgB54ml2r%2FlFguUeCO%2Fxgw4E%2B1ZCVctPFNKN%2FIeD%2FafUtP%2BdusRs4BYVQl2PdPyIe8q%2BbSMSweORytN8Le6fgsUxCCWBfgsher0dUwDouLL9fIG29v7DWSPcrHColilYcMAfOhUNIRI7fDow3PaGvAbr%2BZEwATu2870lBmnOWZCBT3tMwYlZgrc7VwbpgvHO3c%2FqcJNIdQX7OoAkq7mmyMLH1xYnEXVNnPvTfZhgVxbJgD8Rm9eQQQgrwrtLr3lGFzSH7T7MO2r%2F84GOpcBbALyy9J1NrSgJAhYHa8NDTZdektAP0R1H%2B49EAjBa%2BnoSYsceo6j6o494%2FxRIuzyfxCto0DwdVZ0h4iH62NPfrvEkJLeHrJaI4hnDViCEzTHjyEpsl5OX%2B%2FiIkbFjQt16kmNKruGUpFK5gnsOtRJ8fNMdBqY%2Fz9arbSpL15LMJ%2FgCsZ3Z9vZSBNklaT960bPLDCy%2BgzUvw%3D%3D&Expires=1776280147)

***

# ТЗ: Перенос двух дашбордов в Dagster

**Дата:** апрель 2026  
**Статус:** черновик  
**Цель:** перенести логику `generate_heatmap.py` и `generate_dormitory_heatmap.py` в архитектуру Dagster + Iceberg + S3, создав две полностью независимые, не связанные между собой и не связанные с текущими пайплайнами аналитические линии.

***

## Принципы переноса

- Прямые SQL-запросы к PostgreSQL заменяются: данные сначала загружаются в Bronze (Iceberg), затем обрабатываются в трансформациях
- Весь Python-код расчётов переносится из скриптов в модули `transformations/`
- HTML-генераторы выносятся в `dashboards/`
- Каждая линия получает свой файл assets в `dagster_project/assets/`
- Каждый asset в конце вызывает `_refresh_views(context)` по текущему паттерну 
- HTML-дашборды загружаются в S3 через `S3Config` resource и `utils/s3_utils.py` 

***

## Линия 1: Дефицит кадров (`staff_shortage`)

### Источники данных (новые Bronze-таблицы)

Все 4 таблицы загружаются через `ingestion/postgres_loader.py` — нужно добавить их в список `TABLES`:

| PostgreSQL-таблица | Iceberg Bronze-таблица | Назначение |
|---|---|---|
| `public.discipuli` | `bronze.discipuli` | Численность обучающихся по регионам, уровням, годам |
| `public.oo_1_3_4_230` | `bronze.oo_1_3_4_230` | Форма 3.4 ОО — наличие ставок учителей |
| `public.oo_1_3_1_218` | `bronze.oo_1_3_1_218` | Форма 3.1 ОО — знаменатель триггера 1 и бонус |
| `public.oo_1_3_2_221` | `bronze.oo_1_3_2_221` | Форма 3.2 ОО — второй компонент бонуса |

### Расчётная логика (новый Silver-слой)

**Файл:** `transformations/staff_shortage_pipeline.py`  
Переносит Python-логику из `generate_heatmap.py` без изменения алгоритма. Читает из Bronze-таблиц Iceberg.

Функции:
- `load_enrollment(cat)` — читает `bronze.discipuli`, агрегирует `student_count` по `(region_id, year, level_id)`, фильтр: `Определение='Численность обучающихся'`, `Уровень субъектности='Регион'`, `гос/негос='сумма'`, уровни `1.2 / 1.3 / 1.4`
- `load_triggers(cat)` — читает из `bronze.oo_1_3_4_230` и `bronze.oo_1_3_1_218`, воспроизводит логику `SQL_TRIGGERS_VITRINA_HISTORICAL` как pandas-агрегации:
  - `trig1_val` для `1.2` = `r8_c4 / r8_c3` (из 3.4 / из 3.1), для `1.3+1.4` = `(r7_c4 − r8_c4) / (r7_c3 − r8_c3)`
  - `trig2_val` для `1.2` = `(r8_c3 − r8_c5) / r8_c3`, для `1.3+1.4` = аналогично через строку 7−строка 8
- `calc_bonus(cat)` — читает из `bronze.oo_1_3_1_218` и `bronze.oo_1_3_2_221`, воспроизводит логику `bonus_all` — взвешенная сумма долей по 13 колонкам с заданными весами (1, 2, 1, 2, 3, 5, 0.5, 0.75, 0.5, 3, 3, 3, 3 для 3.1; другие веса для 3.2)
- `calc_score(df)` — переносится без изменений: z-score по `(year, level)`, штраф только за положительные z, итог `score = max(0, 5 × (1 − Σσ / 3))`, диапазон 0–10
- `run(cat)` — запускает весь pipeline, пишет в `silver.staff_shortage_triggers`, возвращает `count`

**Схема таблицы** `silver.staff_shortage_triggers`:

```
region_code    STRING
year           INT
level          STRING   # "Учитель начальных классов" | "Учитель предметник"
student_count  INT
trig1_val      DOUBLE
trig2_val      DOUBLE
bonus_score    DOUBLE
score          DOUBLE   # 0–10
```

**Файл:** `dashboards/staff_shortage_dashboard.py`  
Переносит `generate_html()` и весь `HTML_TEMPLATE` из `generate_heatmap.py`.  
Читает из `silver.staff_shortage_triggers`. Принимает `df` и `region_json_path`, возвращает HTML-строку.  
Функция: `build_dashboard(df, regions_json_path, output_path)` — аналог текущего `coverage_dashboard.build_dashboard`. 

### Dagster Assets

**Файл:** `dagster_project/assets/staff_shortage_assets.py`

#### `discipuli_bronze`
```
group_name = "bronze_extraction"
deps = []
вызывает: ingestion/postgres_loader.run(tables=["discipuli"])
пишет в: bronze.discipuli
metadata: total_rows, breakdown
```

#### `staff_shortage_sources_bronze`
```
group_name = "bronze_extraction"
deps = []
вызывает: ingestion/postgres_loader.run(tables=["oo_1_3_4_230", "oo_1_3_1_218", "oo_1_3_2_221"])
пишет в: bronze.oo_1_3_4_230, bronze.oo_1_3_1_218, bronze.oo_1_3_2_221
metadata: total_rows, breakdown по таблицам
```

#### `staff_shortage_silver`
```
group_name = "staff_shortage"
deps = ["discipuli_bronze", "staff_shortage_sources_bronze"]
вызывает: transformations/staff_shortage_pipeline.run(cat)
пишет в: silver.staff_shortage_triggers
metadata:
  - total_rows: int
  - n_regions: int
  - n_years: int  
  - levels: text  (перечень уровней)
  - report_path: path  (Markdown-отчёт валидации)
```

#### `staff_shortage_dashboard`
```
group_name = "staff_shortage"
deps = ["staff_shortage_silver"]
resource: S3Config
вызывает:
  - dashboards/staff_shortage_dashboard.build_dashboard(df, ...)
  - utils/s3_utils.upload_html(...)
s3_key: "dashboards/staff-shortage/<date>/<date>_<run_id[:8]>.html"
metadata:
  - dashboard_url: url
  - dashboard_s3_key: text
  - n_regions: int
  - last_year: int
```

### Граф линии 1

```
discipuli_bronze ──────────────────────────┐
                                            ▼
staff_shortage_sources_bronze ──► staff_shortage_silver ──► staff_shortage_dashboard
```

***

## Линия 2: Инфраструктура ВПО — общежития (`dormitory`)

### Источники данных (новые Bronze-таблицы)

| PostgreSQL-таблица | Iceberg Bronze-таблица | Назначение |
|---|---|---|
| `public.впо_2_р1_3_8` | `bronze.vpo_2_r1_3_8` | Площади учебных зданий |
| `public.впо_2_р1_4_10` | `bronze.vpo_2_r1_4_10` | Нуждаемость в общежитиях и фактическое проживание |
| `public.discipuli` | `bronze.discipuli` | Вес региона на дашборде (уже создан в линии 1, если запущены обе) |

> `bronze.discipuli` — единственный общий Bronze-ресурс. Обе линии зависят от него независимо: каждая объявляет `deps=["discipuli_bronze"]`, но между собой их Silver/Dashboard-assets никак не связаны.

### Расчётная логика (новый Silver-слой)

**Файл:** `transformations/dormitory_pipeline.py`  
Переносит логику из `generate_dormitory_heatmap.py` без изменения алгоритма.

Функции:
- `load_area(cat)` — читает `bronze.vpo_2_r1_3_8`, пивотирует колонки: `area_total`, `area_need_repair`, `area_in_repair`, `area_emergency` через CASE-логику по `column_name`; группирует по `(регион, год)`; исключает `ru-fed`
- `load_dorm(cat)` — читает `bronze.vpo_2_r1_4_10`, суммирует `dorm_need` и `dorm_live` по всем 6 программам через CASE по `row_name`; группирует по `(регион, год)`; исключает `ru-fed`
- `calc_metrics(df)` — 4 метрики без изменений:
  - `metric_1 = area_need_repair / area_total`
  - `metric_2 = max(area_need_repair − area_in_repair, 0) / area_need_repair`
  - `metric_3 = max(dorm_need − dorm_live, 0) / dorm_need`
  - `metric_4 = area_emergency / area_total`
  - `dorm_shortage_abs = max(dorm_need − dorm_live, 0)`
- `build_forecast(hist_df)` — линейный тренд (`numpy.polyfit deg=1`) на `FORECAST_YEARS=5` лет для каждого региона по 6 абсолютным сериям; минимальная история `MIN_HISTORY=5` лет; `is_forecast=True` для прогнозных строк
- `calc_sigmas(df)` — для каждой из 4 метрик по каждому году: mean, std, z-score, `penalty=max(z,0)`; `sigma_sum = Σ(penalty)`; `alert_flag = 1 if sigma_sum >= 5.0`
- `run(cat)` — load → merge → hist forecast → metrics → sigmas → пишет в `silver.dormitory_infrastructure`, возвращает `(count, n_hist_years, n_fc_years)`

**Схема таблицы** `silver.dormitory_infrastructure`:

```
region_code          STRING
year                 INT
is_forecast          BOOLEAN
area_total           DOUBLE
area_need_repair     DOUBLE
area_in_repair       DOUBLE
area_emergency       DOUBLE
dorm_need            DOUBLE
dorm_live            DOUBLE
dorm_shortage_abs    DOUBLE
metric_1             DOUBLE
metric_2             DOUBLE
metric_3             DOUBLE
metric_4             DOUBLE
metric_1_mean        DOUBLE
metric_2_mean        DOUBLE
metric_3_mean        DOUBLE
metric_4_mean        DOUBLE
metric_1_sigma       DOUBLE
metric_2_sigma       DOUBLE
metric_3_sigma       DOUBLE
metric_4_sigma       DOUBLE
metric_1_penalty     DOUBLE
metric_2_penalty     DOUBLE
metric_3_penalty     DOUBLE
metric_4_penalty     DOUBLE
sigma_sum            DOUBLE
alert_flag           INT      # 0 | 1
```

**Файл:** `dashboards/dormitory_dashboard.py`  
Переносит `generate_html()` и `HTML_TEMPLATE` из `generate_dormitory_heatmap.py`.  
Читает из `silver.dormitory_infrastructure`. Для весов регионов читает `bronze.discipuli` (последний год, `student_count`).  
Функция: `build_dashboard(cat, regions_json_path, output_path)`.

### Dagster Assets

**Файл:** `dagster_project/assets/dormitory_assets.py`

#### `dormitory_area_bronze`
```
group_name = "bronze_extraction"
deps = []
вызывает: ingestion/postgres_loader.run(tables=["vpo_2_r1_3_8"])
пишет в: bronze.vpo_2_r1_3_8
metadata: total_rows, n_regions, year_min, year_max
```

#### `dormitory_dorm_bronze`
```
group_name = "bronze_extraction"
deps = []
вызывает: ingestion/postgres_loader.run(tables=["vpo_2_r1_4_10"])
пишет в: bronze.vpo_2_r1_4_10
metadata: total_rows, n_regions, year_min, year_max
```

#### `dormitory_silver`
```
group_name = "dormitory"
deps = ["dormitory_area_bronze", "dormitory_dorm_bronze", "discipuli_bronze"]
вызывает: transformations/dormitory_pipeline.run(cat)
пишет в: silver.dormitory_infrastructure
metadata:
  - total_rows: int
  - n_regions: int
  - hist_year_min / hist_year_max: int
  - forecast_year_max: int
  - n_alert_regions: int   # регионов с alert_flag=1 в последнем историческом году
  - report_path: path      # Markdown-отчёт валидации
```

#### `dormitory_dashboard`
```
group_name = "dormitory"
deps = ["dormitory_silver"]
resource: S3Config
вызывает:
  - dashboards/dormitory_dashboard.build_dashboard(cat, ...)
  - utils/s3_utils.upload_html(...)
s3_key: "dashboards/dormitory/<date>/<date>_<run_id[:8]>.html"
metadata:
  - dashboard_url: url
  - dashboard_s3_key: text
  - n_regions: int
  - n_hist_years: int
  - n_fc_years: int
```

### Граф линии 2

```
dormitory_area_bronze ──┐
                         ▼
dormitory_dorm_bronze ──► dormitory_silver ──► dormitory_dashboard
                         ▲
discipuli_bronze ────────┘
```

***

## Изменения в существующих файлах

### `ingestion/postgres_loader.py`
Добавить в `TABLES`:
```python
"discipuli",
"oo_1_3_4_230",
"oo_1_3_1_218",
"oo_1_3_2_221",
"vpo_2_r1_3_8",
"vpo_2_r1_4_10",
```

### `ingestion/setup_catalog.py`
Добавить создание схем для новых таблиц:
- `bronze.discipuli`
- `bronze.oo_1_3_4_230`, `bronze.oo_1_3_1_218`, `bronze.oo_1_3_2_221`
- `bronze.vpo_2_r1_3_8`, `bronze.vpo_2_r1_4_10`
- `silver.staff_shortage_triggers`
- `silver.dormitory_infrastructure`

### `dagster_project/definitions.py`
Добавить загрузку двух новых файлов assets:
```python
from dagster_project.assets.staff_shortage_assets import *
from dagster_project.assets.dormitory_assets import *
```

***

## Новые файлы проекта

```
transformations/
  staff_shortage_pipeline.py      ← расчёт триггеров + score
  dormitory_pipeline.py           ← расчёт метрик + прогноз + сигмы

dashboards/
  staff_shortage_dashboard.py     ← HTML-генератор дефицита кадров
  dormitory_dashboard.py          ← HTML-генератор общежитий

validation/
  validate_staff_shortage.py      ← Markdown-отчёт по silver.staff_shortage_triggers
  validate_dormitory.py           ← Markdown-отчёт по silver.dormitory_infrastructure

dagster_project/assets/
  staff_shortage_assets.py        ← 4 Dagster assets линии 1
  dormitory_assets.py             ← 4 Dagster assets линии 2
```

***

## Итоговый граф Dagster (только новые линии)

```
── Линия 1: Дефицит кадров ─────────────────────────────────────────────────

discipuli_bronze ─────────────────────────────┐
                                               ▼
staff_shortage_sources_bronze ──► staff_shortage_silver ──► staff_shortage_dashboard


── Линия 2: Общежития ──────────────────────────────────────────────────────

dormitory_area_bronze ──┐
                         ├──► dormitory_silver ──► dormitory_dashboard
dormitory_dorm_bronze ──┤
                         │
discipuli_bronze ────────┘
```

Между двумя новыми линиями единственная косвенная связь — `discipuli_bronze`, но на уровне Silver и Dashboard они полностью независимы и могут запускаться отдельно.