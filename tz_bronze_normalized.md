# Техническое задание: слой `bronze_normalized`

**Версия:** 1.0  
**Дата:** 2026-04-11  
**Статус:** Черновик, готов к реализации

---

## 1. Цель и контекст

В текущей архитектуре логика распознавания региона встроена локально в предметную трансформацию `silver_doshkolka`. Это неустойчиво: при добавлении новых источников каждая новая silver-трансформация вынуждена повторять аналогичную логику самостоятельно.

**Цель данного ТЗ** — создать ранний нормализационный подслой внутри bronze, который централизованно определяет регион и год для всех входных данных, прежде чем они попадут в предметные silver-трансформации. Это обеспечивает единообразие, explainability и чистоту lineage.

---

## 2. Место в архитектуре

```
Источники (Excel, реестр лицензий, другие)
    ↓
bronze.exceltables / bronze.licenses / ...    ← уже есть
    ↓
bronze_normalized.region                      ← создаётся в рамках данного ТЗ
bronze_normalized.region_error
    ↓
bronze_normalized.year                        ← создаётся в рамках данного ТЗ
bronze_normalized.year_error
    ↓
silver.doshkolka / silver.naselenie / ...     ← уже есть, будут переработаны
```

`bronze_normalized` — это **следующий слой после сырого bronze**. Silver-трансформации больше не ищут регион и год самостоятельно, а читают уже нормализованные поля.

---

## 3. Scope

### В рамках данного ТЗ

- Нормализация региона: `bronze_normalized.region` + `bronze_normalized.region_error`
- Нормализация года: `bronze_normalized.year` + `bronze_normalized.year_error`
- Конфигурационный файл `normalization_config.yaml` (заполняется человеком)
- Dagster assets для обоих этапов
- Валидационный отчёт: покрытие по регионам, по годам, матрица пересечения

### За рамками данного ТЗ (следующие итерации)

- Другие типы нормализации (тип учреждения, территориальный уровень и т.д.)
- Механизм ручного override quarantine-записей
- Автоматическое исправление нераспознанных значений
- Переработка существующих silver-трансформаций под новый слой (отдельное ТЗ)

---

## 4. Конфигурация: `normalization_config.yaml`

Человек заполняет этот файл один раз для каждого источника. Алгоритм читает его перед обработкой и выбирает соответствующий пайплайн.

### Структура

```yaml
sources:
  - source_id: "doshkolka"          # уникальный идентификатор источника
    description: "Данные по дошкольному образованию"
    
    region:
      location: "row"               # где искать регион: row | sheet | file | folder | meta_sheet | license_subject+address
      row_column: "col0"            # если location=row: колонка с названием региона
      sheet_hint: null              # если location=sheet: паттерн в названии листа
      file_hint: null               # если location=file: паттерн в имени файла
      folder_hint: null             # если location=folder: паттерн в имени папки
    
    year:
      location: "file"              # где искать год: row | header | sheet | file | folder
      file_pattern: "data(\\d{4})"  # regex для извлечения года из имени файла
      date_field: null              # если location=row и там дата: название поля
      year_type: "period"           # тип года: period | exact_date

  - source_id: "licenses"
    description: "Реестр лицензий"
    
    region:
      location: "license_subject+address"
      subject_field: "subject_rf"
      address_field: "address"
    
    year:
      location: "row"
      date_field: "license_date"    # точная дата; год извлекается из неё
      year_type: "exact_date"
```

### Правило по умолчанию

Если источник не найден в конфиге, система выдаёт ошибку конфигурации и не обрабатывает данные молча.

---

## 5. Нормализация региона

### 5.1. Эталонный справочник

- Используется `bronze.regionlookup`, который уже загружен в БД.
- Справочник содержит варианты написания регионов (`name_variant`) и их коды (`region_code`) в формате ISO-подобного кода `RU-XXX`.
- Алгоритм нормализации названия (приведение к нижнему регистру, замена ё→е, удаление лишних пробелов и знаков препинания) уже реализован в `silver_doshkolka.py` — он переносится без изменений в общую библиотеку.
- Дополнительные alias-ы, которых нет в `regionlookup`, фиксируются в отдельном словаре внутри библиотеки и при необходимости пополняются.

### 5.2. Пайплайн A: регион в строке (`location: row`)

1. Читается колонка, указанная в конфиге (`row_column`).
2. Значение нормализуется через `normalize_region_name()`.
3. Производится поиск в индексе `regionlookup`.
4. Если найден — строка получает `region_code` и статус `ok`.
5. Если не найден — строка записывается в `bronze_normalized.region_error` со статусом `unmatched`.
6. Строки со статусом `ok` идут дальше в `bronze_normalized.region`.

### 5.3. Пайплайн B: регион на уровне документа (`location: sheet | file | folder | meta_sheet`)

1. Извлекается название листа / файла / папки согласно конфигу.
2. Производится поиск региона в этом названии через `normalize_region_name()` и `regionlookup`.
3. Если регион найден — он становится `region_code_doc` для документа.
4. Все строки этого документа наследуют `region_code_doc`.
5. Если регион не найден на уровне документа — **все строки документа** уходят в `bronze_normalized.region_error` со статусом `doc_unmatched`.

### 5.4. Пайплайн C: реестр лицензий (`location: license_subject+address`)

1. Читается поле `subject_rf`.
2. Если оно не пустое — используется как `region_raw`, нормализуется, ищется в справочнике.
3. Если пустое — берётся поле `address`, из него извлекается субъект РФ (первая значимая часть адреса до запятой или через паттерн).
4. Результат нормализуется и ищется в справочнике.
5. Если не найден — запись уходит в `region_error`.

### 5.5. Схема таблицы `bronze_normalized.region`

| Поле | Тип | Обязательное | Описание |
|------|-----|:---:|---------|
| `row_id` | string | ✅ | Уникальный идентификатор строки из bronze-источника |
| `source_id` | string | ✅ | Идентификатор источника из конфига |
| `region_raw` | string | ✅ | Исходное сырое значение, из которого извлечён регион |
| `region_code` | string | ✅ | Нормализованный код региона (`RU-MOW` и т.д.) |
| `resolution_scope` | string | ✅ | Откуда взят регион: `row` / `sheet` / `file` / `folder` / `meta_sheet` / `subject_rf` / `address` |
| `normalized_key` | string | ✅ | Промежуточный ключ после нормализации текста |

### 5.6. Схема таблицы `bronze_normalized.region_error`

| Поле | Тип | Описание |
|------|-----|---------|
| `row_id` | string | Идентификатор строки |
| `source_id` | string | Источник |
| `region_raw` | string | Сырое значение |
| `error_type` | string | `unmatched` / `doc_unmatched` / `empty` / `ambiguous` |
| `resolution_scope` | string | Откуда пытались взять регион |
| `normalized_key` | string | Промежуточный ключ |

---

## 6. Нормализация года

### 6.1. Правила определения типа

- `year_type: period` — год обозначает отчётный период (статистические данные). Хранится только `year` (INT).
- `year_type: exact_date` — год извлекается из точной даты (реестр лицензий). Хранятся `date_raw` (STRING) и `year` (INT).

### 6.2. Пайплайны поиска года

**Пайплайн A: год из файла или папки**

1. Применяется regex-паттерн из конфига (`file_pattern` / `folder_pattern`) к имени файла или папки.
2. Извлекается 4-значный год.
3. Если извлечь не удалось — все строки источника уходят в `year_error`.

**Пайплайн B: год из строки**

1. Читается поле, указанное в конфиге (`date_field` или `year_field`).
2. Если `year_type: exact_date` — из даты парсится год.
3. Если `year_type: period` — значение очищается и преобразуется в INT.
4. Если не удалось — строка уходит в `year_error`.

**Пайплайн C: год из названия листа или заголовка**

1. Применяется regex `\b(20\d{2})\b` к названию листа или содержимому шапки таблицы.
2. Берётся первый найденный 4-значный год из диапазона 2000–2030.
3. Если не найден — строки уходят в `year_error`.

### 6.3. Условие перехода дальше

Строка идёт в `bronze_normalized.year` только если год успешно извлечён.  
Строки с нераспознанным годом уходят в `bronze_normalized.year_error`.  
**Строки, не прошедшие хотя бы один из двух этапов (регион или год), не попадают в silver-слои.**

### 6.4. Схема таблицы `bronze_normalized.year`

| Поле | Тип | Обязательное | Описание |
|------|-----|:---:|---------|
| `row_id` | string | ✅ | Идентификатор строки |
| `source_id` | string | ✅ | Источник |
| `year` | int | ✅ | Извлечённый год |
| `year_type` | string | ✅ | `period` / `exact_date` |
| `date_raw` | string | — | Исходная строка даты (только если `year_type = exact_date`) |
| `year_raw` | string | — | Исходное значение (если год был отдельным полем) |
| `resolution_scope` | string | ✅ | Откуда взят год: `row` / `header` / `sheet` / `file` / `folder` |

### 6.5. Схема таблицы `bronze_normalized.year_error`

| Поле | Тип | Описание |
|------|-----|---------|
| `row_id` | string | Идентификатор строки |
| `source_id` | string | Источник |
| `year_raw` | string | Сырое значение |
| `error_type` | string | `unparseable` / `out_of_range` / `empty` / `no_pattern_match` |
| `resolution_scope` | string | Откуда пытались взять год |

---

## 7. Результирующий набор данных

По завершении обоих этапов строки, успешно прошедшие оба, имеют:

- `row_id` — ключ для JOIN с `bronze_normalized.region` и `bronze_normalized.year`
- `region_code` — нормализованный код региона
- `year` — нормализованный год

Эти поля являются обязательными входными для всех последующих silver-трансформаций.

---

## 8. Валидационный отчёт

Валидация запускается **после** обоих этапов нормализации. Отчёт содержит три блока.

### 8.1. Блок 1 — Покрытие по регионам

- Полный список регионов из эталонного `bronze.regionlookup` (89 регионов + федеральные города).
- Для каждого региона: количество строк в `bronze_normalized.region` с данным `region_code`.
- Регионы, по которым данных нет — явно выводятся как `0`.
- Итог: `% регионов из справочника, представленных в данных`.

### 8.2. Блок 2 — Покрытие по годам

- Список всех распознанных годов.
- Для каждого года: количество строк.
- Отдельно: количество строк с `year_type = period` и `year_type = exact_date`.
- Итог: `% строк с распознанным годом`.

### 8.3. Блок 3 — Матрица пересечения `region × year`

Таблица вида:

| region_code | region_name | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|-------------|-------------|------|------|------|------|------|------|------|
| RU-MOW      | Москва      | 142  | 138  | 141  | ...  | ...  | ...  | ...  |
| RU-SPE      | Санкт-Петербург | 89 | 91 | ... | ... | ... | ... | ... |
| ...         | ...         | ...  | ...  | ...  | ...  | ...  | ...  | ...  |
| **ИТОГО**   |             | N    | N    | N    | N    | N    | N    | N    |

Регионы, у которых `0` по всем годам — помечаются как `ОТСУТСТВУЕТ В ДАННЫХ`.

### 8.4. Блок 4 — Error summary

| Тип ошибки | Количество строк | % от total |
|-----------|-----------------|-----------|
| region: unmatched | N | X% |
| region: doc_unmatched | N | X% |
| year: unparseable | N | X% |
| year: no_pattern_match | N | X% |
| **Итого в quarantine** | **N** | **X%** |

Top-20 нераспознанных `region_raw` (самые частые).

---

## 9. Структура файлов и кода

```
ingestion/
  normalization_config.yaml           ← заполняется человеком
transformations/
  bronze_normalized/
    __init__.py
    region_normalizer.py              ← логика нормализации и lookup региона
    year_normalizer.py                ← логика извлечения и нормализации года
    config_loader.py                  ← чтение normalization_config.yaml
    region_pipeline.py                ← Dagster asset: bronze_normalized_region
    year_pipeline.py                  ← Dagster asset: bronze_normalized_year
validation/
  validate_bronze_normalized.py       ← валидационный отчёт
  reports/
    bronze_normalized_YYYY-MM-DD.md   ← артефакт отчёта
```

### Переиспользование существующего кода

Функции `normalize_region_name()`, `build_region_index()`, `lookup_region()` из `silver_doshkolka.py` **переносятся в `region_normalizer.py` без изменений** и используются оттуда. В `silver_doshkolka.py` остаётся только ссылка на библиотеку.

---

## 10. Dagster assets

### `bronze_normalized_region`

```
deps: [excelbronze, regionsbronze]
group: bronze_normalized
```

Шаги:
1. Загрузить `normalization_config.yaml`.
2. Загрузить `bronze.exceltables` и другие источники.
3. Построить индекс по `bronze.regionlookup`.
4. Для каждого источника: определить пайплайн по конфигу.
5. Прогнать строки через соответствующий пайплайн.
6. Записать `ok`-записи в `bronze_normalized.region`.
7. Записать `error`-записи в `bronze_normalized.region_error`.
8. Вернуть метаданные: `total`, `ok_count`, `error_count`.

### `bronze_normalized_year`

```
deps: [excelbronze, bronze_normalized_region]
group: bronze_normalized
```

Шаги:
1. Загрузить конфиг.
2. Для каждого источника: определить пайплайн по конфигу.
3. Прогнать строки через соответствующий пайплайн.
4. Записать `ok`-записи в `bronze_normalized.year`.
5. Записать `error`-записи в `bronze_normalized.year_error`.
6. Вернуть метаданные: `total`, `ok_count`, `error_count`.

### `bronze_normalized_validation`

```
deps: [bronze_normalized_region, bronze_normalized_year]
group: bronze_normalized
```

Генерирует Markdown-отчёт и сохраняет его в `reports/`.

---

## 11. Iceberg-таблицы: инициализация

Добавить в `ingestion/setup_catalog.py` создание таблиц:

- `bronze_normalized.region`
- `bronze_normalized.region_error`
- `bronze_normalized.year`
- `bronze_normalized.year_error`

Схемы соответствуют разделам 5.5, 5.6, 6.4, 6.5 настоящего ТЗ.

---

## 12. Критерии готовности (Definition of Done)

- [ ] `normalization_config.yaml` заполнен для всех текущих источников (`doshkolka`, `naselenie`, `licenses`)
- [ ] `bronze_normalized.region` содержит записи по всем source_id
- [ ] `bronze_normalized.year` содержит записи по всем source_id
- [ ] Error-таблицы существуют и заполняются при наличии нераспознанных значений
- [ ] Валидационный отчёт генерируется и содержит все три блока
- [ ] Матрица `region × year` не пустая, охватывает минимум 70% регионов из справочника
- [ ] Существующий `silver_doshkolka.py` использует функцию `normalize_region_name()` из `region_normalizer.py`, а не дублирует её
- [ ] Все assets зарегистрированы в `dagster_project/definitions.py`
- [ ] `scripts/refresh_duckdb.py` обновлён с учётом новых таблиц

---

## 13. Открытые вопросы (вне scope, для следующих итераций)

1. **Override-механизм:** как фиксировать ручные исправления quarantine-записей, чтобы они применялись при следующем rebuild.
2. **Переработка silver-трансформаций:** когда `bronze_normalized` будет готов, нужно отдельное ТЗ на переход `silver_doshkolka` и `silver_naselenie` на новые поля.
3. **Реестр лицензий:** отдельное ТЗ на ingestion и подключение к `bronze_normalized`.
4. **Другие нормализации:** тип учреждения, административный уровень и т.д.

