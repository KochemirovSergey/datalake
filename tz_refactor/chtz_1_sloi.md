# ЧТЗ Этап 1: Слой Извлечения (Extraction)

## 1. Задача
Рефакторинг текущих механизмов загрузки из папки `ingestion/` и их интеграция в Dagster на уровне `1_extract`.

## 2. Исходное состояние
- Файлы `excel_loader.py`, `postgres_loader.py`, `json_loader.py` содержат ручную запись в БД через `to_sql`.
- Логика разбросана, ассеты в `bronze_assets.py` вызывают лоадеры как побочный эффект (side-effect).

## 3. Целевое состояние
- Все `*_loader.py` переименованы в `*_extractor.py`.
- Функции извлечения теперь **только читают** данные и возвращают `pd.DataFrame`.
- Все ассеты первого уровня зарегистрированы в `dagster_project/assets/1_extraction_assets.py`.

## 4. Требования к реализации
### 4.1. Технические колонки (Lineage)
Каждый извлеченный DataFrame ДОЛЖЕН содержать:
- `_etl_loaded_at`: timestamp загрузки.
- `_source_file`: имя исходного файла (для Excel).
- `_sheet_name`: имя листа (для Excel).
- `_row_number`: порядковый номер строки в источнике.

### 4.2. Группировка узлов (согласно edu_pipeline.py)
Создать следующие ассеты в группе `1_excel`:
- `obuch_doshkolka`
- `naselenie`

Создать следующие ассеты в группе `1_postgres`:
- `obuch_oo`
- `obuch_vpo`
- `obuch_spo`
- `obuch_pk`
- `obshagi_vpo`
- `ped_oo`

Создать узлы справочников:
- `regions` (из `data/regions.json`)
- `code_programm` (из `data/programma_educationis_codicem.csv`)

## 5. Алгоритм работы для агента
1. Создай новый файл ассетов `1_extraction_assets.py`.
2. Извлеки логику чтения из `ingestion/excel_loader.py`, убери из неё `to_sql`.
3. Оформи чтение как чистые функции в `ingestion/excel_extractor.py`.
4. В ассетах Dagster вызови эти функции и верни результат.
5. Настрой `bronze_io_manager` в `definitions.py`, чтобы эти ассеты сохранялись в схему `bronze`.