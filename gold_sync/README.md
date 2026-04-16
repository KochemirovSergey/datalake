# Gold Layer Sync: DuckDB → PostgreSQL + KG.md

Модуль для синхронизации аналитических витрин (Gold Layer) из локального DuckDB в PostgreSQL и автоматического обновления файла знаний `KG.md`.

## Архитектура

### Шаг 1: Миграция в PostgreSQL (`postgres_sync.py`)

Использует расширение `postgres_scanner` в DuckDB для прямой миграции таблиц:

```
DuckDB (gold_students, gold_staff_load, gold_dormitory)
    ↓
postgres_scanner
    ↓
PostgreSQL (kg_db)
```

**Clean Slate подход:**
- Перед загрузкой выполняется `DROP SCHEMA public CASCADE`
- Затем создаётся пустая схема `public`
- Таблицы создаются как есть: `CREATE TABLE ... AS SELECT * FROM ...`

### Шаг 2: Генерация примеров (`kg_generator.py`)

Для каждой таблицы генерируются **3 секции**:

#### Секция 1: Федеральные агрегаты (RU-FED) + SQL

- **SQL-запрос** в блоке ` ``` `sql ... ``` `
- **Специфика по таблицам:**
  - `gold_students`: ТОП-20 возрастов по проценту охвата образованием (последний год)
  - `gold_staff_load`: агрегаты по уровням образования за 3 последних года
  - `gold_dormitory`: сводная статистика по площадям и местам за все годы

#### Секция 2: Примеры реальных строк

- 2 случайные строки **регионального уровня** (не федеральные)
- Форматируются в Markdown-таблицу

#### Секция 3: Словарь значений

- Уникальные значения для каждой колонки таблицы
- Лимит: если уникальных ≤ 20, выводятся все
- Если > 20, выводятся первые 20 + пометка `... (N всего)`

### Шаг 3: Обновление KG.md (`kg_updater.py`)

Парсит файл `KG.md` в поисках маркеров:

```markdown
#### Пример таблицы `gold_students`
```

Для каждого маркера:
1. **Удаляет** всё содержимое между маркером и следующим заголовком (начинающимся с `#`)
2. **Вставляет** новые примеры, сгенерированные на шаге 2
3. **Сохраняет** остальной текст файла нетронутым

## Использование

### Основной скрипт

```bash
python sync_gold_to_postgres_and_kg.py
```

Опции:

```bash
python sync_gold_to_postgres_and_kg.py \
  --duckdb-path datalake.duckdb \
  --postgres-host localhost \
  --postgres-port 5432 \
  --postgres-user postgres \
  --postgres-password postgres \
  --postgres-db kg_db \
  --kg-path KG.md
```

**Флаги:**

- `--skip-postgres` — пропустить миграцию в PostgreSQL
- `--skip-kg` — пропустить обновление KG.md

### Примеры

**Обновить только PostgreSQL:**
```bash
python sync_gold_to_postgres_and_kg.py --skip-kg
```

**Обновить только KG.md (если данные уже в PostgreSQL):**
```bash
python sync_gold_to_postgres_and_kg.py --skip-postgres
```

**Использовать другую базу:**
```bash
python sync_gold_to_postgres_and_kg.py --postgres-db analytics_kg
```

## Структура KG.md

После первого запуска создастся шаблон `KG.md`:

```markdown
# Граф знаний о системе образования РФ

[Ручные описания, таксономия, ...остальной контент...]

## Аналитические витрины (Gold Layer)

### Обучающиеся (Students)
[Описание таблицы...]

#### Пример таблицы `gold_students`
<!-- Сюда вставляются примеры -->

### Кадровое обеспечение (Staff Load)
[Описание таблицы...]

#### Пример таблицы `gold_staff_load`
<!-- Сюда вставляются примеры -->

### Инфраструктура общежитий (Dormitory)
[Описание таблицы...]

#### Пример таблицы `gold_dormitory`
<!-- Сюда вставляются примеры -->
```

**Важно:** Весь остальной текст (описания, таксономия, комментарии) остаётся нетронутым между запусками.

## Логирование

Скрипт выводит подробные логи всех этапов:

```
2026-04-16 10:30:15 - sync_gold - INFO - Шаг 1: Миграция Gold-слоя в PostgreSQL...
2026-04-16 10:30:16 - gold_sync.postgres_sync - INFO - Подключение к DuckDB и PostgreSQL...
2026-04-16 10:30:16 - gold_sync.postgres_sync - INFO - Очистка PostgreSQL схемы...
2026-04-16 10:30:17 - gold_sync.postgres_sync - INFO - ✓ Миграция в PostgreSQL завершена
2026-04-16 10:30:17 - sync_gold - INFO - Шаг 2: Генерация примеров для KG.md...
2026-04-16 10:30:17 - gold_sync.kg_generator - INFO - Генерация примеров для gold_students...
...
2026-04-16 10:30:20 - sync_gold - INFO - Синхронизация завершена успешно!
```

## Требования

- Python 3.9+
- DuckDB с поддержкой postgres_scanner
- PostgreSQL 12+
- `psycopg2` для работы с PostgreSQL

```bash
pip install duckdb psycopg2-binary
```

## Интеграция с Dagster

В будущем можно добавить asset в Dagster для автоматизации:

```python
@asset(deps=[gold_students, gold_staff_load, gold_dormitory])
def kg_sync():
    """Синхронизирует Gold-слой в PostgreSQL и обновляет KG.md"""
    subprocess.run([
        "python", "sync_gold_to_postgres_and_kg.py"
    ], check=True)
```

## Отладка

### DuckDB не находит postgres_scanner

Убедитесь, что в DuckDB установлено расширение:

```python
import duckdb
conn = duckdb.connect()
conn.execute("INSTALL postgres_scanner")
conn.execute("LOAD postgres_scanner")
```

### PostgreSQL: ошибка подключения

Проверьте параметры:
```bash
psql -h localhost -U postgres -d kg_db
```

### KG.md не обновляется

Проверьте наличие маркеров в формате:
```markdown
#### Пример таблицы `gold_students`
```

Маркер должен быть **точно в этом формате** (уровень заголовка, пробелы и имя в backticks).
