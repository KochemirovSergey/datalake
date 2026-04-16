# Руководство: Синхронизация Gold-слоя в PostgreSQL и обновление KG.md

Это руководство описывает, как запустить синхронизацию аналитических витрин из DuckDB в PostgreSQL и автоматически обновить файл с примерами `KG.md`.

## Быстрый старт

### 1. Проверка зависимостей

Убедитесь, что установлены требуемые пакеты:

```bash
pip install duckdb psycopg2-binary
```

### 2. Проверка PostgreSQL

Убедитесь, что PostgreSQL запущен и доступна база `kg_db`:

```bash
psql -h localhost -U postgres -d kg_db -c "SELECT 1"
```

Если базы нет, создайте её:

```bash
createdb -U postgres kg_db
```

### 3. Проверка DuckDB

Убедитесь, что файл `datalake.duckdb` существует и содержит Gold-таблицы:

```bash
python3 << 'EOF'
import duckdb

conn = duckdb.connect("datalake.duckdb")
tables = conn.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'main' AND table_name LIKE 'gold_%'
""").fetchall()

print("Gold-таблицы в DuckDB:")
for table in tables:
    count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchall()[0][0]
    print(f"  • {table[0]}: {count} строк")
EOF
```

Вывод должен показать три таблицы:
- `gold_students`
- `gold_staff_load`
- `gold_dormitory`

## Запуск синхронизации

### Вариант 1: Через shell-скрипт (рекомендуется)

```bash
chmod +x run_sync_gold.sh
./run_sync_gold.sh
```

**Использование переменных окружения для параметров БД:**

```bash
# Установить переменные окружения
export POSTGRES_USER=etl_user
export POSTGRES_PASSWORD=etl_password
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=kg_db
export DUCKDB_PATH=datalake.duckdb
export KG_PATH=KG.md

# Запустить скрипт
./run_sync_gold.sh
```

**Или передать параметры в одной строке:**

```bash
POSTGRES_USER=etl_user POSTGRES_PASSWORD=etl_password ./run_sync_gold.sh
```

**С дополнительными опциями:**

```bash
./run_sync_gold.sh --skip-postgres    # Только обновить KG.md
./run_sync_gold.sh --skip-kg          # Только мигрировать в PostgreSQL
```

### Вариант 2: Через Python напрямую

```bash
python3 sync_gold_to_postgres_and_kg.py \
  --duckdb-path datalake.duckdb \
  --postgres-host localhost \
  --postgres-port 5432 \
  --postgres-user etl_user \
  --postgres-password etl_password \
  --postgres-db kg_db \
  --kg-path KG.md
```

## Что происходит во время синхронизации

### Этап 1: Миграция в PostgreSQL (2-5 минут)

```
DuckDB (gold_students, gold_staff_load, gold_dormitory)
    ↓
[postgres_scanner в DuckDB]
    ↓
PostgreSQL kg_db
```

**Процесс:**
1. Подключение к PostgreSQL и DuckDB
2. **Очистка:** `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`
3. Загрузка расширения `postgres_scanner` в DuckDB
4. Копирование трёх Gold-таблиц по одной:
   - `gold_students` (~51 тыс. строк)
   - `gold_staff_load` (~1.4 тыс. строк)
   - `gold_dormitory` (~1.3 тыс. строк)

### Этап 2: Генерация примеров (30-60 сек)

Для каждой таблицы генерируются 3 секции:

1. **Федеральные агрегаты (RU-FED) + SQL**
   - SQL-запрос для воспроизведения результата
   - Таблица с агрегированными данными

2. **Примеры реальных строк**
   - 2 случайные строки от регионов (не федеральные)

3. **Словарь значений**
   - Уникальные значения каждой колонки
   - До 20 значений + счётчик `... (N всего)`

### Этап 3: Обновление KG.md (несколько секунд)

```
KG.md (исходный)
    ↓
[Парсинг маркеров `Пример таблицы ...`]
    ↓
[Удаление старых примеров]
    ↓
[Вставка новых примеров]
    ↓
KG.md (обновленный)
```

## Проверка результатов

### PostgreSQL

```bash
# Проверить, что таблицы созданы
psql -h localhost -U postgres -d kg_db -c "
  SELECT table_name, (SELECT COUNT(*) FROM information_schema.tables t2 
    WHERE t2.table_name = t.table_name) as row_count
  FROM information_schema.tables t
  WHERE table_schema = 'public' AND table_name LIKE 'gold_%'
"

# Просмотр данных
psql -h localhost -U postgres -d kg_db -c "SELECT * FROM gold_students LIMIT 5"
```

### KG.md

Откройте файл `KG.md` и проверьте:

1. ✓ Наличие трёх основных секций:
   - `#### Пример таблицы `gold_students``
   - `#### Пример таблицы `gold_staff_load``
   - `#### Пример таблицы `gold_dormitory``

2. ✓ Наличие примеров (SQL + таблицы с данными)

3. ✓ Сохранность остального контента (если он был)

## Примеры использования

### Сценарий 1: Полное обновление (со стандартными параметрами)

```bash
# Параметры БД уже установлены в run_sync_gold.sh
./run_sync_gold.sh
```

### Сценарий 2: Обновить только KG.md

```bash
./run_sync_gold.sh --skip-postgres
```

### Сценарий 3: Обновить только PostgreSQL

```bash
./run_sync_gold.sh --skip-kg
```

### Сценарий 4: Использовать другую БД (через переменные окружения)

```bash
export POSTGRES_DB=analytics_prod
export POSTGRES_USER=analytics_user
export POSTGRES_PASSWORD=secret_password
./run_sync_gold.sh
```

### Сценарий 5: Одноразовый запуск с кастомными параметрами

```bash
POSTGRES_DB=kg_test POSTGRES_USER=test_user ./run_sync_gold.sh --skip-kg
```

### Сценарий 6: Пересчёт Gold-слоя и полная синхронизация

```bash
# Пересчитать Gold-таблицы в DuckDB
python3 scripts/refresh_duckdb.py

# Синхронизировать в PostgreSQL и обновить KG.md
./run_sync_gold.sh
```

## Решение проблем

### Ошибка: "postgres_scanner не найден в DuckDB"

**Причина:** Расширение не установлено
**Решение:**

```bash
python3 << 'EOF'
import duckdb
conn = duckdb.connect()
conn.execute("INSTALL postgres_scanner")
conn.execute("LOAD postgres_scanner")
print("✓ postgres_scanner установлен")
EOF
```

### Ошибка: "Failed to connect to PostgreSQL"

**Причина:** PostgreSQL не запущен или неверные параметры подключения
**Решение:**

```bash
# Проверить доступность PostgreSQL
psql -h localhost -U postgres -c "SELECT 1"

# Если не работает, проверьте:
# 1. Запущен ли PostgreSQL: ps aux | grep postgres
# 2. Правильные ли параметры подключения
# 3. Доступны ли права пользователя postgres
```

### Ошибка: "Таблица не найдена в DuckDB"

**Причина:** Gold-таблицы не созданы
**Решение:**

```bash
# Пересчитать Gold-слой
python3 scripts/refresh_duckdb.py

# или вручную в DuckDB
python3 << 'EOF'
import duckdb
conn = duckdb.connect("datalake.duckdb")

# Просмотр всех таблиц
tables = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
print("Все таблицы:")
for t in tables:
    print(f"  • {t[0]}")
EOF
```

### Ошибка: "KG.md не обновляется"

**Причина:** Маркеры в KG.md не совпадают с ожидаемым форматом
**Решение:**

Проверьте, что в KG.md точно указаны маркеры:

```markdown
#### Пример таблицы `gold_students`
#### Пример таблицы `gold_staff_load`
#### Пример таблицы `gold_dormitory`
```

**Точный формат:**
- Уровень заголовка: `####` (4 решётки)
- Текст: `Пример таблицы`
- Имя таблицы: в backticks (`` ` ``)
- Между словами и backticks — пробелы

## Мониторинг и логирование

Скрипт выводит подробные логи всех операций:

```
2026-04-16 10:30:15 - sync_gold - INFO - Шаг 1: Миграция Gold-слоя в PostgreSQL...
2026-04-16 10:30:16 - gold_sync.postgres_sync - INFO - Очистка PostgreSQL схемы...
2026-04-16 10:30:17 - gold_sync.postgres_sync - INFO - ✓ Миграция в PostgreSQL завершена
2026-04-16 10:30:17 - sync_gold - INFO - Шаг 2: Генерация примеров для KG.md...
2026-04-16 10:30:20 - sync_gold - INFO - Синхронизация завершена успешно!
```

Для сохранения логов в файл:

```bash
python3 sync_gold_to_postgres_and_kg.py 2>&1 | tee logs/sync_$(date +%Y%m%d_%H%M%S).log
```

## Интеграция в Dagster (опционально)

Чтобы добавить синхронизацию в Dagster pipeline:

```python
# dagster_project/assets/gold_assets.py

from dagster import asset, Output
from pathlib import Path
import subprocess

@asset(deps=[gold_students, gold_staff_load, gold_dormitory])
def kg_sync():
    """Синхронизирует Gold-слой в PostgreSQL и обновляет KG.md"""
    result = subprocess.run(
        ["python3", "sync_gold_to_postgres_and_kg.py"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent.parent
    )
    
    if result.returncode != 0:
        raise RuntimeError(f"Ошибка синхронизации:\n{result.stderr}")
    
    return Output(
        value={"status": "success", "output": result.stdout},
        metadata={"log": result.stdout}
    )
```

## Расписание автоматических запусков

Для регулярного запуска используйте cron:

```bash
# Ежедневно в 02:00 ночи
0 2 * * * cd /path/to/datalake && python3 sync_gold_to_postgres_and_kg.py >> logs/sync.log 2>&1
```

## Дополнительно

- **Документация модуля:** см. `gold_sync/README.md`
- **Исходные коды:** `sync_gold_to_postgres_and_kg.py`, `gold_sync/*.py`
- **Конфиг PostgreSQL:** см. параметры подключения в скрипте
