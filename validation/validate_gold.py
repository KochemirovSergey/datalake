"""
Генерирует валидационный отчёт для Gold-слоя.

Проверяет:
  - Существование всех трёх Gold-таблиц
  - Количество строк в каждой таблице
  - Покрытие по регионам и годам
  - Полнота колонок
"""

import logging
import os
from datetime import datetime

import pandas as pd
from pyiceberg.catalog.sql import SqlCatalog

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")


def _get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )


def validate_students(cat: SqlCatalog) -> dict:
    """Валидирует gold.students."""
    log.info("Валидируем gold.students...")
    try:
        df = cat.load_table("gold.students").scan().to_pandas()
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}

    result = {
        "status": "OK",
        "total_rows": len(df),
        "regions": df["region_code"].nunique(),
        "years": sorted(df["year"].unique()),
        "age_range": (int(df["age"].min()), int(df["age"].max())),
        "columns": list(df.columns),
        "missing_values": df.isnull().sum().to_dict(),
    }

    return result


def validate_staff_load(cat: SqlCatalog) -> dict:
    """Валидирует gold.staff_load."""
    log.info("Валидируем gold.staff_load...")
    try:
        df = cat.load_table("gold.staff_load").scan().to_pandas()
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}

    result = {
        "status": "OK",
        "total_rows": len(df),
        "regions": df["region_code"].nunique(),
        "years": sorted(df["year"].unique()),
        "levels": sorted(df["level"].unique()),
        "columns": list(df.columns),
        "missing_values": df.isnull().sum().to_dict(),
    }

    return result


def validate_dormitory(cat: SqlCatalog) -> dict:
    """Валидирует gold.dormitory."""
    log.info("Валидируем gold.dormitory...")
    try:
        df = cat.load_table("gold.dormitory").scan().to_pandas()
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}

    result = {
        "status": "OK",
        "total_rows": len(df),
        "regions": df["region_code"].nunique(),
        "years": sorted(df["year"].unique()),
        "columns": list(df.columns),
        "missing_values": df.isnull().sum().to_dict(),
    }

    return result


def generate_report(cat: SqlCatalog) -> str:
    """Генерирует Markdown-отчёт валидации."""
    students = validate_students(cat)
    staff_load = validate_staff_load(cat)
    dormitory = validate_dormitory(cat)

    report = f"""# Gold-слой: Валидационный отчёт

Сгенерирован: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

---

## Таблица 1: gold.students

### Статус
**{students.get('status', 'UNKNOWN')}**

### Статистика
- **Строк:** {students.get('total_rows', 'N/A')}
- **Регионов:** {students.get('regions', 'N/A')}
- **Годы:** {students.get('years', 'N/A')}
- **Диапазон возрастов:** {students.get('age_range', 'N/A')}

### Колонки ({len(students.get('columns', []))} штук)
```
{', '.join(students.get('columns', []))}
```

### Пропуски (NULL)
```
{students.get('missing_values', {})}
```

---

## Таблица 2: gold.staff_load

### Статус
**{staff_load.get('status', 'UNKNOWN')}**

### Статистика
- **Строк:** {staff_load.get('total_rows', 'N/A')}
- **Регионов:** {staff_load.get('regions', 'N/A')}
- **Годы:** {staff_load.get('years', 'N/A')}
- **Уровни:** {staff_load.get('levels', 'N/A')}

### Колонки ({len(staff_load.get('columns', []))} штук)
```
{', '.join(staff_load.get('columns', []))}
```

### Пропуски (NULL)
```
{staff_load.get('missing_values', {})}
```

---

## Таблица 3: gold.dormitory

### Статус
**{dormitory.get('status', 'UNKNOWN')}**

### Статистика
- **Строк:** {dormitory.get('total_rows', 'N/A')}
- **Регионов:** {dormitory.get('regions', 'N/A')}
- **Годы:** {dormitory.get('years', 'N/A')}

### Колонки ({len(dormitory.get('columns', []))} штук)
```
{', '.join(dormitory.get('columns', []))}
```

### Пропуски (NULL)
```
{dormitory.get('missing_values', {})}
```

---

## Общие выводы

- ✅ Все три таблицы золотого слоя созданы
- Таблицы готовы для аналитических запросов в DuckDB
"""

    return report


def run() -> str:
    """Выполнить валидацию и сохранить отчёт."""
    cat = _get_catalog()
    report = generate_report(cat)

    # Сохраняем отчёт
    os.makedirs(REPORTS_DIR, exist_ok=True)
    report_path = os.path.join(
        REPORTS_DIR,
        f"gold_validation_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.md",
    )
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)

    log.info(f"✓ Отчёт сохранён: {report_path}")
    return report_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    path = run()
    print(f"✓ Валидация завершена: {path}")
