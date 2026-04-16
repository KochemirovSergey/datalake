"""Парсинг и обновление KG.md файла с примерами из Gold-слоя"""

import logging
import re
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class KGUpdater:
    """Обновляет файл KG.md с примерами данных Gold-слоя"""

    # Паттерн для поиска маркеров "Пример таблицы `имя_таблицы`"
    MARKER_PATTERN = r"^(#+)\s+Пример таблицы\s+`([^`]+)`"

    def __init__(self, kg_path: str = "KG.md"):
        self.kg_path = Path(kg_path)

    def update(self, examples: Dict[str, str]):
        """Обновляет KG.md с новыми примерами"""
        logger.info(f"Обновление KG.md: {self.kg_path}")

        # Создаём шаблон (всегда заново, чтобы избежать дубликатов)
        content = self._create_template()

        # Заполняем примеры для каждой таблицы
        content = self._insert_examples(content, examples)

        # Сохраняем файл
        self.kg_path.write_text(content, encoding="utf-8")
        logger.info(f"✓ KG.md сохранён: {self.kg_path}")

    def _insert_examples(self, content: str, examples: Dict[str, str]) -> str:
        """Вставляет примеры в шаблон KG.md"""
        for table_name, example_content in examples.items():
            logger.info(f"Вставка примеров для {table_name}...")
            marker = f"#### Пример таблицы `{table_name}`"

            # Находим маркер и вставляем примеры сразу после него
            lines = content.split("\n")
            result_lines = []

            i = 0
            while i < len(lines):
                result_lines.append(lines[i])

                # Если найден маркер, добавляем примеры на следующей строке
                if marker in lines[i]:
                    result_lines.append("")
                    result_lines.extend(example_content.split("\n"))

                i += 1

            content = "\n".join(result_lines)

        return content

    def _extract_table_name(self, line: str) -> Optional[str]:
        """Извлекает имя таблицы из строки маркера"""
        match = re.search(r"`([^`]+)`", line)
        return match.group(1) if match else None

    def _create_template(self) -> str:
        """Создаёт шаблон KG.md с пустыми секциями для таблиц"""
        return """# Граф знаний о системе образования РФ

Документ описывает структуру и содержание аналитических витрин (Gold Layer)
Data Lake, используемых для анализа образовательной инфраструктуры России.

## Аналитические витрины (Gold Layer)

### Обучающиеся (Students)

Таблица содержит данные о численности обучающихся по регионам, годам, возрастам и уровням образования.

#### Пример таблицы `gold_students`

### Кадровое обеспечение (Staff Load)

Таблица содержит показатели педагогической нагрузки, квалификации и выявленного дефицита кадров по уровням образования.

#### Пример таблицы `gold_staff_load`

### Инфраструктура общежитий (Dormitory)

Таблица содержит метрики по площадям, обеспеченности местами и их использованию в общежитиях учебных заведений.

#### Пример таблицы `gold_dormitory`
"""
