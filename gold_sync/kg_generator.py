"""Генерация примеров для KG.md на основе данных из PostgreSQL"""

import logging
import psycopg2
import psycopg2.extras
from typing import Dict, List, Any

logger = logging.getLogger(__name__)


class KGGenerator:
    """Генерирует примеры таблиц Gold-слоя для вставки в KG.md"""

    def __init__(
        self,
        postgres_host: str,
        postgres_port: int,
        postgres_user: str,
        postgres_password: str,
        postgres_db: str,
    ):
        self.pg_config = {
            "host": postgres_host,
            "port": postgres_port,
            "user": postgres_user,
            "password": postgres_password,
            "database": postgres_db,
        }

    def generate_all(self) -> Dict[str, str]:
        """Генерирует примеры для всех Gold-таблиц"""
        examples = {}

        pg_conn = psycopg2.connect(**self.pg_config)
        pg_cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        try:
            examples["gold_students"] = self._generate_students(pg_cursor)
            examples["gold_staff_load"] = self._generate_staff_load(pg_cursor)
            examples["gold_dormitory"] = self._generate_dormitory(pg_cursor)
        finally:
            pg_cursor.close()
            pg_conn.close()

        return examples

    def _generate_students(self, cursor) -> str:
        """Генерирует 3 секции для gold_students"""
        logger.info("Генерация примеров для gold_students...")

        # Секция 1: Федеральные агрегаты + SQL
        sql_1 = """
-- Федеральные агрегаты: охват образованием по возрастам (последний год)
SELECT
    year,
    age,
    ROUND(SUM(population_total)::numeric, 0) as population,
    ROUND(SUM(education_total)::numeric, 0) as students_total,
    ROUND((100.0 * SUM(education_total) / NULLIF(SUM(population_total), 0))::numeric, 2) as coverage_percent
FROM gold_students
WHERE year = (SELECT MAX(year) FROM gold_students)
GROUP BY year, age
ORDER BY coverage_percent DESC
LIMIT 20
        """.strip()

        cursor.execute(sql_1)
        fed_data = cursor.fetchall()

        section_1 = f"""```sql
{sql_1}
```

| Год | Возраст | Население | Обучающихся | Охват, % |
|---|---|---|---|---|
"""
        for row in fed_data:
            section_1 += f"| {row['year']} | {row['age']} | {int(row['population'])} | {int(row['students_total'])} | {row['coverage_percent']}% |\n"

        # Секция 2: 2 случайные строки регионального уровня
        sql_2 = """
SELECT
    region_code, year, age, population_total, education_total, education_share
FROM gold_students
WHERE region_code != 'RU-FED'
ORDER BY RANDOM()
LIMIT 2
        """.strip()

        cursor.execute(sql_2)
        regional_rows = cursor.fetchall()

        section_2 = """### Примеры реальных данных

| Регион | Год | Возраст | Население | Обучающихся | Доля |
|---|---|---|---|---|---|
"""
        for row in regional_rows:
            section_2 += (
                f"| {row['region_code']} | {row['year']} | {row['age']} | "
                f"{int(row['population_total'])} | {int(row['education_total'])} | "
                f"{row['education_share']:.2%} |\n"
            )

        # Секция 3: Словарь значений
        cursor.execute("""
            SELECT DISTINCT region_code
            FROM gold_students
            WHERE region_code != 'RU-FED'
            ORDER BY region_code
            LIMIT 20
        """)
        regions = cursor.fetchall()

        cursor.execute("""
            SELECT DISTINCT age
            FROM gold_students
            ORDER BY age
            LIMIT 20
        """)
        ages = cursor.fetchall()

        cursor.execute("""
            SELECT DISTINCT year
            FROM gold_students
            ORDER BY year DESC
        """)
        years = cursor.fetchall()

        section_3 = "### Словарь значений\n\n"
        section_3 += "| region_code | age | year |\n|---|---|---|\n"

        max_rows = max(len(regions), len(ages), len(years))
        for i in range(max_rows):
            region = regions[i]['region_code'] if i < len(regions) else ""
            age = ages[i]['age'] if i < len(ages) else ""
            year = years[i]['year'] if i < len(years) else ""
            section_3 += f"| {region} | {age} | {year} |\n"

        cursor.execute("SELECT COUNT(DISTINCT region_code) FROM gold_students WHERE region_code != 'RU-FED'")
        total_regions = cursor.fetchall()[0][0]

        cursor.execute("SELECT COUNT(DISTINCT age) FROM gold_students")
        total_ages = cursor.fetchall()[0][0]

        cursor.execute("SELECT COUNT(DISTINCT year) FROM gold_students")
        total_years = cursor.fetchall()[0][0]

        if total_regions > 20 or total_ages > 20 or total_years > 20:
            section_3 += f"| ... ({total_regions} регионов) | ... ({total_ages} возрастов) | ... ({total_years} лет) |\n"

        return f"### Федеральные агрегаты\n\n{section_1}\n\n{section_2}\n\n{section_3}"

    def _generate_staff_load(self, cursor) -> str:
        """Генерирует 3 секции для gold_staff_load"""
        logger.info("Генерация примеров для gold_staff_load...")

        # Секция 1: Федеральные агрегаты за 3 последних года
        sql_1 = """
-- Федеральные агрегаты: кадровое обеспечение по уровням образования (3 последних года)
SELECT
    year,
    level,
    ROUND(SUM(student_count)::numeric, 0) as students,
    ROUND(SUM(positions_total)::numeric, 0) as positions,
    ROUND(SUM(staff_headcount)::numeric, 0) as staff,
    ROUND((100.0 * SUM(vacancies_unfilled) / NULLIF(SUM(positions_total), 0))::numeric, 2) as vacancy_percent,
    ROUND(AVG(shortage_score)::numeric, 1) as shortage_score
FROM gold_staff_load
WHERE year >= (SELECT MAX(year) - 2 FROM gold_staff_load)
GROUP BY year, level
ORDER BY year DESC, level
        """.strip()

        cursor.execute(sql_1)
        fed_data = cursor.fetchall()

        section_1 = f"""```sql
{sql_1}
```

| Год | Уровень | Обучающихся | Должностей | Штат | Вакансий % | Дефицит |
|---|---|---|---|---|---|---|
"""
        for row in fed_data:
            section_1 += (
                f"| {row['year']} | {row['level']} | {int(row['students'])} | {int(row['positions'])} | "
                f"{int(row['staff'])} | {row['vacancy_percent']}% | {row['shortage_score']} |\n"
            )

        # Секция 2: 2 случайные строки
        sql_2 = """
SELECT
    region_code, year, level, student_count, staff_headcount,
    vacancy_unfilled_share, shortage_score
FROM gold_staff_load
WHERE region_code != 'RU-FED'
ORDER BY RANDOM()
LIMIT 2
        """.strip()

        cursor.execute(sql_2)
        regional_rows = cursor.fetchall()

        section_2 = """### Примеры реальных данных

| Регион | Год | Уровень | Обучающихся | Штат | Вакансий % | Дефицит |
|---|---|---|---|---|---|---|
"""
        for row in regional_rows:
            section_2 += (
                f"| {row['region_code']} | {row['year']} | {row['level']} | "
                f"{int(row['student_count'])} | {int(row['staff_headcount'])} | "
                f"{row['vacancy_unfilled_share']:.2%} | {row['shortage_score']:.1f} |\n"
            )

        # Секция 3: Словарь значений
        cursor.execute("""
            SELECT DISTINCT level
            FROM gold_staff_load
            ORDER BY level
        """)
        levels = cursor.fetchall()

        cursor.execute("""
            SELECT DISTINCT region_code
            FROM gold_staff_load
            WHERE region_code != 'RU-FED'
            ORDER BY region_code
            LIMIT 20
        """)
        regions = cursor.fetchall()

        cursor.execute("""
            SELECT DISTINCT year
            FROM gold_staff_load
            ORDER BY year DESC
        """)
        years = cursor.fetchall()

        section_3 = "### Словарь значений\n\n"
        section_3 += "| level | region_code | year |\n|---|---|---|\n"

        max_rows = max(len(levels), len(regions), len(years))
        for i in range(max_rows):
            level = levels[i]['level'] if i < len(levels) else ""
            region = regions[i]['region_code'] if i < len(regions) else ""
            year = years[i]['year'] if i < len(years) else ""
            section_3 += f"| {level} | {region} | {year} |\n"

        cursor.execute("SELECT COUNT(DISTINCT level) FROM gold_staff_load")
        total_levels = cursor.fetchall()[0][0]

        cursor.execute("SELECT COUNT(DISTINCT region_code) FROM gold_staff_load WHERE region_code != 'RU-FED'")
        total_regions = cursor.fetchall()[0][0]

        cursor.execute("SELECT COUNT(DISTINCT year) FROM gold_staff_load")
        total_years = cursor.fetchall()[0][0]

        if total_levels > 20 or total_regions > 20 or total_years > 20:
            section_3 += f"| ... ({total_levels} уровней) | ... ({total_regions} регионов) | ... ({total_years} лет) |\n"

        return f"### Федеральные агрегаты\n\n{section_1}\n\n{section_2}\n\n{section_3}"

    def _generate_dormitory(self, cursor) -> str:
        """Генерирует 3 секции для gold_dormitory"""
        logger.info("Генерация примеров для gold_dormitory...")

        # Секция 1: Федеральные агрегаты за все доступные годы
        sql_1 = """
-- Федеральные агрегаты: инфраструктура общежитий по годам
SELECT
    year,
    ROUND(SUM(area_total)::numeric, 0) as total_area_m2,
    ROUND(SUM(dorm_need)::numeric, 0) as places_needed,
    ROUND(SUM(dorm_live)::numeric, 0) as places_occupied,
    ROUND(SUM(area_need_repair)::numeric, 0) as area_repair_m2,
    ROUND((100.0 * SUM(dorm_shortage_abs) / NULLIF(SUM(dorm_need), 0))::numeric, 2) as shortage_percent
FROM gold_dormitory
WHERE is_forecast = false
GROUP BY year
ORDER BY year DESC
        """.strip()

        cursor.execute(sql_1)
        fed_data = cursor.fetchall()

        section_1 = f"""```sql
{sql_1}
```

| Год | Площадь м² | Мест нужно | Мест занято | На ремонт м² | Дефицит % |
|---|---|---|---|---|---|
"""
        for row in fed_data:
            section_1 += (
                f"| {row['year']} | {int(row['total_area_m2']):,} | {int(row['places_needed'])} | "
                f"{int(row['places_occupied'])} | {int(row['area_repair_m2']):,} | {row['shortage_percent']}% |\n"
            )

        # Секция 2: 2 случайные строки
        sql_2 = """
SELECT
    region_code, year, area_total, dorm_need, dorm_live,
    dorm_shortage_abs, repair_share, is_forecast
FROM gold_dormitory
WHERE region_code != 'RU-FED' AND is_forecast = false
ORDER BY RANDOM()
LIMIT 2
        """.strip()

        cursor.execute(sql_2)
        regional_rows = cursor.fetchall()

        section_2 = """### Примеры реальных данных

| Регион | Год | Площадь м² | Мест нужно | Мест занято | Дефицит | На ремонт % |
|---|---|---|---|---|---|---|
"""
        for row in regional_rows:
            section_2 += (
                f"| {row['region_code']} | {row['year']} | {int(row['area_total'])} | "
                f"{int(row['dorm_need'])} | {int(row['dorm_live'])} | "
                f"{int(row['dorm_shortage_abs'])} | {row['repair_share']:.1%} |\n"
            )

        # Секция 3: Словарь значений (регионы и годы)
        cursor.execute("""
            SELECT DISTINCT region_code
            FROM gold_dormitory
            WHERE region_code != 'RU-FED'
            ORDER BY region_code
            LIMIT 20
        """)
        regions = cursor.fetchall()

        cursor.execute("""
            SELECT DISTINCT year
            FROM gold_dormitory
            WHERE is_forecast = false
            ORDER BY year DESC
        """)
        years = cursor.fetchall()

        section_3 = "### Словарь значений\n\n"
        section_3 += "| region_code | year |\n|---|---|\n"

        max_rows = max(len(regions), len(years))
        for i in range(max_rows):
            region = regions[i]['region_code'] if i < len(regions) else ""
            year = years[i]['year'] if i < len(years) else ""
            section_3 += f"| {region} | {year} |\n"

        cursor.execute("SELECT COUNT(DISTINCT region_code) FROM gold_dormitory WHERE region_code != 'RU-FED'")
        total_regions = cursor.fetchall()[0][0]

        cursor.execute("SELECT COUNT(DISTINCT year) FROM gold_dormitory WHERE is_forecast = false")
        total_years = cursor.fetchall()[0][0]

        if total_regions > 20 or total_years > 20:
            section_3 += f"| ... ({total_regions} регионов) | ... ({total_years} лет) |\n"

        return f"### Федеральные агрегаты\n\n{section_1}\n\n{section_2}\n\n{section_3}"
