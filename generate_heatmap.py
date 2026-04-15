"""
Генератор HTML-дашборда «Нехватка кадров»
Этап 1 MVP: уровень «Учитель начальных классов»
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------

SRC_DB = {
    "host":     "localhost",
    "port":     5432,
    "database": "etl_db",
    "user":     "etl_user",
    "password": "etl_password",
}

OUTPUT_HTML = Path(__file__).parent / "heatmap_dashboard.html"
OUTPUT_CSV  = Path(__file__).parent / "heatmap_data.csv"

# Соответствие: level_id из таблицы discipuli → отображаемое название уровня
# Расширяйте этот словарь для Этапа 2
LEVEL_MAP = {
    "1.2":     "Учитель начальных классов",
    "1.3+1.4": "Учитель предметник",   # строка 7 - строка 8
    # "2.5": "СПО",
    # "2.6": "Высшее",
}

ACTIVE_LEVELS = ["Учитель начальных классов", "Учитель предметник"]

# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(__file__).parent / "generate_heatmap.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Подключение к БД
# ---------------------------------------------------------------------------

def get_connection():
    try:
        conn = psycopg2.connect(**SRC_DB)
        log.info("Подключение к БД установлено (%s:%s/%s)", SRC_DB["host"], SRC_DB["port"], SRC_DB["database"])
        return conn
    except psycopg2.OperationalError as exc:
        log.error("Ошибка подключения к БД: %s", exc)
        raise


def query_df(conn, sql: str, params=None) -> pd.DataFrame:
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        df = pd.DataFrame(rows)
        log.info("Запрос вернул %d строк", len(df))
        return df
    except psycopg2.Error as exc:
        log.error("Ошибка SQL-запроса: %s\nSQL: %s", exc, sql)
        raise


# ---------------------------------------------------------------------------
# Извлечение данных
# ---------------------------------------------------------------------------

# Последний год в таблице discipuli по уровню 1.2 (начальное)
SQL_LAST_YEAR = """
SELECT MAX("Год") AS last_year
FROM discipuli
WHERE "Вид и уровень образования" = '1.2'
  AND "Определение" = 'Численность обучающихся'
  AND "Уровень субъектности" = 'Регион';
"""

# Численность обучающихся по регионам из таблицы discipuli (ВСЕ годы)
# Коды уровней: 1.2=начальное, 1.3=основное, 1.4=среднее
SQL_ENROLLMENT = """
SELECT
    "Объект"  AS region_id,
    "Год"     AS year,
    CASE
        WHEN "Вид и уровень образования" = '1.2' THEN '1.2'
        WHEN "Вид и уровень образования" IN ('1.3','1.4') THEN '1.3+1.4'
    END       AS level_id,
    SUM("Значение") AS student_count
FROM discipuli
WHERE "Определение" = 'Численность обучающихся'
  AND "Уровень субъектности" = 'Регион'
  AND "гос/негос" = 'сумма'
  AND "Вид и уровень образования" IN ('1.2', '1.3', '1.4')
GROUP BY "Объект", "Год",
    CASE
        WHEN "Вид и уровень образования" = '1.2' THEN '1.2'
        WHEN "Вид и уровень образования" IN ('1.3','1.4') THEN '1.3+1.4'
    END;
"""

# Триггеры из витрины за ВСЕ годы (не только последний)
# Необходимо сначала пересоздать витрину с историческими данными
SQL_TRIGGERS_VITRINA_HISTORICAL = """
WITH

-- ─── RAW: строка 7 (все учителя) и строка 8 (нач. классы) из таблицы 3.4 ──

t34_raw AS (
    SELECT
        o.регион, o."год",
        SUM(CASE WHEN o.column_number = '4' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c4,
        SUM(CASE WHEN o.column_number = '3' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c3,
        SUM(CASE WHEN o.column_number = '5' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c5,
        SUM(CASE WHEN o.column_number = '4' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c4,
        SUM(CASE WHEN o.column_number = '3' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c3,
        SUM(CASE WHEN o.column_number = '5' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c5
    FROM public.oo_1_3_4_230 o
    WHERE o.тег_2 = 'Городские организации' AND o.регион IS NOT NULL
    GROUP BY o.регион, o."год"
),

-- ─── RAW: строки 7 и 8 из таблицы 3.1 (знаменатель триггера 1) ────────────

t31_den_raw AS (
    SELECT
        o.регион, o."год",
        SUM(CASE WHEN o.column_number = '3' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c3,
        SUM(CASE WHEN o.column_number = '3' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c3
    FROM public.oo_1_3_1_218 o
    WHERE o.тег_1 = 'Государственные организации'
      AND o.тег_2 = 'Городские организации'
      AND o.регион IS NOT NULL
    GROUP BY o.регион, o."год"
),

-- ─── Значения триггеров по двум уровням ──────────────────────────────────

base AS (
    SELECT
        a.регион, a."год",
        '1.2'     AS level_code,
        CASE WHEN COALESCE(b.r8_c3, 0) <> 0 THEN a.r8_c4 / b.r8_c3 ELSE NULL END AS trig1_value,
        CASE WHEN COALESCE(a.r8_c3, 0) <> 0
             THEN (a.r8_c3 - a.r8_c5) / a.r8_c3 ELSE NULL END AS trig2_value
    FROM t34_raw a
    LEFT JOIN t31_den_raw b ON a.регион = b.регион AND a."год" = b."год"

    UNION ALL

    SELECT
        a.регион, a."год",
        '1.3+1.4' AS level_code,
        CASE WHEN COALESCE(b.r7_c3 - b.r8_c3, 0) <> 0
             THEN (a.r7_c4 - a.r8_c4) / (b.r7_c3 - b.r8_c3) ELSE NULL END AS trig1_value,
        CASE WHEN COALESCE(a.r7_c3 - a.r8_c3, 0) <> 0
             THEN ((a.r7_c3 - a.r8_c3) - (a.r7_c5 - a.r8_c5)) / (a.r7_c3 - a.r8_c3) ELSE NULL END AS trig2_value
    FROM t34_raw a
    LEFT JOIN t31_den_raw b ON a.регион = b.регион AND a."год" = b."год"
)

SELECT
    регион             AS region_id,
    "год"::int         AS year,
    level_code,
    trig1_value        AS trig1_val,
    trig2_value        AS trig2_val
FROM base
WHERE регион IS NOT NULL;
"""

# ---------------------------------------------------------------------------
# Пересоздание витрины region_triggers_result (строка 8 = учителя начальных классов)
# ---------------------------------------------------------------------------

SQL_REBUILD_VITRINA = """
DROP TABLE IF EXISTS public.region_triggers_result;

CREATE TABLE public.region_triggers_result AS
WITH

-- ─── RAW: строка 7 (все учителя) и строка 8 (нач. классы) из таблицы 3.4 ──

t34_raw AS (
    SELECT
        o.регион, o."год",
        SUM(CASE WHEN o.column_number = '4' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c4,
        SUM(CASE WHEN o.column_number = '3' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c3,
        SUM(CASE WHEN o.column_number = '5' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c5,
        SUM(CASE WHEN o.column_number = '4' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c4,
        SUM(CASE WHEN o.column_number = '3' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c3,
        SUM(CASE WHEN o.column_number = '5' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c5
    FROM public.oo_1_3_4_230 o
    WHERE o.тег_2 = 'Городские организации' AND o.регион IS NOT NULL
    GROUP BY o.регион, o."год"
),

-- ─── RAW: строки 7 и 8 из таблицы 3.1 (знаменатель триггера 1) ────────────

t31_den_raw AS (
    SELECT
        o.регион, o."год",
        SUM(CASE WHEN o.column_number = '3' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c3,
        SUM(CASE WHEN o.column_number = '3' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c3
    FROM public.oo_1_3_1_218 o
    WHERE o.тег_1 = 'Государственные организации'
      AND o.тег_2 = 'Городские организации'
      AND o.регион IS NOT NULL
    GROUP BY o.регион, o."год"
),

-- ─── Значения триггеров по двум уровням ──────────────────────────────────

base AS (
    SELECT
        a.регион, a."год",
        '1.2'     AS level_code,
        -- trig1: col4/col3 из 3.4 / col3 из 3.1 — для нач. классов (строка 8)
        CASE WHEN COALESCE(b.r8_c3, 0) <> 0 THEN a.r8_c4 / b.r8_c3 ELSE NULL END AS trig1_value,
        -- trig2: доля незакрытых ставок = (всего - заполнено) / всего
        -- col3 = всего ставок, col5 = фактически заполнено (ФТЕ)
        CASE WHEN COALESCE(a.r8_c3, 0) <> 0
             THEN (a.r8_c3 - a.r8_c5) / a.r8_c3 ELSE NULL END AS trig2_value
    FROM t34_raw a
    LEFT JOIN t31_den_raw b ON a.регион = b.регион AND a."год" = b."год"

    UNION ALL

    SELECT
        a.регион, a."год",
        '1.3+1.4' AS level_code,
        -- trig1: (col4_r7 - col4_r8) / (col3_r7 - col3_r8) из 3.1 — предметники
        CASE WHEN COALESCE(b.r7_c3 - b.r8_c3, 0) <> 0
             THEN (a.r7_c4 - a.r8_c4) / (b.r7_c3 - b.r8_c3) ELSE NULL END AS trig1_value,
        -- trig2: доля незакрытых ставок для предметников (строка 7 - строка 8)
        CASE WHEN COALESCE(a.r7_c3 - a.r8_c3, 0) <> 0
             THEN ((a.r7_c3 - a.r8_c3) - (a.r7_c5 - a.r8_c5)) / (a.r7_c3 - a.r8_c3) ELSE NULL END AS trig2_value
    FROM t34_raw a
    LEFT JOIN t31_den_raw b ON a.регион = b.регион AND a."год" = b."год"
),

-- ─── Последний год ────────────────────────────────────────────────────────

max_year AS (SELECT MAX("год") AS last_year FROM base),

last_base AS (
    SELECT b.регион, b."год" AS last_year, b.level_code, b.trig1_value, b.trig2_value
    FROM base b JOIN max_year m ON b."год" = m.last_year
),

-- ─── RAW: строки 7 и 8 из таблицы 3.1 (бонус) ───────────────────────────

t31_raw AS (
    SELECT
        o.регион, o."год",
        SUM(CASE WHEN o.column_number = '3'  AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_den,
        SUM(CASE WHEN o.column_number = '4'  AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c4,
        SUM(CASE WHEN o.column_number = '5'  AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c5,
        SUM(CASE WHEN o.column_number = '6'  AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c6,
        SUM(CASE WHEN o.column_number = '7'  AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c7,
        SUM(CASE WHEN o.column_number = '8'  AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c8,
        SUM(CASE WHEN o.column_number = '9'  AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c9,
        SUM(CASE WHEN o.column_number = '10' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c10,
        SUM(CASE WHEN o.column_number = '11' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c11,
        SUM(CASE WHEN o.column_number = '12' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c12,
        SUM(CASE WHEN o.column_number = '13' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c13,
        SUM(CASE WHEN o.column_number = '14' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c14,
        SUM(CASE WHEN o.column_number = '15' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c15,
        SUM(CASE WHEN o.column_number = '16' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c16,
        SUM(CASE WHEN o.column_number = '3'  AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_den,
        SUM(CASE WHEN o.column_number = '4'  AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c4,
        SUM(CASE WHEN o.column_number = '5'  AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c5,
        SUM(CASE WHEN o.column_number = '6'  AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c6,
        SUM(CASE WHEN o.column_number = '7'  AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c7,
        SUM(CASE WHEN o.column_number = '8'  AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c8,
        SUM(CASE WHEN o.column_number = '9'  AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c9,
        SUM(CASE WHEN o.column_number = '10' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c10,
        SUM(CASE WHEN o.column_number = '11' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c11,
        SUM(CASE WHEN o.column_number = '12' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c12,
        SUM(CASE WHEN o.column_number = '13' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c13,
        SUM(CASE WHEN o.column_number = '14' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c14,
        SUM(CASE WHEN o.column_number = '15' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c15,
        SUM(CASE WHEN o.column_number = '16' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c16
    FROM public.oo_1_3_1_218 o
    WHERE o.row_number IN ('7','8') AND o.регион IS NOT NULL
    GROUP BY o.регион, o."год"
),

-- ─── RAW: строки 7 и 8 из таблицы 3.2 (бонус) ───────────────────────────

t32_raw AS (
    SELECT
        o.регион, o."год",
        SUM(CASE WHEN o.column_number = '3'  AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_den,
        SUM(CASE WHEN o.column_number = '4'  AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c4,
        SUM(CASE WHEN o.column_number = '5'  AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c5,
        SUM(CASE WHEN o.column_number = '6'  AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c6,
        SUM(CASE WHEN o.column_number = '7'  AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c7,
        SUM(CASE WHEN o.column_number = '8'  AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c8,
        SUM(CASE WHEN o.column_number = '9'  AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c9,
        SUM(CASE WHEN o.column_number = '11' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c11,
        SUM(CASE WHEN o.column_number = '12' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c12,
        SUM(CASE WHEN o.column_number = '13' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c13,
        SUM(CASE WHEN o.column_number = '14' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c14,
        SUM(CASE WHEN o.column_number = '15' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c15,
        SUM(CASE WHEN o.column_number = '16' AND o.row_number = '7' THEN o."значение"::numeric ELSE 0 END) AS r7_c16,
        SUM(CASE WHEN o.column_number = '3'  AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_den,
        SUM(CASE WHEN o.column_number = '4'  AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c4,
        SUM(CASE WHEN o.column_number = '5'  AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c5,
        SUM(CASE WHEN o.column_number = '6'  AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c6,
        SUM(CASE WHEN o.column_number = '7'  AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c7,
        SUM(CASE WHEN o.column_number = '8'  AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c8,
        SUM(CASE WHEN o.column_number = '9'  AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c9,
        SUM(CASE WHEN o.column_number = '11' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c11,
        SUM(CASE WHEN o.column_number = '12' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c12,
        SUM(CASE WHEN o.column_number = '13' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c13,
        SUM(CASE WHEN o.column_number = '14' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c14,
        SUM(CASE WHEN o.column_number = '15' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c15,
        SUM(CASE WHEN o.column_number = '16' AND o.row_number = '8' THEN o."значение"::numeric ELSE 0 END) AS r8_c16
    FROM public.oo_1_3_2_221 o
    WHERE o.row_number IN ('7','8') AND o.регион IS NOT NULL
    GROUP BY o.регион, o."год"
),

-- ─── Бонус по двум уровням ───────────────────────────────────────────────

bonus_all AS (
    -- Начальные классы (строка 8)
    SELECT
        n.регион, n."год", '1.2' AS level_code,
        CASE WHEN n.r8_den <> 0 THEN (
            (n.r8_c4  / n.r8_den) * 1    + (n.r8_c5  / n.r8_den) * 2    +
            (n.r8_c6  / n.r8_den) * 1    + (n.r8_c7  / n.r8_den) * 2    +
            (n.r8_c8  / n.r8_den) * 3    + (n.r8_c9  / n.r8_den) * 5    +
            (n.r8_c10 / n.r8_den) * 0.5  + (n.r8_c11 / n.r8_den) * 0.75 +
            (n.r8_c12 / n.r8_den) * 0.5  + (n.r8_c13 / n.r8_den) * 3    +
            (n.r8_c14 / n.r8_den) * 3    + (n.r8_c15 / n.r8_den) * 3    +
            (n.r8_c16 / n.r8_den) * 3
        ) ELSE NULL END AS score31,
        CASE WHEN m.r8_den <> 0 THEN (
            (m.r8_c4  / m.r8_den) * 0.5  + (m.r8_c5  / m.r8_den) * 0.75 +
            (m.r8_c6  / m.r8_den) * 1    + (m.r8_c7  / m.r8_den) * 2    +
            (m.r8_c8  / m.r8_den) * 3    + (m.r8_c9  / m.r8_den) * 4    +
            (m.r8_c11 / m.r8_den) * 0.75 + (m.r8_c12 / m.r8_den) * 1    +
            (m.r8_c13 / m.r8_den) * 2    + (m.r8_c14 / m.r8_den) * 3    +
            (m.r8_c15 / m.r8_den) * 5    + (m.r8_c16 / m.r8_den) * 7
        ) ELSE NULL END AS score32
    FROM t31_raw n
    JOIN t32_raw m ON n.регион = m.регион AND n."год" = m."год"

    UNION ALL

    -- Предметники (строка 7 - строка 8)
    SELECT
        n.регион, n."год", '1.3+1.4' AS level_code,
        CASE WHEN (n.r7_den - n.r8_den) <> 0 THEN (
            ((n.r7_c4  - n.r8_c4)  / (n.r7_den - n.r8_den)) * 1    +
            ((n.r7_c5  - n.r8_c5)  / (n.r7_den - n.r8_den)) * 2    +
            ((n.r7_c6  - n.r8_c6)  / (n.r7_den - n.r8_den)) * 1    +
            ((n.r7_c7  - n.r8_c7)  / (n.r7_den - n.r8_den)) * 2    +
            ((n.r7_c8  - n.r8_c8)  / (n.r7_den - n.r8_den)) * 3    +
            ((n.r7_c9  - n.r8_c9)  / (n.r7_den - n.r8_den)) * 5    +
            ((n.r7_c10 - n.r8_c10) / (n.r7_den - n.r8_den)) * 0.5  +
            ((n.r7_c11 - n.r8_c11) / (n.r7_den - n.r8_den)) * 0.75 +
            ((n.r7_c12 - n.r8_c12) / (n.r7_den - n.r8_den)) * 0.5  +
            ((n.r7_c13 - n.r8_c13) / (n.r7_den - n.r8_den)) * 3    +
            ((n.r7_c14 - n.r8_c14) / (n.r7_den - n.r8_den)) * 3    +
            ((n.r7_c15 - n.r8_c15) / (n.r7_den - n.r8_den)) * 3    +
            ((n.r7_c16 - n.r8_c16) / (n.r7_den - n.r8_den)) * 3
        ) ELSE NULL END AS score31,
        CASE WHEN (m.r7_den - m.r8_den) <> 0 THEN (
            ((m.r7_c4  - m.r8_c4)  / (m.r7_den - m.r8_den)) * 0.5  +
            ((m.r7_c5  - m.r8_c5)  / (m.r7_den - m.r8_den)) * 0.75 +
            ((m.r7_c6  - m.r8_c6)  / (m.r7_den - m.r8_den)) * 1    +
            ((m.r7_c7  - m.r8_c7)  / (m.r7_den - m.r8_den)) * 2    +
            ((m.r7_c8  - m.r8_c8)  / (m.r7_den - m.r8_den)) * 3    +
            ((m.r7_c9  - m.r8_c9)  / (m.r7_den - m.r8_den)) * 4    +
            ((m.r7_c11 - m.r8_c11) / (m.r7_den - m.r8_den)) * 0.75 +
            ((m.r7_c12 - m.r8_c12) / (m.r7_den - m.r8_den)) * 1    +
            ((m.r7_c13 - m.r8_c13) / (m.r7_den - m.r8_den)) * 2    +
            ((m.r7_c14 - m.r8_c14) / (m.r7_den - m.r8_den)) * 3    +
            ((m.r7_c15 - m.r8_c15) / (m.r7_den - m.r8_den)) * 5    +
            ((m.r7_c16 - m.r8_c16) / (m.r7_den - m.r8_den)) * 7
        ) ELSE NULL END AS score32
    FROM t31_raw n
    JOIN t32_raw m ON n.регион = m.регион AND n."год" = m."год"
),

bonus_last AS (
    SELECT b.регион, b."год" AS last_year, b.level_code,
           COALESCE(b.score31, 0) + COALESCE(b.score32, 0) AS bonus_score
    FROM bonus_all b
    JOIN max_year m ON b."год" = m.last_year
),

-- ─── Объединение триггеров и бонуса ──────────────────────────────────────

all_regions AS (
    SELECT
        COALESCE(t.регион, b.регион)           AS регион,
        COALESCE(t.last_year, b.last_year)     AS last_year,
        COALESCE(t.level_code, b.level_code)   AS level_code,
        t.trig1_value,
        t.trig2_value,
        b.bonus_score
    FROM last_base t
    FULL JOIN bonus_last b
      ON t.регион = b.регион AND t.level_code = b.level_code
),

-- ─── NTILE внутри каждого уровня ─────────────────────────────────────────

flags AS (
    SELECT
        регион, last_year, level_code, trig1_value, trig2_value, bonus_score,
        NTILE(4) OVER (PARTITION BY level_code ORDER BY trig1_value ASC)  AS trig1_ntile,
        NTILE(2) OVER (PARTITION BY level_code ORDER BY trig2_value ASC)  AS trig2_ntile,
        NTILE(4) OVER (PARTITION BY level_code ORDER BY bonus_score DESC) AS bonus_ntile
    FROM all_regions
)

SELECT
    регион,
    last_year                                                              AS "год",
    level_code,
    trig1_value                                                            AS trig1_значение,
    trig1_ntile                                                            AS trig1_квартиль,
    CASE WHEN trig1_ntile = 4 THEN 1 ELSE 0 END                            AS trigger_1,
    trig2_value                                                            AS trig2_значение,
    trig2_ntile                                                            AS trig2_половина,
    CASE WHEN trig2_ntile = 2 THEN 1 ELSE 0 END                            AS trigger_2,
    bonus_score                                                            AS bonus_значение,
    bonus_ntile                                                            AS bonus_квартиль,
    CASE WHEN bonus_ntile = 1 THEN 1 ELSE 0 END                            AS trigger_bonus,
    (CASE WHEN trig1_ntile = 4 THEN 1 ELSE 0 END
   + CASE WHEN trig2_ntile = 2 THEN 1 ELSE 0 END)                         AS trigger_count,
    (CASE WHEN trig1_ntile = 4 THEN 1 ELSE 0 END
   + CASE WHEN trig2_ntile = 2 THEN 1 ELSE 0 END
   + CASE WHEN bonus_ntile = 1 THEN 1 ELSE 0 END)                         AS trigger_count_with_bonus
FROM flags;
"""


def rebuild_vitrina(conn) -> None:
    """Пересоздаёт витрину region_triggers_result на основе строки 8
    (учителя начального общего образования) из таблиц oo_1_3_*."""
    log.info("Пересоздание витрины region_triggers_result (строка 8)...")
    try:
        with conn.cursor() as cur:
            # psycopg2 не поддерживает несколько команд в одном execute —
            # выполняем DROP и CREATE по отдельности
            cur.execute("DROP TABLE IF EXISTS public.region_triggers_result;")
            # CREATE TABLE ... AS — один большой запрос
            create_sql = SQL_REBUILD_VITRINA.split(
                "CREATE TABLE", 1
            )[1]
            cur.execute("CREATE TABLE" + create_sql)
        conn.commit()
        log.info("Витрина пересоздана успешно.")
    except psycopg2.Error as exc:
        conn.rollback()
        log.error("Ошибка пересоздания витрины: %s", exc)
        raise


def load_data(conn) -> dict:
    # Последний доступный год (для отображения на дашборде)
    year_df = query_df(conn, SQL_LAST_YEAR)
    last_year = int(year_df["last_year"].iloc[0])
    log.info("Последний доступный год: %d", last_year)

    # Численность обучающихся из таблицы discipuli (ВСЕ годы)
    enrollment_df = query_df(conn, SQL_ENROLLMENT)
    log.info("Строк enrollment за все годы: %d", len(enrollment_df))

    # Триггеры за ВСЕ годы
    log.info("Загружаем триггеры за все годы")
    triggers_df = query_df(conn, SQL_TRIGGERS_VITRINA_HISTORICAL)
    log.info("Строк triggers за все годы: %d", len(triggers_df))

    return {
        "year":       last_year,
        "enrollment": enrollment_df,
        "triggers":   triggers_df,
    }


# ---------------------------------------------------------------------------
# Обработка данных
# ---------------------------------------------------------------------------

def normalize_level(level_id: str) -> str | None:
    """Маппинг level_id из БД → отображаемое название. None = уровень пропускается."""
    return LEVEL_MAP.get(str(level_id).strip())


def calc_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Рассчитывает балльную оценку (0–10) на основе стандартных отклонений.

    Логика:
    - Максимум 5 баллов за ставки (начальное значение)
    - Максимум 5 баллов за вакансии (начальное значение)
    - За каждую сигму отклонения ВЫШЕ среднего: -1 балл (мин. 0)
    - За отклонение НИЖЕ среднего: штрафа нет
    - Итоговая оценка: sum of both (0-10)

    Расчет по каждому году и уровню отдельно.
    """
    result_parts = []

    for (year, level), grp in df.groupby(["year", "level"]):
        grp = grp.copy()

        # Расчет статистики по ставкам (trig1_val)
        trig1_vals = grp["trig1_val"].dropna()
        if len(trig1_vals) > 1:
            mean_trig1 = trig1_vals.mean()
            std_trig1 = trig1_vals.std()
        else:
            mean_trig1 = trig1_vals.mean() if len(trig1_vals) > 0 else 0
            std_trig1 = 1

        # Расчет статистики по вакансиям (trig2_val)
        trig2_vals = grp["trig2_val"].dropna()
        if len(trig2_vals) > 1:
            mean_trig2 = trig2_vals.mean()
            std_trig2 = trig2_vals.std()
        else:
            mean_trig2 = trig2_vals.mean() if len(trig2_vals) > 0 else 0
            std_trig2 = 1

        def calc_region_score(row):
            if pd.isna(row["trig1_val"]) or pd.isna(row["trig2_val"]):
                return None

            # Z-score для ставки: количество сигм выше/ниже среднего
            if std_trig1 > 0:
                z_trig1 = (row["trig1_val"] - mean_trig1) / std_trig1
            else:
                z_trig1 = 0

            # Z-score для вакансий: количество сигм выше/ниже среднего
            if std_trig2 > 0:
                z_trig2 = (row["trig2_val"] - mean_trig2) / std_trig2
            else:
                z_trig2 = 0

            # Штраф считаем только если ВЫШЕ среднего (положительные z-score)
            penalty_trig1 = max(0, z_trig1)  # 0 если ниже среднего
            penalty_trig2 = max(0, z_trig2)  # 0 если ниже среднего

            # Суммарное отклонение (в сигмах) только в большую сторону
            sum_sigma = penalty_trig1 + penalty_trig2

            # Пропорциональный расчет: 5 * (1 - sum_sigma / 3)
            # Если sum_sigma >= 3, то score = 0
            # Если sum_sigma = 0, то score = 5
            score = max(0, 5 * (1 - sum_sigma / 3))
            return score

        grp["score"] = grp.apply(calc_region_score, axis=1)
        result_parts.append(grp)

    if not result_parts:
        return df

    return pd.concat(result_parts, ignore_index=True)


def process(data: dict) -> pd.DataFrame:
    year        = data["year"]
    enroll_df   = data["enrollment"]
    triggers_df = data["triggers"]

    # Нормализуем типы
    enroll_df["student_count"] = pd.to_numeric(enroll_df["student_count"], errors="coerce").fillna(0).astype(int)
    enroll_df["year"] = pd.to_numeric(enroll_df["year"], errors="coerce").astype(int)

    # Маппинг уровней
    enroll_df["level"] = enroll_df["level_id"].apply(normalize_level)
    enroll_df = enroll_df[enroll_df["level"].notna()]
    enroll_df = enroll_df[enroll_df["level"].isin(ACTIVE_LEVELS)]

    # Для «Учитель предметник» объединяем basic + secondary
    enroll_df = (
        enroll_df
        .groupby(["region_id", "year", "level"], as_index=False)["student_count"]
        .sum()
    )
    log.info("Строк после маппинга уровней: %d", len(enroll_df))

    # Маппинг level_code витрины → отображаемое имя для джойна по level
    triggers_df["level"] = triggers_df["level_code"].apply(normalize_level)
    triggers_df = triggers_df[triggers_df["level"].notna()]
    triggers_df["year"] = pd.to_numeric(triggers_df["year"], errors="coerce").astype(int)
    triggers_df["trig1_val"] = pd.to_numeric(triggers_df["trig1_val"], errors="coerce")
    triggers_df["trig2_val"] = pd.to_numeric(triggers_df["trig2_val"], errors="coerce")

    # Джойним по region_id + level + year
    df = enroll_df.merge(
        triggers_df[["region_id", "year", "level", "trig1_val", "trig2_val"]],
        on=["region_id", "year", "level"],
        how="left",
    )

    # В discipuli нет отдельной таблицы регионов — используем код региона как имя
    df["region"] = df["region_id"]

    # Расчет новой метрики (балльная оценка 0–10)
    df = calc_score(df)

    # Гарантируем нужные колонки
    output_cols = [
        "region", "year", "level", "student_count",
        "trig1_val", "trig2_val", "score",
    ]
    for c in output_cols:
        if c not in df.columns:
            df[c] = None

    log.info("Итоговая витрина: %d строк", len(df))
    return df[output_cols].sort_values(["year", "level", "region"])


# ---------------------------------------------------------------------------
# Генерация HTML
# ---------------------------------------------------------------------------

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Нехватка кадров — тепловая карта</title>
<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { width: 100%; }
  body {
    font-family: Arial, sans-serif;
    background: #f8fafc;
    min-height: 100vh;
    padding: 16px 12px;
  }
  h1 { font-size: 18px; color: #1F2937; margin-bottom: 4px; text-align: center; }
  .subtitle { font-size: 12px; color: #6B7280; margin-bottom: 14px; text-align: center; }

  #heatmap-panel {
    width: 100%;
    background: white;
    border-radius: 14px;
    box-shadow: 0 6px 32px rgba(0,0,0,0.13);
    padding: 22px 8px 28px;
    overflow-x: hidden;
  }
  .hm-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 16px;
  }
  .hm-title    { font-size: 16px; font-weight: bold; color: #1F2937; }
  .hm-subtitle { font-size: 12px; color: #6B7280; margin-top: 3px; }

  /* Легенда */
  .hm-legend {
    display: flex;
    gap: 18px;
    align-items: center;
    margin-bottom: 10px;
    font-size: 11px;
    color: #374151;
  }
  .hm-legend-item { display: flex; align-items: center; gap: 5px; }
  .hm-swatch { width: 14px; height: 14px; border-radius: 2px; flex-shrink: 0; }

  #hm-svg-container { width: 100%; overflow-x: hidden; }

  /* Tooltip */
  #hm-cell-tooltip {
    position: fixed;
    background: white;
    border: 1px solid #E5E7EB;
    border-radius: 8px;
    padding: 10px 14px;
    font-size: 12px;
    color: #1F2937;
    pointer-events: none;
    box-shadow: 0 4px 18px rgba(0,0,0,0.12);
    min-width: 260px;
    max-width: 360px;
    display: none;
    z-index: 300;
    line-height: 1.6;
  }
</style>
</head>
<body>

<h1>Нехватка кадров — распределение по регионам и уровням образования</h1>
<p class="subtitle">
  Год данных: <strong>__YEAR__</strong> &nbsp;·&nbsp;
  Регионов: <strong>__N_REGIONS__</strong> &nbsp;·&nbsp;
  Уровней: <strong>__N_LEVELS__</strong> &nbsp;·&nbsp;
  Сформировано: __GEN_DT__
</p>

<div id="heatmap-panel">
  <div class="hm-header">
    <div>
      <div class="hm-title">Балльная оценка регионов (0–5) · по уровню образования</div>
      <div class="hm-subtitle">Оценка основана на отклонении от среднего по ставке и вакансиям · наведите на ячейку для детализации · кликните на уровень для таблицы по годам</div>
    </div>
  </div>
  <div class="hm-legend">
    <span style="color:#6B7280;white-space:nowrap">Отклонение от среднего:</span>
    <div style="display:flex;align-items:center;gap:6px">
      <span style="font-size:11px;color:#22C55E">0σ</span>
      <div style="width:160px;height:12px;border-radius:6px;
                  background:linear-gradient(to right,#22C55E,#FBBF24,#F87171);"></div>
      <span style="font-size:11px;color:#F87171">3σ</span>
    </div>
    <div style="font-size:10px;color:#6B7280;margin-top:4px;max-width:300px">
      (три стандартных отклонения)
    </div>
  </div>
  <div id="hm-svg-container"></div>
</div>
<div id="hm-cell-tooltip"></div>

<script>
// ── Данные, подставленные Python-скриптом ─────────────────────────────────
const HEATMAP_DATA = __JSON_DATA__;
const HEATMAP_DATA_ALL = __JSON_DATA_ALL__;
const COLOR_RANGES = __COLOR_RANGES__;
const REGION_LON = __REGION_LON__;
// ──────────────────────────────────────────────────────────────────────────

const COLOR_NONE = "#E5E7EB";

// Плавный градиент: зелёный (лучше) → красный (хуже) с мягкими оттенками
const _colorScale = d3.scaleLinear()
  .domain([0, 0.5, 1.0])
  .range(["#22C55E", "#FBBF24", "#F87171"])
  .interpolate(d3.interpolateRgb);

function gradColor(t) { return _colorScale(t); }

// levelScales определяется ниже в блоке данных (по HEATMAP_DATA_ALL)

// ── Геометрия — адаптивная ────────────────────────────────────────────────
const ROW_H  = 50;
const margin = { top: 148, right: 8, bottom: 46, left: 108 };
const _svgContainer = document.getElementById("hm-svg-container");
const COL_TOTAL = Math.max(400, (_svgContainer.offsetWidth || window.innerWidth) - margin.left - margin.right);

// Уровни и регионы
const levels  = [...new Set(HEATMAP_DATA.map(d => d.level))];
// Сортируем регионы по debuf_count убывающий (для первого уровня), иначе по имени
const regSet  = [...new Set(HEATMAP_DATA.map(d => d.region))];

// Суммарный student_count по региону (для ширины колонок)
const regPop  = {};
HEATMAP_DATA.forEach(d => {
  regPop[d.region] = (regPop[d.region] || 0) + d.student_count;
});
const totalPop = Object.values(regPop).reduce((a, b) => a + b, 0) || 1;

// Сортируем регионы по долготе — с запада на восток
// Сортируем регионы по долготе — с запада на восток
const regionsSorted = regSet.slice().sort((a, b) => {
  const lonA = REGION_LON[a] ?? 999;
  const lonB = REGION_LON[b] ?? 999;
  return lonA - lonB;
});

// Ширины и X-позиции колонок пропорционально student_count
const colW = regionsSorted.map(r => Math.max(1, (regPop[r] || 0) / totalPop * COL_TOTAL));
const colX = [];
let _cx = 0;
colW.forEach(w => { colX.push(_cx); _cx += w; });

const svgW = margin.left + COL_TOTAL + margin.right;

const hmSvg = d3.select("#hm-svg-container").append("svg")
  .attr("width", svgW)
  .style("display", "block");

const gHm = hmSvg.append("g")
  .attr("transform", `translate(${margin.left},${margin.top})`);

// Слои SVG в нужном порядке
const gRows    = gHm.append("g").attr("class", "g-rows");
const gDividers= gHm.append("g").attr("class", "g-dividers").attr("pointer-events", "none");
const gLabels  = gHm.append("g").attr("class", "g-labels");
const gRegLabs = gHm.append("g").attr("class", "g-reglabs").attr("pointer-events", "none");

// Легенда снизу — позиция обновляется динамически
const legendText = hmSvg.append("text")
  .attr("class", "legend-text")
  .attr("x", margin.left)
  .attr("font-size", "9px").attr("fill", "#9CA3AF")
  .attr("font-family", "Arial,sans-serif")
  .text("\u2194 ширина колонки пропорциональна численности обучающихся в регионе");

// ── Данные: latestYear, индекс, шкалы, исторические годы ─────────────────
const latestYear = Math.max(...HEATMAP_DATA.map(d => d.year));

const dataIndex = {};
HEATMAP_DATA_ALL.forEach(d => {
  if (!dataIndex[d.level]) dataIndex[d.level] = {};
  if (!dataIndex[d.level][d.year]) dataIndex[d.level][d.year] = {};
  dataIndex[d.level][d.year][d.region] = d;
});

// Нормировка: от минимума (по всем годам уровня) до 5.0
const levelScales = {};
levels.forEach(lev => {
  const allScores = HEATMAP_DATA_ALL
    .filter(d => d.level === lev && d.score !== null)
    .map(d => d.score);
  const minVal = allScores.length > 0 ? Math.min(...allScores) : 0;
  const maxVal = 5.0;
  levelScales[lev] = v => {
    if (v === null || v === undefined) return 0.5;
    return 1 - (v - minVal) / (maxVal - minVal);
  };
});

// Исторические годы для каждого уровня (по убыванию, без latestYear)
const levelHistYears = {};
levels.forEach(lev => {
  levelHistYears[lev] = [...new Set(
    HEATMAP_DATA_ALL.filter(d => d.level === lev).map(d => d.year)
  )].sort((a, b) => b - a).filter(y => y !== latestYear);
});

// Состояние раскрытых уровней и хранилище групп строк
const expandedLevels = new Set();
const rowGMap = {}; // "level__year" → g element

// ── Список строк по текущему состоянию ───────────────────────────────────
function buildRowList() {
  const rows = [];
  levels.forEach(lev => {
    rows.push({ level: lev, year: latestYear, isMain: true });
    if (expandedLevels.has(lev)) {
      levelHistYears[lev].forEach(yr => {
        rows.push({ level: lev, year: yr, isMain: false });
      });
    }
  });
  return rows;
}

// ── Рендер ячеек одной строки в группу g ─────────────────────────────────
function renderRowCells(g, lev, year) {
  const rowData = dataIndex[lev]?.[year] || {};
  regionsSorted.forEach((reg, ri) => {
    const d = rowData[reg];
    const x = colX[ri];
    const w = Math.max(0.6, colW[ri]);
    const col = d && d.score !== null ? gradColor(levelScales[lev](d.score)) : COLOR_NONE;

    const cellG = g.append("g")
      .attr("transform", `translate(${x}, 0)`)
      .style("cursor", d ? "pointer" : "default");

    cellG.append("rect")
      .attr("width", w - 0.4).attr("height", ROW_H - 0.5)
      .attr("fill", col);

    if (d) {
      cellG
        .on("mouseover", ev => {
          d3.select(ev.currentTarget).select("rect")
            .attr("stroke", "#374151").attr("stroke-width", Math.min(2, w * 0.3));
          showTip(ev, d, reg, lev);
        })
        .on("mousemove", moveTip)
        .on("mouseout", ev => {
          d3.select(ev.currentTarget).select("rect")
            .attr("stroke", null).attr("stroke-width", null);
          hideTip();
        });
    }
  });
}

// ── Обновить разделители + подписи уровней/годов ─────────────────────────
function updateLabelsAndDividers(rows) {
  gDividers.selectAll("*").remove();
  gLabels.selectAll("*").remove();

  // Разделители между строками
  for (let ri = 0; ri <= rows.length; ri++) {
    gDividers.append("line")
      .attr("x1", 0).attr("x2", COL_TOTAL)
      .attr("y1", ri * ROW_H).attr("y2", ri * ROW_H)
      .attr("stroke", "white").attr("stroke-width", 1.5);
  }

  rows.forEach((row, ri) => {
    const y = ri * ROW_H + ROW_H / 2;
    if (row.isMain) {
      const isExpanded = expandedLevels.has(row.level);
      const labelG = gLabels.append("g")
        .style("cursor", "pointer")
        .on("click", () => toggleLevel(row.level));

      // Фон-кнопка для удобного клика
      labelG.append("rect")
        .attr("x", -(margin.left)).attr("y", ri * ROW_H)
        .attr("width", margin.left - 5).attr("height", ROW_H)
        .attr("fill", "transparent");

      // Разбиваем название на строки: каждое слово на новой строке
      const arrow = isExpanded ? " \u25b2" : " \u25bc";
      const words = row.level.split(/ +/);
      const textEl = labelG.append("text")
        .attr("x", -10)
        .attr("text-anchor", "end")
        .attr("font-family", "Arial,sans-serif").attr("font-size", "9px")
        .attr("font-weight", "bold").attr("fill", "#374151");

      const lineH = 11; // высота строки в пикселях
      const totalH = words.length * lineH;
      const startY = y - totalH / 2 + lineH / 2;
      words.forEach((word, wi) => {
        const isLast = wi === words.length - 1;
        textEl.append("tspan")
          .attr("x", -10)
          .attr("y", startY + wi * lineH)
          .text(isLast ? word + arrow : word);
      });
    } else {
      // Подпись года для исторической строки
      gLabels.append("text")
        .attr("x", -10).attr("y", y)
        .attr("text-anchor", "end").attr("dominant-baseline", "middle")
        .attr("font-family", "Arial,sans-serif").attr("font-size", "8px")
        .attr("font-weight", "normal").attr("fill", "#9CA3AF")
        .text(row.year);
    }
  });
}

// ── Полный перерендер ─────────────────────────────────────────────────────
function redraw(animate) {
  const rows = buildRowList();
  const totalH = rows.length * ROW_H + margin.top + margin.bottom;

  if (animate) {
    hmSvg.transition().duration(380).attr("height", totalH);
  } else {
    hmSvg.attr("height", totalH);
  }
  legendText.attr("y", totalH - margin.bottom + 16);

  const seenKeys = new Set();
  rows.forEach((row, ri) => {
    const key = row.level + "__" + row.year;
    seenKeys.add(key);
    const targetY = ri * ROW_H;

    if (!rowGMap[key]) {
      const g = gRows.append("g")
        .attr("class", "row-group")
        .attr("transform", `translate(0, ${targetY})`);
      rowGMap[key] = g;
      renderRowCells(g, row.level, row.year);
      if (!row.isMain) {
        g.attr("opacity", 0).transition().duration(300).delay(60).attr("opacity", 1);
      }
    } else {
      if (animate) {
        rowGMap[key].transition().duration(380).attr("transform", `translate(0, ${targetY})`);
      } else {
        rowGMap[key].attr("transform", `translate(0, ${targetY})`);
      }
    }
  });

  // Удаляем строки которых нет в новом состоянии
  Object.keys(rowGMap).forEach(key => {
    if (!seenKeys.has(key)) {
      const g = rowGMap[key];
      g.transition().duration(220).attr("opacity", 0).on("end", () => g.remove());
      delete rowGMap[key];
    }
  });

  updateLabelsAndDividers(rows);
}

// ── Переключение уровня ───────────────────────────────────────────────────
function toggleLevel(lev) {
  hideTip();
  if (expandedLevels.has(lev)) {
    expandedLevels.delete(lev);
  } else {
    expandedLevels.add(lev);
  }
  redraw(true);
}

// ── Подписи регионов — вертикальные сверху (статичные) ───────────────────
const EDGE_REGIONS = new Set(["Калининградская область", "Чукотский автономный округ"]);
const FORCED_LABELS = new Set([
  "Смоленская область",
  "Севастополь",
  "Тульская область",
  "Республика адыгея",
  "Орловская область",
  "Карачаево черкесская республика",
  "Астраханская область",
  "Томская область",
  "Амурская область",
  "Республика саха якутия",
  "Хабаровский край",
]);
regionsSorted.forEach((reg, ri) => {
  const w      = colW[ri];
  const isEdge = EDGE_REGIONS.has(reg);
  const isForced = FORCED_LABELS.has(reg);
  const fs     = isEdge ? 9 : 8.5;
  if (!isEdge && !isForced && w < fs) return;
  const x = colX[ri] + w / 2;
  gRegLabs.append("text")
    .attr("transform", `translate(${x}, -8) rotate(-90)`)
    .attr("text-anchor", "start")
    .attr("dominant-baseline", "middle")
    .attr("font-family", "Arial,sans-serif")
    .attr("font-size", fs + "px")
    .attr("font-weight", isEdge ? "bold" : "normal")
    .attr("fill", "#374151")
    .text(reg);
});

// Первый рендер (без анимации)
redraw(false);



// ── Tooltip ───────────────────────────────────────────────────────────────
const _tip = document.getElementById("hm-cell-tooltip");

function fmtVal(v, dec) {
  if (v == null) return "—";
  return Number(v).toFixed(dec);
}

function showTip(ev, d, reg, lev) {
  if (!d || d.score === null) {
    _tip.innerHTML = `<strong>${reg}</strong><br/>Нет данных`;
    _tip.style.display = "block";
    moveTip(ev); return;
  }

  // Вычисляем статистику для года и уровня (по всем годам)
  const dataForYearLevel = HEATMAP_DATA_ALL.filter(x => x.year === d.year && x.level === lev);

  // Статистика по ставкам (trig1_val)
  const t1vals = dataForYearLevel.map(x => x.trig1_val).filter(v => v != null);
  const mean_t1 = t1vals.reduce((a, b) => a + b, 0) / t1vals.length;
  const std_t1 = Math.sqrt(t1vals.reduce((sum, v) => sum + Math.pow(v - mean_t1, 2), 0) / t1vals.length);
  const z_t1 = std_t1 > 0 ? (d.trig1_val - mean_t1) / std_t1 : 0;
  const sigma_pct_t1 = (z_t1 * 100).toFixed(1);
  const sign_t1 = z_t1 >= 0 ? "+" : "";
  const color_t1 = z_t1 > 0 ? '#F87171' : '#22C55E';
  const bgColor_t1 = z_t1 > 0 ? '#FFEDEB' : '#DCFCE7';

  // Статистика по вакансиям (trig2_val)
  const t2vals = dataForYearLevel.map(x => x.trig2_val).filter(v => v != null);
  const mean_t2 = t2vals.reduce((a, b) => a + b, 0) / t2vals.length;
  const std_t2 = Math.sqrt(t2vals.reduce((sum, v) => sum + Math.pow(v - mean_t2, 2), 0) / t2vals.length);
  const z_t2 = std_t2 > 0 ? (d.trig2_val - mean_t2) / std_t2 : 0;
  const sigma_pct_t2 = (z_t2 * 100).toFixed(1);
  const sign_t2 = z_t2 >= 0 ? "+" : "";
  const color_t2 = z_t2 > 0 ? '#F87171' : '#22C55E';
  const bgColor_t2 = z_t2 > 0 ? '#FFEDEB' : '#DCFCE7';

  _tip.innerHTML = `
    <div style="font-weight:bold;font-size:13px;color:#111827;margin-bottom:2px">
      ${reg} · ${lev}
    </div>
    <div style="color:#6B7280;font-size:11px;margin-bottom:8px">Год: ${d.year} · Обучающихся: <strong style="color:#111827">${d.student_count.toLocaleString("ru")}</strong></div>

    <table style="border-collapse:collapse;width:100%;font-size:11px;border:1px solid #E5E7EB;border-radius:4px;overflow:hidden">
      <thead style="background:#F3F4F6">
        <tr>
          <td style="padding:6px 8px;font-weight:bold;color:#374151;text-align:left;border-right:1px solid #E5E7EB">Показатель</td>
          <td style="padding:6px 8px;font-weight:bold;color:#374151;text-align:right;border-right:1px solid #E5E7EB;min-width:70px">Регион</td>
          <td style="padding:6px 8px;font-weight:bold;color:#374151;text-align:right;border-right:1px solid #E5E7EB;min-width:90px">Среднее РФ</td>
          <td style="padding:6px 8px;font-weight:bold;color:#374151;text-align:center;min-width:80px">Отклон. σ</td>
        </tr>
      </thead>
      <tbody>
        <tr style="border-top:1px solid #E5E7EB">
          <td style="padding:6px 8px;color:#6B7280;border-right:1px solid #E5E7EB">Ставка на 1 учителя</td>
          <td style="padding:6px 8px;color:#111827;font-weight:600;text-align:right;border-right:1px solid #E5E7EB">${d.trig1_val.toFixed(3)}</td>
          <td style="padding:6px 8px;color:#111827;font-weight:600;text-align:right;border-right:1px solid #E5E7EB">${mean_t1.toFixed(3)}</td>
          <td style="padding:6px 8px;background:${bgColor_t1};color:${color_t1};font-weight:700;text-align:center;border-radius:3px">${sign_t1}${sigma_pct_t1}%</td>
        </tr>
        <tr style="border-top:1px solid #E5E7EB">
          <td style="padding:6px 8px;color:#6B7280;border-right:1px solid #E5E7EB">Незаполненные ставки</td>
          <td style="padding:6px 8px;color:#111827;font-weight:600;text-align:right;border-right:1px solid #E5E7EB">${(d.trig2_val * 100).toFixed(1)}%</td>
          <td style="padding:6px 8px;color:#111827;font-weight:600;text-align:right;border-right:1px solid #E5E7EB">${(mean_t2 * 100).toFixed(1)}%</td>
          <td style="padding:6px 8px;background:${bgColor_t2};color:${color_t2};font-weight:700;text-align:center;border-radius:3px">${sign_t2}${sigma_pct_t2}%</td>
        </tr>
      </tbody>
    </table>`;
  _tip.style.display = "block";
  moveTip(ev);
}

function moveTip(ev) {
  const x = ev.clientX + 16;
  const y = ev.clientY - 10;
  const ox = x + _tip.offsetWidth - window.innerWidth;
  _tip.style.left = (ox > 0 ? ev.clientX - _tip.offsetWidth - 12 : x) + "px";
  _tip.style.top  = Math.max(4, y) + "px";
}

function hideTip() { _tip.style.display = "none"; }

</script>
</body>
</html>
"""


def generate_html(df: pd.DataFrame, year: int) -> str:
    import json

    # Долготы регионов для сортировки с запада на восток + названия
    regions_json_path = Path(__file__).parent / "regions_final.json"
    region_lon  = {}
    region_name = {}
    if regions_json_path.exists():
        regions_data = json.loads(regions_json_path.read_text(encoding="utf-8"))
        for name, info in regions_data.get("regions", {}).items():
            iso = info["iso"]
            display_name = name[0].upper() + name[1:]
            region_lon[display_name]  = info["longitude"]
            region_name[iso]          = display_name

    # Фильтруем данные для главного хитмапа (последний год)
    df_last_year = df[df["year"] == year].copy()

    # Вычисляем динамическую шкалу: от 10 (идеал) до минимума в году/уровне
    color_ranges = {}
    for (year_val, level), grp in df.groupby(["year", "level"]):
        scores = grp["score"].dropna()
        if len(scores) > 0:
            min_score = scores.min()
            max_score = 10.0
            key = f"{year_val}_{level}"
            color_ranges[key] = {
                "min": float(min_score),
                "max": float(max_score),
            }

    # Готовим данные для главного хитмапа (последний год)
    records_latest = []
    for _, row in df_last_year.iterrows():
        iso = str(row["region"])
        region_display = region_name.get(iso, iso)
        level = str(row["level"])
        score = float(row["score"]) if pd.notna(row["score"]) else None

        records_latest.append({
            "region":        region_display,
            "year":          int(row["year"]),
            "level":         level,
            "student_count": int(row["student_count"]) if pd.notna(row["student_count"]) else 0,
            "trig1_val":     round(float(row["trig1_val"]), 3) if pd.notna(row.get("trig1_val")) else None,
            "trig2_val":     round(float(row["trig2_val"]), 3) if pd.notna(row.get("trig2_val")) else None,
            "score":         round(score, 2) if score is not None else None,
        })

    # Готовим полные исторические данные (все годы)
    records_all = []
    for _, row in df.iterrows():
        iso = str(row["region"])
        region_display = region_name.get(iso, iso)
        level = str(row["level"])
        year_val = int(row["year"])
        score = float(row["score"]) if pd.notna(row["score"]) else None

        records_all.append({
            "region":        region_display,
            "year":          year_val,
            "level":         level,
            "score":         round(score, 2) if score is not None else None,
            "trig1_val":     round(float(row["trig1_val"]), 3) if pd.notna(row.get("trig1_val")) else None,
            "trig2_val":     round(float(row["trig2_val"]), 3) if pd.notna(row.get("trig2_val")) else None,
            "student_count": int(row["student_count"]) if pd.notna(row["student_count"]) else 0,
        })

    n_regions = df_last_year["region"].nunique()
    n_levels  = df_last_year["level"].nunique()
    gen_dt    = datetime.now().strftime("%d.%m.%Y %H:%M")

    html = HTML_TEMPLATE
    html = html.replace("__YEAR__",         str(year))
    html = html.replace("__N_REGIONS__",    str(n_regions))
    html = html.replace("__N_LEVELS__",     str(n_levels))
    html = html.replace("__GEN_DT__",       gen_dt)
    html = html.replace("__JSON_DATA__",    json.dumps(records_latest, ensure_ascii=False))
    html = html.replace("__JSON_DATA_ALL__",json.dumps(records_all,    ensure_ascii=False))
    html = html.replace("__COLOR_RANGES__", json.dumps(color_ranges,   ensure_ascii=False))
    html = html.replace("__REGION_LON__",   json.dumps(region_lon,     ensure_ascii=False))

    return html


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

def main():
    log.info("=== Старт генерации дашборда ===")
    conn = get_connection()
    rebuild_vitrina(conn)
    raw  = load_data(conn)
    conn.close()

    df   = process(raw)
    html = generate_html(df, raw["year"])
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    log.info("HTML сохранён: %s", OUTPUT_HTML)
    log.info("CSV  сохранён: %s", OUTPUT_CSV)
    log.info("=== Готово ===")


if __name__ == "__main__":
    main()
