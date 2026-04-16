"""Миграция Gold-слоя из DuckDB в PostgreSQL"""

import logging
import duckdb
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logger = logging.getLogger(__name__)


class PostgresSync:
    """Синхронизирует таблицы Gold-слоя из DuckDB в PostgreSQL"""

    GOLD_TABLES = ["gold_students", "gold_staff_load", "gold_dormitory"]

    def __init__(
        self,
        duckdb_path: str,
        postgres_host: str,
        postgres_port: int,
        postgres_user: str,
        postgres_password: str,
        postgres_db: str,
    ):
        self.duckdb_path = duckdb_path
        self.pg_config = {
            "host": postgres_host,
            "port": postgres_port,
            "user": postgres_user,
            "password": postgres_password,
            "database": postgres_db,
        }

    def sync(self):
        """Выполняет полную синхронизацию Gold-таблиц"""
        logger.info("Подключение к DuckDB и PostgreSQL...")

        # Подключение к DuckDB
        duckdb_conn = duckdb.connect(self.duckdb_path)

        # Подключение к PostgreSQL и очистка
        pg_conn = psycopg2.connect(**self.pg_config)
        pg_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        pg_cursor = pg_conn.cursor()

        try:
            # Очистка схемы (Clean Slate)
            logger.info("Очистка PostgreSQL схемы (DROP SCHEMA public CASCADE)...")
            pg_cursor.execute("DROP SCHEMA IF EXISTS public CASCADE")
            pg_cursor.execute("CREATE SCHEMA public")
            pg_conn.commit()
            logger.info("✓ Схема очищена и переинициализирована")

            # Загрузка postgres_scanner в DuckDB
            logger.info("Загрузка расширения postgres_scanner в DuckDB...")
            duckdb_conn.execute("INSTALL postgres_scanner")
            duckdb_conn.execute("LOAD postgres_scanner")

            # Подключение к PostgreSQL один раз
            logger.info("Подключение DuckDB к PostgreSQL через postgres_scanner...")
            db_string = (
                f"postgresql://{self.pg_config['user']}:{self.pg_config['password']}"
                f"@{self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['database']}"
            )
            duckdb_conn.execute(f"ATTACH '{db_string}' AS pg_db (TYPE POSTGRES)")

            # Миграция каждой таблицы
            for table_name in self.GOLD_TABLES:
                self._migrate_table(duckdb_conn, pg_cursor, table_name)

            pg_conn.commit()
            logger.info("✓ Все таблицы успешно мигрированы")

        finally:
            pg_cursor.close()
            pg_conn.close()
            duckdb_conn.close()

    def _migrate_table(self, duckdb_conn, pg_cursor, table_name: str):
        """Миграция одной таблицы из DuckDB в PostgreSQL"""
        logger.info(f"Миграция таблицы {table_name}...")

        # Проверка существования таблицы в DuckDB
        result = duckdb_conn.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchall()

        if not result or result[0][0] == 0:
            logger.warning(f"⚠ Таблица {table_name} не найдена в DuckDB, пропуск")
            return

        # Получение количества строк
        row_count = duckdb_conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchall()[0][0]
        logger.info(f"  Найдено {row_count} строк")

        try:
            # Создание таблицы в PostgreSQL через DuckDB's postgres_scanner
            duckdb_conn.execute(f"""
                CREATE TABLE pg_db.{table_name} AS
                SELECT * FROM {table_name}
            """)

            logger.info(f"  ✓ {table_name}: {row_count} строк загружено")

        except Exception as e:
            logger.error(f"  ✗ Ошибка при миграции {table_name}: {e}")
            raise
