#!/usr/bin/env python3
"""
Синхронизация Gold-слоя: DuckDB → PostgreSQL

Процесс:
1. Подключение к PostgreSQL и очистка старых данных (DROP SCHEMA public CASCADE)
2. Миграция таблиц из DuckDB (gold.students, gold.staff_load, gold.dormitory)
"""

import argparse
import logging
from datetime import datetime

from gold_sync.postgres_sync import PostgresSync


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Синхронизация Gold-слоя в PostgreSQL"
    )
    parser.add_argument(
        "--duckdb-path",
        default="datalake.duckdb",
        help="Путь к DuckDB файлу (default: datalake.duckdb)"
    )
    parser.add_argument(
        "--postgres-host",
        default="158.160.164.146",
        help="PostgreSQL хост (default: 158.160.164.146)"
    )
    parser.add_argument(
        "--postgres-port",
        type=int,
        default=5432,
        help="PostgreSQL порт (default: 5432)"
    )
    parser.add_argument(
        "--postgres-user",
        default="kg_user",
        help="PostgreSQL пользователь (default: kg_user)"
    )
    parser.add_argument(
        "--postgres-password",
        default="kg_pass_2026",
        help="PostgreSQL пароль (default: kg_pass_2026)"
    )
    parser.add_argument(
        "--postgres-db",
        default="kg_db",
        help="PostgreSQL база данных (default: kg_db)"
    )

    args = parser.parse_args()

    logger.info("Миграция Gold-слоя в PostgreSQL...")
    try:
        syncer = PostgresSync(
            duckdb_path=args.duckdb_path,
            postgres_host=args.postgres_host,
            postgres_port=args.postgres_port,
            postgres_user=args.postgres_user,
            postgres_password=args.postgres_password,
            postgres_db=args.postgres_db,
        )
        syncer.sync()
        logger.info("✓ Миграция в PostgreSQL завершена")
    except Exception as e:
        logger.error(f"✗ Ошибка при миграции: {e}")
        raise

    logger.info("=" * 60)
    logger.info("Синхронизация завершена успешно!")
    logger.info(f"Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
