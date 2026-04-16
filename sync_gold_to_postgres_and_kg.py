#!/usr/bin/env python3
"""
Синхронизация Gold-слоя: DuckDB → PostgreSQL + обновление KG.md

Процесс:
1. Подключение к PostgreSQL и очистка старых данных (DROP SCHEMA public CASCADE)
2. Миграция таблиц из DuckDB (gold.students, gold.staff_load, gold.dormitory)
3. Генерация примеров и обновление KG.md
"""

import argparse
import logging
from pathlib import Path
from datetime import datetime

from gold_sync.postgres_sync import PostgresSync
from gold_sync.kg_generator import KGGenerator
from gold_sync.kg_updater import KGUpdater


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Синхронизация Gold-слоя и обновление KG.md"
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
    parser.add_argument(
        "--kg-path",
        default="KG.md",
        help="Путь к файлу KG.md (default: KG.md)"
    )
    parser.add_argument(
        "--skip-postgres",
        action="store_true",
        help="Пропустить миграцию в PostgreSQL"
    )
    parser.add_argument(
        "--skip-kg",
        action="store_true",
        help="Пропустить обновление KG.md"
    )

    args = parser.parse_args()

    # Шаг 1: Миграция в PostgreSQL
    if not args.skip_postgres:
        logger.info("Шаг 1: Миграция Gold-слоя в PostgreSQL...")
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

    # Шаг 2: Обновление KG.md
    if not args.skip_kg:
        logger.info("Шаг 2: Генерация примеров для KG.md...")
        try:
            kg_gen = KGGenerator(
                postgres_host=args.postgres_host,
                postgres_port=args.postgres_port,
                postgres_user=args.postgres_user,
                postgres_password=args.postgres_password,
                postgres_db=args.postgres_db,
            )
            examples = kg_gen.generate_all()
            logger.info("✓ Примеры сгенерированы")

            logger.info("Шаг 3: Обновление KG.md...")
            updater = KGUpdater(kg_path=args.kg_path)
            updater.update(examples)
            logger.info(f"✓ KG.md обновлён: {args.kg_path}")
        except Exception as e:
            logger.error(f"✗ Ошибка при генерации KG.md: {e}")
            raise

    logger.info("=" * 60)
    logger.info("Синхронизация завершена успешно!")
    logger.info(f"Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
