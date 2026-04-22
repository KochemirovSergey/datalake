#!/usr/bin/env python3
"""Конвертация etl_db (PostgreSQL в Docker) -> etl_db.sqlite"""

import sqlite3
import sys
import time

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

PG_URL = "postgresql://etl_user:etl_password@localhost:5432/etl_db"
SQLITE_PATH = "/Users/sergejkocemirov/datalake/etl_db.sqlite"

def get_user_tables(engine):
    query = """
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY schemaname, tablename
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [(row[0], row[1]) for row in result]

def main():
    print(f"Подключение к PostgreSQL: {PG_URL}")
    pg_engine = create_engine(PG_URL)

    tables = get_user_tables(pg_engine)
    print(f"Найдено таблиц: {len(tables)}")

    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    print(f"SQLite файл: {SQLITE_PATH}")

    errors = []
    for i, (schema, table) in enumerate(tables, 1):
        sqlite_name = f"{schema}__{table}" if schema != "public" else table
        try:
            df = pd.read_sql_table(table, pg_engine, schema=schema)
            df.to_sql(sqlite_name, sqlite_conn, if_exists="replace", index=False)
            print(f"[{i}/{len(tables)}] {schema}.{table} -> {sqlite_name} ({len(df)} строк)")
        except Exception as e:
            print(f"[{i}/{len(tables)}] ОШИБКА {schema}.{table}: {e}", file=sys.stderr)
            errors.append((schema, table, str(e)))

    sqlite_conn.close()

    print(f"\nГотово. Ошибок: {len(errors)}")
    if errors:
        print("Ошибочные таблицы:")
        for schema, table, err in errors:
            print(f"  {schema}.{table}: {err}")

if __name__ == "__main__":
    main()
