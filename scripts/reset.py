"""
Очистка таблиц в Iceberg + DuckDB.

Использование:
  python scripts/reset.py                  — показать меню
  python scripts/reset.py silver           — очистить все Silver таблицы
  python scripts/reset.py bronze           — очистить все Bronze таблицы
  python scripts/reset.py all              — очистить всё
  python scripts/reset.py silver.doshkolka — очистить конкретную таблицу
  python scripts/reset.py --list           — показать что есть в каталоге
"""

import os
import shutil
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")


def get_catalog():
    from pyiceberg.catalog.sql import SqlCatalog
    return SqlCatalog(
        "datalake",
        **{
            "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
            "warehouse": f"file://{CATALOG_DIR}/warehouse",
        },
    )


def list_tables(cat) -> list[tuple[str, str]]:
    result = []
    for ns_tuple in cat.list_namespaces():
        ns = ns_tuple[0]
        for _, name in cat.list_tables(ns):
            result.append((ns, name))
    return result


def drop_table(cat, namespace: str, table_name: str) -> None:
    full = f"{namespace}.{table_name}"
    warehouse_path = os.path.join(CATALOG_DIR, "warehouse", namespace, table_name)

    cat.drop_table(full)
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)
    print(f"  ✓ Удалена: {full}")


def recreate_schemas(cat) -> None:
    """Пересоздаёт таблицы с нуля через setup_catalog."""
    sys.path.insert(0, BASE_DIR)
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "setup_catalog",
        os.path.join(BASE_DIR, "ingestion", "setup_catalog.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)


def confirm(message: str) -> bool:
    ans = input(f"{message} [y/N]: ").strip().lower()
    return ans == "y"


def run(target: str) -> None:
    cat = get_catalog()
    tables = list_tables(cat)

    if target == "--list":
        print("Таблицы в каталоге:")
        for ns, name in tables:
            row_count = "?"
            try:
                tbl = cat.load_table(f"{ns}.{name}")
                row_count = tbl.scan().to_arrow().num_rows
            except Exception:
                pass
            print(f"  {ns}.{name}  ({row_count} строк)")
        return

    # Определяем что удалять
    to_drop: list[tuple[str, str]] = []

    if target == "all":
        to_drop = tables
    elif target in set(ns for ns, _ in tables):
        to_drop = [(ns, name) for ns, name in tables if ns == target]
    elif "." in target:
        ns, name = target.split(".", 1)
        to_drop = [(ns, name)]
    else:
        print(f"Неизвестная цель: '{target}'")
        print(__doc__)
        sys.exit(1)

    if not to_drop:
        print(f"Нет таблиц для очистки в '{target}'")
        return

    print(f"Будут удалены ({len(to_drop)}):")
    for ns, name in to_drop:
        print(f"  {ns}.{name}")

    if not confirm("Продолжить?"):
        print("Отменено.")
        return

    for ns, name in to_drop:
        try:
            drop_table(cat, ns, name)
        except Exception as e:
            print(f"  ✗ Ошибка при удалении {ns}.{name}: {e}")

    print()
    recreate = confirm("Пересоздать схемы (пустые таблицы)?")
    if recreate:
        recreate_schemas(cat)

    print("\nГотово. Не забудь обновить DuckDB:")
    print("  python scripts/refresh_duckdb.py")


def menu() -> None:
    cat = get_catalog()
    tables = list_tables(cat)

    print("Что очистить?\n")
    options = []

    # Группы
    namespaces = sorted(set(ns for ns, _ in tables))
    for i, ns in enumerate(namespaces, 1):
        ns_tables = [(n, t) for n, t in tables if n == ns]
        print(f"  {i}. Слой {ns.upper()} ({len(ns_tables)} таблиц)")
        options.append(("namespace", ns))

    # Отдельные таблицы
    for j, (ns, name) in enumerate(tables, len(namespaces) + 1):
        print(f"  {j}. {ns}.{name}")
        options.append(("table", f"{ns}.{name}"))

    print(f"  {len(options) + 1}. Всё")
    options.append(("all", "all"))
    print(f"  0. Отмена\n")

    try:
        choice = int(input("Выбор: ").strip())
    except ValueError:
        print("Отменено.")
        return

    if choice == 0:
        return
    if choice < 1 or choice > len(options):
        print("Неверный выбор.")
        return

    kind, value = options[choice - 1]
    run(value)


if __name__ == "__main__":
    sys.path.insert(0, BASE_DIR)

    if len(sys.argv) > 1:
        run(sys.argv[1])
    else:
        menu()
