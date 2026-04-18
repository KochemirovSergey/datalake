"""
Очистка таблиц DuckDB по слоям Medallion 2.0.

Использование:
  python scripts/reset.py        — показать статус всех слоёв
  python scripts/reset.py 0      — очистить все слои
  python scripts/reset.py 1      — очистить Слой 1 (bronze)
  python scripts/reset.py 2      — очистить Слой 2 (silver_raw)
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DUCKDB_PATH = os.path.join(BASE_DIR, "datalake.duckdb")

LAYERS: dict[int, dict] = {
    1: {
        "label": "Слой 1 — bronze",
        "schema": "bronze",
        "tables": [
            '"1_obuch_doshkolka"', '"1_naselenie"', '"1_obuch_oo"',
            '"1_obuch_vpo"', '"1_obuch_spo"', '"1_obuch_pk"',
            '"1_obshagi_vpo"', '"1_ped_oo"', '"1_regions"', '"1_code_programm"',
        ],
    },
    2: {
        "label": "Слой 2 — silver_raw",
        "schema": "silver_raw",
        "tables": [
            '"2_n_obuch_doshkolka"', '"2_n_naselenie"', '"2_n_obuch_oo"',
            '"2_n_obuch_vpo"', '"2_n_obuch_spo"', '"2_n_obuch_pk"',
            '"2_n_obshagi_vpo"', '"2_n_obshagi_spo"', '"2_n_ped_kadry"',
        ],
    },
}


def _connect():
    import duckdb
    return duckdb.connect(DUCKDB_PATH)


def show_status() -> None:
    conn = _connect()
    try:
        for layer_num, layer in LAYERS.items():
            print(f"\n{layer['label']}:")
            for table in layer["tables"]:
                full = f'{layer["schema"]}.{table}'
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {full}").fetchone()[0]
                    print(f"  {full:<45} {count:>8} строк")
                except Exception:
                    print(f"  {full:<45}  — не материализована")
    finally:
        conn.close()


def drop_layer(layer_num: int) -> None:
    layer = LAYERS[layer_num]
    conn = _connect()
    try:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {layer['schema']}")
        for table in layer["tables"]:
            full = f'{layer["schema"]}.{table}'
            conn.execute(f"DROP TABLE IF EXISTS {full}")
            print(f"  удалена: {full}")
    finally:
        conn.close()


def confirm(msg: str) -> bool:
    return input(f"{msg} [y/N]: ").strip().lower() == "y"


def main() -> None:
    sys.path.insert(0, BASE_DIR)

    if len(sys.argv) == 1:
        show_status()
        return

    arg = sys.argv[1]

    try:
        target = int(arg)
    except ValueError:
        print(__doc__)
        sys.exit(1)

    if target == 0:
        layers_to_drop = list(LAYERS.keys())
        label = "ВСЕ слои"
    elif target in LAYERS:
        layers_to_drop = [target]
        label = LAYERS[target]["label"]
    else:
        print(f"Неизвестный слой: {target}. Доступны: 0 (все), {', '.join(str(k) for k in LAYERS)}")
        sys.exit(1)

    print(f"Будет очищен: {label}")
    if not confirm("Продолжить?"):
        print("Отменено.")
        return

    for n in layers_to_drop:
        print(f"\n{LAYERS[n]['label']}:")
        drop_layer(n)

    print("\nГотово.")


if __name__ == "__main__":
    main()
