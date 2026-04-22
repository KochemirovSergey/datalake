"""
Очистка таблиц DuckDB по слоям Medallion 2.0.

Использование:
  python scripts/reset.py        — показать статус всех слоёв
  python scripts/reset.py 0      — очистить все слои
  python scripts/reset.py 1      — очистить Слой 1 (bronze)
  python scripts/reset.py 2      — очистить Слой 2 (silver_raw)
  python scripts/reset.py 2.1    — очистить Слой 2.1 (нормализация возраста)
  python scripts/reset.py 3      — очистить Слой 3 (silver_agg)
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
    21: {
        "label": "Слой 2.1 — silver_raw (нормализация возраста)",
        "schema": "silver_raw",
        "tables": [
            '"2_1_n_obuch_doshkolka_age"', '"2_1_n_obuch_oo_age"',
            '"2_1_n_obuch_vpo_age"', '"2_1_n_obuch_spo_age"',
        ],
    },
    3: {
        "label": "Слой 3 — silver_agg (агрегация)",
        "schema": "silver_agg",
        "tables": [
            '"3_a_obuch_oo"', '"3_a_obuch_vpo"', '"3_a_obuch_spo"',
            '"3_a_obshagi_vpo"', '"3_a_obshagi_spo"', '"3_a_ped_kadry"',
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

    # "2.1" → ключ 21 в LAYERS
    _arg_map = {"2.1": 21}
    try:
        target = _arg_map.get(arg, int(arg))
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
        available = ", ".join("2.1" if k == 21 else str(k) for k in LAYERS)
        print(f"Неизвестный слой: {arg}. Доступны: 0 (все), {available}")
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
