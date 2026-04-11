"""
Загружает normalization_config.yaml.

Возвращает список словарей — по одному на источник данных.
Если источник не найден в конфиге — вызывает исключение,
чтобы предотвратить молчаливую обработку.
"""

import os
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_CONFIG_PATH = os.path.join(BASE_DIR, "ingestion", "normalization_config.yaml")


def load_config(path: str = DEFAULT_CONFIG_PATH) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    sources = data.get("sources", [])
    if not sources:
        raise ValueError(f"normalization_config.yaml содержит пустой список sources: {path}")
    return sources


def get_source_config(source_id: str, path: str = DEFAULT_CONFIG_PATH) -> dict:
    sources = load_config(path)
    for s in sources:
        if s["source_id"] == source_id:
            return s
    known = [s["source_id"] for s in sources]
    raise KeyError(
        f"Источник '{source_id}' не найден в normalization_config.yaml. "
        f"Известные источники: {known}"
    )
