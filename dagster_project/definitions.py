import importlib.util
import os

from dagster import Definitions, load_assets_from_modules

from dagster_project.assets import (
    bronze_assets,
    bronze_normalized_assets,
    silver_assets,
    staff_shortage_assets,
    dormitory_assets,
    gold_assets,
)
from dagster_project.io_managers import BronzeDuckDBIOManager, SilverRawDuckDBIOManager, SilverRaw21DuckDBIOManager, SilverAggDuckDBIOManager
from dagster_project.resources import S3Config


def _load_numbered_module(name: str, filename: str):
    """Загружает модуль с именем файла, начинающимся с цифры (importlib)."""
    assets_dir = os.path.dirname(__file__)
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(assets_dir, "assets", filename)
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_layer1 = _load_numbered_module("layer1_extraction_assets", "1_extraction_assets.py")
_layer2 = _load_numbered_module("layer2_normalization_assets", "2_normalization_assets.py")
_layer3 = _load_numbered_module("layer3_aggregation_assets", "3_aggregation_assets.py")

all_assets = load_assets_from_modules([
    bronze_assets,
    bronze_normalized_assets,
    silver_assets,
    staff_shortage_assets,
    dormitory_assets,
    gold_assets,
    _layer1,
    _layer2,
    _layer3,
])

defs = Definitions(
    assets=all_assets,
    resources={
        "s3_config":               S3Config(),
        "bronze_io_manager":       BronzeDuckDBIOManager(),
        "silver_raw_io_manager":   SilverRawDuckDBIOManager(),
        "silver_raw_age_io_manager": SilverRaw21DuckDBIOManager(),
        "silver_agg_io_manager":     SilverAggDuckDBIOManager(),
    },
)
