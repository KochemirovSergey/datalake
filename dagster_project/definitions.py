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
from dagster_project.io_managers import BronzeDuckDBIOManager
from dagster_project.resources import S3Config

# Файл 1_extraction_assets.py нельзя импортировать стандартным способом
# (имя начинается с цифры), поэтому используем importlib.
_assets_dir = os.path.dirname(__file__)
_spec = importlib.util.spec_from_file_location(
    "layer1_extraction_assets",
    os.path.join(_assets_dir, "assets", "1_extraction_assets.py"),
)
_layer1 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_layer1)

all_assets = load_assets_from_modules([
    bronze_assets,
    bronze_normalized_assets,
    silver_assets,
    staff_shortage_assets,
    dormitory_assets,
    gold_assets,
    _layer1,
])

defs = Definitions(
    assets=all_assets,
    resources={
        "s3_config":          S3Config(),
        "bronze_io_manager":  BronzeDuckDBIOManager(),
    },
)
