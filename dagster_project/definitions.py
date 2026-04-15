from dagster import Definitions, load_assets_from_modules

from dagster_project.assets import bronze_assets, bronze_normalized_assets, silver_assets
from dagster_project.resources import S3Config

all_assets = load_assets_from_modules([bronze_assets, bronze_normalized_assets, silver_assets])

defs = Definitions(
    assets=all_assets,
    resources={"s3_config": S3Config()},
)
