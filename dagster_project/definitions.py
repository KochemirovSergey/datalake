from dagster import Definitions, load_assets_from_modules

from dagster_project.assets import bronze_assets, bronze_normalized_assets, silver_assets

all_assets = load_assets_from_modules([bronze_assets, bronze_normalized_assets, silver_assets])

defs = Definitions(assets=all_assets)
