import os, sys, logging
logging.basicConfig(level=logging.INFO)
sys.path.insert(0, os.getcwd())
try:
    from transformations.bronze_normalized.education_level_pipeline import run
    from dagster_project.assets.silver_assets import _get_catalog
    cat = _get_catalog()
    run(cat)
except Exception as e:
    import traceback
    traceback.print_exc()
