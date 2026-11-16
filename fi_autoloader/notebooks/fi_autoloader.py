# Databricks notebook source
# MAGIC %md
# MAGIC # Fixed Income Auto Loader (hourly)
# MAGIC
# MAGIC Uses `cloudFiles` with schema evolution and `rescuedDataColumn`, writes
# MAGIC hourly-partitioned silver, then MERGE into gold Holding Master.
# MAGIC Local pytest uses the batch fallback in `fi_autoloader.pipeline`.

# COMMAND ----------

import sys
from pathlib import Path

sys.path.append(str(Path.cwd().parent / "src") if Path.cwd().name == "notebooks" else "fi_autoloader/src")

from fi_autoloader.config import load_config
from fi_autoloader.pipeline import start_cloud_files_stream, run_batch_pipeline

cfg = load_config()
if cfg["autoloader"].get("use_cloud_files"):
    query = start_cloud_files_stream(spark, cfg)
    query.awaitTermination()
else:
    print(run_batch_pipeline(spark, cfg=cfg))
