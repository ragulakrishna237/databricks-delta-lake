"""Local PySpark path for FI Auto Loader (batch stand-in for cloudFiles)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "fi_autoloader" / "src"))

from fi_autoloader.generator import write_hourly_files
from fi_autoloader.pipeline import run_batch_pipeline
from src.bronze_ingest import create_spark_session, spark_uri


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("fi-spark-warehouse")
    session = create_spark_session(warehouse, app_name="fi-autoloader-tests")
    yield session
    session.stop()


def test_hourly_silver_and_gold_merge(spark, tmp_path):
    landing = tmp_path / "landing"
    write_hourly_files(landing, hours=2, files_per_hour=1, rows_per_file=18, seed=3)
    cfg = {
        "landing": {"path": str(landing), "format": "json"},
        "autoloader": {
            "use_cloud_files": False,
            "schema_evolution_mode": "addNewColumns",
            "rescued_data_column": "_rescued_data",
        },
        "silver": {"path": str(tmp_path / "silver"), "partition_by": "as_of_hour"},
        "gold": {
            "path": str(tmp_path / "gold"),
            "merge_keys": ["account_id", "instrument_id", "as_of_ts"],
        },
    }
    first = run_batch_pipeline(spark, cfg=cfg)
    second = run_batch_pipeline(spark, cfg=cfg)
    assert first["silver_rows"] == first["gold_rows"] > 0
    assert second["gold_rows"] == first["gold_rows"]
    silver = spark.read.format("delta").load(spark_uri(cfg["silver"]["path"]))
    assert "as_of_hour" in silver.columns
    assert "_rescued_data" in silver.columns
