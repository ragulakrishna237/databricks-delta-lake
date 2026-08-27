"""Append-only bronze Delta table: raw order events as-is, including poison rows."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

BRONZE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("event_ts", StringType(), True),
    ]
)


def spark_uri(path: str | Path) -> str:
    """file:// URI so Spark can read local paths on Windows and Linux."""
    return Path(path).resolve().as_uri()


def create_spark_session(
    warehouse_dir: str | Path,
    app_name: str = "databricks-delta-lake",
) -> SparkSession:
    """Local SparkSession with Delta extensions via configure_spark_with_delta_pip.

    delta-spark 3.2.1 pulls io.delta:delta-spark_2.12 for PySpark 3.5.x.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # Prefer pip-pinned pyspark==3.5.3 over a leftover SPARK_HOME from another install.
    os.environ.pop("SPARK_HOME", None)
    os.environ.pop("SPARK_DIST_CLASSPATH", None)
    warehouse = spark_uri(warehouse_dir)
    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def write_events_jsonl(events: list[dict], jsonl_path: str | Path) -> Path:
    path = Path(jsonl_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in events:
            fh.write(json.dumps(row) + "\n")
    return path


def events_to_bronze_df(
    spark: SparkSession,
    events: list[dict],
    jsonl_path: str | Path | None = None,
) -> DataFrame:
    """Load events via JSON Lines so Spark does the parse on the JVM (no Python workers)."""
    if jsonl_path is None:
        jsonl_path = Path(tempfile.mkdtemp(prefix="order_events_landing_")) / "events.json"
    write_events_jsonl(events, jsonl_path)
    return spark.read.schema(BRONZE_SCHEMA).json(spark_uri(jsonl_path))


def write_bronze(spark: SparkSession, events: list[dict], bronze_path: str | Path) -> DataFrame:
    """Append raw events to a Delta path. Creates the table on first write."""
    path = spark_uri(bronze_path)
    landing = Path(bronze_path).resolve().parent / "_landing" / "events.json"
    df = events_to_bronze_df(spark, events, landing)
    df.write.format("delta").mode("append").save(path)
    return spark.read.format("delta").load(path)


def read_bronze(spark: SparkSession, bronze_path: str | Path) -> DataFrame:
    return spark.read.format("delta").load(spark_uri(bronze_path))
