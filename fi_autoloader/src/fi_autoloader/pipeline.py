"""Hourly Auto Loader-style ingest into silver, MERGE into gold.

On Databricks, `autoloader_read_stream` uses `cloudFiles` with schema evolution
and `rescuedDataColumn`. Local PySpark has no Auto Loader source, so tests and
the CLI use a batch JSON read with the same MERGE keys.

Silver is partitioned by `as_of_hour`. Gold is the current Holding Master
view keyed by (account_id, instrument_id, as_of_ts).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from fi_autoloader.config import load_config

FI_SCHEMA = StructType(
    [
        StructField("instrument_id", StringType(), True),
        StructField("cusip", StringType(), True),
        StructField("isin", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("yield", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("position_qty", DoubleType(), True),
        StructField("account_id", StringType(), True),
        StructField("as_of_ts", StringType(), True),
    ]
)


def spark_uri(path: str | Path) -> str:
    return Path(path).resolve().as_uri()


def autoloader_read_stream(spark: SparkSession, cfg: dict[str, Any]) -> DataFrame:
    """Databricks Auto Loader (`cloudFiles`). Not used by local pytest."""
    auto = cfg["autoloader"]
    landing = cfg["landing"]["path"]
    fmt = cfg["landing"].get("format", "json")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaLocation", auto["schema_location"])
        .option("cloudFiles.inferColumnTypes", str(auto.get("infer_column_types", True)).lower())
        .option("cloudFiles.schemaEvolutionMode", auto.get("schema_evolution_mode", "addNewColumns"))
        .option("rescuedDataColumn", auto.get("rescued_data_column", "_rescued_data"))
        .schema(FI_SCHEMA)
        .load(landing)
    )


def batch_read_landing(spark: SparkSession, landing_path: str | Path) -> DataFrame:
    """Local stand-in for Auto Loader: typed JSON plus a rescued-data column."""
    df = spark.read.schema(FI_SCHEMA).json(spark_uri(landing_path))
    return df.withColumn("_rescued_data", F.lit(None).cast(StringType()))


def to_silver(df: DataFrame) -> DataFrame:
    as_of = F.to_timestamp("as_of_ts")
    return (
        df.withColumn("as_of_ts", as_of)
        .withColumn("as_of_date", F.to_date(as_of))
        .withColumn("as_of_hour", F.date_format(as_of, "yyyy-MM-dd HH:00:00"))
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("yield", F.col("yield").cast(DoubleType()))
        .withColumn("volume", F.col("volume").cast(DoubleType()))
        .withColumn("position_qty", F.col("position_qty").cast(DoubleType()))
        .filter(F.col("instrument_id").isNotNull())
        .filter(F.col("account_id").isNotNull())
        .filter(F.col("as_of_ts").isNotNull())
    )


def write_silver(silver: DataFrame, silver_path: str | Path) -> None:
    (
        silver.write.format("delta")
        .mode("overwrite")
        .partitionBy("as_of_hour")
        .save(spark_uri(silver_path))
    )


def merge_into_gold(
    spark: SparkSession,
    silver_path: str | Path,
    gold_path: str | Path,
    merge_keys: list[str] | None = None,
) -> None:
    keys = merge_keys or ["account_id", "instrument_id", "as_of_ts"]
    predicate = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    silver = spark.read.format("delta").load(spark_uri(silver_path))
    gold_uri = spark_uri(gold_path)
    if not DeltaTable.isDeltaTable(spark, gold_uri):
        silver.write.format("delta").mode("overwrite").save(gold_uri)
        return
    target = DeltaTable.forPath(spark, gold_uri)
    (
        target.alias("t")
        .merge(silver.alias("s"), predicate)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_batch_pipeline(
    spark: SparkSession,
    config_path: str | Path | None = None,
    cfg: dict[str, Any] | None = None,
) -> dict[str, int]:
    cfg = cfg or load_config(config_path)
    landing = cfg["landing"]["path"]
    silver_path = cfg["silver"]["path"]
    gold_path = cfg["gold"]["path"]
    silver = to_silver(batch_read_landing(spark, landing))
    write_silver(silver, silver_path)
    merge_into_gold(spark, silver_path, gold_path, cfg["gold"].get("merge_keys"))
    silver_n = spark.read.format("delta").load(spark_uri(silver_path)).count()
    gold_n = spark.read.format("delta").load(spark_uri(gold_path)).count()
    return {"silver_rows": silver_n, "gold_rows": gold_n}


def start_cloud_files_stream(spark: SparkSession, cfg: dict[str, Any]):
    """Workspace-only: Auto Loader -> hourly silver -> foreachBatch MERGE gold."""
    silver_path = cfg["silver"]["path"]
    gold_path = cfg["gold"]["path"]
    checkpoint = cfg["autoloader"]["checkpoint_location"]
    stream_df = to_silver(autoloader_read_stream(spark, cfg))

    def _foreach_batch(batch: DataFrame, _batch_id: int) -> None:
        if batch.rdd.isEmpty():
            return
        (
            batch.write.format("delta")
            .mode("append")
            .partitionBy("as_of_hour")
            .save(spark_uri(silver_path))
        )
        merge_into_gold(spark, silver_path, gold_path, cfg["gold"].get("merge_keys"))

    return (
        stream_df.writeStream.foreachBatch(_foreach_batch)
        .option("checkpointLocation", checkpoint)
        .outputMode("update")
        .start()
    )
