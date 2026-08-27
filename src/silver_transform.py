"""Silver: dedupe on event_id, enforce schema, quarantine bad rows (never silent-drop)."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.bronze_ingest import events_to_bronze_df, spark_uri

SILVER_COLUMNS = (
    "event_id",
    "order_id",
    "customer_id",
    "event_type",
    "amount",
    "event_ts",
    "event_date",
)


def _event_id_ok() -> F.Column:
    return F.col("event_id").isNotNull() & (F.length(F.trim(F.col("event_id"))) > 0)


def _amount_ok() -> F.Column:
    amount_num = F.col("amount").cast("double")
    return amount_num.isNotNull() & (amount_num >= 0)


def quarantine_reason() -> F.Column:
    return F.concat_ws(
        ";",
        F.when(~_event_id_ok(), F.lit("missing_event_id")),
        F.when(~_amount_ok(), F.lit("invalid_amount")),
    )


def split_bronze(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid typed+deduped silver rows, quarantine rows with reason).

    Valid: event_id present (non-empty) and amount numeric >= 0.
    Invalid rows go to quarantine with quarantine_reason — they are not dropped.
    """
    is_valid = _event_id_ok() & _amount_ok()
    quarantined = (
        df.filter(~is_valid)
        .withColumn("quarantine_reason", quarantine_reason())
        .withColumn("quarantined_at", F.current_timestamp())
    )

    typed = (
        df.filter(is_valid)
        .withColumn("event_id", F.trim(F.col("event_id")))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("order_id", F.col("order_id").cast("long"))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date(F.col("event_ts")))
        .select(*SILVER_COLUMNS)
    )
    window = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc_nulls_last())
    silver = typed.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
    return silver, quarantined


def parse_rows_to_silver(spark: SparkSession, events: list[dict]) -> tuple[DataFrame, DataFrame]:
    raw = events_to_bronze_df(spark, events)
    return split_bronze(raw)


def write_silver_and_quarantine(
    spark: SparkSession,
    bronze_path: str | Path,
    silver_path: str | Path,
    quarantine_path: str | Path,
) -> tuple[DataFrame, DataFrame]:
    bronze = spark.read.format("delta").load(spark_uri(bronze_path))
    silver, quarantined = split_bronze(bronze)
    silver.write.format("delta").mode("overwrite").save(spark_uri(silver_path))
    quarantined.write.format("delta").mode("overwrite").save(spark_uri(quarantine_path))
    return (
        spark.read.format("delta").load(spark_uri(silver_path)),
        spark.read.format("delta").load(spark_uri(quarantine_path)),
    )
