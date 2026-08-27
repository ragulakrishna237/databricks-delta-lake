"""Gold aggregates over silver order events — metrics, not a star schema."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.bronze_ingest import spark_uri


def daily_order_metrics(silver: DataFrame) -> DataFrame:
    """order_count (distinct order_id) and sum(amount) by event_date."""
    return (
        silver.groupBy("event_date")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("amount").alias("amount_sum"),
            F.count("*").alias("event_count"),
        )
        .orderBy("event_date")
    )


def event_type_mix(silver: DataFrame) -> DataFrame:
    """Status-like mix: event_type counts and amounts by day."""
    return (
        silver.groupBy("event_date", "event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("amount").alias("amount_sum"),
        )
        .orderBy("event_date", "event_type")
    )


def write_gold(
    spark: SparkSession,
    silver_path: str | Path,
    gold_daily_path: str | Path,
    gold_mix_path: str | Path,
) -> tuple[DataFrame, DataFrame]:
    silver = spark.read.format("delta").load(spark_uri(silver_path))
    daily = daily_order_metrics(silver)
    mix = event_type_mix(silver)
    daily.write.format("delta").mode("overwrite").save(spark_uri(gold_daily_path))
    mix.write.format("delta").mode("overwrite").save(spark_uri(gold_mix_path))
    return (
        spark.read.format("delta").load(spark_uri(gold_daily_path)),
        spark.read.format("delta").load(spark_uri(gold_mix_path)),
    )
