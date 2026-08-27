"""Delta Live Tables pipeline for synthetic order events.

This module is for a Databricks workspace with DLT enabled. It has not been
run in a workspace in this repo yet. Local tests do not import this file
(the `dlt` package is not installed in CI).

Expectations here use DLT's `@dlt.expect_or_drop`. The local PySpark path
writes failures to `silver_quarantine` instead; this DLT file also materializes
a quarantine table so bad rows are still inspectable in a workspace run.
"""

import dlt
from pyspark.sql import functions as F

# Pipeline setting in a real workspace; Autoloader / volume path goes here.
LANDING_PATH = "/FileStore/tables/order_events"


@dlt.table(
    comment="Raw synthetic order events, append-only bronze.",
    table_properties={"quality": "bronze"},
)
def bronze_order_events():
    return spark.read.format("json").load(LANDING_PATH)


@dlt.table(
    comment="Rows that fail event_id / amount checks — inspectable, not silent-dropped.",
    table_properties={"quality": "silver_quarantine"},
)
def silver_quarantine():
    df = dlt.read("bronze_order_events")
    amount_num = F.col("amount").cast("double")
    event_id_ok = F.col("event_id").isNotNull() & (F.length(F.trim(F.col("event_id"))) > 0)
    amount_ok = amount_num.isNotNull() & (amount_num >= 0)
    return (
        df.filter(~(event_id_ok & amount_ok))
        .withColumn(
            "quarantine_reason",
            F.concat_ws(
                ";",
                F.when(~event_id_ok, F.lit("missing_event_id")),
                F.when(~amount_ok, F.lit("invalid_amount")),
            ),
        )
    )


@dlt.table(
    comment="Deduped, typed order events (silver).",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_amount", "amount >= 0")
def silver_order_events():
    df = dlt.read("bronze_order_events")
    amount_num = F.col("amount").cast("double")
    typed = (
        df.filter(F.col("event_id").isNotNull() & (F.length(F.trim(F.col("event_id"))) > 0))
        .withColumn("event_id", F.trim(F.col("event_id")))
        .withColumn("amount", amount_num)
        .withColumn("order_id", F.col("order_id").cast("long"))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date(F.col("event_ts")))
        .filter(F.col("amount").isNotNull())
    )
    return typed.dropDuplicates(["event_id"])


@dlt.table(
    comment="Daily distinct order_count and sum(amount) — gold metrics, not a star schema.",
    table_properties={"quality": "gold"},
)
def gold_daily_order_metrics():
    return (
        dlt.read("silver_order_events")
        .groupBy("event_date")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("amount").alias("amount_sum"),
            F.count("*").alias("event_count"),
        )
    )


@dlt.table(
    comment="event_type mix by day.",
    table_properties={"quality": "gold"},
)
def gold_event_type_mix():
    return (
        dlt.read("silver_order_events")
        .groupBy("event_date", "event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("amount").alias("amount_sum"),
        )
    )
