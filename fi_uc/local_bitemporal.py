"""Local as-of query over bitemporal holdings."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def as_of_holdings(holdings: DataFrame, business_date: str, recorded_at: str) -> DataFrame:
    filtered = holdings.filter(
        (F.col("as_of_date") <= F.lit(business_date).cast("date"))
        & (F.col("recorded_at") <= F.lit(recorded_at).cast("timestamp"))
    )
    w = Window.partitionBy("account_id", "instrument_id").orderBy(
        F.col("as_of_date").desc(), F.col("recorded_at").desc()
    )
    return filtered.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
