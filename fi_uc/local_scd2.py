"""Local PySpark SCD2 stand-in for Security Master (no DLT apply_changes)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def scd2_from_cdc(cdc: DataFrame) -> DataFrame:
    """Expand CDC rows into SCD2 with __START_AT / __END_AT."""
    w = Window.partitionBy("instrument_id").orderBy("recorded_at")
    ranked = cdc.withColumn("_next_at", F.lead("recorded_at").over(w))
    return (
        ranked.withColumn("__START_AT", F.col("recorded_at"))
        .withColumn("__END_AT", F.col("_next_at"))
        .withColumn("__CURRENT", F.col("_next_at").isNull())
        .drop("_next_at")
    )
