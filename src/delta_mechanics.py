"""Delta mechanics that CI actually runs: MERGE upsert and VERSION AS OF time travel.

VACUUM is documented here and must not be executed in CI — it would delete
history that time-travel assertions depend on. Default retention is 7 days.
"""

from __future__ import annotations

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from src.bronze_ingest import spark_uri

# Delta default: VACUUM retains history for 7 days (168 hours).
DEFAULT_VACUUM_RETENTION_HOURS = 168
DEFAULT_VACUUM_RETENTION_DAYS = 7


def merge_into_silver(spark: SparkSession, silver_path: str | Path, updates: DataFrame) -> None:
    """Upsert into silver on event_id. Matched rows update in place; new ids insert."""
    path = spark_uri(silver_path)
    target = DeltaTable.forPath(spark, path)
    (
        target.alias("t")
        .merge(updates.alias("s"), "t.event_id = s.event_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def time_travel_counts(
    spark: SparkSession,
    table_path: str | Path,
    earlier_version: int = 0,
) -> dict[str, int]:
    """Read VERSION AS OF `earlier_version` and current; print both counts.

    Returns earlier_count / current_count for tests to assert against.
    """
    path = spark_uri(table_path)
    earlier = spark.read.format("delta").option("versionAsOf", earlier_version).load(path)
    current = spark.read.format("delta").load(path)
    earlier_count = earlier.count()
    current_count = current.count()
    print(f"VERSION AS OF {earlier_version}: {earlier_count} rows")
    print(f"current: {current_count} rows")
    return {
        "earlier_version": earlier_version,
        "earlier_count": earlier_count,
        "current_count": current_count,
    }


def vacuum_retention_note() -> str:
    """Document default VACUUM retention. Does not run VACUUM."""
    return (
        f"Delta Lake VACUUM default retention is {DEFAULT_VACUUM_RETENTION_DAYS} days "
        f"({DEFAULT_VACUUM_RETENTION_HOURS} hours). This helper documents that default "
        "and does not call VACUUM. CI never vacuums, because removing recent history "
        "would break VERSION AS OF time travel in tests."
    )


def document_vacuum_behavior() -> dict[str, object]:
    return {
        "default_retention_hours": DEFAULT_VACUUM_RETENTION_HOURS,
        "default_retention_days": DEFAULT_VACUUM_RETENTION_DAYS,
        "executed": False,
        "note": vacuum_retention_note(),
    }
