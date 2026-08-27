"""Local PySpark + delta-spark checks for bronze/silver/gold and Delta mechanics.

These tests are the CI accept path. They never import `dlt_pipeline.orders_dlt`
(the `dlt` package is a Databricks workspace dependency, not installed here).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.bronze_ingest import create_spark_session, spark_uri, write_bronze
from src.delta_mechanics import (
    DEFAULT_VACUUM_RETENTION_DAYS,
    DEFAULT_VACUUM_RETENTION_HOURS,
    document_vacuum_behavior,
    merge_into_silver,
    time_travel_counts,
    vacuum_retention_note,
)
from src.generate_events import (
    MERGE_NEW_EVENT_COUNT,
    N_GOOD_EVENTS,
    N_POISON_ROWS,
    expected_bronze_count,
    generate_events,
    generate_merge_batch,
)
from src.gold_aggregate import write_gold
from src.silver_transform import parse_rows_to_silver, write_silver_and_quarantine


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = create_spark_session(warehouse)
    yield session
    session.stop()


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("delta-lake")
    paths = {
        "bronze": str(root / "bronze" / "order_events"),
        "silver": str(root / "silver" / "order_events"),
        "quarantine": str(root / "silver" / "quarantine"),
        "gold_daily": str(root / "gold" / "daily_order_metrics"),
        "gold_mix": str(root / "gold" / "event_type_mix"),
    }
    events = generate_events()
    write_bronze(spark, events, paths["bronze"])
    write_silver_and_quarantine(spark, paths["bronze"], paths["silver"], paths["quarantine"])
    write_gold(spark, paths["silver"], paths["gold_daily"], paths["gold_mix"])
    initial_silver_count = spark.read.format("delta").load(spark_uri(paths["silver"])).count()
    return {
        "paths": paths,
        "events": events,
        "initial_silver_count": initial_silver_count,
    }


def _read(spark, path: str):
    return spark.read.format("delta").load(spark_uri(path))


def test_bronze_row_count(spark, lake):
    bronze = _read(spark, lake["paths"]["bronze"])
    assert bronze.count() == expected_bronze_count()
    assert bronze.count() == len(lake["events"])


def test_quarantine_nonempty_when_poison_rows_exist(spark, lake):
    quarantine = _read(spark, lake["paths"]["quarantine"])
    assert N_POISON_ROWS > 0
    assert quarantine.count() == N_POISON_ROWS
    reasons = {row.quarantine_reason for row in quarantine.collect()}
    assert any("missing_event_id" in r for r in reasons)
    assert any("invalid_amount" in r for r in reasons)


def test_silver_event_ids_are_unique(spark, lake):
    silver = _read(spark, lake["paths"]["silver"])
    n = silver.count()
    n_distinct = silver.select("event_id").distinct().count()
    assert n == n_distinct
    assert lake["initial_silver_count"] == N_GOOD_EVENTS


def test_gold_has_rows(spark, lake):
    daily = _read(spark, lake["paths"]["gold_daily"])
    mix = _read(spark, lake["paths"]["gold_mix"])
    assert daily.count() > 0
    assert mix.count() > 0
    assert "order_count" in daily.columns
    assert "amount_sum" in daily.columns
    assert "event_type" in mix.columns


def test_merge_and_time_travel_counts(spark, lake):
    silver_path = lake["paths"]["silver"]
    updates, _quarantine = parse_rows_to_silver(spark, generate_merge_batch())
    merge_into_silver(spark, silver_path, updates)

    counts = time_travel_counts(spark, silver_path, earlier_version=0)
    assert counts["earlier_count"] == lake["initial_silver_count"]
    assert counts["current_count"] == lake["initial_silver_count"] + MERGE_NEW_EVENT_COUNT
    assert counts["earlier_count"] < counts["current_count"]

    current = _read(spark, silver_path)
    updated = current.filter("event_id = 'evt-0001'").collect()
    assert len(updated) == 1
    assert abs(updated[0].amount - 111.11) < 1e-6

    v0 = spark.read.format("delta").option("versionAsOf", 0).load(spark_uri(silver_path))
    v0_first = v0.filter("event_id = 'evt-0001'").collect()
    assert len(v0_first) == 1
    assert abs(v0_first[0].amount - 111.11) > 1e-6


def test_vacuum_is_documented_not_executed():
    note = vacuum_retention_note()
    meta = document_vacuum_behavior()
    assert DEFAULT_VACUUM_RETENTION_DAYS == 7
    assert DEFAULT_VACUUM_RETENTION_HOURS == 168
    assert meta["executed"] is False
    assert "7 days" in note
    assert "does not call VACUUM" in note


def test_dlt_pipeline_file_exists_but_is_not_imported():
    """Keep DLT as a workspace artifact; local tests must not import `dlt`."""
    dlt_path = Path(__file__).resolve().parents[1] / "dlt_pipeline" / "orders_dlt.py"
    text = dlt_path.read_text(encoding="utf-8")
    assert "@dlt.table" in text
    assert '@dlt.expect_or_drop("valid_amount", "amount >= 0")' in text
    assert "import dlt" in text
