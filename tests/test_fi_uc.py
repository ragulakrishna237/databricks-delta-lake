"""Local PySpark SCD2 + bitemporal holdings (workspace SQL is not imported)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from fi_uc.local_bitemporal import as_of_holdings
from fi_uc.local_scd2 import scd2_from_cdc
from src.bronze_ingest import create_spark_session


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = create_spark_session(tmp_path_factory.mktemp("fi-uc-wh"), app_name="fi-uc-tests")
    yield session
    session.stop()


def test_scd2_marks_current_and_end_dates(spark):
    from pyspark.sql import functions as F

    cdc = spark.createDataFrame(
        [
            ("UST_10Y", "91282CJL6", "2024-01-01T00:00:00"),
            ("UST_10Y", "91282XXXX", "2024-06-01T00:00:00"),
        ],
        "instrument_id string, cusip string, recorded_at string",
    ).withColumn("recorded_at", F.to_timestamp("recorded_at"))
    out = scd2_from_cdc(cdc).orderBy("recorded_at").collect()
    assert out[0]["__CURRENT"] is False
    assert out[1]["__CURRENT"] is True
    assert out[0]["__END_AT"] is not None


def test_bitemporal_as_of_picks_latest_knowledge(spark):
    from pyspark.sql import functions as F

    holdings = spark.createDataFrame(
        [
            ("ACC-0001", "UST_10Y", 100.0, "2024-06-01", "2024-06-01T10:00:00"),
            ("ACC-0001", "UST_10Y", 80.0, "2024-06-03", "2024-06-04T09:00:00"),
            ("ACC-0001", "UST_10Y", 90.0, "2024-06-03", "2024-06-05T09:00:00"),
        ],
        "account_id string, instrument_id string, position_qty double, as_of_date string, recorded_at string",
    ).select(
        "account_id",
        "instrument_id",
        "position_qty",
        F.to_date("as_of_date").alias("as_of_date"),
        F.to_timestamp("recorded_at").alias("recorded_at"),
    )
    as_of = as_of_holdings(holdings, "2024-06-03", "2024-06-04 12:00:00").collect()
    assert len(as_of) == 1
    assert as_of[0]["position_qty"] == 80.0
