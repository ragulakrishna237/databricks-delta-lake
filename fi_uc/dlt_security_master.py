"""DLT SCD2 Security Master via apply_changes.

Workspace-only: local tests do not import this file (same rule as orders_dlt.py).
"""

import dlt
from pyspark.sql.functions import col, lit


@dlt.view
def security_master_cdc():
    return (
        spark.readStream.format("delta")
        .table("fi_invest.silver.security_master_cdc")
        .withColumn("recorded_at", col("recorded_at"))
        .withColumn("__is_deleted", lit(False))
    )


dlt.create_streaming_table(
    name="security_master",
    comment="SCD2 Security Master (instrument_id grain).",
    table_properties={"quality": "gold", "domain": "security_master"},
)

dlt.apply_changes(
    target="security_master",
    source="security_master_cdc",
    keys=["instrument_id"],
    sequence_by=col("recorded_at"),
    stored_as_scd_type="2",
    track_history_column_list=["cusip", "isin", "coupon", "maturity_date", "asset_class"],
)
