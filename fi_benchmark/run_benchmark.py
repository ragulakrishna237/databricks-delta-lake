"""MERGE vs append-then-update on synthetic FI holdings."""

from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "fi_autoloader" / "src"))
sys.path.insert(0, str(ROOT))

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from fi_autoloader.generator import write_hourly_files
from fi_autoloader.pipeline import FI_SCHEMA, spark_uri, to_silver
from src.bronze_ingest import create_spark_session


def _file_count(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for p in path.rglob("*") if p.is_file() and p.suffix in {".parquet", ".c000"} or "part-" in p.name)


def _delta_data_files(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(
        1
        for p in path.rglob("*")
        if p.is_file() and not str(p).replace("\\", "/").count("/_delta_log/") and p.suffix == ".parquet"
    )


def _load_silver(spark: SparkSession, tmp: Path) -> tuple:
    landing = tmp / "landing"
    write_hourly_files(landing, hours=2, files_per_hour=1, rows_per_file=36, seed=7)
    raw = spark.read.schema(FI_SCHEMA).json(spark_uri(landing))
    return to_silver(raw), landing


def run_merge(spark: SparkSession, df, dest: Path) -> float:
    dest.mkdir(parents=True, exist_ok=True)
    uri = spark_uri(dest)
    start = time.perf_counter()
    df.write.format("delta").mode("overwrite").save(uri)
    replay = df.withColumn("position_qty", F.col("position_qty") * 1.01)
    target = DeltaTable.forPath(spark, uri)
    (
        target.alias("t")
        .merge(
            replay.alias("s"),
            "t.account_id = s.account_id AND t.instrument_id = s.instrument_id AND t.as_of_ts = s.as_of_ts",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return time.perf_counter() - start


def run_append_update(spark: SparkSession, df, dest: Path) -> float:
    dest.mkdir(parents=True, exist_ok=True)
    uri = spark_uri(dest)
    start = time.perf_counter()
    df.write.format("delta").mode("overwrite").save(uri)
    replay = df.withColumn("position_qty", F.col("position_qty") * 1.01)
    replay.write.format("delta").mode("append").save(uri)
    spark.sql(f"UPDATE delta.`{Path(dest).resolve().as_posix()}` SET volume = volume")
    return time.perf_counter() - start


def optimize_and_vacuum(spark: SparkSession, dest: Path) -> dict[str, int | str]:
    uri = spark_uri(dest)
    before = _delta_data_files(dest)
    spark.sql(f"OPTIMIZE delta.`{Path(dest).resolve().as_posix()}`")
    after_opt = _delta_data_files(dest)
    spark.sql(f"VACUUM delta.`{Path(dest).resolve().as_posix()}` RETAIN 168 HOURS")
    after_vac = _delta_data_files(dest)
    liquid = "n/a locally (CLUSTER BY is Databricks Runtime)"
    return {
        "files_before_optimize": before,
        "files_after_optimize": after_opt,
        "files_after_vacuum": after_vac,
        "liquid_clustering": liquid,
    }


def run(spark: SparkSession, tmp: Path) -> dict:
    df, _ = _load_silver(spark, tmp)
    merge_dir = tmp / "merge"
    append_dir = tmp / "append"
    merge_s = run_merge(spark, df, merge_dir)
    append_s = run_append_update(spark, df, append_dir)
    merge_files = optimize_and_vacuum(spark, merge_dir)
    append_files = optimize_and_vacuum(spark, append_dir)
    return {
        "merge_runtime_s": round(merge_s, 3),
        "append_runtime_s": round(append_s, 3),
        "merge": merge_files,
        "append": append_files,
    }


def markdown_table(result: dict) -> str:
    m, a = result["merge"], result["append"]
    return (
        "| Path | Runtime s (approx) | Files before OPTIMIZE | Files after OPTIMIZE | Files after VACUUM | Liquid clustering |\n"
        "|---|---|---|---|---|---|\n"
        f"| MERGE | {result['merge_runtime_s']} | {m['files_before_optimize']} | {m['files_after_optimize']} | {m['files_after_vacuum']} | {m['liquid_clustering']} |\n"
        f"| append-then-update | {result['append_runtime_s']} | {a['files_before_optimize']} | {a['files_after_optimize']} | {a['files_after_vacuum']} | {a['liquid_clustering']} |\n"
    )


def main() -> int:
    warehouse = ROOT / "spark-warehouse" / "fi_benchmark"
    warehouse.mkdir(parents=True, exist_ok=True)
    spark = create_spark_session(warehouse, app_name="fi-benchmark")
    tmp = ROOT / "data" / "fi_benchmark"
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        result = run(spark, tmp)
        table = markdown_table(result)
        (Path(__file__).resolve().parent / "RESULTS.md").write_text(table, encoding="utf-8")
        print(table)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
