"""Local checks for FI extensions that do not need Spark or a Databricks workspace."""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "fi_autoloader" / "src"))
sys.path.insert(0, str(ROOT / "fi_cost"))
sys.path.insert(0, str(ROOT / "fi_mlflow"))
sys.path.insert(0, str(ROOT / "fi_admin"))

from aggregate_sample import cost_rows, summarize  # noqa: E402
from cancel_long_runs import runs_over_max_hours  # noqa: E402
from duration_model import fit_linear, macaulay_duration, synthetic_training_rows  # noqa: E402
from fi_autoloader.config import load_config  # noqa: E402
from fi_autoloader.generator import FI_FIELDS, generate_hourly_rows, write_hourly_files  # noqa: E402
from list_fi_jobs import filter_fi_jobs  # noqa: E402


def test_config_and_hourly_rows_have_required_fields(tmp_path):
    cfg = load_config(ROOT / "fi_autoloader" / "config.yaml")
    assert cfg["autoloader"]["rescued_data_column"] == "_rescued_data"
    assert cfg["autoloader"]["schema_evolution_mode"] == "addNewColumns"
    assert "account_id" in cfg["gold"]["merge_keys"]
    as_of = datetime(2024, 6, 3, 9, tzinfo=UTC)
    rows = generate_hourly_rows(as_of=as_of, n=9, seed=1)
    assert rows
    assert all(all(k in row for k in FI_FIELDS) for row in rows)
    written = write_hourly_files(tmp_path / "landing", hours=1, rows_per_file=9, seed=1)
    assert written and written[0].read_text(encoding="utf-8").strip()


def test_sample_cost_joins_usage_to_list_prices():
    rows = cost_rows()
    summary = summarize(rows)
    assert summary["by_team"]["fi-data"] > 0
    assert summary["by_workspace"]
    assert "fi-autoloader-hourly" in summary["by_job"]
    sql_job = (ROOT / "fi_cost" / "sql" / "cost_per_job.sql").read_text(encoding="utf-8")
    assert "system.billing.usage" in sql_job
    assert "system.billing.list_prices" in sql_job
    assert "system.lakeflow" in sql_job


def test_cancel_runs_longer_than_six_hours():
    now = datetime(2024, 6, 3, 18, tzinfo=UTC)
    runs = [
        {"run_id": 1, "start_time_iso": (now - timedelta(hours=7)).isoformat()},
        {"run_id": 2, "start_time_iso": (now - timedelta(hours=1)).isoformat()},
    ]
    flagged = runs_over_max_hours(runs, max_hours=6, now=now)
    assert [r["run_id"] for r in flagged] == [1]


def test_duration_model_and_fi_job_filter():
    dur = macaulay_duration(0.04, 0.04, 10.0)
    assert 7.0 < dur < 10.0
    metrics = fit_linear(synthetic_training_rows(n=20))
    assert metrics["r2"] > 0.5
    jobs = filter_fi_jobs([{"name": "fi-autoloader-hourly"}, {"name": "orders-gold"}])
    assert [j["name"] for j in jobs] == ["fi-autoloader-hourly"]


def test_uc_and_dlt_files_exist_without_importing_dlt():
    uc = ROOT / "fi_uc"
    text = (uc / "dlt_security_master.py").read_text(encoding="utf-8")
    assert "apply_changes" in text
    assert "stored_as_scd_type" in text
    grants = (uc / "grants.sql").read_text(encoding="utf-8")
    assert "GRANT USE CATALOG" in grants
    assert "ROW FILTER" in (uc / "row_filter.sql").read_text(encoding="utf-8")
    assert "SET MASK" in (uc / "column_mask.sql").read_text(encoding="utf-8")
    assert "as_of_date" in (uc / "holdings_bitemporal.sql").read_text(encoding="utf-8")
    assert "recorded_at" in (uc / "holdings_bitemporal.sql").read_text(encoding="utf-8")
