# Fixed Income cost observability

Joins Databricks **system tables** to attribute DBU spend to Lakeflow jobs, teams, and workspaces. Sample CSVs stand in when system tables are not enabled.

## System tables to verify in a real workspace

Confirm these exist and that the identity running the queries has `USE CATALOG` / `SELECT`:

| Object | Role |
|---|---|
| `system.billing.usage` | Usage records (DBUs, SKU, workspace, timestamps) |
| `system.billing.list_prices` | List price per SKU / currency |
| `system.lakeflow.jobs` | Job metadata (name, creator) |
| `system.lakeflow.job_run_timeline` | Run start/end for duration and job_id join |
| `system.lakeflow.job_tasks` | Optional task-level join |
| `system.compute.clusters` | Optional cluster tags / team mapping |
| `system.access.workspaces_latest` | Workspace display name |

Names drift across Databricks releases — verify in Catalog Explorer before scheduling.

## Files

- `sql/cost_per_job.sql`, `cost_per_team.sql`, `cost_per_workspace.sql`
- `sample_usage.csv`, `sample_list_prices.csv`, `sample_job_runs.csv`
- `aggregate_sample.py` — local aggregation over the CSVs
- `cancel_long_runs.py` — Databricks SDK: cancel jobs running longer than 6 hours (`--dry-run` default)

This is operations telemetry, not Security / Holding / Transaction Master data.
