# Unity Catalog + investment-data model

Workspace SQL/Python for grants, a row filter, a column mask, a DLT `apply_changes` SCD2 Security Master, and a bitemporal holdings table. Local PySpark equivalents live in `local_scd2.py` and `local_bitemporal.py`.

## Domain mapping

| Investment data domain | Object |
|---|---|
| **Security Master** | `fi_invest.gold.security_master` (SCD2 via `apply_changes`) |
| **Holding Master** | `fi_invest.gold.holdings_bitemporal` (`as_of_date` + `recorded_at`) |
| **Transaction Master** | `fi_invest.gold.transactions` (cashflows / trades; see streaming-service FI topics) |
| Market-data ingest | Auto Loader landing in `fi_autoloader/` |

## Files

- `grants.sql` — catalog/schema grants
- `row_filter.sql` — account-level row filter
- `column_mask.sql` — CUSIP mask
- `dlt_security_master.py` — DLT SCD2 (workspace-only `dlt` import)
- `holdings_bitemporal.sql` — bitemporal holdings
- `local_scd2.py` / `local_bitemporal.py` — local Spark stand-ins
