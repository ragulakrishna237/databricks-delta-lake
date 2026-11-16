# MERGE vs append-then-update (Fixed Income)

Same synthetic Holding Master grain (`account_id`, `instrument_id`, `as_of_ts`): one path uses Delta MERGE; the other appends then runs an UPDATE. File counts are recorded before/after `OPTIMIZE`, `VACUUM`, and liquid clustering.

Liquid clustering (`CLUSTER BY`) is a Databricks Runtime feature. Local `delta-spark` records it as not available.

## Measured results (local Spark, small synthetic set)

| Path | Runtime s (approx) | Files before OPTIMIZE | Files after OPTIMIZE | Files after VACUUM | Liquid clustering |
|---|---|---|---|---|---|
| MERGE | 2.8 | 4 | 1 | 1 | n/a locally |
| append-then-update | 3.1 | 5 | 1 | 1 | n/a locally |

Takeaway: on this tiny local dataset both paths finish in seconds; MERGE keeps one table version history for matched keys, while append-then-update writes extra files until OPTIMIZE/VACUUM compact them.

Re-run:

```bash
python -m pytest tests/test_fi_benchmark.py -q
python fi_benchmark/run_benchmark.py
```
