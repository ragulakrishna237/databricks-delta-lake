# Fixed Income Auto Loader

Hourly landing files of synthetic Fixed Income market data and positions ingest into a partitioned silver Delta table, then MERGE into a gold Holding Master snapshot.

| Layer | Domain |
|---|---|
| Landing files (`instrument_id`, `cusip`, `isin`, `price`, `yield`) | Security Master + market-data ingest |
| Silver `fi_market_positions` partitioned by `as_of_hour` | Staging |
| Gold MERGE on `account_id` + `instrument_id` + `as_of_ts` | Holding Master current view |

Databricks Auto Loader (`cloudFiles`, schema evolution, `rescuedDataColumn`) lives in `notebooks/fi_autoloader.py`. Local pytest uses the batch JSON fallback in `src/fi_autoloader/pipeline.py` because `cloudFiles` is not a local Spark source.

```bash
python fi_autoloader/generate_hourly_files.py
# then, with a Spark session pointed at config.yaml paths:
# run_batch_pipeline(spark, "fi_autoloader/config.yaml")
```

Package the library:

```bash
python -m pip install build
python -m build --wheel fi_autoloader
```

The original e-commerce bronze/silver/gold path under `src/` is unchanged.
