"""Synthetic Fixed Income market + position files (hourly landing).

Identifiers, prices, yields, and quantities are invented. Domain mapping:
  instrument_id / cusip / isin  -> Security Master
  account_id / position_qty     -> Holding Master
  price / yield / volume        -> market-data ingest
"""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

FI_FIELDS = (
    "instrument_id",
    "cusip",
    "isin",
    "price",
    "yield",
    "volume",
    "position_qty",
    "account_id",
    "as_of_ts",
)

INSTRUMENTS: tuple[dict[str, Any], ...] = (
    {
        "instrument_id": "UST_2Y",
        "cusip": "91282CFA4",
        "isin": "US91282CFA43",
        "base_price": 99.75,
        "base_yield": 0.0420,
    },
    {
        "instrument_id": "UST_10Y",
        "cusip": "91282CJL6",
        "isin": "US91282CJL64",
        "base_price": 98.50,
        "base_yield": 0.0435,
    },
    {
        "instrument_id": "UST_30Y",
        "cusip": "912810TW8",
        "isin": "US912810TW80",
        "base_price": 97.25,
        "base_yield": 0.0460,
    },
    {
        "instrument_id": "FNCL_5_5",
        "cusip": "3140X5ABC",
        "isin": "US3140X5ABC1",
        "base_price": 101.12,
        "base_yield": 0.0510,
    },
    {
        "instrument_id": "G2SF_5_0",
        "cusip": "36202ABC1",
        "isin": "US36202ABC13",
        "base_price": 99.40,
        "base_yield": 0.0505,
    },
    {
        "instrument_id": "USD_IRS_10Y",
        "cusip": "SWAP10Y001",
        "isin": "XS000SWAP10Y",
        "base_price": 100.00,
        "base_yield": 0.0395,
    },
    {
        "instrument_id": "USD_CAP_3Y_4P00",
        "cusip": "CAP3Y40001",
        "isin": "XS000CAP3Y40",
        "base_price": 0.85,
        "base_yield": 0.0400,
    },
    {
        "instrument_id": "USD_FLR_3Y_2P50",
        "cusip": "FLR3Y25001",
        "isin": "XS000FLR3Y25",
        "base_price": 0.42,
        "base_yield": 0.0250,
    },
    {
        "instrument_id": "USD_SWN_5Y5Y",
        "cusip": "SWN5Y5Y001",
        "isin": "XS000SWN5Y5Y",
        "base_price": 1.15,
        "base_yield": 0.0380,
    },
)

ACCOUNTS = tuple(f"ACC-{i:04d}" for i in range(1, 13))


def _iso_ts(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def generate_hourly_rows(*, as_of: datetime, n: int, seed: int = 42) -> list[dict[str, Any]]:
    rng = random.Random(seed + int(as_of.timestamp()))
    rows: list[dict[str, Any]] = []
    for i in range(n):
        inst = INSTRUMENTS[i % len(INSTRUMENTS)]
        account = ACCOUNTS[i % len(ACCOUNTS)]
        px_jitter = rng.uniform(-0.25, 0.25)
        yld_jitter = rng.uniform(-0.0015, 0.0015)
        rows.append(
            {
                "instrument_id": inst["instrument_id"],
                "cusip": inst["cusip"],
                "isin": inst["isin"],
                "price": round(inst["base_price"] + px_jitter, 6),
                "yield": round(max(0.0001, inst["base_yield"] + yld_jitter), 6),
                "volume": round(rng.uniform(1e6, 2.5e7), 2),
                "position_qty": round(rng.uniform(1e4, 5e6), 2),
                "account_id": account,
                "as_of_ts": _iso_ts(as_of),
            }
        )
    return rows


def write_hourly_files(
    landing_dir: str | Path,
    *,
    hours: int = 3,
    files_per_hour: int = 1,
    rows_per_file: int = 48,
    seed: int = 42,
    fmt: str = "json",
    start: datetime | None = None,
) -> list[Path]:
    """Write hourly JSON files under landing/yyyy/mm/dd/hh/.

    Parquet is produced when Spark is available (see `write_hourly_files_spark`).
    """
    landing = Path(landing_dir)
    landing.mkdir(parents=True, exist_ok=True)
    start = start or datetime(2024, 6, 3, 9, tzinfo=UTC)
    written: list[Path] = []
    suffix = "json"
    _ = fmt
    for h in range(hours):
        as_of = start + timedelta(hours=h)
        hour_dir = landing / f"{as_of:%Y}" / f"{as_of:%m}" / f"{as_of:%d}" / f"{as_of:%H}"
        hour_dir.mkdir(parents=True, exist_ok=True)
        for fidx in range(files_per_hour):
            rows = generate_hourly_rows(as_of=as_of, n=rows_per_file, seed=seed + h * 17 + fidx)
            path = hour_dir / f"fi_ticks_{as_of:%Y%m%dT%H}_{fidx:02d}.{suffix}"
            path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
            written.append(path)
    return written


def write_hourly_files_spark(spark, landing_dir: str | Path, **kwargs) -> list[Path]:
    """Same generator, persisted as Parquet via Spark when format=parquet."""
    json_paths = write_hourly_files(landing_dir, fmt="json", **kwargs)
    parquet_paths: list[Path] = []
    for json_path in json_paths:
        parquet_path = json_path.with_suffix(".parquet")
        df = spark.read.json(json_path.resolve().as_uri())
        df.write.mode("overwrite").parquet(parquet_path.resolve().as_uri())
        parquet_paths.append(parquet_path)
    return parquet_paths
