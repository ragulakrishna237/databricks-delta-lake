"""Aggregate sample billing CSVs when system tables are unavailable."""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent


def _load(name: str) -> list[dict[str, str]]:
    with (ROOT / name).open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _price_map(prices: list[dict[str, str]]) -> dict[str, float]:
    return {row["sku_name"]: float(row["list_price_usd"]) for row in prices}


def cost_rows(
    usage: list[dict[str, str]] | None = None,
    prices: list[dict[str, str]] | None = None,
    jobs: list[dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    usage = usage if usage is not None else _load("sample_usage.csv")
    prices = prices if prices is not None else _load("sample_list_prices.csv")
    jobs = jobs if jobs is not None else _load("sample_job_runs.csv")
    job_name = {row["job_id"]: row["job_name"] for row in jobs}
    plist = _price_map(prices)
    out: list[dict[str, Any]] = []
    for row in usage:
        dbus = float(row["usage_quantity"])
        price = plist[row["sku_name"]]
        out.append(
            {
                "job_id": row["job_id"],
                "job_name": job_name.get(row["job_id"], "unknown"),
                "team": row["team"],
                "workspace_id": row["workspace_id"],
                "dbus": dbus,
                "list_cost_usd": round(dbus * price, 4),
            }
        )
    return out


def summarize(rows: list[dict[str, Any]] | None = None) -> dict[str, dict[str, float]]:
    rows = rows if rows is not None else cost_rows()
    buckets: dict[str, dict[str, float]] = {
        "by_job": defaultdict(float),
        "by_team": defaultdict(float),
        "by_workspace": defaultdict(float),
    }
    for row in rows:
        buckets["by_job"][row["job_name"]] += row["list_cost_usd"]
        buckets["by_team"][row["team"]] += row["list_cost_usd"]
        buckets["by_workspace"][row["workspace_id"]] += row["list_cost_usd"]
    return {k: dict(v) for k, v in buckets.items()}


def main() -> None:
    summary = summarize()
    for label, values in summary.items():
        print(label)
        for key, cost in sorted(values.items(), key=lambda kv: -kv[1]):
            print(f"  {key}: {cost:.4f}")


if __name__ == "__main__":
    main()
