"""Cancel Databricks job runs older than six hours.

Default is --dry-run. Requires databricks-sdk and workspace credentials when
talking to a real workspace. Tests call `runs_over_max_hours` on fixtures.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable

MAX_HOURS = 6


def _parse_ts(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        ts = value
    else:
        ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def run_duration_hours(run: dict[str, Any], *, now: datetime | None = None) -> float | None:
    start = _parse_ts(run.get("start_time") or run.get("start_time_iso"))
    if start is None:
        return None
    now = now or datetime.now(UTC)
    return (now - start).total_seconds() / 3600.0


def runs_over_max_hours(
    runs: Iterable[dict[str, Any]],
    *,
    max_hours: float = MAX_HOURS,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    flagged: list[dict[str, Any]] = []
    for run in runs:
        hours = run_duration_hours(run, now=now)
        if hours is not None and hours > max_hours:
            flagged.append({**run, "duration_hours": hours})
    return flagged


def list_active_runs(client: Any) -> list[dict[str, Any]]:
    """Normalize WorkspaceClient.jobs.list_runs(active_only=True)."""
    out: list[dict[str, Any]] = []
    for run in client.jobs.list_runs(active_only=True):
        start = getattr(run, "start_time", None)
        if isinstance(start, (int, float)) and start > 10_000_000_000:
            start_iso = datetime.fromtimestamp(start / 1000.0, tz=UTC).isoformat()
        elif isinstance(start, (int, float)):
            start_iso = datetime.fromtimestamp(start, tz=UTC).isoformat()
        else:
            start_iso = start
        out.append(
            {
                "run_id": getattr(run, "run_id", None),
                "job_id": getattr(run, "job_id", None),
                "start_time_iso": start_iso,
            }
        )
    return out


def cancel_runs(client: Any, runs: Iterable[dict[str, Any]], *, dry_run: bool = True) -> list[int]:
    cancelled: list[int] = []
    for run in runs:
        run_id = int(run["run_id"])
        if dry_run:
            print(f"dry-run: would cancel run_id={run_id} job_id={run.get('job_id')}")
        else:
            client.jobs.cancel_run(run_id)
            print(f"cancelled run_id={run_id}")
        cancelled.append(run_id)
    return cancelled


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Cancel job runs longer than 6 hours.")
    parser.add_argument("--max-hours", type=float, default=MAX_HOURS)
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args(argv)

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("databricks-sdk is not installed; listing is a no-op outside a workspace.")
        return 0

    client = WorkspaceClient()
    now = datetime.now(UTC)
    flagged = runs_over_max_hours(list_active_runs(client), max_hours=args.max_hours, now=now)
    cancel_runs(client, flagged, dry_run=args.dry_run)
    print(f"flagged={len(flagged)} cutoff={now - timedelta(hours=args.max_hours)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
