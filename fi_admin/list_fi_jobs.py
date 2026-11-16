"""List FI-prefixed Databricks jobs via the Jobs API.

Without databricks-sdk or credentials this prints a skip message.
"""

from __future__ import annotations

import argparse
from typing import Any


def filter_fi_jobs(jobs: list[dict[str, Any]], prefix: str = "fi-") -> list[dict[str, Any]]:
    return [j for j in jobs if str(j.get("name") or "").startswith(prefix)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", default="fi-")
    args = parser.parse_args(argv)
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("databricks-sdk is not installed; no workspace listing.")
        return 0
    client = WorkspaceClient()
    jobs = [{"job_id": j.job_id, "name": j.settings.name} for j in client.jobs.list()]
    for job in filter_fi_jobs(jobs, args.prefix):
        print(job)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
