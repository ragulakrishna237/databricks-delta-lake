"""CLI: write synthetic hourly FI landing files from config.yaml."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from fi_autoloader.config import load_config  # noqa: E402
from fi_autoloader.generator import write_hourly_files  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write synthetic hourly FI files.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--landing", default=None)
    args = parser.parse_args(argv)
    cfg = load_config(args.config)
    landing = args.landing or cfg["landing"]["path"]
    paths = write_hourly_files(
        landing,
        hours=int(cfg["landing"]["hours"]),
        files_per_hour=int(cfg["landing"]["files_per_hour"]),
        rows_per_file=int(cfg["landing"]["rows_per_file"]),
        seed=int(cfg["landing"]["seed"]),
        fmt=cfg["landing"].get("format", "json"),
    )
    for path in paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
