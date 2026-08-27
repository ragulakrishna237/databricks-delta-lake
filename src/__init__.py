"""Local PySpark + Delta bronze/silver/gold pipeline for synthetic order events."""

from __future__ import annotations

import os
import sys

# Pip-pinned PySpark 3.5.3 must load before any leftover SPARK_HOME (e.g. Spark 3.2).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.pop("SPARK_HOME", None)
os.environ.pop("SPARK_DIST_CLASSPATH", None)
