"""Shared fixtures: a local SparkSession and the small AIS sample CSV."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from pathlib import Path

# Spark hands timestamps back to Python in the machine's local time zone. Pin the whole test
# process to UTC, before any Spark code runs, so results do not depend on where tests are run.
os.environ["TZ"] = "UTC"
time.tzset()

import pytest  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from common.schemas import POSITION_CSV_SCHEMA  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"
VERSIONS_FILE = Path(__file__).resolve().parents[2] / "versions.env"
MIN_JAVA = 17  # Spark 4.2 runs on 17 and 21; this project uses 21


def _avro_package() -> str:
    """spark-avro is not bundled with pip's PySpark. Use the same package spark-submit uses."""
    for line in VERSIONS_FILE.read_text().splitlines():
        if line.startswith("SPARK_AVRO_PACKAGE="):
            return line.split("=", 1)[1].strip()
    raise RuntimeError("SPARK_AVRO_PACKAGE missing from versions.env")


def _java_major() -> int | None:
    home = os.environ.get("JAVA_HOME")
    java = str(Path(home) / "bin" / "java") if home else shutil.which("java")
    if not java or not Path(java).exists():
        return None
    result = subprocess.run([java, "-version"], capture_output=True, text=True, check=False)
    match = re.search(r'version "(\d+)', result.stderr)
    return int(match.group(1)) if match else None


@pytest.fixture(scope="session")
def spark():
    major = _java_major()
    if major is None or major < MIN_JAVA:
        pytest.fail(
            f"Spark needs Java {MIN_JAVA}+ (found {major or 'none'}). Install Java 21 and point "
            "JAVA_HOME at it. Steps for macOS, Ubuntu and Windows (WSL2) are in docs/setup.md."
        )
    session = (
        SparkSession.builder.master("local[2]")
        .appName("port-congestion-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.jars.packages", _avro_package())
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def ais_sample(spark: SparkSession) -> DataFrame:
    """20 position rows: 1 exact duplicate, 1 impossible speed, 2 bad MMSI, 1 bad latitude."""
    return spark.read.csv(str(FIXTURES / "ais_sample.csv"), header=True, schema=POSITION_CSV_SCHEMA)
