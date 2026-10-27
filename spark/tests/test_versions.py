"""versions.env stays the one place for versions: the pip pin must agree with it."""

from __future__ import annotations

from pathlib import Path

import pyspark

VERSIONS_FILE = Path(__file__).resolve().parents[2] / "versions.env"


def _versions() -> dict[str, str]:
    pairs = (
        line.split("=", 1)
        for line in VERSIONS_FILE.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    )
    return {key.strip(): value.strip() for key, value in pairs}


def test_pyspark_matches_spark_version():
    assert pyspark.__version__ == _versions()["SPARK_VERSION"]


def test_python_version_pins_match_versions_env():
    """uv reads .python-version from the project folder, so each project carries its own copy."""
    root = VERSIONS_FILE.parent
    wanted = _versions()["PYTHON_VERSION"]
    for folder in (".", "spark", "services/api", "services/ais-producer"):
        assert (root / folder / ".python-version").read_text().strip() == wanted, folder


def test_spark_packages_match_spark_version():
    versions = _versions()
    assert versions["SPARK_KAFKA_PACKAGE"].endswith(f":{versions['SPARK_VERSION']}")
    assert versions["SPARK_AVRO_PACKAGE"].endswith(f":{versions['SPARK_VERSION']}")
    assert versions["SPARK_IMAGE_TAG"].startswith(versions["SPARK_VERSION"])
