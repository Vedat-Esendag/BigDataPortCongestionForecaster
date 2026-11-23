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


def test_dockerfile_defaults_match_versions_env():
    """docker build without --build-arg must bake in the same versions compose passes."""
    dockerfile = (VERSIONS_FILE.parent / "spark" / "Dockerfile").read_text()
    defaults = dict(
        line.removeprefix("ARG ").split("=", 1)
        for line in dockerfile.splitlines()
        if line.startswith("ARG ") and "=" in line
    )
    versions = _versions()
    for name in ("SPARK_IMAGE_TAG", "SPARK_KAFKA_PACKAGE", "SPARK_AVRO_PACKAGE"):
        assert defaults[name] == versions[name], name


def test_sender_confluent_kafka_matches_the_producer_lock():
    """The test sender in the Spark image uses the same confluent-kafka as services/ais-producer."""
    root = VERSIONS_FILE.parent
    dockerfile = (root / "spark" / "Dockerfile").read_text()
    pinned = next(
        line.split("=", 1)[1]
        for line in dockerfile.splitlines()
        if line.startswith("ARG CONFLUENT_KAFKA_VERSION=")
    )
    lock = (root / "services" / "ais-producer" / "uv.lock").read_text()
    locked = lock.split('name = "confluent-kafka"\nversion = "', 1)[1].split('"', 1)[0]
    assert pinned == locked
