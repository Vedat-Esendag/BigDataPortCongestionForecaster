"""infra/k8s/spark/school: settings the school cluster needs, checked without kubectl."""

from __future__ import annotations

from pathlib import Path

import yaml

SCHOOL = Path(__file__).resolve().parents[2] / "infra" / "k8s" / "spark" / "school"


def _pairs(path: Path, sep: str | None) -> dict[str, str]:
    lines = (line.strip() for line in path.read_text().splitlines())
    pairs = (line.split(sep, 1) for line in lines if line and not line.startswith("#"))
    return {key.strip(): value.strip() for key, value in pairs}


def test_spark_defaults_target_our_namespace_with_one_hdfs_copy():
    conf = _pairs(SCHOOL / "spark-defaults.conf", None)

    assert conf["spark.kubernetes.namespace"] == "bd-gr-05"
    assert conf["spark.kubernetes.authenticate.driver.serviceAccountName"] == "spark"
    assert conf["spark.hadoop.dfs.replication"] == "1"
    assert "spark.kubernetes.container.image" not in conf  # each pod passes its own image


def test_addresses_are_the_school_service_names():
    env = _pairs(SCHOOL / "school.env", "=")

    assert env == {
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
        "HDFS_URI": "hdfs://hdfs-namenode:8020",
    }


def test_the_two_recorders_differ_only_in_the_topic():
    position = (SCHOOL / "recorder-ais-position.yaml").read_text()
    static = (SCHOOL / "recorder-ais-static.yaml").read_text()

    def normalise(text: str, topic: str, other: str) -> str:
        text = text.replace(f"recorder-{other}.yaml", "recorder-OTHER.yaml")
        return text.replace(topic, "TOPIC").replace(topic.replace(".", "-"), "TOPIC")

    assert normalise(position, "ais.position", "ais-static") == normalise(
        static, "ais.static", "ais-position"
    )


def test_deploying_the_school_folder_runs_no_one_off_job():
    """The test Jobs live in one-off/, which school/ must not list: a deploy would run them."""
    kustomization = yaml.safe_load((SCHOOL / "kustomization.yaml").read_text())
    one_off = {path.name for path in (SCHOOL / "one-off").glob("job-*.yaml")}

    assert one_off == {"job-smoke-count.yaml", "job-send-fixtures.yaml", "job-lake-check.yaml"}
    assert not [r for r in kustomization["resources"] if "job-" in r or "one-off" in r]
