"""Topic creation: the Kubernetes topic Job, scripts/create_topics.sh and the brokers' auto-create.

Issue #9: with auto-create on, a broker recreated under a running Schema Registry made `_schemas`
with the default delete policy. These tests keep the fix in place.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
JOB = REPO / "infra" / "k8s" / "base" / "create-topic-job.yaml"
SCRIPT = REPO / "scripts" / "create_topics.sh"


def job_script() -> str:
    job = yaml.safe_load(JOB.read_text())
    (container,) = job["spec"]["template"]["spec"]["containers"]
    return container["command"][-1]


def job_topics() -> dict[str, tuple[int, str]]:
    """name -> (partitions, the rest of the create_topic line) for every create_topic call."""
    calls = re.findall(r"^\s*create_topic (\S+) (\d+)(.*)$", job_script(), flags=re.MULTILINE)
    return {name: (int(partitions), rest.strip()) for name, partitions, rest in calls}


def test_the_job_creates_schemas_compacted_with_one_partition():
    partitions, options = job_topics()["_schemas"]

    assert partitions == 1
    assert options == "--config cleanup.policy=compact"
    assert "--replication-factor 1" in job_script()


def test_the_job_compacts_an_existing_schemas_topic_too():
    # --if-not-exists leaves a wrongly made _schemas as it is, so the Job also alters it.
    script = " ".join(job_script().replace("\\\n", " ").split())

    assert "--create --if-not-exists" in script
    assert (
        "kafka-configs --bootstrap-server ${BOOTSTRAP} --entity-type topics "
        "--entity-name _schemas --alter --add-config cleanup.policy=compact"
    ) in script.replace('"', "")


def test_the_job_and_create_topics_sh_create_the_same_topics():
    script = SCRIPT.read_text()
    listed = {
        name: int(partitions)
        for name, partitions in re.findall(r"^\s+([a-z][a-z._]*):(\d+)$", script, re.MULTILINE)
    }
    job = {name: partitions for name, (partitions, _) in job_topics().items()}

    assert job.pop("_schemas") == 1
    assert job == listed
    flat = " ".join(script.replace("\\\n", " ").split())
    assert (
        "--topic _schemas --partitions 1 --replication-factor 1 --config cleanup.policy=compact"
    ) in flat
    assert "--entity-name _schemas --alter --add-config cleanup.policy=compact" in flat


def kafka_env() -> dict[str, str]:
    path = REPO / "infra" / "k8s" / "base" / "kafka" / "statefulset.yaml"
    sts = yaml.safe_load(path.read_text())
    (container,) = sts["spec"]["template"]["spec"]["containers"]
    return {e["name"]: e.get("value") for e in container["env"]}


def test_brokers_never_create_topics_on_first_use():
    compose = yaml.safe_load((REPO / "infra" / "compose.services.yml").read_text())

    assert kafka_env()["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] == "false"
    assert compose["services"]["kafka"]["environment"]["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] == "false"
