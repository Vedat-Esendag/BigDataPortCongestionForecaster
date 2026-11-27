"""infra/k8s/spark/school: settings the school cluster needs, checked without kubectl."""

from __future__ import annotations

from pathlib import Path

import yaml

K8S = Path(__file__).resolve().parents[2] / "infra" / "k8s"
SCHOOL = K8S / "spark" / "school"


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

    assert one_off == {
        "job-smoke-count.yaml",
        "job-send-fixtures.yaml",
        "job-lake-check.yaml",
        "job-scratch-check.yaml",
    }
    assert not [r for r in kustomization["resources"] if "job-" in r or "one-off" in r]


SCRATCH = "/spark-local"


def _scratch_settings(conf: dict[str, str], role: str) -> dict[str, str]:
    prefix = f"spark.kubernetes.{role}.volumes.persistentVolumeClaim.spark-local-dir-1."
    return {k.removeprefix(prefix): v for k, v in conf.items() if k.startswith(prefix)}


def test_spark_pods_get_scratch_space_on_the_data_disk():
    """#22: temp and shuffle files on on-demand hostpath-custom claims, owned by the driver."""
    conf = _pairs(SCHOOL / "spark-defaults.conf", None)

    for role in ("driver", "executor"):
        assert _scratch_settings(conf, role) == {
            "options.claimName": "OnDemand",
            "options.storageClass": "hostpath-custom",
            "options.sizeLimit": "20Gi",
            "mount.path": SCRATCH,
            "mount.readOnly": "false",
        }
        assert conf[f"spark.{role}.defaultJavaOptions"] == f"-Djava.io.tmpdir={SCRATCH}"
    assert conf["spark.kubernetes.driver.ownPersistentVolumeClaim"] == "true"
    assert conf["spark.kubernetes.driver.reusePersistentVolumeClaim"] == "true"
    assert conf["spark.kubernetes.driverEnv.TMPDIR"] == SCRATCH
    assert conf["spark.executorEnv.TMPDIR"] == SCRATCH


def test_local_overlay_uses_the_same_scratch_mount_on_kinds_storage_class():
    school = _pairs(SCHOOL / "spark-defaults.conf", None)
    local = _pairs(K8S / "overlays" / "local" / "spark-defaults.conf", None)

    for role in ("driver", "executor"):
        expected = _scratch_settings(school, role)
        # Spark creates an on-demand claim only when both are set, so neither may be left out.
        expected["options.storageClass"] = "standard"
        expected["options.sizeLimit"] = "5Gi"
        assert _scratch_settings(local, role) == expected


def _pod_specs(path: Path) -> list[dict]:
    docs = [d for d in yaml.safe_load_all(path.read_text()) if d]
    return [d["spec"]["template"]["spec"] for d in docs if d["kind"] in ("StatefulSet", "Job")]


def test_spark_drivers_in_their_own_pod_mount_scratch_on_the_data_disk():
    """The recorders (client mode) and lake-check (local mode) are pods Spark does not create."""
    paths = [
        SCHOOL / "recorder-ais-position.yaml",
        SCHOOL / "recorder-ais-static.yaml",
        SCHOOL / "one-off" / "job-lake-check.yaml",
    ]
    for path in paths:
        (spec,) = _pod_specs(path)
        (container,) = spec["containers"]
        env = {e["name"]: e.get("value") for e in container["env"]}
        mounts = {m["mountPath"]: m["name"] for m in container["volumeMounts"]}
        volumes = {v["name"]: v for v in spec["volumes"]}

        assert env["SPARK_LOCAL_DIRS"] == SCRATCH, path.name
        assert env["TMPDIR"] == SCRATCH, path.name
        claim = volumes[mounts[SCRATCH]]["ephemeral"]["volumeClaimTemplate"]["spec"]
        assert claim["storageClassName"] == "hostpath-custom", path.name
