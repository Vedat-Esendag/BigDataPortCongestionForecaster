"""The helper scripts: schema registration (offline parts), connector rendering, DMA download."""

from __future__ import annotations

import json
import re
import zipfile
from datetime import date
from pathlib import Path

import pytest

import download_dma
import register_schemas
import render_connectors

REPO = Path(__file__).resolve().parents[2]


# --- register_schemas -------------------------------------------------------------------------


def test_subject_is_the_topic_name_plus_value():
    assert register_schemas.subject_for(Path("ais.history.position.avsc")) == (
        "ais.history.position-value"
    )


def test_every_real_schema_file_passes_the_offline_check():
    for path in sorted((REPO / "schemas" / "avro").glob("*.avsc")):
        register_schemas.check_file(path)


def test_a_schema_that_is_not_a_record_is_rejected(tmp_path):
    bad = tmp_path / "bad.avsc"
    bad.write_text(json.dumps({"type": "string"}))

    with pytest.raises(ValueError):
        register_schemas.check_file(bad)


def test_create_topics_script_and_schema_files_list_the_same_topics():
    script = (REPO / "scripts" / "create_topics.sh").read_text()
    topics = set(re.findall(r"^\s+([a-z][a-z._]*):\d+$", script, flags=re.MULTILINE))
    schemas = {p.stem for p in (REPO / "schemas" / "avro").glob("*.avsc")}

    assert topics == schemas


# --- render_connectors ------------------------------------------------------------------------


def render(tmp_path, monkeypatch, **env):
    for name in ("DMI_API_KEY", "ALERT_WEBHOOK_URL"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    render_connectors.render(tmp_path)
    return {p.stem: json.loads(p.read_text()) for p in tmp_path.glob("*.json")}


def test_weather_connectors_are_rendered_for_the_configured_port(tmp_path, monkeypatch):
    rendered = render(tmp_path, monkeypatch)

    wind = rendered["open-meteo-wind-dkebj"]
    assert wind["transforms.Port.static.value"] == "dkebj"
    assert "{{" not in json.dumps(wind)
    assert "latitude=55.45&longitude=8.2&" in wind["camel.kamelet.http-source.url"]
    assert "open-meteo-marine-dkebj" in rendered
    assert sorted(name for name in rendered if name.startswith("open-meteo")) == [
        "open-meteo-marine-dkebj",
        "open-meteo-wind-dkebj",
    ]


def test_expansions_cover_every_port_and_dmi_only_where_a_station_exists():
    """The project has one port, so this uses invented ports to keep the multi-port rules tested."""
    ports = [
        {
            "id": "aa",
            "weather": {"open_meteo": {"lat": 1, "lon": 2}, "dmi_station_ids": ["1", "2"]},
        },
        {"id": "bb", "weather": {"open_meteo": {"lat": 3, "lon": 4}, "dmi_station_ids": []}},
        {"id": "cc", "weather": {"open_meteo": {"lat": 5, "lon": 6}}},
    ]

    per_port = list(render_connectors.expansions("{{PORT_ID}} {{OPEN_METEO_LAT}}", ports))
    per_station = list(render_connectors.expansions("{{PORT_ID}} {{DMI_STATION_ID}}", ports))
    plain = list(render_connectors.expansions("no tokens here", ports))

    assert [t["{{PORT_ID}}"] for t in per_port] == ["aa", "bb", "cc"]
    assert per_port[1]["{{OPEN_METEO_LAT}}"] == "3"
    assert [(t["{{PORT_ID}}"], t["{{DMI_STATION_ID}}"]) for t in per_station] == [
        ("aa", "1"),
        ("aa", "2"),
    ]
    assert plain == [{}]


def test_connectors_that_need_a_missing_secret_are_skipped(tmp_path, monkeypatch, capsys):
    rendered = render(tmp_path, monkeypatch)

    assert not [name for name in rendered if name.startswith("dmi-")]
    assert "webhook-sink" not in rendered
    assert {"redis-sink", "cassandra-sink", "hdfs-parquet-sink", "dma-csv-source"} <= set(rendered)
    stderr = capsys.readouterr().err
    assert "DMI_API_KEY" in stderr
    assert "ALERT_WEBHOOK_URL" in stderr


def test_dmi_connector_is_per_station_and_keeps_the_secret_unexpanded(tmp_path, monkeypatch):
    rendered = render(tmp_path, monkeypatch, DMI_API_KEY="x", ALERT_WEBHOOK_URL="http://h")

    dmi = sorted(name for name in rendered if name.startswith("dmi-"))
    assert dmi == ["dmi-wind-dkebj-06080"]
    url = rendered["dmi-wind-dkebj-06080"]["camel.kamelet.http-source.url"]
    assert url.endswith("api-key=${env:DMI_API_KEY}")  # resolved by the Connect worker, not here


def test_sink_templates_are_left_alone(tmp_path, monkeypatch):
    rendered = render(tmp_path, monkeypatch, DMI_API_KEY="x", ALERT_WEBHOOK_URL="http://h")

    content = rendered["webhook-sink"]["connect.http.request.content"]
    assert "{{value.alert_level}}" in content
    assert rendered["webhook-sink"]["connect.http.endpoint"] == "${env:ALERT_WEBHOOK_URL}"


def test_validate_response_errors_are_reported(tmp_path, capsys):
    response = tmp_path / "validate.json"
    response.write_text(
        json.dumps(
            {
                "error_count": 1,
                "configs": [{"value": {"name": "topics", "errors": ["Missing required value"]}}],
            }
        )
    )

    assert render_connectors.show_errors(response) == 1
    assert "topics: Missing required value" in capsys.readouterr().out
    response.write_text(json.dumps({"error_count": 0, "configs": []}))
    assert render_connectors.show_errors(response) == 0


# --- design rules enforced on the connector files ---------------------------------------------


def connector_config(name: str) -> dict:
    return json.loads((REPO / "connect" / name).read_text())["config"]


def topics_of(name: str) -> list[str]:
    config = connector_config(name)
    assert "topics.regex" not in config, "topic lists must be explicit, never a pattern"
    return config["topics"].split(",")


@pytest.mark.parametrize(
    "file", ["redis-sink.json", "cassandra-sink.json", "http-webhook-sink.json"]
)
def test_history_topics_never_reach_redis_cassandra_or_alerts(file):
    assert not [t for t in topics_of(file) if t.startswith("ais.history")]


def test_only_the_hdfs_sink_reads_the_history_topics():
    hdfs = topics_of("hdfs-parquet-sink.json")

    assert "ais.history.position" in hdfs
    assert "ais.history.static" in hdfs


def test_every_sink_topic_has_a_schema():
    schemas = {p.stem for p in (REPO / "schemas" / "avro").glob("*.avsc")}
    for file in ("hdfs-parquet-sink.json", "redis-sink.json", "cassandra-sink.json"):
        assert set(topics_of(file)) <= schemas, file


def test_every_cassandra_sink_topic_has_a_table_mapping():
    config = connector_config("cassandra-sink.json")
    for topic in config["topics"].split(","):
        assert any(key.startswith(f"topic.{topic}.portcongestion.") for key in config), topic


def test_cassandra_mappings_only_use_columns_that_exist():
    schema = (REPO / "cassandra" / "schema.cql").read_text()
    tables = {
        name: set(re.findall(r"^\s{2}(\w+)\s+[a-z<>]+,?$", body, flags=re.MULTILINE))
        for name, body in re.findall(
            r"CREATE TABLE IF NOT EXISTS (\w+) \((.*?)\n\)", schema, flags=re.DOTALL
        )
    }
    config = connector_config("cassandra-sink.json")
    for key, mapping in config.items():
        match = re.match(r"topic\..+\.portcongestion\.(\w+)\.mapping$", key)
        if not match:
            continue
        columns = {pair.split("=")[0].strip() for pair in mapping.split(",")}
        assert columns <= tables[match.group(1)], (match.group(1), columns - tables[match.group(1)])


# --- download_dma -----------------------------------------------------------------------------


def test_daterange_is_inclusive():
    days = list(download_dma.daterange(date(2026, 9, 29), date(2026, 10, 2)))

    assert [d.isoformat() for d in days] == ["2026-09-29", "2026-09-30", "2026-10-01", "2026-10-02"]


def make_zip(path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        for name, text in members.items():
            archive.writestr(name, text)


def test_verify_and_extract_counts_rows_without_the_header(tmp_path):
    archive = tmp_path / "day.zip"
    make_zip(archive, {"aisdk-2026-09-01.csv": "h1,h2\na,1\nb,2\nc,3\n"})

    csv_path, rows = download_dma.verify_and_extract(archive, tmp_path / "out")

    assert rows == 3
    assert csv_path.read_text().startswith("h1,h2")


def test_a_zip_without_a_csv_is_rejected(tmp_path):
    archive = tmp_path / "day.zip"
    make_zip(archive, {"readme.txt": "no data here"})

    with pytest.raises(zipfile.BadZipFile):
        download_dma.verify_and_extract(archive, tmp_path / "out")


def test_url_template_placeholders():
    url = download_dma.DEFAULT_TEMPLATE.format(date="2026-09-01", year=2026)

    assert "2026-09-01" in url
    assert "/2026/" in url
