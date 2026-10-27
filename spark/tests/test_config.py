"""config/*.yaml loads and holds sane geometry."""

from __future__ import annotations

import pytest

from common.config import LAKE_ZONES, get_port, lake_path, load_ports, load_thresholds, model_root


def _inside(bbox, polygon):
    return all(
        bbox.min_lon <= lon <= bbox.max_lon and bbox.min_lat <= lat <= bbox.max_lat
        for lon, lat in polygon
    )


def test_ports_load():
    ports = load_ports()

    assert [p.id for p in ports] == ["dkebj"]
    assert get_port(ports, "dkebj").name == "Esbjerg"


def test_every_port_polygon_is_closed_and_inside_its_bbox():
    for port in load_ports():
        assert port.bbox.min_lat < port.bbox.max_lat
        assert port.bbox.min_lon < port.bbox.max_lon
        for polygon in (port.approach_zone, port.anchorage, port.berth_area):
            assert polygon[0] == polygon[-1]
            assert _inside(port.bbox, polygon), port.id


def test_country_codes_are_strings():
    # Some unquoted ISO codes (NO, for example) parse as the boolean False in YAML, so a port
    # added later could break silently. Check the type explicitly.
    assert [p.country for p in load_ports()] == ["DK"]


def test_unknown_port_raises():
    with pytest.raises(KeyError):
        get_port(load_ports(), "zzzzz")


def test_thresholds_load():
    thresholds = load_thresholds()

    assert thresholds.late_minutes == 30
    assert thresholds.horizons_hours == (24, 48)
    assert thresholds.critical.late_count > thresholds.warning.late_count


def test_lake_paths(monkeypatch):
    monkeypatch.setenv("HDFS_URI", "hdfs://nn:8020")
    monkeypatch.setenv("LAKE_ROOT", "/lake")

    assert lake_path("curated", "features") == "hdfs://nn:8020/lake/curated/features"
    assert lake_path("quarantine") == "hdfs://nn:8020/lake/quarantine"
    assert set(LAKE_ZONES) == {"landing", "raw", "standardized", "curated", "quarantine"}
    with pytest.raises(ValueError):
        lake_path("bronze")


def test_model_root(monkeypatch):
    monkeypatch.setenv("HDFS_URI", "hdfs://nn:8020")
    monkeypatch.setenv("MODEL_ROOT", "/models/eta")

    assert model_root() == "hdfs://nn:8020/models/eta"
