from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

from common.model_paths import is_version_dir, latest_version, version_dir_name


def test_version_dir_name_is_utc_and_sortable():
    assert version_dir_name(datetime(2026, 10, 27, 10, 15, 0, tzinfo=UTC)) == "v=20261027T101500Z"
    assert version_dir_name(datetime(2026, 10, 27, 10, 15, 0)) == "v=20261027T101500Z"
    plus_two = timezone(timedelta(hours=2))
    assert (
        version_dir_name(datetime(2026, 10, 27, 12, 15, 0, tzinfo=plus_two)) == "v=20261027T101500Z"
    )


def test_latest_version_picks_the_newest_and_ignores_other_folders():
    names = ["v=20260101T000000Z", "v=20261027T101500Z", "_tmp", "v=20260601T000000Z", "latest"]

    assert latest_version(names) == "v=20261027T101500Z"
    assert latest_version(["_tmp"]) is None
    assert latest_version([]) is None


def test_is_version_dir():
    assert is_version_dir("v=20261027T101500Z")
    assert not is_version_dir("v=2026-10-27")
    assert not is_version_dir("20261027T101500Z")
