"""Naming of versioned model folders, shared by training (writes) and the live job (reads).

Layout under the model root:  hdfs:///models/eta/v=<UTC timestamp>/
A timestamp like 20261027T101500Z sorts the same as time, so the latest version is the largest name.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import UTC, datetime

VERSION_PREFIX = "v="
_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"
_VERSION_PATTERN = re.compile(rf"^{re.escape(VERSION_PREFIX)}\d{{8}}T\d{{6}}Z$")


def version_dir_name(moment: datetime) -> str:
    """Folder name for a model trained at `moment` (naive datetimes are taken as UTC)."""
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return VERSION_PREFIX + moment.astimezone(UTC).strftime(_TIMESTAMP_FORMAT)


def is_version_dir(name: str) -> bool:
    return bool(_VERSION_PATTERN.match(name))


def latest_version(names: Iterable[str]) -> str | None:
    """Pick the newest model folder name, ignoring anything that is not a version folder."""
    versions = sorted(name for name in names if is_version_dir(name))
    return versions[-1] if versions else None
