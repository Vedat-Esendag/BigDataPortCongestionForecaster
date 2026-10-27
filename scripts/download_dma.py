#!/usr/bin/env python3
"""Download a date range of Danish Maritime Authority (DMA) daily AIS files and check them.

For every day the script downloads the zip, tests that it unzips, extracts the CSV and reports
the zip size and the CSV row count. Files that already exist are not downloaded again.
Warning: one day is large (a zip of a few hundred MB, a CSV of 1 to 3 GB), so start with
one or two days.

The URL is built from a template. The default was NOT verified against the live DMA site when
this script was written (the host was unreachable from the dev sandbox). Check the current
layout on https://aisdata.ais.dk and override with --url-template or DMA_URL_TEMPLATE.
Available placeholders: {date} (YYYY-MM-DD), {year}.

    python3 scripts/download_dma.py --start 2026-09-01 --end 2026-09-02
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import urllib.error
import urllib.request
import zipfile
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path

DEFAULT_TEMPLATE = "https://aisdata.ais.dk/{year}/aisdk-{date}.zip"
CHUNK = 1 << 20


def daterange(start: date, end: date) -> Iterator[date]:
    """Yield every date from start to end, both inclusive."""
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def download(url: str, target: Path, retries: int = 3) -> None:
    """Stream url to target through a .part file so a crash never leaves a half file behind."""
    part = target.with_suffix(target.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=60) as response, part.open("wb") as handle:
                while chunk := response.read(CHUNK):
                    handle.write(chunk)
            part.replace(target)
            return
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise
            error = exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            error = exc
        if attempt == retries:
            raise error
        time.sleep(2**attempt)


def count_rows(path: Path) -> int:
    """Count data rows (lines minus the header) without loading the file."""
    lines = 0
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK):
            lines += chunk.count(b"\n")
    return max(lines - 1, 0)


def verify_and_extract(zip_path: Path, out_dir: Path) -> tuple[Path, int]:
    """Test the zip, extract its CSV into out_dir and return (csv path, row count)."""
    with zipfile.ZipFile(zip_path) as archive:
        bad = archive.testzip()
        if bad is not None:
            raise zipfile.BadZipFile(f"corrupt member {bad}")
        csv_members = [m for m in archive.namelist() if m.lower().endswith(".csv")]
        if not csv_members:
            raise zipfile.BadZipFile("no .csv inside the zip")
        extracted = Path(archive.extract(csv_members[0], out_dir))
    return extracted, count_rows(extracted)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--start", type=date.fromisoformat, required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--end", type=date.fromisoformat, required=True, help="YYYY-MM-DD, inclusive"
    )
    parser.add_argument("--out", type=Path, default=Path("data/dma"), help="base directory")
    parser.add_argument(
        "--url-template", default=os.environ.get("DMA_URL_TEMPLATE", DEFAULT_TEMPLATE)
    )
    args = parser.parse_args()
    if args.end < args.start:
        parser.error("--end must not be before --start")

    zip_dir = args.out / "zips"
    csv_dir = args.out / "incoming"  # the FilePulse connector watches this directory
    zip_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)

    print(f"{'date':<12}{'status':<12}{'zip MB':>10}{'rows':>14}  file")
    failures = 0
    for day in daterange(args.start, args.end):
        url = args.url_template.format(date=day.isoformat(), year=day.year)
        zip_path = zip_dir / f"aisdk-{day.isoformat()}.zip"
        try:
            if not zip_path.exists():
                download(url, zip_path)
            csv_path, rows = verify_and_extract(zip_path, csv_dir)
            size_mb = zip_path.stat().st_size / 1e6
            print(f"{day.isoformat():<12}{'ok':<12}{size_mb:>10.1f}{rows:>14,}  {csv_path.name}")
        except urllib.error.HTTPError as exc:
            failures += 1
            print(f"{day.isoformat():<12}{'HTTP ' + str(exc.code):<12}{'-':>10}{'-':>14}  {url}")
        except (urllib.error.URLError, zipfile.BadZipFile, OSError) as exc:
            failures += 1
            print(f"{day.isoformat():<12}{'FAILED':<12}{'-':>10}{'-':>14}  {exc}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
