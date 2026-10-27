#!/usr/bin/env python3
"""Register every schemas/avro/*.avsc file in Schema Registry (idempotent).

The subject is the file name without the extension plus "-value", which matches the default
TopicNameStrategy: ais.position.avsc -> subject ais.position-value.

Uses only the standard library, so it runs with any python3.

    python3 scripts/register_schemas.py            # register against SCHEMA_REGISTRY_URL
    python3 scripts/register_schemas.py --check    # only parse the files, no network
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SCHEMA_DIR = ROOT / "schemas" / "avro"
CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


def subject_for(path: Path) -> str:
    return f"{path.stem}-value"


def check_file(path: Path) -> str:
    """Return the schema text after basic validation. Raises ValueError on a bad file."""
    text = path.read_text(encoding="utf-8")
    parsed = json.loads(text)
    if parsed.get("type") != "record" or not parsed.get("name") or not parsed.get("fields"):
        raise ValueError(f"{path.name}: expected a record with a name and fields")
    return text


def wait_for_registry(url: str, timeout_s: int) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            with urllib.request.urlopen(f"{url}/subjects", timeout=5):
                return
        except (urllib.error.URLError, OSError) as exc:
            if time.monotonic() >= deadline:
                raise SystemExit(f"Schema Registry not reachable at {url}: {exc}") from exc
            time.sleep(2)


def register(url: str, subject: str, schema_text: str) -> int:
    body = json.dumps({"schemaType": "AVRO", "schema": schema_text}).encode()
    request = urllib.request.Request(
        f"{url}/subjects/{subject}/versions",
        data=body,
        headers={"Content-Type": CONTENT_TYPE},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.load(response)["id"]
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--url", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    )
    parser.add_argument("--schema-dir", type=Path, default=DEFAULT_SCHEMA_DIR)
    parser.add_argument("--check", action="store_true", help="parse the files only, no network")
    parser.add_argument("--wait", type=int, default=60, help="seconds to wait for the registry")
    args = parser.parse_args()

    files = sorted(args.schema_dir.glob("*.avsc"))
    if not files:
        print(f"no .avsc files in {args.schema_dir}", file=sys.stderr)
        return 1

    texts: dict[Path, str] = {}
    for path in files:
        try:
            texts[path] = check_file(path)
        except (ValueError, json.JSONDecodeError) as exc:
            print(f"INVALID {exc}", file=sys.stderr)
            return 1

    if args.check:
        print(f"{len(files)} schema files parsed OK")
        return 0

    wait_for_registry(args.url, args.wait)
    failures = 0
    for path, text in texts.items():
        subject = subject_for(path)
        try:
            schema_id = register(args.url, subject, text)
            print(f"ok   {subject:<40} id={schema_id}")
        except RuntimeError as exc:
            failures += 1
            print(f"FAIL {subject:<40} {exc}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
