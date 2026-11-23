#!/usr/bin/env python3
"""Create the ksqlDB streams and queries in ksql/*.sql (idempotent).

Sends each file to ksqlDB's REST API. Every statement says IF NOT EXISTS, so running this again
changes nothing. A file fails while the topics or Avro schemas it reads don't exist yet (ksqlDB
looks up the value schema of port.outlook, for example), so each file is retried until it works
or the time runs out. This is the Kubernetes counterpart of the ksql-init service in docker
compose, and it works against either.

Uses only the standard library, so it runs with any python3.

    python3 scripts/run_ksql.py                       # against KSQLDB_URL (default localhost:8088)
    python3 scripts/run_ksql.py --timeout 300
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# Order matters only in that each file is complete on its own; history first, as in compose.
FILES = ["history_split.sql", "alerts.sql"]
# Errors that mean "not there yet": worth waiting for. Anything else (a typo, a column that
# doesn't exist) fails at once, because retrying can't fix it.
NOT_READY = re.compile(
    r"does not exist|not reachable|Could not fetch|Timed out|schema registry", re.IGNORECASE
)


def run_file(url: str, path: Path) -> tuple[bool, str]:
    """Send one file. Returns (worked, message)."""
    body = json.dumps({"ksql": path.read_text(encoding="utf-8"), "streamsProperties": {}})
    request = urllib.request.Request(
        f"{url}/ksql",
        data=body.encode(),
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            results = json.load(response)
    except urllib.error.HTTPError as error:
        # ksqlDB answers 400 with {"message": ...} when a statement fails.
        try:
            message = json.load(error).get("message", str(error))
        except (ValueError, AttributeError):
            message = str(error)
        return False, message.strip().splitlines()[0]
    except OSError as error:  # includes URLError: connection refused, DNS, timeouts
        return False, f"ksqlDB not reachable: {error}"
    # A new statement reports what it created; one that exists already comes back as a warning.
    statuses = [
        r.get("commandStatus", {}).get("message")
        or ("already exists" if r.get("@type") == "warning_entity" else "")
        for r in results
    ]
    return True, "; ".join(dict.fromkeys(s for s in statuses if s)) or "ok"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--url", default=os.environ.get("KSQLDB_URL", "http://localhost:8088"))
    parser.add_argument("--timeout", type=int, default=300, help="seconds to keep retrying")
    args = parser.parse_args()

    deadline = time.monotonic() + args.timeout
    for name in FILES:
        path = ROOT / "ksql" / name
        while True:
            worked, message = run_file(args.url, path)
            if worked:
                print(f"ok   {name}: {message}", flush=True)
                break
            if not NOT_READY.search(message) or time.monotonic() >= deadline:
                print(f"FAIL {name}: {message}", flush=True)
                return 1
            print(f"wait {name}: {message} (retrying in 10 s)", flush=True)
            time.sleep(10)
    return 0


if __name__ == "__main__":
    sys.exit(main())
