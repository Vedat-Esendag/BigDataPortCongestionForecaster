#!/usr/bin/env python3
"""Turn connect/*.json templates into deployable connector configs.

Most templates are used as they are. Templates that mention a per-port token are expanded
once per port in config/ports.yaml, so no port-specific value lives in a connector file:

    {{PORT_ID}}          port id
    {{OPEN_METEO_LAT}}   weather lookup latitude
    {{OPEN_METEO_LON}}   weather lookup longitude
    {{DMI_STATION_ID}}   DMI station id (one connector per port and station)

Only these exact tokens are replaced. Other braces (the HTTP sink's {{value.field}} templates)
and Kafka config provider references (${env:NAME}) are left alone.

A connector that references ${env:NAME} is skipped, with a note on stderr, when NAME is not set
in this process's environment. The Connect worker resolves these at run time from its own
environment, so a missing secret would only fail later and less clearly.

    render_connectors.py --out DIR      write one <name>.json (config map only) per connector
                                        and print "name<TAB>connector.class<TAB>file" lines
    render_connectors.py errors FILE    print the errors of a /config/validate response
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONNECT_DIR = ROOT / "connect"
PORTS_FILE = ROOT / "config" / "ports.yaml"


def load_ports() -> list[dict]:
    import yaml  # imported lazily so `errors` works without PyYAML

    return yaml.safe_load(PORTS_FILE.read_text())["ports"]


def expansions(text: str, ports: list[dict]):
    """Yield one token mapping per connector instance this template should produce."""
    if "{{DMI_STATION_ID}}" in text:
        for port in ports:
            weather = port["weather"]
            for station in weather.get("dmi_station_ids", []):
                yield {
                    "{{PORT_ID}}": port["id"],
                    "{{DMI_STATION_ID}}": str(station),
                }
    elif "{{PORT_ID}}" in text or "{{OPEN_METEO_LAT}}" in text:
        for port in ports:
            point = port["weather"]["open_meteo"]
            yield {
                "{{PORT_ID}}": port["id"],
                "{{OPEN_METEO_LAT}}": str(point["lat"]),
                "{{OPEN_METEO_LON}}": str(point["lon"]),
            }
    else:
        yield {}


ENV_REFERENCE = re.compile(r"\$\{env:([A-Za-z0-9_]+)\}")


def render(out_dir: Path) -> int:
    ports = load_ports()
    out_dir.mkdir(parents=True, exist_ok=True)
    for template in sorted(CONNECT_DIR.glob("*.json")):
        text = template.read_text()
        missing = sorted(n for n in set(ENV_REFERENCE.findall(text)) if not os.environ.get(n))
        if missing:
            print(f"SKIP {template.name}: {', '.join(missing)} is not set", file=sys.stderr)
            continue
        for tokens in expansions(text, ports):
            rendered = text
            for token, value in tokens.items():
                rendered = rendered.replace(token, value)
            doc = json.loads(rendered)
            name, config = doc["name"], doc["config"]
            target = out_dir / f"{name}.json"
            target.write_text(json.dumps(config, indent=2))
            print(f"{name}\t{config['connector.class']}\t{target}")
    return 0


def show_errors(path: Path) -> int:
    response = json.loads(path.read_text())
    if response.get("error_count", 0) == 0:
        return 0
    for item in response.get("configs", []):
        value = item.get("value", {})
        for error in value.get("errors", []):
            print(f"  {value.get('name')}: {error}")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command")
    errors = sub.add_parser("errors", help="print errors of a validate response")
    errors.add_argument("file", type=Path)
    parser.add_argument("--out", type=Path, help="directory for rendered configs")
    args = parser.parse_args()

    if args.command == "errors":
        return show_errors(args.file)
    if args.out is None:
        parser.error("--out is required")
    return render(args.out)


if __name__ == "__main__":
    sys.exit(main())
