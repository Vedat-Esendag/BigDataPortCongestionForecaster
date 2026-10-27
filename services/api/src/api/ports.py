"""Ports come from config/ports.yaml, never from code."""

from __future__ import annotations

from pathlib import Path

import yaml
from fastapi import HTTPException

from api.models import PortInfo


def load_ports(path: Path) -> list[PortInfo]:
    raw = yaml.safe_load(path.read_text())["ports"]
    return [
        PortInfo(
            id=item["id"],
            name=item["name"],
            country=item["country"],
            center=item["center"],
            bbox=item["bbox"],
            approach_zone=item["approach_zone"],
            anchorage=item["anchorage"],
            berth_area=item["berth_area"],
        )
        for item in raw
    ]


def require_port(ports: list[PortInfo], port_id: str) -> PortInfo:
    for port in ports:
        if port.id == port_id:
            return port
    raise HTTPException(status_code=404, detail=f"unknown port: {port_id}")
