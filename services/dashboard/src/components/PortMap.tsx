import * as maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { useEffect, useRef } from "react";
import type { PortInfo, Ship } from "../types";

// OpenFreeMap: free vector tiles, no API key and no sign-up. Data from OpenStreetMap.
// Attribution is shown by the map itself, keep it. See docs/decisions.md.
const STYLE_URL = "https://tiles.openfreemap.org/styles/liberty";

const ZONES = [
  { key: "approach_zone", color: "#1d6fb8" },
  { key: "anchorage", color: "#d98200" },
  { key: "berth_area", color: "#b3261e" },
] as const;

function zoneCollection(port: PortInfo) {
  return {
    type: "FeatureCollection" as const,
    features: ZONES.map(({ key, color }) => ({
      type: "Feature" as const,
      properties: { zone: key, color },
      geometry: { type: "Polygon" as const, coordinates: [port[key]] },
    })),
  };
}

interface Props {
  port: PortInfo;
  ships: Ship[];
}

export function PortMap({ port, ships }: Props) {
  const container = useRef<HTMLDivElement>(null);
  const map = useRef<maplibregl.Map | null>(null);
  const markers = useRef<Map<number, maplibregl.Marker>>(new Map());

  // Create the map once, centered on the first port it is given.
  useEffect(() => {
    if (!container.current) return;
    const instance = new maplibregl.Map({
      container: container.current,
      style: STYLE_URL,
      center: [port.center.lon, port.center.lat],
      zoom: 9,
    });
    instance.addControl(new maplibregl.NavigationControl(), "top-right");
    map.current = instance;
    const shipMarkers = markers.current;
    return () => {
      shipMarkers.clear();
      instance.remove();
      map.current = null;
    };
    // The map is created once; the port is read only for the initial center.
  }, []);

  // Move to the selected port and draw its zones.
  useEffect(() => {
    const instance = map.current;
    if (!instance) return;

    const draw = () => {
      instance.flyTo({ center: [port.center.lon, port.center.lat], zoom: 9 });
      const source = instance.getSource("zones") as maplibregl.GeoJSONSource | undefined;
      if (source) {
        source.setData(zoneCollection(port));
        return;
      }
      instance.addSource("zones", { type: "geojson", data: zoneCollection(port) });
      instance.addLayer({
        id: "zones-fill",
        type: "fill",
        source: "zones",
        paint: { "fill-color": ["get", "color"], "fill-opacity": 0.15 },
      });
      instance.addLayer({
        id: "zones-line",
        type: "line",
        source: "zones",
        paint: { "line-color": ["get", "color"], "line-width": 2 },
      });
    };

    if (instance.isStyleLoaded()) draw();
    else instance.once("load", draw);
  }, [port]);

  // One marker per ship that has a position.
  useEffect(() => {
    const instance = map.current;
    if (!instance) return;
    const live = new Set<number>();
    for (const ship of ships) {
      if (ship.lat === null || ship.lon === null) continue;
      live.add(ship.mmsi);
      const existing = markers.current.get(ship.mmsi);
      if (existing) {
        existing.setLngLat([ship.lon, ship.lat]);
      } else {
        const marker = new maplibregl.Marker({ color: "#0b3d91" })
          .setLngLat([ship.lon, ship.lat])
          .setPopup(new maplibregl.Popup().setText(`MMSI ${ship.mmsi}`))
          .addTo(instance);
        markers.current.set(ship.mmsi, marker);
      }
    }
    for (const [mmsi, marker] of markers.current) {
      if (!live.has(mmsi)) {
        marker.remove();
        markers.current.delete(mmsi);
      }
    }
  }, [ships]);

  return <div ref={container} className="map" role="region" aria-label={`Map of ${port.name}`} />;
}
