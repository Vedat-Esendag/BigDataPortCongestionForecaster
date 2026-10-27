import { useEffect, useMemo, useState } from "react";
import { OutlookTimeline } from "./components/OutlookTimeline";
import { PortMap } from "./components/PortMap";
import { ShipTable } from "./components/ShipTable";
import { useLiveSocket } from "./hooks/useLiveSocket";
import type { Outlook, OutlookSlot, PortInfo, Ship } from "./types";

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`/api${path}`);
  if (!response.ok) throw new Error(`${path}: HTTP ${response.status}`);
  return (await response.json()) as T;
}

export default function App() {
  const [ports, setPorts] = useState<PortInfo[]>([]);
  const [portId, setPortId] = useState<string | null>(null);
  const [ships, setShips] = useState<Record<number, Ship>>({});
  const [slots, setSlots] = useState<OutlookSlot[]>([]);
  const [error, setError] = useState<string | null>(null);
  const { status, lastMessage } = useLiveSocket();

  // The first configured port is the default, so the map starts there.
  useEffect(() => {
    getJson<PortInfo[]>("/ports")
      .then((list) => {
        setPorts(list);
        setPortId(list[0]?.id ?? null);
      })
      .catch((e: Error) => setError(e.message));
  }, []);

  // First load of the selected port; the socket keeps it current afterwards.
  useEffect(() => {
    if (!portId) return;
    const query = `?port=${encodeURIComponent(portId)}`;
    Promise.all([getJson<Ship[]>(`/ships${query}`), getJson<Outlook>(`/outlook${query}`)])
      .then(([shipList, outlook]) => {
        setShips(Object.fromEntries(shipList.map((ship) => [ship.mmsi, ship])));
        setSlots(outlook.slots);
        setError(null);
      })
      .catch((e: Error) => setError(e.message));
  }, [portId]);

  useEffect(() => {
    if (!lastMessage) return;
    if (lastMessage.type === "ship" && lastMessage.ship.port === portId) {
      const { ship } = lastMessage;
      setShips((current) => ({ ...current, [ship.mmsi]: ship }));
    } else if (lastMessage.type === "outlook" && lastMessage.port === portId) {
      const { slot } = lastMessage;
      setSlots((current) => [...current.filter((s) => s.slot_start !== slot.slot_start), slot]);
    }
  }, [lastMessage, portId]);

  const port = useMemo(() => ports.find((p) => p.id === portId) ?? null, [ports, portId]);
  const shipList = useMemo(() => Object.values(ships), [ships]);

  return (
    <div className="app">
      <header>
        <h1>Port Congestion Forecaster</h1>
        <label>
          Port{" "}
          <select value={portId ?? ""} onChange={(e) => setPortId(e.target.value)}>
            {ports.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
              </option>
            ))}
          </select>
        </label>
        <span className={`status status-${status}`}>live feed: {status}</span>
      </header>

      {error && <p role="alert">Could not reach the API: {error}</p>}

      <main>
        <div className="map-column">{port ? <PortMap port={port} ships={shipList} /> : <p>Loading ports...</p>}</div>
        <div className="side-column">
          <OutlookTimeline slots={slots} />
          <ShipTable ships={shipList} />
        </div>
      </main>
    </div>
  );
}
