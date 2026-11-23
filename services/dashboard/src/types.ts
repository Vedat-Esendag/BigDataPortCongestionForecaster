// Mirrors services/api/src/api/models.py. Keep the two in step.

export interface PortInfo {
  id: string;
  name: string;
  country: string;
  center: { lat: number; lon: number };
  bbox: { min_lat: number; min_lon: number; max_lat: number; max_lon: number };
  /** Polygons are [lon, lat] pairs, closed. */
  approach_zone: number[][];
  anchorage: number[][];
  berth_area: number[][];
}

export interface Ship {
  mmsi: number;
  port: string;
  broadcast_eta: string | null;
  predicted_eta: string;
  eta_lower: string;
  eta_upper: string;
  model_version: string;
  reason_codes: string[];
  lat: number | null;
  lon: number | null;
  event_time: string;
  /** When Spark produced this prediction: use it to show how old the numbers are. */
  processing_time?: string | null;
}

export interface OutlookSlot {
  slot_start: string;
  slot_end: string;
  expected_arrivals: number;
  late_count: number;
  bunching_flag: boolean;
  expected_waiting_ships: number;
  computed_at: string;
}

export interface Outlook {
  port: string;
  slots: OutlookSlot[];
}

/**
 * Messages pushed over /ws (services/api/src/api/routes/ws.py). Until the client sends
 * {"type": "subscribe", "port": "<id>"}, it receives changes for every port.
 */
export type LiveMessage =
  | { type: "hello"; ports: string[] }
  | { type: "ship"; ship: Ship }
  | { type: "outlook"; port: string; slot: OutlookSlot }
  | { type: "subscribed"; port: string | null }
  | { type: "error"; detail: string };
