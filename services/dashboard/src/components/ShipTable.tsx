import type { Ship } from "../types";

const time = (iso: string | null) =>
  iso ? new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "short" }) : "n/a";

export function ShipTable({ ships }: { ships: Ship[] }) {
  return (
    <section aria-labelledby="ships-heading">
      <h2 id="ships-heading">Ships</h2>
      <table>
        <thead>
          <tr>
            <th>MMSI</th>
            <th>Broadcast ETA</th>
            <th>Predicted ETA</th>
            <th>70% window</th>
            <th>Why</th>
          </tr>
        </thead>
        <tbody>
          {ships.length === 0 ? (
            <tr>
              <td colSpan={5} className="empty">
                No ships yet.
              </td>
            </tr>
          ) : (
            ships.map((ship) => (
              <tr key={ship.mmsi}>
                <td>{ship.mmsi}</td>
                <td>{time(ship.broadcast_eta)}</td>
                <td>{time(ship.predicted_eta)}</td>
                <td>
                  {time(ship.eta_lower)} to {time(ship.eta_upper)}
                </td>
                <td>{ship.reason_codes.join(", ")}</td>
              </tr>
            ))
          )}
        </tbody>
      </table>
    </section>
  );
}
