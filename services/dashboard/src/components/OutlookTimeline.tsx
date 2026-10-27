import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { OutlookSlot } from "../types";

const hour = (iso: string) => new Date(iso).toLocaleString([], { weekday: "short", hour: "2-digit" });

export function OutlookTimeline({ slots }: { slots: OutlookSlot[] }) {
  const data = slots.map((slot) => ({
    label: hour(slot.slot_start),
    "Expected arrivals": slot.expected_arrivals,
    "Late ships": slot.late_count,
  }));

  return (
    <section aria-labelledby="outlook-heading">
      <h2 id="outlook-heading">Outlook, next 24 to 48 hours</h2>
      {data.length === 0 ? (
        <p className="empty">No outlook yet.</p>
      ) : (
        <ResponsiveContainer width="100%" height={240}>
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="label" />
            <YAxis allowDecimals={false} />
            <Tooltip />
            <Legend />
            <Bar dataKey="Expected arrivals" fill="#1d6fb8" />
            <Bar dataKey="Late ships" fill="#b3261e" />
          </BarChart>
        </ResponsiveContainer>
      )}
    </section>
  );
}
