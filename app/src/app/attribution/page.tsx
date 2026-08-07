"use client";

import { useEffect, useState } from "react";

interface AttributionRow {
  CHANNEL: string;
  CONVERSIONS: number;
  ATTRIBUTED_REVENUE: number;
  CREDIT_PCT: string;
}

const MODELS = [
  { id: "first_touch", label: "First Touch" },
  { id: "last_touch", label: "Last Touch" },
  { id: "linear", label: "Linear" },
  { id: "time_decay", label: "Time Decay" },
];

export default function AttributionPage() {
  const [model, setModel] = useState("linear");
  const [data, setData] = useState<AttributionRow[]>([]);
  const [allModels, setAllModels] = useState<Record<string, AttributionRow[]>>({});

  useEffect(() => {
    fetch(`/api/attribution?model=${model}`).then((r) => r.json()).then(setData);
  }, [model]);

  useEffect(() => {
    Promise.all(
      MODELS.map((m) => fetch(`/api/attribution?model=${m.id}`).then((r) => r.json()))
    ).then((results) => {
      const map: Record<string, AttributionRow[]> = {};
      MODELS.forEach((m, i) => (map[m.id] = results[i]));
      setAllModels(map);
    });
  }, []);

  const maxRev = Math.max(...data.map((r) => r.ATTRIBUTED_REVENUE), 1);

  return (
    <>
      <h1 style={{ fontSize: "20px", marginBottom: "16px" }}>Multi-Touch Attribution</h1>

      <div className="tab-bar">
        {MODELS.map((m) => (
          <button key={m.id} className={model === m.id ? "tab-active" : ""} onClick={() => setModel(m.id)}>
            {m.label}
          </button>
        ))}
      </div>

      <div className="card">
        <h2>Credit by Channel ({MODELS.find((m) => m.id === model)?.label})</h2>
        <div style={{ marginTop: "12px" }}>
          {data.map((row) => (
            <div key={row.CHANNEL} className="funnel-bar">
              <div className="funnel-label" style={{ width: "80px" }}>{row.CHANNEL}</div>
              <div
                className="funnel-fill"
                style={{
                  width: `${(row.ATTRIBUTED_REVENUE / maxRev) * 100}%`,
                  minWidth: "60px",
                  background: "#3B82F6",
                }}
              >
                {row.CREDIT_PCT}% (${row.ATTRIBUTED_REVENUE?.toFixed(0)})
              </div>
            </div>
          ))}
        </div>
      </div>

      {Object.keys(allModels).length === 4 && (
        <div className="card">
          <h2>Model Comparison</h2>
          <table>
            <thead>
              <tr>
                <th>Channel</th>
                {MODELS.map((m) => <th key={m.id}>{m.label}</th>)}
              </tr>
            </thead>
            <tbody>
              {["display", "search", "social", "email"].map((ch) => (
                <tr key={ch}>
                  <td style={{ fontWeight: 600 }}>{ch}</td>
                  {MODELS.map((m) => {
                    const row = allModels[m.id]?.find((r) => r.CHANNEL === ch);
                    return <td key={m.id}>{row ? `${row.CREDIT_PCT}%` : "0%"}</td>;
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}
