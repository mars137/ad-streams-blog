"use client";

import { useState } from "react";

interface Condition {
  field: string;
  op: string;
  value: string;
}

interface PreviewResult {
  count: number;
  users: { USER_ID: string; PROPENSITY_SCORE: number; SCORE_BUCKET: string; CLICKS_24H: number; CONVERSIONS_TOTAL: number }[];
  sql: string;
}

const FIELDS = [
  { id: "score_bucket", label: "Score Bucket", type: "select", options: ["HIGH", "MED", "LOW"] },
  { id: "propensity_score", label: "Propensity Score", type: "number" },
  { id: "clicks_1h", label: "Clicks (1h)", type: "number" },
  { id: "clicks_24h", label: "Clicks (24h)", type: "number" },
  { id: "clicks_7d", label: "Clicks (7d)", type: "number" },
  { id: "impressions_1h", label: "Impressions (1h)", type: "number" },
  { id: "impressions_24h", label: "Impressions (24h)", type: "number" },
  { id: "conversions_total", label: "Conversions Total", type: "number" },
  { id: "event_velocity_24h", label: "Event Velocity (24h)", type: "number" },
];

const OPS = ["=", "!=", ">", ">=", "<", "<="];

export default function AudiencesPage() {
  const [conditions, setConditions] = useState<Condition[]>([
    { field: "score_bucket", op: "=", value: "HIGH" },
  ]);
  const [preview, setPreview] = useState<PreviewResult | null>(null);
  const [loading, setLoading] = useState(false);

  function addCondition() {
    setConditions([...conditions, { field: "clicks_24h", op: ">", value: "0" }]);
  }

  function removeCondition(idx: number) {
    setConditions(conditions.filter((_, i) => i !== idx));
  }

  function updateCondition(idx: number, key: keyof Condition, val: string) {
    const updated = [...conditions];
    updated[idx] = { ...updated[idx], [key]: val };
    setConditions(updated);
  }

  async function runPreview() {
    setLoading(true);
    const res = await fetch("/api/audiences/preview", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ conditions }),
    });
    const data = await res.json();
    setPreview(data);
    setLoading(false);
  }

  return (
    <>
      <h1 style={{ fontSize: "20px", marginBottom: "16px" }}>Audience Segment Builder</h1>

      <div className="card">
        <h2>Define Segment Rules</h2>
        <div className="segment-rules">
          {conditions.map((cond, i) => (
            <div key={i} className="segment-rule-row">
              <select value={cond.field} onChange={(e) => updateCondition(i, "field", e.target.value)}>
                {FIELDS.map((f) => <option key={f.id} value={f.id}>{f.label}</option>)}
              </select>
              <select value={cond.op} onChange={(e) => updateCondition(i, "op", e.target.value)}>
                {OPS.map((op) => <option key={op} value={op}>{op}</option>)}
              </select>
              <input
                type="text"
                value={cond.value}
                onChange={(e) => updateCondition(i, "value", e.target.value)}
                placeholder="value"
              />
              <button className="btn-remove" onClick={() => removeCondition(i)}>x</button>
            </div>
          ))}
        </div>
        <div style={{ marginTop: "12px", display: "flex", gap: "8px" }}>
          <button className="btn-secondary" onClick={addCondition}>+ Add Rule</button>
          <button className="btn-primary" onClick={runPreview} disabled={loading}>
            {loading ? "Running..." : "Preview Segment"}
          </button>
        </div>
      </div>

      {preview && (
        <>
          <div className="metrics" style={{ marginTop: "16px" }}>
            <div className="metric-card">
              <div className="metric-value">{preview.count}</div>
              <div className="metric-label">Users in Segment</div>
            </div>
          </div>

          <div className="card">
            <h2>Sample Users</h2>
            <table>
              <thead>
                <tr><th>User ID</th><th>Propensity</th><th>Bucket</th><th>Clicks 24h</th><th>Conversions</th></tr>
              </thead>
              <tbody>
                {preview.users.map((u) => (
                  <tr key={u.USER_ID}>
                    <td><a href={`/user?id=${u.USER_ID}`} style={{ color: "var(--snow-blue)" }}>{u.USER_ID}</a></td>
                    <td>{u.PROPENSITY_SCORE?.toFixed(4)}</td>
                    <td><span className={`badge badge-${u.SCORE_BUCKET?.toLowerCase()}`}>{u.SCORE_BUCKET}</span></td>
                    <td>{u.CLICKS_24H}</td>
                    <td>{u.CONVERSIONS_TOTAL}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card">
            <h2>Generated SQL</h2>
            <pre style={{ fontSize: "11px", overflow: "auto", background: "#F1F5F9", padding: "12px", borderRadius: "4px" }}>
              {preview.sql}
            </pre>
          </div>
        </>
      )}
    </>
  );
}
