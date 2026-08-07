"use client";

import { useEffect, useState } from "react";

interface PropensityRow {
  USER_ID: string;
  PROPENSITY_SCORE: number;
  SCORE_BUCKET: string;
  LAST_UPDATED: string;
}

export default function Dashboard() {
  const [data, setData] = useState<PropensityRow[]>([]);
  const [lastRefresh, setLastRefresh] = useState("");

  async function fetchData() {
    const res = await fetch("/api/propensity?limit=15");
    const rows = await res.json();
    setData(rows);
    setLastRefresh(new Date().toLocaleTimeString());
  }

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 10000);
    return () => clearInterval(interval);
  }, []);

  return (
    <>
      <div className="metrics">
        <div className="metric-card">
          <div className="metric-value">{data.length}</div>
          <div className="metric-label">Users Scored</div>
        </div>
        <div className="metric-card">
          <div className="metric-value" style={{ color: "var(--high)" }}>
            {data.filter((r) => r.SCORE_BUCKET === "HIGH").length}
          </div>
          <div className="metric-label">High Propensity</div>
        </div>
        <div className="metric-card">
          <div className="metric-value">
            {data.length > 0
              ? (data.reduce((s, r) => s + r.PROPENSITY_SCORE, 0) / data.length).toFixed(3)
              : "—"}
          </div>
          <div className="metric-label">Avg Score (Top 15)</div>
        </div>
      </div>

      <div className="card">
        <h2>Top Users by Propensity Score</h2>
        <div className="refresh-indicator">Auto-refreshes every 10s | Last: {lastRefresh}</div>
        <table>
          <thead>
            <tr>
              <th>User</th>
              <th>Score</th>
              <th>Bucket</th>
              <th>Bar</th>
              <th>Last Updated</th>
            </tr>
          </thead>
          <tbody>
            {data.map((row) => (
              <tr key={row.USER_ID}>
                <td>
                  <a href={`/user?id=${row.USER_ID}`} style={{ color: "var(--snow-blue)" }}>
                    {row.USER_ID}
                  </a>
                </td>
                <td>{row.PROPENSITY_SCORE.toFixed(6)}</td>
                <td>
                  <span className={`badge badge-${row.SCORE_BUCKET.toLowerCase()}`}>
                    {row.SCORE_BUCKET}
                  </span>
                </td>
                <td>
                  <div className="score-bar">
                    <div
                      className="score-bar-fill"
                      style={{
                        width: `${row.PROPENSITY_SCORE * 100}%`,
                        background:
                          row.SCORE_BUCKET === "HIGH"
                            ? "var(--high)"
                            : row.SCORE_BUCKET === "MED"
                            ? "var(--med)"
                            : "var(--low)",
                      }}
                    />
                  </div>
                </td>
                <td style={{ fontSize: "12px", color: "var(--text-muted)" }}>
                  {new Date(row.LAST_UPDATED).toLocaleString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
