"use client";

import { useEffect, useState } from "react";

interface FunnelData {
  HIGH: number;
  MED: number;
  LOW: number;
  total: number;
}

export default function FunnelPage() {
  const [funnel, setFunnel] = useState<FunnelData>({ HIGH: 0, MED: 0, LOW: 0, total: 0 });

  useEffect(() => {
    async function load() {
      const res = await fetch("/api/propensity?limit=100");
      const rows: { SCORE_BUCKET: string }[] = await res.json();
      const counts = { HIGH: 0, MED: 0, LOW: 0, total: rows.length };
      rows.forEach((r) => {
        if (r.SCORE_BUCKET in counts) counts[r.SCORE_BUCKET as keyof typeof counts]++;
      });
      setFunnel(counts);
    }
    load();
  }, []);

  const maxCount = Math.max(funnel.HIGH, funnel.MED, funnel.LOW, 1);

  return (
    <>
      <h1 style={{ fontSize: "20px", marginBottom: "16px" }}>Conversion Funnel</h1>

      <div className="metrics">
        <div className="metric-card">
          <div className="metric-value">{funnel.total}</div>
          <div className="metric-label">Total Scored Users</div>
        </div>
        <div className="metric-card">
          <div className="metric-value" style={{ color: "var(--high)" }}>{funnel.HIGH}</div>
          <div className="metric-label">HIGH (≥ 0.7)</div>
        </div>
        <div className="metric-card">
          <div className="metric-value" style={{ color: "var(--med)" }}>{funnel.MED}</div>
          <div className="metric-label">MED (0.4 – 0.7)</div>
        </div>
        <div className="metric-card">
          <div className="metric-value" style={{ color: "var(--low)" }}>{funnel.LOW}</div>
          <div className="metric-label">LOW (&lt; 0.4)</div>
        </div>
      </div>

      <div className="card">
        <h2>Score Distribution</h2>
        <div style={{ marginTop: "12px" }}>
          <div className="funnel-bar">
            <div className="funnel-label">HIGH</div>
            <div className="funnel-fill" style={{ width: `${(funnel.HIGH / maxCount) * 100}%`, minWidth: "40px", background: "var(--high)" }}>
              {funnel.HIGH}
            </div>
          </div>
          <div className="funnel-bar">
            <div className="funnel-label">MED</div>
            <div className="funnel-fill" style={{ width: `${(funnel.MED / maxCount) * 100}%`, minWidth: "40px", background: "var(--med)" }}>
              {funnel.MED}
            </div>
          </div>
          <div className="funnel-bar">
            <div className="funnel-label">LOW</div>
            <div className="funnel-fill" style={{ width: `${(funnel.LOW / maxCount) * 100}%`, minWidth: "40px", background: "var(--low)" }}>
              {funnel.LOW}
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
