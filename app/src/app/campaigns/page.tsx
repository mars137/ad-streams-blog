"use client";

import { useEffect, useState } from "react";
import { KPICard } from "@/components/KPICard";
import { HeatmapChart } from "@/components/HeatmapChart";

interface KPIs {
  TOTAL_IMPRESSIONS: number;
  TOTAL_CLICKS: number;
  TOTAL_CONVERSIONS: number;
  TOTAL_REVENUE: number;
  CTR: number;
  CONVERSION_RATE: number;
}

interface RFRow {
  CAMPAIGN_NAME: string;
  CHANNEL: string;
  REACH: number;
  AVG_FREQUENCY: number;
  FREQ_1: number;
  FREQ_2_3: number;
  FREQ_4_5: number;
  FREQ_6PLUS: number;
}

export default function CampaignsPage() {
  const [kpis, setKpis] = useState<KPIs | null>(null);
  const [heatmap, setHeatmap] = useState([]);
  const [rf, setRf] = useState<RFRow[]>([]);
  const [tab, setTab] = useState<"performance" | "reach">("performance");

  useEffect(() => {
    fetch("/api/campaigns/kpis").then((r) => r.json()).then(setKpis);
    fetch("/api/campaigns/heatmap").then((r) => r.json()).then(setHeatmap);
    fetch("/api/campaigns/reach-frequency").then((r) => r.json()).then(setRf);
  }, []);

  return (
    <>
      <h1 style={{ fontSize: "20px", marginBottom: "16px" }}>Campaign Performance</h1>

      {kpis && (
        <div className="metrics">
          <KPICard label="Impressions" value={kpis.TOTAL_IMPRESSIONS?.toLocaleString()} accentColor="#3B82F6" />
          <KPICard label="Clicks" value={kpis.TOTAL_CLICKS?.toLocaleString()} accentColor="#8B5CF6" />
          <KPICard label="CTR" value={`${(kpis.CTR * 100).toFixed(1)}%`} accentColor="#F59E0B" />
          <KPICard label="Revenue" value={`$${kpis.TOTAL_REVENUE?.toFixed(0)}`} accentColor="#16A34A" />
          <KPICard label="Conversions" value={kpis.TOTAL_CONVERSIONS?.toLocaleString()} accentColor="#EC4899" />
          <KPICard label="Conv Rate" value={`${(kpis.CONVERSION_RATE * 100).toFixed(1)}%`} accentColor="#06B6D4" />
        </div>
      )}

      <div className="tab-bar">
        <button className={tab === "performance" ? "tab-active" : ""} onClick={() => setTab("performance")}>
          Channel Heatmap
        </button>
        <button className={tab === "reach" ? "tab-active" : ""} onClick={() => setTab("reach")}>
          Reach & Frequency
        </button>
      </div>

      {tab === "performance" && <HeatmapChart data={heatmap} title="Events by Channel x Hour" />}

      {tab === "reach" && (
        <div className="card">
          <h2>Reach & Frequency by Campaign</h2>
          <table>
            <thead>
              <tr>
                <th>Campaign</th>
                <th>Channel</th>
                <th>Reach</th>
                <th>Avg Freq</th>
                <th>1x</th>
                <th>2-3x</th>
                <th>4-5x</th>
                <th>6+</th>
              </tr>
            </thead>
            <tbody>
              {rf.map((row, i) => (
                <tr key={i}>
                  <td>{row.CAMPAIGN_NAME}</td>
                  <td>{row.CHANNEL}</td>
                  <td>{row.REACH}</td>
                  <td>{row.AVG_FREQUENCY?.toFixed(1)}</td>
                  <td>{row.FREQ_1}</td>
                  <td>{row.FREQ_2_3}</td>
                  <td>{row.FREQ_4_5}</td>
                  <td>{row.FREQ_6PLUS}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}
