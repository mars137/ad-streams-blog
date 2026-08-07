"use client";

import { Suspense, useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";

interface Feature {
  USER_ID: string;
  CLICKS_1H: number;
  CLICKS_24H: number;
  CLICKS_7D: number;
  IMPRESSIONS_1H: number;
  IMPRESSIONS_24H: number;
  IMPRESSIONS_7D: number;
  PAID_SEARCH_1H: number;
  PAID_SEARCH_24H: number;
  PAID_SEARCH_7D: number;
  CONVERSIONS_TOTAL: number;
  TIME_SINCE_LAST_CONVERSION_HRS: number | null;
  EVENT_VELOCITY_24H: number;
}

interface Event {
  EVENT_ID: string;
  EVENT_TYPE: string;
  CAMPAIGN_NAME: string;
  CHANNEL: string;
  EVENT_TS: string;
  CONVERSION_VALUE: number | null;
}

export default function UserPage() {
  return (
    <Suspense fallback={<div>Loading...</div>}>
      <UserContent />
    </Suspense>
  );
}

function UserContent() {
  const searchParams = useSearchParams();
  const userId = searchParams.get("id") || "U0001";
  const [features, setFeatures] = useState<Feature | null>(null);
  const [events, setEvents] = useState<Event[]>([]);

  useEffect(() => {
    fetch(`/api/features?user_id=${userId}`).then((r) => r.json()).then(setFeatures);
    fetch(`/api/events?user_id=${userId}&limit=20`).then((r) => r.json()).then(setEvents);
  }, [userId]);

  const typeLabel: Record<string, string> = { CL: "Click", IM: "Impression", PS: "Paid Search", CN: "Conversion" };
  const typeColor: Record<string, string> = { CL: "#3B82F6", IM: "#8B5CF6", PS: "#F59E0B", CN: "#16A34A" };

  return (
    <>
      <h1 style={{ fontSize: "20px", marginBottom: "16px" }}>User Profile: {userId}</h1>

      {features && (
        <div className="card">
          <h2>Feature Vector</h2>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: "12px", fontSize: "13px" }}>
            <div><strong>Clicks 1h:</strong> {features.CLICKS_1H}</div>
            <div><strong>Clicks 24h:</strong> {features.CLICKS_24H}</div>
            <div><strong>Clicks 7d:</strong> {features.CLICKS_7D}</div>
            <div><strong>Impressions 1h:</strong> {features.IMPRESSIONS_1H}</div>
            <div><strong>Impressions 24h:</strong> {features.IMPRESSIONS_24H}</div>
            <div><strong>Impressions 7d:</strong> {features.IMPRESSIONS_7D}</div>
            <div><strong>Paid Search 1h:</strong> {features.PAID_SEARCH_1H}</div>
            <div><strong>Paid Search 24h:</strong> {features.PAID_SEARCH_24H}</div>
            <div><strong>Conversions:</strong> {features.CONVERSIONS_TOTAL}</div>
            <div><strong>Hrs Since Conv:</strong> {features.TIME_SINCE_LAST_CONVERSION_HRS ?? "N/A"}</div>
            <div><strong>Velocity 24h:</strong> {features.EVENT_VELOCITY_24H}</div>
          </div>
        </div>
      )}

      <div className="card">
        <h2>Recent Events</h2>
        <table>
          <thead>
            <tr><th>Time</th><th>Type</th><th>Campaign</th><th>Channel</th><th>Value</th></tr>
          </thead>
          <tbody>
            {events.map((e) => (
              <tr key={e.EVENT_ID}>
                <td style={{ fontSize: "12px" }}>{new Date(e.EVENT_TS).toLocaleString()}</td>
                <td>
                  <span style={{ color: typeColor[e.EVENT_TYPE] || "#333", fontWeight: 600 }}>
                    {typeLabel[e.EVENT_TYPE] || e.EVENT_TYPE}
                  </span>
                </td>
                <td>{e.CAMPAIGN_NAME}</td>
                <td>{e.CHANNEL}</td>
                <td>{e.CONVERSION_VALUE ? `$${e.CONVERSION_VALUE.toFixed(2)}` : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
