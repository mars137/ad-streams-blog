interface HeatmapRow {
  [key: string]: any;
}

interface HeatmapChartProps {
  data: HeatmapRow[];
  title?: string;
}

export function HeatmapChart({ data, title }: HeatmapChartProps) {
  // Handle uppercase column names from Snowflake
  const getChannel = (r: HeatmapRow) => r.CHANNEL || r.channel || "";
  const getHourTs = (r: HeatmapRow) => r.HOUR_TS || r.hour_ts || "";
  const getEvents = (r: HeatmapRow) => r.TOTAL_EVENTS || r.total_events || 0;

  const channels = [...new Set(data.map(getChannel))].filter(Boolean).sort();
  const hours = [...new Set(data.map((r) => {
    const ts = getHourTs(r);
    return ts ? new Date(ts).getHours() : -1;
  }))].filter((h) => h >= 0).sort((a, b) => a - b);

  const grid: Record<string, Record<number, number>> = {};
  channels.forEach((ch) => (grid[ch] = {}));
  data.forEach((r) => {
    const ts = getHourTs(r);
    if (!ts) return;
    const h = new Date(ts).getHours();
    const ch = getChannel(r);
    if (ch) grid[ch][h] = (grid[ch][h] || 0) + getEvents(r);
  });

  const maxVal = Math.max(...data.map(getEvents), 1);

  function cellColor(val: number): string {
    const ratio = val / maxVal;
    const r = Math.round(212 - ratio * 201);
    const g = Math.round(237 - ratio * 150);
    const b = Math.round(252 - ratio * 125);
    return `rgb(${r}, ${g}, ${b})`;
  }

  return (
    <div className="card">
      {title && <h2>{title}</h2>}
      <div className="heatmap-container">
        <div className="heatmap-row heatmap-header">
          <div className="heatmap-label" />
          {hours.map((h) => (
            <div key={h} className="heatmap-cell heatmap-hour">{h}:00</div>
          ))}
        </div>
        {channels.map((ch) => (
          <div key={ch} className="heatmap-row">
            <div className="heatmap-label">{ch}</div>
            {hours.map((h) => {
              const val = grid[ch]?.[h] || 0;
              return (
                <div
                  key={h}
                  className="heatmap-cell"
                  style={{ background: val > 0 ? cellColor(val) : "#F8FAFC" }}
                  title={`${ch} @ ${h}:00 — ${val} events`}
                >
                  {val > 0 ? val : ""}
                </div>
              );
            })}
          </div>
        ))}
      </div>
    </div>
  );
}
