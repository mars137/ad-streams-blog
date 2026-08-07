interface KPICardProps {
  label: string;
  value: string;
  delta?: { value: string; direction: "up" | "down" | "flat" };
  subtitle?: string;
  accentColor?: string;
}

export function KPICard({ label, value, delta, subtitle, accentColor = "#29B5E8" }: KPICardProps) {
  const arrowMap = { up: "↑", down: "↓", flat: "→" };
  const colorMap = { up: "#16A34A", down: "#DC2626", flat: "#64748B" };

  return (
    <div className="kpi-card" style={{ borderTopColor: accentColor }}>
      <div className="kpi-label">{label}</div>
      <div className="kpi-value">{value}</div>
      {delta && (
        <div className="kpi-delta" style={{ color: colorMap[delta.direction] }}>
          {arrowMap[delta.direction]} {delta.value}
        </div>
      )}
      {subtitle && <div className="kpi-subtitle">{subtitle}</div>}
    </div>
  );
}
