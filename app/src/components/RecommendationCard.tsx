interface RecommendationCardProps {
  title: string;
  description: string;
  impact?: string;
  channel?: string;
}

export function RecommendationCard({ title, description, impact, channel }: RecommendationCardProps) {
  return (
    <div className="rec-card">
      {channel && <div className="rec-channel">{channel}</div>}
      <h3 className="rec-title">{title}</h3>
      <p className="rec-desc">{description}</p>
      {impact && <div className="rec-impact">Expected Impact: {impact}</div>}
    </div>
  );
}
