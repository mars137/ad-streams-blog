interface TraceStep {
  phase: string;
  detail: string;
  status: "thinking" | "done" | "error";
}

interface IntelligenceTraceProps {
  steps: TraceStep[];
  isThinking: boolean;
}

export function IntelligenceTrace({ steps, isThinking }: IntelligenceTraceProps) {
  return (
    <div className="trace-panel">
      <h3 className="trace-title">Intelligence Trace</h3>
      <div className="trace-steps">
        {steps.map((step, i) => (
          <div key={i} className={`trace-step trace-${step.status}`}>
            <div className="trace-indicator">
              {step.status === "thinking" ? "⟳" : step.status === "done" ? "✓" : "✗"}
            </div>
            <div className="trace-content">
              <div className="trace-phase">{step.phase}</div>
              <div className="trace-detail">{step.detail}</div>
            </div>
          </div>
        ))}
        {isThinking && (
          <div className="trace-step trace-thinking">
            <div className="trace-indicator pulse">⟳</div>
            <div className="trace-content">
              <div className="trace-phase">Processing...</div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
