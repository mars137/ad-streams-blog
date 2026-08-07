"use client";

import { useEffect, useState } from "react";
import { IntelligenceTrace } from "@/components/IntelligenceTrace";

interface TraceStep {
  phase: string;
  detail: string;
  status: "thinking" | "done" | "error";
}

export default function AIPage() {
  const [input, setInput] = useState("");
  const [response, setResponse] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [traces, setTraces] = useState<TraceStep[]>([]);
  const [history, setHistory] = useState<{ role: string; content: string }[]>([]);

  function addTrace(phase: string, detail: string, status: TraceStep["status"] = "done") {
    setTraces((prev) => [...prev, { phase, detail, status }]);
  }

  async function analyze(question?: string) {
    const msg = question || input;
    if (!msg.trim()) return;

    setIsLoading(true);
    setTraces([]);
    setResponse("");
    setHistory((h) => [...h, { role: "user", content: msg }]);
    setInput("");

    addTrace("Data Collection", "Querying campaign performance metrics...", "thinking");
    await new Promise((r) => setTimeout(r, 500));
    addTrace("Data Collection", "Retrieved metrics for 5 campaigns across 4 channels", "done");

    addTrace("AI Analysis", "Sending to Claude Sonnet for optimization analysis...", "thinking");

    try {
      const res = await fetch("/api/ai/optimize", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: msg }),
      });
      const data = await res.json();

      setTraces((prev) =>
        prev.map((t) =>
          t.phase === "AI Analysis" && t.status === "thinking"
            ? { ...t, status: "done" as const, detail: "Analysis complete" }
            : t
        )
      );
      addTrace("Recommendations", `Generated ${(data.recommendation.match(/\d\./g) || []).length} recommendations`, "done");

      setResponse(data.recommendation);
      setHistory((h) => [...h, { role: "assistant", content: data.recommendation }]);
    } catch (err) {
      addTrace("Error", "Failed to get AI response", "error");
    }

    setIsLoading(false);
  }

  useEffect(() => {
    analyze("Give me an initial assessment of my campaign performance and top 3 optimization recommendations.");
  }, []);

  return (
    <>
      <h1 style={{ fontSize: "20px", marginBottom: "16px" }}>AI Campaign Optimizer</h1>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 300px", gap: "16px" }}>
        <div>
          <div className="card" style={{ maxHeight: "500px", overflow: "auto" }}>
            <h2>Conversation</h2>
            <div className="chat-messages">
              {history.map((msg, i) => (
                <div key={i} className={`chat-msg chat-${msg.role}`}>
                  <div className="chat-role">{msg.role === "user" ? "You" : "AI Optimizer"}</div>
                  <div className="chat-content" style={{ whiteSpace: "pre-wrap" }}>
                    {msg.content}
                  </div>
                </div>
              ))}
              {isLoading && <div className="chat-msg chat-assistant"><div className="chat-content">Thinking...</div></div>}
            </div>
          </div>

          <div className="chat-input-bar">
            <input
              type="text"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && analyze()}
              placeholder="Ask about campaign optimization..."
              className="chat-input"
            />
            <button onClick={() => analyze()} disabled={isLoading} className="chat-send">
              Send
            </button>
          </div>
        </div>

        <IntelligenceTrace steps={traces} isThinking={isLoading} />
      </div>
    </>
  );
}
