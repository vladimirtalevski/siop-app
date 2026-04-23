import { useState, useRef, useEffect } from "react";
import { sendChat } from "../api";

const SUGGESTIONS = [
  "What is the open PO value for US2 compared to all other companies?",
  "Which items have a shortage in the next 30 days?",
  "Show me the top 10 slow-moving items by on-hand value in ZA4",
  "What is our total on-hand inventory value across all companies?",
  "How many open purchase orders are overdue today?",
  "Show the sales order backlog by customer for US2",
  "Which items have on-hand quantity below their minimum (ROP)?",
  "What is the demand forecast for 2026 by month?",
];

function MarkdownText({ text }) {
  // Simple markdown: bold, bullet points, code
  const lines = text.split("\n");
  return (
    <div style={{ lineHeight: 1.6 }}>
      {lines.map((line, i) => {
        // Heading
        if (line.startsWith("### ")) return <div key={i} style={{ fontWeight: 700, fontSize: 15, color: "#f9fafb", marginTop: 12, marginBottom: 4 }}>{line.slice(4)}</div>;
        if (line.startsWith("## ")) return <div key={i} style={{ fontWeight: 700, fontSize: 16, color: "#f9fafb", marginTop: 14, marginBottom: 6 }}>{line.slice(3)}</div>;
        if (line.startsWith("# ")) return <div key={i} style={{ fontWeight: 700, fontSize: 18, color: "#f9fafb", marginTop: 16, marginBottom: 8 }}>{line.slice(2)}</div>;
        // Bullet
        if (line.startsWith("- ") || line.startsWith("* ")) {
          return <div key={i} style={{ display: "flex", gap: 8, paddingLeft: 8, color: "#d1d5db" }}>
            <span style={{ color: "#60a5fa", marginTop: 2 }}>•</span>
            <span>{renderInline(line.slice(2))}</span>
          </div>;
        }
        // Numbered
        if (/^\d+\. /.test(line)) {
          const [num, ...rest] = line.split(". ");
          return <div key={i} style={{ display: "flex", gap: 8, paddingLeft: 8, color: "#d1d5db" }}>
            <span style={{ color: "#60a5fa", minWidth: 20 }}>{num}.</span>
            <span>{renderInline(rest.join(". "))}</span>
          </div>;
        }
        if (line.trim() === "") return <div key={i} style={{ height: 6 }} />;
        return <div key={i} style={{ color: "#d1d5db" }}>{renderInline(line)}</div>;
      })}
    </div>
  );
}

function renderInline(text) {
  // Bold **text**
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g);
  return parts.map((part, i) => {
    if (part.startsWith("**") && part.endsWith("**"))
      return <strong key={i} style={{ color: "#f9fafb" }}>{part.slice(2, -2)}</strong>;
    if (part.startsWith("`") && part.endsWith("`"))
      return <code key={i} style={{ background: "#374151", padding: "1px 5px", borderRadius: 3, fontSize: 12, color: "#a5f3fc" }}>{part.slice(1, -1)}</code>;
    return part;
  });
}

function DataTable({ rows }) {
  if (!rows || rows.length === 0) return null;
  const cols = Object.keys(rows[0]);
  return (
    <div style={{ overflowX: "auto", marginTop: 12, borderRadius: 8, border: "1px solid #374151" }}>
      <table style={{ borderCollapse: "collapse", width: "100%", fontSize: 12 }}>
        <thead>
          <tr style={{ background: "#111827" }}>
            {cols.map((c) => (
              <th key={c} style={{ padding: "7px 12px", color: "#60a5fa", fontWeight: 600, textAlign: "left", whiteSpace: "nowrap", borderBottom: "1px solid #374151" }}>
                {c.replace(/_/g, " ").toUpperCase()}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 100).map((row, i) => (
            <tr key={i} style={{ background: i % 2 === 0 ? "#1f2937" : "#111827", borderBottom: "1px solid #1f2937" }}>
              {cols.map((c) => {
                const v = row[c];
                const isNum = typeof v === "number" || (!isNaN(parseFloat(v)) && String(v).trim() !== "");
                return (
                  <td key={c} style={{ padding: "6px 12px", color: isNum ? "#a7f3d0" : "#d1d5db", textAlign: isNum ? "right" : "left", whiteSpace: "nowrap" }}>
                    {v == null ? "—" : isNum ? parseFloat(v).toLocaleString(undefined, { maximumFractionDigits: 2 }) : String(v)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > 100 && (
        <div style={{ padding: "6px 12px", color: "#6b7280", fontSize: 11, background: "#111827" }}>
          Showing 100 of {rows.length} rows
        </div>
      )}
    </div>
  );
}

function Message({ msg }) {
  const isUser = msg.role === "user";
  return (
    <div style={{ display: "flex", justifyContent: isUser ? "flex-end" : "flex-start", marginBottom: 16, gap: 10 }}>
      {!isUser && (
        <div style={{ width: 32, height: 32, borderRadius: "50%", background: "linear-gradient(135deg,#2563eb,#7c3aed)", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14, flexShrink: 0 }}>
          ⬡
        </div>
      )}
      <div style={{ maxWidth: "80%", minWidth: 60 }}>
        <div style={{
          background: isUser ? "#2563eb" : "#1f2937",
          border: isUser ? "none" : "1px solid #374151",
          borderRadius: isUser ? "18px 18px 4px 18px" : "4px 18px 18px 18px",
          padding: "12px 16px",
        }}>
          {isUser
            ? <span style={{ color: "#fff", fontSize: 14 }}>{msg.content}</span>
            : <MarkdownText text={msg.content} />
          }
        </div>
        {msg.data && <DataTable rows={msg.data} />}
        {msg.queries && msg.queries.length > 0 && (
          <details style={{ marginTop: 6 }}>
            <summary style={{ cursor: "pointer", color: "#4b5563", fontSize: 11 }}>
              {msg.queries.length} SQL {msg.queries.length === 1 ? "query" : "queries"} executed
            </summary>
            {msg.queries.map((q, i) => (
              <pre key={i} style={{ background: "#111827", border: "1px solid #374151", borderRadius: 6, padding: "8px 12px", fontSize: 11, color: "#86efac", overflowX: "auto", margin: "4px 0" }}>{q}</pre>
            ))}
          </details>
        )}
        {msg.loading && (
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 8, color: "#6b7280", fontSize: 12 }}>
            <span style={{ display: "inline-flex", gap: 3 }}>
              {[0, 1, 2].map((j) => (
                <span key={j} style={{ width: 6, height: 6, borderRadius: "50%", background: "#4b5563", animation: `pulse 1.2s ease-in-out ${j * 0.2}s infinite` }} />
              ))}
            </span>
            Querying Snowflake…
          </div>
        )}
      </div>
      {isUser && (
        <div style={{ width: 32, height: 32, borderRadius: "50%", background: "#374151", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14, flexShrink: 0 }}>
          👤
        </div>
      )}
    </div>
  );
}

export default function ChatPage() {
  const [company, setCompany] = useState("US2");
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [staticMode, setStaticMode] = useState(false);
  const bottomRef = useRef(null);
  const inputRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const send = async (text) => {
    const content = text || input.trim();
    if (!content || loading) return;
    setInput("");

    const userMsg = { role: "user", content };
    const loadingMsg = { role: "assistant", content: "", loading: true };
    setMessages((prev) => [...prev, userMsg, loadingMsg]);
    setLoading(true);

    // Build conversation history (exclude loading placeholders)
    const history = [...messages, userMsg].map((m) => ({ role: m.role, content: m.content }));

    try {
      const result = await sendChat(history, company);
      if (result?.static_mode) {
        setStaticMode(true);
        setMessages((prev) => prev.filter((m) => !m.loading));
        return;
      }
      setMessages((prev) => [
        ...prev.filter((m) => !m.loading),
        {
          role: "assistant",
          content: result.answer || "No answer returned.",
          data: result.data || null,
          queries: result.queries || [],
        },
      ]);
    } catch (e) {
      setMessages((prev) => [
        ...prev.filter((m) => !m.loading),
        { role: "assistant", content: `Error: ${e.message}` },
      ]);
    } finally {
      setLoading(false);
      inputRef.current?.focus();
    }
  };

  const handleKey = (e) => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); }
  };

  const clear = () => setMessages([]);

  if (staticMode) {
    return (
      <div style={{ height: "100%", display: "flex", alignItems: "center", justifyContent: "center", flexDirection: "column", gap: 12, color: "#6b7280" }}>
        <div style={{ fontSize: 40 }}>⬡</div>
        <div style={{ fontSize: 18, fontWeight: 600, color: "#374151" }}>AI Assistant requires a live backend connection</div>
        <div>Run locally with <code style={{ background: "#f3f4f6", padding: "2px 8px", borderRadius: 4 }}>start.bat</code> to use this feature.</div>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "calc(100vh - 48px)", fontFamily: "inherit" }}>
      {/* Header */}
      <div style={{ padding: "16px 24px", borderBottom: "1px solid #1f2937", display: "flex", alignItems: "center", gap: 16, flexShrink: 0 }}>
        <div style={{ width: 36, height: 36, borderRadius: "50%", background: "linear-gradient(135deg,#2563eb,#7c3aed)", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 18 }}>⬡</div>
        <div>
          <div style={{ fontWeight: 700, color: "#f9fafb", fontSize: 16 }}>Supply Chain Assistant</div>
          <div style={{ color: "#6b7280", fontSize: 12 }}>Ask anything about inventory, POs, forecasts, or supply gaps</div>
        </div>
        <div style={{ marginLeft: "auto", display: "flex", gap: 10, alignItems: "center" }}>
          <select value={company} onChange={(e) => setCompany(e.target.value)}
            style={{ background: "#1f2937", border: "1px solid #374151", color: "#f9fafb", borderRadius: 6, padding: "5px 10px", fontSize: 12 }}>
            <option value="US2">US2 — USA</option>
            <option value="ZA4">ZA4 — South Africa (Wadeville)</option>
            <option value="ZA3">ZA3 — South Africa (Stormill)</option>
            <option value="DK1">DK1 — Denmark</option>
            <option value="GH1">GH1 — Ghana</option>
          </select>
          {messages.length > 0 && (
            <button onClick={clear} style={{ background: "#374151", color: "#9ca3af", border: "none", borderRadius: 6, padding: "5px 12px", fontSize: 12, cursor: "pointer" }}>
              Clear
            </button>
          )}
        </div>
      </div>

      {/* Messages */}
      <div style={{ flex: 1, overflowY: "auto", padding: "20px 24px" }}>
        {messages.length === 0 && (
          <div style={{ maxWidth: 640, margin: "0 auto" }}>
            <div style={{ textAlign: "center", marginBottom: 32, paddingTop: 24 }}>
              <div style={{ fontSize: 48, marginBottom: 12 }}>⬡</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: "#f9fafb", marginBottom: 8 }}>
                What would you like to analyse?
              </div>
              <div style={{ color: "#6b7280", fontSize: 14 }}>
                Ask a question about your supply chain data and I'll query Snowflake to answer it.
              </div>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
              {SUGGESTIONS.map((s, i) => (
                <button key={i} onClick={() => send(s)} disabled={loading}
                  style={{
                    background: "#1f2937", border: "1px solid #374151", borderRadius: 10,
                    padding: "12px 14px", textAlign: "left", color: "#d1d5db", fontSize: 13,
                    cursor: "pointer", lineHeight: 1.4, transition: "border-color 0.15s",
                  }}
                  onMouseEnter={(e) => e.currentTarget.style.borderColor = "#2563eb"}
                  onMouseLeave={(e) => e.currentTarget.style.borderColor = "#374151"}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, i) => <Message key={i} msg={msg} />)}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div style={{ padding: "12px 24px 20px", borderTop: "1px solid #1f2937", flexShrink: 0 }}>
        <div style={{ display: "flex", gap: 10, maxWidth: 900, margin: "0 auto", alignItems: "flex-end" }}>
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKey}
            placeholder="Ask a question… (Enter to send, Shift+Enter for new line)"
            rows={1}
            style={{
              flex: 1, background: "#1f2937", border: "1px solid #374151",
              borderRadius: 12, padding: "12px 16px", color: "#f9fafb", fontSize: 14,
              resize: "none", outline: "none", fontFamily: "inherit", lineHeight: 1.5,
              maxHeight: 120, overflowY: "auto",
            }}
            onInput={(e) => {
              e.target.style.height = "auto";
              e.target.style.height = Math.min(e.target.scrollHeight, 120) + "px";
            }}
          />
          <button onClick={() => send()} disabled={loading || !input.trim()}
            style={{
              background: loading || !input.trim() ? "#374151" : "#2563eb",
              color: "#fff", border: "none", borderRadius: 10, padding: "12px 20px",
              fontSize: 14, cursor: loading || !input.trim() ? "not-allowed" : "pointer",
              fontWeight: 600, transition: "background 0.15s", whiteSpace: "nowrap",
            }}>
            {loading ? "…" : "Send ↑"}
          </button>
        </div>
        <div style={{ textAlign: "center", marginTop: 8, color: "#4b5563", fontSize: 11 }}>
          Powered by Claude · Queries run live against Snowflake · Company: {company}
        </div>
      </div>

      <style>{`
        @keyframes pulse {
          0%, 80%, 100% { opacity: 0.3; transform: scale(0.8); }
          40% { opacity: 1; transform: scale(1); }
        }
      `}</style>
    </div>
  );
}
