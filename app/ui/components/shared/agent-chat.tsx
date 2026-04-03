import { useState, useRef, useEffect, useCallback } from "react";
import { Send, Sparkles, MapPin } from "lucide-react";
import { cn } from "@/lib/utils";
import { api, type AgentResponse, type ChatMapPoint } from "@/lib/api";
import ReactMarkdown from "react-markdown";

const FORMAT_BADGE: Record<string, { text: string; bg: string }> = {
  express: { text: "text-blue-600", bg: "bg-blue-50" },
  standard: { text: "text-emerald-600", bg: "bg-emerald-50" },
  flagship: { text: "text-amber-600", bg: "bg-amber-50" },
};

function fmtRevenue(val: unknown): string {
  const n = Number(val);
  if (!n || isNaN(n)) return "";
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `$${(n / 1_000).toFixed(0)}K`;
  return `$${n.toFixed(0)}`;
}

function ResultCard({ point, index }: { point: ChatMapPoint; index: number }) {
  const props = point.properties || {};
  const str = (v: unknown) => (v != null ? String(v) : "");
  const format = str(props.format || props.recommended_format).toLowerCase();
  const badge = FORMAT_BADGE[format];
  const revenue = props.projected_revenue || props.annual_revenue || props.recommended_revenue;
  const pois = str(props.total_poi_count);
  const urbanicity = str(props.urbanicity || props.urbanicity_category);
  const zip = str(props.zip_code);

  return (
    <div className="flex items-start gap-2 rounded-md border border-slate-200 bg-white px-2.5 py-2">
      <span className="text-[10px] font-bold text-slate-400 mt-0.5 w-4 shrink-0">
        {index + 1}
      </span>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-1.5 mb-0.5">
          <span className="text-[11px] font-semibold text-slate-800 truncate">{point.label || "Location"}</span>
          {format && badge && (
            <span className={cn("text-[9px] font-medium px-1.5 py-0.5 rounded-full capitalize border border-slate-200", badge.text, badge.bg)}>
              {format}
            </span>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-x-2.5 gap-y-0.5 text-[10px] text-slate-500">
          {revenue ? <span className="font-semibold text-slate-700">{fmtRevenue(revenue)}</span> : null}
          {str(props.city) ? <span>{str(props.city)}</span> : null}
          {urbanicity ? <span className="capitalize">{urbanicity}</span> : null}
          {zip ? <span>ZIP {zip}</span> : null}
          {pois ? <span>{pois} POIs</span> : null}
        </div>
      </div>
      <MapPin size={10} className="text-violet-400 shrink-0 mt-1" />
    </div>
  );
}

interface Message {
  id: string;
  role: "user" | "assistant";
  text: string;
  suggestions?: string[];
  mapPoints?: ChatMapPoint[];
}

interface AgentChatProps {
  pageContext: "network" | "expansion";
  onMapPoints?: (points: AgentResponse["map_points"]) => void;
  onH3Trigger?: (storeId: string) => void;
  className?: string;
}

export function AgentChat({ pageContext, onMapPoints, onH3Trigger, className }: AgentChatProps) {
  const [messages, setMessages] = useState<Message[]>([
    {
      id: "welcome",
      role: "assistant",
      text:
        pageContext === "network"
          ? "I can help you analyze your NY State store network — performance, competitors, closure risks. What would you like to know?"
          : "Tell me where in New York State you'd like to expand — a borough, city, or region — and I'll find the best candidate sites.",
      suggestions:
        pageContext === "network"
          ? ["Top 10 stores by revenue", "Performance drivers in Manhattan", "Competitor density in Brooklyn"]
          : ["Expand in NYC — top 10 sites", "Suburban sites in Westchester", "Best sites in Brooklyn by POI density"],
    },
  ]);
  const [input, setInput] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const endRef = useRef<HTMLDivElement>(null);

  const scroll = useCallback(() => endRef.current?.scrollIntoView({ behavior: "smooth" }), []);
  useEffect(() => scroll(), [messages, scroll]);

  const handleSend = async (text?: string) => {
    const message = text ?? input.trim();
    if (!message || isLoading) return;

    setMessages((prev) => [...prev, { id: `u-${Date.now()}`, role: "user", text: message }]);
    setInput("");
    setIsLoading(true);

    try {
      const history = messages
        .filter((m) => m.id !== "welcome")
        .map((m) => ({ role: m.role, content: m.text }));

      // For expansion context: inject working set IDs from the last agent result
      let augmentedMessage = message;
      if (pageContext === "expansion") {
        const lastWithPoints = [...messages].reverse().find((m) => m.role === "assistant" && m.mapPoints && m.mapPoints.length > 0);
        if (lastWithPoints?.mapPoints) {
          const ids = lastWithPoints.mapPoints
            .map((p) => p.label || (p.properties?.location_id as string))
            .filter(Boolean);
          if (ids.length > 0) {
            augmentedMessage = `[WORKING_SET: ${ids.join(", ")}]\n\n${message}`;
          }
        }
      }

      const response: AgentResponse = await api.agentChat(augmentedMessage, pageContext, history);

      setMessages((prev) => [
        ...prev,
        {
          id: `a-${Date.now()}`,
          role: "assistant",
          text: response.response,
          suggestions: response.suggestions,
          mapPoints: response.map_points?.filter((p) => p.properties?.type !== "h3_trigger"),
        },
      ]);

      if (response.map_points?.length) {
        // Check for H3 trigger
        const h3Trigger = response.map_points.find((p) => p.properties?.type === "h3_trigger");
        if (h3Trigger && onH3Trigger) {
          onH3Trigger(h3Trigger.properties.store_id as string);
        }
        // Pass real map points (not h3 triggers) to parent
        const realPoints = response.map_points.filter((p) => p.properties?.type !== "h3_trigger");
        if (realPoints.length && onMapPoints) {
          onMapPoints(realPoints);
        }
      }
    } catch {
      setMessages((prev) => [
        ...prev,
        { id: `err-${Date.now()}`, role: "assistant", text: "Sorry, I encountered an error. Please try again." },
      ]);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className={cn("flex flex-col h-full bg-white rounded-lg border border-slate-200 shadow-sm overflow-hidden", className)}>
      {/* Header */}
      <div className="flex items-center gap-2 px-3 py-2.5 border-b border-slate-100 shrink-0 bg-gradient-to-r from-violet-50 to-white">
        <Sparkles size={14} className="text-violet-500" />
        <span className="text-xs font-semibold text-slate-800">Site Agent</span>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-3 space-y-3 min-h-0">
        {messages.map((msg) => (
          <div key={msg.id} className={cn("flex flex-col", msg.role === "user" ? "items-end" : "items-start")}>
            {/* Text bubble */}
            <div
              className={cn(
                "max-w-[92%] rounded-lg px-3 py-2 text-xs leading-relaxed",
                msg.role === "user" ? "bg-violet-600 text-white" : "bg-slate-50 text-slate-700 border border-slate-100"
              )}
            >
              {msg.role === "assistant" ? (
                <div className="prose prose-xs prose-slate max-w-none [&_table]:hidden [&_p]:my-1 [&_strong]:text-slate-900 [&_ul]:my-1 [&_li]:my-0.5 [&_h1]:text-sm [&_h2]:text-xs [&_h3]:text-xs overflow-x-auto">
                  <ReactMarkdown>{msg.text}</ReactMarkdown>
                </div>
              ) : (
                msg.text
              )}
            </div>

            {/* Result cards — rendered from map_points */}
            {msg.role === "assistant" && msg.mapPoints && msg.mapPoints.length > 0 && (
              <div className="w-full mt-2 space-y-1.5">
                <div className="flex items-center gap-1 text-[10px] text-violet-500 px-1">
                  <MapPin size={10} />
                  <span>{msg.mapPoints.length} locations on map</span>
                </div>
                {msg.mapPoints.slice(0, 15).map((pt, i) => (
                  <ResultCard key={`${msg.id}-pt-${i}`} point={pt} index={i} />
                ))}
                {msg.mapPoints.length > 15 && (
                  <div className="text-[10px] text-slate-400 px-1">
                    +{msg.mapPoints.length - 15} more on map
                  </div>
                )}
              </div>
            )}

            {/* Suggestion chips */}
            {msg.suggestions && msg.suggestions.length > 0 && (
              <div className="mt-2 flex flex-wrap gap-1 max-w-[92%]">
                {msg.suggestions.map((s) => (
                  <button
                    key={s}
                    onClick={() => handleSend(s)}
                    className="rounded-full border border-slate-200 bg-white px-2 py-0.5 text-[10px] text-slate-500 transition-colors hover:border-violet-400 hover:text-violet-600"
                  >
                    {s}
                  </button>
                ))}
              </div>
            )}
          </div>
        ))}
        {isLoading && (
          <div className="flex justify-start">
            <div className="rounded-lg bg-slate-50 border border-slate-100 px-3 py-2 text-xs text-slate-400">
              <span className="inline-flex gap-1">
                <span className="animate-bounce">.</span>
                <span className="animate-bounce" style={{ animationDelay: "0.1s" }}>.</span>
                <span className="animate-bounce" style={{ animationDelay: "0.2s" }}>.</span>
              </span>
            </div>
          </div>
        )}
        <div ref={endRef} />
      </div>

      {/* Input */}
      <div className="border-t border-slate-100 p-2 shrink-0">
        <div className="flex items-center gap-1.5">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                handleSend();
              }
            }}
            placeholder={pageContext === "network" ? "Ask about your network..." : "Where should we expand?"}
            className="flex-1 rounded-md border border-slate-200 bg-slate-50 px-2.5 py-1.5 text-xs text-slate-900 placeholder-slate-400 focus:border-violet-500 focus:outline-none focus:ring-1 focus:ring-violet-500"
          />
          <button
            onClick={() => handleSend()}
            disabled={!input.trim() || isLoading}
            className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-violet-500 text-white transition-colors hover:bg-violet-600 disabled:cursor-not-allowed disabled:opacity-40"
          >
            <Send size={12} />
          </button>
        </div>
      </div>
    </div>
  );
}
