"use client";

import { useState, useRef, useEffect, useCallback } from "react";
import { MessageCircle, X, Send } from "lucide-react";
import { cn } from "@/lib/utils";
import { api, type ChatResponse } from "@/lib/api";
import ReactMarkdown from "react-markdown";

interface Message {
  id: string;
  role: "user" | "bot";
  text: string;
  suggestions?: string[];
}

export function ChatBot() {
  const [isOpen, setIsOpen] = useState(false);
  const [messages, setMessages] = useState<Message[]>([
    {
      id: "welcome",
      role: "bot",
      text: "Hello! I can help you analyze site selection data, compare locations, and answer questions about your network. What would you like to know?",
      suggestions: [
        "Top 10 stores by annual revenue",
        "Top 20 rural expansion hotspots",
        "Competitor count by brand",
      ],
    },
  ]);
  const [inputText, setInputText] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [conversationId, setConversationId] = useState<string | undefined>();
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = useCallback(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages, scrollToBottom]);

  // Listen for location-selected events from the map
  useEffect(() => {
    const handler = (e: Event) => {
      const detail = (e as CustomEvent).detail;
      if (!detail) return;
      const { lat, lng, name, format, sales } = detail;
      setIsOpen(true);
      const prompt = `Show me the annual revenue and monthly sales for the store named "${name}"`;
      // Small delay so the panel opens first
      setTimeout(() => handleSend(prompt), 100);
    };
    window.addEventListener("location-selected", handler);
    return () => window.removeEventListener("location-selected", handler);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const handleSend = async (text?: string) => {
    const message = text ?? inputText.trim();
    if (!message || isLoading) return;

    const userMsg: Message = {
      id: `user-${Date.now()}`,
      role: "user",
      text: message,
    };
    setMessages((prev) => [...prev, userMsg]);
    setInputText("");
    setIsLoading(true);

    try {
      // Build conversation history from previous messages (excluding welcome)
      const history = messages
        .filter((m) => m.id !== "welcome")
        .map((m) => ({
          role: m.role === "user" ? "user" : "assistant",
          content: m.text,
        }));

      const response: ChatResponse = await api.sendChat(message, undefined, history, conversationId);
      if (response.conversation_id) {
        setConversationId(response.conversation_id);
      }
      const botMsg: Message = {
        id: `bot-${Date.now()}`,
        role: "bot",
        text: response.response,
        suggestions: response.suggestions,
      };
      setMessages((prev) => [...prev, botMsg]);
    } catch {
      const errorMsg: Message = {
        id: `error-${Date.now()}`,
        role: "bot",
        text: "Sorry, I encountered an error. Please try again.",
      };
      setMessages((prev) => [...prev, errorMsg]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <>
      {/* Toggle button */}
      <button
        data-tour="chatbot-button"
        onClick={() => setIsOpen((prev) => !prev)}
        className={cn(
          "fixed bottom-6 right-6 z-[9999] flex h-14 w-14 items-center justify-center",
          "rounded-full bg-gradient-to-br from-blue-500 to-blue-700 shadow-lg",
          "transition-transform hover:scale-105 active:scale-95",
          "focus:outline-none focus:ring-2 focus:ring-blue-400 focus:ring-offset-2 focus:ring-offset-white"
        )}
        aria-label={isOpen ? "Close chat" : "Open chat"}
      >
        {isOpen ? (
          <X className="h-6 w-6 text-white" />
        ) : (
          <MessageCircle className="h-6 w-6 text-white" />
        )}
      </button>

      {/* Chat panel */}
      {isOpen && (
        <div
          className={cn(
            "fixed bottom-24 right-6 z-[9999] flex flex-col",
            "w-[396px] rounded-xl border border-slate-200 bg-white shadow-2xl"
          )}
          style={{ maxHeight: "600px" }}
        >
          {/* Header */}
          <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
            <div className="flex items-center gap-2">
              <div className="h-2.5 w-2.5 rounded-full bg-emerald-400" />
              <div>
                <h3 className="text-sm font-semibold text-slate-900 leading-tight">Site Agent</h3>
                <p className="text-[10px] text-slate-400">powered by Genie</p>
              </div>
            </div>
            <button
              onClick={() => setIsOpen(false)}
              className="rounded-md p-1 text-slate-400 transition-colors hover:bg-slate-100 hover:text-slate-700"
            >
              <X className="h-4 w-4" />
            </button>
          </div>

          {/* Messages */}
          <div className="flex-1 space-y-3 overflow-y-auto p-4" style={{ maxHeight: "440px" }}>
            {messages.map((msg) => (
              <div
                key={msg.id}
                className={cn(
                  "flex",
                  msg.role === "user" ? "justify-end" : "justify-start"
                )}
              >
                <div
                  className={cn(
                    "max-w-[85%] rounded-lg px-3.5 py-2.5 text-sm leading-relaxed",
                    msg.role === "user"
                      ? "bg-blue-600 text-white"
                      : "bg-slate-100 text-slate-700"
                  )}
                >
                  {msg.role === "bot" ? (
                    <div className="prose prose-sm prose-slate max-w-none [&_p]:my-1 [&_ul]:my-1 [&_ol]:my-1 [&_li]:my-0.5 [&_strong]:text-slate-900 [&_h3]:text-sm [&_h3]:font-semibold [&_h3]:my-1">
                      <ReactMarkdown>{msg.text}</ReactMarkdown>
                    </div>
                  ) : (
                    msg.text
                  )}
                  {/* Suggestion chips */}
                  {msg.suggestions && msg.suggestions.length > 0 && (
                    <div className="mt-2.5 flex flex-wrap gap-1.5">
                      {msg.suggestions.map((suggestion) => (
                        <button
                          key={suggestion}
                          onClick={() => handleSend(suggestion)}
                          className={cn(
                            "rounded-full border border-slate-300 bg-white px-2.5 py-1",
                            "text-xs text-slate-600 transition-colors",
                            "hover:border-blue-400 hover:bg-blue-50 hover:text-blue-600"
                          )}
                        >
                          {suggestion}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            ))}
            {isLoading && (
              <div className="flex justify-start">
                <div className="rounded-lg bg-slate-100 px-3.5 py-2.5 text-sm text-slate-400">
                  <span className="inline-flex gap-1">
                    <span className="animate-bounce">.</span>
                    <span className="animate-bounce" style={{ animationDelay: "0.1s" }}>.</span>
                    <span className="animate-bounce" style={{ animationDelay: "0.2s" }}>.</span>
                  </span>
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>

          {/* Input */}
          <div className="border-t border-slate-200 p-3">
            <div className="flex items-center gap-2">
              <input
                type="text"
                value={inputText}
                onChange={(e) => setInputText(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Ask about your network..."
                className={cn(
                  "flex-1 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2",
                  "text-sm text-slate-900 placeholder-slate-400",
                  "focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                )}
              />
              <button
                onClick={() => handleSend()}
                disabled={!inputText.trim() || isLoading}
                className={cn(
                  "flex h-9 w-9 shrink-0 items-center justify-center rounded-lg",
                  "bg-blue-500 text-white transition-colors",
                  "hover:bg-blue-600 disabled:cursor-not-allowed disabled:opacity-40"
                )}
              >
                <Send className="h-4 w-4" />
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
