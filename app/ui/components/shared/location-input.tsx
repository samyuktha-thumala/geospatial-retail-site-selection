"use client";

import { useState } from "react";
import { MapPin } from "lucide-react";
import { cn } from "@/lib/utils";

interface LocationInputProps {
  onAnalyze: (lat: number, lng: number, format: string) => void;
}

const FORMAT_OPTIONS = [
  { value: "express", label: "Express" },
  { value: "standard", label: "Standard" },
  { value: "flagship", label: "Flagship" },
];

export function LocationInput({ onAnalyze }: LocationInputProps) {
  const [lat, setLat] = useState("");
  const [lng, setLng] = useState("");
  const [format, setFormat] = useState("standard");

  const isValid =
    lat !== "" &&
    lng !== "" &&
    !isNaN(Number(lat)) &&
    !isNaN(Number(lng));

  const handleAnalyze = () => {
    if (!isValid) return;
    onAnalyze(Number(lat), Number(lng), format);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && isValid) {
      handleAnalyze();
    }
  };

  return (
    <div
      className={cn(
        "border-t border-slate-200 bg-white px-5 py-3",
        "flex items-center gap-3"
      )}
    >
      <MapPin className="h-4 w-4 shrink-0 text-slate-400" />

      {/* Latitude */}
      <input
        type="text"
        placeholder="Latitude"
        value={lat}
        onChange={(e) => setLat(e.target.value)}
        onKeyDown={handleKeyDown}
        className={cn(
          "w-28 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2",
          "text-sm text-slate-900 placeholder-slate-400",
          "focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
        )}
      />

      {/* Longitude */}
      <input
        type="text"
        placeholder="Longitude"
        value={lng}
        onChange={(e) => setLng(e.target.value)}
        onKeyDown={handleKeyDown}
        className={cn(
          "w-28 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2",
          "text-sm text-slate-900 placeholder-slate-400",
          "focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
        )}
      />

      {/* Format select */}
      <select
        value={format}
        onChange={(e) => setFormat(e.target.value)}
        className={cn(
          "rounded-lg border border-slate-200 bg-slate-50 px-3 py-2",
          "text-sm text-slate-900",
          "focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
        )}
      >
        {FORMAT_OPTIONS.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
          </option>
        ))}
      </select>

      {/* Analyze button */}
      <button
        onClick={handleAnalyze}
        disabled={!isValid}
        className={cn(
          "ml-auto rounded-lg bg-blue-500 px-4 py-2 text-sm font-medium text-white",
          "transition-colors hover:bg-blue-600",
          "disabled:cursor-not-allowed disabled:opacity-40"
        )}
      >
        Analyze
      </button>
    </div>
  );
}
