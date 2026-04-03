import { useState, useEffect, useRef } from "react";
import { Play, Pause, RotateCcw } from "lucide-react";

interface TimelineControlProps {
  minYear: number;
  maxYear: number;
  currentYear: number;
  onYearChange: (year: number) => void;
  events?: Array<{ year: number; type: "historic" | "projected" }>;
}

export function TimelineControl({
  minYear,
  maxYear,
  currentYear,
  onYearChange,
  events = [],
}: TimelineControlProps) {
  const [playing, setPlaying] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    if (playing) {
      intervalRef.current = setInterval(() => {
        onYearChange(currentYear < maxYear ? currentYear + 1 : minYear);
      }, 1000);
    } else if (intervalRef.current) {
      clearInterval(intervalRef.current);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [playing, currentYear, minYear, maxYear, onYearChange]);

  const reset = () => {
    setPlaying(false);
    onYearChange(minYear);
  };

  const pct = ((currentYear - minYear) / (maxYear - minYear)) * 100;

  const historicCount = events.filter((e) => e.type === "historic" && e.year <= currentYear).length;
  const projectedCount = events.filter((e) => e.type === "projected" && e.year <= currentYear).length;

  return (
    <div data-tour="pg-timeline" className="absolute bottom-3 left-4 right-4 z-[1000] bg-white/95 backdrop-blur border border-slate-200 rounded-lg px-4 py-3 shadow-lg">
      {/* Title row */}
      <div className="flex items-center justify-between mb-2">
        <div>
          <p className="text-xs font-semibold text-slate-700">Competitor Scenario Timeline</p>
          <p className="text-[10px] text-slate-400">Visualize competitor openings across time — drag or play to animate</p>
        </div>
        <div className="flex items-center gap-3 text-[10px]">
          <div className="flex items-center gap-1">
            <span className="w-2 h-2 rounded-full bg-purple-400" />
            <span className="text-slate-500">{historicCount} historic</span>
          </div>
          <div className="flex items-center gap-1">
            <span className="w-2 h-2 rounded-full bg-red-400" />
            <span className="text-slate-500">{projectedCount} projected</span>
          </div>
        </div>
      </div>

      {/* Controls */}
      <div className="flex items-center gap-3">
        <button
          onClick={() => setPlaying(!playing)}
          className="p-1.5 rounded-md bg-blue-600 hover:bg-blue-500 text-white"
        >
          {playing ? <Pause size={14} /> : <Play size={14} />}
        </button>
        <button
          onClick={reset}
          className="p-1.5 rounded-md bg-slate-100 hover:bg-slate-200 text-slate-600"
        >
          <RotateCcw size={14} />
        </button>

        <div className="flex-1 relative">
          <div className="h-1.5 bg-slate-200 rounded-full relative">
            <div
              className="absolute top-0 left-0 h-full bg-blue-500 rounded-full transition-all"
              style={{ width: `${pct}%` }}
            />
            {events.map((evt) => {
              const evtPct = ((evt.year - minYear) / (maxYear - minYear)) * 100;
              return (
                <span
                  key={`${evt.year}-${evt.type}`}
                  className={`absolute top-1/2 -translate-y-1/2 w-2 h-2 rounded-full ${
                    evt.type === "historic" ? "bg-purple-400" : "bg-red-400"
                  }`}
                  style={{ left: `${evtPct}%` }}
                />
              );
            })}
          </div>
          <input
            type="range"
            min={minYear}
            max={maxYear}
            value={currentYear}
            onChange={(e) => onYearChange(Number(e.target.value))}
            className="absolute inset-0 w-full opacity-0 cursor-pointer"
          />
        </div>

        <span className="text-sm font-mono text-blue-600 min-w-[3rem] text-center">
          {currentYear}
        </span>
      </div>
      <div className="flex justify-between mt-1 text-[10px] text-slate-400 px-16">
        {Array.from({ length: maxYear - minYear + 1 }, (_, i) => minYear + i).map((y) => (
          <span key={y}>{y}</span>
        ))}
      </div>
    </div>
  );
}
