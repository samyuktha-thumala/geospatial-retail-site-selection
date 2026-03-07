"use client";

import { useState } from "react";
import { ChevronDown, ChevronRight, Loader2 } from "lucide-react";
import { cn } from "@/lib/utils";

interface UrbanicityDistances {
  urban: number;
  suburban: number;
  rural: number;
}

interface ScenarioParams {
  competitorYear: number;
  minDistanceFromNetwork: UrbanicityDistances;
  minDistanceBetweenNew: UrbanicityDistances;
  finalLocationsCount: number;
  excludedClosureRisks: string[];
}

interface ClosureOption {
  id: string;
  name: string;
  risk: number;
}

interface ScenarioPanelProps {
  onSimulate: (params: ScenarioParams) => void;
  isLoading: boolean;
  closureCandidates?: ClosureOption[];
}

const SLIDER_CLASS = cn(
  "h-1.5 w-full cursor-pointer appearance-none rounded-full bg-slate-200",
  "[&::-webkit-slider-thumb]:h-3.5 [&::-webkit-slider-thumb]:w-3.5",
  "[&::-webkit-slider-thumb]:appearance-none [&::-webkit-slider-thumb]:rounded-full",
  "[&::-webkit-slider-thumb]:bg-blue-500 [&::-webkit-slider-thumb]:shadow-md"
);

function UrbanicitySliders({
  label,
  values,
  onChange,
  min = 0.5,
  max = 20,
  step = 0.5,
}: {
  label: string;
  values: UrbanicityDistances;
  onChange: (v: UrbanicityDistances) => void;
  min?: number;
  max?: number;
  step?: number;
}) {
  return (
    <div>
      <p className="text-xs font-medium text-slate-500 mb-2">{label}</p>
      <div className="space-y-2 pl-1">
        {(["urban", "suburban", "rural"] as const).map((key) => (
          <div key={key} className="flex items-center gap-2">
            <span className="text-[10px] text-slate-400 w-14 capitalize">{key}</span>
            <input
              type="range"
              min={min}
              max={max}
              step={step}
              value={values[key]}
              onChange={(e) => onChange({ ...values, [key]: Number(e.target.value) })}
              className={SLIDER_CLASS}
            />
            <span className="text-[11px] font-semibold text-slate-700 w-10 text-right">{values[key]} mi</span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function ScenarioPanel({ onSimulate, isLoading, closureCandidates = [] }: ScenarioPanelProps) {
  const [isCollapsed, setIsCollapsed] = useState(false);
  const [competitorYear, setCompetitorYear] = useState(2025);
  const [minDistanceFromNetwork, setMinDistanceFromNetwork] = useState<UrbanicityDistances>({
    urban: 1.5, suburban: 3, rural: 5,
  });
  const [minDistanceBetweenNew, setMinDistanceBetweenNew] = useState<UrbanicityDistances>({
    urban: 2, suburban: 5, rural: 8,
  });
  const [finalLocationsCount, setFinalLocationsCount] = useState(10);
  const [excludedIds, setExcludedIds] = useState<Set<string>>(new Set());

  const toggleExclude = (id: string) => {
    setExcludedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };

  const handleSimulate = () => {
    onSimulate({
      competitorYear,
      minDistanceFromNetwork,
      minDistanceBetweenNew,
      finalLocationsCount,
      excludedClosureRisks: Array.from(excludedIds),
    });
  };

  return (
    <div
      className={cn(
        "rounded-xl border border-slate-200 bg-white shadow-sm w-full",
        "transition-all"
      )}
    >
      <button
        onClick={() => setIsCollapsed((prev) => !prev)}
        className={cn(
          "flex w-full items-center justify-between px-5 py-4",
          "text-left transition-colors hover:bg-slate-50 rounded-t-xl",
          !isCollapsed && "border-b border-slate-200"
        )}
      >
        <h3 className="text-sm font-semibold text-slate-900">Scenario Modeling</h3>
        {isCollapsed ? (
          <ChevronRight className="h-4 w-4 text-slate-400" />
        ) : (
          <ChevronDown className="h-4 w-4 text-slate-400" />
        )}
      </button>

      {!isCollapsed && (
        <>
        <div className="space-y-4 p-5">
          {/* Competitor Year */}
          <div>
            <div className="mb-2 flex items-center justify-between">
              <label className="text-xs font-medium text-slate-500">Competitor Data Year</label>
              <span className="text-xs font-semibold text-slate-900">{competitorYear}</span>
            </div>
            <input
              type="range"
              min={2021}
              max={2030}
              value={competitorYear}
              onChange={(e) => setCompetitorYear(Number(e.target.value))}
              className={SLIDER_CLASS}
            />
            <div className="mt-1 flex justify-between text-[10px] text-slate-400">
              <span>2021</span>
              <span>2030</span>
            </div>
          </div>

          {/* Min Distance from Network — per urbanicity */}
          <UrbanicitySliders
            label="Min Distance from Network (mi)"
            values={minDistanceFromNetwork}
            onChange={setMinDistanceFromNetwork}
          />

          {/* Min Distance Between New — per urbanicity */}
          <UrbanicitySliders
            label="Min Distance Between New (mi)"
            values={minDistanceBetweenNew}
            onChange={setMinDistanceBetweenNew}
          />

          {/* Final Locations Count */}
          <div>
            <label className="mb-2 block text-xs font-medium text-slate-500">
              Final Locations Count
            </label>
            <input
              type="number"
              min={1}
              max={50}
              value={finalLocationsCount}
              onChange={(e) => {
                const val = Math.min(50, Math.max(1, Number(e.target.value)));
                setFinalLocationsCount(val);
              }}
              className={cn(
                "w-full rounded-lg border border-slate-200 bg-slate-50 px-3 py-2",
                "text-sm text-slate-900",
                "focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
              )}
            />
          </div>

          {/* Exclude At-Risk Locations */}
          {closureCandidates.length > 0 && (
            <div>
              <div className="flex items-center justify-between mb-2">
                <p className="text-xs font-medium text-slate-500">Exclude At-Risk Stores</p>
                <button
                  onClick={() => {
                    if (excludedIds.size === closureCandidates.length) {
                      setExcludedIds(new Set());
                    } else {
                      setExcludedIds(new Set(closureCandidates.map((c) => c.id)));
                    }
                  }}
                  className="text-[10px] text-blue-500 hover:text-blue-700 font-medium"
                >
                  {excludedIds.size === closureCandidates.length ? "Deselect All" : "Select All"}
                </button>
              </div>
              <div className="max-h-28 overflow-y-auto space-y-1 rounded-lg border border-slate-100 bg-slate-50 p-2">
                {closureCandidates.map((cc) => (
                  <label key={cc.id} className="flex items-center gap-2 text-[11px] text-slate-600 cursor-pointer hover:text-slate-900">
                    <input
                      type="checkbox"
                      checked={excludedIds.has(cc.id)}
                      onChange={() => toggleExclude(cc.id)}
                      className="rounded border-slate-300 bg-white text-orange-500 focus:ring-orange-500 focus:ring-offset-0 w-3.5 h-3.5"
                    />
                    <span className="truncate flex-1">{cc.name}</span>
                    <span className="text-[10px] text-orange-500 font-medium shrink-0">{(cc.risk * 100).toFixed(0)}%</span>
                  </label>
                ))}
              </div>
            </div>
          )}

        </div>
          {/* Calculate Button — sticky at bottom */}
          <div className="p-5 pt-3 border-t border-slate-100 shrink-0">
            <button
              onClick={handleSimulate}
              disabled={isLoading}
              className={cn(
                "flex w-full items-center justify-center gap-2 rounded-lg px-4 py-2.5",
                "bg-blue-500 text-sm font-medium text-white",
                "transition-colors hover:bg-blue-600",
                "disabled:cursor-not-allowed disabled:opacity-50"
              )}
            >
              {isLoading ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Calculating...
                </>
              ) : (
                "Calculate Demand"
              )}
            </button>
          </div>
        </>
      )}
    </div>
  );
}
