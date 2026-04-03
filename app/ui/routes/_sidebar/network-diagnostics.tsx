import { createFileRoute } from "@tanstack/react-router";
import { useState, useEffect, useCallback, useRef } from "react";
import { api } from "@/lib/api";
import type { Location, Competitor, ClosureCandidate, Isochrone, Kpi, H3FeatureCollection } from "@/lib/api";
import { NetworkMap, type MapFilter, type H3Metric } from "@/components/maps/network-map";
import { GuidedTour, type TourStep } from "@/components/shared/guided-tour";
import { AlertTriangle, Search, ChevronDown, X, TrendingUp, MapPin, Sparkles } from "lucide-react";
import { AgentChat } from "@/components/shared/agent-chat";
import type { AgentResponse } from "@/lib/api";

export const Route = createFileRoute("/_sidebar/network-diagnostics")({
  component: NetworkDiagnosticsPage,
});

const H3_METRIC_OPTIONS: { value: H3Metric; label: string }[] = [
  { value: "total_population", label: "Population" },
  { value: "median_household_income", label: "Income" },
  { value: "total_competitor_count", label: "Competition" },
  { value: "total_poi_count", label: "POI Density" },
  { value: "median_age", label: "Median Age" },
];

const FILTER_OPTIONS: { value: MapFilter; label: string }[] = [
  { value: "all", label: "All" },
  { value: "express", label: "Express" },
  { value: "standard", label: "Standard" },
  { value: "flagship", label: "Flagship" },
  { value: "competitors", label: "Competitors" },
  { value: "at_risk", label: "At Risk" },
];

function riskLevel(score: number): { label: string; color: string; bg: string; barColor: string } {
  if (score >= 75) return { label: "High", color: "text-red-700", bg: "bg-red-50", barColor: "bg-red-500" };
  if (score >= 60) return { label: "Medium", color: "text-amber-700", bg: "bg-amber-50", barColor: "bg-amber-500" };
  return { label: "Elevated", color: "text-yellow-700", bg: "bg-yellow-50", barColor: "bg-yellow-400" };
}

function NetworkDiagnosticsPage() {
  const [locations, setLocations] = useState<Location[]>([]);
  const [competitors, setCompetitors] = useState<Competitor[]>([]);
  const [closureCandidates, setClosureCandidates] = useState<ClosureCandidate[]>([]);
  const [isochrones, setIsochrones] = useState<Isochrone[]>([]);
  const [kpis, setKpis] = useState<Kpi[]>([]);
  const [loading, setLoading] = useState(true);

  const [filter, setFilter] = useState<MapFilter>("all");
  const [h3Metric, setH3Metric] = useState<H3Metric>("total_population");
  const [h3Data, setH3Data] = useState<H3FeatureCollection | null>(null);
  const [h3Loading, setH3Loading] = useState(false);

  const [selectedLocationIds, setSelectedLocationIds] = useState<Set<string>>(new Set());
  const [selectionWarning, setSelectionWarning] = useState<string | null>(null);

  // Agent map points
  const [agentPoints, setAgentPoints] = useState<AgentResponse["map_points"]>([]);
  const [agentOnlyMap, setAgentOnlyMap] = useState(false);

  // Right panel state
  const [storeSearch, setStoreSearch] = useState("");
  const [selectedRiskId, setSelectedRiskId] = useState<string | null>(null);
  const [panelSection, setPanelSection] = useState<"agent" | "top" | "risk" | "search">("agent");

  useEffect(() => {
    Promise.all([
      api.listLocations(),
      api.listCompetitors(),
      api.listClosureCandidates(),
      api.listIsochrones().catch(() => [] as Isochrone[]),
      api.listKpis(),
    ]).then(([l, c, cc, iso, k]) => {
      setLocations(l);
      setCompetitors(c);
      setClosureCandidates(cc);
      setIsochrones(iso);
      setKpis(k);
      setLoading(false);
    });
  }, []);

  // Fetch H3 data when exactly one location is selected
  useEffect(() => {
    if (selectedLocationIds.size === 1) {
      const storeId = Array.from(selectedLocationIds)[0];
      setH3Loading(true);
      api.getH3Features(storeId)
        .then((data) => setH3Data(data))
        .catch(() => setH3Data(null))
        .finally(() => setH3Loading(false));
    } else {
      setH3Data(null);
    }
  }, [selectedLocationIds]);

  // Map click: toggle (supports multi-select with shift+drag)
  const handleSelectLocations = useCallback((ids: string[]) => {
    setSelectionWarning(null);
    setH3Data(null);
    if (ids.length === 1) {
      setSelectedLocationIds((prev) => {
        const next = new Set(prev);
        if (next.has(ids[0])) {
          next.delete(ids[0]);
        } else {
          next.add(ids[0]);
        }
        if (next.size > 10) {
          setSelectionWarning(`${next.size} selected — trade areas shown for ≤10`);
        }
        return next;
      });
    } else {
      // Box select — replace
      if (ids.length > 10) {
        setSelectionWarning(`${ids.length} selected — trade areas shown for ≤10`);
      }
      setSelectedLocationIds(new Set(ids));
    }
  }, []);

  // Panel click: always replace selection (single location)
  const handlePanelSelect = useCallback((id: string) => {
    setSelectionWarning(null);
    setH3Data(null);
    setSelectedLocationIds((prev) => {
      // Deselect if already the only selected
      if (prev.size === 1 && prev.has(id)) {
        return new Set();
      }
      return new Set([id]);
    });
  }, []);

  const clearSelection = useCallback(() => {
    setSelectedLocationIds(new Set());
    setSelectionWarning(null);
    setH3Data(null);
  }, []);

  // Tour state
  const [showTour, setShowTour] = useState(false);
  useEffect(() => {
    if (!loading && locations.length > 0) {
      const seen = localStorage.getItem("tour-network-seen");
      if (!seen) setShowTour(true);
    }
  }, [loading, locations.length]);

  // Listen for tour trigger from top nav button
  useEffect(() => {
    const handler = () => setShowTour(true);
    window.addEventListener("start-tour", handler);
    return () => window.removeEventListener("start-tour", handler);
  }, []);

  const tourSteps: TourStep[] = [
    {
      target: "kpi-cards",
      title: "Revenue Overview",
      description: "Monitor aggregate annual revenue segmented by store format — Express, Standard, and Flagship — alongside active location and competitor counts.",
      position: "bottom",
    },
    {
      target: "network-map",
      title: "Network Visualization",
      description: "View the complete store network across all geographies. Circle size corresponds to store format, providing an at-a-glance view of your market presence.",
      position: "right",
      action: () => setFilter("all"),
    },
    {
      target: "filter-express",
      title: "Express Format",
      description: "Isolate Express locations — smaller-footprint stores optimized for high-density urban environments.",
      position: "top",
      action: () => setFilter("express"),
    },
    {
      target: "filter-standard",
      title: "Standard Format",
      description: "View Standard locations — mid-size stores that form the core of the network across suburban and urban markets.",
      position: "top",
      action: () => setFilter("standard"),
    },
    {
      target: "filter-flagship",
      title: "Flagship Format",
      description: "Highlight Flagship locations — large-format, high-revenue anchor stores positioned in key metropolitan markets.",
      position: "top",
      action: () => setFilter("flagship"),
    },
    {
      target: "filter-competitors",
      title: "Competitive Landscape",
      description: "Overlay competitor positions across brands to assess competitive density and identify areas of market saturation or opportunity.",
      position: "top",
      action: () => setFilter("competitors"),
    },
    {
      target: "filter-at_risk",
      title: "At-Risk Locations",
      description: "Surface stores flagged for elevated closure risk based on underperformance metrics. Orange markers indicate locations requiring attention.",
      position: "top",
      action: () => { setFilter("at_risk"); setPanelSection("risk"); },
    },
    {
      target: "panel-risk",
      title: "Risk Analysis",
      description: "Drill into any at-risk store to review its closure risk score, key underperformance indicators, and variance from network benchmarks.",
      position: "left",
    },
    {
      target: "panel-risk",
      title: "AI Site Agent",
      description: "Switch to the Agent tab for AI-powered analysis — ask about top performers, competitive dynamics, and network insights using natural language.",
      position: "left",
    },
  ];

  const handleTourComplete = useCallback(() => {
    setShowTour(false);
    setFilter("all");
    localStorage.setItem("tour-network-seen", "true");
  }, []);

  // Top 10 locations by monthly sales
  const top10 = [...locations].sort((a, b) => b.monthly_sales - a.monthly_sales).slice(0, 10);

  // Search-filtered locations for selector
  const searchLower = storeSearch.toLowerCase();
  const searchedLocations = storeSearch
    ? locations.filter((l) =>
        l.id.toLowerCase().includes(searchLower) ||
        l.name.toLowerCase().includes(searchLower)
      )
    : [];

  if (loading) {
    return (
      <div className="flex flex-col h-full p-6 space-y-4">
        <div className="grid grid-cols-6 gap-3">
          {[1, 2, 3, 4, 5, 6].map((i) => (
            <div key={i} className="h-20 bg-white border border-slate-200 rounded-lg animate-pulse" />
          ))}
        </div>
        <div className="flex-1 bg-white border border-slate-200 rounded-lg animate-pulse" />
      </div>
    );
  }

  const selectedLoc = selectedLocationIds.size === 1
    ? locations.find((l) => l.id === Array.from(selectedLocationIds)[0])
    : null;

  return (
    <div className="flex flex-col h-full max-w-[1920px] mx-auto relative">
      {/* Tour */}
      <GuidedTour
        steps={tourSteps}
        isOpen={showTour}
        onComplete={handleTourComplete}
        introTitle="Network Diagnostics"
        introDescription="Gain a comprehensive view of your physical footprint. Identify the geographic drivers behind location performance — demographics, competitive density, and economic activity. Investigate at-risk locations to pinpoint the root causes of underperformance, and leverage the AI-powered Site Agent for on-demand network intelligence."
      />

      {/* KPIs + Format Cards — revenue KPIs first, format totals, then count KPIs */}
      <div data-tour="kpi-cards" className="grid grid-cols-6 gap-3 px-6 pt-5 pb-3 shrink-0">
        {(() => {
          const revenueKpis = kpis.filter((k) => k.label === "Annual Revenue");
          const countKpis = kpis.filter((k) => !k.label.toLowerCase().includes("revenue"));
          const formatLabels: Record<string, string> = { express: "Express", standard: "Standard", flagship: "Flagship" };
          const formatColors: Record<string, string> = { express: "#3b82f6", standard: "#22c55e", flagship: "#f59e0b" };
          const fmtDollarsK = (k: number) => {
            if (k >= 1_000_000) return `$${(k / 1_000_000).toFixed(1)}B`;
            if (k >= 1_000) return `$${(k / 1_000).toFixed(1)}M`;
            return `$${k.toFixed(0)}K`;
          };

          const kpiCard = (kpi: typeof kpis[0]) => (
            <div key={kpi.label} className="bg-white border border-slate-200 rounded-lg px-4 py-3.5 shadow-sm">
              <p className="text-[11px] font-semibold text-slate-500 mb-1 truncate uppercase tracking-wide">{kpi.label}</p>
              <p className="text-2xl font-bold text-slate-900 tracking-tight">{kpi.value}</p>
              {kpi.subtext && <p className="text-[10px] text-slate-400 mt-1">{kpi.subtext}</p>}
            </div>
          );

          const formatCard = (fmt: "express" | "standard" | "flagship") => {
            const fmtLocations = locations.filter((l) => l.format === fmt);
            // monthly_sales is in K — show TOTAL revenue by format so they add up to Annual Revenue
            const totalAnnualK = fmtLocations.reduce((sum, l) => sum + l.monthly_sales * 12, 0);
            const totalMonthlyK = fmtLocations.reduce((sum, l) => sum + l.monthly_sales, 0);
            return (
              <div key={fmt} className="bg-white border border-slate-200 rounded-lg px-4 py-3.5 shadow-sm">
                <div className="flex items-center justify-between mb-1">
                  <span className="text-[11px] font-semibold text-slate-500 uppercase tracking-wide">{formatLabels[fmt]}</span>
                  <span
                    className="text-[10px] px-1.5 py-0.5 rounded-full"
                    style={{
                      backgroundColor: `${formatColors[fmt]}15`,
                      color: formatColors[fmt],
                      border: `1px solid ${formatColors[fmt]}40`,
                    }}
                  >
                    {fmtLocations.length}
                  </span>
                </div>
                <p className="text-2xl font-bold text-slate-900 tracking-tight">
                  {fmtDollarsK(totalAnnualK)}
                </p>
                <p className="text-[10px] text-slate-400 mt-1">
                  {fmtDollarsK(totalMonthlyK)} monthly (T12M)
                </p>
              </div>
            );
          };

          return (
            <>
              {revenueKpis.map(kpiCard)}
              {(["express", "standard", "flagship"] as const).map(formatCard)}
              {countKpis.map(kpiCard)}
            </>
          );
        })()}
      </div>

      {/* Selection info bar */}
      {(selectedLocationIds.size > 0 || selectionWarning) && (
        <div className="flex items-center px-6 pb-2 shrink-0 gap-2 text-[11px]">
          {selectedLocationIds.size > 0 && (
            <>
              <span className="text-blue-600 font-medium">{selectedLocationIds.size} selected</span>
              {selectedLocationIds.size === 1 && selectedLoc && (
                <span className="text-slate-400">— {selectedLoc.name}</span>
              )}
              {selectedLocationIds.size <= 10 && selectedLocationIds.size > 1 && (
                <span className="text-slate-400">— showing trade areas</span>
              )}
              <button onClick={clearSelection} className="ml-1 p-0.5 rounded hover:bg-slate-100">
                <X size={12} className="text-slate-400" />
              </button>
            </>
          )}
          {selectionWarning && (
            <span className="text-[10px] text-amber-600 bg-amber-50 border border-amber-200 px-2 py-0.5 rounded">
              {selectionWarning}
            </span>
          )}
        </div>
      )}

      {/* Map + Right Panel — fill remaining height */}
      <div className="flex gap-4 px-6 pb-4 flex-1 min-h-0">
        {/* Map */}
        <div className="flex-[3] min-w-0 flex flex-col">
          <h3 className="text-base font-bold text-slate-900 mb-1.5">Network Map <span className="text-sm font-normal text-slate-400">— What's driving your locations' performance</span></h3>
          <div data-tour="network-map" className="flex-1 rounded-lg overflow-hidden border border-slate-200 relative">
          <NetworkMap
            locations={agentOnlyMap ? (() => {
              // Filter existing store dots to only those matching agent results
              const agentLabels = new Set(agentPoints.map(p => p.label));
              const agentNames = new Set(agentPoints.map(p => p.label));
              const matched = locations.filter(l => agentLabels.has(l.id) || agentNames.has(l.name));
              return matched.length > 0 ? matched : locations;
            })() : locations}
            competitors={agentOnlyMap ? (() => {
              const agentLabels = new Set(agentPoints.map(p => p.label));
              const matched = competitors.filter(c => agentLabels.has(c.id));
              return matched.length > 0 ? matched : [];
            })() : competitors}
            closureCandidates={agentOnlyMap ? [] : closureCandidates}
            isochrones={agentOnlyMap ? [] : isochrones}
            selectedLocationIds={agentOnlyMap ? new Set() : selectedLocationIds}
            onSelectLocations={handleSelectLocations}
            h3Data={agentOnlyMap ? null : h3Data}
            h3Metric={h3Metric}
            filter={filter}
            agentPoints={(() => {
              // Only show purple diamonds for points that DON'T match existing stores or competitors
              const storeNames = new Set(locations.map(l => l.name));
              const storeIds = new Set(locations.map(l => l.id));
              const compIds = new Set(competitors.map(c => c.id));
              return agentPoints.filter(p => !storeNames.has(p.label) && !storeIds.has(p.label) && !compIds.has(p.label));
            })()}
          />
          {/* Filter bar at bottom of map */}
          <div className="absolute bottom-10 left-1/2 -translate-x-1/2 z-[1000] bg-white/90 backdrop-blur border border-slate-200 rounded-lg px-1 py-0.5 shadow-sm flex items-center gap-0.5">
            {FILTER_OPTIONS.map((opt) => (
              <button
                key={opt.value}
                data-tour={`filter-${opt.value}`}
                onClick={() => setFilter(opt.value)}
                className={`px-2.5 py-1 text-[11px] rounded-md transition-colors ${
                  filter === opt.value
                    ? "bg-slate-800 text-white font-medium"
                    : "text-slate-500 hover:text-slate-800 hover:bg-slate-100"
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>
          {/* H3 metric selector — visible on map when single location selected */}
          {(h3Data || h3Loading) && (
            <div className="absolute top-3 right-14 z-[1000] bg-white/90 backdrop-blur border border-slate-200 rounded-lg px-3 py-2 shadow-sm">
              <p className="text-[10px] text-slate-400 mb-1.5 font-medium">Color hexagons by</p>
              <div className="flex flex-col gap-1">
                {H3_METRIC_OPTIONS.map((opt) => (
                  <button
                    key={opt.value}
                    onClick={() => setH3Metric(opt.value)}
                    className={`text-left px-2 py-1 text-[11px] rounded transition-colors ${
                      h3Metric === opt.value
                        ? "bg-pink-100 text-pink-800 font-medium"
                        : "text-slate-600 hover:bg-slate-100"
                    }`}
                  >
                    {opt.label}
                  </button>
                ))}
              </div>
              {h3Loading && <p className="text-[10px] text-slate-400 mt-1.5">Loading...</p>}
            </div>
          )}
          </div>
        </div>

        {/* Right Panel: Locations */}
        <div data-tour="panel-risk" className="w-[380px] flex flex-col min-w-0 min-h-0 shrink-0">
          <h3 className="text-base font-bold text-slate-900 mb-1.5 shrink-0">Locations <span className="text-sm font-normal text-slate-400">— Let's deep dive</span></h3>
          <div className="flex-1 min-h-0 flex flex-col bg-white border border-slate-200 rounded-lg shadow-sm overflow-hidden">
          {/* Panel tabs */}
          <div className="flex border-b border-slate-100 shrink-0">
            {([
              { key: "agent" as const, label: "Agent", icon: Sparkles, accent: true },
              { key: "top" as const, label: "Top 10", icon: TrendingUp, accent: false },
              { key: "risk" as const, label: `At Risk (${closureCandidates.length})`, icon: AlertTriangle, accent: false },
              { key: "search" as const, label: "Search", icon: MapPin, accent: false },
            ]).map(({ key, label, icon: Icon, accent }) => (
              <button
                key={key}
                onClick={() => { setPanelSection(key); if (key !== "agent") { setAgentOnlyMap(false); } }}
                className={`flex-1 flex items-center justify-center gap-1 px-2 py-2 text-[10px] font-medium transition-colors ${
                  panelSection === key
                    ? accent ? "text-violet-700 border-b-2 border-violet-500 bg-violet-50" : "text-slate-900 border-b-2 border-slate-800"
                    : accent ? "text-violet-400 hover:text-violet-600" : "text-slate-400 hover:text-slate-600"
                }`}
              >
                <Icon size={11} />
                {label}
              </button>
            ))}
          </div>

          {/* Panel content */}
          <div className="flex-1 min-h-0 overflow-y-auto">
            {/* Top 10 Locations */}
            {panelSection === "top" && (
              <div className="p-3 space-y-2">
                <p className="text-[10px] text-slate-400 uppercase tracking-wider font-medium mb-2">Highest Revenue</p>
                {top10.map((loc, i) => (
                  <button
                    key={loc.id}
                    onClick={() => handlePanelSelect(loc.id)}
                    className={`w-full text-left border rounded-lg px-3 py-2.5 transition-colors ${
                      selectedLocationIds.has(loc.id)
                        ? "border-blue-300 bg-blue-50"
                        : "border-slate-200 bg-white hover:border-slate-300"
                    }`}
                  >
                    <div className="flex items-center gap-2">
                      <span className="text-[10px] font-bold text-slate-300 w-4">#{i + 1}</span>
                      <div className="flex-1 min-w-0">
                        <p className="text-xs font-medium text-slate-900 truncate">{loc.name}</p>
                        <div className="flex items-center gap-2 mt-0.5">
                          <span className="text-[10px] text-slate-400 capitalize">{loc.format}</span>
                          <span className="text-[10px] text-slate-300">|</span>
                          <span className="text-[10px] font-medium text-emerald-600">${loc.monthly_sales}K/mo</span>
                        </div>
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            )}

            {/* At Risk Locations */}
            {panelSection === "risk" && (
              <div className="p-3 space-y-2">
                {closureCandidates.length === 0 ? (
                  <div className="text-center py-8 text-xs text-slate-400">No at-risk stores</div>
                ) : (
                  closureCandidates.map((cc) => {
                    const risk = riskLevel(cc.closure_risk);
                    const isExpanded = selectedRiskId === cc.id;
                    return (
                      <div
                        key={cc.id}
                        className={`border rounded-lg transition-colors cursor-pointer ${
                          isExpanded ? "border-slate-300 bg-slate-50" : "border-slate-200 bg-white hover:border-slate-300"
                        }`}
                        onClick={() => {
                          setSelectedRiskId(isExpanded ? null : cc.id);
                          handlePanelSelect(cc.id);
                        }}
                      >
                        <div className="px-3 py-2.5">
                          <div className="flex items-center justify-between mb-1.5">
                            <div className="min-w-0">
                              <p className="text-xs font-medium text-slate-900 truncate">{cc.name}</p>
                              <p className="text-[10px] text-slate-400">{cc.id}</p>
                            </div>
                            <div className="flex items-center gap-2 flex-shrink-0">
                              <span className={`text-[10px] px-2 py-0.5 rounded-full font-medium border ${risk.bg} ${risk.color}`}>
                                {risk.label}
                              </span>
                              <ChevronDown
                                size={12}
                                className={`text-slate-400 transition-transform ${isExpanded ? "rotate-180" : ""}`}
                              />
                            </div>
                          </div>
                          <div className="flex items-center gap-2 mb-1.5">
                            <div className="flex-1 h-1.5 bg-slate-100 rounded-full overflow-hidden">
                              <div
                                className={`h-full rounded-full ${risk.barColor}`}
                                style={{ width: `${Math.min(cc.closure_risk, 100)}%` }}
                              />
                            </div>
                            <span className="text-[10px] text-slate-400 w-8 text-right">{cc.closure_risk}%</span>
                          </div>
                          <p className="text-[11px] text-slate-500 leading-relaxed">{cc.reason}</p>
                        </div>
                        {isExpanded && cc.closure_metrics && cc.closure_metrics.length > 0 && (
                          <div className="px-3 pb-3 pt-1 border-t border-slate-100">
                            <p className="text-[10px] font-medium text-slate-400 uppercase tracking-wider mb-2">Key Indicators</p>
                            <div className="space-y-1.5">
                              {cc.closure_metrics.map((m) => (
                                <div key={m.label} className="flex items-center justify-between text-[11px]">
                                  <span className="text-slate-500">{m.label}</span>
                                  <div className="flex items-center gap-3">
                                    <span className="text-red-600 font-medium">{m.store_value}</span>
                                    <span className="text-slate-400">vs</span>
                                    <span className="text-slate-600">{m.network_avg}</span>
                                  </div>
                                </div>
                              ))}
                            </div>
                            <div className="mt-2 pt-2 border-t border-slate-100 flex items-center gap-3 text-[10px]">
                              <span className="text-slate-400">Format:</span>
                              <span className="text-slate-600 capitalize">{cc.format}</span>
                              <span className="text-slate-400">Area:</span>
                              <span className="text-slate-600 capitalize">{cc.urbanicity}</span>
                              <span className="text-slate-400">Sales:</span>
                              <span className="text-slate-600">${cc.monthly_sales}K/mo</span>
                            </div>
                          </div>
                        )}
                      </div>
                    );
                  })
                )}
              </div>
            )}

            {/* Location Search/Selector */}
            {panelSection === "search" && (
              <div className="p-3">
                <div className="relative mb-3">
                  <Search size={13} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-slate-400" />
                  <input
                    type="text"
                    value={storeSearch}
                    onChange={(e) => setStoreSearch(e.target.value)}
                    placeholder="Search by store # or name..."
                    className="w-full pl-8 pr-3 py-1.5 text-xs border border-slate-200 rounded-md focus:outline-none focus:ring-1 focus:ring-blue-400 focus:border-blue-400"
                  />
                </div>
                <div className="space-y-1">
                  {storeSearch ? (
                    searchedLocations.length === 0 ? (
                      <div className="text-center py-6 text-xs text-slate-400">No matching stores</div>
                    ) : (
                      searchedLocations.slice(0, 20).map((loc) => (
                        <button
                          key={loc.id}
                          onClick={() => {
                            handlePanelSelect(loc.id);
                            setStoreSearch("");
                          }}
                          className={`w-full text-left px-3 py-2 rounded-md transition-colors text-xs ${
                            selectedLocationIds.has(loc.id)
                              ? "bg-blue-50 text-blue-800"
                              : "hover:bg-slate-50 text-slate-700"
                          }`}
                        >
                          <div className="flex items-center justify-between">
                            <div className="min-w-0">
                              <p className="font-medium truncate">{loc.name}</p>
                              <p className="text-[10px] text-slate-400 mt-0.5">{loc.id} · {loc.format}</p>
                            </div>
                            <span className="text-[10px] text-slate-400 shrink-0 ml-2">${loc.monthly_sales}K</span>
                          </div>
                        </button>
                      ))
                    )
                  ) : (
                    <div className="text-center py-6 text-xs text-slate-400">
                      Type to search {locations.length} locations
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Agent Chat — always mounted, hidden when other tab active */}
            <div className={panelSection === "agent" ? "flex flex-col h-full" : "hidden"}>
                {agentPoints.length > 0 && (
                  <div className="flex items-center gap-2 px-3 py-1.5 border-b border-slate-100 shrink-0">
                    <label className="flex items-center gap-1.5 text-[10px] text-slate-500 cursor-pointer select-none">
                      <input
                        type="checkbox"
                        checked={agentOnlyMap}
                        onChange={(e) => setAgentOnlyMap(e.target.checked)}
                        className="rounded border-slate-300 text-violet-500 focus:ring-violet-400 w-3 h-3"
                      />
                      Agent results only
                    </label>
                    <button
                      onClick={() => {
                        // Just zoom out to NY state level — keep agent points visible
                        const map = document.querySelector('[data-tour="network-map"]');
                        if (map) {
                          // Dispatch a custom event the map listens for
                          window.dispatchEvent(new CustomEvent("map-zoom-out"));
                        }
                      }}
                      className="text-[10px] text-blue-500 hover:text-blue-700 font-medium ml-auto"
                    >
                      Zoom out
                    </button>
                  </div>
                )}
                <AgentChat
                  pageContext="network"
                  onMapPoints={(pts) => { setAgentPoints(pts); setAgentOnlyMap(true); }}
                  onH3Trigger={(storeId) => {
                    setSelectedLocationIds(new Set([storeId]));
                    setAgentOnlyMap(false);
                  }}
                  className="border-0 shadow-none rounded-none flex-1 min-h-0"
                />
            </div>
          </div>
          </div>
        </div>
      </div>
    </div>
  );
}
