import { createFileRoute } from "@tanstack/react-router";
import { useState, useEffect, useCallback } from "react";
import { api } from "@/lib/api";
import type { Location, Competitor, Hotspot, ClosureCandidate, Isochrone, NetworkMetrics, SimulationResult, SaveScenarioParams } from "@/lib/api";
import { PlaygroundMap } from "@/components/maps/playground-map";
import { MapLayerControl, type MapLayer } from "@/components/maps/map-layer-control";
import { TimelineControl } from "@/components/maps/timeline-control";
import { ScenarioPanel } from "@/components/shared/scenario-panel";
import { GuidedTour, type TourStep } from "@/components/shared/guided-tour";
import { MapPin, Plus, X, Save, Minus, TrendingUp, ChevronDown, ChevronRight, Trash2, Sparkles, SlidersHorizontal } from "lucide-react";
import { AgentChat } from "@/components/shared/agent-chat";
import type { AgentResponse } from "@/lib/api";

export const Route = createFileRoute("/_sidebar/site-playground")({
  component: SitePlaygroundPage,
});

/** Format a value in K (thousands) to B/M/K string */
function fmtK(valK: number): string {
  if (valK >= 1000000) return `$${(valK / 1000000).toFixed(1)}B`;
  if (valK >= 1000) return `$${(valK / 1000).toFixed(1)}M`;
  return `$${valK.toFixed(0)}K`;
}

interface SavedScenario {
  id: string;
  title: string;
  timestamp: string;
  params: {
    competitorYear: number;
    minDistanceFromNetwork: { urban: number; suburban: number; rural: number };
    minDistanceBetweenNew: { urban: number; suburban: number; rural: number };
    finalLocationsCount: number;
    excludedCount: number;
    removedCount: number;
    addedCount: number;
  };
  result: SimulationResult;
  collapsed: boolean;
}

function SitePlaygroundPage() {
  const [locations, setLocations] = useState<Location[]>([]);
  const [allCompetitors, setAllCompetitors] = useState<Competitor[]>([]);
  const [competitors, setCompetitors] = useState<Competitor[]>([]);
  const [hotspots, setHotspots] = useState<Hotspot[]>([]);
  const [closureCandidates, setClosureCandidates] = useState<ClosureCandidate[]>([]);
  const [isochrones, setIsochrones] = useState<Isochrone[]>([]);
  const [networkMetrics, setNetworkMetrics] = useState<NetworkMetrics | null>(null);
  const [scenarios, setScenarios] = useState<SavedScenario[]>([]);
  const [activeScenarioId, setActiveScenarioId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [simulating, setSimulating] = useState(false);
  const [savingScenarioId, setSavingScenarioId] = useState<string | null>(null);
  const [savedScenarioIds, setSavedScenarioIds] = useState<Set<string>>(new Set());

  const [currentYear, setCurrentYear] = useState(2025);
  const [userLocations, setUserLocations] = useState<Array<{ lat: number; lng: number; format: string; estimated_sales: number; nearest_store: string }>>([]);
  const [removedStoreIds, setRemovedStoreIds] = useState<Set<string>>(new Set());

  // Add location form
  const [latInput, setLatInput] = useState("");
  const [lngInput, setLngInput] = useState("");
  const [formatInput, setFormatInput] = useState("standard");

  // Remove location form
  const [removeIdInput, setRemoveIdInput] = useState("");

  // Active tab for add/remove
  const [actionTab, setActionTab] = useState<"add" | "remove">("add");

  // Panel mode: agent (chat) or manual (scenario sliders)
  const [panelMode, setPanelMode] = useState<"agent" | "manual">("agent");
  const [agentPoints, setAgentPoints] = useState<AgentResponse["map_points"]>([]);
  const [agentOnlyMap, setAgentOnlyMap] = useState(false);

  const [layers, setLayers] = useState<MapLayer[]>([
    { id: "express", label: "Express", enabled: true, color: "#3b82f6", size: "sm" },
    { id: "standard", label: "Standard", enabled: true, color: "#3b82f6", size: "md" },
    { id: "flagship", label: "Flagship", enabled: true, color: "#3b82f6", size: "lg" },
    { id: "tradeAreas", label: "Trade Areas", enabled: true },
    { id: "competitors", label: "Competitors", enabled: true, color: "#ef4444" },
    { id: "hotspots", label: "Hotspots", enabled: true, color: "#fbbf24" },
    { id: "closureRisks", label: "At Risk", enabled: true, color: "#f97316", icon: "!" },
    { id: "finalLocations", label: "Optimized", enabled: false, color: "#10b981" },
    { id: "userLocations", label: "User Added", enabled: false, color: "#22d3ee" },
  ]);

  useEffect(() => {
    Promise.all([
      api.listLocations(),
      api.listCompetitors(),
      api.listHotspots(),
      api.listClosureCandidates(),
      api.getNetworkMetrics(),
      api.listIsochrones().catch(() => [] as Isochrone[]),
    ]).then(([l, c, h, cc, nm, iso]) => {
      setLocations(l);
      setAllCompetitors(c);
      setCompetitors(c.filter((comp) => comp.open_year <= 2025));
      setHotspots(h);
      setClosureCandidates(cc);
      setNetworkMetrics(nm);
      setIsochrones(iso);
      setLoading(false);
    });
  }, []);

  useEffect(() => {
    setCompetitors(allCompetitors.filter((c) => c.open_year <= currentYear));
  }, [currentYear, allCompetitors]);

  const handleToggleLayer = (layerId: string) => {
    setLayers((prev) =>
      prev.map((l) => (l.id === layerId ? { ...l, enabled: !l.enabled } : l))
    );
  };

  const layerState = layers.reduce(
    (acc, l) => ({ ...acc, [l.id]: l.enabled }),
    {} as Record<string, boolean>
  );

  const handleSimulate = useCallback(async (params: { competitorYear: number; minDistanceFromNetwork: { urban: number; suburban: number; rural: number }; minDistanceBetweenNew: { urban: number; suburban: number; rural: number }; finalLocationsCount: number; excludedClosureRisks: string[] }) => {
    setSimulating(true);
    try {
      const result = await api.runSimulation({
        competitor_year: params.competitorYear,
        min_distance_from_network: params.minDistanceFromNetwork,
        min_distance_between_new: params.minDistanceBetweenNew,
        final_locations_count: params.finalLocationsCount,
        excluded_closure_risks: params.excludedClosureRisks,
        removed_store_ids: Array.from(removedStoreIds),
        added_locations: userLocations.map((ul) => ({ lat: ul.lat, lng: ul.lng, format: ul.format })),
      });

      const id = `S${Date.now()}`;
      const title = `${params.finalLocationsCount} locations · Net: ${params.minDistanceFromNetwork.urban}/${params.minDistanceFromNetwork.suburban}/${params.minDistanceFromNetwork.rural}mi · New: ${params.minDistanceBetweenNew.urban}/${params.minDistanceBetweenNew.suburban}/${params.minDistanceBetweenNew.rural}mi · Year ${params.competitorYear}`;

      const scenario: SavedScenario = {
        id,
        title,
        timestamp: new Date().toLocaleTimeString(),
        params: {
          competitorYear: params.competitorYear,
          minDistanceFromNetwork: params.minDistanceFromNetwork,
          minDistanceBetweenNew: params.minDistanceBetweenNew,
          finalLocationsCount: params.finalLocationsCount,
          excludedCount: params.excludedClosureRisks.length,
          removedCount: removedStoreIds.size,
          addedCount: userLocations.length,
        },
        result,
        collapsed: false,
      };

      // Collapse all previous scenarios, add new one
      setScenarios((prev) => [...prev.map((s) => ({ ...s, collapsed: true })), scenario]);
      setActiveScenarioId(id);
      setLayers((prev) => prev.map((l) => l.id === "finalLocations" ? { ...l, enabled: true } : l));
    } finally {
      setSimulating(false);
    }
  }, [removedStoreIds, userLocations]);

  const handleAddLocation = () => {
    const lat = Number(latInput);
    const lng = Number(lngInput);
    if (isNaN(lat) || isNaN(lng) || latInput === "" || lngInput === "") return;

    // Find nearest store and use its monthly_sales as estimate
    let nearest = locations[0];
    let minDist = Infinity;
    for (const loc of locations) {
      const d = Math.sqrt((loc.lat - lat) ** 2 + (loc.lng - lng) ** 2);
      if (d < minDist) { minDist = d; nearest = loc; }
    }

    setUserLocations((prev) => [...prev, {
      lat, lng, format: formatInput,
      estimated_sales: nearest?.monthly_sales || 0,
      nearest_store: nearest?.name || "Unknown",
    }]);
    setLatInput("");
    setLngInput("");
    // Auto-enable user locations layer
    setLayers((prev) => prev.map((l) => l.id === "userLocations" ? { ...l, enabled: true } : l));
  };

  const handleRemoveUserLocation = (index: number) => {
    setUserLocations((prev) => prev.filter((_, i) => i !== index));
  };

  const handleRemoveStore = () => {
    const id = removeIdInput.trim().toUpperCase();
    if (!id) return;
    // Check if it's a valid store
    const store = locations.find((l) => l.id === id || l.id === `LOC${id}` || l.name.toLowerCase().includes(id.toLowerCase()));
    if (store) {
      setRemovedStoreIds((prev) => new Set(prev).add(store.id));
      setRemoveIdInput("");
    }
  };

  const handleRestoreStore = (id: string) => {
    setRemovedStoreIds((prev) => {
      const next = new Set(prev);
      next.delete(id);
      return next;
    });
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") handleAddLocation();
  };

  const handleRemoveKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") handleRemoveStore();
  };

  const isValidInput = latInput !== "" && lngInput !== "" && !isNaN(Number(latInput)) && !isNaN(Number(lngInput));

  // Tour state
  const [showTour, setShowTour] = useState(false);
  useEffect(() => {
    if (!loading && locations.length > 0) {
      const seen = localStorage.getItem("tour-playground-seen");
      if (!seen) setShowTour(true);
    }
  }, [loading, locations.length]);

  useEffect(() => {
    const handler = () => setShowTour(true);
    window.addEventListener("start-tour", handler);
    return () => window.removeEventListener("start-tour", handler);
  }, []);

  const playgroundTourSteps: TourStep[] = [
    {
      target: "pg-metrics",
      title: "Performance & Opportunity Metrics",
      description: "Track current network revenue against total whitespace potential — quantifying the untapped revenue opportunity across all candidate expansion sites.",
      position: "bottom",
    },
    {
      target: "pg-map",
      title: "Strategic Expansion Map",
      description: "Visualize your network (blue), competitors (red), high-potential expansion hotspots (yellow), and at-risk locations (orange) in a single unified view. Toggle layers to isolate specific dimensions.",
      position: "right",
    },
    {
      target: "pg-timeline",
      title: "Competitor Growth Over Time",
      description: "Project competitive landscape evolution from 2025 through 2030. Adjust the timeline to assess how competitor density shifts across your target markets.",
      position: "top",
    },
    {
      target: "pg-scenario",
      title: "Scenario Configuration",
      description: "Define distance constraints by urbanicity tier (urban, suburban, rural), set the target location count, and execute the greedy optimizer to generate a revenue-maximizing expansion plan.",
      position: "left",
    },
    {
      target: "pg-actions",
      title: "Network Modifications",
      description: "Add candidate locations by coordinates or remove existing stores to model what-if scenarios. Changes feed directly into the optimizer for real-time impact analysis.",
      position: "top",
    },
  ];

  const handleTourComplete = useCallback(() => {
    setShowTour(false);
    localStorage.setItem("tour-playground-seen", "true");
  }, []);

  // Filter out removed stores
  const activeLocations = locations.filter((l) => !removedStoreIds.has(l.id));

  const timelineEvents = allCompetitors
    .filter((c) => c.open_year >= 2018 && c.open_year <= 2030)
    .reduce<Array<{ year: number; type: "historic" | "projected" }>>((acc, c) => {
      const existing = acc.find((e) => e.year === c.open_year);
      if (!existing) {
        acc.push({ year: c.open_year, type: c.is_projected ? "projected" : "historic" });
      }
      return acc;
    }, []);

  if (loading) {
    return (
      <div className="h-full flex items-center justify-center">
        <div className="animate-spin w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full" />
      </div>
    );
  }

  const handleRemoveOptimizedLocation = (scenarioId: string, locId: string) => {
    setScenarios((prev) => prev.map((s) => {
      if (s.id !== scenarioId) return s;
      const updated = s.result.optimized_locations.filter((ol) => ol.id !== locId);
      const totalRev = updated.reduce((sum, ol) => sum + ol.projected_revenue, 0);
      const scaledChange = s.result.total_projected_revenue > 0
        ? Number((s.result.network_revenue_change * totalRev / s.result.total_projected_revenue).toFixed(1))
        : 0;
      return {
        ...s,
        result: { ...s.result, optimized_locations: updated, total_projected_revenue: totalRev, network_revenue_change: scaledChange },
      };
    }));
  };

  const handleDeleteScenario = (scenarioId: string) => {
    setScenarios((prev) => prev.filter((s) => s.id !== scenarioId));
    if (activeScenarioId === scenarioId) {
      setActiveScenarioId(null);
    }
  };

  const handleToggleScenarioCollapse = (scenarioId: string) => {
    setScenarios((prev) => prev.map((s) => s.id === scenarioId ? { ...s, collapsed: !s.collapsed } : s));
  };

  const handleSetActiveScenario = (scenarioId: string) => {
    setActiveScenarioId(scenarioId);
    // Expand the active one, collapse others
    setScenarios((prev) => prev.map((s) => ({ ...s, collapsed: s.id !== scenarioId })));
    setLayers((prev) => prev.map((l) => l.id === "finalLocations" ? { ...l, enabled: true } : l));
  };

  const handleSaveScenarioToCatalog = async (scenario: SavedScenario) => {
    setSavingScenarioId(scenario.id);
    try {
      const params: SaveScenarioParams = {
        scenario_id: scenario.id,
        scenario_summary: scenario.title,
        competitor_year: scenario.params.competitorYear,
        min_distance_from_network_urban: scenario.params.minDistanceFromNetwork.urban,
        min_distance_from_network_suburban: scenario.params.minDistanceFromNetwork.suburban,
        min_distance_from_network_rural: scenario.params.minDistanceFromNetwork.rural,
        min_distance_between_new_urban: scenario.params.minDistanceBetweenNew.urban,
        min_distance_between_new_suburban: scenario.params.minDistanceBetweenNew.suburban,
        min_distance_between_new_rural: scenario.params.minDistanceBetweenNew.rural,
        final_locations_count: scenario.params.finalLocationsCount,
        excluded_at_risk_count: scenario.params.excludedCount,
        removed_store_count: scenario.params.removedCount,
        added_location_count: scenario.params.addedCount,
        total_projected_revenue: scenario.result.total_projected_revenue,
        network_revenue_change: scenario.result.network_revenue_change,
        cannibalization_rate: scenario.result.cannibalization_rate,
        avg_site_score: scenario.result.avg_site_score,
        optimized_locations: scenario.result.optimized_locations,
      };
      const res = await api.saveScenario(params);
      if (res.success) {
        setSavedScenarioIds((prev) => new Set(prev).add(scenario.id));
      }
    } catch {
      // silent fail — button stays enabled for retry
    } finally {
      setSavingScenarioId(null);
    }
  };

  // Active scenario for map display and metric cards
  const activeScenario = scenarios.find((s) => s.id === activeScenarioId) || null;

  return (
    <div className="flex flex-col max-w-[1920px] mx-auto w-full overflow-y-auto h-full">
      {/* Tour */}
      <GuidedTour
        steps={playgroundTourSteps}
        isOpen={showTour}
        onComplete={handleTourComplete}
        introTitle="Site Playground"
        introDescription="Translate network performance insights into actionable expansion strategy. Evaluate whitespace opportunities across your geographic footprint, selecting locations with the highest revenue potential. Model competitive growth trajectories through 2030, optimize for population coverage and revenue uplift, and build scenario-driven expansion plans tailored to your strategic priorities."
      />

      {/* Metric Cards */}
      <div data-tour="pg-metrics" className="grid grid-cols-5 gap-3 px-6 pt-5 pb-3 shrink-0">
        <MetricCard label="Current Revenue" value={networkMetrics?.total_current_revenue || "$0"} />
        <MetricCard
          label="Whitespace Potential"
          value={networkMetrics?.projected_optimized_revenue || "$0"}
          subtext={networkMetrics ? `${networkMetrics.whitespace_locations} candidate locations` : undefined}
          accent
        />
        <MetricCard label="Projected Optimized" value={activeScenario ? fmtK(activeScenario.result.total_projected_revenue) : "—"} />
        <MetricCard label="Revenue Uplift" value={activeScenario ? `+${activeScenario.result.network_revenue_change}%` : "—"} />
        <MetricCard label="New Locations" value={activeScenario ? String(activeScenario.result.optimized_locations.length) : "0"} />
      </div>

      {/* Map + Scenario Panel side by side — fixed height, never shrinks */}
      <div className="flex gap-3 mx-6 mb-2 shrink-0" style={{ height: "calc(100vh - 200px)", minHeight: "500px" }}>
        {/* Map */}
        <div data-tour="pg-map" className="flex-1 relative rounded-lg overflow-hidden border border-slate-200 min-w-0">
          <PlaygroundMap
            locations={agentOnlyMap ? [] : activeLocations}
            competitors={agentOnlyMap ? [] : competitors}
            hotspots={agentOnlyMap ? (() => {
              if (agentPoints.length === 0) return [];
              // Match by lat/lng at 3 decimal places (~111m tolerance)
              const agentCoords = new Set(agentPoints.map(p => `${p.lat.toFixed(3)},${p.lng.toFixed(3)}`));
              const matched = hotspots.filter(h => agentCoords.has(`${h.lat.toFixed(3)},${h.lng.toFixed(3)}`));
              return matched.length > 0 ? matched : [];
            })() : hotspots}
            closureCandidates={agentOnlyMap ? [] : closureCandidates}
            isochrones={agentOnlyMap ? [] : isochrones}
            layers={agentOnlyMap ? { ...layerState, hotspots: true, competitors: false, closureRisks: false, tradeAreas: false, express: false, standard: false, flagship: false } : layerState}
            userLocations={agentOnlyMap ? [] : userLocations}
            optimizedLocations={agentOnlyMap ? [] : (activeScenario?.result.optimized_locations || [])}
            agentPoints={[]}
          />


          {/* Map Layer Control */}
          <MapLayerControl layers={layers} onToggle={handleToggleLayer} />

          {/* Timeline */}
          <TimelineControl
            minYear={2021}
            maxYear={2030}
            currentYear={currentYear}
            onYearChange={setCurrentYear}
            events={timelineEvents}
          />
        </div>

        {/* Right Panel — Manual or Agent */}
        <div data-tour="pg-scenario" className="w-[380px] shrink-0 flex flex-col min-h-0">
          {/* Mode toggle */}
          <div className="flex items-center gap-0.5 mb-2 bg-slate-100 rounded-lg p-0.5 shrink-0">
            <button
              onClick={() => setPanelMode("agent")}
              className={`flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 text-[11px] font-medium rounded-md transition-colors ${
                panelMode === "agent" ? "bg-violet-100 text-violet-700 shadow-sm" : "text-slate-400 hover:text-slate-600"
              }`}
            >
              <Sparkles size={12} />
              Site Agent
            </button>
            <button
              onClick={() => { setPanelMode("manual"); setAgentOnlyMap(false); setAgentPoints([]); }}
              className={`flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 text-[11px] font-medium rounded-md transition-colors ${
                panelMode === "manual" ? "bg-white text-slate-800 shadow-sm" : "text-slate-400 hover:text-slate-600"
              }`}
            >
              <SlidersHorizontal size={12} />
              Manual
            </button>
          </div>

          {/* Manual panel — hidden when agent active */}
          <div className={panelMode === "manual" ? "flex-1 overflow-y-auto min-h-0" : "hidden"}>
            <ScenarioPanel
              onSimulate={handleSimulate}
              isLoading={simulating}
              closureCandidates={closureCandidates.map((cc) => ({ id: cc.id, name: cc.name, risk: cc.closure_risk }))}
            />
          </div>

          {/* Agent panel — always mounted, hidden when manual active */}
          <div className={panelMode === "agent" ? "flex-1 min-h-0 flex flex-col" : "hidden"}>
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
                  onClick={() => window.dispatchEvent(new CustomEvent("map-zoom-out"))}
                  className="text-[10px] text-blue-500 hover:text-blue-700 font-medium ml-auto"
                >
                  Zoom out
                </button>
              </div>
            )}
            <AgentChat
              pageContext="expansion"
              onMapPoints={(pts) => { setAgentPoints(pts); setAgentOnlyMap(true); }}
              className="flex-1 min-h-0 border-0 shadow-none rounded-none"
            />
          </div>
        </div>
      </div>

      {/* Add/Remove Locations — below map, scrollable area */}
      <div data-tour="pg-actions" className="border-t border-slate-200 bg-white px-6 py-3 shrink-0 overflow-y-auto">
        <div className="flex items-start gap-6">
          {/* Action tabs + form */}
          <div className="shrink-0">
            <div className="flex items-center gap-1 mb-2">
              <button
                onClick={() => setActionTab("add")}
                className={`flex items-center gap-1 px-2.5 py-1 text-[11px] rounded-md transition-colors ${
                  actionTab === "add" ? "bg-blue-100 text-blue-700 font-medium" : "text-slate-400 hover:text-slate-600"
                }`}
              >
                <Plus size={11} />
                Add Location
              </button>
              <button
                onClick={() => setActionTab("remove")}
                className={`flex items-center gap-1 px-2.5 py-1 text-[11px] rounded-md transition-colors ${
                  actionTab === "remove" ? "bg-red-100 text-red-700 font-medium" : "text-slate-400 hover:text-slate-600"
                }`}
              >
                <Minus size={11} />
                Remove Location
              </button>
            </div>

            {actionTab === "add" ? (
              <div className="flex items-center gap-2">
                <MapPin className="h-4 w-4 text-slate-400 shrink-0" />
                <input
                  type="text"
                  placeholder="Latitude"
                  value={latInput}
                  onChange={(e) => setLatInput(e.target.value)}
                  onKeyDown={handleKeyDown}
                  className="w-28 rounded-md border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs text-slate-900 placeholder-slate-400 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                />
                <input
                  type="text"
                  placeholder="Longitude"
                  value={lngInput}
                  onChange={(e) => setLngInput(e.target.value)}
                  onKeyDown={handleKeyDown}
                  className="w-28 rounded-md border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs text-slate-900 placeholder-slate-400 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                />
                <select
                  value={formatInput}
                  onChange={(e) => setFormatInput(e.target.value)}
                  className="rounded-md border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs text-slate-900 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                >
                  <option value="express">Express</option>
                  <option value="standard">Standard</option>
                  <option value="flagship">Flagship</option>
                </select>
                <button
                  onClick={handleAddLocation}
                  disabled={!isValidInput}
                  className="flex items-center gap-1 rounded-md bg-blue-500 px-3 py-1.5 text-xs font-medium text-white transition-colors hover:bg-blue-600 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  <Plus size={12} />
                  Add
                </button>
              </div>
            ) : (
              <div className="flex items-center gap-2">
                <Minus className="h-4 w-4 text-slate-400 shrink-0" />
                <input
                  type="text"
                  placeholder="Store ID (e.g. LOC1042)"
                  value={removeIdInput}
                  onChange={(e) => setRemoveIdInput(e.target.value)}
                  onKeyDown={handleRemoveKeyDown}
                  className="w-52 rounded-md border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs text-slate-900 placeholder-slate-400 focus:border-red-500 focus:outline-none focus:ring-1 focus:ring-red-500"
                />
                <button
                  onClick={handleRemoveStore}
                  disabled={!removeIdInput.trim()}
                  className="flex items-center gap-1 rounded-md bg-red-500 px-3 py-1.5 text-xs font-medium text-white transition-colors hover:bg-red-600 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  <X size={12} />
                  Remove
                </button>
              </div>
            )}
          </div>

          {/* Changes list */}
          {(userLocations.length > 0 || removedStoreIds.size > 0) && (
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 mb-1.5">
                <p className="text-[10px] text-slate-400 uppercase tracking-wider font-medium">
                  Changes ({userLocations.length + removedStoreIds.size})
                </p>
                <button
                  onClick={() => alert("Save functionality coming soon")}
                  className="flex items-center gap-0.5 text-[10px] text-blue-600 hover:text-blue-800 font-medium"
                >
                  <Save size={9} />
                  Save
                </button>
              </div>
              <div className="flex flex-wrap gap-1.5">
                {/* Added locations */}
                {userLocations.map((loc, i) => (
                  <span
                    key={`add-${i}`}
                    className="inline-flex items-center gap-1.5 bg-cyan-50 border border-cyan-200 rounded-md px-2 py-1 text-[11px] text-cyan-800"
                    title={`Based on nearest store: ${loc.nearest_store}`}
                  >
                    <Plus size={9} className="text-cyan-500" />
                    {loc.lat.toFixed(3)}, {loc.lng.toFixed(3)}
                    <span className="text-cyan-500 capitalize">({loc.format})</span>
                    <span className="text-cyan-600 font-medium">~${loc.estimated_sales >= 1000 ? `${(loc.estimated_sales / 1000).toFixed(1)}M` : `${loc.estimated_sales.toFixed(0)}K`}/mo</span>
                    <button
                      onClick={() => handleRemoveUserLocation(i)}
                      className="ml-0.5 p-0.5 rounded hover:bg-cyan-100 text-cyan-400 hover:text-red-500 transition-colors"
                    >
                      <X size={10} />
                    </button>
                  </span>
                ))}
                {/* Removed stores */}
                {Array.from(removedStoreIds).map((id) => {
                  const store = locations.find((l) => l.id === id);
                  return (
                    <span
                      key={`rm-${id}`}
                      className="inline-flex items-center gap-1.5 bg-red-50 border border-red-200 rounded-md px-2 py-1 text-[11px] text-red-800"
                    >
                      <Minus size={9} className="text-red-500" />
                      {store?.name || id}
                      <button
                        onClick={() => handleRestoreStore(id)}
                        className="ml-0.5 p-0.5 rounded hover:bg-red-100 text-red-400 hover:text-blue-500 transition-colors"
                        title="Restore"
                      >
                        <X size={10} />
                      </button>
                    </span>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Scenario Results */}
      {scenarios.length > 0 && (
        <div className="border-t border-slate-200 bg-white px-6 py-4 shrink-0 space-y-3">
          <div className="flex items-center gap-2">
            <TrendingUp size={14} className="text-emerald-600" />
            <h3 className="text-sm font-semibold text-slate-900">Scenarios ({scenarios.length})</h3>
          </div>

          {scenarios.map((scenario, idx) => {
            const isActive = scenario.id === activeScenarioId;
            return (
              <div
                key={scenario.id}
                className={`border rounded-lg overflow-hidden ${isActive ? "border-emerald-300 bg-emerald-50/30" : "border-slate-200 bg-white"}`}
              >
                {/* Scenario header */}
                <div
                  className={`flex items-center gap-2 px-4 py-2.5 cursor-pointer hover:bg-slate-50 ${isActive ? "bg-emerald-50" : ""}`}
                  onClick={() => handleToggleScenarioCollapse(scenario.id)}
                >
                  {scenario.collapsed ? <ChevronRight size={14} className="text-slate-400 shrink-0" /> : <ChevronDown size={14} className="text-slate-400 shrink-0" />}
                  <span className="text-[11px] font-semibold text-slate-700">Scenario {idx + 1}</span>
                  <span className="text-[10px] text-slate-400">{scenario.timestamp}</span>
                  <span className="text-[10px] text-emerald-600 font-medium">
                    {scenario.result.optimized_locations.length} locations · {fmtK(scenario.result.total_projected_revenue)}/yr · +{scenario.result.network_revenue_change}%
                  </span>
                  <div className="ml-auto flex items-center gap-1">
                    {!isActive && (
                      <button
                        onClick={(e) => { e.stopPropagation(); handleSetActiveScenario(scenario.id); }}
                        className="text-[10px] text-blue-500 hover:text-blue-700 font-medium px-2 py-0.5 rounded hover:bg-blue-50"
                      >
                        Show on Map
                      </button>
                    )}
                    {isActive && <span className="text-[10px] text-emerald-600 font-medium px-2">Active</span>}
                    <button
                      onClick={(e) => { e.stopPropagation(); handleSaveScenarioToCatalog(scenario); }}
                      disabled={savingScenarioId === scenario.id || savedScenarioIds.has(scenario.id)}
                      className={`p-1 rounded transition-colors ${savedScenarioIds.has(scenario.id) ? "text-emerald-500 cursor-default" : "text-slate-400 hover:bg-slate-100 hover:text-blue-500"} disabled:opacity-50`}
                      title={savedScenarioIds.has(scenario.id) ? "Saved" : "Save to catalog"}
                    >
                      <Save size={11} />
                    </button>
                    <button
                      onClick={(e) => { e.stopPropagation(); handleDeleteScenario(scenario.id); }}
                      className="p-1 rounded hover:bg-red-50 text-slate-400 hover:text-red-500 transition-colors"
                      title="Delete scenario"
                    >
                      <Trash2 size={11} />
                    </button>
                  </div>
                </div>

                {/* Scenario details */}
                {!scenario.collapsed && (
                  <div className="px-4 pb-3">
                    {/* Constraint summary */}
                    <div className="flex flex-wrap gap-2 mb-3 text-[10px] text-slate-500">
                      <span className="bg-slate-100 rounded px-1.5 py-0.5">Year: {scenario.params.competitorYear}</span>
                      <span className="bg-slate-100 rounded px-1.5 py-0.5">Net dist: {scenario.params.minDistanceFromNetwork.urban}/{scenario.params.minDistanceFromNetwork.suburban}/{scenario.params.minDistanceFromNetwork.rural}mi (U/S/R)</span>
                      <span className="bg-slate-100 rounded px-1.5 py-0.5">New dist: {scenario.params.minDistanceBetweenNew.urban}/{scenario.params.minDistanceBetweenNew.suburban}/{scenario.params.minDistanceBetweenNew.rural}mi (U/S/R)</span>
                      {scenario.params.excludedCount > 0 && <span className="bg-orange-100 text-orange-700 rounded px-1.5 py-0.5">{scenario.params.excludedCount} at-risk excluded</span>}
                      {scenario.params.removedCount > 0 && <span className="bg-red-100 text-red-700 rounded px-1.5 py-0.5">{scenario.params.removedCount} stores removed</span>}
                      {scenario.params.addedCount > 0 && <span className="bg-cyan-100 text-cyan-700 rounded px-1.5 py-0.5">{scenario.params.addedCount} locations added</span>}
                    </div>

                    {/* Locations table */}
                    <div className="overflow-x-auto">
                      <table className="w-full text-[11px]">
                        <thead>
                          <tr className="border-b border-slate-100 text-left text-slate-400 uppercase tracking-wider">
                            <th className="pb-2 pr-4 font-medium">#</th>
                            <th className="pb-2 pr-4 font-medium">ID</th>
                            <th className="pb-2 pr-4 font-medium">Lat</th>
                            <th className="pb-2 pr-4 font-medium">Lng</th>
                            <th className="pb-2 pr-4 font-medium">Format</th>
                            <th className="pb-2 pr-4 font-medium text-right">Monthly Rev</th>
                            <th className="pb-2 pr-4 font-medium text-right">Annual Rev</th>
                            <th className="pb-2 font-medium text-center">Action</th>
                          </tr>
                        </thead>
                        <tbody>
                          {scenario.result.optimized_locations.map((ol, i) => (
                            <tr key={ol.id} className="border-b border-slate-50 hover:bg-slate-50">
                              <td className="py-1.5 pr-4 text-slate-400">{i + 1}</td>
                              <td className="py-1.5 pr-4 font-medium text-slate-700">{ol.id}</td>
                              <td className="py-1.5 pr-4 text-slate-600">{ol.lat.toFixed(4)}</td>
                              <td className="py-1.5 pr-4 text-slate-600">{ol.lng.toFixed(4)}</td>
                              <td className="py-1.5 pr-4"><span className="capitalize text-slate-700">{ol.format}</span></td>
                              <td className="py-1.5 pr-4 text-right font-medium text-slate-900">{fmtK(ol.projected_revenue / 12)}</td>
                              <td className="py-1.5 pr-4 text-right font-medium text-emerald-700">{fmtK(ol.projected_revenue)}</td>
                              <td className="py-1.5 text-center">
                                <button
                                  onClick={() => handleRemoveOptimizedLocation(scenario.id, ol.id)}
                                  className="p-1 rounded hover:bg-red-50 text-slate-400 hover:text-red-500 transition-colors"
                                  title="Remove location"
                                >
                                  <X size={12} />
                                </button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function MetricCard({ label, value, subtext, accent }: { label: string; value: string; subtext?: string; accent?: boolean }) {
  return (
    <div className="bg-white border border-slate-200 rounded-lg px-4 py-3.5 shadow-sm">
      <p className="text-[11px] font-semibold text-slate-500 mb-1 truncate uppercase tracking-wide">{label}</p>
      <p className={`text-2xl font-bold tracking-tight ${accent ? "text-emerald-600" : "text-slate-900"}`}>{value}</p>
      {subtext && <p className="text-[10px] text-slate-400 mt-1">{subtext}</p>}
    </div>
  );
}
