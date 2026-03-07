import { createFileRoute } from "@tanstack/react-router";
import { useState, useEffect, useCallback, useRef } from "react";
import {
  Layers,
  Eye,
  EyeOff,
  Loader2,
  CheckCircle2,
  XCircle,
  ChevronDown,
  ChevronRight,
  X,
} from "lucide-react";
import {
  MapContainer,
  TileLayer,
  GeoJSON,
  CircleMarker,
  Popup,
  useMap,
} from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { api, type ValidationLayerInfo, type ValidationGeoJSON } from "@/lib/api";

export const Route = createFileRoute("/_sidebar/data-validation")({
  component: DataValidationPage,
});

// NY State center
const NY_CENTER: [number, number] = [42.9, -75.5];
const NY_ZOOM = 7;

// Layer color palette
const LAYER_COLORS: Record<string, string> = {
  store_locations: "#3b82f6",
  competitor_locations: "#ef4444",
  osm_pois: "#8b5cf6",
  expansion_candidates: "#10b981",
  seed_points: "#f59e0b",
  h3_features: "#06b6d4",
  store_isochrones: "#3b82f6",
  competitor_isochrones: "#ef4444",
  census_blockgroups: "#6b7280",
  census_demographics: "#ec4899",
  census_zcta: "#14b8a6",
  state_boundary: "#1e293b",
};

const LAYER_FILL_OPACITY: Record<string, number> = {
  store_isochrones: 0.15,
  competitor_isochrones: 0.12,
  census_blockgroups: 0.05,
  census_zcta: 0.3,
  state_boundary: 0.02,
};

interface LayerState {
  visible: boolean;
  loading: boolean;
  data: ValidationGeoJSON | null;
  opacity: number;
}

function DataValidationPage() {
  const [layers, setLayers] = useState<ValidationLayerInfo[]>([]);
  const [layerStates, setLayerStates] = useState<Record<string, LayerState>>({});
  const [loadingMeta, setLoadingMeta] = useState(true);
  const [inspectedFeature, setInspectedFeature] = useState<Record<string, unknown> | null>(null);
  const [panelCollapsed, setPanelCollapsed] = useState(false);

  // Fetch layer metadata on mount
  useEffect(() => {
    api.listValidationLayers().then((data) => {
      setLayers(data);
      const states: Record<string, LayerState> = {};
      for (const layer of data) {
        states[layer.name] = { visible: false, loading: false, data: null, opacity: 0.8 };
      }
      setLayerStates(states);
      setLoadingMeta(false);
    }).catch(() => setLoadingMeta(false));
  }, []);

  const toggleLayer = useCallback(async (name: string) => {
    setLayerStates((prev) => {
      const current = prev[name];
      if (!current) return prev;

      // If turning off, just hide
      if (current.visible) {
        return { ...prev, [name]: { ...current, visible: false } };
      }

      // If data already loaded, just show
      if (current.data) {
        return { ...prev, [name]: { ...current, visible: true } };
      }

      // Need to fetch — mark loading
      return { ...prev, [name]: { ...current, loading: true, visible: true } };
    });

    // Fetch if needed
    const current = layerStates[name];
    if (!current?.data) {
      try {
        const data = await api.getValidationLayer(name);
        setLayerStates((prev) => ({
          ...prev,
          [name]: { ...prev[name], data, loading: false },
        }));
      } catch {
        setLayerStates((prev) => ({
          ...prev,
          [name]: { ...prev[name], loading: false, visible: false },
        }));
      }
    }
  }, [layerStates]);

  const setOpacity = useCallback((name: string, opacity: number) => {
    setLayerStates((prev) => ({
      ...prev,
      [name]: { ...prev[name], opacity },
    }));
  }, []);

  return (
    <div className="flex h-[calc(100vh-3.5rem)] relative">
      {/* Layer Control Panel */}
      <div
        className={`bg-white border-r border-slate-200 flex flex-col shrink-0 transition-all ${
          panelCollapsed ? "w-10" : "w-80"
        }`}
      >
        {panelCollapsed ? (
          <button
            onClick={() => setPanelCollapsed(false)}
            className="p-2 hover:bg-slate-100 h-full flex items-start pt-3"
          >
            <ChevronRight size={16} className="text-slate-500" />
          </button>
        ) : (
          <>
            <div className="px-4 py-3 border-b border-slate-200 flex items-center justify-between">
              <div>
                <h2 className="text-sm font-semibold text-slate-900 flex items-center gap-2">
                  <Layers size={15} /> Data Layers
                </h2>
                <p className="text-[11px] text-slate-500 mt-0.5">
                  Toggle layers to validate pipeline output
                </p>
              </div>
              <button onClick={() => setPanelCollapsed(true)} className="p-1 hover:bg-slate-100 rounded">
                <ChevronDown size={14} className="text-slate-400 rotate-90" />
              </button>
            </div>
            <div className="flex-1 overflow-y-auto">
              {loadingMeta ? (
                <div className="p-4 flex items-center gap-2 text-sm text-slate-500">
                  <Loader2 size={14} className="animate-spin" /> Loading layers...
                </div>
              ) : (
                layers.map((layer) => (
                  <LayerRow
                    key={layer.name}
                    layer={layer}
                    state={layerStates[layer.name]}
                    onToggle={() => toggleLayer(layer.name)}
                    onOpacityChange={(v) => setOpacity(layer.name, v)}
                    color={LAYER_COLORS[layer.name] || "#6b7280"}
                  />
                ))
              )}
            </div>
          </>
        )}
      </div>

      {/* Map */}
      <div className="flex-1 relative">
        <MapContainer
          center={NY_CENTER}
          zoom={NY_ZOOM}
          className="h-full w-full"
          zoomControl={true}
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
            url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
          />
          <CoordinateTooltip />

          {/* Render active layers */}
          {layers.map((layer) => {
            const state = layerStates[layer.name];
            if (!state?.visible || !state.data || state.data.features.length === 0)
              return null;

            return (
              <ValidationLayer
                key={layer.name}
                layerName={layer.name}
                data={state.data}
                color={LAYER_COLORS[layer.name] || "#6b7280"}
                opacity={state.opacity}
                fillOpacity={LAYER_FILL_OPACITY[layer.name]}
                onFeatureClick={(props) => setInspectedFeature(props)}
              />
            );
          })}
        </MapContainer>

        {/* Coordinate display */}
        <div
          id="coord-display"
          className="absolute bottom-2 left-2 bg-white/90 backdrop-blur-sm text-[11px] font-mono text-slate-600 px-2 py-1 rounded shadow-sm z-[1000] pointer-events-none"
        />

        {/* Feature Inspector */}
        {inspectedFeature && (
          <div className="absolute top-3 right-3 w-80 max-h-[70vh] bg-white rounded-lg shadow-lg border border-slate-200 z-[1000] overflow-hidden">
            <div className="flex items-center justify-between px-3 py-2 border-b border-slate-200 bg-slate-50">
              <span className="text-xs font-semibold text-slate-700">Feature Inspector</span>
              <button
                onClick={() => setInspectedFeature(null)}
                className="p-0.5 hover:bg-slate-200 rounded"
              >
                <X size={14} className="text-slate-500" />
              </button>
            </div>
            <div className="overflow-y-auto max-h-[60vh] p-3">
              <table className="w-full text-xs">
                <tbody>
                  {Object.entries(inspectedFeature).map(([key, value]) => (
                    <tr key={key} className="border-b border-slate-100 last:border-0">
                      <td className="py-1.5 pr-2 text-slate-500 font-medium align-top whitespace-nowrap">
                        {key}
                      </td>
                      <td className="py-1.5 text-slate-800 break-all">
                        {typeof value === "object" ? JSON.stringify(value) : String(value ?? "null")}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// --- Sub-components ---

function LayerRow({
  layer,
  state,
  onToggle,
  onOpacityChange,
  color,
}: {
  layer: ValidationLayerInfo;
  state: LayerState | undefined;
  onToggle: () => void;
  onOpacityChange: (v: number) => void;
  color: string;
}) {
  const isVisible = state?.visible ?? false;
  const isLoading = state?.loading ?? false;
  const opacity = state?.opacity ?? 0.8;

  return (
    <div className={`border-b border-slate-100 ${isVisible ? "bg-blue-50/30" : ""}`}>
      <div className="flex items-center gap-2 px-3 py-2">
        {/* Color swatch */}
        <div
          className="w-3 h-3 rounded-sm shrink-0"
          style={{ backgroundColor: color, opacity: isVisible ? 1 : 0.3 }}
        />

        {/* Toggle button */}
        <button
          onClick={onToggle}
          disabled={!layer.available}
          className={`flex-1 flex items-center gap-2 text-left ${
            !layer.available ? "opacity-40 cursor-not-allowed" : "cursor-pointer"
          }`}
        >
          <div className="flex-1 min-w-0">
            <p className="text-xs font-medium text-slate-800 truncate">{layer.display_name}</p>
            <p className="text-[10px] text-slate-500">
              {layer.row_count.toLocaleString()} rows
              <span className="mx-1">·</span>
              {layer.geometry_type}
            </p>
          </div>
        </button>

        {/* Status indicator */}
        <div className="shrink-0">
          {isLoading ? (
            <Loader2 size={14} className="animate-spin text-blue-500" />
          ) : !layer.available ? (
            <XCircle size={14} className="text-red-400" />
          ) : isVisible ? (
            <Eye
              size={14}
              className="text-blue-600 cursor-pointer hover:text-blue-800"
              onClick={onToggle}
            />
          ) : (
            <EyeOff
              size={14}
              className="text-slate-400 cursor-pointer hover:text-slate-600"
              onClick={onToggle}
            />
          )}
        </div>
      </div>

      {/* Opacity slider (shown when visible) */}
      {isVisible && (
        <div className="px-3 pb-2 flex items-center gap-2">
          <span className="text-[10px] text-slate-500 w-12">Opacity</span>
          <input
            type="range"
            min={0}
            max={1}
            step={0.05}
            value={opacity}
            onChange={(e) => onOpacityChange(parseFloat(e.target.value))}
            className="flex-1 h-1 accent-blue-600"
          />
          <span className="text-[10px] text-slate-500 w-8 text-right">
            {Math.round(opacity * 100)}%
          </span>
        </div>
      )}
    </div>
  );
}

function ValidationLayer({
  layerName,
  data,
  color,
  opacity,
  fillOpacity,
  onFeatureClick,
}: {
  layerName: string;
  data: ValidationGeoJSON;
  color: string;
  opacity: number;
  fillOpacity?: number;
  onFeatureClick: (props: Record<string, unknown>) => void;
}) {
  const pointLayers = [
    "store_locations",
    "competitor_locations",
    "osm_pois",
    "expansion_candidates",
    "seed_points",
    "census_demographics",
  ];

  if (pointLayers.includes(layerName)) {
    return (
      <>
        {data.features.map((f, i) => {
          if (f.geometry.type !== "Point") return null;
          const [lng, lat] = f.geometry.coordinates as [number, number];
          if (!lat || !lng) return null;

          // Size based on layer
          const radius = layerName === "osm_pois" ? 2 : layerName === "census_demographics" ? 3 : 5;

          return (
            <CircleMarker
              key={`${layerName}-${i}`}
              center={[lat, lng]}
              radius={radius}
              pathOptions={{
                color,
                fillColor: color,
                fillOpacity: opacity * 0.7,
                weight: 1,
                opacity,
              }}
              eventHandlers={{
                click: () => onFeatureClick(f.properties),
              }}
            >
              <Popup>
                <div className="text-xs max-w-48">
                  {Object.entries(f.properties)
                    .slice(0, 5)
                    .map(([k, v]) => (
                      <div key={k}>
                        <strong>{k}:</strong> {String(v ?? "")}
                      </div>
                    ))}
                </div>
              </Popup>
            </CircleMarker>
          );
        })}
      </>
    );
  }

  // Polygon layers (isochrones, block groups, state boundary, h3 hexagons)
  const geojsonData = {
    type: "FeatureCollection" as const,
    features: data.features.map((f) => ({
      type: "Feature" as const,
      geometry: f.geometry,
      properties: f.properties,
    })),
  };

  const getStyle = (feature?: GeoJSON.Feature) => {
    // H3 hexagons & ZCTAs: color by urbanicity
    if ((layerName === "h3_features" || layerName === "census_zcta") && feature?.properties) {
      const cat = (feature.properties.urbanicity_category as string) || "suburban";
      const hexColor = cat === "urban" ? "#ef4444" : cat === "suburban" ? "#f59e0b" : "#22c55e";
      const weight = layerName === "census_zcta" ? 1.5 : 0.5;
      const fill = layerName === "census_zcta" ? opacity * 0.35 : opacity * 0.5;
      return {
        color: hexColor,
        weight,
        opacity,
        fillColor: hexColor,
        fillOpacity: fill,
      };
    }
    return {
      color,
      weight: layerName === "state_boundary" ? 2.5 : 1.5,
      opacity,
      fillColor: color,
      fillOpacity: fillOpacity ?? opacity * 0.2,
    };
  };

  return (
    <GeoJSON
      key={`${layerName}-${opacity}`}
      data={geojsonData}
      style={getStyle}
      onEachFeature={(feature, layer) => {
        layer.on("click", () => {
          onFeatureClick(feature.properties as Record<string, unknown>);
        });
      }}
    />
  );
}

function CoordinateTooltip() {
  const map = useMap();

  useEffect(() => {
    const handler = (e: L.LeafletMouseEvent) => {
      const el = document.getElementById("coord-display");
      if (el) {
        el.textContent = `${e.latlng.lat.toFixed(5)}, ${e.latlng.lng.toFixed(5)}`;
      }
    };
    map.on("mousemove", handler);
    return () => {
      map.off("mousemove", handler);
    };
  }, [map]);

  return null;
}
