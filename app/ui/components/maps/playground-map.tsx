import { useEffect, useRef } from "react";
import L from "leaflet";
import type { Location, Competitor, Hotspot, ClosureCandidate, Isochrone, ChatMapPoint } from "@/lib/api";

const FORMAT_RADIUS: Record<string, number> = {
  flagship: 8,
  standard: 6,
  express: 4,
};

/** Format sales in K to readable B/M/K string */
function fmtSalesK(valK: number): string {
  if (valK >= 1000000) return `$${(valK / 1000000).toFixed(1)}B`;
  if (valK >= 1000) return `$${(valK / 1000).toFixed(1)}M`;
  return `$${valK.toFixed(0)}K`;
}

interface PlaygroundMapProps {
  locations: Location[];
  competitors: Competitor[];
  hotspots: Hotspot[];
  closureCandidates: ClosureCandidate[];
  isochrones?: Isochrone[];
  layers: Record<string, boolean>;
  userLocations?: Array<{ lat: number; lng: number; format: string; estimated_sales?: number; nearest_store?: string }>;
  optimizedLocations?: Array<{ lat: number; lng: number; format: string; projected_revenue: number; score: number }>;
  agentPoints?: ChatMapPoint[];
}

export function PlaygroundMap({
  locations,
  competitors,
  hotspots,
  closureCandidates,
  isochrones = [],
  layers,
  userLocations = [],
  optimizedLocations = [],
  agentPoints = [],
}: PlaygroundMapProps) {
  const mapRef = useRef<L.Map | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const layerGroupsRef = useRef<Record<string, L.LayerGroup>>({});

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const map = L.map(containerRef.current, {
      center: [42.5, -75.5],  // NY State center
      zoom: 7,
      zoomControl: false,
    });

    L.control.zoom({ position: "topright" }).addTo(map);

    L.tileLayer(
      "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
      {
        attribution: '&copy; <a href="https://carto.com/">CARTO</a>',
        maxZoom: 19,
      }
    ).addTo(map);

    mapRef.current = map;

    const handleZoomOut = () => map.setView([42.5, -75.5], 7, { animate: true });
    window.addEventListener("map-zoom-out", handleZoomOut);

    return () => { window.removeEventListener("map-zoom-out", handleZoomOut); map.remove(); mapRef.current = null; };
  }, []);

  // Auto-fit bounds — only on first load, not on every locations change
  const initialFitDoneRef = useRef(false);
  useEffect(() => {
    const map = mapRef.current;
    if (!map || locations.length === 0 || initialFitDoneRef.current || agentPoints.length > 0) return;
    initialFitDoneRef.current = true;
    const points: L.LatLngExpression[] = locations.map((l) => [l.lat, l.lng]);
    if (points.length > 0) {
      map.fitBounds(L.latLngBounds(points), { padding: [40, 40], maxZoom: 12 });
    }
  }, [locations]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;

    Object.values(layerGroupsRef.current).forEach((lg) => lg.clearLayers());

    // Trade areas
    const tradeGroup = L.layerGroup();
    if (layers.tradeAreas) {
      const isoMap = new Map(isochrones.map((iso) => [iso.store_number, iso]));
      locations.forEach((loc) => {
        const storeNum = loc.id.replace("LOC", "");
        const iso = isoMap.get(storeNum);
        if (iso) {
          L.geoJSON(iso.geojson as GeoJSON.GeoJsonObject, {
            style: { fillColor: "#3b82f6", fillOpacity: 0.08, color: "#3b82f6", weight: 0.5, opacity: 0.3 },
          }).addTo(tradeGroup);
        }
      });
    }
    tradeGroup.addTo(map);
    layerGroupsRef.current.tradeAreas = tradeGroup;

    // Competitors
    const compGroup = L.layerGroup();
    if (layers.competitors) {
      competitors.forEach((comp) => {
        L.circleMarker([comp.lat, comp.lng], {
          radius: 3,
          fillColor: "#ef4444",
          fillOpacity: comp.is_projected ? 0.5 : 0.8,
          color: "#ef4444",
          weight: 0.5,
          dashArray: comp.is_projected ? "3" : undefined,
        })
          .bindTooltip(
            `<b>${comp.brand}</b><br/>Opened: ${comp.open_year}${comp.is_projected ? " (Projected)" : ""}`,
            { direction: "top", offset: L.point(0, -3) }
          )
          .addTo(compGroup);
      });
    }
    compGroup.addTo(map);
    layerGroupsRef.current.competitors = compGroup;

    // Hotspots
    const hotGroup = L.layerGroup();
    if (layers.hotspots) {
      hotspots.forEach((hs) => {
        L.circleMarker([hs.lat, hs.lng], {
          radius: 6,
          fillColor: "#fbbf24",
          fillOpacity: 0.7,
          color: "#f59e0b",
          weight: 1.5,
        })
          .bindTooltip(
            `<b>Hotspot ${hs.id}</b><br/>Projected Annual Revenue: ${(() => { const annual = hs.projected_sales * 12; return annual >= 1000 ? `$${(annual / 1000).toFixed(1)}M` : `$${Math.round(annual)}K`; })()}`,
            { direction: "top", offset: L.point(0, -6) }
          )
          .addTo(hotGroup);
      });
    }
    hotGroup.addTo(map);
    layerGroupsRef.current.hotspots = hotGroup;

    // At-risk (closure candidates) — orange ! icon, consistent with network diagnostics
    const riskGroup = L.layerGroup();
    if (layers.closureRisks !== false) {
      closureCandidates.forEach((cc) => {
        const icon = L.divIcon({
          className: "",
          html: `<div style="width:14px;height:14px;border-radius:50%;background:#f97316;display:flex;align-items:center;justify-content:center;"><span style="color:#fff;font-size:8px;font-weight:700;line-height:1;">!</span></div>`,
          iconSize: [14, 14],
          iconAnchor: [7, 7],
        });
        L.marker([cc.lat, cc.lng], { icon })
          .bindTooltip(
            `<b>${cc.name}</b><br/>${cc.reason}`,
            { direction: "top", offset: L.point(0, -7) }
          )
          .addTo(riskGroup);
      });
    }
    riskGroup.addTo(map);
    layerGroupsRef.current.closureRisks = riskGroup;

    // Store markers — all blue, sized by format (consistent with network diagnostics)
    const locGroup = L.layerGroup();
    locations.forEach((loc) => {
      const radius = FORMAT_RADIUS[loc.format] || 6;
      L.circleMarker([loc.lat, loc.lng], {
        radius,
        fillColor: "#3b82f6",
        fillOpacity: 0.8,
        color: "#fff",
        weight: 1,
      })
        .bindTooltip(
          `<b>${loc.name}</b><br/>${loc.format}<br/>Monthly: ${fmtSalesK(loc.monthly_sales)}<br/>Annual: ${fmtSalesK(loc.monthly_sales * 12)}`,
          { direction: "top", offset: L.point(0, -radius) }
        )
        .addTo(locGroup);
    });
    locGroup.addTo(map);
    layerGroupsRef.current.locations = locGroup;

    // Optimized locations
    const optGroup = L.layerGroup();
    if (layers.finalLocations) {
      optimizedLocations.forEach((ol) => {
        L.circleMarker([ol.lat, ol.lng], {
          radius: 9,
          fillColor: "#10b981",
          fillOpacity: 0.9,
          color: "#fff",
          weight: 2,
        })
          .bindTooltip(
            `<b>Optimized Site</b><br/>Format: ${ol.format}<br/>Est. Revenue: $${ol.projected_revenue >= 1000 ? (ol.projected_revenue / 1000).toFixed(1) + "M" : ol.projected_revenue.toFixed(0) + "K"}/yr`,
            { direction: "top", offset: L.point(0, -9) }
          )
          .addTo(optGroup);
      });
    }
    optGroup.addTo(map);
    layerGroupsRef.current.optimized = optGroup;

    // User-added locations
    const userGroup = L.layerGroup();
    if (layers.userLocations !== false) {
      userLocations.forEach((ul) => {
        L.circleMarker([ul.lat, ul.lng], {
          radius: 8,
          fillColor: "#22d3ee",
          fillOpacity: 0.9,
          color: "#fff",
          weight: 2,
        })
          .bindTooltip(
            `<b>User Location</b><br/>${ul.format}${ul.estimated_sales ? `<br/>Est. Monthly: ~${fmtSalesK(ul.estimated_sales)}<br/>Est. Annual: ~${fmtSalesK(ul.estimated_sales * 12)}` : ""}${ul.nearest_store ? `<br/><span style="color:#94a3b8">Based on: ${ul.nearest_store}</span>` : ""}`,
            { direction: "top", offset: L.point(0, -8) }
          )
          .addTo(userGroup);
      });
    }
    userGroup.addTo(map);
    layerGroupsRef.current.userLocations = userGroup;

    // Agent points — purple diamond markers (filter to valid NY-area coords)
    const agentGroup = L.layerGroup();
    const validAgentPts = agentPoints.filter(
      (pt) => pt.lat && pt.lng && pt.lat > 39 && pt.lat < 46 && pt.lng > -81 && pt.lng < -71
    );
    if (validAgentPts.length > 0) {
      validAgentPts.forEach((pt) => {
        const isOptimized = pt.properties?.type === "optimized_location";
        const color = isOptimized ? "#8b5cf6" : "#a855f7";
        const size = isOptimized ? 16 : 12;
        const icon = L.divIcon({
          className: "",
          html: `<div style="width:${size}px;height:${size}px;background:${color};border:2px solid #fff;border-radius:2px;transform:rotate(45deg);box-shadow:0 1px 3px rgba(0,0,0,0.3);"></div>`,
          iconSize: [size, size],
          iconAnchor: [size / 2, size / 2],
        });
        const rev = pt.properties?.projected_revenue;
        const revStr = rev ? (Number(rev) >= 1000000 ? `$${(Number(rev) / 1000000).toFixed(1)}M` : `$${(Number(rev) / 1000).toFixed(0)}K`) : "";
        L.marker([pt.lat, pt.lng], { icon })
          .bindTooltip(
            `<b>${pt.label || "Agent Result"}</b>${pt.properties?.format ? `<br/>Format: ${pt.properties.format}` : ""}${revStr ? `<br/>Revenue: ${revStr}` : ""}${pt.properties?.total_poi_count ? `<br/>POIs: ${pt.properties.total_poi_count}` : ""}`,
            { direction: "top", offset: L.point(0, -8) }
          )
          .addTo(agentGroup);
      });
    } else if (locations.length === 0) {
      map.setView([42.5, -75.5], 7);
    }
    agentGroup.addTo(map);
    layerGroupsRef.current.agent = agentGroup;
  }, [locations, competitors, hotspots, closureCandidates, isochrones, layers, userLocations, optimizedLocations, agentPoints]);

  // Separate effect for agent fitBounds — only fires when agentPoints reference changes
  const lastAgentFitRef = useRef<number>(0);
  useEffect(() => {
    const map = mapRef.current;
    if (!map || agentPoints.length === 0) return;
    const validPts = agentPoints.filter(
      (pt) => pt.lat && pt.lng && pt.lat > 39 && pt.lat < 46 && pt.lng > -81 && pt.lng < -71
    );
    if (validPts.length === 0) return;
    // Only fit if these are genuinely new points (avoid re-fit on other state changes)
    const sig = validPts.map((p) => `${p.lat},${p.lng}`).join("|").length;
    if (sig === lastAgentFitRef.current) return;
    lastAgentFitRef.current = sig;
    map.fitBounds(
      L.latLngBounds(validPts.map((p) => [p.lat, p.lng] as L.LatLngExpression)),
      { padding: [60, 60], maxZoom: 10 }
    );
  }, [agentPoints]);

  return <div ref={containerRef} className="w-full h-full" />;
}
