import { useEffect, useRef } from "react";
import L from "leaflet";
import type { Location, Competitor, Hotspot, ClosureCandidate, Isochrone } from "@/lib/api";

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
  optimizedLocations?: Array<{ lat: number; lng: number; format: string; score: number }>;
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
}: PlaygroundMapProps) {
  const mapRef = useRef<L.Map | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const layerGroupsRef = useRef<Record<string, L.LayerGroup>>({});

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const map = L.map(containerRef.current, {
      center: [39.8283, -98.5795],
      zoom: 5,
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
    return () => { map.remove(); mapRef.current = null; };
  }, []);

  // Auto-fit bounds
  useEffect(() => {
    const map = mapRef.current;
    if (!map || locations.length === 0) return;
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
            `<b>Hotspot ${hs.id}</b><br/>Score: ${hs.score}<br/>Projected: $${hs.projected_sales}K/mo`,
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
            `<b>${cc.name}</b><br/>Risk: ${(cc.closure_risk * 100).toFixed(0)}%<br/>${cc.reason}`,
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
            `<b>Optimized Site</b><br/>Format: ${ol.format}<br/>Score: ${ol.score}`,
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
  }, [locations, competitors, hotspots, closureCandidates, isochrones, layers, userLocations, optimizedLocations]);

  return <div ref={containerRef} className="w-full h-full" />;
}
