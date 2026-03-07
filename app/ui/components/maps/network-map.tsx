import { useEffect, useRef, useMemo } from "react";
import L from "leaflet";
import type { Location, Competitor, ClosureCandidate, Isochrone, H3FeatureCollection } from "@/lib/api";

const FORMAT_RADIUS: Record<string, number> = {
  flagship: 8,
  standard: 6,
  express: 4,
};

const PINK_SCALE = ["#fdf2f8", "#fbcfe8", "#f472b6", "#db2777", "#9d174d"];

export type H3Metric = "total_population" | "population_density" | "median_household_income" | "total_competitor_count" | "total_poi_count" | "median_age";

export type MapFilter = "all" | "express" | "standard" | "flagship" | "competitors" | "at_risk";

interface NetworkMapProps {
  locations: Location[];
  competitors: Competitor[];
  closureCandidates: ClosureCandidate[];
  isochrones?: Isochrone[];
  selectedLocationIds?: Set<string>;
  onSelectLocations?: (ids: string[]) => void;
  h3Data?: H3FeatureCollection | null;
  h3Metric?: H3Metric;
  filter?: MapFilter;
}

function getQuantileColor(value: number, min: number, max: number): string {
  if (max === min) return PINK_SCALE[2];
  const ratio = Math.max(0, Math.min(1, (value - min) / (max - min)));
  const idx = Math.min(4, Math.floor(ratio * 5));
  return PINK_SCALE[idx];
}

export function NetworkMap({
  locations,
  competitors,
  closureCandidates,
  isochrones = [],
  selectedLocationIds,
  onSelectLocations,
  h3Data,
  h3Metric = "total_population",
  filter = "all",
}: NetworkMapProps) {
  const mapRef = useRef<L.Map | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const layersRef = useRef<L.LayerGroup[]>([]);
  const selectBoxRef = useRef<L.Rectangle | null>(null);
  const dragStartRef = useRef<L.LatLng | null>(null);

  const closureIds = useMemo(
    () => new Set(closureCandidates.map((c) => c.id)),
    [closureCandidates]
  );

  const onSelectLocationsRef = useRef(onSelectLocations);
  onSelectLocationsRef.current = onSelectLocations;

  const locationsRef = useRef(locations);
  locationsRef.current = locations;

  // Initialize map once
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const map = L.map(containerRef.current, {
      center: [39.8283, -98.5795],
      zoom: 5,
      zoomControl: false,
      boxZoom: false,
    });

    L.control.zoom({ position: "topright" }).addTo(map);

    L.tileLayer(
      "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
      {
        attribution: '&copy; <a href="https://carto.com/">CARTO</a>',
        maxZoom: 19,
      }
    ).addTo(map);

    // Box selection: shift+drag
    map.on("mousedown", (e: L.LeafletMouseEvent) => {
      if (!e.originalEvent.shiftKey) return;
      e.originalEvent.preventDefault();
      e.originalEvent.stopPropagation();
      map.dragging.disable();
      dragStartRef.current = e.latlng;
      if (selectBoxRef.current) {
        selectBoxRef.current.remove();
        selectBoxRef.current = null;
      }
    });

    map.on("mousemove", (e: L.LeafletMouseEvent) => {
      if (!dragStartRef.current) return;
      const bounds = L.latLngBounds(dragStartRef.current, e.latlng);
      if (selectBoxRef.current) {
        selectBoxRef.current.setBounds(bounds);
      } else {
        selectBoxRef.current = L.rectangle(bounds, {
          color: "#3b82f6", weight: 2, fillOpacity: 0.1, dashArray: "5",
        }).addTo(map);
      }
    });

    map.on("mouseup", (e: L.LeafletMouseEvent) => {
      if (!dragStartRef.current) return;
      const bounds = L.latLngBounds(dragStartRef.current, e.latlng);
      dragStartRef.current = null;
      map.dragging.enable();
      if (selectBoxRef.current) {
        selectBoxRef.current.remove();
        selectBoxRef.current = null;
      }
      // Zoom into the selected area
      map.fitBounds(bounds, { padding: [20, 20] });
      const selected = locationsRef.current
        .filter((loc) => bounds.contains(L.latLng(loc.lat, loc.lng)))
        .map((loc) => loc.id);
      if (selected.length > 0 && onSelectLocationsRef.current) {
        onSelectLocationsRef.current(selected);
      }
    });

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

  // Zoom to selection
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !selectedLocationIds || selectedLocationIds.size === 0) return;
    const pts = locations.filter((l) => selectedLocationIds.has(l.id)).map((l) => L.latLng(l.lat, l.lng));
    if (pts.length > 0) {
      map.fitBounds(L.latLngBounds(pts), { padding: [60, 60], maxZoom: 14 });
    }
  }, [selectedLocationIds, locations]);

  // Render layers
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;

    layersRef.current.forEach((lg) => lg.remove());
    layersRef.current = [];

    // H3 hexagons (when single location selected and data available)
    if (h3Data && h3Data.features.length > 0) {
      const h3Group = L.layerGroup();
      const range = h3Data.metric_ranges[h3Metric] || { min: 0, max: 1 };

      h3Data.features.forEach((feat) => {
        const val = feat.properties[h3Metric] as number;
        const color = getQuantileColor(val, range.min, range.max);

        L.geoJSON(feat.geometry as GeoJSON.GeoJsonObject, {
          style: {
            fillColor: color,
            fillOpacity: 0.6,
            color: "#be185d",
            weight: 0.5,
            opacity: 0.4,
          },
        })
          .bindTooltip(
            `<b>${h3Metric.replace(/_/g, " ")}</b>: ${typeof val === "number" ? val.toLocaleString() : val}<br/>Pop: ${feat.properties.total_population.toLocaleString()}<br/>Income: $${feat.properties.median_household_income.toLocaleString()}`,
            { sticky: true }
          )
          .addTo(h3Group);
      });

      h3Group.addTo(map);
      layersRef.current.push(h3Group);
    }

    // Trade areas (isochrones) for selected locations (≤10, only if no H3 data)
    if (!h3Data && selectedLocationIds && selectedLocationIds.size > 0 && selectedLocationIds.size <= 10) {
      const tradeGroup = L.layerGroup();
      const isoMap = new Map(isochrones.map((iso) => [iso.store_number, iso]));
      locations.filter((loc) => selectedLocationIds.has(loc.id)).forEach((loc) => {
        const storeNum = loc.id.replace("LOC", "");
        const iso = isoMap.get(storeNum);
        if (iso) {
          L.geoJSON(iso.geojson as GeoJSON.GeoJsonObject, {
            style: { fillColor: "#3b82f6", fillOpacity: 0.1, color: "#3b82f6", weight: 1, opacity: 0.5 },
          }).addTo(tradeGroup);
        }
      });
      tradeGroup.addTo(map);
      layersRef.current.push(tradeGroup);
    }

    // Competitor markers
    if (filter === "all" || filter === "competitors") {
      const compGroup = L.layerGroup();
      competitors.filter((c) => !c.is_projected).forEach((comp) => {
        L.circleMarker([comp.lat, comp.lng], {
          radius: 3, fillColor: "#ef4444", fillOpacity: 0.8, color: "#ef4444", weight: 0.5,
        })
          .bindTooltip(`<b>${comp.brand}</b><br/>Opened: ${comp.open_year}`, { direction: "top", offset: L.point(0, -3) })
          .addTo(compGroup);
      });
      compGroup.addTo(map);
      layersRef.current.push(compGroup);
    }

    // Store markers
    const storeGroup = L.layerGroup();
    const filteredLocs = filter === "all" ? locations
      : filter === "at_risk" ? locations.filter((l) => closureIds.has(l.id))
      : filter === "competitors" ? locations // show all stores when viewing competitors
      : locations.filter((l) => l.format === filter);

    filteredLocs.forEach((loc) => {
      const isAtRisk = closureIds.has(loc.id);
      const isSelected = selectedLocationIds?.has(loc.id);
      const radius = FORMAT_RADIUS[loc.format] || 6;

      let marker: L.Marker | L.CircleMarker;

      if (isAtRisk) {
        // At-risk locations get a ⚠ icon marker
        const size = isSelected ? 22 : 18;
        const icon = L.divIcon({
          className: "",
          html: `<div style="
            width:${size}px;height:${size}px;
            display:flex;align-items:center;justify-content:center;
            background:#f97316;color:#fff;
            border-radius:50%;
            font-size:${size * 0.6}px;font-weight:bold;
            border:2px solid ${isSelected ? "#1d4ed8" : "#fff"};
            box-shadow:0 1px 3px rgba(0,0,0,0.3);
            line-height:1;
          ">!</div>`,
          iconSize: [size, size],
          iconAnchor: [size / 2, size / 2],
        });
        marker = L.marker([loc.lat, loc.lng], { icon });
      } else {
        marker = L.circleMarker([loc.lat, loc.lng], {
          radius: isSelected ? radius + 2 : radius,
          fillColor: "#3b82f6",
          fillOpacity: isSelected ? 1 : 0.8,
          color: isSelected ? "#1d4ed8" : "#fff",
          weight: isSelected ? 2 : 1,
        });
      }

      const mo = loc.monthly_sales;
      const moStr = mo >= 1000 ? `$${(mo / 1000).toFixed(1)}M` : `$${mo.toFixed(0)}K`;
      const annStr = (mo * 12) >= 1000 ? `$${(mo * 12 / 1000).toFixed(1)}M` : `$${(mo * 12).toFixed(0)}K`;
      marker.bindTooltip(
        `<b>${loc.name}</b><br/>${loc.format}<br/>Monthly: ${moStr}<br/>Annual: ${annStr}${isAtRisk ? "<br/>⚠ At Risk" : ""}`,
        { direction: "top", offset: L.point(0, -radius), className: "store-tooltip" }
      );

      marker.on("click", (e) => {
        L.DomEvent.stopPropagation(e);
        if (onSelectLocationsRef.current) {
          onSelectLocationsRef.current([loc.id]);
        }
        window.dispatchEvent(new CustomEvent("location-selected", {
          detail: { lat: loc.lat, lng: loc.lng, name: loc.name, format: loc.format, sales: loc.monthly_sales, id: loc.id },
        }));
      });

      marker.addTo(storeGroup);
    });
    storeGroup.addTo(map);
    layersRef.current.push(storeGroup);
  }, [locations, competitors, closureIds, isochrones, selectedLocationIds, h3Data, h3Metric, filter]);

  return (
    <div className="relative w-full h-full bg-white overflow-hidden">
      <div ref={containerRef} className="w-full h-full" />

      {/* Legend — vertical, bottom left */}
      <div className="absolute bottom-3 left-3 z-[1000] bg-white/90 backdrop-blur border border-slate-200 rounded-md px-2.5 py-2 text-[10px] text-slate-600 shadow-sm flex flex-col gap-1.5">
        <span className="flex items-center gap-1.5"><span className="w-1.5 h-1.5 rounded-full bg-blue-500 shrink-0" />Express</span>
        <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-blue-500 shrink-0" />Standard</span>
        <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full bg-blue-500 shrink-0" />Flagship</span>
        <span className="flex items-center gap-1.5"><span className="w-3.5 h-3.5 rounded-full bg-orange-500 text-white text-[8px] font-bold flex items-center justify-center leading-none shrink-0">!</span>At Risk</span>
        <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-red-500 shrink-0" />Competitor</span>
        {h3Data && h3Data.features.length > 0 && (
          <>
            <span className="w-full h-px bg-slate-200" />
            <span className="flex items-center gap-1.5">
              <span className="flex gap-px">{PINK_SCALE.map((c) => <span key={c} className="w-2 h-2" style={{ backgroundColor: c }} />)}</span>
              H3
            </span>
          </>
        )}
      </div>

      {/* Selection hint */}
      <div className="absolute top-3 left-3 z-[1000] bg-white/80 backdrop-blur border border-slate-200 rounded-md px-2 py-1 text-[10px] text-slate-400">
        Shift + drag to select area
      </div>
    </div>
  );
}
