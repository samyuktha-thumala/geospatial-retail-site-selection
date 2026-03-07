// API client - fetches from FastAPI backend

const BASE = "/api";

async function fetchJson<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

// --- Types ---

export interface Location {
  id: string;
  name: string;
  lat: number;
  lng: number;
  format: "express" | "standard" | "flagship";
  monthly_sales: number;
}

export interface LocationDetail extends Location {
  urbanicity: string;
  store_age_years: number;
}

export interface Competitor {
  id: string;
  lat: number;
  lng: number;
  brand: string;
  open_year: number;
  is_projected: boolean;
}

export interface Hotspot {
  id: string;
  lat: number;
  lng: number;
  score: number;
  projected_sales: number;
  format: string;
  urbanicity: string;
}

export interface ClosureMetric {
  label: string;
  store_value: string;
  network_avg: string;
}

export interface ClosureCandidate {
  id: string;
  name: string;
  lat: number;
  lng: number;
  format: string;
  urbanicity: string;
  monthly_sales: number;
  closure_risk: number;
  reason: string;
  closure_metrics: ClosureMetric[];
}

export interface ChartDataPoint {
  label: string;
  value: number;
}

export interface DataSourceStat {
  key: string;
  value: string;
}

export interface DataSource {
  id: string;
  name: string;
  type: string;
  icon: string;
  records: string;
  last_sync: string;
  refresh_rate: string;
  stats: DataSourceStat[];
  chart_data: ChartDataPoint[];
  chart_type: string;
}

export interface Kpi {
  label: string;
  value: string;
  subtext: string | null;
  trend: number | null;
  trend_label: string | null;
  icon: string;
}

export interface FeatureImportance {
  name: string;
  importance: number;
  direction: string;
  category: string;
}

export interface ModelPerformance {
  format: string;
  store_count: number;
  r_squared: number;
  mae: string;
  rmse: string;
  top_features: FeatureImportance[];
}

export interface DistributionBucket {
  label: string;
  value: number;
  color?: string;
}

export interface Demographics {
  age_distribution: DistributionBucket[];
  income_distribution: DistributionBucket[];
}

export interface NetworkMetrics {
  total_current_revenue: string;
  projected_optimized_revenue: string;
  revenue_uplift: string;
  new_locations_recommended: number;
  whitespace_locations: number;
}

export interface ClosestCompetitor {
  brand: string;
  distance_miles: number;
  location_name: string;
}

export interface SimulationResult {
  optimized_locations: Array<{
    id: string;
    lat: number;
    lng: number;
    format: string;
    projected_revenue: number;
    score: number;
  }>;
  total_projected_revenue: number;
  network_revenue_change: number;
  cannibalization_rate: number;
  avg_site_score: number;
}

export interface ChatResponse {
  response: string;
  suggestions: string[];
}

export interface H3CellFeature {
  type: "Feature";
  geometry: GeoJSON.Geometry;
  properties: {
    h3_cell_id: string;
    total_population: number;
    population_density: number;
    median_household_income: number;
    total_competitor_count: number;
    total_poi_count: number;
    median_age: number;
    urbanicity_category: string;
  };
}

export interface H3FeatureCollection {
  type: "FeatureCollection";
  features: H3CellFeature[];
  store_id: string;
  cell_count: number;
  metric_ranges: Record<string, { min: number; max: number }>;
}

export interface Isochrone {
  store_number: string;
  geojson: GeoJSON.Geometry;
  drive_time_minutes: number;
  urbanicity_category: string;
  area_sqkm: number;
}

export interface Alert {
  id: number;
  type: "warning" | "info";
  title: string;
  message: string;
  time: string;
  severity: "high" | "medium" | "low";
}

// --- Validation Layer Types ---

export interface ValidationLayerInfo {
  name: string;
  display_name: string;
  table_name: string;
  row_count: number;
  geometry_type: "point" | "polygon" | "hexagon";
  available: boolean;
}

export interface ValidationFeature {
  type: "Feature";
  geometry: GeoJSON.Geometry;
  properties: Record<string, unknown>;
}

export interface ValidationGeoJSON {
  type: "FeatureCollection";
  features: ValidationFeature[];
  layer_name: string;
  total_count: number;
}

// --- API functions ---

export const api = {
  listDataSources: () => fetchJson<DataSource[]>("/data-sources"),
  listLocations: (format?: string) =>
    fetchJson<Location[]>(`/locations${format ? `?format=${format}` : ""}`),
  getLocation: (id: string) => fetchJson<LocationDetail>(`/locations/${id}`),
  listCompetitors: (year?: number, brand?: string) => {
    const params = new URLSearchParams();
    if (year) params.set("year", String(year));
    if (brand) params.set("brand", brand);
    const qs = params.toString();
    return fetchJson<Competitor[]>(`/competitors${qs ? `?${qs}` : ""}`);
  },
  listHotspots: () => fetchJson<Hotspot[]>("/hotspots"),
  listClosureCandidates: () => fetchJson<ClosureCandidate[]>("/closure-candidates"),
  listKpis: () => fetchJson<Kpi[]>("/kpis"),
  listModelPerformance: () => fetchJson<ModelPerformance[]>("/model-performance"),
  getDemographics: () => fetchJson<Demographics>("/demographics"),
  getNetworkMetrics: () => fetchJson<NetworkMetrics>("/network-metrics"),
  listClosestCompetitors: () => fetchJson<ClosestCompetitor[]>("/closest-competitors"),
  runSimulation: (params: Record<string, unknown>) =>
    fetchJson<SimulationResult>("/simulate", {
      method: "POST",
      body: JSON.stringify(params),
    }),
  sendChat: (message: string, context?: Record<string, unknown>, history?: Array<{role: string; content: string}>) =>
    fetchJson<ChatResponse>("/chat", {
      method: "POST",
      body: JSON.stringify({ message, context, history: history || [] }),
    }),
  listIsochrones: () => fetchJson<Isochrone[]>("/isochrones"),
  getH3Features: (storeId: string) => fetchJson<H3FeatureCollection>(`/h3-features/${storeId}`),
  listAlerts: () => fetchJson<Alert[]>("/alerts"),
  // Validation
  listValidationLayers: () => fetchJson<ValidationLayerInfo[]>("/validation/layers"),
  getValidationLayer: (layerName: string, bbox?: string) =>
    fetchJson<ValidationGeoJSON>(
      `/validation/${layerName}${bbox ? `?bbox=${bbox}` : ""}`
    ),
};
