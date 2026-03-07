from pydantic import BaseModel
from enum import Enum
from typing import Optional


# --- Enums ---

class StoreFormat(str, Enum):
    EXPRESS = "express"
    STANDARD = "standard"
    FLAGSHIP = "flagship"


class Urbanicity(str, Enum):
    URBAN_CORE = "urban_core"
    URBAN = "urban"
    SUBURBAN = "suburban"
    EXURBAN = "exurban"
    RURAL = "rural"


class AlertSeverity(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class AlertType(str, Enum):
    WARNING = "warning"
    INFO = "info"


# --- Location Models ---

class LocationOut(BaseModel):
    id: str
    name: str
    lat: float
    lng: float
    format: StoreFormat
    urbanicity: Urbanicity
    monthly_sales: float
    store_age_years: float


class LocationListOut(BaseModel):
    id: str
    name: str
    lat: float
    lng: float
    format: StoreFormat
    monthly_sales: float


# --- Competitor Models ---

class CompetitorOut(BaseModel):
    id: str
    lat: float
    lng: float
    brand: str
    open_year: int
    is_projected: bool


# --- Hotspot Models ---

class HotspotOut(BaseModel):
    id: str
    lat: float
    lng: float
    score: float
    projected_sales: float
    format: StoreFormat
    urbanicity: Urbanicity


# --- Closure Candidate Models ---

class ClosureMetric(BaseModel):
    label: str
    store_value: str
    network_avg: str


class ClosureCandidateOut(BaseModel):
    id: str
    name: str
    lat: float
    lng: float
    format: StoreFormat
    urbanicity: Urbanicity
    monthly_sales: float
    closure_risk: float
    reason: str
    closure_metrics: list[ClosureMetric] = []


# --- Data Source Models ---

class ChartDataPoint(BaseModel):
    label: str
    value: float


class DataSourceStat(BaseModel):
    key: str
    value: str


class DataSourceOut(BaseModel):
    id: str
    name: str
    type: str
    icon: str
    records: str
    last_sync: str
    refresh_rate: str
    stats: list[DataSourceStat]
    chart_data: list[ChartDataPoint]
    chart_type: str


# --- KPI Models ---

class KpiOut(BaseModel):
    label: str
    value: str
    subtext: Optional[str] = None
    trend: Optional[float] = None
    trend_label: Optional[str] = None
    icon: str


# --- Model Performance ---

class FeatureImportance(BaseModel):
    name: str
    importance: float
    direction: str
    category: str


class ModelPerformanceOut(BaseModel):
    format: StoreFormat
    store_count: int
    r_squared: float
    mae: str
    rmse: str
    top_features: list[FeatureImportance]


# --- Demographics ---

class DistributionBucket(BaseModel):
    label: str
    value: float
    color: Optional[str] = None


class DemographicsOut(BaseModel):
    age_distribution: list[DistributionBucket]
    income_distribution: list[DistributionBucket]


# --- Scenario / Simulation ---

class UrbanicityDistances(BaseModel):
    urban: float = 1.5
    suburban: float = 3.0
    rural: float = 5.0

class UserLocation(BaseModel):
    lat: float
    lng: float
    format: str = "standard"

class DemandParams(BaseModel):
    competitor_year: int = 2025
    spatial_optimization: bool = True
    min_distance_from_network: UrbanicityDistances = UrbanicityDistances()
    min_distance_between_new: UrbanicityDistances = UrbanicityDistances(urban=2.0, suburban=5.0, rural=8.0)
    brand_popularity: dict[str, float] = {}
    final_locations_count: int = 10
    excluded_closure_risks: list[str] = []
    removed_store_ids: list[str] = []
    added_locations: list[UserLocation] = []


class OptimizedLocation(BaseModel):
    id: str
    lat: float
    lng: float
    format: StoreFormat
    projected_revenue: float
    score: float


class SimulationResultOut(BaseModel):
    optimized_locations: list[OptimizedLocation]
    total_projected_revenue: float
    network_revenue_change: float
    cannibalization_rate: float
    avg_site_score: float


# --- Chat ---

class ChatHistoryMessage(BaseModel):
    role: str
    content: str


class ChatMessageIn(BaseModel):
    message: str
    context: Optional[dict] = None
    history: list[ChatHistoryMessage] = []


class ChatResponseOut(BaseModel):
    response: str
    suggestions: list[str] = []


# --- Alerts ---

class AlertOut(BaseModel):
    id: int
    type: AlertType
    title: str
    message: str
    time: str
    severity: AlertSeverity


# --- Network Metrics ---

class NetworkMetricsOut(BaseModel):
    total_current_revenue: str
    projected_optimized_revenue: str
    revenue_uplift: str
    new_locations_recommended: int
    whitespace_locations: int = 0


# --- Closest Competitors ---

class ClosestCompetitorOut(BaseModel):
    brand: str
    distance_miles: float
    location_name: str


# --- Isochrones ---

class IsochroneOut(BaseModel):
    store_number: str
    geojson: dict
    drive_time_minutes: int
    urbanicity_category: str
    area_sqkm: float


# --- H3 Feature Models ---

class H3CellFeature(BaseModel):
    type: str = "Feature"
    geometry: dict
    properties: dict


class H3FeatureCollection(BaseModel):
    type: str = "FeatureCollection"
    features: list[H3CellFeature]
    store_id: str
    cell_count: int
    metric_ranges: dict  # {metric_name: {min, max}} for color scaling


# --- Validation Layer Models ---

class ValidationLayerInfo(BaseModel):
    name: str
    display_name: str
    table_name: str
    row_count: int
    geometry_type: str  # "point", "polygon", "hexagon"
    available: bool


class ValidationFeature(BaseModel):
    type: str = "Feature"
    geometry: dict
    properties: dict


class ValidationGeoJSON(BaseModel):
    type: str = "FeatureCollection"
    features: list[ValidationFeature]
    layer_name: str
    total_count: int
