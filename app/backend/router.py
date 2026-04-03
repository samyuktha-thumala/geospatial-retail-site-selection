import math
import json
import logging
from typing import Optional
from fastapi import APIRouter
from .config import conf
from .models import (
    LocationOut, LocationListOut, CompetitorOut, HotspotOut,
    ClosureCandidateOut, DataSourceOut, KpiOut, ModelPerformanceOut,
    DemographicsOut, DemandParams, SimulationResultOut, OptimizedLocation,
    ChatMessageIn, ChatResponseOut, AlertOut, NetworkMetricsOut,
    ClosestCompetitorOut, IsochroneOut, H3FeatureCollection, StoreFormat,
    SaveScenarioIn, SaveScenarioOut,
    AgentChatIn, AgentChatOut,
)
from .data.store import db

logger = logging.getLogger(__name__)

api = APIRouter(prefix=conf.api_prefix)


@api.get("/data-sources", response_model=list[DataSourceOut], operation_id="listDataSources")
async def list_data_sources():
    return db.data_sources


@api.get("/locations", response_model=list[LocationListOut], operation_id="listLocations")
async def list_locations(format: Optional[StoreFormat] = None):
    locations = db.locations
    if format:
        locations = [l for l in locations if l.format == format]
    return [
        LocationListOut(
            id=l.id, name=l.name, lat=l.lat, lng=l.lng,
            format=l.format, monthly_sales=l.monthly_sales,
        )
        for l in locations
    ]


@api.get("/locations/{location_id}", response_model=LocationOut, operation_id="getLocation")
async def get_location(location_id: str):
    for loc in db.locations:
        if loc.id == location_id:
            return loc
    return {"error": "not found"}


@api.get("/competitors", response_model=list[CompetitorOut], operation_id="listCompetitors")
async def list_competitors(year: Optional[int] = None, brand: Optional[str] = None):
    comps = db.competitors
    if year is not None:
        comps = [c for c in comps if c.open_year <= year]
    if brand:
        comps = [c for c in comps if c.brand == brand]
    return comps


@api.get("/hotspots", response_model=list[HotspotOut], operation_id="listHotspots")
async def list_hotspots():
    return db.hotspots


@api.get("/closure-candidates", response_model=list[ClosureCandidateOut], operation_id="listClosureCandidates")
async def list_closure_candidates():
    return db.closure_candidates


@api.get("/kpis", response_model=list[KpiOut], operation_id="listKpis")
async def list_kpis():
    return db.kpis


@api.get("/model-performance", response_model=list[ModelPerformanceOut], operation_id="listModelPerformance")
async def list_model_performance():
    return db.model_performance


@api.get("/demographics", response_model=DemographicsOut, operation_id="getDemographics")
async def get_demographics():
    return db.demographics


@api.get("/network-metrics", response_model=NetworkMetricsOut, operation_id="getNetworkMetrics")
async def get_network_metrics():
    return db.network_metrics


@api.get("/closest-competitors", response_model=list[ClosestCompetitorOut], operation_id="listClosestCompetitors")
async def list_closest_competitors():
    return db.closest_competitors


@api.get("/isochrones", response_model=list[IsochroneOut], operation_id="listIsochrones")
async def list_isochrones():
    return db.isochrones


def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in miles."""
    R = 3958.8  # Earth radius in miles
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


@api.post("/simulate", response_model=SimulationResultOut, operation_id="runSimulation")
async def run_simulation(params: DemandParams):
    """Greedy site optimizer with distance constraints."""
    candidates = sorted(db.hotspots, key=lambda h: h.score, reverse=True)

    # Existing store positions — exclude removed stores and closure risks
    excluded_ids = set(params.excluded_closure_risks) | set(params.removed_store_ids)
    existing_positions = [
        (l.lat, l.lng) for l in db.locations if l.id not in excluded_ids
    ]

    # Include user-added locations as part of the network
    for ul in params.added_locations:
        existing_positions.append((ul.lat, ul.lng))

    selected: list[OptimizedLocation] = []

    def _get_distance(distances, urbanicity_val):
        """Get distance threshold based on candidate urbanicity."""
        u = urbanicity_val.value if hasattr(urbanicity_val, 'value') else str(urbanicity_val)
        if u in ("urban_core", "urban"):
            return distances.urban
        if u == "suburban":
            return distances.suburban
        return distances.rural  # exurban, rural, or unknown

    for hs in candidates:
        if len(selected) >= params.final_locations_count:
            break

        net_dist = _get_distance(params.min_distance_from_network, hs.urbanicity)
        new_dist = _get_distance(params.min_distance_between_new, hs.urbanicity)

        # Check distance from existing network
        too_close_to_network = any(
            _haversine_miles(hs.lat, hs.lng, lat, lng) < net_dist
            for lat, lng in existing_positions
        )
        if too_close_to_network:
            continue

        # Check distance from already-selected new locations
        too_close_to_selected = any(
            _haversine_miles(hs.lat, hs.lng, s.lat, s.lng) < new_dist
            for s in selected
        )
        if too_close_to_selected:
            continue

        selected.append(OptimizedLocation(
            id=f"OPT{len(selected)+1:03d}",
            lat=hs.lat,
            lng=hs.lng,
            format=hs.format,
            projected_revenue=round(hs.projected_sales * 12, 1),
            score=hs.score,
        ))

    total_rev = sum(o.projected_revenue for o in selected)
    # Current revenue excludes removed stores
    current_rev = sum(l.monthly_sales * 12 for l in db.locations if l.id not in excluded_ids)

    revenue_change = round((total_rev / max(current_rev, 0.1)) * 100, 1) if current_rev > 0 else 0
    cannibalization = round(min(15, len(selected) * 0.8), 1)

    return SimulationResultOut(
        optimized_locations=selected,
        total_projected_revenue=round(total_rev, 1),
        network_revenue_change=revenue_change,
        cannibalization_rate=cannibalization,
        avg_site_score=round(sum(o.score for o in selected) / max(1, len(selected)), 1),
    )


@api.post("/save-scenario", response_model=SaveScenarioOut, operation_id="saveScenario")
async def save_scenario(body: SaveScenarioIn):
    """Append a scenario result to gold_expansion_results."""
    from .data import sql_client as sql

    locations_json = json.dumps([loc.model_dump() for loc in body.optimized_locations])

    insert_sql = f"""
    INSERT INTO {sql.table('gold_expansion_results')}
    (scenario_id, scenario_date, scenario_summary, competitor_year,
     min_distance_from_network_urban, min_distance_from_network_suburban, min_distance_from_network_rural,
     min_distance_between_new_urban, min_distance_between_new_suburban, min_distance_between_new_rural,
     final_locations_count, excluded_at_risk_count, removed_store_count, added_location_count,
     total_projected_revenue, network_revenue_change, cannibalization_rate, avg_site_score,
     optimized_locations)
    VALUES
    ('{body.scenario_id}', current_timestamp(), '{body.scenario_summary.replace("'", "''")}', {body.competitor_year},
     {body.min_distance_from_network_urban}, {body.min_distance_from_network_suburban}, {body.min_distance_from_network_rural},
     {body.min_distance_between_new_urban}, {body.min_distance_between_new_suburban}, {body.min_distance_between_new_rural},
     {body.final_locations_count}, {body.excluded_at_risk_count}, {body.removed_store_count}, {body.added_location_count},
     {body.total_projected_revenue}, {body.network_revenue_change}, {body.cannibalization_rate}, {body.avg_site_score},
     '{locations_json.replace("'", "''")}')
    """

    try:
        sql.execute_sql(insert_sql)
        return SaveScenarioOut(success=True, scenario_id=body.scenario_id, message="Scenario saved successfully")
    except Exception as e:
        logger.error(f"Failed to save scenario: {e}")
        return SaveScenarioOut(success=False, scenario_id=body.scenario_id, message=f"Failed to save: {str(e)[:200]}")


@api.post("/agent/chat", response_model=AgentChatOut, operation_id="agentChat")
async def agent_chat(body: AgentChatIn):
    """Site Agent — Claude Sonnet 4.6 with tool-calling."""
    from .data.agent import run_agent

    history = [{"role": h.role, "content": h.content} for h in body.history]
    result = run_agent(
        message=body.message,
        history=history,
        page_context=body.page_context,
    )
    return AgentChatOut(
        response=result["response"],
        map_points=[
            {"lat": p["lat"], "lng": p["lng"], "label": p.get("label", ""), "properties": p.get("properties", {})}
            for p in result.get("map_points", [])
        ],
        suggestions=result.get("suggestions", []),
    )


@api.post("/chat", response_model=ChatResponseOut, operation_id="sendChatMessage")
async def send_chat_message(msg: ChatMessageIn):
    from .data.genie_chat import chat_with_genie

    result = chat_with_genie(
        message=msg.message,
        conversation_id=msg.conversation_id,
    )

    return ChatResponseOut(
        response=result["response"],
        suggestions=result["suggestions"],
        conversation_id=result.get("conversation_id"),
        map_points=result.get("map_points", []),
    )


@api.get("/h3-features/{store_id}", response_model=H3FeatureCollection, operation_id="getH3Features")
async def get_h3_features(store_id: str):
    from .data.fetchers import fetch_h3_features
    return fetch_h3_features(store_id)


@api.get("/alerts", response_model=list[AlertOut], operation_id="listAlerts")
async def list_alerts():
    return db.alerts


# ---- Validation / Testing Endpoints ----
# COMMENTED OUT: Validation layers and data source exploration endpoints.
# These are useful for development/debugging but not needed in production.
# To re-enable, uncomment the VALIDATION_LAYERS dict and the two endpoint functions below.
# See IMPLEMENTATION_PLAN.md for details on available validation layers.

# VALIDATION_LAYERS = {
#     "store_locations": {
#         "display_name": "Store Locations",
#         "table": "bronze_store_locations",
#         "geometry_type": "point",
#         "query": "SELECT store_number, name, lat, lng, format, urbanicity FROM {table}",
#         "point_fields": ("lat", "lng"),
#     },
#     "competitor_locations": {
#         "display_name": "Competitor Locations",
#         "table": "bronze_competitor_locations",
#         "geometry_type": "point",
#         "query": "SELECT competitor_id, brand, lat, lng, open_year FROM {table}",
#         "point_fields": ("lat", "lng"),
#     },
#     "osm_pois": {
#         "display_name": "OSM Points of Interest",
#         "table": "silver_osm_pois",
#         "geometry_type": "point",
#         "query": "SELECT poi_id, name, latitude as lat, longitude as lng, poi_category, poi_subcategory FROM {table} LIMIT 15000",
#         "point_fields": ("lat", "lng"),
#     },
#     "expansion_candidates": {
#         "display_name": "Expansion Candidates",
#         "table": "gold_expansion_candidates",
#         "geometry_type": "point",
#         "query": "SELECT lat, lng, recommended_revenue, recommended_format, urbanicity_category, recommended_rev_per_sqft, total_population, median_household_income FROM {table}",
#         "point_fields": ("lat", "lng"),
#     },
#     "seed_points": {
#         "display_name": "Seed Points",
#         "table": "bronze_seed_points",
#         "geometry_type": "point",
#         "query": "SELECT seed_point_id, latitude as lat, longitude as lng, format, urbanicity_category, ROUND(composite_score, 4) as composite_score, total_population, ROUND(distance_to_nearest_store_miles, 2) as dist_to_store_mi FROM {table}",
#         "point_fields": ("lat", "lng"),
#     },
#     "h3_features": {
#         "display_name": "H3 Hexagons (Res 8)",
#         "table": "silver_h3_features",
#         "geometry_type": "polygon",
#         "query": "SELECT h3_cell_id, ST_AsGeoJSON(h3_geometry) as geojson, urbanicity_category, urbanicity_score, total_population, total_poi_count, total_competitor_count, median_household_income FROM {table} WHERE total_population > 0 LIMIT 50000",
#         "geojson_field": "geojson",
#     },
#     "store_isochrones": {
#         "display_name": "Store Isochrones",
#         "table": "silver_store_isochrones",
#         "geometry_type": "polygon",
#         "query": "SELECT location_id, format, ST_AsGeoJSON(ST_GeomFromWKT(geometry_wkt)) as geojson, drive_time_minutes, urbanicity_category, ROUND(area_sqkm, 1) as area_sqkm FROM {table}",
#         "geojson_field": "geojson",
#     },
#     "competitor_isochrones": {
#         "display_name": "Competitor Isochrones",
#         "table": "silver_competitor_isochrones",
#         "geometry_type": "polygon",
#         "query": "SELECT location_id, format as brand, ST_AsGeoJSON(ST_GeomFromWKT(geometry_wkt)) as geojson, drive_time_minutes, urbanicity_category, ROUND(area_sqkm, 1) as area_sqkm FROM {table}",
#         "geojson_field": "geojson",
#     },
#     "census_blockgroups": {
#         "display_name": "Census Block Groups",
#         "table": "bronze_census_blockgroups",
#         "geometry_type": "polygon",
#         "query": "SELECT geoid, ST_AsGeoJSON(geometry) as geojson, aland, awater FROM {table}",
#         "geojson_field": "geojson",
#     },
#     "census_demographics": {
#         "display_name": "Census Demographics (Bronze)",
#         "table": "bronze_census_demographics",
#         "geometry_type": "point",
#         "query": "SELECT geoid, total_population, median_household_income, latitude as lat, longitude as lng FROM {table} WHERE latitude IS NOT NULL",
#         "point_fields": ("lat", "lng"),
#     },
#     "silver_demographics": {
#         "display_name": "Census Demographics (Silver)",
#         "table": "silver_census_demographics",
#         "geometry_type": "point",
#         "query": "SELECT geoid, total_population, median_household_income, unemployment_rate, transit_share, higher_education_rate, owner_occupied_rate, income_top_coded, income_bottom_coded, latitude as lat, longitude as lng FROM {table} WHERE latitude IS NOT NULL",
#         "point_fields": ("lat", "lng"),
#     },
#     "census_zcta": {
#         "display_name": "Census ZCTAs (Urbanicity)",
#         "table": "silver_census_zcta",
#         "geometry_type": "polygon",
#         "query": """SELECT zcta, ST_AsGeoJSON(ST_Simplify(geometry, 0.005)) as geojson,
#             total_population, ROUND(population_density_sqkm, 1) as population_density_sqkm, median_household_income,
#             CASE
#                 WHEN population_density_sqkm > 5000 THEN 'urban'
#                 WHEN population_density_sqkm > 500 THEN 'suburban'
#                 ELSE 'rural'
#             END as urbanicity_category,
#             ROUND(LEAST(1.0, population_density_sqkm / 10000.0), 4) as urbanicity_score
#             FROM {table}""",
#         "geojson_field": "geojson",
#     },
#     "state_boundary": {
#         "display_name": "State Boundary",
#         "table": "bronze_census_states",
#         "geometry_type": "polygon",
#         "query": "SELECT statefp, name, ST_AsGeoJSON(geometry) as geojson FROM {table} WHERE statefp = '36'",
#         "geojson_field": "geojson",
#     },
# }
#
#
# @api.get("/validation/layers", response_model=list[ValidationLayerInfo], operation_id="listValidationLayers")
# async def list_validation_layers():
#     """List all available validation layers with row counts."""
#     from .data import sql_client as sql
#
#     layers = []
#     for name, config in VALIDATION_LAYERS.items():
#         try:
#             rows = sql.execute_sql(
#                 f"SELECT COUNT(*) as cnt FROM {sql.table(config['table'])}",
#                 cache_key=f"val_count_{name}",
#             )
#             count = int(rows[0]["cnt"]) if rows else 0
#             available = True
#         except Exception:
#             count = 0
#             available = False
#
#         layers.append(ValidationLayerInfo(
#             name=name,
#             display_name=config["display_name"],
#             table_name=config["table"],
#             row_count=count,
#             geometry_type=config["geometry_type"],
#             available=available,
#         ))
#     return layers
#
#
# @api.get("/validation/{layer_name}", response_model=ValidationGeoJSON, operation_id="getValidationLayer")
# async def get_validation_layer(
#     layer_name: str,
#     bbox: Optional[str] = Query(None, description="Bounding box: min_lng,min_lat,max_lng,max_lat"),
# ):
#     """Return GeoJSON for a specific validation layer."""
#     from .data import sql_client as sql
#
#     if layer_name not in VALIDATION_LAYERS:
#         return ValidationGeoJSON(type="FeatureCollection", features=[], layer_name=layer_name, total_count=0)
#
#     config = VALIDATION_LAYERS[layer_name]
#     query = config["query"].format(table=sql.table(config["table"]))
#
#     try:
#         rows = sql.execute_sql(query, cache_key=f"val_data_{layer_name}")
#     except Exception as e:
#         logger.warning(f"Validation layer {layer_name} failed: {e}")
#         return ValidationGeoJSON(type="FeatureCollection", features=[], layer_name=layer_name, total_count=0)
#
#     features = []
#     geom_type = config["geometry_type"]
#
#     for row in rows:
#         props = {k: v for k, v in row.items()}
#         geometry = None
#
#         if geom_type == "point" and "point_fields" in config:
#             lat_field, lng_field = config["point_fields"]
#             lat = row.get(lat_field)
#             lng = row.get(lng_field)
#             if lat is not None and lng is not None:
#                 try:
#                     geometry = {"type": "Point", "coordinates": [float(lng), float(lat)]}
#                     props.pop(lat_field, None)
#                     props.pop(lng_field, None)
#                 except (ValueError, TypeError):
#                     continue
#
#         elif geom_type == "polygon" and "geojson_field" in config:
#             raw = row.get(config["geojson_field"])
#             if raw:
#                 try:
#                     geometry = json.loads(raw) if isinstance(raw, str) else raw
#                     props.pop(config["geojson_field"], None)
#                 except (json.JSONDecodeError, TypeError):
#                     continue
#
#         elif geom_type == "hexagon" and "h3_field" in config:
#             h3_id = row.get(config["h3_field"])
#             if h3_id:
#                 geometry = {"type": "Point", "coordinates": [0, 0]}
#                 props["h3_cell_id"] = h3_id
#
#         if geometry:
#             if bbox and geometry["type"] == "Point":
#                 try:
#                     parts = [float(x) for x in bbox.split(",")]
#                     if len(parts) == 4:
#                         min_lng, min_lat, max_lng, max_lat = parts
#                         coords = geometry["coordinates"]
#                         if not (min_lng <= coords[0] <= max_lng and min_lat <= coords[1] <= max_lat):
#                             continue
#                 except ValueError:
#                     pass
#
#             features.append(ValidationFeature(
#                 type="Feature",
#                 geometry=geometry,
#                 properties=props,
#             ))
#
#     return ValidationGeoJSON(
#         type="FeatureCollection",
#         features=features,
#         layer_name=layer_name,
#         total_count=len(features),
#     )
