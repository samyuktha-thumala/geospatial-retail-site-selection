"""Site Selection Agent — Claude Sonnet 4.6 with tool-calling via FMAPI."""

import json
import logging
import math
import os
from typing import Any

logger = logging.getLogger(__name__)

ENDPOINT_NAME = os.environ.get("SERVING_ENDPOINT", "YOUR_SERVING_ENDPOINT")
MAX_TOOL_ROUNDS = 5

# --- Area resolution: CBSA (primary) → bounding box (fallback for boroughs/neighborhoods) ---

# Map user-friendly area names to CBSA names (from bronze_cbsa_boundaries)
AREA_TO_CBSA: dict[str, str] = {
    "new york": "New York-Newark-Jersey City, NY-NJ",
    "new york city": "New York-Newark-Jersey City, NY-NJ",
    "nyc": "New York-Newark-Jersey City, NY-NJ",
    "new york metro": "New York-Newark-Jersey City, NY-NJ",
    "albany": "Albany-Schenectady-Troy, NY",
    "schenectady": "Albany-Schenectady-Troy, NY",
    "troy": "Albany-Schenectady-Troy, NY",
    "capital region": "Albany-Schenectady-Troy, NY",
    "buffalo": "Buffalo-Cheektowaga, NY",
    "cheektowaga": "Buffalo-Cheektowaga, NY",
    "rochester": "Rochester, NY",
    "syracuse": "Syracuse, NY",
    "utica": "Utica-Rome, NY",
    "rome": "Utica-Rome, NY",
    "binghamton": "Binghamton, NY",
    "ithaca": "Ithaca, NY",
    "poughkeepsie": "Kiryas Joel-Poughkeepsie-Newburgh, NY",
    "newburgh": "Kiryas Joel-Poughkeepsie-Newburgh, NY",
    "hudson valley": "Kiryas Joel-Poughkeepsie-Newburgh, NY",
    "glens falls": "Glens Falls, NY",
    "kingston": "Kingston, NY",
    "elmira": "Elmira, NY",
    "watertown": "Watertown-Fort Drum, NY",
}

# Bounding boxes — ONLY for sub-CBSA areas (boroughs, neighborhoods) that CBSA can't resolve
BBOX_FALLBACK: dict[str, tuple[float, float, float, float]] = {
    # NYC boroughs (all within the NYC CBSA, but users ask by borough)
    "manhattan": (40.6996, 40.8821, -74.0188, -73.9070),
    "brooklyn": (40.5707, 40.7395, -74.0418, -73.8334),
    "queens": (40.5414, 40.8010, -73.9630, -73.7004),
    "bronx": (40.7855, 40.9176, -73.9339, -73.7654),
    "staten island": (40.4961, 40.6490, -74.2557, -74.0522),
    # Neighborhoods
    "harlem": (40.7980, 40.8340, -73.9590, -73.9300),
    "lower manhattan": (40.6996, 40.7400, -74.0188, -73.9700),
    "midtown": (40.7480, 40.7680, -73.9950, -73.9680),
    "upper east side": (40.7600, 40.7850, -73.9700, -73.9400),
    "upper west side": (40.7700, 40.8020, -73.9900, -73.9600),
    "williamsburg": (40.7000, 40.7250, -73.9700, -73.9350),
    # Non-CBSA regions
    "long island": (40.5800, 41.1600, -73.7004, -71.8560),
    "westchester": (40.8800, 41.3700, -73.9800, -73.4800),
    "upstate": (42.0000, 45.0000, -79.8000, -73.2400),
    "downstate": (40.4961, 41.3700, -74.2557, -71.8560),
    "finger lakes": (42.4000, 43.3000, -77.6000, -76.3000),
    "western ny": (42.0000, 43.3000, -79.8000, -77.5000),
}


def _resolve_area_filter(area: str) -> list[str]:
    """Resolve a user area string to SQL WHERE clauses.

    Priority: CBSA match → zip code → bounding box fallback.
    Returns a list of SQL conditions.
    """
    area_lower = area.lower().strip()
    clauses: list[str] = []

    # 1. Try CBSA match
    for key, cbsa_name in AREA_TO_CBSA.items():
        if key in area_lower:
            clauses.append(f"cbsa_name = '{cbsa_name}'")
            return clauses

    # 2. Try zip code (user typed a 5-digit number)
    import re
    zip_match = re.search(r"\b(\d{5})\b", area)
    if zip_match:
        clauses.append(f"zip_code = '{zip_match.group(1)}'")
        return clauses

    # 3. Bounding box fallback (boroughs, neighborhoods)
    for key, bbox in BBOX_FALLBACK.items():
        if key in area_lower:
            min_lat, max_lat, min_lng, max_lng = bbox
            clauses.append(f"lat BETWEEN {min_lat} AND {max_lat}")
            clauses.append(f"lng BETWEEN {min_lng} AND {max_lng}")
            return clauses

    # 4. No match — return empty (will search all data)
    return clauses


def _get_system_prompt(page_context: str) -> str:
    base = """You are a Site Selection AI Agent for a retail chain operating in New York State. You help analysts with network analysis and expansion planning.

IMPORTANT SCOPE: All data is restricted to New York State. When the user says "NYC" they mean New York City (the five boroughs: Manhattan, Brooklyn, Queens, Bronx, Staten Island). Do NOT ask about which metro area — you already know the geography. Available areas include NYC boroughs, Long Island, Westchester, Hudson Valley, Albany, Buffalo, Rochester, Syracuse, and other NY State cities/regions.

You have access to tools that let you:
1. Run SQL queries directly against the catalog tables
2. Search expansion candidate sites with structured filters
3. Run expansion optimization scenarios

AVAILABLE TABLES (all in Unity Catalog, scoped to New York State):

1. bronze_store_locations — our retail stores
   Columns: store_number, name, format (express/standard/flagship), lat, lng, city, state, address, urbanicity, zip_code, cbsa_code, cbsa_name

2. bronze_competitor_locations — competitor stores
   Columns: competitor_id, brand (Competitor A-E), lat, lng, open_year, is_projected, urbanicity, zip_code, cbsa_code, cbsa_name

3. gold_store_sales — monthly revenue per store (12 months)
   Columns: location_id, jan_sales..dec_sales, annual_revenue
   JOIN to stores: CAST(store_number AS STRING) = location_id

4. gold_store_features_and_sales — store features + sales combined
   Columns: location_id, annual_revenue, plus demographic/POI features

5. gold_expansion_candidates — scored expansion candidate sites
   Columns: location_id, lat, lng, recommended_format, recommended_revenue, urbanicity_category, total_poi_count, total_competitor_count, total_population, median_household_income, distance_to_nearest_store_miles, zip_code, cbsa_code, cbsa_name

6. silver_h3_features — H3 hex-level features (res 8)
   Columns: h3_cell_id, total_poi_count, poi_count_shop, poi_count_amenity, total_population, median_household_income, urbanicity_category, total_competitor_count

SQL GUIDELINES:
- ALWAYS include lat, lng in your SELECT so results appear on the map. The map is the primary output.
- ALWAYS include `name` as the first column for stores (the frontend uses it as the card title).
- ALWAYS use LIMIT (max 50 rows).
- NEVER produce duplicate rows. If joining tables, ensure your query returns exactly one row per entity.
- For store queries: SELECT s.name, s.lat, s.lng, s.format, s.city, s.urbanicity, s.zip_code, ROUND(g.annual_revenue) as annual_revenue FROM bronze_store_locations s JOIN gold_store_sales g ON CAST(s.store_number AS STRING) = g.location_id
- Use cbsa_name for metro-area filtering (e.g., WHERE cbsa_name = 'New York-Newark-Jersey City, NY-NJ')
- Use zip_code for zip-level filtering
- Monthly sales values in gold_store_sales are in raw dollars. annual_revenue is also raw dollars.
- For revenue display: divide by 1000000 for millions (e.g., 7100000 → $7.1M)

BEHAVIOR RULES:
- When the user names a specific area (e.g., "expand in NYC", "sites in Brooklyn"), act immediately — search and show results. Do NOT ask which metro area.
- Only ask clarifying questions when truly ambiguous — e.g., "How many sites?" or "Any format preference?" Keep it to 1-2 questions max.
- When the user refines ("remove sites near Competitor B", "prioritize POI density"), apply the filter and re-search. Accumulate constraints from prior turns.
- Keep your text response SHORT (2-3 sentences max). Do NOT output markdown tables — the frontend renders styled result cards from the query data automatically. Just give a brief insight.
- Format revenue in dollars (e.g., $7.1M, $450K)
"""

    if page_context == "network":
        base += """
CURRENT CONTEXT: Network Diagnostics page
Focus on: store performance analysis, at-risk locations, competitor landscape, revenue trends across NY State.
The user is looking at their existing network and wants to understand performance drivers.
Use query_sql to answer performance, ranking, and comparison questions. Always include lat, lng so results appear on the map.
Use show_trade_area when the user asks to see a store's trade area, catchment, hex data, or surrounding demographics. Example: "show me the trade area for store #1246" → call show_trade_area("1246").
"""
    elif page_context == "expansion":
        base += """
CURRENT CONTEXT: Site Playground — Expansion Planning (New York State)
Focus on: finding new store sites in NY State, running scenarios, comparing expansion options.
When the user says a city name (NYC, Brooklyn, Albany, etc.), immediately search for candidates in that area. Don't ask which state or metro — it's always NY.
Use search_expansion_sites for structured candidate search, or query_sql for custom analysis.

CUMULATIVE STATE — CRITICAL:
The user's message may contain a [WORKING_SET: ...] block with location IDs from the previous search.
When this is present, these IDs are the ACTIVE result set. All refinement questions operate on THIS set only:
- "Remove sites near Competitor B" → filter the working set, NOT the full table
- "Prioritize POI density" → re-rank the working set
- "Show only suburban" → filter the working set by urbanicity
To filter the working set, pass the location_ids parameter to search_expansion_sites, or use WHERE location_id IN (...) in query_sql.
Only do a FULL search (ignoring the working set) when the user explicitly asks for new/different locations (e.g., "now search in Buffalo", "start over", "find new sites").
"""

    return base


TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_sql",
            "description": "Execute a read-only SQL SELECT query against the site selection catalog tables. Use this to answer questions about store performance, competitors, sales, demographics, etc. Always include lat and lng columns when results should appear on the map. Returns formatted results as a markdown table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "A SELECT SQL query. Use the table names directly (e.g., bronze_store_locations) — they will be auto-qualified with catalog.schema.",
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Brief explanation of what this query answers.",
                    },
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_expansion_sites",
            "description": "Search for expansion candidate sites in New York State with geographic and feature filters. Returns scored candidates with lat/lng, projected revenue, format recommendation, POI density, demographics. Results are shown on the map automatically.",
            "parameters": {
                "type": "object",
                "properties": {
                    "area": {
                        "type": "string",
                        "description": "City, borough, region, or zip code in NY State. Examples: 'NYC', 'Manhattan', 'Brooklyn', 'Albany', 'Buffalo', '10001'. For metro areas uses CBSA; for boroughs/neighborhoods uses geographic boundaries.",
                    },
                    "format": {
                        "type": "string",
                        "enum": ["express", "standard", "flagship"],
                        "description": "Filter by recommended store format",
                    },
                    "urbanicity": {
                        "type": "string",
                        "enum": ["urban", "suburban", "rural"],
                        "description": "Filter by urbanicity category",
                    },
                    "min_distance_from_store_miles": {
                        "type": "number",
                        "description": "Minimum distance from any existing store (miles)",
                    },
                    "min_poi_count": {
                        "type": "integer",
                        "description": "Minimum POI count (higher = more commercial activity)",
                    },
                    "min_income": {
                        "type": "number",
                        "description": "Minimum median household income",
                    },
                    "max_competitor_count": {
                        "type": "integer",
                        "description": "Maximum number of competitors nearby",
                    },
                    "rank_by": {
                        "type": "string",
                        "enum": ["revenue", "poi_density", "income", "distance_to_store"],
                        "description": "How to rank results. Default: revenue",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return. Default: 10",
                    },
                    "location_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "If refining a previous result set, pass these location IDs to filter within. Only these candidates will be considered.",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_expansion_scenario",
            "description": "Run the greedy expansion optimizer to select revenue-maximizing new store locations. Applies distance constraints between new sites and the existing network. Returns optimized locations with projected revenue and scenario summary.",
            "parameters": {
                "type": "object",
                "properties": {
                    "min_distance_from_network_miles": {
                        "type": "number",
                        "description": "Min distance from existing stores (miles). Default: 2.0",
                    },
                    "min_distance_between_new_miles": {
                        "type": "number",
                        "description": "Min distance between new locations (miles). Default: 3.0",
                    },
                    "final_locations_count": {
                        "type": "integer",
                        "description": "Number of new locations to select. Default: 10",
                    },
                },
                "required": [],
            },
        },
    },
]


def _get_workspace_client():
    from databricks.sdk import WorkspaceClient

    if os.environ.get("DATABRICKS_APP_NAME"):
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
    return WorkspaceClient(profile=profile)


def _call_fmapi(w, messages: list[dict], tools: list[dict] | None = None) -> dict:
    """Call FMAPI endpoint with raw HTTP to support tools parameter."""
    body: dict[str, Any] = {"messages": messages, "max_tokens": 4096}
    if tools:
        body["tools"] = tools
    return w.api_client.do(
        "POST",
        f"/serving-endpoints/{ENDPOINT_NAME}/invocations",
        body=body,
    )


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------


MAX_RESULT_ROWS = 50

# Tables the agent is allowed to query
_KNOWN_TABLES = [
    "bronze_store_locations", "bronze_competitor_locations",
    "gold_store_sales", "gold_store_features_and_sales",
    "gold_expansion_candidates", "gold_simulated_competitor_growth",
    "silver_h3_features", "silver_zcta_cbsa", "bronze_census_zcta",
]


def _tool_query_sql(args: dict) -> tuple[str, list[dict]]:
    from . import sql_client as sql

    query = args.get("sql", "").strip()
    reasoning = args.get("reasoning", "")

    # Safety: only SELECT
    if not query.upper().startswith("SELECT") and not query.upper().startswith("WITH"):
        return "ERROR: Only SELECT queries are allowed.", []

    # Auto-qualify table names with catalog.schema
    qualified = query
    for t in _KNOWN_TABLES:
        fq = sql.table(t)
        if fq not in qualified:
            qualified = qualified.replace(t, fq)

    # Enforce row limit
    if "LIMIT" not in qualified.upper():
        qualified = qualified.rstrip().rstrip(";") + f" LIMIT {MAX_RESULT_ROWS}"

    if reasoning:
        logger.info(f"Agent SQL ({reasoning}): {qualified[:200]}")

    try:
        rows = sql.execute_sql(qualified)
    except Exception as e:
        return f"SQL error: {str(e)[:300]}", []

    if not rows:
        return "Query returned no results.", []

    rows = rows[:MAX_RESULT_ROWS]

    # Build markdown table
    columns = list(rows[0].keys())
    lines = ["| " + " | ".join(columns) + " |"]
    lines.append("| " + " | ".join(["---"] * len(columns)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(c, "")) for c in columns) + " |")

    # Extract map points if lat/lng present
    map_points: list[dict] = []
    col_lower = [c.lower() for c in columns]
    lat_idx = next((i for i, c in enumerate(col_lower) if c in ("lat", "latitude")), None)
    lng_idx = next((i for i, c in enumerate(col_lower) if c in ("lng", "longitude", "lon")), None)

    if lat_idx is not None and lng_idx is not None:
        # Find a label column — strongly prefer "name"
        label_priority = ["name", "store_name", "location_name", "location_id", "competitor_id", "brand"]
        label_idx = next(
            (i for i, c in enumerate(col_lower) if c in label_priority),
            next((i for i in range(len(columns)) if i != lat_idx and i != lng_idx), None),
        )
        # Known property fields the frontend cards understand
        card_fields = {"name", "format", "city", "urbanicity", "zip_code", "cbsa_name",
                       "annual_revenue", "projected_revenue", "recommended_revenue", "recommended_format",
                       "total_poi_count", "urbanicity_category", "monthly_sales",
                       "store_number", "location_id", "brand", "closure_risk"}
        for row in rows:
            try:
                lat = float(row[columns[lat_idx]])
                lng = float(row[columns[lng_idx]])
                label = str(row[columns[label_idx]]) if label_idx is not None else ""
                # Only include known fields in properties to avoid garbage in cards
                props = {}
                for c in columns:
                    cl = c.lower()
                    if cl in ("lat", "lng", "latitude", "longitude"):
                        continue
                    if cl in card_fields or cl.replace(" ", "_") in card_fields:
                        props[cl] = row[c]
                map_points.append({"lat": lat, "lng": lng, "label": label, "properties": props})
            except (ValueError, TypeError):
                continue

    # Deduplicate map points by lat/lng
    seen = set()
    unique_points: list[dict] = []
    for pt in map_points:
        key = (round(pt["lat"], 6), round(pt["lng"], 6))
        if key not in seen:
            seen.add(key)
            unique_points.append(pt)

    # If we have map points, return just the data summary (frontend renders cards).
    # If no map points (non-spatial query), return the full table.
    if unique_points:
        result_text = f"{len(unique_points)} results shown on map."
    else:
        result_text = f"**{len(rows)} results:**\n\n" + "\n".join(lines)

    return result_text, unique_points


def _tool_search_expansion_sites(args: dict) -> tuple[str, list[dict]]:
    from . import sql_client as sql

    area = args.get("area", "")
    fmt = args.get("format")
    urbanicity = args.get("urbanicity")
    min_dist_store = args.get("min_distance_from_store_miles")
    min_poi = args.get("min_poi_count")
    min_income = args.get("min_income")
    max_comp = args.get("max_competitor_count")
    rank_by = args.get("rank_by", "revenue")
    limit = args.get("limit", 10)

    # If refining a working set, filter by those IDs first
    location_ids = args.get("location_ids")
    where: list[str] = []
    if location_ids:
        placeholders = ", ".join(f"'{lid}'" for lid in location_ids)
        where.append(f"location_id IN ({placeholders})")

    # Resolve area to SQL filters (CBSA → zip → bbox fallback)
    if area and not location_ids:
        where.extend(_resolve_area_filter(area))

    if fmt:
        where.append(f"recommended_format = '{fmt}'")
    if urbanicity:
        where.append(f"urbanicity_category = '{urbanicity}'")
    if min_dist_store is not None:
        where.append(f"distance_to_nearest_store_miles >= {min_dist_store}")
    if min_poi is not None:
        where.append(f"total_poi_count >= {min_poi}")
    if min_income is not None:
        where.append(f"median_household_income >= {min_income}")
    if max_comp is not None:
        where.append(f"total_competitor_count <= {max_comp}")

    order_map = {
        "revenue": "recommended_revenue DESC",
        "poi_density": "total_poi_count DESC",
        "income": "median_household_income DESC",
        "distance_to_store": "distance_to_nearest_store_miles DESC",
    }
    order = order_map.get(rank_by, "recommended_revenue DESC")
    where_clause = " AND ".join(where) if where else "1=1"

    query = f"""
    SELECT location_id, lat, lng, recommended_format, urbanicity_category,
           zip_code, cbsa_name,
           ROUND(recommended_revenue, 0) as projected_revenue,
           total_poi_count, total_competitor_count, total_population,
           ROUND(median_household_income, 0) as median_hhi,
           ROUND(distance_to_nearest_store_miles, 2) as dist_to_store_mi
    FROM {sql.table('gold_expansion_candidates')}
    WHERE {where_clause}
    ORDER BY {order}
    LIMIT {limit}
    """

    try:
        rows = sql.execute_sql(query)
    except Exception as e:
        return f"Error querying expansion candidates: {str(e)[:200]}", []

    if not rows:
        return f"No expansion candidates found with those filters. Try broadening your criteria.", []

    area_label = area.strip() or "all of NY"

    map_points: list[dict] = []
    for i, row in enumerate(rows):
        lat = float(row.get("lat") or 0)
        lng = float(row.get("lng") or 0)
        rev = int(row.get("projected_revenue") or 0)
        if lat and lng:
            map_points.append(
                {
                    "lat": lat,
                    "lng": lng,
                    "label": row.get("location_id", f"Site {i + 1}"),
                    "properties": {
                        "format": row.get("recommended_format"),
                        "projected_revenue": rev,
                        "total_poi_count": row.get("total_poi_count"),
                        "urbanicity": row.get("urbanicity_category"),
                        "zip_code": row.get("zip_code"),
                        "cbsa_name": row.get("cbsa_name"),
                        "median_hhi": row.get("median_hhi"),
                        "type": "expansion_candidate",
                    },
                }
            )

    return f"**{len(map_points)} expansion candidates in {area_label}** shown on map.", map_points


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.asin(math.sqrt(a))


def _tool_run_expansion_scenario(args: dict) -> tuple[str, list[dict]]:
    from .store import db

    min_net = args.get("min_distance_from_network_miles", 2.0)
    min_new = args.get("min_distance_between_new_miles", 3.0)
    count = args.get("final_locations_count", 10)

    candidates = sorted(db.hotspots, key=lambda h: h.score, reverse=True)
    existing = [(loc.lat, loc.lng) for loc in db.locations]

    selected: list[dict] = []
    for hs in candidates:
        if len(selected) >= count:
            break
        if any(_haversine(hs.lat, hs.lng, lat, lng) < min_net for lat, lng in existing):
            continue
        if any(_haversine(hs.lat, hs.lng, s["lat"], s["lng"]) < min_new for s in selected):
            continue
        fmt_val = hs.format.value if hasattr(hs.format, "value") else str(hs.format)
        selected.append(
            {
                "id": f"OPT{len(selected) + 1:03d}",
                "lat": hs.lat,
                "lng": hs.lng,
                "format": fmt_val,
                "projected_revenue": round(hs.projected_sales * 12, 1),
                "score": hs.score,
            }
        )

    total_rev = sum(s["projected_revenue"] for s in selected)
    total_str = f"${total_rev / 1_000:.1f}M" if total_rev >= 1_000 else f"${total_rev:.0f}K"

    map_points: list[dict] = []
    for s in selected:
        map_points.append(
            {
                "lat": s["lat"],
                "lng": s["lng"],
                "label": s["id"],
                "properties": {
                    "format": s["format"],
                    "projected_revenue": s["projected_revenue"],
                    "score": s["score"],
                    "type": "optimized_location",
                },
            }
        )

    return f"**Expansion scenario: {len(selected)} locations, {total_str} projected revenue.** Shown on map.", map_points


SHOW_TRADE_AREA_TOOL = {
    "type": "function",
    "function": {
        "name": "show_trade_area",
        "description": "Display the H3 hexagonal trade area for a specific store, showing demographics, POI density, and competition in each hex cell around the store's isochrone. Use when the user asks to see a store's trade area, catchment, or surrounding demographics.",
        "parameters": {
            "type": "object",
            "properties": {
                "store_id": {
                    "type": "string",
                    "description": "Store identifier, e.g., 'LOC1246' or '1246'. Will be normalized to LOCxxxx format.",
                },
            },
            "required": ["store_id"],
        },
    },
}


def _tool_show_trade_area(args: dict) -> tuple[str, list[dict]]:
    store_id = str(args.get("store_id", ""))
    # Normalize: if just a number or has # prefix, convert to LOCxxxx
    store_id = store_id.replace("#", "").strip()
    if store_id.isdigit():
        store_id = f"LOC{store_id}"
    elif not store_id.upper().startswith("LOC"):
        store_id = f"LOC{store_id}"
    else:
        store_id = store_id.upper()

    # Return a special marker that the frontend will interpret as an H3 trigger
    return f"Showing H3 trade area for {store_id}.", [
        {"lat": 0, "lng": 0, "label": store_id, "properties": {"type": "h3_trigger", "store_id": store_id}}
    ]


def _execute_tool(name: str, args: dict) -> tuple[str, list[dict]]:
    dispatch = {
        "query_sql": _tool_query_sql,
        "search_expansion_sites": _tool_search_expansion_sites,
        "run_expansion_scenario": _tool_run_expansion_scenario,
        "show_trade_area": _tool_show_trade_area,
    }
    fn = dispatch.get(name)
    if not fn:
        return f"Unknown tool: {name}", []
    try:
        return fn(args)
    except Exception as e:
        logger.error(f"Tool {name} failed: {e}", exc_info=True)
        return f"Tool error: {str(e)[:300]}", []


# ---------------------------------------------------------------------------
# Agent loop
# ---------------------------------------------------------------------------


def _extract_text(content: Any) -> str:
    """Extract plain text from FMAPI content (may be str or list of blocks)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            b.get("text", "") if isinstance(b, dict) and b.get("type") == "text" else str(b)
            for b in content
        )
    return str(content) if content else ""


def run_agent(
    message: str,
    history: list[dict] | None = None,
    page_context: str = "expansion",
) -> dict:
    """Run the multi-turn agent loop and return the final response."""
    w = _get_workspace_client()

    messages: list[dict] = [{"role": "system", "content": _get_system_prompt(page_context)}]
    if history:
        for h in history:
            messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": message})

    all_map_points: list[dict] = []

    # Build tool list based on page context
    active_tools = list(TOOLS)
    if page_context == "network":
        active_tools.append(SHOW_TRADE_AREA_TOOL)

    for _ in range(MAX_TOOL_ROUNDS):
        try:
            resp = _call_fmapi(w, messages, active_tools)
        except Exception as e:
            logger.error(f"FMAPI call failed: {e}", exc_info=True)
            return {
                "response": f"Error reaching the AI model. {str(e)[:200]}",
                "map_points": all_map_points,
                "suggestions": [],
            }

        choices = resp.get("choices")
        if not choices:
            return {"response": "No response from the model.", "map_points": all_map_points, "suggestions": []}

        assistant_msg = choices[0]["message"]
        tool_calls = assistant_msg.get("tool_calls")

        if not tool_calls:
            return {
                "response": _extract_text(assistant_msg.get("content", "")),
                "map_points": all_map_points,
                "suggestions": _suggestions(message, page_context),
            }

        # Append assistant message (with tool_calls) to conversation
        messages.append(assistant_msg)

        # Execute tools
        for tc in tool_calls:
            tc_id = tc["id"]
            fn = tc["function"]
            fn_name = fn["name"]
            try:
                fn_args = json.loads(fn["arguments"]) if isinstance(fn["arguments"], str) else fn["arguments"]
            except (json.JSONDecodeError, TypeError):
                fn_args = {}

            logger.info(f"Agent tool: {fn_name}({fn_args})")
            text_result, points = _execute_tool(fn_name, fn_args)
            all_map_points.extend(points)

            messages.append({"role": "tool", "tool_call_id": tc_id, "content": text_result})

    return {
        "response": "I've completed several rounds of analysis. Let me know if you'd like to refine further.",
        "map_points": all_map_points,
        "suggestions": _suggestions(message, page_context),
    }


def _suggestions(message: str, ctx: str) -> list[str]:
    m = message.lower()
    if ctx == "expansion":
        if any(w in m for w in ["expand", "site", "new", "open", "candidate"]):
            return [
                "Rank these by POI density instead",
                "Remove sites near Competitor B",
                "Show me suburban options",
            ]
        return [
            "Expand in NYC — top 10 sites",
            "Suburban expansion in Westchester",
            "Best sites in Brooklyn by POI density",
        ]
    # network
    if any(w in m for w in ["perform", "revenue", "top", "best"]):
        return [
            "What drives urban store performance?",
            "Compare express vs flagship revenue",
            "Bottom 10 underperformers",
        ]
    return [
        "Top 10 stores by revenue",
        "Store performance in Manhattan",
        "Competitor density in Brooklyn",
    ]
