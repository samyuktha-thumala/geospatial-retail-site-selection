"""Site Selection Agent — Claude Sonnet 4.6 with tool-calling via FMAPI."""

import json
import logging
import math
import os
from typing import Any

logger = logging.getLogger(__name__)

from ..config import conf
ENDPOINT_NAME = conf.serving_endpoint
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


# Approximate bounding boxes for CBSA metro areas (lat/lng only, no cbsa_name column dependency)
_CBSA_BBOXES: dict[str, tuple[float, float, float, float]] = {
    "New York-Newark-Jersey City, NY-NJ": (40.4961, 41.3700, -74.2557, -73.2400),
    "Albany-Schenectady-Troy, NY": (42.4000, 43.0000, -74.3000, -73.5000),
    "Buffalo-Cheektowaga, NY": (42.7000, 43.1000, -79.1000, -78.5000),
    "Rochester, NY": (42.9000, 43.4000, -77.9000, -77.2000),
    "Syracuse, NY": (42.8000, 43.3000, -76.5000, -75.8000),
    "Utica-Rome, NY": (42.9000, 43.4000, -75.8000, -75.0000),
    "Binghamton, NY": (42.0000, 42.4000, -76.3000, -75.5000),
    "Ithaca, NY": (42.3000, 42.6000, -76.8000, -76.3000),
    "Kiryas Joel-Poughkeepsie-Newburgh, NY": (41.2000, 41.8000, -74.5000, -73.7000),
    "Glens Falls, NY": (43.0000, 43.6000, -74.0000, -73.3000),
    "Kingston, NY": (41.7000, 42.2000, -74.5000, -73.8000),
    "Elmira, NY": (42.0000, 42.3000, -77.1000, -76.6000),
    "Watertown-Fort Drum, NY": (43.5000, 44.2000, -76.3000, -75.5000),
}


def _resolve_area_filter(area: str) -> list[str]:
    """Resolve a user area string to SQL WHERE clauses using lat/lng bounding boxes.

    Priority: CBSA name → bbox lookup → borough/region bbox fallback.
    All filters use lat/lng bounds (no dependency on cbsa_name or zip_code columns).
    """
    area_lower = area.lower().strip()
    clauses: list[str] = []

    # 1. Try CBSA match → convert to bounding box via known metro areas
    for key, cbsa_name in AREA_TO_CBSA.items():
        if key in area_lower:
            # Map CBSA names to approximate bounding boxes
            cbsa_bbox = _CBSA_BBOXES.get(cbsa_name)
            if cbsa_bbox:
                min_lat, max_lat, min_lng, max_lng = cbsa_bbox
                clauses.append(f"lat BETWEEN {min_lat} AND {max_lat}")
                clauses.append(f"lng BETWEEN {min_lng} AND {max_lng}")
                return clauses
            break  # matched key but no bbox — fall through

    # 2. Bounding box lookup (boroughs, neighborhoods, regions)
    for key, bbox in BBOX_FALLBACK.items():
        if key in area_lower:
            min_lat, max_lat, min_lng, max_lng = bbox
            clauses.append(f"lat BETWEEN {min_lat} AND {max_lat}")
            clauses.append(f"lng BETWEEN {min_lng} AND {max_lng}")
            return clauses

    # 3. No match — return empty (will search all data)
    return clauses


def _get_system_prompt(page_context: str, benchmark: str | None = None, closure_candidates: list[str] | None = None) -> str:
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
   Columns: location_id, lat, lng, recommended_format, recommended_revenue, urbanicity_category, total_poi_count, total_competitor_count, total_population, median_household_income, distance_to_nearest_store_miles

6. silver_h3_features — H3 hex-level features (res 8)
   Columns: h3_cell_id, total_poi_count, poi_count_shop, poi_count_amenity, total_population, median_household_income, urbanicity_category, total_competitor_count

SQL GUIDELINES:
- Include lat, lng in your SELECT when the user asks about specific locations that should appear on the map (e.g., "top 10 stores", "show stores in Brooklyn").
- Do NOT include lat, lng for aggregate/summary queries (e.g., "what drives performance", "average revenue by format", "compare top vs bottom half"). These are analytical insights, not map results.
- ALWAYS include `name` as the first column for store-level queries (the frontend uses it as the card title).
- ALWAYS use LIMIT (max 50 rows).
- NEVER produce duplicate rows. If joining tables, ensure your query returns exactly one row per entity.
- The primary table for store analysis is gold_store_features_and_sales — it has location_id, format, annual_revenue, all demographic features, and monthly sales. JOIN to bronze_store_locations for name, city, zip_code.
- For store queries: SELECT s.name, s.lat, s.lng, s.format, s.city, s.urbanicity, s.zip_code, ROUND(g.annual_revenue) as annual_revenue FROM gold_store_features_and_sales g JOIN bronze_store_locations s ON CAST(s.store_number AS STRING) = g.location_id
- For geographic filtering, use lat/lng bounding boxes — the search_expansion_sites tool handles area resolution automatically
- annual_revenue is in raw dollars. For display: divide by 1000000 for millions (e.g., 7100000 → $7.1M)
- For performance driver analysis (top X% vs bottom X%), use a SINGLE query with NTILE to partition stores, then aggregate features per group. Do NOT run multiple queries — do it in ONE query with a CTE. Example pattern:
  WITH ranked AS (SELECT *, NTILE(4) OVER (ORDER BY annual_revenue DESC) as quartile FROM gold_store_features_and_sales)
  SELECT quartile, COUNT(*) as stores, ROUND(AVG(annual_revenue)/1e6, 1) as avg_rev_m, ROUND(AVG(median_household_income)) as avg_hhi, ROUND(AVG(total_poi_count)) as avg_poi, ROUND(AVG(total_competitor_count), 1) as avg_comp, ROUND(AVG(total_population)) as avg_pop FROM ranked GROUP BY quartile ORDER BY quartile

BEHAVIOR RULES:
- When the user names a specific area (e.g., "expand in NYC", "sites in Brooklyn"), act immediately — search and show results. Do NOT ask which metro area.
- Only ask clarifying questions when truly ambiguous — e.g., "How many sites?" or "Any format preference?" Keep it to 1-2 questions max.
- When the user refines ("remove sites near Competitor B", "prioritize POI density"), apply the filter and re-search. Accumulate constraints from prior turns.
- CRITICAL: Call ONE tool per question, then summarize the results. Do NOT chain multiple tool calls trying to refine. If the user wants refinement, they will ask.
- Keep your text response SHORT and scannable (2-3 sentences). The frontend renders styled result cards from map_points automatically — your text should just summarize, not repeat the data.
- For analytical queries (performance drivers, comparisons), use bullet points — NEVER use markdown tables (they render poorly in this UI).
- Format revenue in dollars (e.g., $7.1M, $450K)
- Do NOT use emoji in responses.
"""

    if page_context == "network":
        base += """
CURRENT CONTEXT: Network Diagnostics page
Focus on: store performance analysis, at-risk locations, competitor landscape, revenue trends across NY State.
The user is looking at their existing network and wants to understand performance drivers.

TOOL SELECTION:
- Use get_store_performance when the user asks about a specific store's performance, revenue, or drivers (e.g., "how is store #1246 doing?", "performance of location 1001"). It returns a full breakdown AND auto-loads hex visualization.
- Use compare_stores when the user asks to compare two specific stores (e.g., "compare store #1108 vs #1001", "how does #1246 stack up against #1789"). It returns side-by-side analysis AND enables hex toggling.
- Use query_sql for:
  a) RANKING queries ("top 10 performers", "bottom 5 stores"): Include lat, lng, name. Use ROW_NUMBER(). Return individual stores on map.
  b) DRIVER/ANALYSIS queries ("what drives top 20%", "revenue drivers", "what separates top from bottom"): This is an AGGREGATE analysis — do NOT return individual stores. Use ONE query with NTILE to split stores into groups, then compare average features. Return text insights with bullet points, NO map points. Use the NTILE pattern from SQL GUIDELINES.
  CRITICAL: "revenue drivers" or "performance drivers" means ANALYZE what features correlate with high revenue (aggregate comparison). It does NOT mean "list the top stores."
- Use show_trade_area when the user asks to see a store's trade area, catchment, hex data, or surrounding demographics.

BENCHMARK SUMMARY:
When you analyze performance drivers (e.g., "what drives top 50% performance", "what separates top quartile"), ALWAYS end your response with a single-line summary on its own line, prefixed with [BENCHMARK]:
Example: [BENCHMARK] Look for: Suburban/rural flagship, HHI > $104K, POI > 830, competitors < 2, pop > 66K
This line should be a concise, actionable checklist of what to look for when evaluating new sites. Use the actual numbers from your analysis. Keep it under 30 words after "Look for:".
"""
    elif page_context == "expansion":
        base += """
CURRENT CONTEXT: Site Playground — Expansion Planning (New York State)
Focus on: finding new store sites in NY State, running scenarios, comparing expansion options.
When the user says a city name (NYC, Brooklyn, Albany, etc.), immediately search for candidates in that area. Don't ask which state or metro — it's always NY.

TOOL SELECTION:
- Use search_expansion_sites for finding candidate locations with filters (area, format, urbanicity, income, POI, competitors)
- Use run_scenario to run a full expansion optimization scenario. Use when user says "run a scenario", "optimize", "find best new locations", "drop at risk", etc.
- Use query_sql for custom analytical queries

SCENARIO INSTRUCTIONS:
- When the user asks to run a scenario without specifying all parameters, ask 1-2 clarifying questions: how many locations? any constraints (drop at-risk, urbanicity focus, competitor year)?
- "Drop at risk" or "exclude at-risk stores" → pass the CLOSURE CANDIDATES IDs to excluded_closure_risks
- "Anchor competition to 2028" or "set competitor year to 2028" → set competitor_year=2028
- "Focus on urban" → set urbanicity_focus="urban"
- After running a scenario, briefly describe the steps you took and the results

CUMULATIVE STATE — CRITICAL:
The user's message may contain a [WORKING_SET: ...] block with location IDs from the previous search.
When this is present, these IDs are the ACTIVE result set. All refinement questions operate on THIS set only.
Only do a FULL search (ignoring the working set) when the user explicitly asks for new/different locations.
"""

    # Inject benchmark if available
    if benchmark:
        base += f"""
PERFORMANCE BENCHMARK (from network diagnostics analysis):
{benchmark}
When the user asks to 'apply benchmarks', 'show locations that meet the benchmarks', 'best locations for expansion', or similar:
- Do NOT apply all benchmark criteria as strict filters — that returns too few results.
- Instead, apply the 1-2 most important criteria as filters (e.g., min_income for the HHI threshold) and use ORDER BY recommended_revenue DESC to rank. The goal is ~30-50 results.
- If using search_expansion_sites, pick at most 2 filters. For example: min_income=94000 and urbanicity="suburban" — leave POI/competitors unfiltered.
- If no specific area is mentioned, search across all of NY State.
"""

    # Inject closure candidates if available
    if closure_candidates:
        cc_list = ", ".join(closure_candidates)
        base += f"\nCLOSURE CANDIDATES (at-risk store IDs): [{cc_list}]\nWhen the user says 'drop at risk' or 'exclude at-risk stores', pass these IDs to excluded_closure_risks in run_scenario.\n"

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
                        "description": "Max results to return. Default: 50",
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
]

RUN_SCENARIO_TOOL = {
    "type": "function",
    "function": {
        "name": "run_scenario",
        "description": "Run a full expansion optimization scenario. Selects revenue-maximizing new store locations with distance constraints. Creates a scenario in the UI panel. Use when the user asks to run a scenario, optimize, or find best new locations.",
        "parameters": {
            "type": "object",
            "properties": {
                "competitor_year": {"type": "integer", "description": "Competitor landscape year (2025-2030). Default: 2025"},
                "final_locations_count": {"type": "integer", "description": "Number of new locations. Default: 10"},
                "excluded_closure_risks": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Store IDs of at-risk locations to exclude from network (e.g., ['LOC1042', 'LOC1108']). Use when user says 'drop at risk'.",
                },
                "urbanicity_focus": {
                    "type": "string", "enum": ["urban", "suburban", "rural"],
                    "description": "Focus on a specific urbanicity type. Optional.",
                },
                "min_distance_from_network_urban": {"type": "number", "description": "Miles. Default: 1.5"},
                "min_distance_from_network_suburban": {"type": "number", "description": "Miles. Default: 3.0"},
                "min_distance_from_network_rural": {"type": "number", "description": "Miles. Default: 5.0"},
                "min_distance_between_new_urban": {"type": "number", "description": "Miles. Default: 2.0"},
                "min_distance_between_new_suburban": {"type": "number", "description": "Miles. Default: 5.0"},
                "min_distance_between_new_rural": {"type": "number", "description": "Miles. Default: 8.0"},
            },
            "required": [],
        },
    },
}


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
    "gold_store_trade_area_features",
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
                       "store_number", "location_id", "brand", "closure_risk", "rank"}
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
    limit = args.get("limit", 50)

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


def _urbanicity_matches(urbanicity, focus: str) -> bool:
    u = urbanicity.value if hasattr(urbanicity, "value") else str(urbanicity)
    if focus == "urban":
        return u in ("urban_core", "urban")
    if focus == "suburban":
        return u == "suburban"
    if focus == "rural":
        return u in ("rural", "exurban")
    return True


def _tool_run_scenario(args: dict) -> tuple[str, list[dict]]:
    from .store import db

    competitor_year = args.get("competitor_year", 2025)
    count = args.get("final_locations_count", 10)
    excluded = args.get("excluded_closure_risks", [])
    urbanicity_focus = args.get("urbanicity_focus")

    net_urban = args.get("min_distance_from_network_urban", 1.5)
    net_sub = args.get("min_distance_from_network_suburban", 3.0)
    net_rural = args.get("min_distance_from_network_rural", 5.0)
    new_urban = args.get("min_distance_between_new_urban", 2.0)
    new_sub = args.get("min_distance_between_new_suburban", 5.0)
    new_rural = args.get("min_distance_between_new_rural", 8.0)

    candidates = sorted(db.hotspots, key=lambda h: h.score, reverse=True)
    excluded_ids = set(excluded)
    existing = [(l.lat, l.lng) for l in db.locations if l.id not in excluded_ids]

    if urbanicity_focus:
        candidates = [c for c in candidates if _urbanicity_matches(c.urbanicity, urbanicity_focus)]

    selected: list[dict] = []
    for hs in candidates:
        if len(selected) >= count:
            break
        u = hs.urbanicity.value if hasattr(hs.urbanicity, "value") else str(hs.urbanicity)
        net_dist = net_urban if u in ("urban_core", "urban") else (net_sub if u == "suburban" else net_rural)
        new_dist = new_urban if u in ("urban_core", "urban") else (new_sub if u == "suburban" else new_rural)

        if any(_haversine(hs.lat, hs.lng, lat, lng) < net_dist for lat, lng in existing):
            continue
        if any(_haversine(hs.lat, hs.lng, s["lat"], s["lng"]) < new_dist for s in selected):
            continue

        fmt_val = hs.format.value if hasattr(hs.format, "value") else str(hs.format)
        selected.append({
            "id": f"OPT{len(selected) + 1:03d}",
            "lat": hs.lat, "lng": hs.lng,
            "format": fmt_val,
            "projected_revenue": round(hs.projected_sales * 12, 1),
            "score": hs.score,
        })

    total_rev = sum(s["projected_revenue"] for s in selected)
    current_rev = sum(l.monthly_sales * 12 for l in db.locations if l.id not in excluded_ids)
    rev_change = round((total_rev / max(current_rev, 0.1)) * 100, 1) if current_rev > 0 else 0
    cannibalization = round(min(15, len(selected) * 0.8), 1)
    avg_score = round(sum(s["score"] for s in selected) / max(1, len(selected)), 1)

    total_str = f"${total_rev / 1_000:.1f}M" if total_rev >= 1_000 else f"${total_rev:.0f}K"

    # Build scenario_trigger marker
    scenario_payload = {
        "type": "scenario_trigger",
        "competitor_year": competitor_year,
        "final_locations_count": count,
        "excluded_closure_risks": excluded,
        "urbanicity_focus": urbanicity_focus,
        "min_distance_from_network": {"urban": net_urban, "suburban": net_sub, "rural": net_rural},
        "min_distance_between_new": {"urban": new_urban, "suburban": new_sub, "rural": new_rural},
        "result": {
            "optimized_locations": selected,
            "total_projected_revenue": round(total_rev, 1),
            "network_revenue_change": rev_change,
            "cannibalization_rate": cannibalization,
            "avg_site_score": avg_score,
        },
    }

    map_points: list[dict] = [
        {"lat": 0, "lng": 0, "label": "scenario", "properties": scenario_payload}
    ]
    for s in selected:
        map_points.append({
            "lat": s["lat"], "lng": s["lng"], "label": s["id"],
            "properties": {"format": s["format"], "projected_revenue": s["projected_revenue"], "score": s["score"], "type": "optimized_location"},
        })

    # Describe steps
    steps = f"**Scenario: {len(selected)} locations, {total_str} projected revenue, +{rev_change}% uplift.**"
    if excluded:
        steps += f"\n- Excluded {len(excluded)} at-risk stores"
    if urbanicity_focus:
        steps += f"\n- Focused on {urbanicity_focus} locations"
    if competitor_year != 2025:
        steps += f"\n- Competitor landscape set to {competitor_year}"
    steps += "\n\nScenario added to the panel below."

    return steps, map_points


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


GET_STORE_PERFORMANCE_TOOL = {
    "type": "function",
    "function": {
        "name": "get_store_performance",
        "description": "Get detailed performance data for a specific store including revenue, monthly trend, performance drivers (demographics, POI, competition), and percentile rank vs network. Also triggers H3 hex visualization automatically. Use when the user asks about a specific store's performance, details, or drivers.",
        "parameters": {
            "type": "object",
            "properties": {
                "store_id": {
                    "type": "string",
                    "description": "Store identifier, e.g., 'LOC1246', '1246', or '#1246'.",
                },
            },
            "required": ["store_id"],
        },
    },
}

COMPARE_STORES_TOOL = {
    "type": "function",
    "function": {
        "name": "compare_stores",
        "description": "Compare two stores side by side — revenue, performance drivers, demographics, and competitive landscape. Returns both stores' data and enables hex visualization toggle between them. Use when the user asks to compare two specific stores.",
        "parameters": {
            "type": "object",
            "properties": {
                "store_id_a": {"type": "string", "description": "First store identifier"},
                "store_id_b": {"type": "string", "description": "Second store identifier"},
            },
            "required": ["store_id_a", "store_id_b"],
        },
    },
}


def _normalize_store_id(raw: str) -> str:
    """Normalize store ID input to LOCxxxx format."""
    sid = raw.replace("#", "").strip()
    if sid.isdigit():
        return f"LOC{sid}"
    if not sid.upper().startswith("LOC"):
        return f"LOC{sid}"
    return sid.upper()


def _tool_get_store_performance(args: dict) -> tuple[str, list[dict]]:
    from . import sql_client as sql

    store_id = _normalize_store_id(str(args.get("store_id", "")))
    location_id = store_id.replace("LOC", "")

    # Query store data + format averages from gold_store_features_and_sales (has everything)
    query = f"""
    WITH store AS (
        SELECT s.store_number, s.name, s.city, s.zip_code,
               g.location_id, g.lat, g.lng, g.format, g.urbanicity_category as urbanicity,
               g.annual_revenue, g.jan_sales, g.feb_sales, g.mar_sales, g.apr_sales,
               g.may_sales, g.jun_sales, g.jul_sales, g.aug_sales, g.sep_sales,
               g.oct_sales, g.nov_sales, g.dec_sales,
               g.total_population, g.median_household_income, g.total_poi_count,
               g.total_competitor_count, g.higher_education_rate, g.urbanicity_score
        FROM {sql.table('gold_store_features_and_sales')} g
        JOIN {sql.table('bronze_store_locations')} s ON CAST(s.store_number AS STRING) = g.location_id
        WHERE g.location_id = '{location_id}'
    ),
    format_avgs AS (
        SELECT g2.format,
               ROUND(AVG(g2.annual_revenue)) as avg_revenue,
               ROUND(AVG(g2.total_population)) as avg_population,
               ROUND(AVG(g2.median_household_income)) as avg_income,
               ROUND(AVG(g2.total_poi_count)) as avg_poi,
               ROUND(AVG(g2.total_competitor_count)) as avg_competitors
        FROM {sql.table('gold_store_features_and_sales')} g2
        WHERE g2.format = (SELECT format FROM store LIMIT 1)
        GROUP BY g2.format
    ),
    pct AS (
        SELECT PERCENT_RANK() OVER (ORDER BY g3.annual_revenue) as percentile
        FROM {sql.table('gold_store_features_and_sales')} g3
        WHERE g3.location_id = '{location_id}'
    )
    SELECT store.*, fa.avg_revenue, fa.avg_population, fa.avg_income, fa.avg_poi, fa.avg_competitors,
           pct.percentile
    FROM store
    CROSS JOIN format_avgs fa
    CROSS JOIN pct
    """

    try:
        rows = sql.execute_sql(query)
    except Exception as e:
        return f"Error fetching store performance: {str(e)[:200]}", []

    if not rows:
        return f"Store {store_id} not found.", []

    r = rows[0]
    rev = float(r.get("annual_revenue") or 0)
    avg_rev = float(r.get("avg_revenue") or 1)
    pct = float(r.get("percentile") or 0)
    fmt = r.get("format", "standard")
    name = r.get("name", store_id)

    # Monthly sales
    months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
    month_sales = {m: float(r.get(f"{m}_sales") or 0) for m in months}
    best_months = sorted(month_sales, key=month_sales.get, reverse=True)[:3]
    worst_months = sorted(month_sales, key=month_sales.get)[:2]

    # Drivers — compare to format average
    drivers_pos = []
    drivers_neg = []
    driver_checks = [
        ("total_population", "avg_population", "Trade area population"),
        ("median_household_income", "avg_income", "Median household income"),
        ("total_poi_count", "avg_poi", "POI density"),
        ("total_competitor_count", "avg_competitors", "Nearby competitors"),
    ]
    for store_col, avg_col, label in driver_checks:
        store_val = float(r.get(store_col) or 0)
        avg_val = float(r.get(avg_col) or 1)
        if avg_val > 0:
            pct_diff = ((store_val - avg_val) / avg_val) * 100
            if store_col == "total_competitor_count":
                # Lower competitors is positive
                if pct_diff < -10:
                    drivers_pos.append(f"{label}: {int(store_val)} ({abs(pct_diff):.0f}% fewer than {fmt} avg)")
                elif pct_diff > 10:
                    drivers_neg.append(f"{label}: {int(store_val)} ({pct_diff:.0f}% more than {fmt} avg)")
            else:
                if pct_diff > 10:
                    drivers_pos.append(f"{label}: {int(store_val):,} ({pct_diff:.0f}% above {fmt} avg)")
                elif pct_diff < -10:
                    drivers_neg.append(f"{label}: {int(store_val):,} ({abs(pct_diff):.0f}% below {fmt} avg)")

    # Build summary
    rev_str = f"${rev / 1_000_000:.1f}M" if rev >= 1_000_000 else f"${rev / 1_000:.0f}K"
    avg_str = f"${avg_rev / 1_000_000:.1f}M" if avg_rev >= 1_000_000 else f"${avg_rev / 1_000:.0f}K"
    vs_avg = "above" if rev > avg_rev else "below"
    pct_str = f"{pct * 100:.0f}th"

    summary = f"**{name}** ({fmt} · {r.get('urbanicity', '')} · {r.get('city', '')})\n\n"
    summary += f"**Revenue:** {rev_str}/yr ({vs_avg} {fmt} avg of {avg_str}) · **{pct_str} percentile**\n"
    summary += f"**Strongest months:** {', '.join(m.capitalize() for m in best_months)} · **Weakest:** {', '.join(m.capitalize() for m in worst_months)}\n\n"

    if drivers_pos:
        summary += "**Positive drivers:** " + "; ".join(drivers_pos[:3]) + "\n"
    if drivers_neg:
        summary += "**Headwinds:** " + "; ".join(drivers_neg[:3]) + "\n"

    summary += "\nH3 hex visualization loaded — use the metric selector to explore demographics around this store."

    # Map points: store location + h3_trigger
    map_points = [
        {
            "lat": float(r.get("lat", 0)),
            "lng": float(r.get("lng", 0)),
            "label": name,
            "properties": {
                "name": name,
                "format": fmt,
                "annual_revenue": rev,
                "city": r.get("city", ""),
                "urbanicity": r.get("urbanicity", ""),
                "store_number": r.get("store_number", ""),
                "percentile_rank": round(pct * 100, 1),
            },
        },
        {"lat": 0, "lng": 0, "label": store_id, "properties": {"type": "h3_trigger", "store_id": store_id}},
    ]

    return summary, map_points


def _tool_compare_stores(args: dict) -> tuple[str, list[dict]]:
    from . import sql_client as sql

    id_a = _normalize_store_id(str(args.get("store_id_a", "")))
    id_b = _normalize_store_id(str(args.get("store_id_b", "")))
    loc_a = id_a.replace("LOC", "")
    loc_b = id_b.replace("LOC", "")

    query = f"""
    SELECT s.store_number, s.name, s.city,
           g.lat, g.lng, g.format, g.urbanicity_category as urbanicity,
           g.annual_revenue,
           g.total_population, g.median_household_income, g.total_poi_count,
           g.total_competitor_count, g.higher_education_rate
    FROM {sql.table('gold_store_features_and_sales')} g
    JOIN {sql.table('bronze_store_locations')} s ON CAST(s.store_number AS STRING) = g.location_id
    WHERE g.location_id IN ('{loc_a}', '{loc_b}')
    """

    try:
        rows = sql.execute_sql(query)
    except Exception as e:
        return f"Error comparing stores: {str(e)[:200]}", []

    if len(rows) < 2:
        found = [r.get("store_number") for r in rows]
        return f"Could not find both stores. Found: {found}. Check the store numbers.", []

    # Sort to match requested order
    store_a = next((r for r in rows if str(r.get("store_number")) == loc_a), rows[0])
    store_b = next((r for r in rows if str(r.get("store_number")) == loc_b), rows[1])

    def _fmt_rev(v):
        v = float(v or 0)
        return f"${v / 1_000_000:.1f}M" if v >= 1_000_000 else f"${v / 1_000:.0f}K"

    rev_a = float(store_a.get("annual_revenue") or 0)
    rev_b = float(store_b.get("annual_revenue") or 0)
    name_a = store_a.get("name", id_a)
    name_b = store_b.get("name", id_b)

    summary = f"**{name_a}** vs **{name_b}**\n\n"
    summary += f"| Metric | {name_a} | {name_b} |\n|---|---|---|\n"
    summary += f"| Revenue | {_fmt_rev(rev_a)} | {_fmt_rev(rev_b)} |\n"
    summary += f"| Format | {store_a.get('format')} | {store_b.get('format')} |\n"
    summary += f"| Urbanicity | {store_a.get('urbanicity')} | {store_b.get('urbanicity')} |\n"

    compare_fields = [
        ("total_population", "Trade Area Pop", "{:,.0f}"),
        ("median_household_income", "Median HHI", "${:,.0f}"),
        ("total_poi_count", "POI Count", "{:.0f}"),
        ("total_competitor_count", "Competitors", "{:.0f}"),
        ("higher_education_rate", "Higher Ed Rate", "{:.1%}"),
    ]
    for col, label, fmt_str in compare_fields:
        val_a = float(store_a.get(col) or 0)
        val_b = float(store_b.get(col) or 0)
        summary += f"| {label} | {fmt_str.format(val_a)} | {fmt_str.format(val_b)} |\n"

    winner = name_a if rev_a > rev_b else name_b
    diff_pct = abs(rev_a - rev_b) / max(rev_a, rev_b) * 100
    summary += f"\n{winner} leads in revenue by {diff_pct:.0f}%. Toggle between stores below to compare their trade area hexagons."

    # Map points: both stores + comparison_trigger
    map_points = []
    for store, sid in [(store_a, id_a), (store_b, id_b)]:
        map_points.append({
            "lat": float(store.get("lat", 0)),
            "lng": float(store.get("lng", 0)),
            "label": store.get("name", sid),
            "properties": {
                "name": store.get("name", sid),
                "format": store.get("format"),
                "annual_revenue": float(store.get("annual_revenue") or 0),
                "city": store.get("city", ""),
                "urbanicity": store.get("urbanicity", ""),
                "store_number": store.get("store_number", ""),
            },
        })
    map_points.append({
        "lat": 0, "lng": 0, "label": "comparison",
        "properties": {"type": "comparison_trigger", "store_id_a": id_a, "store_id_b": id_b},
    })

    return summary, map_points


def _tool_show_trade_area(args: dict) -> tuple[str, list[dict]]:
    store_id = _normalize_store_id(str(args.get("store_id", "")))

    # Return a special marker that the frontend will interpret as an H3 trigger
    return f"Showing H3 trade area for {store_id}.", [
        {"lat": 0, "lng": 0, "label": store_id, "properties": {"type": "h3_trigger", "store_id": store_id}}
    ]


def _execute_tool(name: str, args: dict) -> tuple[str, list[dict]]:
    dispatch = {
        "query_sql": _tool_query_sql,
        "search_expansion_sites": _tool_search_expansion_sites,
        "run_scenario": _tool_run_scenario,
        "run_expansion_scenario": _tool_run_scenario,
        "show_trade_area": _tool_show_trade_area,
        "get_store_performance": _tool_get_store_performance,
        "compare_stores": _tool_compare_stores,
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
    benchmark: str | None = None,
    closure_candidates: list[str] | None = None,
) -> dict:
    """Run the multi-turn agent loop and return the final response."""
    w = _get_workspace_client()

    messages: list[dict] = [{"role": "system", "content": _get_system_prompt(page_context, benchmark, closure_candidates)}]
    if history:
        for h in history:
            messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": message})

    all_map_points: list[dict] = []

    # Build tool list based on page context
    active_tools = list(TOOLS)
    if page_context == "network":
        active_tools.extend([SHOW_TRADE_AREA_TOOL, GET_STORE_PERFORMANCE_TOOL, COMPARE_STORES_TOOL])
    elif page_context == "expansion":
        active_tools.append(RUN_SCENARIO_TOOL)

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
        if any(w in m for w in ["scenario", "optimiz", "drop", "at risk", "run"]):
            return [
                "Run another scenario with urban focus",
                "What are the best locations for expansion?",
            ]
        if any(w in m for w in ["expand", "site", "new", "open", "candidate", "best", "benchmark"]):
            return [
                "Run a scenario: drop at-risk, competition 2028",
                "Show me suburban options",
                "Optimize network",
            ]
        return [
            "What are the best locations for expansion?",
            "Optimize network",
        ]
    # network
    if any(w in m for w in ["driver", "top", "bottom", "quartile", "percentile"]):
        return [
            "Top 10 performers",
            "Bottom 10 performers",
            "Performance of store #1113",
        ]
    if any(w in m for w in ["perform", "revenue", "store", "compare"]):
        return [
            "What are the drivers of performance for the top 50%?",
            "Bottom 10 performers",
            "Compare store #1108 vs #1001",
        ]
    return [
        "Top 10 performers",
        "Bottom 10 performers",
        "What are the drivers of performance for the top 50% of locations?",
    ]
