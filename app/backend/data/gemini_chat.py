"""Gemini chat via Databricks Foundation Model API with SQL tool calling."""

import json
import logging
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

import os
ENDPOINT_NAME = os.environ.get("SERVING_ENDPOINT", "YOUR_SERVING_ENDPOINT")

# Maximum number of tool-call round trips before forcing a text reply
MAX_TOOL_ROUNDS = 3
# Maximum rows returned from a single SQL query
MAX_RESULT_ROWS = 100

SYSTEM_PROMPT = """You are a Site Selection AI assistant for a retail chain operating in New York State.
You help analysts understand store performance, competitor landscape, expansion opportunities, and closure risks.

You have two sources of information:
1. A pre-computed DATA CONTEXT (summary metrics shown below).
2. A `query_database` tool that lets you run SELECT queries against the catalog tables described below.

When the user asks a question that the summary can answer, use the summary.
When the user asks for specific filtered data, rankings, top-N lists, or anything the summary doesn't cover, use the query_database tool to get the data you need, then present the results clearly.

DATA CONTEXT:
{context}

AVAILABLE TABLES (all in the configured catalog.schema):

1. bronze_store_locations — our retail store locations
   Columns: store_number, name, lat, lng, format (express/standard/flagship), urbanicity

2. bronze_competitor_locations — competitor store locations
   Columns: competitor_id, brand, lat, lng, open_year

3. bronze_census_demographics — census block-group demographics
   Columns: geoid, total_population, median_household_income, latitude, longitude

4. gold_store_features_and_sales — store-level features and monthly sales
   Columns: location_id, jan_sales..dec_sales, annual_revenue

5. gold_expansion_candidates — scored expansion hotspot locations
   Columns: lat, lng, recommended_revenue, recommended_format (express/standard/flagship),
            urbanicity_category (urban/suburban/rural), recommended_rev_per_sqft,
            total_population, median_household_income

6. gold_simulated_competitor_growth — projected future competitor locations
   Columns: sim_id, brand, lat, lng, projected_year

7. gold_model_artifacts — ML model metadata
   Columns: model_name, model_version, n_samples, n_features, rmse, r2, mae

8. silver_osm_pois — OpenStreetMap points of interest
   Columns: poi_id, name, latitude, longitude, poi_category, poi_subcategory

9. silver_store_isochrones — drive-time polygons around stores
   Columns: location_id, format, geometry_wkt, drive_time_minutes, urbanicity_category, area_sqkm

10. silver_h3_features — H3 hex-level aggregated features
    Columns: h3_cell_id, urbanicity_category, urbanicity_score, total_population,
             total_poi_count, total_competitor_count, median_household_income

QUERY GUIDELINES:
- Always use SELECT only. Never modify data.
- Use LIMIT to keep results manageable (max {max_rows} rows).
- When joining bronze_store_locations with gold_store_features_and_sales, join on CAST(store_number AS STRING) = location_id.
- For proximity queries, you can compute approximate distance using the Haversine-like formula:
  3958.8 * 2 * ASIN(SQRT(POW(SIN(RADIANS((lat2 - lat1) / 2)), 2) + COS(RADIANS(lat1)) * COS(RADIANS(lat2)) * POW(SIN(RADIANS((lng2 - lng1) / 2)), 2)))
- Monthly sales values in gold_store_features_and_sales are in raw dollars; the app displays them in K (thousands).
- Present results in a clear, formatted way — use tables or numbered lists for multi-row results.

Guidelines:
- Be concise and data-driven in your responses
- Reference specific numbers when relevant
- Suggest actionable next steps when appropriate
- If a query fails, explain what happened and suggest an alternative
"""

# Tool definition in OpenAI function-calling format
SQL_TOOL = {
    "type": "function",
    "function": {
        "name": "query_database",
        "description": "Execute a read-only SQL SELECT query against the site selection catalog tables. Use this to answer questions that require filtering, ranking, aggregation, or joining data that isn't available in the pre-computed summary.",
        "parameters": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "A SELECT SQL query to execute against the catalog tables. Must be read-only.",
                },
                "reasoning": {
                    "type": "string",
                    "description": "Brief explanation of why this query is needed and what it will answer.",
                },
            },
            "required": ["sql"],
        },
    },
}


def _execute_tool_query(sql_query: str) -> str:
    """Execute a SQL query via the sql_client and return results as a string."""
    from . import sql_client as sql

    # Safety: only allow SELECT
    stripped = sql_query.strip().upper()
    if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
        return "ERROR: Only SELECT queries are allowed."

    # Inject catalog.schema into unqualified table names
    qualified_query = sql_query
    table_names = [
        "bronze_store_locations", "bronze_competitor_locations",
        "bronze_census_demographics", "gold_store_features_and_sales",
        "gold_expansion_candidates", "gold_simulated_competitor_growth",
        "gold_model_artifacts", "silver_osm_pois", "silver_store_isochrones",
        "silver_h3_features",
    ]
    for t in table_names:
        # Replace unqualified table references with fully qualified ones
        # Avoid double-qualifying if already qualified
        fq = sql.table(t)
        if fq not in qualified_query:
            qualified_query = qualified_query.replace(t, fq)

    # Enforce row limit if no LIMIT clause present
    if "LIMIT" not in qualified_query.upper():
        qualified_query = qualified_query.rstrip().rstrip(";") + f" LIMIT {MAX_RESULT_ROWS}"

    try:
        rows = sql.execute_sql(qualified_query)
        if not rows:
            return "Query returned no results."
        # Truncate to max rows
        rows = rows[:MAX_RESULT_ROWS]
        return json.dumps(rows, indent=2, default=str)
    except Exception as e:
        return f"SQL ERROR: {str(e)[:500]}"


def _build_context(summaries: dict) -> str:
    """Build a context string from pre-computed summaries."""
    parts = []

    if "kpis" in summaries:
        parts.append("KEY METRICS:")
        for kpi in summaries["kpis"]:
            line = f"  - {kpi['label']}: {kpi['value']}"
            if kpi.get("subtext"):
                line += f" ({kpi['subtext']})"
            parts.append(line)

    if "format_breakdown" in summaries:
        parts.append("\nSTORE FORMAT BREAKDOWN:")
        for fmt in summaries["format_breakdown"]:
            parts.append(f"  - {fmt['format']}: {fmt['count']} stores, avg annual revenue ${fmt['avg_annual']:.0f}K")

    if "closure_risks" in summaries:
        parts.append(f"\nCLOSURE RISKS: {len(summaries['closure_risks'])} stores flagged")
        for risk in summaries["closure_risks"][:5]:
            parts.append(f"  - {risk['name']} ({risk['id']}): {risk['closure_risk']}% risk — {risk['reason']}")

    if "competitors" in summaries:
        parts.append(f"\nCOMPETITORS: {summaries['competitors']['total']} locations tracked")
        if summaries["competitors"].get("by_brand"):
            for brand, count in summaries["competitors"]["by_brand"].items():
                parts.append(f"  - {brand}: {count} locations")

    if "hotspots" in summaries:
        parts.append(f"\nEXPANSION OPPORTUNITIES: {summaries['hotspots']['count']} hotspots identified")
        if summaries["hotspots"].get("top"):
            for hs in summaries["hotspots"]["top"][:3]:
                parts.append(f"  - {hs['format']} site at ({hs['lat']:.3f}, {hs['lng']:.3f}): score {hs['score']}, projected ${hs['projected_sales']}K/mo")

    if "network_metrics" in summaries:
        nm = summaries["network_metrics"]
        parts.append(f"\nNETWORK OPTIMIZATION:")
        parts.append(f"  - Current revenue: {nm.get('total_current_revenue', 'N/A')}")
        parts.append(f"  - Projected optimized: {nm.get('projected_optimized_revenue', 'N/A')}")
        parts.append(f"  - Revenue uplift: {nm.get('revenue_uplift', 'N/A')}")
        parts.append(f"  - New locations recommended: {nm.get('new_locations_recommended', 'N/A')}")

    return "\n".join(parts) if parts else "No data context available."


def build_summaries_from_store(db) -> dict:
    """Build pre-computed summaries from the data store."""
    summaries = {}

    # KPIs
    try:
        summaries["kpis"] = [
            {"label": k.label, "value": k.value, "subtext": k.subtext}
            for k in db.kpis
        ]
    except Exception as e:
        logger.warning(f"Failed to build KPI summary: {e}")

    # Format breakdown
    try:
        from collections import Counter
        format_counts = Counter(l.format.value for l in db.locations)
        format_breakdown = []
        for fmt in ["express", "standard", "flagship"]:
            fmt_locs = [l for l in db.locations if l.format.value == fmt]
            if fmt_locs:
                avg_annual = sum(l.monthly_sales * 12 for l in fmt_locs) / len(fmt_locs)
                format_breakdown.append({
                    "format": fmt,
                    "count": len(fmt_locs),
                    "avg_annual": avg_annual,
                })
        summaries["format_breakdown"] = format_breakdown
    except Exception as e:
        logger.warning(f"Failed to build format summary: {e}")

    # Closure risks
    try:
        summaries["closure_risks"] = [
            {"name": c.name, "id": c.id, "closure_risk": c.closure_risk, "reason": c.reason}
            for c in db.closure_candidates[:10]
        ]
    except Exception as e:
        logger.warning(f"Failed to build closure summary: {e}")

    # Competitors
    try:
        from collections import Counter
        brand_counts = Counter(c.brand for c in db.competitors if not c.is_projected)
        summaries["competitors"] = {
            "total": len(db.competitors),
            "by_brand": dict(brand_counts),
        }
    except Exception as e:
        logger.warning(f"Failed to build competitor summary: {e}")

    # Hotspots
    try:
        summaries["hotspots"] = {
            "count": len(db.hotspots),
            "top": [
                {"lat": h.lat, "lng": h.lng, "score": h.score,
                 "projected_sales": h.projected_sales, "format": h.format.value}
                for h in sorted(db.hotspots, key=lambda x: x.score, reverse=True)[:5]
            ],
        }
    except Exception as e:
        logger.warning(f"Failed to build hotspot summary: {e}")

    # Network metrics
    try:
        nm = db.network_metrics
        summaries["network_metrics"] = {
            "total_current_revenue": nm.total_current_revenue,
            "projected_optimized_revenue": nm.projected_optimized_revenue,
            "revenue_uplift": nm.revenue_uplift,
            "new_locations_recommended": nm.new_locations_recommended,
        }
    except Exception as e:
        logger.warning(f"Failed to build network metrics summary: {e}")

    return summaries


def _extract_text_content(content) -> str:
    """Extract plain text from a response content field.

    The FMAPI may return content as a plain string or as a list of content blocks
    like [{"type": "text", "text": "..."}].
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text", ""))
            elif isinstance(block, str):
                parts.append(block)
        return "\n".join(parts)
    return str(content) if content else ""


def _query_endpoint_raw(w: WorkspaceClient, endpoint_name: str, body: dict) -> dict:
    """Call a serving endpoint with a raw JSON body (supports tools, tool messages, etc.)."""
    response = w.api_client.do(
        "POST",
        f"/serving-endpoints/{endpoint_name}/invocations",
        body=body,
    )
    return response


def chat_with_gemini(
    message: str,
    conversation_history: list[dict] | None = None,
    summaries: dict | None = None,
) -> dict:
    """Send a chat message to Gemini via Databricks FMAPI with SQL tool calling.

    Returns: {"response": str, "suggestions": list[str]}
    """
    from . import sql_client

    context_str = _build_context(summaries) if summaries else "No data context available."
    system_message = SYSTEM_PROMPT.format(context=context_str, max_rows=MAX_RESULT_ROWS)

    messages = [{"role": "system", "content": system_message}]

    # Add conversation history
    if conversation_history:
        for h in conversation_history:
            messages.append({"role": h["role"], "content": h["content"]})

    messages.append({"role": "user", "content": message})

    # Only offer SQL tool if Databricks SQL is available
    tools = [SQL_TOOL] if sql_client.is_available() else None

    try:
        w = WorkspaceClient()

        # Tool-call loop: let the model call query_database, then feed results back
        for _round in range(MAX_TOOL_ROUNDS + 1):
            body = {
                "messages": messages,
                "max_tokens": 1024,
                "temperature": 0.7,
            }
            if tools:
                body["tools"] = tools

            response = _query_endpoint_raw(w, ENDPOINT_NAME, body)

            choice = response["choices"][0]
            assistant_msg = choice["message"]

            # If the model wants to call a tool
            tool_calls = assistant_msg.get("tool_calls")
            if tool_calls:
                # Add the assistant's tool-call message to history
                messages.append(assistant_msg)

                for tool_call in tool_calls:
                    fn = tool_call["function"]
                    if fn["name"] == "query_database":
                        args = json.loads(fn["arguments"]) if isinstance(fn["arguments"], str) else fn["arguments"]
                        sql_query = args.get("sql", "")
                        reasoning = args.get("reasoning", "")
                        logger.info(f"Agent SQL query (round {_round + 1}): {sql_query}")
                        if reasoning:
                            logger.info(f"  Reasoning: {reasoning}")

                        result = _execute_tool_query(sql_query)

                        messages.append({
                            "role": "tool",
                            "content": result,
                            "tool_call_id": tool_call["id"],
                        })
                    else:
                        messages.append({
                            "role": "tool",
                            "content": f"Unknown tool: {fn['name']}",
                            "tool_call_id": tool_call["id"],
                        })
                # Continue the loop to get the model's next response
                continue

            # No tool calls — we have the final text reply
            reply = _extract_text_content(assistant_msg.get("content", ""))
            suggestions = _generate_suggestions(message, reply)
            return {"response": reply, "suggestions": suggestions}

        # If we exhausted rounds, return whatever we have
        raw_content = response["choices"][0]["message"].get("content", "I ran multiple queries but couldn't fully answer. Please try a simpler question.")
        reply = _extract_text_content(raw_content)
        suggestions = _generate_suggestions(message, reply)
        return {"response": reply, "suggestions": suggestions}

    except Exception as e:
        logger.error(f"Gemini chat error: {e}")
        return {
            "response": f"I'm having trouble connecting to the AI service. Error: {str(e)[:200]}",
            "suggestions": ["Try again", "Show network overview"],
        }


def _generate_suggestions(user_message: str, bot_response: str) -> list[str]:
    """Generate contextual follow-up suggestions."""
    msg = user_message.lower()

    if any(w in msg for w in ["revenue", "sales", "performance"]):
        return ["Break down by format", "Show underperforming stores", "Compare to last year"]
    elif any(w in msg for w in ["competitor", "competition"]):
        return ["Competitor density map", "Impact on our stores", "Projected growth"]
    elif any(w in msg for w in ["expand", "new", "site", "hotspot", "location"]):
        return ["Top 5 expansion sites", "Filter by format", "Show on map"]
    elif any(w in msg for w in ["risk", "closure", "close", "underperform"]):
        return ["Show all at-risk stores", "Mitigation strategies", "Revenue impact of closures"]
    elif any(w in msg for w in ["map", "where", "area", "neighborhood", "address"]):
        return ["Show nearby competitors", "Demographics for this area", "Trade area analysis"]
    else:
        return ["Analyze revenue trends", "Find expansion opportunities", "Review closure risks"]
