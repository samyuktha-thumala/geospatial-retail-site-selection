"""Gemini chat via Databricks Foundation Model API."""

import json
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

logger = logging.getLogger(__name__)

ENDPOINT_NAME = "databricks-gemini-2-5-flash"

SYSTEM_PROMPT = """You are a Site Selection AI assistant for a retail chain operating in New York State.
You help analysts understand store performance, competitor landscape, expansion opportunities, and closure risks.

You have access to the following data context about the network. Use it to answer questions accurately.
When the user asks about locations, competitors, revenue, or strategy, reference this data.
If asked about a specific place or address, use your knowledge of New York geography to provide relevant insights.

DATA CONTEXT:
{context}

Guidelines:
- Be concise and data-driven in your responses
- Reference specific numbers from the context when relevant
- For location questions, relate to the store network and competitive landscape
- Suggest actionable next steps when appropriate
- If you don't have specific data to answer, say so and suggest what analysis could help
"""


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


def chat_with_gemini(
    message: str,
    conversation_history: list[dict] | None = None,
    summaries: dict | None = None,
) -> dict:
    """Send a chat message to Gemini via Databricks FMAPI.

    Returns: {"response": str, "suggestions": list[str]}
    """
    context_str = _build_context(summaries) if summaries else "No data context available."
    system_message = SYSTEM_PROMPT.format(context=context_str)

    role_map = {
        "system": ChatMessageRole.SYSTEM,
        "user": ChatMessageRole.USER,
        "assistant": ChatMessageRole.ASSISTANT,
    }

    messages = [ChatMessage(role=ChatMessageRole.SYSTEM, content=system_message)]

    # Add conversation history
    if conversation_history:
        for h in conversation_history:
            messages.append(ChatMessage(
                role=role_map.get(h["role"], ChatMessageRole.USER),
                content=h["content"],
            ))

    messages.append(ChatMessage(role=ChatMessageRole.USER, content=message))

    try:
        w = WorkspaceClient()
        response = w.serving_endpoints.query(
            name=ENDPOINT_NAME,
            messages=messages,
            max_tokens=1024,
            temperature=0.7,
        )

        reply = response.choices[0].message.content

        # Generate follow-up suggestions based on the response
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
