"""Genie-powered chat via Databricks Genie Conversation API."""

import json
import logging
import os
import time

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")

# How long to poll for a Genie response before giving up
POLL_TIMEOUT_SECONDS = 60
POLL_INTERVAL_SECONDS = 2


def _get_workspace_client() -> WorkspaceClient:
    if os.environ.get("DATABRICKS_APP_NAME"):
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
    return WorkspaceClient(profile=profile)


def _extract_genie_response(w: WorkspaceClient, space_id: str, conversation_id: str, message_id: str) -> dict:
    """Poll for and extract the Genie response from a message."""
    deadline = time.time() + POLL_TIMEOUT_SECONDS

    while time.time() < deadline:
        msg = w.genie.get_message(space_id, conversation_id, message_id)

        status = msg.status.value if msg.status else "UNKNOWN"

        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            break

        time.sleep(POLL_INTERVAL_SECONDS)

    # Extract response from attachments
    text_parts = []
    sql_query = None
    result_columns = []
    result_rows = []

    if msg.attachments:
        for attachment in msg.attachments:
            if hasattr(attachment, 'text') and attachment.text:
                content = attachment.text.content if hasattr(attachment.text, 'content') else str(attachment.text)
                if content:
                    text_parts.append(content)
            if hasattr(attachment, 'query') and attachment.query:
                query_obj = attachment.query
                sql_query = query_obj.query if hasattr(query_obj, 'query') else None
                title = query_obj.title if hasattr(query_obj, 'title') else None

                # Try to get the query results
                if sql_query:
                    try:
                        result = w.genie.get_message_query_result(space_id, conversation_id, message_id)
                        columns = []
                        rows_data = []

                        if hasattr(result, 'statement_response') and result.statement_response:
                            sr = result.statement_response
                            if hasattr(sr, 'manifest') and sr.manifest and hasattr(sr.manifest, 'schema') and sr.manifest.schema:
                                columns = [col.name for col in sr.manifest.schema.columns]
                            if hasattr(sr, 'result') and sr.result and hasattr(sr.result, 'data_array') and sr.result.data_array:
                                rows_data = sr.result.data_array

                        if columns and rows_data:
                            # Store raw results for map layer
                            result_columns = columns
                            result_rows = rows_data

                            # Format as a readable table
                            if title:
                                text_parts.append(f"**{title}**\n")
                            header = " | ".join(columns)
                            separator = " | ".join(["---"] * len(columns))
                            text_parts.append(f"| {header} |")
                            text_parts.append(f"| {separator} |")
                            for row in rows_data[:50]:
                                formatted = " | ".join(str(v) if v is not None else "" for v in row)
                                text_parts.append(f"| {formatted} |")
                            if len(rows_data) > 50:
                                text_parts.append(f"\n*Showing 50 of {len(rows_data)} rows*")
                    except Exception as e:
                        logger.warning(f"Failed to get query result: {e}")

    if not text_parts:
        if status == "FAILED":
            text_parts.append("I wasn't able to answer that question. Could you try rephrasing it?")
        elif status == "CANCELLED":
            text_parts.append("The query was cancelled. Please try again.")
        else:
            text_parts.append("I didn't get a response. Please try again.")

    # Build map_points if results contain lat/lng columns
    map_points = _extract_map_points(result_columns, result_rows) if result_columns and result_rows else []

    return {
        "response": "\n".join(text_parts),
        "conversation_id": conversation_id,
        "sql": sql_query,
        "map_points": map_points,
    }


def _extract_map_points(columns: list[str], rows: list[list]) -> list[dict]:
    """Extract map-plottable points from query results if lat/lng columns exist."""
    col_lower = [c.lower() for c in columns]

    # Find lat/lng column indices
    lat_idx = None
    lng_idx = None
    for i, c in enumerate(col_lower):
        if c in ("lat", "latitude"):
            lat_idx = i
        elif c in ("lng", "longitude", "lon"):
            lng_idx = i

    if lat_idx is None or lng_idx is None:
        return []

    # Build a label from other columns (first string-like column, or name)
    label_idx = None
    for i, c in enumerate(col_lower):
        if c in ("name", "store_name", "location_name", "title", "brand"):
            label_idx = i
            break
    # Fallback: use first column that isn't lat/lng
    if label_idx is None:
        for i, c in enumerate(col_lower):
            if i != lat_idx and i != lng_idx:
                label_idx = i
                break

    points = []
    for row in rows[:100]:
        try:
            lat = float(row[lat_idx])
            lng = float(row[lng_idx])
            label = str(row[label_idx]) if label_idx is not None and row[label_idx] is not None else ""
            # Include all columns as properties
            props = {columns[i]: row[i] for i in range(len(columns)) if i != lat_idx and i != lng_idx}
            points.append({"lat": lat, "lng": lng, "label": label, "properties": props})
        except (ValueError, TypeError, IndexError):
            continue

    return points


def chat_with_genie(
    message: str,
    conversation_id: str | None = None,
) -> dict:
    """Send a chat message to the Genie Space.

    Args:
        message: The user's question
        conversation_id: Optional existing conversation ID for follow-ups

    Returns: {"response": str, "suggestions": list[str], "conversation_id": str}
    """
    if not GENIE_SPACE_ID:
        return {
            "response": "Genie Space is not configured. Please set GENIE_SPACE_ID.",
            "suggestions": [],
            "conversation_id": "",
        }

    try:
        w = _get_workspace_client()

        if conversation_id:
            # Follow-up in existing conversation
            result = w.genie.create_message_and_wait(
                space_id=GENIE_SPACE_ID,
                conversation_id=conversation_id,
                content=message,
            )
            msg_id = result.id
        else:
            # New conversation
            result = w.genie.start_conversation_and_wait(
                space_id=GENIE_SPACE_ID,
                content=message,
            )
            conversation_id = result.conversation_id
            msg_id = result.id

        extracted = _extract_genie_response(w, GENIE_SPACE_ID, conversation_id, msg_id)

        suggestions = _generate_suggestions(message, extracted["response"])

        return {
            "response": extracted["response"],
            "suggestions": suggestions,
            "conversation_id": extracted["conversation_id"],
            "map_points": extracted.get("map_points", []),
        }

    except Exception as e:
        logger.error(f"Genie chat error: {e}")
        return {
            "response": f"I'm having trouble connecting to Genie. Error: {str(e)[:200]}",
            "suggestions": ["Try again", "Show network overview"],
            "conversation_id": conversation_id or "",
        }


def _generate_suggestions(user_message: str, bot_response: str) -> list[str]:
    """Generate contextual follow-up suggestions."""
    msg = user_message.lower()

    if any(w in msg for w in ["revenue", "sales", "performance", "top"]):
        return ["Average annual revenue by store format", "Bottom 10 stores by annual revenue", "Total revenue across all stores"]
    elif any(w in msg for w in ["competitor", "competition", "brand"]):
        return ["How many competitors by brand?", "Competitors that opened after 2020", "Which brand has the most locations?"]
    elif any(w in msg for w in ["expand", "new", "hotspot", "opportunity", "rural", "suburban"]):
        return ["Top 20 suburban hotspots by revenue", "Hotspots with population over 50000", "Average income in expansion areas"]
    elif any(w in msg for w in ["store", "location", "format"]):
        return ["How many stores per format?", "Flagship stores ranked by revenue", "Express stores with highest sales"]
    else:
        return ["Top 10 stores by annual revenue", "Top 20 rural expansion hotspots", "Competitor count by brand"]
