"""Databricks SQL client with dual-mode auth and caching."""

import os
import time
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Cache: key -> (timestamp, data)
_cache: dict[str, tuple[float, Any]] = {}
_CACHE_TTL = 600  # 10 minutes

CATALOG = os.environ.get("DATABRICKS_CATALOG", "YOUR_CATALOG")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "YOUR_SCHEMA")


def _is_databricks_app() -> bool:
    return bool(os.environ.get("DATABRICKS_APP_NAME"))


def _get_workspace_client():
    from databricks.sdk import WorkspaceClient

    if _is_databricks_app():
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
    return WorkspaceClient(profile=profile)


def _get_warehouse_id() -> str | None:
    wh = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if wh:
        return wh
    # Try to find a running warehouse
    try:
        w = _get_workspace_client()
        warehouses = w.warehouses.list()
        for wh_obj in warehouses:
            if wh_obj.state and wh_obj.state.value == "RUNNING":
                return wh_obj.id
    except Exception:
        pass
    return None


def execute_sql(query: str, cache_key: str | None = None) -> list[dict[str, Any]]:
    """Execute SQL against Databricks warehouse. Returns list of row dicts."""
    # Check cache
    if cache_key and cache_key in _cache:
        ts, data = _cache[cache_key]
        if time.time() - ts < _CACHE_TTL:
            return data

    warehouse_id = _get_warehouse_id()
    if not warehouse_id:
        raise RuntimeError("No DATABRICKS_WAREHOUSE_ID configured and no running warehouse found")

    w = _get_workspace_client()

    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout="50s",
        )

        if response.status and response.status.state and response.status.state.value == "FAILED":
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            raise RuntimeError(f"SQL execution failed: {error_msg}")

        if not response.manifest or not response.result:
            return []

        columns = [col.name for col in response.manifest.schema.columns]
        rows = []
        if response.result.data_array:
            for row_data in response.result.data_array:
                rows.append(dict(zip(columns, row_data)))

        # Cache result
        if cache_key:
            _cache[cache_key] = (time.time(), rows)

        return rows

    except Exception as e:
        logger.error(f"SQL execution error: {e}")
        raise


def table(name: str) -> str:
    """Return fully qualified table name."""
    return f"{CATALOG}.{SCHEMA}.{name}"


def is_available() -> bool:
    """Check if Databricks SQL is available."""
    try:
        if not _get_warehouse_id():
            return False
        _get_workspace_client()
        return True
    except Exception:
        return False


def clear_cache():
    """Clear the SQL cache."""
    _cache.clear()
