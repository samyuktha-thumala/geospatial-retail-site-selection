"""Hybrid data store — tries Databricks SQL fetchers, falls back to generators."""

import logging

from .generators import (
    generate_locations,
    generate_competitors,
    generate_hotspots,
    generate_closure_candidates,
    generate_data_sources,
    generate_kpis,
    generate_model_performance,
    generate_demographics,
    generate_alerts,
    generate_network_metrics,
    generate_closest_competitors,
)

logger = logging.getLogger(__name__)


def _try_import_fetchers():
    """Try to import SQL fetchers; returns None if unavailable."""
    try:
        from . import sql_client
        if not sql_client.is_available():
            return None
        from . import fetchers
        return fetchers
    except Exception as e:
        logger.info(f"Databricks SQL not available: {e}")
        return None


class DataStore:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

        fetchers = _try_import_fetchers()

        if fetchers:
            self._mode = "LIVE"
            logger.info("Data mode: LIVE (Databricks SQL)")

            def _try(name, fetcher_fn, fallback_fn):
                try:
                    result = fetcher_fn()
                    logger.info(f"  {name}: loaded {len(result) if isinstance(result, list) else 'OK'} from catalog")
                    return result
                except Exception as e:
                    logger.warning(f"  {name}: fetch failed ({e}), using synthetic")
                    return fallback_fn()

            self.locations = _try("locations", fetchers.fetch_locations, generate_locations)
            self.competitors = _try("competitors", fetchers.fetch_competitors, generate_competitors)
            self.hotspots = _try("hotspots", fetchers.fetch_hotspots, lambda: generate_hotspots(self.locations))
            self.closure_candidates = _try("closure", fetchers.fetch_closure_candidates, lambda: generate_closure_candidates(self.locations))
            self.data_sources = _try("data_sources", fetchers.fetch_data_sources, generate_data_sources)
            self.kpis = _try("kpis", fetchers.fetch_kpis, lambda: generate_kpis(self.locations))
            self.model_performance = _try("model_perf", fetchers.fetch_model_performance, generate_model_performance)
            self.demographics = _try("demographics", fetchers.fetch_demographics, generate_demographics)
            self.alerts = _try("alerts", fetchers.fetch_alerts, generate_alerts)
            self.network_metrics = _try("network_metrics", fetchers.fetch_network_metrics, generate_network_metrics)
            self.closest_competitors = _try("closest_comps", fetchers.fetch_closest_competitors, generate_closest_competitors)
            self.isochrones = _try("isochrones", fetchers.fetch_isochrones, lambda: [])
        else:
            self._mode = "SYNTHETIC"
            logger.info("Data mode: SYNTHETIC (fallback generators)")
            self.locations = generate_locations()
            self.competitors = generate_competitors()
            self.hotspots = generate_hotspots(self.locations)
            self.closure_candidates = generate_closure_candidates(self.locations)
            self.data_sources = generate_data_sources()
            self.kpis = generate_kpis(self.locations)
            self.model_performance = generate_model_performance()
            self.demographics = generate_demographics()
            self.alerts = generate_alerts()
            self.network_metrics = generate_network_metrics()
            self.closest_competitors = generate_closest_competitors()
            self.isochrones = []

    @property
    def mode(self) -> str:
        return self._mode


db = DataStore()
