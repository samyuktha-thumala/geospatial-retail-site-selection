"""SQL-backed data fetchers for Databricks catalog tables."""

import logging
import math
from ..models import (
    LocationOut, CompetitorOut, HotspotOut, ClosureCandidateOut,
    ClosureMetric, DataSourceOut, DataSourceStat, ChartDataPoint,
    KpiOut, ModelPerformanceOut, FeatureImportance,
    DemographicsOut, DistributionBucket,
    AlertOut, NetworkMetricsOut, ClosestCompetitorOut,
    IsochroneOut, H3CellFeature, H3FeatureCollection,
    StoreFormat, Urbanicity,
)
from . import sql_client as sql

logger = logging.getLogger(__name__)


def fetch_locations() -> list[LocationOut]:
    """Fetch store locations joined with sales data from gold_store_features_and_sales."""
    rows = sql.execute_sql(f"""
        SELECT
            s.store_number,
            s.name,
            s.lat,
            s.lng,
            s.format,
            s.urbanicity,
            COALESCE(g.monthly_sales, 0) as monthly_sales,
            2.0 as store_age_years
        FROM {sql.table('bronze_store_locations')} s
        LEFT JOIN (
            SELECT location_id,
                   (jan_sales + feb_sales + mar_sales + apr_sales + may_sales + jun_sales +
                    jul_sales + aug_sales + sep_sales + oct_sales + nov_sales + dec_sales) / 12.0 as monthly_sales
            FROM {sql.table('gold_store_features_and_sales')}
        ) g ON CAST(s.store_number AS STRING) = g.location_id
        ORDER BY s.store_number
    """, cache_key="locations")

    return [
        LocationOut(
            id=f"LOC{r['store_number']}",
            name=r['name'] or f"Store #{r['store_number']}",
            lat=float(r['lat']),
            lng=float(r['lng']),
            format=StoreFormat(r['format']),
            urbanicity=_map_urbanicity(r['urbanicity']),
            monthly_sales=round(float(r['monthly_sales']) / 1000, 1),
            store_age_years=float(r['store_age_years']),
        )
        for r in rows
    ]


def fetch_competitors() -> list[CompetitorOut]:
    """Fetch competitor locations including simulated future growth."""
    rows = sql.execute_sql(f"""
        SELECT competitor_id, brand, lat, lng, open_year, 0 as is_projected
        FROM {sql.table('bronze_competitor_locations')}
        UNION ALL
        SELECT
            ROW_NUMBER() OVER (ORDER BY simulated_year, brand) + 10000 as competitor_id,
            brand, latitude as lat, longitude as lng, simulated_year as open_year, 1 as is_projected
        FROM {sql.table('gold_simulated_competitor_growth')}
        ORDER BY competitor_id
    """, cache_key="competitors")

    return [
        CompetitorOut(
            id=f"COMP{r['competitor_id']}",
            lat=float(r['lat']),
            lng=float(r['lng']),
            brand=r['brand'],
            open_year=int(r['open_year']),
            is_projected=int(r['is_projected']) == 1,
        )
        for r in rows
    ]


def fetch_hotspots() -> list[HotspotOut]:
    """Fetch expansion candidates as hotspots."""
    rows = sql.execute_sql(f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY recommended_revenue DESC) as rank_id,
            lat, lng, recommended_revenue, recommended_format, urbanicity_category,
            recommended_rev_per_sqft, total_population, median_household_income
        FROM {sql.table('gold_expansion_candidates')}
        ORDER BY recommended_revenue DESC
    """, cache_key="hotspots")

    return [
        HotspotOut(
            id=f"HS{r['rank_id']}",
            lat=float(r['lat']),
            lng=float(r['lng']),
            score=round(min(100, float(r['recommended_revenue']) / 50000), 1),
            projected_sales=round(float(r['recommended_revenue']) / 12000, 1),
            format=StoreFormat(r.get('recommended_format', 'standard')),
            urbanicity=_map_urbanicity(r.get('urbanicity_category', 'suburban')),
        )
        for r in rows
    ]


def fetch_closure_candidates() -> list[ClosureCandidateOut]:
    """Fetch bottom-performing stores as closure candidates."""
    rows = sql.execute_sql(f"""
        WITH store_sales AS (
            SELECT
                g.location_id, s.name, s.lat, s.lng, s.format, s.urbanicity,
                (g.jan_sales + g.feb_sales + g.mar_sales + g.apr_sales + g.may_sales + g.jun_sales +
                 g.jul_sales + g.aug_sales + g.sep_sales + g.oct_sales + g.nov_sales + g.dec_sales) / 12.0 as avg_monthly_sales
            FROM {sql.table('gold_store_features_and_sales')} g
            JOIN {sql.table('bronze_store_locations')} s ON CAST(s.store_number AS STRING) = g.location_id
        ),
        network_avgs AS (
            SELECT format, AVG(avg_monthly_sales) as network_avg
            FROM store_sales
            GROUP BY format
        )
        SELECT
            ss.location_id, ss.name, ss.lat, ss.lng, ss.format, ss.urbanicity,
            ss.avg_monthly_sales, na.network_avg,
            ROUND((1 - ss.avg_monthly_sales / NULLIF(na.network_avg, 0)) * 100, 1) as closure_risk
        FROM store_sales ss
        JOIN network_avgs na ON ss.format = na.format
        WHERE ss.avg_monthly_sales < na.network_avg * 0.6
        ORDER BY closure_risk DESC
        LIMIT 15
    """, cache_key="closure_candidates")

    results = []
    for r in rows:
        risk = max(0, min(100, float(r.get('closure_risk', 50))))
        monthly = float(r['avg_monthly_sales']) / 1000
        net_avg = float(r['network_avg']) / 1000

        reason = "Underperforming vs. network average"
        if risk > 80:
            reason = "Severely underperforming — immediate review recommended"
        elif risk > 60:
            reason = "Significant underperformance relative to format peers"

        results.append(ClosureCandidateOut(
            id=f"LOC{r['location_id']}",
            name=r['name'] or f"Store #{r['location_id']}",
            lat=float(r['lat']),
            lng=float(r['lng']),
            format=StoreFormat(r['format']),
            urbanicity=_map_urbanicity(r['urbanicity']),
            monthly_sales=round(monthly, 1),
            closure_risk=round(risk, 1),
            reason=reason,
            closure_metrics=[
                ClosureMetric(label="Monthly Sales", store_value=f"${monthly:.0f}K", network_avg=f"${net_avg:.0f}K"),
                ClosureMetric(label="Performance Ratio", store_value=f"{(monthly/net_avg*100):.0f}%", network_avg="100%"),
            ],
        ))
    return results


def fetch_kpis(locations: list[LocationOut] | None = None) -> list[KpiOut]:
    """Fetch KPIs from gold_store_features_and_sales — annual values with monthly subtext."""
    rows = sql.execute_sql(f"""
        SELECT
            COUNT(*) as store_count,
            SUM(annual_revenue) / 1e6 as total_annual_rev,
            AVG(annual_revenue) as avg_annual_sales
        FROM {sql.table('gold_store_features_and_sales')}
    """, cache_key="kpis_summary")

    if not rows:
        return []

    r = rows[0]
    store_count = int(r['store_count'] or 0)
    total_annual_m = float(r['total_annual_rev'] or 0)  # already in millions
    avg_annual = float(r['avg_annual_sales'] or 0)  # raw dollars

    comp_count_rows = sql.execute_sql(f"""
        SELECT COUNT(*) as cnt FROM {sql.table('bronze_competitor_locations')}
    """, cache_key="competitor_count")
    comp_count = int(comp_count_rows[0]['cnt']) if comp_count_rows else 0

    def fmt_dollars(val_dollars: float) -> str:
        """Format dollar amount with appropriate suffix."""
        if abs(val_dollars) >= 1e9:
            return f"${val_dollars/1e9:.1f}B"
        if abs(val_dollars) >= 1e6:
            return f"${val_dollars/1e6:.1f}M"
        if abs(val_dollars) >= 1e3:
            return f"${val_dollars/1e3:.0f}K"
        return f"${val_dollars:.0f}"

    total_annual_dollars = total_annual_m * 1e6

    return [
        KpiOut(label="Active Locations", value=str(store_count), icon="MapPin"),
        KpiOut(label="Annual Revenue", value=fmt_dollars(total_annual_dollars), subtext=f"{fmt_dollars(total_annual_dollars/12)} avg monthly (T12M)", icon="DollarSign"),
        KpiOut(label="Avg Store Revenue", value=fmt_dollars(avg_annual), subtext=f"{fmt_dollars(avg_annual/12)} avg monthly (T12M)", icon="TrendingUp"),
        KpiOut(label="Competitors Tracked", value=str(comp_count), icon="Target"),
    ]


def fetch_model_performance() -> list[ModelPerformanceOut]:
    """Fetch model performance from gold_model_artifacts."""
    try:
        rows = sql.execute_sql(f"""
            SELECT model_name, model_version, n_samples, n_features,
                   test_r2, test_mae, test_rmse, test_mape,
                   cv_r2_mean, feature_importance_json
            FROM {sql.table('gold_model_artifacts')}
        """, cache_key="model_performance")

        if not rows:
            return []

        r = rows[0]
        features = []
        if r.get('feature_importance_json'):
            import json
            fi = json.loads(r['feature_importance_json']) if isinstance(r['feature_importance_json'], str) else r['feature_importance_json']
            # fi is a dict of {feature_name: importance_score}
            for name, importance in list(fi.items())[:8]:
                direction = "negative" if name in ("total_competitor_count", "unemployment_rate", "sqft_market_ratio") else "positive"
                category = "competition" if "competitor" in name or "distance" in name else "demographic" if name in ("total_population", "median_household_income", "higher_education_rate", "unemployment_rate") else "location"
                features.append(FeatureImportance(
                    name=name,
                    importance=round(float(importance), 4),
                    direction=direction,
                    category=category,
                ))

        # Return one entry per format with same model metrics
        results = []
        for fmt in ["express", "standard", "flagship"]:
            results.append(ModelPerformanceOut(
                format=StoreFormat(fmt),
                store_count=int(r['n_samples']),
                r_squared=round(float(r['test_r2']), 3),
                mae=f"${float(r['test_mae'])/1000:.0f}K",
                rmse=f"${float(r['test_rmse'])/1000:.0f}K",
                top_features=features,
            ))
        return results
    except Exception as e:
        logger.warning(f"Model performance fetch failed: {e}")
        return []


def fetch_demographics() -> DemographicsOut:
    """Fetch demographics from census data — uses available columns."""
    table = sql.table('bronze_census_demographics')

    # Income distribution: bucket block groups by median_household_income
    rows = sql.execute_sql(f"""
        SELECT
            SUM(CASE WHEN median_household_income < 25000 THEN total_population ELSE 0 END) as income_low,
            SUM(CASE WHEN median_household_income >= 25000 AND median_household_income < 50000 THEN total_population ELSE 0 END) as income_mid_low,
            SUM(CASE WHEN median_household_income >= 50000 AND median_household_income < 100000 THEN total_population ELSE 0 END) as income_mid,
            SUM(CASE WHEN median_household_income >= 100000 AND median_household_income < 150000 THEN total_population ELSE 0 END) as income_mid_high,
            SUM(CASE WHEN median_household_income >= 150000 THEN total_population ELSE 0 END) as income_high,
            SUM(total_population) as total_pop,
            ROUND(AVG(median_age), 1) as avg_age,
            SUM(bachelors_degree + masters_degree + doctorate_degree) as higher_ed,
            SUM(unemployed) as unemployed,
            SUM(in_labor_force) as labor_force,
            SUM(owner_occupied) as owner_occ,
            SUM(total_housing_units) as total_housing
        FROM {table}
        WHERE total_population > 0
    """, cache_key="demographics")

    if not rows:
        return DemographicsOut(age_distribution=[], income_distribution=[])

    r = rows[0]
    total_pop = float(r['total_pop'] or 1)

    # Age distribution: use median_age buckets across block groups
    age_rows = sql.execute_sql(f"""
        SELECT
            SUM(CASE WHEN median_age < 25 THEN total_population ELSE 0 END) as age_young,
            SUM(CASE WHEN median_age >= 25 AND median_age < 35 THEN total_population ELSE 0 END) as age_25_34,
            SUM(CASE WHEN median_age >= 35 AND median_age < 45 THEN total_population ELSE 0 END) as age_35_44,
            SUM(CASE WHEN median_age >= 45 AND median_age < 55 THEN total_population ELSE 0 END) as age_45_54,
            SUM(CASE WHEN median_age >= 55 AND median_age < 65 THEN total_population ELSE 0 END) as age_55_64,
            SUM(CASE WHEN median_age >= 65 THEN total_population ELSE 0 END) as age_65_plus
        FROM {table}
        WHERE total_population > 0
    """, cache_key="demographics_age")

    ar = age_rows[0] if age_rows else {}
    age_dist = [
        DistributionBucket(label="<25", value=round(float(ar.get('age_young', 0) or 0) / total_pop * 100, 1)),
        DistributionBucket(label="25-34", value=round(float(ar.get('age_25_34', 0) or 0) / total_pop * 100, 1)),
        DistributionBucket(label="35-44", value=round(float(ar.get('age_35_44', 0) or 0) / total_pop * 100, 1)),
        DistributionBucket(label="45-54", value=round(float(ar.get('age_45_54', 0) or 0) / total_pop * 100, 1)),
        DistributionBucket(label="55-64", value=round(float(ar.get('age_55_64', 0) or 0) / total_pop * 100, 1)),
        DistributionBucket(label="65+", value=round(float(ar.get('age_65_plus', 0) or 0) / total_pop * 100, 1)),
    ]

    income_total = sum(float(r[k] or 0) for k in ['income_low', 'income_mid_low', 'income_mid', 'income_mid_high', 'income_high'])
    if income_total == 0:
        income_total = 1

    income_dist = [
        DistributionBucket(label="<$25K", value=round(float(r['income_low'] or 0) / income_total * 100, 1), color="#ef4444"),
        DistributionBucket(label="$25-50K", value=round(float(r['income_mid_low'] or 0) / income_total * 100, 1), color="#f59e0b"),
        DistributionBucket(label="$50-100K", value=round(float(r['income_mid'] or 0) / income_total * 100, 1), color="#22c55e"),
        DistributionBucket(label="$100-150K", value=round(float(r['income_mid_high'] or 0) / income_total * 100, 1), color="#3b82f6"),
        DistributionBucket(label="$150K+", value=round(float(r['income_high'] or 0) / income_total * 100, 1), color="#8b5cf6"),
    ]

    return DemographicsOut(age_distribution=age_dist, income_distribution=income_dist)


def fetch_network_metrics() -> NetworkMetricsOut:
    """Compute network metrics from sales data."""
    rows = sql.execute_sql(f"""
        SELECT
            SUM(annual_revenue) / 1e6 as total_annual_rev,
            COUNT(*) as store_count
        FROM {sql.table('gold_store_features_and_sales')}
    """, cache_key="network_metrics_base")

    total_rev = float(rows[0]['total_annual_rev'] or 0) if rows else 0
    store_count = int(rows[0]['store_count'] or 0) if rows else 0

    try:
        exp_rows = sql.execute_sql(f"""
            SELECT COUNT(*) as cnt, SUM(recommended_revenue) / 1e6 as projected_rev
            FROM {sql.table('gold_expansion_candidates')}
        """, cache_key="network_metrics_expansion")
        exp_count = int(exp_rows[0]['cnt'] or 0) if exp_rows else 0
        exp_rev = float(exp_rows[0]['projected_rev'] or 0) if exp_rows else 0
    except Exception:
        exp_count = 0
        exp_rev = 0

    projected_total = total_rev + exp_rev * 0.3
    uplift = round((projected_total / max(total_rev, 0.1) - 1) * 100, 1) if total_rev > 0 else 0

    def fmt_m(val_m: float) -> str:
        """Format value in millions with appropriate suffix."""
        val_dollars = val_m * 1e6
        if abs(val_dollars) >= 1e9:
            return f"${val_dollars/1e9:.1f}B"
        if abs(val_dollars) >= 1e6:
            return f"${val_dollars/1e6:.1f}M"
        return f"${val_dollars/1e3:.0f}K"

    return NetworkMetricsOut(
        total_current_revenue=fmt_m(total_rev),
        projected_optimized_revenue=fmt_m(projected_total),
        revenue_uplift=f"+{uplift}%",
        new_locations_recommended=min(exp_count, 30),
        whitespace_locations=exp_count,
    )


def fetch_closest_competitors() -> list[ClosestCompetitorOut]:
    """Fetch closest competitor for each brand."""
    rows = sql.execute_sql(f"""
        WITH store_comp_dist AS (
            SELECT
                c.brand,
                s.name as store_name,
                SQRT(
                    POW((s.lat - c.lat) * 69.0, 2) +
                    POW((s.lng - c.lng) * 69.0 * COS(RADIANS(s.lat)), 2)
                ) as distance_miles
            FROM {sql.table('bronze_store_locations')} s
            CROSS JOIN {sql.table('bronze_competitor_locations')} c
        ),
        closest AS (
            SELECT brand, MIN(distance_miles) as min_dist
            FROM store_comp_dist
            GROUP BY brand
        )
        SELECT
            c.brand,
            ROUND(c.min_dist, 2) as distance_miles,
            scd.store_name
        FROM closest c
        JOIN store_comp_dist scd ON scd.brand = c.brand AND ABS(scd.distance_miles - c.min_dist) < 0.01
        GROUP BY c.brand, c.min_dist, scd.store_name
        ORDER BY c.min_dist
        LIMIT 5
    """, cache_key="closest_competitors")

    return [
        ClosestCompetitorOut(
            brand=r['brand'],
            distance_miles=round(float(r['distance_miles']), 2),
            location_name=r['store_name'] or "Unknown",
        )
        for r in rows
    ]


def fetch_isochrones() -> list[IsochroneOut]:
    """Fetch store isochrone polygons as GeoJSON."""
    try:
        rows = sql.execute_sql(f"""
            SELECT
                location_id,
                ST_AsGeoJSON(ST_GeomFromWKT(geometry_wkt)) as geojson,
                drive_time_minutes,
                COALESCE(urbanicity_category, 'suburban') as urbanicity_category,
                ROUND(area_sqkm, 2) as area_sqkm
            FROM {sql.table('silver_store_isochrones')}
            ORDER BY location_id
        """, cache_key="isochrones")

        import json
        return [
            IsochroneOut(
                store_number=str(r['location_id']),
                geojson=json.loads(r['geojson']) if isinstance(r['geojson'], str) else r['geojson'],
                drive_time_minutes=int(r['drive_time_minutes']),
                urbanicity_category=r['urbanicity_category'],
                area_sqkm=float(r['area_sqkm'] or 0),
            )
            for r in rows
        ]
    except Exception as e:
        logger.warning(f"Isochrone fetch failed: {e}")
        return []


def fetch_data_sources() -> list[DataSourceOut]:
    """Build data source cards from catalog metadata."""
    sources = []

    try:
        loc_count = sql.execute_sql(f"SELECT COUNT(*) as cnt FROM {sql.table('bronze_store_locations')}", cache_key="ds_loc_count")
        cnt = loc_count[0]['cnt'] if loc_count else "0"
        sources.append(DataSourceOut(
            id="ds_locations", name="Store Locations", type="internal", icon="MapPin",
            records=f"{cnt} stores", last_sync="2 min ago", refresh_rate="Daily",
            stats=[DataSourceStat(key="Source", value="Finance")],
            chart_data=[ChartDataPoint(label="Express", value=500), ChartDataPoint(label="Standard", value=700), ChartDataPoint(label="Flagship", value=300)],
            chart_type="bar",
        ))
    except Exception:
        pass

    try:
        comp_count = sql.execute_sql(f"SELECT COUNT(*) as cnt FROM {sql.table('bronze_competitor_locations')}", cache_key="ds_comp_count")
        cnt = comp_count[0]['cnt'] if comp_count else "0"
        sources.append(DataSourceOut(
            id="ds_competitors", name="Competitor Intelligence", type="external", icon="Target",
            records=f"{cnt} locations", last_sync="6 hr ago", refresh_rate="Weekly",
            stats=[DataSourceStat(key="Brands", value="5 tracked")],
            chart_data=[ChartDataPoint(label="A", value=1500), ChartDataPoint(label="B", value=1200), ChartDataPoint(label="C", value=900), ChartDataPoint(label="D", value=800), ChartDataPoint(label="E", value=600)],
            chart_type="bar",
        ))
    except Exception:
        pass

    try:
        demo_count = sql.execute_sql(f"SELECT COUNT(*) as cnt FROM {sql.table('bronze_census_demographics')}", cache_key="ds_demo_count")
        cnt = demo_count[0]['cnt'] if demo_count else "0"
        sources.append(DataSourceOut(
            id="ds_census", name="Census Bureau ACS Data", type="External API", icon="users",
            records=f"{cnt} block groups", last_sync="24 hr ago", refresh_rate="Monthly",
            stats=[
                DataSourceStat(key="Census Tracts", value=str(cnt)),
                DataSourceStat(key="Variables", value="73"),
            ],
            chart_data=[
                ChartDataPoint(label="<$30K", value=18),
                ChartDataPoint(label="$30-50K", value=20),
                ChartDataPoint(label="$50-75K", value=24),
                ChartDataPoint(label="$75-100K", value=17),
                ChartDataPoint(label="$100-150K", value=13),
                ChartDataPoint(label=">$150K", value=8),
            ],
            chart_type="bar",
        ))
    except Exception:
        pass

    try:
        poi_count = sql.execute_sql(f"SELECT COUNT(*) as cnt FROM {sql.table('silver_osm_pois')}", cache_key="ds_poi_count")
        cnt = poi_count[0]['cnt'] if poi_count else "0"
        cnt_display = f"{int(cnt)//1000}K POIs" if cnt else "0"
        sources.append(DataSourceOut(
            id="ds_osm", name="OpenStreetMap POI Data", type="External API", icon="map-pin",
            records=cnt_display, last_sync="3 hr ago", refresh_rate="Weekly",
            stats=[
                DataSourceStat(key="Total POIs", value=str(cnt)),
                DataSourceStat(key="Source", value="Geofabrik"),
            ],
            chart_data=[
                ChartDataPoint(label="Retail", value=89),
                ChartDataPoint(label="Food", value=67),
                ChartDataPoint(label="Services", value=54),
                ChartDataPoint(label="Health", value=38),
                ChartDataPoint(label="Education", value=31),
                ChartDataPoint(label="Transport", value=33),
            ],
            chart_type="bar",
        ))
    except Exception:
        pass

    return sources


def fetch_alerts() -> list[AlertOut]:
    """Generate alerts from data analysis."""
    alerts = []
    try:
        closure = fetch_closure_candidates()
        high_risk = [c for c in closure if c.closure_risk > 75]
        if high_risk:
            alerts.append(AlertOut(
                id=1, type="warning", title="High Closure Risk",
                message=f"{len(high_risk)} stores have closure risk > 75%",
                time="Just now", severity="high",
            ))
    except Exception:
        pass

    alerts.append(AlertOut(
        id=2, type="info", title="Data Refresh Complete",
        message="All catalog tables refreshed from Finance",
        time="Today", severity="low",
    ))

    return alerts


def fetch_h3_features(store_id: str) -> H3FeatureCollection:
    """Fetch H3 cells within a store's trade area with feature properties."""
    import json

    # store_id comes as "LOC123" — extract the number
    location_id = store_id.replace("LOC", "")

    rows = sql.execute_sql(f"""
        WITH iso AS (
            SELECT geometry_wkt
            FROM {sql.table('silver_store_isochrones')}
            WHERE location_id = '{location_id}'
            LIMIT 1
        ),
        h3_cells AS (
            SELECT EXPLODE(h3_polyfillash3string((SELECT geometry_wkt FROM iso), 8)) AS h3_cell_id
        )
        SELECT
            h.h3_cell_id,
            ST_AsGeoJSON(h.h3_geometry) as geojson,
            COALESCE(h.total_population, 0) as total_population,
            COALESCE(h.population_density, 0) as population_density,
            COALESCE(h.median_household_income, 0) as median_household_income,
            COALESCE(h.total_competitor_count, 0) as total_competitor_count,
            COALESCE(h.total_poi_count, 0) as total_poi_count,
            COALESCE(h.median_age, 0) as median_age,
            COALESCE(h.urbanicity_category, 'suburban') as urbanicity_category
        FROM h3_cells c
        JOIN {sql.table('silver_h3_features')} h ON c.h3_cell_id = h.h3_cell_id
    """, cache_key=f"h3_features_{location_id}")

    if not rows:
        return H3FeatureCollection(
            features=[], store_id=store_id, cell_count=0, metric_ranges={}
        )

    metrics = ["total_population", "population_density", "median_household_income",
               "total_competitor_count", "total_poi_count", "median_age"]

    features = []
    metric_vals: dict[str, list[float]] = {m: [] for m in metrics}

    for r in rows:
        geojson_str = r.get("geojson")
        if not geojson_str:
            continue

        geometry = json.loads(geojson_str) if isinstance(geojson_str, str) else geojson_str
        props = {
            "h3_cell_id": r["h3_cell_id"],
            "total_population": float(r["total_population"] or 0),
            "population_density": round(float(r["population_density"] or 0), 1),
            "median_household_income": float(r["median_household_income"] or 0),
            "total_competitor_count": int(r["total_competitor_count"] or 0),
            "total_poi_count": int(r["total_poi_count"] or 0),
            "median_age": round(float(r["median_age"] or 0), 1),
            "urbanicity_category": r["urbanicity_category"],
        }

        for m in metrics:
            metric_vals[m].append(props[m])

        features.append(H3CellFeature(type="Feature", geometry=geometry, properties=props))

    # Compute min/max for each metric
    metric_ranges = {}
    for m in metrics:
        vals = metric_vals[m]
        if vals:
            metric_ranges[m] = {"min": min(vals), "max": max(vals)}
        else:
            metric_ranges[m] = {"min": 0, "max": 0}

    return H3FeatureCollection(
        features=features,
        store_id=store_id,
        cell_count=len(features),
        metric_ranges=metric_ranges,
    )


def _map_urbanicity(val: str | None) -> Urbanicity:
    """Map various urbanicity strings to the enum."""
    if not val:
        return Urbanicity.SUBURBAN
    val = val.lower().strip()
    mapping = {
        "urban": Urbanicity.URBAN,
        "urban_core": Urbanicity.URBAN_CORE,
        "suburban": Urbanicity.SUBURBAN,
        "exurban": Urbanicity.EXURBAN,
        "rural": Urbanicity.RURAL,
    }
    return mapping.get(val, Urbanicity.SUBURBAN)
