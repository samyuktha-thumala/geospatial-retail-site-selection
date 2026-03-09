"""Synthetic data generation for the Retail Site Selection Platform."""

import random
import math
from ..models import (
    LocationOut, CompetitorOut, HotspotOut, ClosureCandidateOut,
    ClosureMetric,
    DataSourceOut, DataSourceStat, ChartDataPoint,
    KpiOut, ModelPerformanceOut, FeatureImportance,
    DemographicsOut, DistributionBucket,
    AlertOut, AlertType, AlertSeverity,
    NetworkMetricsOut, ClosestCompetitorOut,
    StoreFormat, Urbanicity,
)

random.seed(42)

# New York State metro areas and cities
MSAS = [
    ("Manhattan", 40.7580, -73.9855),
    ("Brooklyn", 40.6782, -73.9442),
    ("Queens", 40.7282, -73.7949),
    ("Bronx", 40.8448, -73.8648),
    ("Staten Island", 40.5795, -74.1502),
    ("Long Island", 40.7891, -73.1350),
    ("Westchester", 41.0340, -73.7629),
    ("Albany", 42.6526, -73.7562),
    ("Buffalo", 42.8864, -78.8784),
    ("Rochester", 43.1566, -77.6088),
    ("Syracuse", 43.0481, -76.1474),
    ("Yonkers", 40.9312, -73.8987),
    ("White Plains", 41.0340, -73.7629),
    ("New Rochelle", 40.9115, -73.7824),
    ("Poughkeepsie", 41.7004, -73.9210),
]

COMPETITOR_BRANDS = ["Target", "Walmart", "Costco", "Whole Foods", "Trader Joe's"]

FORMATS = [StoreFormat.EXPRESS, StoreFormat.STANDARD, StoreFormat.FLAGSHIP]
URBANICITIES = [Urbanicity.URBAN_CORE, Urbanicity.URBAN, Urbanicity.SUBURBAN, Urbanicity.EXURBAN, Urbanicity.RURAL]
FORMAT_WEIGHTS = [0.4, 0.45, 0.15]
URBANICITY_WEIGHTS = [0.15, 0.30, 0.35, 0.12, 0.08]


def _jitter(center: float, spread: float = 0.15) -> float:
    return center + random.uniform(-spread, spread)


def generate_locations() -> list[LocationOut]:
    locations: list[LocationOut] = []
    idx = 0
    for msa_name, lat, lng in MSAS:
        count = random.randint(7, 14)
        for i in range(count):
            idx += 1
            fmt = random.choices(FORMATS, weights=FORMAT_WEIGHTS, k=1)[0]
            urb = random.choices(URBANICITIES, weights=URBANICITY_WEIGHTS, k=1)[0]

            base_sales = {"express": 45, "standard": 82, "flagship": 135}[fmt.value]
            sales = base_sales + random.gauss(0, base_sales * 0.2)
            sales = max(15, sales)

            locations.append(LocationOut(
                id=f"ST{idx:03d}",
                name=f"{msa_name} {fmt.value.title()} #{i+1}",
                lat=_jitter(lat),
                lng=_jitter(lng),
                format=fmt,
                urbanicity=urb,
                monthly_sales=round(sales, 1),
                store_age_years=round(random.uniform(1, 25), 1),
            ))
    return locations


def generate_competitors() -> list[CompetitorOut]:
    competitors: list[CompetitorOut] = []
    idx = 0
    for msa_name, lat, lng in MSAS:
        count = random.randint(10, 18)
        for _ in range(count):
            idx += 1
            brand = random.choice(COMPETITOR_BRANDS)
            year = random.choices(
                range(2018, 2031),
                weights=[3, 4, 5, 6, 7, 7, 6, 5, 4, 3, 2, 2, 1],
                k=1
            )[0]
            competitors.append(CompetitorOut(
                id=f"COMP{idx:04d}",
                lat=_jitter(lat, 0.2),
                lng=_jitter(lng, 0.2),
                brand=brand,
                open_year=year,
                is_projected=year > 2025,
            ))
    return competitors


def generate_hotspots(locations: list[LocationOut]) -> list[HotspotOut]:
    hotspots: list[HotspotOut] = []
    idx = 0
    for msa_name, lat, lng in MSAS:
        count = random.randint(1, 3)
        for _ in range(count):
            idx += 1
            fmt = random.choices(FORMATS, weights=FORMAT_WEIGHTS, k=1)[0]
            urb = random.choices(URBANICITIES, weights=URBANICITY_WEIGHTS, k=1)[0]
            score = round(random.uniform(60, 98), 1)
            base_sales = {"express": 52, "standard": 95, "flagship": 155}[fmt.value]
            projected = base_sales + random.gauss(0, base_sales * 0.15)

            hotspots.append(HotspotOut(
                id=f"HS{idx:03d}",
                lat=_jitter(lat, 0.18),
                lng=_jitter(lng, 0.18),
                score=score,
                projected_sales=round(max(30, projected), 1),
                format=fmt,
                urbanicity=urb,
            ))
    return hotspots


def _metrics_for_reason(reason: str, loc: LocationOut) -> list[ClosureMetric]:
    """Generate backing data metrics relevant to the closure reason."""
    if "Below-threshold revenue" in reason:
        store_6mo = round(loc.monthly_sales * 6, 1)
        return [
            ClosureMetric(label="6-Month Revenue", store_value=f"${store_6mo}K", network_avg="$462K"),
            ClosureMetric(label="Monthly Avg Revenue", store_value=f"${loc.monthly_sales}K", network_avg="$77K"),
            ClosureMetric(label="Revenue Threshold", store_value="Below", network_avg="$55K/mo"),
        ]
    elif "foot traffic" in reason:
        store_ft = round(random.uniform(120, 280))
        return [
            ClosureMetric(label="Daily Foot Traffic", store_value=f"{store_ft}", network_avg="520"),
            ClosureMetric(label="YoY Foot Traffic Change", store_value="-23%", network_avg="+4.1%"),
            ClosureMetric(label="Conversion Rate", store_value=f"{round(random.uniform(8, 15), 1)}%", network_avg="18.3%"),
        ]
    elif "competitor" in reason.lower() and "cannibalization" not in reason.lower():
        dist = round(random.uniform(0.1, 0.5), 2)
        return [
            ClosureMetric(label="Nearest Competitor Distance", store_value=f"{dist} mi", network_avg="1.8 mi"),
            ClosureMetric(label="Competitors within 1mi", store_value=f"{random.randint(3, 6)}", network_avg="1.4"),
            ClosureMetric(label="Revenue Impact Since Opening", store_value=f"-{random.randint(12, 28)}%", network_avg="-3.2%"),
        ]
    elif "Lease renewal" in reason:
        current = round(random.uniform(8, 14), 1)
        new_rate = round(current * 2.3, 1)
        return [
            ClosureMetric(label="Current Lease Rate", store_value=f"${current}/sqft", network_avg="$11.20/sqft"),
            ClosureMetric(label="Proposed Lease Rate", store_value=f"${new_rate}/sqft", network_avg="$12.50/sqft"),
            ClosureMetric(label="Rent-to-Revenue Ratio", store_value=f"{round(random.uniform(28, 42), 1)}%", network_avg="18.5%"),
        ]
    elif "Population decline" in reason:
        return [
            ClosureMetric(label="Trade Area Population Change", store_value="-8%", network_avg="+1.2%"),
            ClosureMetric(label="Trade Area Population", store_value=f"{random.randint(8, 18)}K", network_avg="32K"),
            ClosureMetric(label="Household Growth Rate", store_value=f"-{round(random.uniform(3, 9), 1)}%", network_avg="+2.1%"),
        ]
    else:  # cannibalization
        cannibal_pct = random.randint(15, 35)
        return [
            ClosureMetric(label="Revenue Cannibalized", store_value=f"{cannibal_pct}%", network_avg="4.2%"),
            ClosureMetric(label="Overlap with Nearest Store", store_value=f"{round(random.uniform(0.3, 1.2), 1)} mi", network_avg="3.8 mi"),
            ClosureMetric(label="Shared Customer Base", store_value=f"{random.randint(25, 45)}%", network_avg="8%"),
        ]


def generate_closure_candidates(locations: list[LocationOut]) -> list[ClosureCandidateOut]:
    candidates: list[ClosureCandidateOut] = []
    low_performers = sorted(locations, key=lambda l: l.monthly_sales)[:12]
    reasons = [
        "Declining foot traffic (-23% YoY)",
        "New competitor within 0.5mi radius",
        "Lease renewal at 2.3x current rate",
        "Below-threshold revenue for 6+ months",
        "Population decline in trade area (-8%)",
        "High cannibalization from nearby location",
    ]
    for loc in low_performers[:10]:
        risk = round(random.uniform(55, 95), 1)
        reason = random.choice(reasons)
        candidates.append(ClosureCandidateOut(
            id=loc.id,
            name=loc.name,
            lat=loc.lat,
            lng=loc.lng,
            format=loc.format,
            urbanicity=loc.urbanicity,
            monthly_sales=loc.monthly_sales,
            closure_risk=risk,
            reason=reason,
            closure_metrics=_metrics_for_reason(reason, loc),
        ))
    return sorted(candidates, key=lambda c: -c.closure_risk)


def generate_data_sources() -> list[DataSourceOut]:
    return [
        DataSourceOut(
            id="ds-transactions",
            name="Transaction Records",
            type="Delta Table",
            icon="database",
            records="2.4M",
            last_sync="2 min ago",
            refresh_rate="Real-time",
            stats=[
                DataSourceStat(key="Avg Transaction", value="$12.47"),
                DataSourceStat(key="Peak Hour", value="8-9 AM"),
                DataSourceStat(key="Avg Basket Size", value="2.3 items"),
                DataSourceStat(key="Digital Orders", value="34%"),
            ],
            chart_data=[
                ChartDataPoint(label="Jan", value=182),
                ChartDataPoint(label="Feb", value=195),
                ChartDataPoint(label="Mar", value=210),
                ChartDataPoint(label="Apr", value=198),
                ChartDataPoint(label="May", value=225),
                ChartDataPoint(label="Jun", value=240),
            ],
            chart_type="line",
        ),
        DataSourceOut(
            id="ds-locations",
            name="Store Locations",
            type="Delta Table",
            icon="map-pin",
            records="156",
            last_sync="1 hr ago",
            refresh_rate="Daily",
            stats=[
                DataSourceStat(key="Active Stores", value="148"),
                DataSourceStat(key="Avg Store Age", value="8.3 yrs"),
                DataSourceStat(key="Avg Sq. Footage", value="1,850"),
                DataSourceStat(key="Regions Covered", value="15"),
            ],
            chart_data=[
                ChartDataPoint(label="Express", value=62),
                ChartDataPoint(label="Standard", value=68),
                ChartDataPoint(label="Flagship", value=26),
            ],
            chart_type="bar",
        ),
        DataSourceOut(
            id="ds-census",
            name="Census Demographics",
            type="External API",
            icon="users",
            records="74K",
            last_sync="12 hr ago",
            refresh_rate="Monthly",
            stats=[
                DataSourceStat(key="Median Income", value="$67,521"),
                DataSourceStat(key="Median Age", value="38.2"),
                DataSourceStat(key="Pop. Density", value="3,420/mi²"),
                DataSourceStat(key="Growth Rate", value="+1.2%"),
            ],
            chart_data=[
                ChartDataPoint(label="18-24", value=14),
                ChartDataPoint(label="25-34", value=22),
                ChartDataPoint(label="35-44", value=20),
                ChartDataPoint(label="45-54", value=18),
                ChartDataPoint(label="55-64", value=15),
                ChartDataPoint(label="65+", value=11),
            ],
            chart_type="bar",
        ),
        DataSourceOut(
            id="ds-competitors",
            name="Competitor Intelligence",
            type="Third Party Vendor",
            icon="building",
            records="1.2K",
            last_sync="6 hr ago",
            refresh_rate="Weekly",
            stats=[
                DataSourceStat(key="Tracked Brands", value="5"),
                DataSourceStat(key="Avg Proximity", value="1.8 mi"),
                DataSourceStat(key="New Openings (YTD)", value="47"),
                DataSourceStat(key="Closures (YTD)", value="12"),
            ],
            chart_data=[
                ChartDataPoint(label="Target", value=420),
                ChartDataPoint(label="Walmart", value=310),
                ChartDataPoint(label="Costco", value=185),
                ChartDataPoint(label="Whole Foods", value=160),
                ChartDataPoint(label="Trader Joe's", value=125),
            ],
            chart_type="bar",
        ),
        DataSourceOut(
            id="ds-osm-poi",
            name="OpenStreetMap POI Data",
            type="External API",
            icon="map-pin",
            records="312K",
            last_sync="3 hr ago",
            refresh_rate="Weekly",
            stats=[
                DataSourceStat(key="POI Categories", value="48"),
                DataSourceStat(key="Retail POIs", value="89,420"),
                DataSourceStat(key="Food & Dining", value="67,310"),
                DataSourceStat(key="Avg POI Density", value="142/mi²"),
                DataSourceStat(key="Coverage Area", value="New York State"),
                DataSourceStat(key="Last OSM Update", value="2026-02-28"),
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
        ),
        DataSourceOut(
            id="ds-census-detailed",
            name="Census Bureau ACS Data",
            type="External API",
            icon="users",
            records="218K",
            last_sync="24 hr ago",
            refresh_rate="Monthly",
            stats=[
                DataSourceStat(key="Census Tracts", value="12,480"),
                DataSourceStat(key="Median HH Income", value="$67,521"),
                DataSourceStat(key="Median Age", value="38.2 yrs"),
                DataSourceStat(key="Pop. Density", value="3,420/mi²"),
                DataSourceStat(key="College Educated", value="33.7%"),
                DataSourceStat(key="Homeownership", value="64.8%"),
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
        ),
    ]


def generate_kpis(locations: list[LocationOut]) -> list[KpiOut]:
    total = len(locations)
    avg_rev = sum(l.monthly_sales for l in locations) / total
    express_rev = sum(l.monthly_sales for l in locations if l.format == StoreFormat.EXPRESS) / max(1, sum(1 for l in locations if l.format == StoreFormat.EXPRESS))
    standard_rev = sum(l.monthly_sales for l in locations if l.format == StoreFormat.STANDARD) / max(1, sum(1 for l in locations if l.format == StoreFormat.STANDARD))
    flagship_rev = sum(l.monthly_sales for l in locations if l.format == StoreFormat.FLAGSHIP) / max(1, sum(1 for l in locations if l.format == StoreFormat.FLAGSHIP))
    avg_age = sum(l.store_age_years for l in locations) / total

    return [
        KpiOut(label="Total Locations", value=str(total), trend=2.4, trend_label="+2.4% YoY", icon="store"),
        KpiOut(label="Avg Revenue", value=f"${avg_rev:.0f}K/mo", trend=5.1, trend_label="+5.1% YoY", icon="dollar-sign"),
        KpiOut(label="Express Revenue", value=f"${express_rev:.0f}K/mo", trend=3.2, trend_label="+3.2%", icon="trending-up"),
        KpiOut(label="Standard Revenue", value=f"${standard_rev:.0f}K/mo", trend=4.8, trend_label="+4.8%", icon="trending-up"),
        KpiOut(label="Flagship Revenue", value=f"${flagship_rev:.0f}K/mo", trend=7.3, trend_label="+7.3%", icon="trending-up"),
        KpiOut(label="Avg Store Age", value=f"{avg_age:.1f} yrs", trend=-0.5, trend_label="-0.5 yrs", icon="calendar"),
    ]


def generate_model_performance() -> list[ModelPerformanceOut]:
    return [
        ModelPerformanceOut(
            format=StoreFormat.EXPRESS,
            store_count=62,
            r_squared=0.847,
            mae="$8.2K",
            rmse="$11.4K",
            top_features=[
                FeatureImportance(name="Foot Traffic Index", importance=0.89, direction="positive", category="transactional"),
                FeatureImportance(name="Population Density", importance=0.76, direction="positive", category="demographics"),
                FeatureImportance(name="Competitor Count (1mi)", importance=0.68, direction="negative", category="competition"),
                FeatureImportance(name="Transit Score", importance=0.62, direction="positive", category="poi"),
                FeatureImportance(name="Median Income", importance=0.55, direction="positive", category="demographics"),
            ],
        ),
        ModelPerformanceOut(
            format=StoreFormat.STANDARD,
            store_count=68,
            r_squared=0.891,
            mae="$12.5K",
            rmse="$16.8K",
            top_features=[
                FeatureImportance(name="Trade Area Population", importance=0.92, direction="positive", category="demographics"),
                FeatureImportance(name="Avg Transaction Value", importance=0.84, direction="positive", category="transactional"),
                FeatureImportance(name="Parking Availability", importance=0.71, direction="positive", category="poi"),
                FeatureImportance(name="Competitor Proximity", importance=0.65, direction="negative", category="competition"),
                FeatureImportance(name="Household Income", importance=0.58, direction="positive", category="demographics"),
            ],
        ),
        ModelPerformanceOut(
            format=StoreFormat.FLAGSHIP,
            store_count=26,
            r_squared=0.912,
            mae="$18.7K",
            rmse="$24.1K",
            top_features=[
                FeatureImportance(name="Brand Awareness Index", importance=0.95, direction="positive", category="transactional"),
                FeatureImportance(name="Tourism Score", importance=0.88, direction="positive", category="poi"),
                FeatureImportance(name="Median Income (3mi)", importance=0.82, direction="positive", category="demographics"),
                FeatureImportance(name="Retail Density", importance=0.73, direction="positive", category="poi"),
                FeatureImportance(name="Competitor Premium Ratio", importance=0.61, direction="negative", category="competition"),
            ],
        ),
    ]


def generate_demographics() -> DemographicsOut:
    return DemographicsOut(
        age_distribution=[
            DistributionBucket(label="18-24", value=14.2, color="#3b82f6"),
            DistributionBucket(label="25-34", value=22.1, color="#3b82f6"),
            DistributionBucket(label="35-44", value=19.8, color="#3b82f6"),
            DistributionBucket(label="45-54", value=17.5, color="#3b82f6"),
            DistributionBucket(label="55-64", value=15.3, color="#3b82f6"),
            DistributionBucket(label="65+", value=11.1, color="#3b82f6"),
        ],
        income_distribution=[
            DistributionBucket(label="<$25K", value=12.3, color="#8b5cf6"),
            DistributionBucket(label="$25-50K", value=18.7, color="#8b5cf6"),
            DistributionBucket(label="$50-75K", value=22.4, color="#8b5cf6"),
            DistributionBucket(label="$75-100K", value=19.8, color="#8b5cf6"),
            DistributionBucket(label="$100-150K", value=15.6, color="#8b5cf6"),
            DistributionBucket(label="$150K+", value=11.2, color="#8b5cf6"),
        ],
    )


def generate_alerts() -> list[AlertOut]:
    return [
        AlertOut(
            id=1,
            type=AlertType.WARNING,
            title="High Closure Risk Detected",
            message="3 locations in the Buffalo MSA have exceeded the 80% closure risk threshold. Recommend immediate review.",
            time="2 hours ago",
            severity=AlertSeverity.HIGH,
        ),
        AlertOut(
            id=2,
            type=AlertType.WARNING,
            title="Competitor Expansion Alert",
            message="Starbucks has filed permits for 5 new locations within your primary trade areas in Brooklyn and Queens.",
            time="6 hours ago",
            severity=AlertSeverity.MEDIUM,
        ),
        AlertOut(
            id=3,
            type=AlertType.INFO,
            title="Model Refresh Complete",
            message="Revenue prediction models have been retrained with Q4 data. R² improved from 0.87 to 0.89 across all formats.",
            time="1 day ago",
            severity=AlertSeverity.LOW,
        ),
    ]


def generate_network_metrics() -> NetworkMetricsOut:
    return NetworkMetricsOut(
        total_current_revenue="$12.4M",
        projected_optimized_revenue="$14.8M",
        revenue_uplift="+19.4%",
        new_locations_recommended=12,
    )


def generate_closest_competitors() -> list[ClosestCompetitorOut]:
    return [
        ClosestCompetitorOut(brand="Target", distance_miles=0.3, location_name="Main St & 5th Ave"),
        ClosestCompetitorOut(brand="Walmart", distance_miles=0.7, location_name="Central Plaza"),
        ClosestCompetitorOut(brand="Costco", distance_miles=1.2, location_name="Riverside Mall"),
        ClosestCompetitorOut(brand="Whole Foods", distance_miles=1.8, location_name="Highway 101 Plaza"),
        ClosestCompetitorOut(brand="Trader Joe's", distance_miles=2.4, location_name="Westfield Center"),
    ]
