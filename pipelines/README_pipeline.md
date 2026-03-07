# Site Selection Demo

Geospatial analytics platform for retail site selection using Databricks, Unity Catalog, and Valhalla routing.

## Architecture

**Medallion pattern** with Bronze → Silver → Gold layers, orchestrated via Databricks Asset Bundles (DABs).

```
site-selection-demo/
├── databricks.yml                    # DABs bundle configuration
├── app/                              # Streamlit dashboard
│   ├── app.py                        # Main app (3 tabs)
│   ├── app.yaml                      # Databricks App config
│   └── requirements.txt
├── resources/                        # DABs job definitions
│   ├── bronze_job.yml                # Bronze ingestion job
│   ├── silver_job.yml                # Silver processing job
│   ├── gold_job.yml                  # Gold feature engineering job
│   ├── orchestration_job.yml         # End-to-end pipeline
│   ├── catalog_setup.yml             # Unity Catalog setup
│   ├── init_scripts/                 # Cluster init scripts
│   │   └── init-valhalla.sh          # Valhalla routing engine setup
│   └── configs/                      # Feature/variable configs
│       ├── census_variables.yml
│       ├── poi_config.yml
│       ├── h3_features_config.yml
│       └── isochrone_config.yml
├── transformations/
│   ├── 01_bronze/                    # Raw data ingestion
│   │   ├── osm_download.ipynb        # Geofabrik OSM PBF download
│   │   ├── census_demographics.ipynb # Census ACS API ingestion
│   │   ├── census_boundaries.ipynb   # TIGER/Line boundaries
│   │   └── extract_pois.ipynb        # POI extraction from OSM
│   ├── 02_silver/                    # Data processing
│   │   ├── clean_pois.ipynb          # POI cleaning and categorization
│   │   └── urbanicity_isochrones_valhalla.ipynb  # Drive-time polygon generation
│   └── 03_gold/                      # Feature engineering
│       ├── create_h3_features.ipynb  # H3 hexagon aggregations
│       ├── aggregate_trade_area_features.ipynb  # Trade area metrics
│       └── predict_seed_point_sales.ipynb       # Sales prediction model
└── exploration/                      # Analysis notebooks
    ├── generate_rmc_retail_locations.ipynb
    ├── generate_competitor_locations.ipynb
    └── sales_driver_analysis.ipynb
```

## Data Pipeline

### Bronze Layer
- **OSM Road Network**: Geofabrik PBF files for routing graph
- **Census Demographics**: ACS 5-Year estimates via Census API
- **Census Boundaries**: Block groups, tracts, counties (TIGER/Line)
- **POIs**: Points of interest extracted from OSM

### Silver Layer
- **Isochrones**: Drive-time polygons (5/10/15/20/30 min) via Valhalla
- **Cleaned POIs**: Categorized and deduplicated
- **Urbanicity Classification**: Urban/suburban/rural based on population density

### Gold Layer
- **H3 Features**: Demographics and POI counts at H3 resolution 8
- **Trade Area Features**: Aggregated metrics per isochrone polygon
- **Sales Predictions**: Model-based revenue forecasting for expansion sites

## Streamlit Application

Three-tab dashboard for site analysis:

1. **Store Detail Analysis**: Individual store performance metrics, trade area demographics, nearby POIs
2. **Expansion Candidates**: Map of potential new locations with urbanicity filtering and sales estimates
3. **Network Optimizer**: Greedy algorithm to select optimal N locations maximizing coverage and revenue

Features:
- PyDeck map visualizations with H3 hexagons
- Real-time SQL queries to Unity Catalog
- Session state persistence for optimization results
- Export to Delta table

## Deployment

### Prerequisites
- Databricks workspace with Unity Catalog
- Catalog: `geo_site_selection`
- Schemas: `bronze`, `silver`, `gold`
- Environment variables: `DATABRICKS_TOKEN`, `CENSUS_API_KEY`

### Deploy Bundle
```bash
source .env
databricks bundle deploy --target dev
```

### Run Jobs
```bash
databricks bundle run bronze_ingestion --target dev
databricks bundle run silver_processing --target dev
databricks bundle run gold_feature_engineering --target dev
```

### Deploy App
```bash
databricks apps deploy rmc-site-selection --source-code-path app/
```

## Configuration

All job parameters are configurable via `databricks.yml` variables:
- `catalog`: Unity Catalog name
- `state_fips`: Target state (25 = Massachusetts)
- `drive_time_buckets`: Isochrone intervals
- `node_type`: Cluster node type (Standard_D4s_v3)

Spark version: `17.3.x-scala2.13` with Photon runtime (required for ST_* geospatial functions).
