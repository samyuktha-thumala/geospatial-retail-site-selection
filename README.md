# Retail SiteLab

**Where should we open the next store?** Traditional site selection relies on gut instinct, spreadsheets, and manual market research — a process that takes months per location and still gets it wrong 30% of the time. This platform combines geospatial analytics, ML-driven revenue prediction, and scenario modeling into a single interactive tool. Retailers using data-driven site selection expand 3-5x faster with higher success rates, and this accelerator proves it's possible with open-source data and Databricks.

The entire pipeline — from Census data ingestion to H3 hexagonal analysis to XGBoost revenue prediction — runs on Databricks with Unity Catalog governance. The app itself is a full-stack Databricks App with a FastAPI backend and React frontend, deployed via Databricks Asset Bundles.

---

![Architecture](docs/architecture.png)

### Stack

| Layer | Technology |
|-------|-----------|
| **Data Platform** | Databricks Unity Catalog, Serverless SQL Warehouses |
| **Pipeline** | Medallion Architecture (Bronze → Silver → Gold), PySpark notebooks |
| **ML** | XGBoost revenue model, MLflow tracking, UC Model Registry |
| **Geospatial** | H3 hexagons (res 8), Valhalla isochrones, Haversine distance |
| **Backend** | FastAPI, Databricks SDK (`WorkspaceClient`), SQL Statement Execution API |
| **Frontend** | React 18, TypeScript, Vite, TanStack Router, Leaflet maps, Recharts, Tailwind CSS |
| **LLM** | Gemini 2.5 Flash via Databricks Foundation Model API |
| **Deployment** | Databricks Apps, Databricks Asset Bundles (DABs) |


| Network Diagnostics | Site Playground |
|---|---|
| ![Network Diagnostics](docs/network-diagnostics.png) | ![Site Playground](docs/site-playground.png) |

### Key Features

- **Network Diagnostics** — Interactive map with H3 trade area analysis, store performance metrics, at-risk detection
- **Site Playground** — Scenario modeling with a greedy optimizer: add/remove locations, tune per-urbanicity distance constraints, compare scenarios side-by-side
- **AI Site Agent** — Natural language Q&A about the store network powered by Gemini 2.5 Flash
- **Revenue Prediction** — XGBoost model predicting $/sqft across 3 store formats (express/standard/flagship), capturing format-market fit dynamics
- **Competitor Simulation** — Projected competitor growth (2026–2028) with brand-specific expansion rates

---

## Installation

### Prerequisites

- A Databricks workspace with Unity Catalog enabled
- Databricks CLI installed and configured (`databricks auth profiles`)
- Python 3.11+ (managed via `uv`)
- Node.js 18+ and npm
- A Serverless SQL Warehouse

### 1. Clone the repo

```bash
git clone https://github.com/samyuktha17/geospatial-retail-site-selection.git
cd geospatial-retail-site-selection
```

### 2. Configure Databricks

There are two bundles and one app config to update. All use placeholder values (`YOUR_*`) — just replace them with your own.

**`databricks.yml`** (app bundle) — set your CLI profile:

```yaml
targets:
  dev:
    workspace:
      profile: YOUR_PROFILE       # ← databricks auth profiles
```

**`pipelines/databricks.yml`** (pipeline bundle) — set your CLI profile, catalog, schema, and Census API key:

```yaml
targets:
  dev:
    workspace:
      profile: YOUR_PROFILE       # ← databricks auth profiles
    variables:
      catalog: your_catalog       # ← Unity Catalog name
      schema: your_schema         # ← schema to create tables in
      census_api_key: "abc123"    # ← free at api.census.gov/data/key_signup.html
      cluster_id: "xxxx-xxxxxx"   # ← cluster with GDAL/pyosmium (for 2 bronze tasks)
```

**`app/app.yaml`** — set your SQL Warehouse ID and catalog/schema:

```yaml
env:
  - name: DATABRICKS_WAREHOUSE_ID
    value: "your-warehouse-id"    # ← from SQL Warehouses page
  - name: DATABRICKS_CATALOG
    value: "your_catalog"         # ← same as above
  - name: DATABRICKS_SCHEMA
    value: "your_schema"          # ← same as above
```

### 3. Run the data pipeline (optional)

> **Note:** The app works without running the pipeline — it falls back to synthetic data. Run the pipeline to populate your catalog with real data.

Clone the repo as a **Git folder** in your Databricks workspace (Workspace → Git folders → Add Git folder). This preserves the directory structure so notebooks can find their config files automatically.

Every notebook takes just two widget parameters: **`catalog`** and **`schema`**. Set these to your Unity Catalog location. All other parameters (state FIPS, OSM URLs, config paths, volumes) have sensible defaults for New York State.

**Additional requirement:** Bronze notebooks that call the Census API need a free API key. Get one at [api.census.gov/data/key_signup.html](https://api.census.gov/data/key_signup.html) and set it in the `census_api_key` widget.

**Run these sequentially — each tier depends on the previous one:**

**Bronze** (raw ingestion):
1. `pipelines/bronze/census_demographics.py` — Census ACS demographics via API
2. `pipelines/bronze/census_boundaries.py` — TIGER/Line block group boundaries
3. `pipelines/bronze/osm_download.py` — OpenStreetMap road network from Geofabrik
4. `pipelines/bronze/extract_pois.py` — Points of Interest from OSM

**Exploration** (synthetic store/competitor data):
1. `pipelines/exploration/generate_store_locations.py` — ~230 store locations across NY
2. `pipelines/exploration/generate_competitor_locations.py` — ~400 competitor locations across NY
3. `pipelines/exploration/generate_seed_points.py` — Expansion candidate seed points
4. `pipelines/exploration/generate_sales_data.py` — Synthetic monthly sales

**Silver** (cleaning & feature engineering):
1. `pipelines/silver/clean_census_demographics.py` — Clean and derive rates
2. `pipelines/silver/census_zcta.py` — ZCTA boundaries + demographics
3. `pipelines/silver/clean_pois.py` — Filter and structure POIs
4. `pipelines/silver/create_h3_features.py` — H3 hex features (demographics, POIs, competitors)
5. `pipelines/silver/generate_seed_points.py` — Score and filter expansion candidates
6. `pipelines/silver/create_isochrones_valhalla.py` — Drive-time trade area polygons

**Gold** (ML & predict revenue):
1. `pipelines/gold/aggregate_trade_area_features.py` — Aggregate features within store trade areas
2. `pipelines/gold/generate_store_sales.py` — Revenue per sqft model
3. `pipelines/gold/train_sales_model.py` — Train XGBoost, log to MLflow
4. `pipelines/gold/aggregate_seed_trade_area_features.py` — Aggregate features for seed points
5. `pipelines/gold/predict_seed_sales.py` — Score seed points with trained model
6. `pipelines/gold/simulate_competitor_growth.py` — Project competitor expansion (2026–2028)

### 4. Build and deploy the app

```bash
# Install frontend dependencies and build
cd app/ui
npm install --legacy-peer-deps
npm run build
cd ../..

# Deploy with Databricks Asset Bundles
databricks bundle deploy --target dev

# Restart the app to pick up new code
databricks apps stop site-selection-dev --no-wait
sleep 15
databricks apps start site-selection-dev --no-wait
```

### 5. Run locally (optional)

The app works locally in synthetic mode (no Databricks connection needed). Requires [uv](https://docs.astral.sh/uv/) for Python dependency management:

```bash
# Backend
cd app
uv run uvicorn backend.main:app --reload --port 8000

# Frontend (separate terminal)
cd app/ui
npm install --legacy-peer-deps
npm run dev
```

Set `DATABRICKS_PROFILE=YOUR_PROFILE` to connect to a live workspace instead.

---

## Project Structure

```
├── databricks.yml                  # DABs bundle config (app deployment)
├── resources/
│   └── site_selection_app.app.yml  # Databricks App resource
├── pipelines/
│   ├── databricks.yml              # DABs bundle config (pipeline jobs)
│   ├── resources/
│   │   ├── configs/                # YAML configs (isochrone, H3, POI, census)
│   │   └── jobs/                   # Job definitions (bronze, silver, gold)
│   ├── bronze/                     # Raw data ingestion (Census, OSM, stores)
│   ├── silver/                     # H3 features, isochrones, cleaned data
│   ├── gold/                       # ML model, scoring, competitor simulation
│   └── exploration/                # Synthetic data generators (stores, competitors)
└── app/
    ├── app.yaml                    # App entrypoint config
    ├── backend/                    # FastAPI backend
    │   ├── main.py                 # App entry, mounts API + SPA
    │   ├── router.py               # API endpoints + greedy optimizer
    │   └── data/
    │       ├── store.py            # Hybrid data store (SQL or synthetic)
    │       ├── sql_client.py       # Databricks SQL execution + caching
    │       ├── fetchers.py         # SQL queries for each data domain
    │       └── gemini_chat.py      # LLM chat via Foundation Model API
    └── ui/                         # React frontend
        └── src/
            ├── routes/             # TanStack Router pages
            ├── components/         # Maps, panels, shared components
            └── lib/                # API client, utilities
```
