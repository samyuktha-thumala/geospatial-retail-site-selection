# Site Selection Accelerator — Design Doc

## Context & Problem Statement

**Why does this matter:** Every new retail location is a ***~$5M bet on geography***. There are over 1M retail locations in the US — get your network right and unlock millions, get it wrong and you're stuck with a liability that takes years to unwind. The core question: **"Where should I build next?"**

**Challenge:** Today, answering that question takes 2-6 months. The analysis spans GIS platforms, custom python scripting, BI dashboards — each with proprietary formats and siloed data. Nothing talks to anything else. What should take days stretches into months.

**Solution:** My app helps Real Estate teams decide where to build next by putting spatial intelligence, ML modeling, and scenario modeling on one platform — ++***so the cycle goes from months to weeks.***++

## Personas

**Primary: Head of Network Strategy / Real Estate**

- Owns the decision on where to open, close, or reformat stores
- Wants speed to decision — board-ready recommendations, not raw data
- Needs scenario flexibility: "what if we close 3 underperformers and open 10?"
- Low tolerance for waiting on analysts to re-run things

**Secondary: Data Scientist**

- Builds the spatial features, trains the models, runs the inference
- Currently stitching together data from GIS tools, Python scripts, and spreadsheets
- Wants one platform where data engineering, ML, and delivery are connected

**Tertiary: CFO / Board**

- Wants a quick read on network health and expansion strategy
- Doesn't interact deeply, but needs to trust the numbers

## Demo Flow (2 min walkthrough)

**1. See Your Network** (Network Diagnostics)

- Get a high-level view of top-line network KPIs — total stores, revenue, competitor count
- Full store network on an interactive map — view your locations by format alongside competitors and at-risk stores
- Toggle between location layers on the map (Express, Standard, Flagship, Competitors, At Risk)
- Surface at-risk stores — those performing below 60% of network average for their format
- Click any store to inspect its trade area features (population density, median income, competition, POI count) and closure risk metrics

**2. Run a Scenario** (Site Playground)

- Toggle map layers (trade areas, hotspots, competitors, at-risk, optimized) to identify high-potential whitespace
- Zoom into specific areas — urban cores, suburban corridors, rural markets
- Visualize projected competitor growth through 2027 with the timeline slider
- Add or remove locations from the network interactively
- Configure optimization constraints: close underperformers, set minimum distance rules by urbanicity (urban / suburban / rural), and request N new locations
- See before/after network metrics — revenue uplift, recommended formats — side by side
- Run multiple scenarios, compare results, and finalize based on demand potential
- Save the final expansion plan

**3. Ask a Question** (AI Chat)

- "Which regions have the highest expansion opportunity?"
- Context-aware answers backed by the same data powering the maps

## Key Capabilities (Prioritized)

### P0 — Core Experience

- **Interactive Network Map** — Leaflet + H3 overlays, filter by format/status, trade area polygons
- **Scenario Optimizer** — Greedy site selection with distance constraints, add/remove stores, compare results
- **Live Data from Warehouse** — App queries Unity Catalog tables via serverless SQL in real time
- **Hybrid Fallback** — If warehouse is unavailable, app runs on synthetic generators (demo-safe)

### P1 — Differentiators

- **AI Chat** — Natural language Q&A via Foundation Model API (Gemini 2.5 Flash)
- **Competitor Growth Projection** — Timeline slider simulates 2025-2028 competitor expansion
- **Closure Risk Detection** — Flags stores performing below network average with contributing metrics
- **Embedded Slide Deck** — Business-facing pitch deck at `/slides` for presentation mode

### P2 — Future

- **Lakebase Migration** — Move from Delta tables to Lakebase for lower-latency app queries
- **Multi-Region** — Expand beyond NY demo dataset to national coverage

## Platform Anchors (Why Databricks)


| Capability                  | Databricks Primitive                                            |
| --------------------------- | --------------------------------------------------------------- |
| Spatial processing at scale | Native Spatial SQL, H3 functions + Photon on serverless compute |
| Data storage & governance   | Unity Catalog                                                   |
| ML model lifecycle          | MLflow + UC Model Registry (XGBoost revenue model)              |
| Real-time app queries       | Serverless SQL Warehouse                                        |
| Conversational AI           | Foundation Model API (Gemini 2.5 Flash)                         |
| App hosting + auth          | Databricks Apps (React + FastAPI)                               |
| Multi-env deployment        | Databricks Asset Bundles                                        |


It exercises the full platform end-to-end. Data engineering, spatial processing, ML training, model serving, interactive apps, and AI — all governed under Unity Catalog, deployed with DABs.

**Future:** Lakebase is an excellent fit for the app's query patterns (point lookups, filtered scans). Migration would reduce latency and eliminate the need for a running SQL warehouse.

## Data Model (Unity Catalog — Delta Tables)

### Bronze (Raw Ingestion)


| Table                         | Rows   | Source                                                         |
| ----------------------------- | ------ | -------------------------------------------------------------- |
| `bronze_store_locations`      | 249    | Synthetic (Clover & Co, NY)                                    |
| `bronze_competitor_locations` | ~400   | Synthetic (Target, Walmart, Costco, Whole Foods, Trader Joe's) |
| `bronze_seed_points`          | 1,626  | Synthetic expansion candidates                                 |
| `bronze_census_demographics`  | 15,963 | Census ACS 5-Year API                                          |
| `bronze_census_blockgroups`   | 15,963 | TIGER/Line geometries                                          |
| `bronze_census_zcta`          | 1,818  | TIGER/Line ZCTAs (urbanicity classification via pop density)   |
| `bronze_census_states`        | 1      | NY state boundary                                              |
| `bronze_osm_pois_raw`         | 232K   | OpenStreetMap (Geofabrik)                                      |
| `bronze_store_sales_history`  | 249    | Synthetic monthly sales (12 months)                            |


### Silver (Features)


| Table                          | Rows   | What It Does                                                            |
| ------------------------------ | ------ | ----------------------------------------------------------------------- |
| `silver_h3_features`           | 164K   | H3 res-8 cells — demographics, POI counts, competitor counts, distances |
| `silver_store_isochrones`      | 249    | Store drive-time trade area polygons (urbanicity-based)                 |
| `silver_competitor_isochrones` | ~400   | Competitor drive-time trade area polygons                               |
| `silver_seed_point_isochrones` | 1,626  | Expansion candidate trade area polygons                                 |
| `silver_census_demographics`   | 15,963 | Cleaned block groups with derived rates                                 |
| `silver_osm_pois`              | 230K   | Categorized, deduplicated POIs                                          |


### Gold (ML & Scoring)


| Table                              | Rows  | What It Does                                                |
| ---------------------------------- | ----- | ----------------------------------------------------------- |
| `gold_store_trade_area_features`   | 249   | H3 features aggregated per store isochrone                  |
| `gold_store_features_and_sales`    | 238   | Full feature set + sqft + 12 monthly revenue columns        |
| `gold_seed_trade_area_features`    | 1,626 | H3 features aggregated per seed point isochrone             |
| `gold_expansion_candidates`        | 1,626 | Scored at 3 formats, recommended format + projected revenue |
| `gold_expansion_results`           | var   | Finalized expansion selections from scenario optimizer      |
| `gold_model_artifacts`             | 3     | XGBoost metadata per format (R², feature importance)        |
| `gold_simulated_competitor_growth` | ~76   | 2026-2028 competitor projections                            |


**Key spatial pattern:** Polyfill NY → H3 hexagons → aggregate features per cell → join within drive-time isochrones → score candidates at 3 store formats → recommend max $/sqft efficiency.

## Tech Stack

- **Backend:** FastAPI + Pydantic v2
- **Frontend:** React 19 + TanStack Router + Leaflet + Recharts + Tailwind
- **Data:** Delta tables in Unity Catalog (medallion architecture)
- **ML:** XGBoost + MLflow + UC Model Registry
- **Spatial:** H3, Valhalla routing engine, Census TIGER/Line
- **AI:** Gemini 2.5 Flash via Foundation Model API
- **Auth:** Databricks Apps native auth
- **Hosting:** Databricks Apps
- **Deployment:** Databricks Asset Bundles (multi-target: dev - azure / prod - aws)

## Launch Checklist

[] Create `gold_expansion_results` table — persist scenario optimizer outputs (selected locations, parameters, revenue projections)
[] Wire up save functionality — Site Playground writes finalized scenarios to `gold_expansion_results` instead of local state only

