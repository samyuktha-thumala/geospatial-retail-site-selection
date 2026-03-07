# Databricks notebook source
# MAGIC %md
# MAGIC # Drive-Time Isochrones via Valhalla API
# MAGIC
# MAGIC Generates drive-time trade area polygons using the public Valhalla routing API.
# MAGIC
# MAGIC **Modes:**
# MAGIC - Default: processes stores + competitors from bronze tables
# MAGIC - With `input_table` override: processes any location table (e.g., seed points)
# MAGIC
# MAGIC Drive times vary by urbanicity (from ZCTA-based classification):
# MAGIC - Urban: 5 min (pedestrian)
# MAGIC - Suburban: 10 min (auto)
# MAGIC - Rural: 15 min (auto)
# MAGIC
# MAGIC **Serverless-safe**: Uses HTTP requests only
# MAGIC
# MAGIC **Outputs:** `silver_store_isochrones`, `silver_competitor_isochrones`, or custom

# COMMAND ----------

# MAGIC %pip install pyyaml h3

# COMMAND ----------

import requests
import json
import time
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("gold_schema", "")
dbutils.widgets.text("config_path", "")
dbutils.widgets.text("skip_setup", "")
dbutils.widgets.text("input_table", "")
dbutils.widgets.text("output_table_override", "")
dbutils.widgets.text("valhalla_url", "https://valhalla1.openstreetmap.de/isochrone")
dbutils.widgets.text("max_workers", "4")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
config_path = dbutils.widgets.get("config_path")
input_table_override = dbutils.widgets.get("input_table").strip()
output_table_override = dbutils.widgets.get("output_table_override").strip()
VALHALLA_URL = dbutils.widgets.get("valhalla_url")
MAX_WORKERS = int(dbutils.widgets.get("max_workers"))

assert catalog and bronze_schema and silver_schema, "Missing required parameters"

# Load config for drive times if available
DRIVE_TIMES = {"urban": 5, "suburban": 10, "rural": 15}
COSTING = {"urban": "pedestrian", "suburban": "auto", "rural": "auto"}

if config_path:
    try:
        with open(config_path, 'r') as f:
            iso_config = yaml.safe_load(f)
        ur = iso_config.get('urbanicity_routing', {})
        if ur.get('drive_times'):
            DRIVE_TIMES = {k: v for k, v in ur['drive_times'].items() if k != 'default'}
        print(f"Drive times from config: {DRIVE_TIMES}")
    except Exception as e:
        print(f"Config load warning: {e}, using defaults")

print(f"Valhalla URL: {VALHALLA_URL}")
print(f"Max workers: {MAX_WORKERS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Valhalla isochrone function

# COMMAND ----------

def fetch_isochrone(lat, lng, drive_time_minutes, costing="auto", retries=3):
    """Fetch a drive-time isochrone polygon from Valhalla API.
    Returns GeoJSON polygon string or None on failure.
    """
    payload = {
        "locations": [{"lat": lat, "lon": lng}],
        "costing": costing,
        "contours": [{"time": drive_time_minutes}],
        "polygons": True,
        "generalize": 50 if costing == "pedestrian" else 100,
    }

    for attempt in range(retries):
        try:
            resp = requests.post(
                VALHALLA_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                features = data.get("features", [])
                if features and features[0].get("geometry"):
                    return json.dumps(features[0]["geometry"])
            elif resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            else:
                if attempt < retries - 1:
                    time.sleep(1)
                    continue
                return None
        except (requests.RequestException, json.JSONDecodeError):
            if attempt < retries - 1:
                time.sleep(1)
                continue
            return None
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parallel batch isochrone computation

# COMMAND ----------

def compute_batch_parallel(locations, label="locations", max_workers=4):
    """Compute isochrones for a list of locations using ThreadPoolExecutor.
    Each row must have: location_id, lat, lng, format, urbanicity
    """
    results = []
    failed = []
    total = len(locations)
    start = time.time()

    def process_location(row):
        loc_id = row["location_id"]
        lat, lng = row["lat"], row["lng"]
        urbanicity = (row["urbanicity"] or "suburban").lower()
        fmt = row["format"] or "standard"
        drive_time = DRIVE_TIMES.get(urbanicity, 10)
        costing = COSTING.get(urbanicity, "auto")
        geojson = fetch_isochrone(lat, lng, drive_time, costing=costing)
        return (loc_id, fmt, urbanicity, drive_time, geojson, lat, lng)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_location, row): row for row in locations}
        for i, future in enumerate(as_completed(futures)):
            loc_id, fmt, urbanicity, drive_time, geojson, lat, lng = future.result()
            if geojson:
                results.append({
                    "location_id": str(loc_id),
                    "format": fmt,
                    "urbanicity_category": urbanicity,
                    "drive_time_minutes": drive_time,
                    "geometry_geojson": geojson,
                    "lat": lat,
                    "lng": lng,
                })
            else:
                failed.append(loc_id)

            if (i + 1) % 50 == 0:
                elapsed = time.time() - start
                rate = (i + 1) / elapsed
                eta = (total - i - 1) / rate / 60
                print(f"  [{i+1}/{total}] {len(results)} ok, {len(failed)} failed | {rate:.1f}/sec | ETA: {eta:.1f} min")

    elapsed = time.time() - start
    print(f"\n{label}: {len(results)} isochrones in {elapsed/60:.1f} min ({len(failed)} failed)")

    if failed:
        print(f"  Failed IDs (first 10): {failed[:10]}")

    # Fail if >10% failed
    if total > 0 and len(failed) > total * 0.1:
        raise RuntimeError(f"Too many failures for {label}: {len(failed)}/{total} ({len(failed)/total*100:.0f}%)")

    return results

# COMMAND ----------

def write_isochrones(results, output_table):
    """Write isochrone results to a Delta table with correct area calculation."""
    if not results:
        print(f"No results to write for {output_table}!")
        return

    schema_def = StructType([
        StructField("location_id", StringType()),
        StructField("format", StringType()),
        StructField("urbanicity_category", StringType()),
        StructField("drive_time_minutes", IntegerType()),
        StructField("geometry_geojson", StringType()),
        StructField("lat", DoubleType()),
        StructField("lng", DoubleType()),
    ])

    df = spark.createDataFrame(results, schema=schema_def)

    # Convert GeoJSON to geometry and compute area
    # ST_Area on GEOGRAPHY type returns square meters → divide by 1e6 for sq km
    df = df.withColumn(
        "geometry", F.expr("ST_GeomFromGeoJSON(geometry_geojson)")
    ).withColumn(
        "geometry_wkt", F.expr("ST_AsText(geometry)")
    ).withColumn(
        "area_sqkm", F.expr("ST_Area(geometry)") / 1e6
    ).drop("geometry_geojson")

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

    count = spark.sql(f"SELECT COUNT(*) as c FROM {output_table}").collect()[0]["c"]
    print(f"Written {count} isochrones to {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process locations

# COMMAND ----------

if input_table_override:
    # Custom input table mode (e.g., seed points)
    locations = spark.sql(f"""
        SELECT
            COALESCE(seed_point_id, location_id, CAST(store_number AS STRING)) as location_id,
            COALESCE(latitude, lat) as lat,
            COALESCE(longitude, lng) as lng,
            COALESCE(format, 'standard') as format,
            COALESCE(urbanicity_category, urbanicity, 'suburban') as urbanicity
        FROM {input_table_override}
    """).collect()

    out_name = output_table_override if output_table_override else "silver_custom_isochrones"
    out_table = f"{catalog}.{silver_schema}.{out_name}" if "." not in out_name else out_name

    print(f"Custom input: {input_table_override} ({len(locations)} locations)")
    results = compute_batch_parallel(locations, label=out_name, max_workers=MAX_WORKERS)
    write_isochrones(results, out_table)

else:
    # Default mode: process stores + competitors
    # Urbanicity is derived inline by joining to ZCTA (no separate update step needed)
    zcta_table = f"{catalog}.{silver_schema}.silver_census_zcta"
    stores = spark.sql(f"""
        SELECT s.store_number as location_id, s.lat, s.lng, s.format,
            CASE
                WHEN z.population_density_sqkm > 5000 THEN 'urban'
                WHEN z.population_density_sqkm > 500 THEN 'suburban'
                ELSE 'rural'
            END as urbanicity
        FROM {catalog}.{bronze_schema}.bronze_store_locations s
        LEFT JOIN {zcta_table} z
            ON ST_Contains(z.geometry, ST_SetSRID(ST_Point(s.lng, s.lat), 4326))
    """).collect()
    print(f"Stores to process: {len(stores)}")

    competitors = spark.sql(f"""
        SELECT c.competitor_id as location_id, c.lat, c.lng, c.brand as format,
            CASE
                WHEN z.population_density_sqkm > 5000 THEN 'urban'
                WHEN z.population_density_sqkm > 500 THEN 'suburban'
                ELSE 'rural'
            END as urbanicity
        FROM {catalog}.{bronze_schema}.bronze_competitor_locations c
        LEFT JOIN {zcta_table} z
            ON ST_Contains(z.geometry, ST_SetSRID(ST_Point(c.lng, c.lat), 4326))
    """).collect()
    print(f"Competitors to process: {len(competitors)}")

    store_results = compute_batch_parallel(stores, label="Stores", max_workers=MAX_WORKERS)
    comp_results = compute_batch_parallel(competitors, label="Competitors", max_workers=MAX_WORKERS)

    store_table = f"{catalog}.{silver_schema}.silver_store_isochrones"
    write_isochrones(store_results, store_table)

    comp_table = f"{catalog}.{silver_schema}.silver_competitor_isochrones"
    write_isochrones(comp_results, comp_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

if input_table_override:
    out_name = output_table_override if output_table_override else "silver_custom_isochrones"
    out_table = f"{catalog}.{silver_schema}.{out_name}" if "." not in out_name else out_name
    display(spark.sql(f"""
        SELECT urbanicity_category, COUNT(*) as cnt,
               ROUND(AVG(area_sqkm), 1) as avg_area_sqkm,
               AVG(drive_time_minutes) as drive_time
        FROM {out_table}
        GROUP BY urbanicity_category
        ORDER BY urbanicity_category
    """))
else:
    for tbl_name, tbl in [("Store Isochrones", store_table), ("Competitor Isochrones", comp_table)]:
        print(f"\n=== {tbl_name} ===")
        display(spark.sql(f"""
            SELECT urbanicity_category, COUNT(*) as cnt,
                   ROUND(AVG(area_sqkm), 1) as avg_area_sqkm,
                   ROUND(MIN(area_sqkm), 1) as min_area,
                   ROUND(MAX(area_sqkm), 1) as max_area,
                   AVG(drive_time_minutes) as drive_time
            FROM {tbl}
            GROUP BY urbanicity_category
            ORDER BY urbanicity_category
        """))
