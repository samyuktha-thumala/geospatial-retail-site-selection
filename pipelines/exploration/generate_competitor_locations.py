# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Competitor Locations
# MAGIC
# MAGIC This notebook generates approximately **5,000 anonymized competitor locations** across 20 major US MSAs.
# MAGIC
# MAGIC Five synthetic competitor brands are distributed proportionally across metropolitan areas, each with
# MAGIC distinct geographic clustering patterns and coverage strategies. The output is written to the
# MAGIC `bronze_competitor_locations` table for downstream analysis.

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# DBTITLE 1,Define competitor brands and MSA data
# ---------------------------------------------------------------------------
# MSA centers (same 20 MSAs used for store-location generation)
# ---------------------------------------------------------------------------
msa_centers = {
    "New York":       {"lat": 40.7128, "lng": -74.0060, "state": "NY", "weight": 1.00},
    "Los Angeles":    {"lat": 34.0522, "lng": -118.2437, "state": "CA", "weight": 0.85},
    "Chicago":        {"lat": 41.8781, "lng": -87.6298, "state": "IL", "weight": 0.70},
    "Dallas":         {"lat": 32.7767, "lng": -96.7970, "state": "TX", "weight": 0.60},
    "Houston":        {"lat": 29.7604, "lng": -95.3698, "state": "TX", "weight": 0.58},
    "Washington":     {"lat": 38.9072, "lng": -77.0369, "state": "DC", "weight": 0.55},
    "Philadelphia":   {"lat": 39.9526, "lng": -75.1652, "state": "PA", "weight": 0.50},
    "Miami":          {"lat": 25.7617, "lng": -80.1918, "state": "FL", "weight": 0.48},
    "Atlanta":        {"lat": 33.7490, "lng": -84.3880, "state": "GA", "weight": 0.46},
    "Boston":         {"lat": 42.3601, "lng": -71.0589, "state": "MA", "weight": 0.44},
    "Phoenix":        {"lat": 33.4484, "lng": -112.0740, "state": "AZ", "weight": 0.42},
    "San Francisco":  {"lat": 37.7749, "lng": -122.4194, "state": "CA", "weight": 0.40},
    "Riverside":      {"lat": 33.9533, "lng": -117.3962, "state": "CA", "weight": 0.38},
    "Detroit":        {"lat": 42.3314, "lng": -83.0458, "state": "MI", "weight": 0.36},
    "Seattle":        {"lat": 47.6062, "lng": -122.3321, "state": "WA", "weight": 0.34},
    "Minneapolis":    {"lat": 44.9778, "lng": -93.2650, "state": "MN", "weight": 0.32},
    "San Diego":      {"lat": 32.7157, "lng": -117.1611, "state": "CA", "weight": 0.30},
    "Tampa":          {"lat": 27.9506, "lng": -82.4572, "state": "FL", "weight": 0.28},
    "Denver":         {"lat": 39.7392, "lng": -104.9903, "state": "CO", "weight": 0.26},
    "St. Louis":      {"lat": 38.6270, "lng": -90.1994, "state": "MO", "weight": 0.24},
}

msa_names = list(msa_centers.keys())

# ---------------------------------------------------------------------------
# Competitor brand definitions
# ---------------------------------------------------------------------------
brands = {
    "Competitor A": {
        "total": 1500,
        "sigma": 0.15,
        "description": "Broad coverage, similar to standard stores",
        "msas": msa_names,                  # all 20 MSAs
        "store_types": ["Standard", "Superstore", "Express"],
        "type_weights": [0.50, 0.30, 0.20],
    },
    "Competitor B": {
        "total": 1200,
        "sigma": 0.08,
        "description": "Urban-focused, tighter clustering",
        "msas": msa_names,                  # all 20 MSAs
        "store_types": ["Urban", "Metro", "Downtown"],
        "type_weights": [0.45, 0.35, 0.20],
    },
    "Competitor C": {
        "total": 900,
        "sigma": 0.12,
        "description": "Regional clusters, concentrated in 10 MSAs",
        "msas": msa_names[:10],             # first 10 MSAs only
        "store_types": ["Regional", "Flagship", "Outlet"],
        "type_weights": [0.55, 0.25, 0.20],
    },
    "Competitor D": {
        "total": 800,
        "sigma": 0.25,
        "description": "Suburban focus",
        "msas": msa_names,                  # all 20 MSAs
        "store_types": ["Suburban", "Strip Mall", "Big Box"],
        "type_weights": [0.40, 0.35, 0.25],
    },
    "Competitor E": {
        "total": 600,
        "sigma": 0.10,
        "description": "Niche markets, 8 MSAs only",
        "msas": msa_names[:8],              # first 8 MSAs only
        "store_types": ["Boutique", "Specialty", "Pop-up"],
        "type_weights": [0.50, 0.30, 0.20],
    },
}

print(f"Total target locations: {sum(b['total'] for b in brands.values()):,}")
for name, cfg in brands.items():
    print(f"  {name}: {cfg['total']:>5} locations across {len(cfg['msas']):>2} MSAs  (sigma={cfg['sigma']})")

# COMMAND ----------

# DBTITLE 1,Generate competitor locations
import random
import numpy as np

random.seed(123)
np.random.seed(123)

rows = []
competitor_id = 1

for brand_name, cfg in brands.items():
    total = cfg["total"]
    sigma = cfg["sigma"]
    brand_msas = cfg["msas"]
    store_types = cfg["store_types"]
    type_weights = cfg["type_weights"]

    # Compute per-MSA allocation proportional to MSA weight
    brand_weights = [msa_centers[m]["weight"] for m in brand_msas]
    total_weight = sum(brand_weights)
    allocations = [max(1, int(round(total * w / total_weight))) for w in brand_weights]

    # Adjust to match exact total
    diff = total - sum(allocations)
    for i in range(abs(diff)):
        idx = i % len(allocations)
        allocations[idx] += 1 if diff > 0 else -1

    for msa_name, count in zip(brand_msas, allocations):
        center = msa_centers[msa_name]
        lats = np.random.normal(center["lat"], sigma, count)
        lngs = np.random.normal(center["lng"], sigma, count)

        for j in range(count):
            # open_year: 70% historic (2015-2023), 30% projected (2024-2030)
            if random.random() < 0.70:
                open_year = random.randint(2015, 2023)
            else:
                open_year = random.randint(2024, 2030)

            # store_type weighted by brand pattern
            store_type = random.choices(store_types, weights=type_weights, k=1)[0]

            rows.append((
                competitor_id,
                brand_name,
                round(float(lats[j]), 6),
                round(float(lngs[j]), 6),
                msa_name,
                center["state"],
                store_type,
                open_year,
            ))
            competitor_id += 1

print(f"Generated {len(rows):,} competitor locations across {len(brands)} brands")

# COMMAND ----------

# DBTITLE 1,Write to Delta table
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema_def = StructType([
    StructField("competitor_id", IntegerType(), False),
    StructField("brand", StringType(), False),
    StructField("lat", DoubleType(), False),
    StructField("lng", DoubleType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("store_type", StringType(), False),
    StructField("open_year", IntegerType(), False),
])

df = spark.createDataFrame(rows, schema=schema_def)

table_name = f"{catalog}.{schema}.bronze_competitor_locations"

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(table_name)
)

print(f"Wrote {df.count():,} rows to {table_name}")

# COMMAND ----------

# DBTITLE 1,Summary
display(spark.sql(f"""
    SELECT brand, COUNT(*) as cnt,
           MIN(open_year) as min_year, MAX(open_year) as max_year,
           SUM(CASE WHEN open_year > 2025 THEN 1 ELSE 0 END) as projected
    FROM {catalog}.{schema}.bronze_competitor_locations
    GROUP BY brand ORDER BY cnt DESC
"""))