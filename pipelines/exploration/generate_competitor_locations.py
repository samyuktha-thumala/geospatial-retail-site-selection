# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Competitor Locations
# MAGIC
# MAGIC Generates anonymized competitor locations across New York State metro areas.
# MAGIC Five synthetic competitor brands are distributed proportionally, each with
# MAGIC distinct geographic clustering patterns and coverage strategies.
# MAGIC
# MAGIC **Output:** `{catalog}.{schema}.bronze_competitor_locations`

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# DBTITLE 1,Define competitor brands and MSA data
# ---------------------------------------------------------------------------
# New York State metro areas (matches store-location generation)
# ---------------------------------------------------------------------------
msa_centers = {
    "Manhattan":      {"lat": 40.7831, "lng": -73.9712, "state": "NY", "weight": 1.00},
    "Brooklyn":       {"lat": 40.6782, "lng": -73.9442, "state": "NY", "weight": 0.90},
    "Queens":         {"lat": 40.7282, "lng": -73.7949, "state": "NY", "weight": 0.80},
    "Bronx":          {"lat": 40.8448, "lng": -73.8648, "state": "NY", "weight": 0.70},
    "Staten Island":  {"lat": 40.5795, "lng": -74.1502, "state": "NY", "weight": 0.50},
    "Long Island":    {"lat": 40.7891, "lng": -73.1350, "state": "NY", "weight": 0.80},
    "Westchester":    {"lat": 41.1220, "lng": -73.7949, "state": "NY", "weight": 0.60},
    "Buffalo":        {"lat": 42.8864, "lng": -78.8784, "state": "NY", "weight": 0.70},
    "Rochester":      {"lat": 43.1566, "lng": -77.6088, "state": "NY", "weight": 0.60},
    "Syracuse":       {"lat": 43.0481, "lng": -76.1474, "state": "NY", "weight": 0.55},
    "Albany":         {"lat": 42.6526, "lng": -73.7562, "state": "NY", "weight": 0.55},
    "Yonkers":        {"lat": 40.9312, "lng": -73.8988, "state": "NY", "weight": 0.50},
    "White Plains":   {"lat": 41.0340, "lng": -73.7629, "state": "NY", "weight": 0.40},
    "Poughkeepsie":   {"lat": 41.7004, "lng": -73.9210, "state": "NY", "weight": 0.30},
    "Binghamton":     {"lat": 42.0987, "lng": -75.9180, "state": "NY", "weight": 0.25},
}

msa_names = list(msa_centers.keys())

# ---------------------------------------------------------------------------
# Competitor brand definitions
# ---------------------------------------------------------------------------
brands = {
    "Competitor A": {
        "total": 120,
        "sigma": 0.15,
        "description": "Broad coverage, similar to standard stores",
        "msas": msa_names,
        "store_types": ["Standard", "Superstore", "Express"],
        "type_weights": [0.50, 0.30, 0.20],
    },
    "Competitor B": {
        "total": 100,
        "sigma": 0.08,
        "description": "Urban-focused, tighter clustering",
        "msas": msa_names,
        "store_types": ["Urban", "Metro", "Downtown"],
        "type_weights": [0.45, 0.35, 0.20],
    },
    "Competitor C": {
        "total": 70,
        "sigma": 0.12,
        "description": "Regional clusters, concentrated in top metros",
        "msas": msa_names[:10],
        "store_types": ["Regional", "Flagship", "Outlet"],
        "type_weights": [0.55, 0.25, 0.20],
    },
    "Competitor D": {
        "total": 65,
        "sigma": 0.25,
        "description": "Suburban focus",
        "msas": msa_names,
        "store_types": ["Suburban", "Strip Mall", "Big Box"],
        "type_weights": [0.40, 0.35, 0.25],
    },
    "Competitor E": {
        "total": 45,
        "sigma": 0.10,
        "description": "Niche markets, NYC metro only",
        "msas": msa_names[:8],
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