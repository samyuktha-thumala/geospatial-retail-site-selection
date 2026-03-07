# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Synthetic Store Locations
# MAGIC
# MAGIC This notebook generates approximately 1,500 synthetic retail store locations spread across 20 major US Metropolitan Statistical Areas (MSAs). Each store is assigned a format (express, standard, or flagship), urbanicity classification, and realistic attributes.
# MAGIC
# MAGIC The output is written to the `bronze_store_locations` table in Unity Catalog for downstream site-selection analysis.
# MAGIC
# MAGIC **Target table:** `retail_consumer_goods.site_selection.bronze_store_locations`

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MSA definitions: (name, state, center_lat, center_lng, store_count)
MSA_DATA = [
    ("New York",      "NY", 40.7128,  -74.0060,  180),
    ("Los Angeles",   "CA", 34.0522, -118.2437,  150),
    ("Chicago",       "IL", 41.8781,  -87.6298,  120),
    ("Houston",       "TX", 29.7604,  -95.3698,   90),
    ("Phoenix",       "AZ", 33.4484, -112.0740,   80),
    ("Philadelphia",  "PA", 39.9526,  -75.1652,   75),
    ("Dallas",        "TX", 32.7767,  -96.7970,   90),
    ("Washington DC", "DC", 38.9072,  -77.0369,   75),
    ("Atlanta",       "GA", 33.7490,  -84.3880,   75),
    ("Miami",         "FL", 25.7617,  -80.1918,   70),
    ("Seattle",       "WA", 47.6062, -122.3321,   60),
    ("Boston",        "MA", 42.3601,  -71.0589,   60),
    ("San Diego",     "CA", 32.7157, -117.1611,   60),
    ("San Antonio",   "TX", 29.4241,  -98.4936,   60),
    ("Denver",        "CO", 39.7392, -104.9903,   50),
    ("Austin",        "TX", 30.2672,  -97.7431,   50),
    ("Minneapolis",   "MN", 44.9778,  -93.2650,   40),
    ("Portland",      "OR", 45.5152, -122.6784,   40),
    ("Nashville",     "TN", 36.1627,  -86.7816,   40),
    ("Charlotte",     "NC", 35.2271,  -80.8431,   35),
]

# Format distribution: express=33%, standard=47%, flagship=20%
FORMAT_DISTRIBUTION = {
    "express":   0.33,
    "standard":  0.47,
    "flagship":  0.20,
}

# Spatial spread (sigma in degrees) per format
FORMAT_SIGMA = {
    "flagship": 0.05,
    "standard": 0.12,
    "express":  0.20,
}

total_stores = sum(count for _, _, _, _, count in MSA_DATA)
print(f"Total planned stores: {total_stores}")
print(f"Number of MSAs: {len(MSA_DATA)}")
print(f"Format split: {FORMAT_DISTRIBUTION}")

# COMMAND ----------

import random
import numpy as np

random.seed(42)
np.random.seed(42)

# Street name components for address generation
STREET_NAMES = [
    "Main", "Oak", "Maple", "Cedar", "Elm", "Pine", "Washington",
    "Park", "Lake", "Hill", "Broad", "Market", "Church", "Spring",
    "High", "Union", "Center", "River", "Highland", "Franklin",
    "Jefferson", "Madison", "Lincoln", "Commerce", "Industrial",
    "Peachtree", "Sunset", "Broadway", "Atlantic", "Pacific",
]
STREET_TYPES = ["St", "Ave", "Blvd", "Dr", "Rd", "Ln", "Way", "Pkwy"]


def assign_format(n_stores):
    """Assign store formats according to the target distribution."""
    formats = []
    for fmt, pct in FORMAT_DISTRIBUTION.items():
        count = round(n_stores * pct)
        formats.extend([fmt] * count)
    # Adjust for rounding: fill or trim to exact n_stores
    while len(formats) < n_stores:
        formats.append("standard")
    formats = formats[:n_stores]
    random.shuffle(formats)
    return formats


def classify_urbanicity(distance_deg):
    """Classify urbanicity based on distance from MSA center."""
    if distance_deg < 0.1:
        return "urban"
    elif distance_deg < 0.25:
        return "suburban"
    else:
        return "rural"


def generate_address(rng):
    """Generate a realistic-looking street address."""
    number = rng.integers(100, 9999)
    street = rng.choice(STREET_NAMES)
    stype = rng.choice(STREET_TYPES)
    return f"{number} {street} {stype}"


# ---------- Main generation loop ----------
rng = np.random.default_rng(42)
stores = []
store_number = 1001

for city, state, center_lat, center_lng, n_stores in MSA_DATA:
    formats = assign_format(n_stores)

    for fmt in formats:
        sigma = FORMAT_SIGMA[fmt]

        # Generate lat/lng offsets using normal distribution
        lat_offset = rng.normal(0, sigma)
        lng_offset = rng.normal(0, sigma)

        lat = round(center_lat + lat_offset, 6)
        lng = round(center_lng + lng_offset, 6)

        distance = np.sqrt(lat_offset**2 + lng_offset**2)
        urbanicity = classify_urbanicity(distance)

        stores.append({
            "store_number": store_number,
            "name": f"Store #{store_number}",
            "format": fmt,
            "lat": lat,
            "lng": lng,
            "city": city,
            "state": state,
            "urbanicity": urbanicity,
            "address": generate_address(rng),
        })
        store_number += 1

print(f"Generated {len(stores)} store locations")
print(f"Store numbers: {stores[0]['store_number']} - {stores[-1]['store_number']}")
print(f"\nFormat counts:")
from collections import Counter
fmt_counts = Counter(s["format"] for s in stores)
for fmt, cnt in sorted(fmt_counts.items()):
    print(f"  {fmt}: {cnt} ({cnt/len(stores)*100:.1f}%)")
print(f"\nUrbanicity counts:")
urb_counts = Counter(s["urbanicity"] for s in stores)
for urb, cnt in sorted(urb_counts.items()):
    print(f"  {urb}: {cnt} ({cnt/len(stores)*100:.1f}%)")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

spark = SparkSession.builder.getOrCreate()

schema_def = StructType([
    StructField("store_number", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("format", StringType(), False),
    StructField("lat", DoubleType(), False),
    StructField("lng", DoubleType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("urbanicity", StringType(), False),
    StructField("address", StringType(), False),
])

df = spark.createDataFrame(stores, schema=schema_def)

table_name = f"{catalog}.{schema}.bronze_store_locations"
print(f"Writing {df.count()} rows to {table_name} ...")

df.write.mode("overwrite").saveAsTable(table_name)

# Verify
written_df = spark.table(table_name)
print(f"\nRows written: {written_df.count()}")
print("\nSchema:")
written_df.printSchema()
print("\nSummary statistics for lat/lng:")
written_df.select("lat", "lng").summary("count", "min", "max", "mean", "stddev").show()

# COMMAND ----------

display(spark.sql(f"""
    SELECT format, urbanicity, COUNT(*) as cnt
    FROM {catalog}.{schema}.bronze_store_locations
    GROUP BY format, urbanicity
    ORDER BY format, urbanicity
"""))