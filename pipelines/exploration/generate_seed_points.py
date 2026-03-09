# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Expansion Candidate Seed Points
# MAGIC
# MAGIC This notebook identifies promising H3 cells as candidate locations for new retail sites.
# MAGIC
# MAGIC **Process:**
# MAGIC 1. Read H3 feature data (POI counts, demographics, competitor presence, urbanicity)
# MAGIC 2. Read existing store locations
# MAGIC 3. Compute a composite attractiveness score per H3 cell using min-max normalized features
# MAGIC 4. Exclude cells within 2 miles of any existing store (haversine distance)
# MAGIC 5. Select the top 25% of remaining cells by composite score
# MAGIC 6. Assign a store format (flagship / standard / express) based on urbanicity
# MAGIC 7. Write the resulting seed points to the `bronze_seed_points` table

# COMMAND ----------

# DBTITLE 1,Configure Widgets
# ---------------------------------------------------------------------------
# Widget parameters
# ---------------------------------------------------------------------------
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Using catalog: {catalog}")
print(f"Using schema:  {schema}")

# COMMAND ----------

# DBTITLE 1,Read H3 Features
# ---------------------------------------------------------------------------
# Read the silver H3 features table
# ---------------------------------------------------------------------------
h3_features_table = f"{catalog}.{schema}.silver_h3_features"

df_h3 = spark.read.table(h3_features_table)

print(f"Loaded {df_h3.count():,} H3 cells from {h3_features_table}")
df_h3.printSchema()

# COMMAND ----------

# DBTITLE 1,Read Store Locations
# ---------------------------------------------------------------------------
# Read existing store locations
# ---------------------------------------------------------------------------
store_locations_table = f"{catalog}.{schema}.bronze_store_locations"

df_stores = spark.read.table(store_locations_table)

print(f"Loaded {df_stores.count():,} existing store locations from {store_locations_table}")
df_stores.select("store_number", "lat", "lng", "format").show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Compute Composite Score
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Min-max normalisation helper
# norm(x) = (x - min(x)) / (max(x) - min(x))
# ---------------------------------------------------------------------------
def min_max_normalize(df, col_name):
    """Return an expression that min-max normalises *col_name* across the dataframe."""
    stats = df.select(
        F.min(F.col(col_name)).alias("min_val"),
        F.max(F.col(col_name)).alias("max_val")
    ).first()
    min_val = float(stats["min_val"])
    max_val = float(stats["max_val"])
    range_val = max_val - min_val
    if range_val == 0:
        return F.lit(0.0)
    return (F.col(col_name) - F.lit(min_val)) / F.lit(range_val)

# ---------------------------------------------------------------------------
# Compute normalised columns and composite score
# Score = 0.30 * norm(total_poi_count)
#       + 0.25 * norm(total_population)
#       + 0.20 * norm(median_household_income)
#       + 0.15 * (1 - norm(total_competitor_count))
#       + 0.10 * urbanicity_score
# ---------------------------------------------------------------------------
df_scored = (
    df_h3
    .withColumn("norm_poi", min_max_normalize(df_h3, "total_poi_count"))
    .withColumn("norm_pop", min_max_normalize(df_h3, "total_population"))
    .withColumn("norm_income", min_max_normalize(df_h3, "median_household_income"))
    .withColumn("norm_competitor", min_max_normalize(df_h3, "total_competitor_count"))
    .withColumn(
        "composite_score",
        F.round(
            F.lit(0.30) * F.col("norm_poi")
            + F.lit(0.25) * F.col("norm_pop")
            + F.lit(0.20) * F.col("norm_income")
            + F.lit(0.15) * (F.lit(1.0) - F.col("norm_competitor"))
            + F.lit(0.10) * F.col("urbanicity_score"),
            6
        )
    )
)

print("Composite score distribution:")
df_scored.select("composite_score").describe().show()

# COMMAND ----------

# DBTITLE 1,Exclude Cells Near Existing Stores (Haversine, 2-mile Buffer)
import math
from pyspark.sql.types import BooleanType

# ---------------------------------------------------------------------------
# Haversine distance (miles) as a Python function
# ---------------------------------------------------------------------------
EARTH_RADIUS_MILES = 3958.8
EXCLUSION_RADIUS_MILES = 2.0

# Collect store locations to driver for broadcast comparison
store_coords = df_stores.select("lat", "lng").collect()
store_coords_list = [(float(row["lat"]), float(row["lng"])) for row in store_coords]
broadcast_stores = spark.sparkContext.broadcast(store_coords_list)


def _is_near_store(lat, lng):
    """Return True if (lat, lng) is within EXCLUSION_RADIUS_MILES of any store."""
    if lat is None or lng is None:
        return True  # exclude cells with missing coordinates
    lat1 = math.radians(lat)
    lng1 = math.radians(lng)
    for s_lat, s_lng in broadcast_stores.value:
        lat2 = math.radians(s_lat)
        lng2 = math.radians(s_lng)
        dlat = lat2 - lat1
        dlng = lng2 - lng1
        a = (
            math.sin(dlat / 2.0) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2.0) ** 2
        )
        dist = 2 * EARTH_RADIUS_MILES * math.asin(math.sqrt(a))
        if dist <= EXCLUSION_RADIUS_MILES:
            return True
    return False


is_near_store_udf = F.udf(_is_near_store, BooleanType())

# ---------------------------------------------------------------------------
# Filter out H3 cells within 2 miles of any existing store
# ---------------------------------------------------------------------------
df_filtered = df_scored.filter(~is_near_store_udf(F.col("lat"), F.col("lng")))

total_before = df_scored.count()
total_after = df_filtered.count()
print(f"H3 cells before exclusion: {total_before:,}")
print(f"H3 cells after exclusion:  {total_after:,}")
print(f"Excluded:                  {total_before - total_after:,}")

# COMMAND ----------

# DBTITLE 1,Select Top 25%, Assign Format, Write to Table
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# ---------------------------------------------------------------------------
# Keep only the top 25% of cells by composite_score
# ---------------------------------------------------------------------------
threshold = df_filtered.approxQuantile("composite_score", [0.75], 0.01)[0]
print(f"75th-percentile composite score (top-25% threshold): {threshold:.6f}")

df_top = df_filtered.filter(F.col("composite_score") >= threshold)

# ---------------------------------------------------------------------------
# Assign store format based on urbanicity_score
#   > 0.7  -> flagship
#   0.4-0.7 -> standard
#   < 0.4  -> express
# ---------------------------------------------------------------------------
df_formatted = df_top.withColumn(
    "format",
    F.when(F.col("urbanicity_score") > 0.7, F.lit("flagship"))
     .when(F.col("urbanicity_score") >= 0.4, F.lit("standard"))
     .otherwise(F.lit("express"))
)

# ---------------------------------------------------------------------------
# Generate a unique seed_id using monotonically_increasing_id
# ---------------------------------------------------------------------------
df_seed = (
    df_formatted
    .withColumn("seed_id", F.monotonically_increasing_id())
    .select(
        "seed_id",
        "h3_index",
        "lat",
        "lng",
        "composite_score",
        "format",
        "urbanicity_score",
        "total_poi_count",
        "total_population",
        "median_household_income",
        "total_competitor_count",
    )
)

# ---------------------------------------------------------------------------
# Write to bronze_seed_points table (overwrite)
# ---------------------------------------------------------------------------
seed_table = f"{catalog}.{schema}.bronze_seed_points"

df_seed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(seed_table)

print(f"Wrote {df_seed.count():,} seed points to {seed_table}")

# COMMAND ----------

# DBTITLE 1,Summary Statistics
# ---------------------------------------------------------------------------
# Display summary statistics for the generated seed points
# ---------------------------------------------------------------------------
df_result = spark.read.table(seed_table)

print("=" * 60)
print("SEED POINTS SUMMARY")
print("=" * 60)
print(f"Total seed points: {df_result.count():,}")
print()

# Format distribution
print("Format distribution:")
df_result.groupBy("format").agg(
    F.count("*").alias("count"),
    F.round(F.avg("composite_score"), 4).alias("avg_composite_score"),
    F.round(F.avg("urbanicity_score"), 4).alias("avg_urbanicity"),
    F.round(F.avg("total_population"), 0).alias("avg_population"),
    F.round(F.avg("median_household_income"), 0).alias("avg_income"),
).orderBy("format").show(truncate=False)

# Overall feature statistics
print("Feature statistics across all seed points:")
df_result.select(
    "composite_score",
    "urbanicity_score",
    "total_poi_count",
    "total_population",
    "median_household_income",
    "total_competitor_count",
).describe().show(truncate=False)