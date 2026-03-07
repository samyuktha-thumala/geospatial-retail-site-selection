# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Expansion Candidate Seed Points
# MAGIC
# MAGIC Identifies promising H3 cells as candidate locations for new retail sites.
# MAGIC 1. Read H3 features (POIs, demographics, competitors, urbanicity)
# MAGIC 2. Compute composite attractiveness score
# MAGIC 3. Exclude cells within configurable radius of existing stores
# MAGIC 4. Select top 50% **per urbanicity category** (ensures mix of urban/suburban/rural)
# MAGIC 5. Assign format (flagship/standard/express) by urbanicity category
# MAGIC
# MAGIC **Serverless-safe**: No sparkContext, no GEOGRAPHY collect
# MAGIC
# MAGIC **Output:** `silver_seed_points`

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("min_poi_count", "2")
dbutils.widgets.text("exclusion_urban_miles", "0.5")
dbutils.widgets.text("exclusion_suburban_miles", "5.0")
dbutils.widgets.text("exclusion_rural_miles", "10.0")
dbutils.widgets.text("top_pct", "0.50")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
MIN_POI_COUNT = int(dbutils.widgets.get("min_poi_count"))
EXCLUSION_RADIUS = {
    "urban": float(dbutils.widgets.get("exclusion_urban_miles")),
    "suburban": float(dbutils.widgets.get("exclusion_suburban_miles")),
    "rural": float(dbutils.widgets.get("exclusion_rural_miles")),
}
TOP_PCT = float(dbutils.widgets.get("top_pct"))

assert catalog and schema, "catalog and schema must be provided"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load H3 features (no geometry columns)

# COMMAND ----------

h3_df = spark.sql(f"""
    SELECT h3_cell_id, center_lat, center_lon, h3_area_sqkm,
           total_poi_count, total_population, median_household_income,
           total_competitor_count, urbanicity_score, urbanicity_category,
           distance_to_nearest_store_miles, population_density
    FROM {catalog}.{schema}.silver_h3_features
    WHERE total_population > 0 AND total_poi_count > {MIN_POI_COUNT}
""")

total_cells = h3_df.count()
print(f"H3 cells with population > 0: {total_cells:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute composite score

# COMMAND ----------

stats = h3_df.agg(
    F.min("total_poi_count").alias("poi_min"), F.max("total_poi_count").alias("poi_max"),
    F.min("total_population").alias("pop_min"), F.max("total_population").alias("pop_max"),
    F.min("median_household_income").alias("inc_min"), F.max("median_household_income").alias("inc_max"),
    F.min("total_competitor_count").alias("comp_min"), F.max("total_competitor_count").alias("comp_max"),
).collect()[0]

def norm_expr(col, min_val, max_val):
    rng = max_val - min_val
    if rng == 0:
        return F.lit(0.0)
    return (F.col(col) - F.lit(float(min_val))) / F.lit(float(rng))

df_scored = h3_df.withColumn(
    "composite_score",
    F.round(
        F.lit(0.30) * norm_expr("total_poi_count", stats["poi_min"], stats["poi_max"])
        + F.lit(0.25) * norm_expr("total_population", stats["pop_min"], stats["pop_max"])
        + F.lit(0.20) * norm_expr("median_household_income", stats["inc_min"], stats["inc_max"])
        + F.lit(0.15) * (F.lit(1.0) - norm_expr("total_competitor_count", stats["comp_min"], stats["comp_max"]))
        + F.lit(0.10) * F.col("urbanicity_score"),
        6
    )
)

display(df_scored.select("composite_score").describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exclude cells near existing stores

# COMMAND ----------

df_filtered = df_scored.filter(
    F.when(F.col("urbanicity_category") == "urban",
           F.col("distance_to_nearest_store_miles") >= EXCLUSION_RADIUS["urban"])
     .when(F.col("urbanicity_category") == "suburban",
           F.col("distance_to_nearest_store_miles") >= EXCLUSION_RADIUS["suburban"])
     .otherwise(F.col("distance_to_nearest_store_miles") >= EXCLUSION_RADIUS["rural"])
)

before = df_scored.count()
after = df_filtered.count()
print(f"Before exclusion: {before:,}")
print(f"After exclusion:  {after:,}")
print(f"Excluded:         {before - after:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select top candidates per urbanicity category and assign format

# COMMAND ----------

w = Window.partitionBy("urbanicity_category").orderBy(F.desc("composite_score"))
df_ranked = df_filtered.withColumn("rank", F.row_number().over(w))
df_ranked = df_ranked.withColumn(
    "cat_count", F.count("*").over(Window.partitionBy("urbanicity_category"))
)

df_top = df_ranked.filter(F.col("rank") <= F.col("cat_count") * TOP_PCT).drop("rank", "cat_count")

top_count = df_top.count()
print(f"Top {TOP_PCT*100:.0f}% per urbanicity: {top_count} seed points")
display(df_top.groupBy("urbanicity_category").count().orderBy("urbanicity_category"))

df_seed = df_top.withColumn(
    "format",
    F.when(F.col("urbanicity_category") == "urban", "flagship")
     .when(F.col("urbanicity_category") == "suburban", "standard")
     .otherwise("express")
).withColumn(
    "seed_point_id", F.concat(F.lit("SP_"), F.monotonically_increasing_id())
).select(
    "seed_point_id",
    "h3_cell_id",
    F.col("center_lat").alias("latitude"),
    F.col("center_lon").alias("longitude"),
    "composite_score",
    "format",
    "urbanicity_score",
    "urbanicity_category",
    "total_poi_count",
    "total_population",
    "median_household_income",
    "total_competitor_count",
    "distance_to_nearest_store_miles",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to table

# COMMAND ----------

seed_table = f"{catalog}.{schema}.silver_seed_points"
df_seed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(seed_table)

count = spark.sql(f"SELECT COUNT(*) as c FROM {seed_table}").collect()[0]["c"]
print(f"Written {count:,} seed points to {seed_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

display(spark.sql(f"""
    SELECT format, urbanicity_category, COUNT(*) as cnt,
           ROUND(AVG(composite_score), 4) as avg_score,
           ROUND(AVG(total_population), 0) as avg_pop,
           ROUND(AVG(median_household_income), 0) as avg_income,
           ROUND(AVG(total_poi_count), 0) as avg_pois
    FROM {seed_table}
    GROUP BY format, urbanicity_category
    ORDER BY format, urbanicity_category
"""))
