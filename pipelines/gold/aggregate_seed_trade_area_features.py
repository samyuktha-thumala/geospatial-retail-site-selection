# Databricks notebook source
# MAGIC %md
# MAGIC # Trade Area Feature Aggregation — Seed Points
# MAGIC
# MAGIC Same aggregation as stores but for expansion seed points.
# MAGIC Uses **ST_Contains** with native geometry for spatial join (no polyfill).
# MAGIC
# MAGIC **Output:** `gold_seed_trade_area_features`

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Spatial join — H3 cell centers inside seed isochrones

# COMMAND ----------

isochrones = spark.table(f"{catalog}.{schema}.seed_point_isochrones")
print(f"Seed isochrones: {isochrones.count()}")
isochrones.groupBy("urbanicity_category").count().show()

# COMMAND ----------

# Spatial join: find H3 cells whose center falls inside each isochrone polygon
# Uses native GEOMETRY(4326) + spatial index — no polyfill explosion
h3_features = spark.table(f"{catalog}.{schema}.silver_h3_features").drop("processing_timestamp")

# Drop columns from H3 that conflict with isochrone columns or aren't needed for aggregation
h3_drop_cols = ["h3_geometry", "processing_timestamp", "urbanicity_category"]
h3_cleaned = spark.table(f"{catalog}.{schema}.silver_h3_features")
for c in h3_drop_cols:
    if c in h3_cleaned.columns:
        h3_cleaned = h3_cleaned.drop(c)
h3_cleaned.createOrReplaceTempView("_h3_clean")

ta_with_features = spark.sql(f"""
    SELECT
        iso.location_id,
        iso.format,
        iso.urbanicity_category,
        iso.drive_time_minutes,
        iso.area_sqkm,
        iso.lat,
        iso.lng,
        h3.*
    FROM {catalog}.{schema}.seed_point_isochrones iso
    INNER JOIN _h3_clean h3
        ON ST_Contains(iso.geometry, ST_SetSRID(ST_Point(h3.center_lon, h3.center_lat), 4326))
""")

print(f"Joined rows: {ta_with_features.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Aggregate by seed point

# COMMAND ----------

# Discover column groups dynamically
all_cols = ta_with_features.columns

count_vars = [c for c in [
    "total_population", "bachelors_degree", "masters_degree", "doctorate_degree",
    "in_labor_force", "unemployed", "total_housing_units", "total_households",
    "owner_occupied", "renter_occupied", "total_commuters", "public_transit_commuters",
    "employed", "not_in_labor_force", "vacant_housing_units",
] if c in all_cols]

median_vars = [c for c in [
    "median_household_income", "median_home_value", "median_age", "median_gross_rent",
] if c in all_cols]

poi_cols = [c for c in all_cols if c.startswith("poi_count_")]
competitor_cols = [c for c in all_cols if c.startswith("competitor_count_")]
distance_cols = [c for c in all_cols if c.startswith("distance_to_")]

# COMMAND ----------

agg_exprs = []

# Count variables: sum across H3 cells in trade area
for v in count_vars:
    agg_exprs.append(F.sum(F.coalesce(F.col(v), F.lit(0))).cast("long").alias(v))

# POI counts: sum
for c in poi_cols:
    agg_exprs.append(F.sum(F.coalesce(F.col(c), F.lit(0))).cast("long").alias(c))
agg_exprs.append(F.sum(F.coalesce(F.col("total_poi_count"), F.lit(0))).cast("long").alias("total_poi_count"))

# Competitor counts: sum
for c in competitor_cols:
    agg_exprs.append(F.sum(F.coalesce(F.col(c), F.lit(0))).cast("long").alias(c))
agg_exprs.append(F.sum(F.coalesce(F.col("total_competitor_count"), F.lit(0))).cast("long").alias("total_competitor_count"))

# Median/rate variables: average across cells
for v in median_vars:
    agg_exprs.append(F.round(F.avg(F.col(v)), 2).alias(v))

# Distance features: min (closest)
for c in distance_cols:
    agg_exprs.append(F.round(F.min(F.col(c)), 2).alias(c))

# Urbanicity and density
agg_exprs.extend([
    F.round(F.avg("urbanicity_score"), 4).alias("urbanicity_score"),
    F.round(F.avg("population_density"), 2).alias("avg_population_density"),
    F.count("h3_cell_id").alias("h3_cell_count"),
])

ta_agg = ta_with_features.groupBy(
    "location_id", "format", "urbanicity_category",
    "drive_time_minutes", "area_sqkm", "lat", "lng",
).agg(*agg_exprs)

print(f"Aggregated seed trade areas: {ta_agg.count()}")

# COMMAND ----------

# Derived features
ta_final = ta_agg.withColumn(
    "higher_education_rate",
    F.when(F.col("total_population") > 0,
        (F.col("bachelors_degree") + F.col("masters_degree") + F.col("doctorate_degree")) / F.col("total_population")
    ).otherwise(0)
).withColumn(
    "unemployment_rate",
    F.when(F.col("in_labor_force") > 0, F.col("unemployed") / F.col("in_labor_force")).otherwise(0)
).withColumn(
    "transit_share",
    F.when(F.col("total_commuters") > 0, F.col("public_transit_commuters") / F.col("total_commuters")).otherwise(0)
).withColumn(
    "owner_occupied_rate",
    F.when(F.col("total_housing_units") > 0, F.col("owner_occupied") / F.col("total_housing_units")).otherwise(0)
).withColumn(
    "processing_timestamp", F.current_timestamp()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write to gold

# COMMAND ----------

output_table = f"{catalog}.{schema}.gold_seed_trade_area_features"
ta_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

result = spark.table(output_table)
print(f"Written {result.count()} seed trade areas to {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        urbanicity_category,
        COUNT(*) as seeds,
        ROUND(AVG(total_population), 0) as avg_pop,
        ROUND(AVG(median_household_income), 0) as avg_income,
        ROUND(AVG(total_poi_count), 0) as avg_pois,
        ROUND(AVG(total_competitor_count), 1) as avg_comps,
        ROUND(AVG(h3_cell_count), 0) as avg_cells,
        ROUND(AVG(area_sqkm), 1) as avg_area
    FROM {output_table}
    GROUP BY urbanicity_category
    ORDER BY urbanicity_category
"""))
