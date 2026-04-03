# Databricks notebook source
# MAGIC %md
# MAGIC # Trade Area Feature Aggregation
# MAGIC
# MAGIC For each store isochrone, aggregates H3 features within the trade area polygon
# MAGIC using a **spatial join** (`ST_Contains`) instead of H3 polyfill.
# MAGIC
# MAGIC **Approach:** Find H3 cell centers (`ST_Point(center_lon, center_lat)`) that fall
# MAGIC inside each isochrone's native geometry. This is far more efficient than exploding
# MAGIC isochrones into thousands of H3 cells via `h3_polyfillash3string`.
# MAGIC
# MAGIC **Aggregation rules:**
# MAGIC - **Count vars** (population, households, etc.): summed
# MAGIC - **Median vars** (income, age, home value): population-weighted average
# MAGIC - **POI counts**: summed by category
# MAGIC - **Competitor counts**: summed by brand
# MAGIC - **Distance features**: min (closest)
# MAGIC - **Urbanicity**: averaged score, majority-vote category
# MAGIC
# MAGIC **Output:** `gold_store_trade_area_features`

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Using catalog={catalog}, schema={schema}")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load tables

# COMMAND ----------

isochrones = spark.table(f"{catalog}.{schema}.silver_store_isochrones")
h3_features = spark.table(f"{catalog}.{schema}.silver_h3_features")

iso_count = isochrones.count()
h3_count = h3_features.count()
print(f"Store isochrones: {iso_count:,}")
print(f"H3 feature cells: {h3_count:,}")
isochrones.groupBy("urbanicity_category").count().orderBy("urbanicity_category").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Spatial join — ST_Contains(isochrone, H3 center point)
# MAGIC
# MAGIC Instead of polyfilling isochrones into H3 cells (expensive), we check which H3
# MAGIC cell center points fall inside each isochrone polygon. This leverages Databricks
# MAGIC native spatial indexing for efficient point-in-polygon evaluation.

# COMMAND ----------

# Prepare isochrones: select only needed columns, keep native geometry
iso = isochrones.select(
    "location_id", "format", "urbanicity_category",
    "drive_time_minutes", "area_sqkm", "lat", "lng",
    "geometry",  # native GEOMETRY(4326)
)

# Prepare H3 features: construct center point, drop columns we don't aggregate
h3 = (
    h3_features
    .drop("h3_geometry", "processing_timestamp", "urbanicity_category")
    .withColumn("h3_center", F.expr("ST_SetSRID(ST_Point(center_lon, center_lat), 4326)"))
)

# Spatial join: find H3 cells whose center falls inside the isochrone polygon
ta_with_features = iso.join(
    h3,
    F.expr("ST_Contains(geometry, h3_center)"),
    "inner",
)

# Drop geometry columns — no longer needed after the join
ta_with_features = ta_with_features.drop("geometry", "h3_center")

joined_count = ta_with_features.count()
print(f"Joined rows (H3 cells inside trade areas): {joined_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Dynamic column discovery and aggregation

# COMMAND ----------

# Dynamically discover column groups from the joined DataFrame
all_cols = set(ta_with_features.columns)

# Count variables: sum
count_vars = [
    c for c in [
        "total_population", "bachelors_degree", "masters_degree", "doctorate_degree",
        "in_labor_force", "unemployed", "total_housing_units", "total_households",
        "owner_occupied", "renter_occupied", "total_commuters", "public_transit_commuters",
        "employed", "not_in_labor_force", "vacant_housing_units",
    ]
    if c in all_cols
]

# Median/rate variables: weighted average (weight by population where sensible)
median_vars = [
    c for c in [
        "median_household_income", "median_home_value", "median_age", "median_gross_rent",
    ]
    if c in all_cols
]

# POI columns: sum
poi_cols = sorted([c for c in all_cols if c.startswith("poi_count_")])

# Competitor columns: sum
competitor_cols = sorted([c for c in all_cols if c.startswith("competitor_count_")])

# Distance columns: min (closest)
distance_cols = sorted([c for c in all_cols if c.startswith("distance_to_")])

print(f"Count vars ({len(count_vars)}): {count_vars}")
print(f"Median vars ({len(median_vars)}): {median_vars}")
print(f"POI cols ({len(poi_cols)}): {poi_cols}")
print(f"Competitor cols ({len(competitor_cols)}): {competitor_cols}")
print(f"Distance cols ({len(distance_cols)}): {distance_cols}")

# COMMAND ----------

# Build aggregation expressions — single list, no mutation loops
agg_exprs = (
    [F.sum(F.coalesce(F.col(v), F.lit(0))).cast("long").alias(v) for v in count_vars]
    + [F.sum(F.coalesce(F.col(c), F.lit(0))).cast("long").alias(c) for c in poi_cols]
    + ([F.sum(F.coalesce(F.col("total_poi_count"), F.lit(0))).cast("long").alias("total_poi_count")] if "total_poi_count" in all_cols else [])
    + [F.sum(F.coalesce(F.col(c), F.lit(0))).cast("long").alias(c) for c in competitor_cols]
    + ([F.sum(F.coalesce(F.col("total_competitor_count"), F.lit(0))).cast("long").alias("total_competitor_count")] if "total_competitor_count" in all_cols else [])
    + [F.round(
        F.when(
            F.sum(F.when(F.col(v).isNotNull() & F.col("total_population").isNotNull(), F.col("total_population"))) > 0,
            F.sum(F.when(F.col(v).isNotNull(), F.col(v) * F.coalesce(F.col("total_population"), F.lit(0)))) /
            F.sum(F.when(F.col(v).isNotNull(), F.coalesce(F.col("total_population"), F.lit(0))))
        ).otherwise(F.avg(F.col(v))), 2).alias(v) for v in median_vars]
    + [F.round(F.min(F.col(c)), 2).alias(c) for c in distance_cols]
    + ([F.round(F.avg("urbanicity_score"), 4).alias("urbanicity_score")] if "urbanicity_score" in all_cols else [])
    + ([F.round(F.avg("population_density"), 2).alias("avg_population_density")] if "population_density" in all_cols else [])
    + [F.count("h3_cell_id").alias("h3_cell_count")]
)

# COMMAND ----------

# Group by store identity columns and aggregate
group_cols = [
    "location_id", "format", "urbanicity_category",
    "drive_time_minutes", "area_sqkm", "lat", "lng",
]

ta_agg = ta_with_features.groupBy(*group_cols).agg(*agg_exprs)

print(f"Aggregated trade areas: {ta_agg.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Derived features

# COMMAND ----------

ta_final = (
    ta_agg
    .withColumn(
        "higher_education_rate",
        F.when(
            F.col("total_population") > 0,
            F.round(
                (F.col("bachelors_degree") + F.col("masters_degree") + F.col("doctorate_degree"))
                / F.col("total_population"),
                4,
            ),
        ).otherwise(0.0),
    )
    .withColumn(
        "unemployment_rate",
        F.when(
            F.col("in_labor_force") > 0,
            F.round(F.col("unemployed") / F.col("in_labor_force"), 4),
        ).otherwise(0.0),
    )
    .withColumn(
        "transit_share",
        F.when(
            F.col("total_commuters") > 0,
            F.round(F.col("public_transit_commuters") / F.col("total_commuters"), 4),
        ).otherwise(0.0),
    )
    .withColumn(
        "owner_occupied_rate",
        F.when(
            F.col("total_housing_units") > 0,
            F.round(F.col("owner_occupied") / F.col("total_housing_units"), 4),
        ).otherwise(0.0),
    )
    .withColumn("processing_timestamp", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write to gold table

# COMMAND ----------

output_table = f"{catalog}.{schema}.gold_store_trade_area_features"

(
    ta_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(output_table)
)

result = spark.table(output_table)
print(f"Written {result.count()} store trade areas to {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        COUNT(*)                                    AS total_stores,
        ROUND(AVG(total_population), 0)             AS avg_population,
        ROUND(AVG(total_poi_count), 0)              AS avg_poi_count,
        ROUND(AVG(total_competitor_count), 1)        AS avg_competitor_count,
        ROUND(AVG(median_household_income), 0)       AS avg_income,
        ROUND(AVG(h3_cell_count), 0)                AS avg_h3_cells,
        ROUND(AVG(area_sqkm), 1)                    AS avg_area_sqkm,
        ROUND(AVG(higher_education_rate), 4)         AS avg_higher_ed_rate,
        ROUND(AVG(unemployment_rate), 4)             AS avg_unemployment_rate,
        ROUND(AVG(transit_share), 4)                 AS avg_transit_share,
        ROUND(AVG(owner_occupied_rate), 4)           AS avg_owner_occupied_rate
    FROM {output_table}
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        urbanicity_category,
        COUNT(*)                                    AS stores,
        ROUND(AVG(total_population), 0)             AS avg_pop,
        ROUND(AVG(median_household_income), 0)       AS avg_income,
        ROUND(AVG(total_poi_count), 0)              AS avg_pois,
        ROUND(AVG(h3_cell_count), 0)                AS avg_h3_cells,
        ROUND(AVG(higher_education_rate), 4)         AS avg_higher_ed_rate,
        ROUND(AVG(distance_to_nearest_store_miles), 2) AS avg_dist_nearest_store
    FROM {output_table}
    GROUP BY urbanicity_category
    ORDER BY urbanicity_category
"""))

# COMMAND ----------

# Sanity check: no stores should have zero H3 cells
display(spark.sql(f"""
    SELECT location_id, h3_cell_count, area_sqkm
    FROM {output_table}
    WHERE h3_cell_count = 0
"""))
