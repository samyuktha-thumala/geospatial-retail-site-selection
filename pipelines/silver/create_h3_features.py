# Databricks notebook source
# MAGIC %md
# MAGIC # H3 Feature Engineering
# MAGIC
# MAGIC Generates H3 resolution-8 features by aggregating:
# MAGIC - **Urbanicity** from ZCTA population density (polyfill + string join)
# MAGIC - **Demographics** from Block Groups (area-weighted interpolation)
# MAGIC - **POI counts** by category from `silver_osm_pois`
# MAGIC - **Competitor counts** by brand from `bronze_competitor_locations`
# MAGIC - **Distance features** (haversine, in miles) to nearest stores and competitors
# MAGIC
# MAGIC **Serverless-safe:** No .cache(), no GEOGRAPHY .collect()
# MAGIC
# MAGIC **Output:** `silver_h3_features`

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("state_fips", "36")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
state_fips = dbutils.widgets.get("state_fips")

H3_RES = 8
H3_AREA_SQKM = 0.7373  # constant for res-8
NULL_DISTANCE = 999.0  # miles

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Generate H3 grid from state polygon polyfill

# COMMAND ----------

h3_base_table = f"{catalog}.{schema}._tmp_h3_base"

state_df = spark.table(f"{catalog}.{schema}.bronze_census_states").filter(F.col("state_fips") == state_fips)

# Convert WKT to native geometry if needed, then polyfill
state_cols = [c.lower() for c in state_df.columns]
if "geometry" in state_cols:
    polyfill_expr = f"h3_polyfillash3string(ST_AsText(geometry), {H3_RES})"
else:
    polyfill_expr = f"h3_polyfillash3string(geometry_wkt, {H3_RES})"

h3_cells = state_df.select(
    F.explode(F.expr(polyfill_expr)).alias("h3_cell_id")
).distinct()

h3_cells.select(
    "h3_cell_id",
    F.expr("ST_GeomFromGeoJSON(h3_boundaryasgeojson(h3_cell_id))").alias("h3_geometry"),
    F.expr("h3_boundaryaswkt(h3_cell_id)").alias("h3_geometry_wkt"),
    F.expr("ST_GeomFromWKT(h3_centeraswkt(h3_cell_id), 4326)").alias("h3_center"),
    F.expr("ST_Y(ST_GeomFromWKT(h3_centeraswkt(h3_cell_id), 4326))").alias("center_lat"),
    F.expr("ST_X(ST_GeomFromWKT(h3_centeraswkt(h3_cell_id), 4326))").alias("center_lon"),
    F.lit(H3_AREA_SQKM).alias("h3_area_sqkm"),
).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(h3_base_table)

cell_count = spark.sql(f"SELECT COUNT(*) as c FROM {h3_base_table}").collect()[0]["c"]
print(f"H3 base: {cell_count:,} cells (res {H3_RES}, area={H3_AREA_SQKM} km²)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: POI aggregation (h3_pointash3string — no spatial join)

# COMMAND ----------

poi_agg = spark.sql(f"""
    SELECT
        h3_pointash3string(CONCAT('POINT(', longitude, ' ', latitude, ')'), {H3_RES}) AS h3_cell_id,
        poi_category,
        COUNT(*) AS cnt
    FROM {catalog}.{schema}.silver_osm_pois
    WHERE poi_category IS NOT NULL
    GROUP BY 1, 2
""")

poi_pivot = poi_agg.groupBy("h3_cell_id").pivot("poi_category").agg(F.sum("cnt"))
poi_cats = [c for c in poi_pivot.columns if c != "h3_cell_id"]
poi_pivot = poi_pivot.fillna(0, subset=poi_cats)
poi_pivot = poi_pivot.withColumn("total_poi_count", sum(F.col(c) for c in poi_cats))

for c in poi_cats:
    poi_pivot = poi_pivot.withColumnRenamed(c, f"poi_count_{c}")

print(f"POI features: {len(poi_cats)} categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Urbanicity from ZCTA population density (point-in-polygon)
# MAGIC
# MAGIC Each H3 cell's center point is matched to its containing ZCTA polygon
# MAGIC via ST_Contains. Simple point-in-polygon — no polyfill explosion needed.

# COMMAND ----------

h3_urbanicity = spark.sql(f"""
    SELECT
        h.h3_cell_id,
        ROUND(LEAST(1.0, COALESCE(z.population_density_sqkm, 0.0) / 10000.0), 4) AS urbanicity_score,
        CASE
            WHEN z.population_density_sqkm > 5000 THEN 'urban'
            WHEN z.population_density_sqkm > 500 THEN 'suburban'
            ELSE 'rural'
        END AS urbanicity_category
    FROM {h3_base_table} h
    LEFT JOIN {catalog}.{schema}.silver_census_zcta z
        ON ST_Contains(z.geometry, h.h3_center)
""")

display(h3_urbanicity.groupBy("urbanicity_category").count().orderBy("urbanicity_category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Demographics — area-weighted from Block Groups

# COMMAND ----------

# Spatial join H3 × Block Groups using native geometry types.
# Direct ST_Intersects with area-weighted fractions — spatial index handles pruning.
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW bg_h3_overlap AS
    SELECT
        h.h3_cell_id,
        d.geoid AS bg_geoid,
        CASE WHEN ST_Area(d.geometry) > 0
            THEN ST_Area(ST_Intersection(h.h3_geometry, d.geometry)) / ST_Area(d.geometry)
            ELSE 0 END AS area_fraction,
        d.*
    FROM {h3_base_table} h
    INNER JOIN {catalog}.{schema}.silver_census_demographics d
        ON ST_Intersects(h.h3_geometry, d.geometry)
""")

bg_h3_count = spark.sql("SELECT COUNT(*) as c FROM bg_h3_overlap").collect()[0]["c"]
print(f"H3 × BG intersections: {bg_h3_count:,}")

# COMMAND ----------

# Dynamically discover available columns from the demographics table
all_demo_cols = [r["col_name"] for r in spark.sql(f"DESCRIBE {catalog}.{schema}.silver_census_demographics").collect()]

desired_count_vars = [
    "total_population", "bachelors_degree", "masters_degree", "doctorate_degree",
    "in_labor_force", "unemployed", "total_housing_units", "total_households",
    "owner_occupied", "renter_occupied", "total_commuters", "public_transit_commuters",
    "employed", "not_in_labor_force", "vacant_housing_units",
]
desired_median_vars = ["median_household_income", "median_home_value", "median_age", "median_gross_rent"]

count_vars = [v for v in desired_count_vars if v in all_demo_cols]
median_vars = [v for v in desired_median_vars if v in all_demo_cols]
select_cols = ["h3_cell_id", "area_fraction"] + count_vars + median_vars
print(f"Demographics columns available: count={count_vars}, median={median_vars}")

bg_h3 = spark.sql(f"SELECT {', '.join(select_cols)} FROM bg_h3_overlap")

# Count variables: area-weighted sum
weighted_aggs = [
    F.round(F.sum(F.coalesce(F.col(v), F.lit(0)) * F.col("area_fraction"))).cast("long").alias(v)
    for v in count_vars
]
demo_counts = bg_h3.groupBy("h3_cell_id").agg(*weighted_aggs)

# Median variables: population-weighted average
bg_h3_w = bg_h3.withColumn(
    "weight", F.coalesce(F.col("total_population"), F.lit(0)) * F.col("area_fraction")
)

median_aggs = [
    F.round(
        F.sum(F.coalesce(F.col(v).cast("double"), F.lit(0.0)) * F.col("weight"))
        / F.greatest(F.sum("weight"), F.lit(1.0))
    ).alias(v)
    for v in median_vars
]

demo_medians = bg_h3_w.groupBy("h3_cell_id").agg(*median_aggs)

demo_features = demo_counts.join(demo_medians, "h3_cell_id", "outer").fillna(0, subset=count_vars)
print(f"Demographic features: {len(count_vars)} count + {len(median_vars)} median")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Competitor counts (h3_pointash3string — no spatial join)

# COMMAND ----------

comp_h3 = spark.sql(f"""
    SELECT
        h3_pointash3string(CONCAT('POINT(', lng, ' ', lat, ')'), {H3_RES}) AS h3_cell_id,
        brand
    FROM {catalog}.{schema}.bronze_competitor_locations
""")

comp_total = comp_h3.filter(F.col("brand").isNotNull()).groupBy("h3_cell_id").agg(
    F.count("brand").alias("total_competitor_count")
)

comp_brand = comp_h3.filter(F.col("brand").isNotNull()).groupBy("h3_cell_id").pivot("brand").agg(F.count("brand"))
brand_cols = [c for c in comp_brand.columns if c != "h3_cell_id"]
for c in brand_cols:
    comp_brand = comp_brand.withColumnRenamed(c, f"competitor_count_{c.replace(' ', '_')}")

comp_features = comp_total.join(comp_brand, "h3_cell_id", "left")
comp_all_cols = [c for c in comp_features.columns if c != "h3_cell_id"]
comp_features = comp_features.fillna(0, subset=comp_all_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Distance features (H3 k-ring — no cross join)
# MAGIC
# MAGIC For each store/competitor, generate a k-ring of nearby H3 cells and compute
# MAGIC haversine distance only within that ring. At res-8 with k=15, each ring covers
# MAGIC ~75 miles — far enough for any meaningful trade area analysis.

# COMMAND ----------

HAVERSINE_SQL = """3958.8 * 2 * ASIN(SQRT(
    POWER(SIN(RADIANS((lat2 - lat1) / 2)), 2) +
    COS(RADIANS(lat1)) * COS(RADIANS(lat2)) *
    POWER(SIN(RADIANS((lon2 - lon1) / 2)), 2)
))"""

KRING_K = 15

# Store distances via k-ring
store_dist = spark.sql(f"""
    WITH store_rings AS (
        SELECT
            s.store_number,
            s.lat AS store_lat,
            s.lng AS store_lng,
            EXPLODE(h3_kring(h3_pointash3string(CONCAT('POINT(', s.lng, ' ', s.lat, ')'), {H3_RES}), {KRING_K})) AS h3_cell_id
        FROM {catalog}.{schema}.bronze_store_locations s
    )
    SELECT
        sr.h3_cell_id,
        MIN({HAVERSINE_SQL.replace('lat1', 'h.center_lat').replace('lon1', 'h.center_lon').replace('lat2', 'sr.store_lat').replace('lon2', 'sr.store_lng')}) AS distance_to_nearest_store_miles
    FROM store_rings sr
    INNER JOIN {h3_base_table} h ON sr.h3_cell_id = h.h3_cell_id
    GROUP BY sr.h3_cell_id
""")

# Competitor distances via k-ring
comp_dist = spark.sql(f"""
    WITH comp_rings AS (
        SELECT
            c.brand,
            c.lat AS comp_lat,
            c.lng AS comp_lng,
            EXPLODE(h3_kring(h3_pointash3string(CONCAT('POINT(', c.lng, ' ', c.lat, ')'), {H3_RES}), {KRING_K})) AS h3_cell_id
        FROM {catalog}.{schema}.bronze_competitor_locations c
    )
    SELECT
        cr.h3_cell_id,
        cr.brand,
        MIN({HAVERSINE_SQL.replace('lat1', 'h.center_lat').replace('lon1', 'h.center_lon').replace('lat2', 'cr.comp_lat').replace('lon2', 'cr.comp_lng')}) AS min_dist
    FROM comp_rings cr
    INNER JOIN {h3_base_table} h ON cr.h3_cell_id = h.h3_cell_id
    GROUP BY cr.h3_cell_id, cr.brand
""")

comp_dist_pivot = comp_dist.groupBy("h3_cell_id").pivot("brand").agg(F.first("min_dist"))
dist_brand_cols = [c for c in comp_dist_pivot.columns if c != "h3_cell_id"]
for c in dist_brand_cols:
    comp_dist_pivot = comp_dist_pivot.withColumnRenamed(c, f"distance_to_{c.lower().replace(' ', '_')}_miles")

distance_features = store_dist.join(comp_dist_pivot, "h3_cell_id", "left")
dist_all_cols = [c for c in distance_features.columns if c.startswith("distance_to_")]
distance_features = distance_features.fillna(NULL_DISTANCE, subset=dist_all_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Join all features and write

# COMMAND ----------

h3_base_cols = spark.sql(f"SELECT h3_cell_id, h3_geometry, h3_area_sqkm, center_lat, center_lon FROM {h3_base_table}")

h3_features = h3_base_cols \
    .join(poi_pivot, "h3_cell_id", "left") \
    .join(demo_features, "h3_cell_id", "left") \
    .join(comp_features, "h3_cell_id", "left") \
    .join(distance_features, "h3_cell_id", "left") \
    .join(h3_urbanicity, "h3_cell_id", "left") \
    .withColumn("processing_timestamp", F.current_timestamp())

# Compute population density
h3_features = h3_features.withColumn(
    "population_density",
    F.when(F.col("h3_area_sqkm") > 0, F.col("total_population") / F.col("h3_area_sqkm")).otherwise(0)
)

# Write
output_table = f"{catalog}.{schema}.silver_h3_features"
h3_features.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {output_table}").collect()[0]["cnt"]
print(f"Written {row_count:,} rows to {output_table}")

# COMMAND ----------

# Drop temp table
spark.sql(f"DROP TABLE IF EXISTS {h3_base_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        COUNT(*) as total_cells,
        ROUND(AVG(h3_area_sqkm), 4) as avg_area_sqkm,
        CAST(SUM(total_poi_count) AS BIGINT) as total_pois_mapped,
        CAST(SUM(total_population) AS BIGINT) as total_population,
        CAST(SUM(total_competitor_count) AS BIGINT) as total_competitors_mapped,
        ROUND(AVG(distance_to_nearest_store_miles), 2) as avg_dist_to_store
    FROM {output_table}
"""))

# By urbanicity
display(spark.sql(f"""
    SELECT urbanicity_category,
        COUNT(*) as cells,
        ROUND(AVG(total_population), 1) as avg_pop,
        ROUND(AVG(total_poi_count), 1) as avg_pois,
        ROUND(AVG(median_household_income), 0) as avg_income,
        ROUND(AVG(population_density), 1) as avg_pop_density_per_sqkm,
        ROUND(AVG(distance_to_nearest_store_miles), 2) as avg_dist_to_store
    FROM {output_table}
    GROUP BY urbanicity_category
    ORDER BY urbanicity_category
"""))

# Sanity check: total population
total_pop = spark.sql(f"SELECT SUM(total_population) as tp FROM {output_table}").collect()[0]["tp"]
print(f"\nTotal population in H3 grid: {total_pop:,.0f}")
print(f"Expected NY state population: ~19,700,000")
print(f"Ratio: {total_pop/19700000:.2%}")
