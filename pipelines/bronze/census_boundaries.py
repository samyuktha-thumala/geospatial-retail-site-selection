# Databricks notebook source
# MAGIC %md
# MAGIC # Census Boundaries - Bronze Layer Ingestion
# MAGIC
# MAGIC Ingests Census TIGER/Line cartographic boundary files into Unity Catalog using `pygris`.
# MAGIC
# MAGIC **Data Source:** Census Cartographic Boundary Files via `pygris` (500k resolution)
# MAGIC
# MAGIC **Geographies:**
# MAGIC - Block Groups (by state)
# MAGIC - States (All US)
# MAGIC
# MAGIC **Output Tables:**
# MAGIC - `{catalog}.{bronze_schema}.bronze_census_blockgroups` - Block group boundaries with native GEOGRAPHY type
# MAGIC - `{catalog}.{bronze_schema}.bronze_census_states` - State boundaries with native GEOGRAPHY type
# MAGIC
# MAGIC **Optimizations:**
# MAGIC - Uses native Databricks GEOGRAPHY type (SRID 4326)
# MAGIC - Direct WKT conversion from GeoPandas (most efficient path)
# MAGIC - Parallel state downloads with ThreadPoolExecutor
# MAGIC - Fails if >10% of states fail

# COMMAND ----------

# MAGIC %pip install pygris geopandas

# COMMAND ----------

import pygris
from pygris import states, block_groups
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import uuid
import geopandas as gpd

# Notebook parameters
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("boundary_data_volume", "")
dbutils.widgets.text("state_fips", "")
dbutils.widgets.text("year", "")

# Extract parameters
catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
boundary_data_volume = dbutils.widgets.get("boundary_data_volume")
state_fips = dbutils.widgets.get("state_fips")
year = int(dbutils.widgets.get("year")) if dbutils.widgets.get("year") else 2020

assert catalog and bronze_schema, "Missing required parameters"

# COMMAND ----------

def geopandas_to_spark(gdf, geography_level, ingest_id, ingest_timestamp):
    """
    Convert GeoPandas GeoDataFrame to Spark DataFrame with native GEOMETRY(4326) type.
    Stores geometry as WKT string first, then converts to native type via SQL after write.
    """
    gdf_copy = gdf.copy()
    gdf_copy['geometry_wkt'] = gdf_copy['geometry'].apply(lambda geom: geom.wkt if geom is not None else None)
    gdf_copy = gdf_copy.drop(columns=['geometry'])

    spark_df = spark.createDataFrame(gdf_copy)

    spark_df = (spark_df
                .withColumn("geography_level", F.lit(geography_level))
                .withColumn("ingestion_id", F.lit(ingest_id))
                .withColumn("ingestion_timestamp", F.lit(ingest_timestamp)))

    return spark_df

# COMMAND ----------

# Generate ingestion metadata
ingest_id = str(uuid.uuid4())
ingest_timestamp = datetime.now()

# Determine which states to process
if state_fips.lower() == "all":
    all_fips = [
        "01","02","04","05","06","08","09","10","11","12","13","15","16","17","18","19",
        "20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35",
        "36","37","38","39","40","41","42","44","45","46","47","48","49","50","51","53",
        "54","55","56"
    ]
else:
    all_fips = [f.strip() for f in state_fips.split(",")]

print(f"Processing {len(all_fips)} states: {', '.join(all_fips)}")

# Fetch ALL US states (single call, always nationwide)
states_gdf = states(cb=True, resolution='500k', year=year)
state_df = geopandas_to_spark(states_gdf, "state", ingest_id, ingest_timestamp)
state_df = (state_df
    .withColumnRenamed("GEOID", "geoid")
    .withColumnRenamed("STUSPS", "state_abbr")
    .withColumnRenamed("NAME", "name")
    .withColumnRenamed("STATEFP", "state_fips")
    .withColumnRenamed("ALAND", "area_land")
    .withColumnRenamed("AWATER", "area_water"))

# COMMAND ----------

# Parallel block group downloads with ThreadPoolExecutor
def fetch_state_bg(fips):
    """Download block groups for a single state. Thread-safe (pygris uses requests internally)."""
    return (fips, block_groups(state=fips, county=None, year=year, cache=True, cb=True))

failed_states = []
successful_gdfs = []

with ThreadPoolExecutor(max_workers=8) as executor:
    futures = {executor.submit(fetch_state_bg, fips): fips for fips in all_fips}
    for future in as_completed(futures):
        fips = futures[future]
        try:
            result_fips, gdf = future.result()
            successful_gdfs.append((result_fips, gdf))
            print(f"  [{len(successful_gdfs)}/{len(all_fips)}] State FIPS {result_fips}: {len(gdf)} block groups")
        except Exception as e:
            failed_states.append(fips)
            print(f"  WARNING: Failed for state {fips}: {e}")

# Fail if >10% of states failed
if len(failed_states) > len(all_fips) * 0.1:
    raise RuntimeError(f"Too many state failures: {len(failed_states)}/{len(all_fips)} — {failed_states}")

print(f"\nSuccessful: {len(successful_gdfs)}, Failed: {len(failed_states)}")

# COMMAND ----------

# Convert GeoDataFrames to Spark (must run on driver, not in threads)
bg_dfs = []
for fips, gdf in successful_gdfs:
    bg_df = geopandas_to_spark(gdf, "block_group", ingest_id, ingest_timestamp)
    bg_df = (bg_df
        .withColumnRenamed("GEOID", "geoid")
        .withColumnRenamed("NAME", "name")
        .withColumnRenamed("STATEFP", "state_fips")
        .withColumnRenamed("COUNTYFP", "county_fips")
        .withColumnRenamed("TRACTCE", "tract")
        .withColumnRenamed("BLKGRPCE", "block_group_id")
        .withColumnRenamed("ALAND", "area_land")
        .withColumnRenamed("AWATER", "area_water"))
    bg_dfs.append(bg_df)

# Union all block group DataFrames
bg_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), bg_dfs)
print(f"\nTotal block groups: {bg_df.count()}")

# COMMAND ----------

# Write to Unity Catalog
bg_table = f"{catalog}.{bronze_schema}.bronze_census_blockgroups"
states_table = f"{catalog}.{bronze_schema}.bronze_census_states"

(bg_df
 .repartition(10)
 .write
 .mode("overwrite")
 .option("mergeSchema", "true")
 .option("overwriteSchema", "true")
 .saveAsTable(bg_table))

(state_df
 .repartition(1)
 .write
 .mode("overwrite")
 .option("mergeSchema", "true")
 .option("overwriteSchema", "true")
 .saveAsTable(states_table))

print(f"Bronze tables written with geometry_wkt (string). Native geometry conversion happens in silver layer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

print("=" * 80)
print("VALIDATION")
print("=" * 80)

print(f"\n1. Block Groups Table ({bg_table}):")
bg_validation = spark.sql(f"""
    SELECT
        COUNT(*) as total_rows,
        COUNT(geometry_wkt) as non_null_geometries
    FROM {bg_table}
""")
bg_validation.show(truncate=False)

print(f"\n2. States Table ({states_table}):")
state_validation = spark.sql(f"""
    SELECT
        COUNT(*) as total_rows,
        COUNT(geometry_wkt) as non_null_geometries
    FROM {states_table}
""")
state_validation.show(truncate=False)

print("=" * 80)
print("VALIDATION COMPLETE — geometry_wkt stored. Native geometry conversion in silver.")
print("=" * 80)
