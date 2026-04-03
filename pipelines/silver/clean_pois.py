# Databricks notebook source
# MAGIC %md
# MAGIC # POI Cleaning - Silver Layer
# MAGIC
# MAGIC Cleans and structures raw POI data from Bronze layer.
# MAGIC
# MAGIC **Purpose**: Transform raw OSM POI data into cleaned structured format.
# MAGIC
# MAGIC **Input**: Bronze POI table (`{catalog}.bronze.osm_pois_raw`)
# MAGIC **Output**: Silver table with cleaned POI data (poi_id, name, category, latitude, longitude, address)
# MAGIC

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# Notebook parameters
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("state_filter", "NY")

# Extract parameters
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
state_filter = dbutils.widgets.get("state_filter")

# Auto-derive config path
import os
_nb_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
config_path = os.path.join("/Workspace", _nb_dir.lstrip("/"), "..", "resources", "configs", "poi_config.yml")

assert catalog and schema, "Missing required parameters: catalog, schema"

# Load configuration
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

poi_cleaning_config = config['poi_cleaning']
table_config = config['table_names']

# Construct table names from config
input_table = f"{catalog}.{schema}.bronze_{table_config['bronze_raw_suffix']}"
output_table = f"{catalog}.{schema}.silver_{table_config['silver_cleaned_suffix']}"

# COMMAND ----------

# Read raw POI data
pois_raw = spark.read.table(input_table)

poi_count = pois_raw.count()
print(f"Input table: {input_table} ({poi_count} rows)")

if poi_count == 0:
    raise RuntimeError(f"No POIs found in input table: {input_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract and Clean Columns

# COMMAND ----------

# Extract category and subcategory from tags using UDF
category_priority = poi_cleaning_config.get('category_priority', ['shop', 'amenity', 'leisure', 'tourism', 'office', 'public_transport', 'railway'])

# Extract category/subcategory — Spark-native, no UDF
address_fields = poi_cleaning_config.get('address_fields', ['addr:housenumber', 'addr:street', 'addr:city', 'addr:state', 'addr:postcode'])

category_expr = F.coalesce(*[
    F.when(
        F.col("tags")[tag].isNotNull() & (F.trim(F.col("tags")[tag]) != ""),
        F.struct(F.lit(tag).alias("category"), F.trim(F.col("tags")[tag]).alias("subcategory"))
    )
    for tag in category_priority
])

# Build address — Spark-native, no UDF
address_expr = F.concat_ws(", ", *[
    F.when(F.col("tags")[field].isNotNull(), F.col("tags")[field])
    for field in address_fields
])
address_expr = F.when(address_expr == "", None).otherwise(address_expr)

# Clean POI data
poi_id_prefix = poi_cleaning_config.get('poi_id_prefix', 'poi_')
pois_with_category = pois_raw \
    .withColumn("poi_id", F.concat(F.lit(poi_id_prefix), F.col("osm_id"))) \
    .withColumn("name", F.col("tags")["name"]) \
    .withColumn("category_struct", category_expr) \
    .withColumn("poi_category", F.col("category_struct.category")) \
    .withColumn("poi_subcategory", F.col("category_struct.subcategory")) \
    .withColumn("latitude", F.col("latitude").cast("double")) \
    .withColumn("longitude", F.col("longitude").cast("double")) \
    .withColumn("address", address_expr) \
    .withColumn("ingestion_timestamp", F.lit(datetime.now()))

pois_cleaned = pois_with_category \
    .select(
        "poi_id", "name", "poi_category", "poi_subcategory",
        "latitude", "longitude", "address",
        "osm_id", "osm_type", "ingestion_timestamp"
    ) \
    .filter(
        F.col("latitude").isNotNull() &
        F.col("longitude").isNotNull() &
        F.col("poi_category").isNotNull()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Data Quality

# COMMAND ----------

# Validate coordinate bounds
coord_bounds = poi_cleaning_config.get('coordinate_bounds', {
    'latitude_min': -90, 'latitude_max': 90,
    'longitude_min': -180, 'longitude_max': 180
})

pois_cleaned = pois_cleaned.filter(
    (F.col("latitude") >= coord_bounds['latitude_min']) &
    (F.col("latitude") <= coord_bounds['latitude_max']) &
    (F.col("longitude") >= coord_bounds['longitude_min']) &
    (F.col("longitude") <= coord_bounds['longitude_max'])
)

# Apply state filter if provided (filter POIs to state bounding box)
if state_filter and state_filter.strip():
    print(f"Applying state filter: {state_filter}")
    state_bbox = spark.sql(f"""
        SELECT ST_XMin(ST_GeomFromText(geometry_wkt, 4326)) as lng_min,
               ST_XMax(ST_GeomFromText(geometry_wkt, 4326)) as lng_max,
               ST_YMin(ST_GeomFromText(geometry_wkt, 4326)) as lat_min,
               ST_YMax(ST_GeomFromText(geometry_wkt, 4326)) as lat_max
        FROM {catalog}.{schema}.bronze_census_states
        WHERE state_abbr = '{state_filter.strip().upper()}'
    """).collect()
    if state_bbox:
        bb = state_bbox[0]
        pois_cleaned = pois_cleaned.filter(
            (F.col("latitude") >= bb.lat_min) & (F.col("latitude") <= bb.lat_max) &
            (F.col("longitude") >= bb.lng_min) & (F.col("longitude") <= bb.lng_max)
        )

display(pois_cleaned.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

pois_cleaned.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .option("delta.autoOptimize.autoCompact", "true") \
    .saveAsTable(output_table)

summary = spark.sql(f"""
    SELECT
        COUNT(*) as total_pois,
        COUNT(DISTINCT poi_category) as poi_categories,
        COUNT(DISTINCT poi_subcategory) as poi_subcategories,
        COUNT(DISTINCT osm_type) as osm_types,
        COUNT(CASE WHEN name IS NOT NULL THEN 1 END) as pois_with_name,
        COUNT(CASE WHEN address IS NOT NULL THEN 1 END) as pois_with_address
    FROM {output_table}
""")

display(summary)
