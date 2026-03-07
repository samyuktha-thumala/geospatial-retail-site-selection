# Databricks notebook source
# MAGIC %md
# MAGIC # POI Extraction from OSM - Bronze Layer
# MAGIC
# MAGIC Extracts raw Point of Interest (POI) data from OpenStreetMap PBF files.
# MAGIC
# MAGIC **Purpose**: Raw extraction of POI nodes with their tags.
# MAGIC
# MAGIC **Input**: OSM PBF file from Bronze volume
# MAGIC **Output**: Bronze table with raw POI data (osm_id, osm_type, latitude, longitude, tags)
# MAGIC

# COMMAND ----------

# MAGIC %pip install pyyaml osmium

# COMMAND ----------

import osmium
import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import os

# Notebook parameters
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("osm_region", "")
dbutils.widgets.text("config_path", "")

# Extract parameters
catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
osm_region = dbutils.widgets.get("osm_region")
config_path = dbutils.widgets.get("config_path")

assert catalog and bronze_schema and osm_region and config_path, "Missing required parameters"

# Load configuration
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

poi_config = config['poi_extraction']
table_config = config['table_names']
paths_config = config['paths']

# Define paths
osm_file_path = f"/Volumes/{catalog}/{bronze_schema}/osm_data/{osm_region}-latest.osm.pbf"
output_table = f"{catalog}.{bronze_schema}.bronze_{table_config['bronze_raw_suffix']}"
temp_path = paths_config['temp_path']

# COMMAND ----------

# Check if OSM file exists
try:
    dbutils.fs.ls(osm_file_path)
except Exception as e:
    raise RuntimeError(f"OSM file not found: {osm_file_path}. Please ensure OSM download task completed successfully.") from e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define POI Handler
# MAGIC
# MAGIC Extracts nodes that have POI tags (amenity, shop, leisure, etc.).
# MAGIC Only nodes with relevant POI tags are extracted - nodes without POI tags are skipped.
# MAGIC

# COMMAND ----------

BATCH_SIZE = 500_000  # Flush to Delta every 500K POIs

class POIHandler(osmium.SimpleHandler):
    """Handler to extract POI nodes from OSM data, with batched writes for large files."""

    def __init__(self, extract_all=True, poi_tag_categories=None, batch_size=BATCH_SIZE):
        super().__init__()
        self.pois = []
        self.batch_size = batch_size
        self.total_count = 0
        self.batch_num = 0

        default_poi_tags = [
            'amenity', 'shop', 'leisure', 'tourism', 'office',
            'public_transport', 'railway', 'natural', 'building'
        ]

        if extract_all:
            self.poi_tag_categories = default_poi_tags
        else:
            self.poi_tag_categories = poi_tag_categories if poi_tag_categories else default_poi_tags

    def _has_poi_tag(self, tags):
        tag_keys = {tag.k for tag in tags}
        return any(poi_tag in tag_keys for poi_tag in self.poi_tag_categories)

    def _flush_batch(self):
        """Write current batch to Delta table."""
        if not self.pois:
            return

        schema = StructType([
            StructField("osm_id", StringType(), False),
            StructField("osm_type", StringType(), False),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("tags", MapType(StringType(), StringType()), True)
        ])

        batch_df = spark.createDataFrame(self.pois, schema=schema)

        # First batch overwrites, subsequent batches append
        mode = "overwrite" if self.batch_num == 0 else "append"
        batch_df.write \
            .format("delta") \
            .mode(mode) \
            .option("overwriteSchema", "true" if self.batch_num == 0 else "false") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .saveAsTable(output_table)

        self.batch_num += 1
        self.total_count += len(self.pois)
        print(f"  Batch {self.batch_num}: wrote {len(self.pois)} POIs (total: {self.total_count})")
        self.pois = []

    def node(self, n):
        if not n.location.valid():
            return

        if self._has_poi_tag(n.tags):
            tags_dict = dict(n.tags)
            if tags_dict:
                self.pois.append({
                    'osm_id': str(n.id),
                    'osm_type': 'node',
                    'latitude': n.location.lat,
                    'longitude': n.location.lon,
                    'tags': tags_dict
                })

                # Flush when batch is full
                if len(self.pois) >= self.batch_size:
                    self._flush_batch()

    def finalize(self):
        """Flush any remaining POIs."""
        self._flush_batch()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract POIs from OSM File
# MAGIC

# COMMAND ----------

# Parse OSM file and extract POIs with batched writes
extract_all = poi_config.get('extract_all', True)
poi_tag_categories = poi_config.get('poi_tag_categories', [])

handler = POIHandler(extract_all=extract_all, poi_tag_categories=poi_tag_categories, batch_size=BATCH_SIZE)

if not os.path.exists(osm_file_path):
    raise RuntimeError(f"OSM file not found at: {osm_file_path}")

print(f"Parsing OSM file: {osm_file_path}")
print(f"Batch size: {BATCH_SIZE:,} POIs per flush")
handler.apply_file(osm_file_path)

# Flush remaining POIs
handler.finalize()

poi_count = handler.total_count
print(f"\nExtraction complete: {poi_count:,} total POIs written to {output_table}")

if poi_count == 0:
    raise RuntimeError("No POIs found in OSM file.")


# COMMAND ----------

# Display sample of extracted POIs
poi_df = spark.table(output_table)
display(poi_df.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics
# MAGIC

# COMMAND ----------

summary = spark.sql(f"""
    SELECT
        COUNT(*) as total_pois,
        COUNT(DISTINCT osm_id) as unique_pois,
        COUNT(DISTINCT osm_type) as osm_types,
        COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as pois_with_coords,
        COUNT(CASE WHEN tags IS NOT NULL THEN 1 END) as pois_with_tags,
        COUNT(CASE WHEN tags IS NULL THEN 1 END) as pois_without_tags
    FROM {output_table}
""")

display(summary)
