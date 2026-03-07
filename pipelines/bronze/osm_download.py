# Databricks notebook source
# MAGIC %md
# MAGIC # OSM Road Network Download - Bronze Layer
# MAGIC
# MAGIC Downloads OpenStreetMap road network data from Geofabrik and stores in Unity Catalog Volume.
# MAGIC
# MAGIC **Data Source:** Geofabrik OSM Extracts
# MAGIC **Format:** PBF (Protocolbuffer Binary Format)
# MAGIC
# MAGIC **Output:**
# MAGIC - Volume: `/Volumes/{catalog}/{bronze_schema}/osm_data/{region}-latest.osm.pbf`
# MAGIC - Table: `{catalog}.{bronze_schema}.bronze_osm_downloads` (tracking metadata)
# MAGIC

# COMMAND ----------

import requests
import shutil
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import uuid

# Notebook parameters
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("osm_data_volume", "")
dbutils.widgets.text("osm_url", "")
dbutils.widgets.text("region", "")

# Extract parameters
catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
osm_data_volume = dbutils.widgets.get("osm_data_volume")
osm_url = dbutils.widgets.get("osm_url")
region = dbutils.widgets.get("region")

assert catalog and bronze_schema and osm_url, "Missing required parameters"

# COMMAND ----------

download_id = str(uuid.uuid4())
download_start = datetime.now()

# Extract filename from URL
osm_filename = osm_url.split('/')[-1]
volume_file_path = f"{osm_data_volume}{osm_filename}"

# Check if file already exists (idempotency)
file_exists = False
try:
    existing_files = dbutils.fs.ls(volume_file_path)
    file_size_mb = existing_files[0].size / (1024 * 1024)
    status = "existing"
    download_end = download_start
    duration_seconds = 0.0
    file_exists = True
    print(f"File already exists: {volume_file_path} ({file_size_mb:.1f} MB)")
except:
    file_exists = False

if not file_exists:
    # Create volume if needed
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{bronze_schema}.osm_data")

    # Download directly to volume path
    local_volume_path = volume_file_path.replace("dbfs:", "/dbfs")
    print(f"Downloading {osm_url} to {volume_file_path}...")

    with requests.get(osm_url, stream=True, timeout=600) as r:
        r.raise_for_status()
        file_size_bytes = int(r.headers.get('content-length', 0))
        file_size_mb = file_size_bytes / (1024 * 1024)
        print(f"File size: {file_size_mb:.1f} MB")

        with open(local_volume_path, "wb") as f:
            shutil.copyfileobj(r.raw, f)

    download_end = datetime.now()
    duration_seconds = (download_end - download_start).total_seconds()
    status = "completed"
    print(f"Download complete in {duration_seconds:.1f}s")

# COMMAND ----------

# Write tracking metadata to Unity Catalog
osm_table = f"{catalog}.{bronze_schema}.bronze_osm_downloads"

data = [(
    download_id,
    datetime.now().date(),
    region,
    volume_file_path,
    int(file_size_mb),
    status,
    duration_seconds,
    download_start,
    download_end
)]

schema = StructType([
    StructField("download_id", StringType(), False),
    StructField("download_date", DateType(), False),
    StructField("region", StringType(), False),
    StructField("osm_file_path", StringType(), False),
    StructField("file_size_mb", LongType(), False),
    StructField("download_status", StringType(), False),
    StructField("duration_seconds", DoubleType(), False),
    StructField("download_start", TimestampType(), False),
    StructField("download_end", TimestampType(), False)
])

osm_df = spark.createDataFrame(data, schema)

(osm_df
 .write
 .mode("append")
 .saveAsTable(osm_table))
