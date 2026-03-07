# Databricks notebook source
# MAGIC %md
# MAGIC # Census Demographics - Bronze Layer Ingestion
# MAGIC
# MAGIC Ingests ACS 5-Year demographic data via Census API into Unity Catalog.
# MAGIC
# MAGIC **Configuration:** Externalized to YAML (`resources/configs/census_variables.yml`)
# MAGIC **Orchestration:** Databricks Asset Bundle with task-level retries
# MAGIC **Storage:** Unity Catalog managed tables and volumes
# MAGIC

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

import requests
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import uuid

# Widget parameters (injected by DABs job)
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("census_api_key", "")
dbutils.widgets.text("census_data_volume", "")
dbutils.widgets.text("config_path", "")
dbutils.widgets.text("acs_year", "")
dbutils.widgets.text("state_fips", "")

# Extract parameters
catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
census_api_key = dbutils.widgets.get("census_api_key")
census_data_volume = dbutils.widgets.get("census_data_volume")
config_path = dbutils.widgets.get("config_path")
acs_year = dbutils.widgets.get("acs_year")
state_fips = dbutils.widgets.get("state_fips")

# Validate required parameters
assert catalog and bronze_schema and census_api_key and config_path, "Missing required parameters"

# COMMAND ----------

# Load census variables from externalized YAML config
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

# Flatten nested structure
census_variables = {}
for category, variables in config['acs_5_year_variables'].items():
    census_variables.update(variables)

# COMMAND ----------

def get_census_data(geography_level, state_fips_code, variables_dict, api_key, year):
    """
    Fetch ACS 5-Year data from Census API.
    Census API limits to 50 variables per GET request (including NAME).
    We batch into chunks of 48 data variables + NAME, then merge on geo keys.
    """
    base_url = f"https://api.census.gov/data/{year}/acs/acs5"
    geo_suffix = f"&for=block%20group:*&in=state:{state_fips_code}&in=county:*&in=tract:*&key={api_key}"
    geo_keys = ["NAME", "state", "county", "tract", "block group"]

    var_codes = list(variables_dict.keys())
    BATCH_SIZE = 48  # leave room for NAME + geo fields within 50 limit

    all_headers = None
    all_rows = None

    for batch_start in range(0, len(var_codes), BATCH_SIZE):
        batch_vars = var_codes[batch_start:batch_start + BATCH_SIZE]
        var_string = ",".join(batch_vars)
        url = f"{base_url}?get=NAME,{var_string}{geo_suffix}"

        response = requests.get(url, timeout=120)
        response.raise_for_status()
        data = response.json()
        assert data and len(data) >= 2, f"Invalid API response for {geography_level} batch {batch_start}"

        headers = data[0]
        rows = data[1:]

        if all_headers is None:
            all_headers = headers
            all_rows = {tuple(r[headers.index(k)] for k in geo_keys): r for r in rows}
        else:
            # Merge new data columns into existing rows by geo key
            new_col_indices = [i for i, h in enumerate(headers) if h not in geo_keys]
            new_col_names = [headers[i] for i in new_col_indices]
            all_headers = all_headers + new_col_names
            for r in rows:
                key = tuple(r[headers.index(k)] for k in geo_keys)
                if key in all_rows:
                    all_rows[key] = all_rows[key] + [r[i] for i in new_col_indices]

    final_rows = list(all_rows.values())
    # Fix header: replace "block group" with "block_group" for downstream
    all_headers = [h.replace("block group", "block_group") for h in all_headers]
    return (all_headers, final_rows)


def transform_to_dataframe(headers, rows, geography_level, variables_dict, ingest_id, ingest_timestamp):
    """Transform API response to Spark DataFrame with type casting and metadata."""
    df = spark.createDataFrame(rows, schema=headers)

    # Rename to friendly names
    for census_code, friendly_name in variables_dict.items():
        if census_code in df.columns:
            df = df.withColumnRenamed(census_code, friendly_name)

    # Add metadata
    df = (df
          .withColumn("geography_level", F.lit(geography_level))
          .withColumn("acs_year", F.lit(acs_year))
          .withColumn("ingestion_id", F.lit(ingest_id))
          .withColumn("ingestion_timestamp", F.lit(ingest_timestamp)))

    # Cast numeric columns — use double for median/rate fields, long for counts
    geo_cols = ["NAME", "state", "county", "tract", "block_group",
                "geography_level", "acs_year", "ingestion_id", "ingestion_timestamp"]
    double_cols = {"median_age", "median_household_income", "median_home_value", "median_gross_rent"}
    for col_name in df.columns:
        if col_name not in geo_cols:
            cast_type = "double" if col_name in double_cols else "long"
            df = df.withColumn(col_name, F.col(col_name).cast(cast_type))

    return df

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

print(f"Processing {len(all_fips)} states for census demographics...")

# COMMAND ----------

# Parallel Census API calls with ThreadPoolExecutor
def fetch_state_demographics(fips):
    """Fetch demographics for a single state. Thread-safe (no Spark ops)."""
    headers, rows = get_census_data("block_group", fips, census_variables, census_api_key, acs_year)
    return (fips, headers, rows)

failed_states = []
successful_results = []

with ThreadPoolExecutor(max_workers=8) as executor:
    futures = {executor.submit(fetch_state_demographics, fips): fips for fips in all_fips}
    for future in as_completed(futures):
        fips = futures[future]
        try:
            result = future.result()
            successful_results.append(result)
            print(f"  [{len(successful_results)}/{len(all_fips)}] State FIPS {fips}: {len(result[2])} records")
        except Exception as e:
            failed_states.append(fips)
            print(f"  WARNING: Failed for state {fips}: {e}")

# Fail if >10% of states failed
if len(failed_states) > len(all_fips) * 0.1:
    raise RuntimeError(f"Too many state failures: {len(failed_states)}/{len(all_fips)} — {failed_states}")

print(f"\nSuccessful: {len(successful_results)}, Failed: {len(failed_states)}")

# COMMAND ----------

# Convert results to Spark DataFrames (must run on driver, not in threads)
census_dfs = []
for fips, headers, rows in successful_results:
    df = transform_to_dataframe(headers, rows, "block_group", census_variables, ingest_id, ingest_timestamp)
    census_dfs.append(df)

# Union all state DataFrames
census_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), census_dfs)
print(f"Total demographic records: {census_df.count()}")

# COMMAND ----------

# Write to Unity Catalog
census_table = f"{catalog}.{bronze_schema}.bronze_census_demographics"

(census_df
 .write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(census_table))
