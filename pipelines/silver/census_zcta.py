# Databricks notebook source
# MAGIC %md
# MAGIC # Census ZCTA Boundaries + Demographics — Silver Layer
# MAGIC
# MAGIC Downloads ZIP Code Tabulation Area (ZCTA) boundaries and ACS 5-Year demographics.
# MAGIC ZCTAs are used to define urbanicity at a meaningful geographic scale.
# MAGIC
# MAGIC **No external dependencies** — uses requests + Spark geospatial functions only.
# MAGIC
# MAGIC **Output:** `silver_census_zcta`

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("census_api_key", "")
dbutils.widgets.text("state_fips", "")
dbutils.widgets.text("acs_year", "2023")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
census_api_key = dbutils.widgets.get("census_api_key")
state_fips = dbutils.widgets.get("state_fips")
acs_year = dbutils.widgets.get("acs_year")

assert catalog and schema and state_fips, "catalog, schema, and state_fips must be provided"

# COMMAND ----------

import requests
import json
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Fetch ZCTA boundaries from Census TIGERweb REST API
# MAGIC
# MAGIC Uses the Census TIGERweb REST API which returns GeoJSON directly.
# MAGIC Bounding box computed dynamically from state boundary.

# COMMAND ----------

# Dynamic bounding box from state boundary (no hard-coded coordinates)
# Convert geometry_wkt string to native GEOGRAPHY (runs on serverless which supports ST functions)
state_geom = (spark.table(f"{catalog}.{schema}.bronze_census_states")
    .filter(F.col("state_fips") == state_fips)
    .withColumn("geometry", F.expr("ST_GeomFromText(geometry_wkt, 4326)"))
)
bbox = state_geom.select(
    F.expr("ST_XMin(geometry)").alias("xmin"),
    F.expr("ST_YMin(geometry)").alias("ymin"),
    F.expr("ST_XMax(geometry)").alias("xmax"),
    F.expr("ST_YMax(geometry)").alias("ymax"),
).collect()[0]

print(f"State {state_fips} bounding box: ({bbox.xmin:.2f}, {bbox.ymin:.2f}) to ({bbox.xmax:.2f}, {bbox.ymax:.2f})")

# COMMAND ----------

# Fetch ZCTA boundaries from TIGERweb REST API using the bounding box
TIGERWEB_URL = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2021/MapServer/0/query"

all_features = []
offset = 0
batch_size = 500

while True:
    params = {
        "where": "1=1",
        "geometry": f"{bbox.xmin},{bbox.ymin},{bbox.xmax},{bbox.ymax}",
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "GEOID,AREALAND,AREAWATER",
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "geojson",
        "resultOffset": offset,
        "resultRecordCount": batch_size,
    }

    print(f"Fetching ZCTAs (offset {offset})...")
    resp = requests.get(TIGERWEB_URL, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    features = data.get("features", [])
    if not features:
        break

    all_features.extend(features)
    offset += len(features)

    if len(features) < batch_size:
        break

print(f"Total ZCTAs in bounding box: {len(all_features)}")

# COMMAND ----------

# Convert GeoJSON features to Spark DataFrame
zcta_rows = []
for feat in all_features:
    props = feat["properties"]
    geom = feat["geometry"]
    zcta_rows.append({
        "zcta": str(props["GEOID"]),
        "area_land_sqm": int(props.get("AREALAND", 0)),
        "area_water_sqm": int(props.get("AREAWATER", 0)),
        "geometry_geojson": json.dumps(geom),
    })

zcta_schema = StructType([
    StructField("zcta", StringType()),
    StructField("area_land_sqm", LongType()),
    StructField("area_water_sqm", LongType()),
    StructField("geometry_geojson", StringType()),
])

zcta_df = spark.createDataFrame(zcta_rows, schema=zcta_schema)

# Convert GeoJSON to geometry and compute area
zcta_df = zcta_df.withColumn(
    "geometry", F.expr("ST_GeomFromGeoJSON(geometry_geojson)")
).drop("geometry_geojson").withColumn(
    "area_land_sqkm", F.col("area_land_sqm") / 1e6
)

print(f"ZCTA boundaries loaded: {zcta_df.count()}")

# COMMAND ----------

# Filter to ZCTAs whose centroid is within the state boundary
state_zctas = zcta_df.withColumn(
    "centroid", F.expr("ST_Centroid(geometry)")
).alias("z").join(
    state_geom.select(F.col("geometry").alias("state_geom")).alias("s"),
    F.expr("ST_Contains(s.state_geom, z.centroid)"),
    "inner"
).select("z.zcta", "z.area_land_sqm", "z.area_water_sqm", "z.geometry", "z.area_land_sqkm")

zcta_count = state_zctas.count()
print(f"ZCTAs (centroid within state): {zcta_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Fetch ACS demographics at ZCTA level

# COMMAND ----------

zcta_variables = {
    "B01003_001E": "total_population",
    "B19013_001E": "median_household_income",
    "B25077_001E": "median_home_value",
    "B01002_001E": "median_age",
    "B23025_002E": "in_labor_force",
    "B23025_005E": "unemployed",
    "B25001_001E": "total_housing_units",
    "B25003_002E": "owner_occupied",
    "B25003_003E": "renter_occupied",
    "B08301_001E": "total_commuters",
    "B08301_010E": "public_transit_commuters",
    "B15003_022E": "bachelors_degree",
    "B15003_023E": "masters_degree",
    "B15003_025E": "doctorate_degree",
}

var_string = ",".join(zcta_variables.keys())
url = f"https://api.census.gov/data/{acs_year}/acs/acs5?get=NAME,{var_string}&for=zip%20code%20tabulation%20area:*"
if census_api_key:
    url += f"&key={census_api_key}"

print("Fetching ACS ZCTA demographics...")
response = requests.get(url, timeout=120)
response.raise_for_status()
data = response.json()
headers = data[0]
rows = data[1:]
print(f"Total ZCTA records from ACS: {len(rows)}")

# COMMAND ----------

# Convert to Spark DataFrame
headers = [h.replace("zip code tabulation area", "zcta") for h in headers]
acs_df = spark.createDataFrame(rows, schema=headers)

for code, name in zcta_variables.items():
    if code in acs_df.columns:
        acs_df = acs_df.withColumnRenamed(code, name)

# Cast numeric columns — Census returns -666666666 for missing values
float_vars = {"median_age"}
geo_cols = {"NAME", "zcta"}
for col_name in acs_df.columns:
    if col_name not in geo_cols:
        cast_type = "double" if col_name in float_vars else "long"
        acs_df = acs_df.withColumn(col_name, F.col(col_name).cast(cast_type))
        acs_df = acs_df.withColumn(col_name, F.when(F.col(col_name) < 0, None).otherwise(F.col(col_name)))

# Filter to state ZCTAs using a join (avoids .collect())
acs_filtered = acs_df.join(state_zctas.select("zcta"), "zcta", "inner")
print(f"State ZCTA demographic records: {acs_filtered.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Combine boundaries + demographics

# COMMAND ----------

# Join demographics to boundaries
zcta_combined = state_zctas.join(acs_filtered.drop("NAME"), "zcta", "inner")

# Population density (people per sq km of land area)
zcta_combined = zcta_combined.withColumn(
    "population_density_sqkm",
    F.when(F.col("area_land_sqkm") > 0, F.col("total_population") / F.col("area_land_sqkm")).otherwise(0)
)

# Urbanicity classification at ZCTA level
zcta_combined = zcta_combined.withColumn(
    "urbanicity_category",
    F.when(F.col("population_density_sqkm") > 5000, "urban")
     .when(F.col("population_density_sqkm") > 500, "suburban")
     .otherwise("rural")
)

print(f"Combined ZCTA records: {zcta_combined.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to catalog

# COMMAND ----------

# Add geometry_wkt for debugging/export alongside native geometry
zcta_combined = zcta_combined.withColumn("geometry_wkt", F.expr("ST_AsText(geometry)"))

output_table = f"{catalog}.{schema}.silver_census_zcta"
zcta_combined.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

result = spark.table(output_table)
print(f"Written {result.count()} ZCTAs to {output_table}")
display(result.select("zcta", "total_population", "area_land_sqkm", "population_density_sqkm", "median_household_income").orderBy(F.desc("total_population")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        COUNT(*) as total_zctas,
        SUM(total_population) as total_pop,
        ROUND(AVG(population_density_sqkm), 1) as avg_pop_density,
        ROUND(AVG(median_household_income), 0) as avg_income,
        ROUND(SUM(area_land_sqkm), 1) as total_area_sqkm
    FROM {output_table}
"""))
