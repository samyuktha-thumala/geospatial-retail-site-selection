# Databricks notebook source
# MAGIC %md
# MAGIC # Census Demographics Cleaning — Silver Layer
# MAGIC
# MAGIC Cleans bronze census demographics and produces enriched silver table.
# MAGIC
# MAGIC **Cleaning steps:**
# MAGIC 1. Flag Census top-coded ($250,001) and bottom-coded ($2,499) income values
# MAGIC 2. Replace remaining nulls with 0 where appropriate (count fields)
# MAGIC 3. Derive rates: unemployment_rate, transit_share, owner_occupied_rate, education_index
# MAGIC
# MAGIC **Input:** `bronze_census_demographics`
# MAGIC **Output:** `silver_census_demographics`

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("state_fips", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
state_fips = dbutils.widgets.get("state_fips")

assert catalog and schema, "catalog and schema must be provided"

input_table = f"{catalog}.{schema}.bronze_census_demographics"
output_table = f"{catalog}.{schema}.silver_census_demographics"

# COMMAND ----------

from pyspark.sql import functions as F

raw = spark.table(input_table)
print(f"Input rows: {raw.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Clean and Flag Census-Coded Values

# COMMAND ----------

cleaned = raw

# Replace Census sentinel values (-666666666) with NULL across all numeric columns
geo_cols = {"NAME", "state", "county", "tract", "block_group", "geoid",
            "geography_level", "acs_year", "ingestion_id", "ingestion_timestamp"}
for col_name in cleaned.columns:
    if col_name not in geo_cols:
        cleaned = cleaned.withColumn(
            col_name,
            F.when(F.col(col_name) < 0, None).otherwise(F.col(col_name))
        )

# Derive geoid from component columns if not already present
if "geoid" not in cleaned.columns and all(c in cleaned.columns for c in ["state", "county", "tract", "block_group"]):
    cleaned = cleaned.withColumn(
        "geoid",
        F.concat(F.col("state"), F.col("county"), F.col("tract"), F.col("block_group"))
    )
    print(f"Derived geoid from state+county+tract+block_group (e.g., {cleaned.select('geoid').first()[0]})")

# Flag top-coded and bottom-coded income
cleaned = (
    cleaned
    .withColumn("income_top_coded", F.col("median_household_income") >= 250001)
    .withColumn("income_bottom_coded", F.col("median_household_income") <= 2499)
)

# Cap top-coded income at 250000
cleaned = cleaned.withColumn(
    "median_household_income",
    F.when(F.col("median_household_income") >= 250001, F.lit(250000.0))
     .otherwise(F.col("median_household_income"))
)

# For count fields, replace NULL with 0
count_columns = [
    "bachelors_degree", "masters_degree", "doctorate_degree",
    "in_labor_force", "unemployed", "unemployment_count",
    "total_housing_units", "owner_occupied", "renter_occupied",
    "total_commuters", "public_transit_commuters",
]
# Only apply to columns that exist in the table
existing_count_cols = [c for c in count_columns if c in cleaned.columns]
for col_name in existing_count_cols:
    cleaned = cleaned.withColumn(
        col_name,
        F.coalesce(F.col(col_name), F.lit(0.0))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Derive Rates and Indices

# COMMAND ----------

# Normalize column names: handle both "unemployed" and "unemployment_count"
cols = cleaned.columns
if "unemployment_count" in cols and "unemployed" not in cols:
    cleaned = cleaned.withColumnRenamed("unemployment_count", "unemployed")

# Derive rates — only when source columns exist
if "unemployed" in cleaned.columns and "in_labor_force" in cleaned.columns:
    cleaned = cleaned.withColumn(
        "unemployment_rate",
        F.when(F.col("in_labor_force") > 0,
               F.round(F.col("unemployed") / F.col("in_labor_force"), 4))
         .otherwise(F.lit(None))
    )

if "public_transit_commuters" in cleaned.columns and "total_commuters" in cleaned.columns:
    cleaned = cleaned.withColumn(
        "transit_share",
        F.when(F.col("total_commuters") > 0,
               F.round(F.col("public_transit_commuters") / F.col("total_commuters"), 4))
         .otherwise(F.lit(None))
    )

if "owner_occupied" in cleaned.columns and "total_housing_units" in cleaned.columns:
    cleaned = cleaned.withColumn(
        "owner_occupied_rate",
        F.when(F.col("total_housing_units") > 0,
               F.round(F.col("owner_occupied") / F.col("total_housing_units"), 4))
         .otherwise(F.lit(None))
    )

if "bachelors_degree" in cleaned.columns and "masters_degree" in cleaned.columns and "doctorate_degree" in cleaned.columns:
    cleaned = cleaned.withColumn(
        "higher_education_rate",
        F.when(F.col("total_population") > 0,
               F.round(
                   (F.col("bachelors_degree") + F.col("masters_degree") + F.col("doctorate_degree"))
                   / F.col("total_population"), 4
               ))
         .otherwise(F.lit(None))
    )

if "in_labor_force" in cleaned.columns:
    cleaned = cleaned.withColumn(
        "labor_force_participation_rate",
        F.when(F.col("total_population") > 0,
               F.round(F.col("in_labor_force") / F.col("total_population"), 4))
         .otherwise(F.lit(None))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write Silver Table

# COMMAND ----------

# Select available columns dynamically (some may not exist depending on census config)
desired_cols = [
    "geoid", "NAME",
    "total_population", "median_household_income",
    "income_top_coded", "income_bottom_coded",
    "median_age", "median_home_value",
    "bachelors_degree", "masters_degree", "doctorate_degree", "higher_education_rate",
    "in_labor_force", "unemployed", "unemployment_rate", "labor_force_participation_rate",
    "total_housing_units", "owner_occupied", "renter_occupied", "owner_occupied_rate",
    "total_commuters", "public_transit_commuters", "transit_share",
]
# Keep only columns that exist
available_cols = [c for c in desired_cols if c in cleaned.columns]
# Also keep any additional demographic columns from config
extra_cols = [c for c in cleaned.columns if c not in available_cols and c not in ["state", "county", "tract", "block_group", "geography_level", "acs_year", "ingestion_id", "ingestion_timestamp"]]
final_cols = available_cols + [c for c in extra_cols if c not in available_cols]

silver = cleaned.select(*final_cols)

# Join native geometry from bronze block groups
bg_table = f"{catalog}.{schema}.bronze_census_blockgroups"
bg_geom = spark.table(bg_table).select(
    F.col("geoid").alias("bg_geoid"),
    F.col("geometry_wkt"),
    F.expr("ST_SetSRID(ST_GeomFromWKT(geometry_wkt), 4326)").alias("geometry")
).filter(F.col("geometry_wkt").isNotNull())

silver = silver.join(bg_geom, silver["geoid"] == bg_geom["bg_geoid"], "left").drop("bg_geoid")

silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

result = spark.table(output_table)
print(f"Silver rows: {result.count()}")
print(f"Columns ({len(result.columns)}): {result.columns}")

# Dynamic validation — only check columns that exist
result_cols = result.columns
val_exprs = ["count(*) AS total"]
if "income_top_coded" in result_cols:
    val_exprs.append("count(CASE WHEN income_top_coded THEN 1 END) AS top_coded")
    val_exprs.append("count(CASE WHEN income_bottom_coded THEN 1 END) AS bottom_coded")
for col in ["unemployment_rate", "transit_share", "higher_education_rate", "owner_occupied_rate"]:
    if col in result_cols:
        val_exprs.append(f"count(CASE WHEN {col} IS NULL THEN 1 END) AS null_{col}")
        val_exprs.append(f"round(avg({col}), 4) AS avg_{col}")

display(spark.sql(f"SELECT {', '.join(val_exprs)} FROM {output_table}"))
display(result.orderBy(F.desc("total_population")).limit(20))
