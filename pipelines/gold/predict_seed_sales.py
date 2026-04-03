# Databricks notebook source
# MAGIC %md
# MAGIC # Score Seed Points — Predict Revenue at 3 Format Levels (Spark-Native)
# MAGIC
# MAGIC For each seed point:
# MAGIC 1. Load trade area features from `gold_seed_trade_area_features`
# MAGIC 2. Compute market capacity (same formula as store sales generation)
# MAGIC 3. Explode each seed into 3 rows (express / standard / flagship)
# MAGIC 4. Score all rows at once using `mlflow.pyfunc.spark_udf` for distributed inference
# MAGIC 5. Recommend format that maximizes $/sqft (efficiency)
# MAGIC
# MAGIC **Output:** `gold_expansion_candidates`

# COMMAND ----------

# MAGIC %pip install xgboost

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

import mlflow
import mlflow.xgboost
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load trained model and get feature columns

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
model_name = f"{catalog}.{schema}.site_selection_revenue_model"

# Dynamically get the latest model version (no hardcoded version)
client = MlflowClient()
versions = client.search_model_versions(f"name='{model_name}'")
latest_version = max(int(v.version) for v in versions)
model_uri = f"models:/{model_name}/{latest_version}"
print(f"Using model: {model_uri}")

# Load the underlying XGBoost model directly (avoids MLflow pyfunc schema enforcement issues)
model = mlflow.xgboost.load_model(model_uri)

# Get feature columns in the TRAINING order from the model itself
feature_cols = model.get_booster().feature_names
print(f"Feature columns ({len(feature_cols)}): {feature_cols}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Model ready for scoring
# MAGIC
# MAGIC Using direct XGBoost predict — only ~500 seeds × 3 formats = 1,500 rows.
# MAGIC Loaded via `mlflow.xgboost.load_model()` to bypass pyfunc schema enforcement.

# COMMAND ----------

print(f"Model loaded. Will score using direct XGBoost predict (~1500 rows).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load seed trade area features and compute market capacity

# COMMAND ----------

seeds_df = spark.table(f"{catalog}.{schema}.gold_seed_trade_area_features")
print(f"Seed points with trade area features: {seeds_df.count()}")

# COMMAND ----------

# Compute market capacity in pure Spark
# Formula: pop * 18 * income_mult * poi_mult
# income_mult = 0.7 + 0.6 * (income / avg_income)
# poi_mult    = 0.8 + 0.4 * (pois / avg_pois)

avg_income = seeds_df.agg(F.avg("median_household_income")).collect()[0][0]
avg_pois = seeds_df.agg(F.avg("total_poi_count")).collect()[0][0]
avg_pois = max(avg_pois, 1.0)  # avoid division by zero

seeds_df = seeds_df.withColumn(
    "pop_safe", F.coalesce(F.col("total_population"), F.lit(0)).cast("double")
).withColumn(
    "inc_safe", F.coalesce(F.col("median_household_income"), F.lit(50000)).cast("double")
).withColumn(
    "pois_safe", F.coalesce(F.col("total_poi_count"), F.lit(0)).cast("double")
).withColumn(
    "income_mult", F.lit(0.7) + F.lit(0.6) * (F.col("inc_safe") / F.lit(avg_income))
).withColumn(
    "poi_mult", F.lit(0.8) + F.lit(0.4) * (F.col("pois_safe") / F.lit(avg_pois))
).withColumn(
    "market_capacity_sqft",
    F.greatest(
        F.col("pop_safe") * F.lit(18.0) * F.col("income_mult") * F.col("poi_mult"),
        F.lit(10000.0),
    ),
).drop("pop_safe", "inc_safe", "pois_safe", "income_mult", "poi_mult")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Explode each seed into 3 format rows and score

# COMMAND ----------

# Explode each seed into 3 rows: one per format (express, standard, flagship)
exploded_df = seeds_df.select(
    "*",
    F.explode(
        F.array(
            F.struct(F.lit("express").alias("format_name"), F.lit(14000).cast("double").alias("store_sqft")),
            F.struct(F.lit("standard").alias("format_name"), F.lit(32000).cast("double").alias("store_sqft")),
            F.struct(F.lit("flagship").alias("format_name"), F.lit(52000).cast("double").alias("store_sqft")),
        )
    ).alias("fmt"),
).select(
    "*",
    F.col("fmt.format_name").alias("format_name"),
    F.col("fmt.store_sqft").alias("store_sqft"),
).drop("fmt")

# Compute sqft_market_ratio for each format row
exploded_df = exploded_df.withColumn(
    "sqft_market_ratio", F.col("store_sqft") / F.col("market_capacity_sqft")
)

# COMMAND ----------

# Convert to pandas for scoring (~1500 rows — perfectly fine)
keep_cols = list(dict.fromkeys([
    "location_id", "format_name", "store_sqft", "sqft_market_ratio", "market_capacity_sqft",
    "lat", "lng", "urbanicity_category", "total_population", "median_household_income",
    "total_poi_count", "total_competitor_count", "higher_education_rate",
    "urbanicity_score", "distance_to_nearest_store_miles",
] + feature_cols))

pdf = exploded_df.select(*[c for c in keep_cols if c in exploded_df.columns]).toPandas()

pdf[feature_cols] = pdf[feature_cols].fillna(0).astype("float64")

# Score with raw XGBoost model (no schema enforcement)
pdf["predicted_revenue"] = model.predict(pdf[feature_cols]).clip(min=0)
pdf["rev_per_sqft"] = pdf["predicted_revenue"] / pdf["store_sqft"]

# Convert back to Spark
scored_df = spark.createDataFrame(pdf)

print(f"Scoring complete: {len(pdf)} rows scored")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Recommend best format per location (highest rev/sqft)

# COMMAND ----------

# Window to rank formats by rev_per_sqft within each location
w = Window.partitionBy("location_id").orderBy(F.col("rev_per_sqft").desc())

best_df = scored_df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).select(
    "location_id",
    F.col("format_name").alias("recommended_format"),
    F.round(F.col("predicted_revenue"), 0).alias("recommended_revenue"),
    F.round(F.col("rev_per_sqft"), 2).alias("recommended_rev_per_sqft"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Pivot format predictions to wide columns and join recommendation

# COMMAND ----------

# Pivot: one row per location with columns for each format
pivot_df = (
    scored_df
    .groupBy("location_id")
    .pivot("format_name", ["express", "standard", "flagship"])
    .agg(
        F.round(F.first("store_sqft"), 0).alias("sqft"),
        F.round(F.first("predicted_revenue"), 0).alias("revenue"),
        F.round(F.first("rev_per_sqft"), 2).alias("rev_per_sqft"),
        F.round(F.first("sqft_market_ratio"), 4).alias("sqft_ratio"),
    )
)

# Rename pivoted columns from e.g. "express_sqft" (already done by pivot)

# COMMAND ----------

# Join wide predictions + recommendation + context columns from the original seeds
context_cols = [
    "location_id", "lat", "lng", "urbanicity_category",
    F.round(F.col("market_capacity_sqft"), 0).alias("market_capacity_sqft"),
    F.col("total_population").cast("int").alias("total_population"),
    F.col("median_household_income").cast("double").alias("median_household_income"),
    F.col("total_poi_count").cast("int").alias("total_poi_count"),
    F.col("total_competitor_count").cast("int").alias("total_competitor_count"),
    F.col("higher_education_rate").cast("double").alias("higher_education_rate"),
    F.col("urbanicity_score").cast("double").alias("urbanicity_score"),
    F.col("distance_to_nearest_store_miles").cast("double").alias("distance_to_nearest_store_miles"),
]

context_df = seeds_df.select(*context_cols)

result_df = (
    context_df
    .join(pivot_df, on="location_id", how="inner")
    .join(best_df, on="location_id", how="inner")
    .withColumn("scoring_timestamp", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write to gold

# COMMAND ----------

output_table = f"{catalog}.{schema}.gold_expansion_candidates"
result_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

count = spark.table(output_table).count()
print(f"Written {count} expansion candidates to {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        urbanicity_category,
        recommended_format,
        COUNT(*) as candidates,
        ROUND(AVG(recommended_revenue)/1e6, 2) as avg_rev_M,
        ROUND(AVG(recommended_rev_per_sqft), 0) as avg_rev_sqft,
        ROUND(AVG(total_population), 0) as avg_pop,
        ROUND(AVG(median_household_income), 0) as avg_income,
        ROUND(AVG(market_capacity_sqft), 0) as avg_mkt_cap
    FROM {output_table}
    GROUP BY urbanicity_category, recommended_format
    ORDER BY urbanicity_category, recommended_format
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Format recommendation distribution

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        recommended_format,
        COUNT(*) as count,
        ROUND(AVG(express_rev_per_sqft), 0) as avg_express_sqft,
        ROUND(AVG(standard_rev_per_sqft), 0) as avg_standard_sqft,
        ROUND(AVG(flagship_rev_per_sqft), 0) as avg_flagship_sqft,
        ROUND(AVG(market_capacity_sqft), 0) as avg_mkt_cap
    FROM {output_table}
    GROUP BY recommended_format
    ORDER BY recommended_format
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 20 candidates by recommended revenue

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        location_id, urbanicity_category, recommended_format,
        ROUND(recommended_revenue/1e6, 2) as rev_M,
        ROUND(recommended_rev_per_sqft, 0) as rev_sqft,
        total_population as pop,
        ROUND(median_household_income, 0) as income,
        total_poi_count as pois,
        total_competitor_count as comps,
        ROUND(distance_to_nearest_store_miles, 1) as dist_store
    FROM {output_table}
    ORDER BY recommended_revenue DESC
    LIMIT 20
"""))
