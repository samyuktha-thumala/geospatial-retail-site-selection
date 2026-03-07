# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Store Sales — Revenue per Sqft Model (Spark-Native)
# MAGIC
# MAGIC Enriches `gold_store_trade_area_features` with sales data using a **revenue per square foot** model
# MAGIC with **format-market fit** interaction. Fully Spark-native — no `.collect()`, no Python loops, no pandas.
# MAGIC
# MAGIC 1. Assign store square footage by format (deterministic via `xxhash64`)
# MAGIC 2. Compute market capacity from demographics/POIs
# MAGIC 3. Compute $/sqft from features + sqft-to-market ratio
# MAGIC 4. Calculate annual revenue = sqft x $/sqft
# MAGIC 5. Generate 12 months of sales with seasonality
# MAGIC
# MAGIC **Output:** `gold_store_features_and_sales` — one row per store with ALL features + sales

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load trade area features

# COMMAND ----------

ta = spark.table(f"{catalog}.{schema}.gold_store_trade_area_features")
print(f"Loaded trade area features: {ta.count()} stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configuration constants

# COMMAND ----------

# Baseline $/sqft (industry average ~$550-650/yr for grocery)
BASELINE_REV_PER_SQFT = 600.0

# Seasonality multipliers (month 1-12)
SEASONALITY = {
    1: 0.85, 2: 0.82, 3: 0.90, 4: 0.95, 5: 1.00, 6: 1.05,
    7: 1.08, 8: 1.06, 9: 0.98, 10: 1.02, 11: 1.15, 12: 1.20,
}

# Scoring weights
W_POP = 0.22
W_INC = 0.22
W_POI = 0.12
W_EDU = 0.10
W_WF_PROX = 0.08
W_FM_PROX = 0.04
W_COMP = 0.12
W_ALDI = 0.04
W_TGT = 0.04
W_UNEMP = 0.06

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Assign store sqft using deterministic hash-based randomness

# COMMAND ----------

# Use xxhash64(location_id, salt) to generate a deterministic pseudo-random number in [0, 1)
# Then scale to the appropriate sqft range per format.
#
# hash_frac = (xxhash64(location_id, lit("sqft_seed")) % 10000) / 10000.0
# store_sqft = sqft_low + hash_frac * (sqft_high - sqft_low)  -- cast to int

hash_frac_sqft = (
    F.abs(F.xxhash64(F.col("location_id"), F.lit("sqft_seed"))) % 10000
) / 10000.0

ta_with_sqft = ta.withColumn(
    "store_sqft",
    F.when(F.lower(F.coalesce(F.col("format"), F.lit("standard"))) == "flagship",
           F.lit(45000) + hash_frac_sqft * F.lit(15000))
     .when(F.lower(F.coalesce(F.col("format"), F.lit("standard"))) == "express",
           F.lit(10000) + hash_frac_sqft * F.lit(8000))
     .otherwise(
           F.lit(25000) + hash_frac_sqft * F.lit(15000))
     .cast("int")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Compute market capacity

# COMMAND ----------

# Market capacity = population * 18 * income_mult * poi_mult
# income_mult = 0.7 + 0.6 * (income / avg_income)
# poi_mult = 0.8 + 0.4 * (poi_count / avg_poi)
# Floor at 10,000

# Compute dataset-level averages for income and POI (used in capacity formula)
w_all = Window.partitionBy(F.lit(1))

ta_with_stats = ta_with_sqft.withColumn(
    "_avg_income", F.avg("median_household_income").over(w_all)
).withColumn(
    "_avg_poi", F.avg("total_poi_count").over(w_all)
)

safe_pop = F.coalesce(F.col("total_population"), F.lit(0)).cast("double")
safe_inc = F.coalesce(F.col("median_household_income"), F.lit(50000)).cast("double")
safe_poi = F.coalesce(F.col("total_poi_count"), F.lit(0)).cast("double")
safe_avg_inc = F.coalesce(F.col("_avg_income"), F.lit(90000)).cast("double")
safe_avg_poi = F.coalesce(F.col("_avg_poi"), F.lit(700)).cast("double")

income_mult = F.lit(0.7) + F.lit(0.6) * (safe_inc / safe_avg_inc)
poi_mult = F.lit(0.8) + F.lit(0.4) * (safe_poi / safe_avg_poi)

raw_capacity = safe_pop * F.lit(18.0) * income_mult * poi_mult

ta_with_capacity = ta_with_stats.withColumn(
    "market_capacity_sqft", F.greatest(raw_capacity, F.lit(10000.0)).cast("int")
).withColumn(
    "sqft_market_ratio",
    F.round(F.col("store_sqft").cast("double") / F.col("market_capacity_sqft").cast("double"), 4)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Normalize features to [0, 1] using window-based min/max

# COMMAND ----------

# Helper: min-max normalize a column using dataset-wide min/max via window functions.
# Returns a column expression yielding values in [0, 1], defaulting to 0.5 if null or range is 0.
def norm_col(col_name):
    c = F.col(col_name).cast("double")
    mn = F.min(col_name).over(w_all)
    mx = F.max(col_name).over(w_all)
    rng = mx - mn
    return F.when(
        c.isNull(), F.lit(0.5)
    ).otherwise(
        F.when(rng == F.lit(0.0), F.lit(0.5))
         .otherwise(F.greatest(F.lit(0.0), F.least(F.lit(1.0), (c - mn) / rng)))
    )

ta_normed = (
    ta_with_capacity
    .withColumn("_pop_n", norm_col("total_population"))
    .withColumn("_inc_n", norm_col("median_household_income"))
    .withColumn("_poi_n", norm_col("total_poi_count"))
    .withColumn("_comp_n", norm_col("total_competitor_count"))
    .withColumn("_edu_n", norm_col("higher_education_rate"))
    .withColumn("_unemp_n", norm_col("unemployment_rate"))
    # Proximity: closer = higher, so invert
    .withColumn("_wf_prox", F.lit(1.0) - norm_col("distance_to_whole_foods_miles"))
    .withColumn("_fm_prox", F.lit(1.0) - norm_col("distance_to_fresh_market_miles"))
    # Distance to discount competitors: farther = better
    .withColumn("_aldi_dist_n", norm_col("distance_to_aldi_miles"))
    .withColumn("_tgt_dist_n", norm_col("distance_to_target_miles"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Compute location quality score and revenue per sqft

# COMMAND ----------

# location_quality = weighted sum of normalized features
location_quality = (
    F.lit(W_POP) * F.col("_pop_n")
    + F.lit(W_INC) * F.col("_inc_n")
    + F.lit(W_POI) * F.col("_poi_n")
    + F.lit(W_EDU) * F.col("_edu_n")
    + F.lit(W_WF_PROX) * F.col("_wf_prox")
    + F.lit(W_FM_PROX) * F.col("_fm_prox")
    - F.lit(W_COMP) * F.col("_comp_n")
    - F.lit(W_ALDI) * (F.lit(1.0) - F.col("_aldi_dist_n"))
    - F.lit(W_TGT) * (F.lit(1.0) - F.col("_tgt_dist_n"))
    - F.lit(W_UNEMP) * F.col("_unemp_n")
)

# Location modifier: maps ~0-1 score to -0.25..+0.25
loc_modifier = (location_quality - F.lit(0.35)) * F.lit(0.7)

# Format-market fit modifier: penalize oversized stores in small markets
# fit_modifier = -0.3 * (sqft_ratio - 0.8), clamped to [-0.25, 0.15]
fit_modifier = F.greatest(
    F.lit(-0.25),
    F.least(
        F.lit(0.15),
        F.lit(-0.3) * (F.col("sqft_market_ratio") - F.lit(0.8))
    )
)

# Raw rev_per_sqft before noise
raw_rev_per_sqft = F.lit(BASELINE_REV_PER_SQFT) * (F.lit(1.0) + loc_modifier + fit_modifier)
clamped_rev = F.greatest(F.lit(300.0), F.least(F.lit(1000.0), raw_rev_per_sqft))

# Deterministic noise (~+-5%) using hash. Map hash to a factor in [0.90, 1.10].
# This approximates random.gauss(1.0, 0.05) but deterministically.
hash_noise = (F.abs(F.xxhash64(F.col("location_id"), F.lit("noise_seed"))) % 10000) / 10000.0
# Map [0,1) uniform to approximately [-2.5sigma, +2.5sigma] via simple linear mapping
# noise_factor in [0.90, 1.10] centered at 1.0
noise_factor = F.lit(0.90) + hash_noise * F.lit(0.20)

ta_scored = ta_normed.withColumn(
    "revenue_per_sqft", F.round(clamped_rev * noise_factor, 2)
).withColumn(
    "annual_revenue", (F.col("store_sqft").cast("double") * F.col("revenue_per_sqft")).cast("long")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Generate 12 months of sales with seasonality

# COMMAND ----------

# Monthly sales = (annual_revenue / 12) * seasonality[month] * monthly_noise
# Monthly noise uses a different hash salt per month for determinism.

base_monthly = F.col("annual_revenue").cast("double") / F.lit(12.0)

for month_num, season_mult in SEASONALITY.items():
    month_name = [
        "jan", "feb", "mar", "apr", "may", "jun",
        "jul", "aug", "sep", "oct", "nov", "dec"
    ][month_num - 1]

    # Deterministic monthly noise in [0.94, 1.06] (~+-3%)
    month_hash = (
        F.abs(F.xxhash64(F.col("location_id"), F.lit(f"month_{month_num}_seed"))) % 10000
    ) / 10000.0
    month_noise = F.lit(0.94) + month_hash * F.lit(0.12)

    ta_scored = ta_scored.withColumn(
        f"{month_name}_sales",
        F.round(base_monthly * F.lit(season_mult) * month_noise).cast("long")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Clean up temp columns and write output

# COMMAND ----------

# Drop all temporary columns (prefixed with underscore)
temp_cols = [c for c in ta_scored.columns if c.startswith("_")]
result_df = ta_scored.drop(*temp_cols)

output_table = f"{catalog}.{schema}.gold_store_features_and_sales"
result_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

final = spark.table(output_table)
print(f"Written {final.count()} stores to {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        COUNT(*) as total_stores,
        ROUND(AVG(store_sqft), 0) as avg_sqft,
        ROUND(AVG(revenue_per_sqft), 2) as avg_rev_per_sqft,
        ROUND(AVG(annual_revenue), 0) as avg_annual_rev,
        ROUND(AVG(market_capacity_sqft), 0) as avg_market_cap,
        ROUND(AVG(sqft_market_ratio), 3) as avg_sqft_ratio
    FROM {output_table}
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        urbanicity_category,
        format,
        COUNT(*) as stores,
        ROUND(AVG(store_sqft), 0) as avg_sqft,
        ROUND(AVG(market_capacity_sqft), 0) as avg_mkt_cap,
        ROUND(AVG(sqft_market_ratio), 3) as avg_ratio,
        ROUND(AVG(revenue_per_sqft), 0) as avg_rev_sqft,
        ROUND(AVG(annual_revenue)/1e6, 2) as avg_annual_M
    FROM {output_table}
    GROUP BY urbanicity_category, format
    ORDER BY urbanicity_category, format
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Format-market fit validation
# MAGIC Express stores should have HIGHER $/sqft than flagships in weak markets,
# MAGIC while flagships should do well in strong markets.

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        format,
        CASE
            WHEN total_population > 60000 AND median_household_income > 90000 THEN 'strong_market'
            WHEN total_population < 30000 OR median_household_income < 70000 THEN 'weak_market'
            ELSE 'mid_market'
        END as market_strength,
        COUNT(*) as stores,
        ROUND(AVG(store_sqft), 0) as avg_sqft,
        ROUND(AVG(revenue_per_sqft), 0) as avg_rev_sqft,
        ROUND(AVG(sqft_market_ratio), 3) as avg_ratio,
        ROUND(AVG(annual_revenue)/1e6, 2) as avg_annual_M
    FROM {output_table}
    GROUP BY format,
        CASE
            WHEN total_population > 60000 AND median_household_income > 90000 THEN 'strong_market'
            WHEN total_population < 30000 OR median_household_income < 70000 THEN 'weak_market'
            ELSE 'mid_market'
        END
    ORDER BY format, market_strength
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature correlations with $/sqft

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        ROUND(CORR(median_household_income, revenue_per_sqft), 3) as corr_income,
        ROUND(CORR(total_population, revenue_per_sqft), 3) as corr_population,
        ROUND(CORR(total_poi_count, revenue_per_sqft), 3) as corr_pois,
        ROUND(CORR(higher_education_rate, revenue_per_sqft), 3) as corr_education,
        ROUND(CORR(total_competitor_count, revenue_per_sqft), 3) as corr_competitors,
        ROUND(CORR(unemployment_rate, revenue_per_sqft), 3) as corr_unemployment,
        ROUND(CORR(store_sqft, revenue_per_sqft), 3) as corr_sqft,
        ROUND(CORR(sqft_market_ratio, revenue_per_sqft), 3) as corr_sqft_ratio
    FROM {output_table}
"""))
