# Databricks notebook source
# MAGIC %md
# MAGIC # Simulate Competitor Growth (2026-2028)
# MAGIC
# MAGIC Projects competitor expansion using one Spark SQL query per year:
# MAGIC 1. Compute per-brand growth rate and urbanicity preference
# MAGIC 2. Cross join brands × candidate H3 cells, weighted by attractiveness × urbanicity
# MAGIC 3. Exclude existing same-brand k-ring zones via ANTI JOIN
# MAGIC 4. Pick top-N per brand via ROW_NUMBER — no Python for-loops over data
# MAGIC
# MAGIC **Output:** `gold_simulated_competitor_growth`

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Brand profiles

# COMMAND ----------

brand_profiles = spark.sql(f"""
    WITH rates AS (
        SELECT brand, ROUND(COUNT(*) / 3.0, 1) AS avg_annual_rate
        FROM {catalog}.{schema}.bronze_competitor_locations
        WHERE open_year >= 2023
        GROUP BY brand
    ),
    urbanicity_dist AS (
        SELECT brand, urbanicity,
            COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY brand) AS pct
        FROM {catalog}.{schema}.bronze_competitor_locations
        GROUP BY brand, urbanicity
    ),
    pivoted AS (
        SELECT brand,
            COALESCE(MAX(CASE WHEN urbanicity = 'urban' THEN pct END), 0) AS urban_pct,
            COALESCE(MAX(CASE WHEN urbanicity = 'suburban' THEN pct END), 0) AS suburban_pct,
            COALESCE(MAX(CASE WHEN urbanicity = 'rural' THEN pct END), 0) AS rural_pct
        FROM urbanicity_dist
        GROUP BY brand
    )
    SELECT r.brand, CAST(CEIL(r.avg_annual_rate) AS INT) AS n_per_year,
           p.urban_pct, p.suburban_pct, p.rural_pct
    FROM rates r
    JOIN pivoted p ON r.brand = p.brand
""")

brand_profiles.createOrReplaceTempView("brand_profiles")
display(brand_profiles)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Candidate cells and exclusion zones

# COMMAND ----------

H3_RES = 8
EXCLUSION_K = 5  # ~3 miles at res-8

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW h3_candidates AS
    SELECT h3_cell_id, center_lat, center_lon, urbanicity_category,
        total_population, population_density, total_poi_count,
        GREATEST(0.01, COALESCE(population_density, 0) / 1000.0
            + COALESCE(total_poi_count, 0) / 100.0) AS attractiveness
    FROM {catalog}.{schema}.silver_h3_features
    WHERE total_population > 0
""")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW existing_exclusions AS
    SELECT brand,
        EXPLODE(h3_kring(
            h3_pointash3string(CONCAT('POINT(', lng, ' ', lat, ')'), {H3_RES}),
            {EXCLUSION_K}
        )) AS excluded_h3
    FROM {catalog}.{schema}.bronze_competitor_locations
""")

print(f"Candidates: {spark.sql('SELECT COUNT(*) FROM h3_candidates').collect()[0][0]:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Simulate — one query per year

# COMMAND ----------

# Build SQL CASE expressions from brand profiles
profiles = brand_profiles.collect()

brand_weight_cases = " ".join(
    f"WHEN '{r['brand']}' THEN CASE c.urbanicity_category "
    f"WHEN 'urban' THEN {r['urban_pct']} "
    f"WHEN 'suburban' THEN {r['suburban_pct']} "
    f"WHEN 'rural' THEN {r['rural_pct']} ELSE 0.01 END"
    for r in profiles
)
brand_weight_sql = f"CASE b.brand {brand_weight_cases} ELSE 0.01 END"

brand_n_cases = " ".join(f"WHEN '{r['brand']}' THEN {r['n_per_year']}" for r in profiles)
brand_n_sql = f"CASE brand {brand_n_cases} ELSE 1 END"

year_dfs = []

for year in [2026, 2027, 2028]:
    # Build exclusion union: existing + all prior year placements
    exclusion_parts = ["SELECT brand, excluded_h3 FROM existing_exclusions"]
    for prev_year in range(2026, year):
        exclusion_parts.append(
            f"SELECT brand, EXPLODE(h3_kring(h3_cell_id, {EXCLUSION_K})) AS excluded_h3 FROM _placed_{prev_year}"
        )
    exclusion_sql = " UNION ALL ".join(exclusion_parts)

    placed_df = spark.sql(f"""
        WITH all_exclusions AS ({exclusion_sql}),
        scored AS (
            SELECT b.brand, c.*,
                c.attractiveness * ({brand_weight_sql}) AS weighted_score
            FROM brand_profiles b
            CROSS JOIN h3_candidates c
            WHERE NOT EXISTS (
                SELECT 1 FROM all_exclusions e
                WHERE e.brand = b.brand AND e.excluded_h3 = c.h3_cell_id
            )
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY brand ORDER BY weighted_score * RAND({year * 31337}) DESC
                ) AS rn,
                {brand_n_sql} AS max_n
            FROM scored
        )
        SELECT brand, {year} AS simulated_year,
            center_lat AS latitude, center_lon AS longitude,
            h3_cell_id, urbanicity_category,
            total_population AS population_in_cell
        FROM ranked
        WHERE rn <= max_n
    """)

    placed_df.createOrReplaceTempView(f"_placed_{year}")
    year_dfs.append(placed_df)

    count = placed_df.count()
    print(f"  {year}: placed {count} locations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to gold

# COMMAND ----------

sim_df = reduce(lambda a, b: a.union(b), year_dfs)
sim_df = sim_df.withColumn("simulation_timestamp", F.current_timestamp())

output_table = f"{catalog}.{schema}.gold_simulated_competitor_growth"
sim_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

count = spark.table(output_table).count()
print(f"Written {count} simulated locations to {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

display(spark.sql(f"""
    SELECT brand, simulated_year,
        COUNT(*) AS new_openings,
        SUM(COUNT(*)) OVER (PARTITION BY brand ORDER BY simulated_year) AS cumulative
    FROM {output_table}
    GROUP BY brand, simulated_year
    ORDER BY brand, simulated_year
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT simulated_year, urbanicity_category,
        COUNT(*) AS openings,
        ROUND(AVG(population_in_cell), 0) AS avg_cell_pop
    FROM {output_table}
    GROUP BY simulated_year, urbanicity_category
    ORDER BY simulated_year, urbanicity_category
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cumulative competitor landscape

# COMMAND ----------

display(spark.sql(f"""
    SELECT e.brand, e.current_count,
        s.by_2026, s.by_2027, s.by_2028,
        e.current_count + s.by_2028 AS total_2028
    FROM (
        SELECT brand, COUNT(*) AS current_count
        FROM {catalog}.{schema}.bronze_competitor_locations
        GROUP BY brand
    ) e
    JOIN (
        SELECT brand,
            SUM(CASE WHEN simulated_year <= 2026 THEN 1 ELSE 0 END) AS by_2026,
            SUM(CASE WHEN simulated_year <= 2027 THEN 1 ELSE 0 END) AS by_2027,
            COUNT(*) AS by_2028
        FROM {output_table}
        GROUP BY brand
    ) s ON e.brand = s.brand
    ORDER BY e.brand
"""))
