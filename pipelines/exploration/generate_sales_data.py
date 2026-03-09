# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Synthetic Sales Data
# MAGIC
# MAGIC Creates 1 year of monthly sales data (~1,500 stores) with regression patterns tied to real features.
# MAGIC Sales are driven by store format, urbanicity, and location-based market quality proxies,
# MAGIC with monthly seasonality applied. Output is written to `gold_store_sales`.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

stores_df = spark.table(f"{catalog}.{schema}.bronze_store_locations")
print(f"Store count: {stores_df.count()}")
stores = stores_df.toPandas()

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import date

np.random.seed(42)

# Base annual sales by format
base_sales = {"express": 800_000, "standard": 2_500_000, "flagship": 5_000_000}

# Urbanicity bonus
urbanicity_bonus = {"urban": 200_000, "suburban": 100_000, "rural": -50_000}

# Monthly seasonality factors
seasonality = [0.85, 0.82, 0.95, 1.0, 1.05, 1.08, 1.10, 1.08, 1.02, 1.0, 1.05, 1.15]

records = []
for _, store in stores.iterrows():
    fmt = store['format']
    urb = store.get('urbanicity', 'suburban')

    # Base sales
    annual = base_sales.get(fmt, 2_500_000)
    annual += urbanicity_bonus.get(urb, 0)

    # Location-based variation (use lat/lng as proxy for market quality)
    # Higher population areas (closer to MSA center) get bonus
    lat_factor = np.random.normal(1.0, 0.15)
    annual *= max(0.5, lat_factor)

    # Add noise
    annual *= np.random.uniform(0.85, 1.15)
    annual = max(annual, base_sales.get(fmt, 800_000) * 0.3)

    # Generate 12 months of data
    for month_idx in range(12):
        month_date = date(2024, month_idx + 1, 1)
        monthly = (annual / 12) * seasonality[month_idx] * np.random.uniform(0.95, 1.05)

        records.append({
            'store_number': int(store['store_number']),
            'format': fmt,
            'month': month_date,
            'monthly_sales': round(monthly, 2),
            'annual_sales': round(annual, 2),
            'urbanicity': urb,
            'lat': float(store['lat']),
            'lng': float(store['lng']),
            'city': store.get('city', ''),
            'state': store.get('state', ''),
        })

sales_pdf = pd.DataFrame(records)
print(f"Generated {len(sales_pdf)} monthly records for {len(stores)} stores")
print(f"\nSales by format:")
print(sales_pdf.groupby('format')['annual_sales'].mean().round(0))

# COMMAND ----------

sales_df = spark.createDataFrame(sales_pdf)
sales_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_store_sales")
print(f"Written {sales_df.count()} records to {catalog}.{schema}.gold_store_sales")

# COMMAND ----------

display(spark.sql(f"""
    SELECT format, urbanicity,
           COUNT(DISTINCT store_number) as stores,
           ROUND(AVG(monthly_sales), 0) as avg_monthly,
           ROUND(AVG(annual_sales), 0) as avg_annual
    FROM {catalog}.{schema}.gold_store_sales
    GROUP BY format, urbanicity
    ORDER BY format, urbanicity
"""))

# COMMAND ----------

print("Sales Distribution Verification:")
display(spark.sql(f"""
    SELECT format,
           ROUND(MIN(annual_sales), 0) as min_sales,
           ROUND(PERCENTILE(annual_sales, 0.25), 0) as p25,
           ROUND(PERCENTILE(annual_sales, 0.50), 0) as median,
           ROUND(PERCENTILE(annual_sales, 0.75), 0) as p75,
           ROUND(MAX(annual_sales), 0) as max_sales
    FROM {catalog}.{schema}.gold_store_sales
    WHERE month = '2024-01-01'
    GROUP BY format
    ORDER BY format
"""))