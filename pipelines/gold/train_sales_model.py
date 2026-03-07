# Databricks notebook source
# MAGIC %md
# MAGIC # Train Sales Prediction Model
# MAGIC
# MAGIC Trains an XGBoost regressor on store trade area features to predict `annual_revenue`.
# MAGIC Key feature: `store_sqft` and `sqft_market_ratio` capture format-market fit.
# MAGIC
# MAGIC 1. Load `gold_store_features_and_sales`
# MAGIC 2. Prepare feature matrix
# MAGIC 3. Train XGBoost with cross-validation
# MAGIC 4. Log to MLflow, register best model in Unity Catalog
# MAGIC
# MAGIC **Output:** Registered model in Unity Catalog

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
from xgboost import XGBRegressor
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np
import json
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load and prepare features

# COMMAND ----------

df = spark.table(f"{catalog}.{schema}.gold_store_features_and_sales")
print(f"Training samples: {df.count()}")

# COMMAND ----------

# Feature columns for the model
FEATURE_COLS = [
    # Demographics (drop median_home_value/median_age/transit_share/owner_occ/pop_density — correlated with income or urbanicity)
    "total_population",
    "median_household_income",
    "higher_education_rate",
    "unemployment_rate",
    # POI breakdown (drop total_poi_count — sum of these; drop leisure/transport — correlated with amenity/urbanicity)
    "poi_count_shop",
    "poi_count_amenity",
    "poi_count_office",
    # Competition — discount vs premium signal (drop target — correlated with aldi; drop fresh_market — barely in NY)
    "total_competitor_count",
    "distance_to_nearest_store_miles",
    "distance_to_aldi_miles",
    "distance_to_whole_foods_miles",
    "distance_to_trader_joes_miles",
    # Location (drop urbanicity_score/area_sqkm/h3_cell_count)
    "transit_share",
    # Format-market fit (KEY features)
    "store_sqft",
    "sqft_market_ratio",
]

TARGET_COL = "annual_revenue"

# Filter to only columns that exist
available_cols = df.columns
feature_cols = [c for c in FEATURE_COLS if c in available_cols]
print(f"Using {len(feature_cols)} features: {feature_cols}")

# COMMAND ----------

# Convert to pandas for sklearn/xgboost
pdf = df.select(*feature_cols, TARGET_COL).toPandas()

# Fill nulls with 0
pdf = pdf.fillna(0)

X = pdf[feature_cols]
y = pdf[TARGET_COL]

print(f"Feature matrix: {X.shape}")
print(f"Target range: ${y.min():,.0f} - ${y.max():,.0f} (mean ${y.mean():,.0f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Train/test split

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

print(f"Train: {len(X_train)}, Test: {len(X_test)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Train XGBoost with MLflow tracking

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
import os
user_email = spark.sql("SELECT current_user()").collect()[0][0]
experiment_name = f"/Users/{user_email}/site-selection-sales-model"
mlflow.set_experiment(experiment_name)

# COMMAND ----------

with mlflow.start_run(run_name="xgboost_revenue_prediction") as run:
    # Hyperparameters
    params = {
        "n_estimators": 200,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 3,
        "reg_alpha": 0.1,
        "reg_lambda": 1.0,
        "random_state": 42,
    }

    model = XGBRegressor(**params)
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    # Predictions
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    # Metrics
    train_r2 = r2_score(y_train, y_pred_train)
    test_r2 = r2_score(y_test, y_pred_test)
    test_mae = mean_absolute_error(y_test, y_pred_test)
    test_rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))
    test_mape = np.mean(np.abs((y_test - y_pred_test) / y_test)) * 100

    # Cross-validation
    cv_scores = cross_val_score(
        XGBRegressor(**params), X, y, cv=5, scoring="r2"
    )

    # Log parameters
    mlflow.log_params(params)
    mlflow.log_param("n_features", len(feature_cols))
    mlflow.log_param("n_samples", len(X))
    mlflow.log_param("feature_columns", json.dumps(feature_cols))

    # Log metrics
    mlflow.log_metric("train_r2", train_r2)
    mlflow.log_metric("test_r2", test_r2)
    mlflow.log_metric("test_mae", test_mae)
    mlflow.log_metric("test_rmse", test_rmse)
    mlflow.log_metric("test_mape", test_mape)
    mlflow.log_metric("cv_r2_mean", cv_scores.mean())
    mlflow.log_metric("cv_r2_std", cv_scores.std())

    # Feature importance
    importance = dict(zip(feature_cols, model.feature_importances_.tolist()))
    importance_sorted = dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))
    mlflow.log_dict(importance_sorted, "feature_importance.json")

    # Log model
    mlflow.xgboost.log_model(
        model,
        artifact_path="model",
        input_example=X_test.iloc[:1],
    )

    print(f"Train R²: {train_r2:.4f}")
    print(f"Test R²:  {test_r2:.4f}")
    print(f"Test MAE: ${test_mae:,.0f}")
    print(f"Test RMSE: ${test_rmse:,.0f}")
    print(f"Test MAPE: {test_mape:.1f}%")
    print(f"CV R² (5-fold): {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
    print(f"\nTop 10 features:")
    for feat, imp in list(importance_sorted.items())[:10]:
        print(f"  {feat:40s} {imp:.4f}")

    run_id = run.info.run_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Register model in Unity Catalog

# COMMAND ----------

model_name = f"{catalog}.{schema}.site_selection_revenue_model"

model_uri = f"runs:/{run_id}/model"
mv = mlflow.register_model(model_uri, model_name)

print(f"Registered model: {model_name}")
print(f"Version: {mv.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Save model artifacts to gold table

# COMMAND ----------

artifacts_data = [{
    "model_name": model_name,
    "model_version": int(mv.version),
    "run_id": run_id,
    "train_r2": float(train_r2),
    "test_r2": float(test_r2),
    "test_mae": float(test_mae),
    "test_rmse": float(test_rmse),
    "test_mape": float(test_mape),
    "cv_r2_mean": float(cv_scores.mean()),
    "cv_r2_std": float(cv_scores.std()),
    "n_features": len(feature_cols),
    "n_samples": len(X),
    "feature_importance_json": json.dumps(importance_sorted),
}]

artifacts_df = spark.createDataFrame(artifacts_data)
artifacts_df = artifacts_df.withColumn("trained_at", F.current_timestamp())

artifacts_table = f"{catalog}.{schema}.gold_model_artifacts"
artifacts_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(artifacts_table)

print(f"Model artifacts saved to {artifacts_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation: Predict on training data to verify

# COMMAND ----------

display(spark.sql(f"""
    SELECT model_name, model_version,
           ROUND(train_r2, 4) as train_r2,
           ROUND(test_r2, 4) as test_r2,
           ROUND(test_mae, 0) as test_mae,
           ROUND(test_mape, 1) as test_mape_pct,
           ROUND(cv_r2_mean, 4) as cv_r2,
           n_features, n_samples, trained_at
    FROM {artifacts_table}
"""))
