# Databricks notebook source
# MAGIC %md
# MAGIC # Practice 3: Machine Learning with MLflow
# MAGIC
# MAGIC **Time: ~10 minutes** | Use the Databricks Assistant to generate all your code from the business requirements.
# MAGIC
# MAGIC You just saw the lightning talk — now try it yourself!
# MAGIC Each exercise describes **what** needs to happen. Use `Cmd+I` to prompt the Assistant.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What You'll Do
# MAGIC 1. Load the diabetes readmission dataset and define features/target
# MAGIC 2. Train a RandomForest model with MLflow tracking
# MAGIC 3. Visualize feature importance
# MAGIC 4. Register the model
# MAGIC 5. Run the validation check

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Run this cell first (no changes needed)

# COMMAND ----------

spark.sql("USE CATALOG dataops_olympics")
spark.sql("USE SCHEMA default")

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, classification_report
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature

df = spark.table("diabetes_readmission").toPandas()
print(f"Loaded {df.shape[0]} patients x {df.shape[1]} features")
print(f"Readmission rate: {df['readmission_risk'].mean():.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Define Features and Split
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > From the `diabetes_readmission` dataset, define the feature columns
# MAGIC > (pregnancies, glucose, blood_pressure, skin_thickness, insulin, bmi,
# MAGIC > diabetes_pedigree, age) and the target column (`readmission_risk`).
# MAGIC >
# MAGIC > Split the data into 80% training and 20% test sets using
# MAGIC > `random_state=42` and stratify by the target column.
# MAGIC > Print the sizes of each split.

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Train a Model with MLflow
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Train a `RandomForestClassifier` with 100 trees and `random_state=42`.
# MAGIC > Use MLflow to track the experiment:
# MAGIC > - Set the experiment path to `/Users/{your_email}/practice_ml`
# MAGIC > - Log the model type and hyperparameters
# MAGIC > - Log the F1 score metric
# MAGIC > - Log the model with a signature (use `infer_signature`)
# MAGIC >
# MAGIC > Print the F1 score and classification report.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Feature Importance
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create a Plotly bar chart showing which features are most important
# MAGIC > for predicting readmission risk. Use the trained model's
# MAGIC > `feature_importances_` attribute. Label axes clearly.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Register the Model
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Register your trained model in the MLflow Model Registry.
# MAGIC > Find the best run from the experiment and register it.
# MAGIC > Use `mlflow.register_model(model_uri, model_name)`.
# MAGIC >
# MAGIC > Note: Unity Catalog requires 3-level model names (catalog.schema.model).
# MAGIC > If registration fails due to permissions, that's OK — the model is still tracked in MLflow.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — Run this to check your work!

# COMMAND ----------

print("=" * 55)
print("  PRACTICE 3 — VALIDATION")
print("=" * 55)

score = 0

try:
    assert len(df) > 700
    print(f"  [PASS] Data loaded: {len(df)} rows")
    score += 1
except Exception:
    print("  [FAIL] Data not loaded correctly")

try:
    assert hasattr(model, 'predict')
    preds = model.predict(X_test[:5])
    print(f"  [PASS] Model trained and can predict")
    score += 1
except Exception:
    print("  [FAIL] Model not trained (variable 'model' not found)")

try:
    assert f1 > 0.3, f"F1 too low: {f1:.4f}"
    print(f"  [PASS] F1 score: {f1:.4f}")
    score += 1
except Exception:
    print(f"  [FAIL] F1 score issue")

try:
    _user = spark.sql("SELECT current_user()").collect()[0][0]
    runs = mlflow.search_runs(experiment_names=[f"/Users/{_user}/practice_ml"])
    assert len(runs) > 0
    print(f"  [PASS] MLflow experiment has {len(runs)} run(s)")
    score += 1
except Exception:
    print("  [FAIL] MLflow experiment not found")

print(f"\n  Score: {score}/4")
if score == 4:
    print("\n  ALL PASSED! You're ready for the competition!")
elif score >= 2:
    print("\n  Good progress! Fix the remaining items and re-run.")
else:
    print("\n  Ask the Databricks Assistant for help!")
print("=" * 55)
