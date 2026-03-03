# Databricks notebook source
# MAGIC %md
# MAGIC # Practice 3: Machine Learning with MLflow
# MAGIC
# MAGIC **Time: ~10 minutes** | Fill in the blanks, run each cell, check your work at the end.
# MAGIC
# MAGIC You just saw the lightning talk — now try it yourself!
# MAGIC Fill in the `_____` blanks below. Use the **Databricks Assistant** (`Cmd+I`) if you get stuck.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What You'll Do
# MAGIC 1. Load the diabetes readmission dataset
# MAGIC 2. Split into train/test
# MAGIC 3. Train a RandomForest model with MLflow tracking
# MAGIC 4. Check your F1 score
# MAGIC 5. Run the validation check

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Run this cell first (no changes needed)

# COMMAND ----------

spark.sql("USE dataops_olympics")

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, classification_report
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature

df = spark.table("diabetes_readmission").toPandas()
print(f"Loaded {df.shape[0]} patients × {df.shape[1]} features")
print(f"Readmission rate: {df['readmission_risk'].mean():.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Define Features and Split
# MAGIC
# MAGIC Fill in the target column name. The column we want to predict is `readmission_risk`.
# MAGIC
# MAGIC **Hint:** Look at the column names printed above.

# COMMAND ----------

feature_cols = ["pregnancies", "glucose", "blood_pressure", "skin_thickness",
                "insulin", "bmi", "diabetes_pedigree", "age"]

X = df[feature_cols]

# FILL IN: Replace _____ with the target column name
y = df["_____"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"Train: {len(X_train)} | Test: {len(X_test)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Train a Model with MLflow
# MAGIC
# MAGIC Fill in the model constructor. We'll use a RandomForestClassifier.
# MAGIC
# MAGIC **Hint:** `RandomForestClassifier(n_estimators=100, random_state=42)`

# COMMAND ----------

_user = spark.sql("SELECT current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{_user}/practice_ml")

with mlflow.start_run(run_name="practice_random_forest"):
    # FILL IN: Replace _____ with RandomForestClassifier(n_estimators=100, random_state=42)
    model = _____

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    f1 = f1_score(y_test, y_pred)

    sig = infer_signature(X_train, model.predict(X_train))
    mlflow.log_param("model", "RandomForest")
    mlflow.log_param("n_estimators", 100)
    mlflow.log_metric("f1_score", f1)
    mlflow.sklearn.log_model(model, "model", signature=sig, input_example=X_test[:3])

    print(f"F1 Score: {f1:.4f}")
    print(f"\n{classification_report(y_test, y_pred)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Feature Importance
# MAGIC
# MAGIC Fill in the correct attribute to get feature importances from the trained model.
# MAGIC
# MAGIC **Hint:** RandomForest models have a `.feature_importances_` attribute.

# COMMAND ----------

import plotly.express as px

# FILL IN: Replace _____ with model.feature_importances_
importances = _____

fig = px.bar(x=feature_cols, y=importances,
             title="Which Features Predict Readmission?",
             labels={"x": "Feature", "y": "Importance"},
             template="plotly_white")
fig.update_layout(xaxis_tickangle=-45)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Register the Model
# MAGIC
# MAGIC Fill in the function name to register the model.
# MAGIC
# MAGIC **Hint:** Use `mlflow.register_model(model_uri, model_name)`

# COMMAND ----------

run_id = mlflow.search_runs(
    experiment_names=[f"/Users/{_user}/practice_ml"],
    order_by=["metrics.f1_score DESC"],
    max_results=1
).iloc[0].run_id

model_uri = f"runs:/{run_id}/model"

# Auto-detect model name (UC requires 3-level names like catalog.schema.model)
_model_name = "practice_readmission_model"
try:
    catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
    uc_catalog = next((c for c in catalogs if "sandbox" in c.lower()), None)
    if uc_catalog:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_catalog}.dataops_olympics")
        _model_name = f"{uc_catalog}.dataops_olympics.practice_readmission_model"
except:
    pass

# FILL IN: Replace _____ with the correct MLflow function to register
try:
    result = mlflow._____(model_uri, _model_name)
    print(f"Registered: {result.name} v{result.version}")
except Exception as e:
    print(f"Note: {str(e)[:100]}")
    print("Model is tracked in MLflow — registration is optional.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — Run this to check your work!

# COMMAND ----------

print("=" * 55)
print("  PRACTICE 3 — VALIDATION")
print("=" * 55)

score = 0

# Check 1: Data loaded
try:
    assert len(df) > 700
    print(f"  [PASS] Data loaded: {len(df)} rows")
    score += 1
except:
    print("  [FAIL] Data not loaded correctly")

# Check 2: Model trained
try:
    assert hasattr(model, 'predict')
    preds = model.predict(X_test[:5])
    print(f"  [PASS] Model trained and can predict")
    score += 1
except:
    print("  [FAIL] Model not trained")

# Check 3: F1 score reasonable
try:
    assert f1 > 0.3, f"F1 too low: {f1:.4f}"
    print(f"  [PASS] F1 score: {f1:.4f}")
    score += 1
except:
    print(f"  [FAIL] F1 score issue: {f1:.4f}" if 'f1' in dir() else "  [FAIL] F1 not calculated")

# Check 4: MLflow experiment exists
try:
    runs = mlflow.search_runs(experiment_names=[f"/Users/{_user}/practice_ml"])
    assert len(runs) > 0
    print(f"  [PASS] MLflow experiment has {len(runs)} run(s)")
    score += 1
except:
    print("  [FAIL] MLflow experiment not found")

print(f"\n  Score: {score}/4")
if score == 4:
    print("\n  ALL PASSED! You're ready for the competition!")
elif score >= 2:
    print("\n  Good progress! Fix the remaining items and re-run.")
else:
    print("\n  Ask the Databricks Assistant for help!")
print("=" * 55)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answers (for reference — try without peeking!)
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answers</summary>
# MAGIC
# MAGIC - **Exercise 1:** `readmission_risk`
# MAGIC - **Exercise 2:** `RandomForestClassifier(n_estimators=100, random_state=42)`
# MAGIC - **Exercise 3:** `model.feature_importances_`
# MAGIC - **Exercise 4:** `register_model`
# MAGIC
# MAGIC </details>
