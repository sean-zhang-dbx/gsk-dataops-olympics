# Databricks notebook source
# MAGIC %md
# MAGIC # 🏅 Event 4: The Relay Challenge — Leg 3
# MAGIC
# MAGIC ## Person C: Model Training & MLflow Tracking
# MAGIC **Time Limit: 10 minutes**
# MAGIC
# MAGIC ### Your Mission
# MAGIC Train a classification model using the features from Person B, track everything with MLflow.
# MAGIC
# MAGIC ### Input
# MAGIC - Training data: `{TEAM_NAME}_relay_train`
# MAGIC - Test data: `{TEAM_NAME}_relay_test`
# MAGIC
# MAGIC ### Checkpoint (Baton Pass)
# MAGIC Your model must:
# MAGIC - ✅ Be logged in MLflow with parameters and metrics
# MAGIC - ✅ Achieve F1 score > 0.60 on the test set
# MAGIC - ✅ Have at least 2 experiment runs logged
# MAGIC - ✅ Pass the validation cell at the bottom
# MAGIC
# MAGIC > ⚠️ If checkpoint fails: **2-minute penalty** added to team time!
# MAGIC
# MAGIC > ⏱️ START YOUR TIMER NOW!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TEAM_NAME = "_____"  # e.g., "team_01" — MUST MATCH PREVIOUS LEGS!
TRAIN_TABLE = f"{TEAM_NAME}_relay_train"
TEST_TABLE = f"{TEAM_NAME}_relay_test"
EXPERIMENT_NAME = f"/dataops_olympics/{TEAM_NAME}_relay"

print(f"Training data: {TRAIN_TABLE}")
print(f"Test data:     {TEST_TABLE}")
print(f"Experiment:    {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Load Data

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, accuracy_score, classification_report
import mlflow
import mlflow.sklearn

# Load data
train_df = spark.table(TRAIN_TABLE).toPandas()
test_df = spark.table(TEST_TABLE).toPandas()

# Identify feature columns (numeric, non-target, non-metadata)
exclude_cols = ["target_above_median", "country", "year"]
feature_cols = [c for c in train_df.select_dtypes(include=[np.number]).columns if c not in exclude_cols]

X_train = train_df[feature_cols].fillna(0)
y_train = train_df["target_above_median"]
X_test = test_df[feature_cols].fillna(0)
y_test = test_df["target_above_median"]

print(f"Features: {feature_cols}")
print(f"Training samples: {len(X_train)}")
print(f"Test samples: {len(X_test)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Set Up MLflow Experiment

# COMMAND ----------

mlflow.set_experiment(EXPERIMENT_NAME)
print(f"✅ MLflow experiment set: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Train Model 1 (Baseline)
# MAGIC
# MAGIC **TODO:** Train a baseline model and log it with MLflow.

# COMMAND ----------

# Run 1: Baseline - Logistic Regression
with mlflow.start_run(run_name=f"{TEAM_NAME}_baseline"):
    model_1 = LogisticRegression(max_iter=1000, random_state=42)
    model_1.fit(X_train, y_train)
    
    y_pred_1 = model_1.predict(X_test)
    f1_1 = f1_score(y_test, y_pred_1)
    acc_1 = accuracy_score(y_test, y_pred_1)
    
    mlflow.log_params({"model_type": "LogisticRegression", "max_iter": 1000})
    mlflow.log_metric("f1_score", f1_1)
    mlflow.log_metric("accuracy", acc_1)
    mlflow.sklearn.log_model(model_1, "model")
    
    print(f"Baseline - F1: {f1_1:.4f}, Accuracy: {acc_1:.4f}")
    print(classification_report(y_test, y_pred_1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Train Model 2 (Improved)
# MAGIC
# MAGIC **TODO:** Train a better model. Try:
# MAGIC - RandomForestClassifier
# MAGIC - GradientBoostingClassifier
# MAGIC - Different hyperparameters

# COMMAND ----------

# Run 2: TODO - Train your improved model
with mlflow.start_run(run_name=f"{TEAM_NAME}_improved"):
    
    # TODO: Choose and configure your model
    model_2 = _____  # YOUR MODEL HERE
    # e.g., RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
    
    model_2.fit(X_train, y_train)
    
    y_pred_2 = model_2.predict(X_test)
    f1_2 = f1_score(y_test, y_pred_2)
    acc_2 = accuracy_score(y_test, y_pred_2)
    
    # TODO: Log your parameters
    mlflow.log_params({
        "model_type": "_____",
        # Add your hyperparameters here
    })
    mlflow.log_metric("f1_score", f1_2)
    mlflow.log_metric("accuracy", acc_2)
    mlflow.sklearn.log_model(model_2, "model")
    
    print(f"Improved - F1: {f1_2:.4f}, Accuracy: {acc_2:.4f}")
    print(classification_report(y_test, y_pred_2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: (Optional) Train Model 3
# MAGIC
# MAGIC If you have time, try a third approach!

# COMMAND ----------

# Optional Run 3: Your best attempt
# with mlflow.start_run(run_name=f"{TEAM_NAME}_best"):
#     model_3 = ...
#     ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6: Select Best Model

# COMMAND ----------

# Compare models
best_f1 = max(f1_1, f1_2)
best_model = model_1 if f1_1 >= f1_2 else model_2
best_name = "Baseline (LR)" if f1_1 >= f1_2 else "Improved"

print(f"🏆 Best model: {best_name}")
print(f"   F1 Score: {best_f1:.4f}")

# Save best model info for Person D
spark.sql(f"""
    CREATE OR REPLACE TABLE {TEAM_NAME}_relay_model_info AS
    SELECT 
        '{best_name}' as model_name,
        {best_f1} as f1_score,
        '{EXPERIMENT_NAME}' as experiment_name,
        current_timestamp() as trained_at
""")

print(f"✅ Model info saved to {TEAM_NAME}_relay_model_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ CHECKPOINT — Run This to Pass the Baton!

# COMMAND ----------

print("=" * 60)
print("LEG 3 CHECKPOINT VALIDATION")
print("=" * 60)

passed = True

# Check 1: MLflow experiment exists with runs
try:
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    if experiment:
        runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
        num_runs = len(runs)
        if num_runs >= 2:
            print(f"  ✅ MLflow experiment has {num_runs} runs")
        else:
            print(f"  ❌ Only {num_runs} MLflow runs (need at least 2)")
            passed = False
    else:
        print(f"  ❌ MLflow experiment not found!")
        passed = False
except Exception as e:
    print(f"  ❌ MLflow error: {e}")
    passed = False

# Check 2: F1 score threshold
if best_f1 > 0.60:
    print(f"  ✅ F1 Score: {best_f1:.4f} (above 0.60 threshold)")
else:
    print(f"  ❌ F1 Score: {best_f1:.4f} (below 0.60 threshold)")
    passed = False

# Check 3: Model logged in MLflow
try:
    if num_runs >= 1:
        latest_run = runs.iloc[0]
        if "sklearn" in str(runs.columns.tolist()):
            print(f"  ✅ Model artifacts logged")
        else:
            print(f"  ✅ MLflow runs logged with metrics")
except:
    print(f"  ⚠️  Could not verify model artifacts")

# Check 4: Model info table
try:
    info = spark.sql(f"SELECT * FROM {TEAM_NAME}_relay_model_info").collect()[0]
    print(f"  ✅ Model info table: {info.model_name} (F1={info.f1_score:.4f})")
except:
    print(f"  ❌ Model info table not found!")
    passed = False

print(f"\n{'='*60}")
if passed:
    print(f"  🎉 CHECKPOINT PASSED! Hand the baton to Person D!")
    print(f"  📌 Info for Person D:")
    print(f"      Best model F1: {best_f1:.4f}")
    print(f"      Experiment: {EXPERIMENT_NAME}")
    print(f"      Test table: {TEST_TABLE}")
else:
    print(f"  ❌ CHECKPOINT FAILED — 2 MINUTE PENALTY!")
    print(f"  Fix the issues above and re-run validation.")
print(f"{'='*60}")
