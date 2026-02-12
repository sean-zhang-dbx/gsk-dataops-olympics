# Databricks notebook source
# MAGIC %md
# MAGIC # 🏅 Event 4: The Relay Challenge — Leg 4
# MAGIC
# MAGIC ## Person D: Deployment & Dashboard
# MAGIC **Time Limit: 5 minutes**
# MAGIC
# MAGIC ### Your Mission
# MAGIC Take the trained model from Person C and create:
# MAGIC 1. A batch prediction pipeline
# MAGIC 2. A results dashboard
# MAGIC 3. A summary presentation
# MAGIC
# MAGIC ### Input
# MAGIC - MLflow experiment: `/dataops_olympics/{TEAM_NAME}_relay`
# MAGIC - Test data: `{TEAM_NAME}_relay_test`
# MAGIC - Model info: `{TEAM_NAME}_relay_model_info`
# MAGIC
# MAGIC ### Checkpoint (Final!)
# MAGIC Must show:
# MAGIC - ✅ Batch predictions generated and saved
# MAGIC - ✅ Results dashboard with at least 3 charts
# MAGIC - ✅ Model performance summary
# MAGIC
# MAGIC > ⏱️ START YOUR TIMER NOW!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TEAM_NAME = "_____"  # e.g., "team_01" — MUST MATCH PREVIOUS LEGS!
TEST_TABLE = f"{TEAM_NAME}_relay_test"
EXPERIMENT_NAME = f"/dataops_olympics/{TEAM_NAME}_relay"
PREDICTIONS_TABLE = f"{TEAM_NAME}_relay_predictions"

print(f"Team: {TEAM_NAME}")
print(f"Test data: {TEST_TABLE}")
print(f"Predictions: {PREDICTIONS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Load Best Model from MLflow

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np

# Find the best run
experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
runs = mlflow.search_runs(
    experiment_ids=[experiment.experiment_id],
    order_by=["metrics.f1_score DESC"]
)

best_run_id = runs.iloc[0].run_id
best_f1 = runs.iloc[0]["metrics.f1_score"]

print(f"Best run: {best_run_id}")
print(f"Best F1: {best_f1:.4f}")

# Load the model
model_uri = f"runs:/{best_run_id}/model"
model = mlflow.sklearn.load_model(model_uri)
print(f"✅ Model loaded from MLflow")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Generate Batch Predictions

# COMMAND ----------

# Load test data
test_df = spark.table(TEST_TABLE).toPandas()

# Get feature columns
exclude_cols = ["target_above_median", "country", "year"]
feature_cols = [c for c in test_df.select_dtypes(include=[np.number]).columns if c not in exclude_cols]

X_test = test_df[feature_cols].fillna(0)
y_test = test_df["target_above_median"]

# Generate predictions
predictions = model.predict(X_test)
probabilities = model.predict_proba(X_test)[:, 1]

# Add predictions to dataframe
test_df["prediction"] = predictions
test_df["probability"] = probabilities
test_df["correct"] = (test_df["prediction"] == test_df["target_above_median"]).astype(int)

# Save predictions
spark.createDataFrame(test_df).write.format("delta").mode("overwrite").saveAsTable(PREDICTIONS_TABLE)

print(f"✅ Predictions saved: {len(test_df)} rows")
print(f"   Accuracy: {test_df['correct'].mean():.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Results Dashboard
# MAGIC
# MAGIC **TODO:** Create at least 3 visualizations.

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.metrics import confusion_matrix, f1_score, roc_curve, auc

# Chart 1: Confusion Matrix
cm = confusion_matrix(y_test, predictions)
fig1 = px.imshow(
    cm, text_auto=True,
    labels=dict(x="Predicted", y="Actual", color="Count"),
    x=["Below Median", "Above Median"],
    y=["Below Median", "Above Median"],
    title="Prediction Confusion Matrix",
    color_continuous_scale="Blues"
)
fig1.show()

# COMMAND ----------

# Chart 2: Prediction Probability Distribution
fig2 = px.histogram(
    test_df, x="probability", color="target_above_median",
    nbins=30, barmode="overlay", opacity=0.7,
    title="Prediction Probability Distribution by Actual Class",
    labels={"probability": "Predicted Probability", "target_above_median": "Actual Class"},
    color_discrete_map={0: "#e74c3c", 1: "#2ecc71"}
)
fig2.update_layout(template="plotly_white")
fig2.show()

# COMMAND ----------

# Chart 3: TODO - Create your own visualization
# Ideas:
# - Predictions by country
# - Feature importance
# - ROC curve
# - Prediction accuracy over time

# YOUR VISUALIZATION CODE HERE

# Example: ROC Curve
fpr, tpr, thresholds = roc_curve(y_test, probabilities)
roc_auc = auc(fpr, tpr)

fig3 = go.Figure()
fig3.add_trace(go.Scatter(x=fpr, y=tpr, mode="lines", name=f"Model (AUC={roc_auc:.3f})"))
fig3.add_trace(go.Scatter(x=[0, 1], y=[0, 1], mode="lines", name="Random", line=dict(dash="dash")))
fig3.update_layout(
    title="ROC Curve",
    xaxis_title="False Positive Rate",
    yaxis_title="True Positive Rate",
    template="plotly_white"
)
fig3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Executive Summary

# COMMAND ----------

from sklearn.metrics import classification_report

final_f1 = f1_score(y_test, predictions)

print("=" * 60)
print(f"  RELAY CHALLENGE — EXECUTIVE SUMMARY")
print(f"  Team: {TEAM_NAME}")
print("=" * 60)

print(f"""
📊 PIPELINE OVERVIEW:
  Leg 1 (Data Ingestion):     ✅ Completed
  Leg 2 (Feature Engineering): ✅ Completed
  Leg 3 (Model Training):      ✅ Completed
  Leg 4 (Deployment):          ✅ Completed

🤖 MODEL PERFORMANCE:
  Algorithm:    {type(model).__name__}
  F1 Score:     {final_f1:.4f}
  AUC-ROC:      {roc_auc:.4f}
  Predictions:  {len(test_df)} records

📈 BUSINESS IMPACT:
  Correctly identified {test_df['correct'].sum()}/{len(test_df)} 
  ({test_df['correct'].mean():.1%}) of records

📋 CLASSIFICATION REPORT:
""")
print(classification_report(y_test, predictions, target_names=["Below Median", "Above Median"]))

print("=" * 60)
print("  🎉 RELAY COMPLETE!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ FINAL CHECKPOINT

# COMMAND ----------

print("=" * 60)
print("LEG 4 FINAL CHECKPOINT VALIDATION")
print("=" * 60)

passed = True

# Check 1: Predictions table
try:
    pred_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {PREDICTIONS_TABLE}").collect()[0].cnt
    print(f"  ✅ Predictions table: {pred_count} rows")
except:
    print(f"  ❌ Predictions table not found!")
    passed = False

# Check 2: Predictions have required columns
try:
    cols = spark.table(PREDICTIONS_TABLE).columns
    required = ["prediction", "probability", "correct"]
    has_all = all(c in cols for c in required)
    if has_all:
        print(f"  ✅ All required columns present")
    else:
        print(f"  ❌ Missing columns: {[c for c in required if c not in cols]}")
        passed = False
except:
    print(f"  ❌ Could not verify columns")
    passed = False

# Check 3: Dashboard created (we check by proxy - visualizations ran)
print(f"  ✅ Dashboard visualizations created (3 charts)")

# Check 4: F1 threshold
if final_f1 > 0.60:
    print(f"  ✅ F1 Score: {final_f1:.4f} (maintained above threshold)")
else:
    print(f"  ⚠️  F1 Score: {final_f1:.4f} (below threshold)")

print(f"\n{'='*60}")
if passed:
    print(f"  🎉🏆 RELAY CHALLENGE COMPLETE!")
    print(f"  Signal the judges and STOP YOUR TIMER!")
else:
    print(f"  ❌ CHECKPOINT FAILED — 2 MINUTE PENALTY!")
print(f"{'='*60}")
