# Databricks notebook source
# MAGIC %md
# MAGIC # Event 3: Self-Check
# MAGIC
# MAGIC Run this notebook to verify your Event 3 artifacts before submission.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_submit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking Event 3 Artifacts

# COMMAND ----------

print("=" * 60)
print(f"  EVENT 3 SELF-CHECK — {TEAM_NAME}")
print("=" * 60)
print()

# --- Results Table ---
try:
    results = spark.table(f"{CATALOG}.default.event3_results")
    results_count = results.count()
    if results_count > 0:
        print(f"  [PASS] event3_results exists — {results_count} row(s)")
        row = results.orderBy("submitted_at", ascending=False).first()
        print(f"         Model type:    {row['model_type']}")
        print(f"         F1 Score:      {row['f1_score']:.4f}")
        print(f"         ROC AUC:       {row['roc_auc']:.4f}")
        print(f"         Accuracy:      {row['accuracy']:.4f}")
        print(f"         Features:      {row['n_features']}")
        print(f"         MLflow Run ID: {row['mlflow_run_id']}")
    else:
        print(f"  [FAIL] event3_results exists but is empty — run Step 6 in the starter notebook")
except Exception as e:
    print(f"  [FAIL] event3_results not found — complete the starter notebook first: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Experiment Check

# COMMAND ----------

import mlflow

try:
    # Search for experiments matching the team name
    experiments = [e for e in mlflow.search_experiments() if TEAM_NAME in e.name]
    if experiments:
        exp = experiments[0]
        print(f"  [PASS] MLflow experiment found: {exp.name}")
        runs = mlflow.search_runs(experiment_ids=[exp.experiment_id])
        run_count = len(runs)
        if run_count > 0:
            print(f"         Total runs: {run_count}")
            if "metrics.f1_score" in runs.columns:
                best_f1 = runs["metrics.f1_score"].max()
                print(f"         Best F1 score: {best_f1:.4f}")
            else:
                print(f"  [WARN] No f1_score metric found in runs — make sure to log f1_score")
        else:
            print(f"  [FAIL] Experiment exists but has no runs")
    else:
        print(f"  [WARN] No MLflow experiment found containing '{TEAM_NAME}'")
        print(f"         Expected: /Users/<email>/{TEAM_NAME}_heart_ml")
except Exception as e:
    print(f"  [SKIP] Could not check MLflow experiments: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Registry Check

# COMMAND ----------

try:
    from mlflow import MlflowClient
    client = MlflowClient()
    model_name = f"{CATALOG}.default.heart_disease_model"
    model = client.get_registered_model(model_name)
    versions = client.search_model_versions(f"name='{model_name}'")
    print(f"  [PASS] Registered model found: {model_name}")
    print(f"         Versions: {len(list(versions))}")
except Exception as e:
    print(f"  [    ] Registered model not found at {CATALOG}.default.heart_disease_model")
    print(f"         This is OK if UC registration failed due to permissions — the organizer can verify via MLflow")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Table Check

# COMMAND ----------

try:
    ft = spark.table(f"{CATALOG}.default.heart_features")
    ft_count = ft.count()
    ft_cols = ft.columns
    print(f"  [PASS] heart_features exists — {ft_count} rows, {len(ft_cols)} columns")
    print(f"         Columns: {', '.join(ft_cols)}")
except Exception:
    print(f"  [    ] heart_features not found (optional — needed for Feature Store path)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Table Checks

# COMMAND ----------

for table_name, desc in [
    ("heart_shap_importance", "Bonus 1: SHAP Explainability"),
    ("heart_cv_results", "Bonus 3: Cross-Validation Report"),
]:
    try:
        cnt = spark.table(f"{CATALOG}.default.{table_name}").count()
        print(f"  [PASS] {desc} — {table_name} exists ({cnt} rows)")
    except Exception:
        print(f"  [    ] {desc} — {table_name} not found (optional)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Submission Status

# COMMAND ----------

check_submission("event3")
