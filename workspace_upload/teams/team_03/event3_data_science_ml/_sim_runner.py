# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.default.heart_silver_correct AS SELECT * FROM {SHARED_CATALOG}.default.heart_disease")
# COMMAND ----------
import mlflow, mlflow.sklearn
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, accuracy_score, roc_auc_score

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(EXPERIMENT_PATH)
# COMMAND ----------
df = spark.table(f"{CATALOG}.default.heart_silver_correct").toPandas()
FEATURE_COLS = ["age", "sex", "cp", "trestbps", "chol", "fbs", "restecg", "thalach", "exang", "oldpeak", "slope", "ca", "thal"]
X = df[FEATURE_COLS]
y = df["target"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
# COMMAND ----------
with mlflow.start_run(run_name=f"{TEAM_NAME}_baseline"):
    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("n_features", len(FEATURE_COLS))
    model = LogisticRegression(max_iter=1000, random_state=42)
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    f1 = f1_score(y_test, preds)
    auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
    acc = accuracy_score(y_test, preds)
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("accuracy", acc)
    mlflow.log_metric("roc_auc", auc)
    mlflow.sklearn.log_model(model, "model")
    print(f"Baseline LR: F1={f1:.4f}")
# COMMAND ----------
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.event3_results AS
    SELECT '{TEAM_NAME}' AS team, 'LogisticRegression' AS model_type,
        {f1} AS f1_score, {auc} AS roc_auc, {acc} AS accuracy,
        {len(FEATURE_COLS)} AS n_features, '' AS mlflow_run_id, current_timestamp() AS submitted_at""")
# COMMAND ----------
submit("event3", {"model": "LogisticRegression", "f1": round(f1, 4)})
print(f"team_03 Event 3 complete (baseline only — LR, no features table, F1={f1:.4f})")
