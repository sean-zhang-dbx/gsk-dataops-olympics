# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
# Step 0: Ensure heart_silver_correct exists
spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.default.heart_silver_correct AS SELECT * FROM {SHARED_CATALOG}.default.heart_disease")
# COMMAND ----------
import mlflow, mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, accuracy_score, roc_auc_score
import pandas as pd
# COMMAND ----------
mlflow.set_tracking_uri("databricks")
exp_name = f"/Users/odl_instructor_2119401@databrickslabs.com/{TEAM_NAME}_heart_ml"
mlflow.set_experiment(exp_name)
# COMMAND ----------
df = spark.table(f"{CATALOG}.default.heart_silver_correct").toPandas()
X = df.drop("target", axis=1)
y = df["target"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
# COMMAND ----------
# Run 1: RandomForest
with mlflow.start_run(run_name=f"{TEAM_NAME}_rf"):
    mlflow.log_param("model_type", "RandomForest")
    mlflow.log_param("n_features", X.shape[1])
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    f1 = f1_score(y_test, preds)
    acc = accuracy_score(y_test, preds)
    auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("accuracy", acc)
    mlflow.log_metric("roc_auc", auc)
    mlflow.sklearn.log_model(model, "model")
    print(f"RF: F1={f1:.4f}, Acc={acc:.4f}, AUC={auc:.4f}")
# COMMAND ----------
# Run 2: another model
from sklearn.linear_model import LogisticRegression
with mlflow.start_run(run_name=f"{TEAM_NAME}_lr"):
    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("n_features", X.shape[1])
    model2 = LogisticRegression(max_iter=1000, random_state=42)
    model2.fit(X_train, y_train)
    preds2 = model2.predict(X_test)
    f1_2 = f1_score(y_test, preds2)
    mlflow.log_metric("f1_score", f1_2)
    mlflow.log_metric("accuracy", accuracy_score(y_test, preds2))
    mlflow.sklearn.log_model(model2, "model")
    print(f"LR: F1={f1_2:.4f}")
# COMMAND ----------
# Save results
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.event3_results AS
    SELECT '{TEAM_NAME}' AS team, 'RandomForest' AS model_type,
        {f1} AS f1_score, {auc} AS roc_auc, {acc} AS accuracy,
        {X.shape[1]} AS n_features, '' AS mlflow_run_id, current_timestamp() AS submitted_at""")
# COMMAND ----------
# Register best model
import mlflow
mlflow.set_registry_uri("databricks-uc")
best_run = mlflow.search_runs(experiment_names=[exp_name], order_by=["metrics.f1_score DESC"]).iloc[0]
model_uri = f"runs:/{best_run.run_id}/model"
try:
    mlflow.register_model(model_uri, f"{CATALOG}.{SCHEMA}.heart_disease_model")
    print("Model registered in UC")
except Exception as e:
    print(f"Registration: {e}")
# COMMAND ----------
submit("event3", {"model": "RandomForest", "f1": round(f1, 4)})
