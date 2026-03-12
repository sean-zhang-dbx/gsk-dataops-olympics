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
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, accuracy_score, roc_auc_score

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(EXPERIMENT_PATH)
# COMMAND ----------
df = spark.table(f"{CATALOG}.default.heart_silver_correct").toPandas()

FEATURE_COLS = ["age", "sex", "cp", "trestbps", "chol", "fbs", "restecg", "thalach", "exang", "oldpeak", "slope", "ca", "thal", "age_chol", "hr_reserve"]
df["age_chol"] = df["age"] * df["chol"]
df["hr_reserve"] = 220 - df["age"] - df["thalach"]

features_sdf = spark.createDataFrame(df[FEATURE_COLS + ["target"]])
features_sdf.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_features")
print(f"Features table saved: {len(FEATURE_COLS)} columns")
# COMMAND ----------
X = df[FEATURE_COLS]
y = df["target"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
# COMMAND ----------
with mlflow.start_run(run_name=f"{TEAM_NAME}_rf"):
    mlflow.log_param("model_type", "RandomForest")
    mlflow.log_param("n_features", len(FEATURE_COLS))
    model = RandomForestClassifier(n_estimators=150, random_state=42)
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    f1 = f1_score(y_test, preds)
    auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
    acc = accuracy_score(y_test, preds)
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("accuracy", acc)
    mlflow.log_metric("roc_auc", auc)
    mlflow.sklearn.log_model(model, "model")
    print(f"RF: F1={f1:.4f}")
# COMMAND ----------
with mlflow.start_run(run_name=f"{TEAM_NAME}_lr"):
    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("n_features", len(FEATURE_COLS))
    model2 = LogisticRegression(max_iter=1000, random_state=42)
    model2.fit(X_train, y_train)
    preds2 = model2.predict(X_test)
    f1_2 = f1_score(y_test, preds2)
    mlflow.log_metric("f1_score", f1_2)
    mlflow.log_metric("accuracy", accuracy_score(y_test, preds2))
    mlflow.sklearn.log_model(model2, "model")
    print(f"LR: F1={f1_2:.4f}")
# COMMAND ----------
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.event3_results AS
    SELECT '{TEAM_NAME}' AS team, 'RandomForest' AS model_type,
        {f1} AS f1_score, {auc} AS roc_auc, {acc} AS accuracy,
        {len(FEATURE_COLS)} AS n_features, '' AS mlflow_run_id, current_timestamp() AS submitted_at""")
# COMMAND ----------
try:
    mlflow.set_registry_uri("databricks-uc")
    best_run = mlflow.search_runs(experiment_names=[EXPERIMENT_PATH], order_by=["metrics.f1_score DESC"]).iloc[0]
    mlflow.register_model(f"runs:/{best_run.run_id}/model", f"{CATALOG}.default.heart_disease_model")
    print("Model registered")
except Exception as e:
    print(f"Registration: {str(e)[:60]}")
# COMMAND ----------
submit("event3", {"model": "RandomForest", "f1": round(f1, 4)})
print(f"team_02 Event 3 complete (good — RF+LR, features table, F1={f1:.4f})")
