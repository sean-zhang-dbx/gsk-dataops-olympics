# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.default.heart_silver_correct AS SELECT * FROM {SHARED_CATALOG}.default.heart_disease")
# COMMAND ----------
import mlflow, mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, accuracy_score
# COMMAND ----------
mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(f"/Users/odl_instructor_2119401@databrickslabs.com/{TEAM_NAME}_heart_ml")
df = spark.table(f"{CATALOG}.default.heart_silver_correct").toPandas()
X = df.drop("target", axis=1); y = df["target"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
with mlflow.start_run(run_name=f"{TEAM_NAME}_lr"):
    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("n_features", X.shape[1])
    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    f1 = f1_score(y_test, preds)
    acc = accuracy_score(y_test, preds)
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("accuracy", acc)
    mlflow.sklearn.log_model(model, "model")
# COMMAND ----------
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.event3_results AS
    SELECT '{TEAM_NAME}' AS team, 'LogisticRegression' AS model_type,
        {f1} AS f1_score, 0.0 AS roc_auc, {acc} AS accuracy,
        {X.shape[1]} AS n_features, '' AS mlflow_run_id, current_timestamp() AS submitted_at""")
submit("event3", {"model": "LR", "f1": round(f1, 4)})
