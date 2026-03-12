# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.default.heart_silver_correct AS SELECT * FROM {SHARED_CATALOG}.default.heart_disease")
# Team trained a model but forgot to use MLflow
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import f1_score
import pandas as pd
df = spark.table(f"{CATALOG}.default.heart_silver_correct").toPandas()
X = df.drop("target", axis=1); y = df["target"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = DecisionTreeClassifier(random_state=42)
model.fit(X_train, y_train)
f1 = f1_score(y_test, model.predict(X_test))
# Saved results but no MLflow
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.event3_results AS
    SELECT '{TEAM_NAME}' AS team, 'DecisionTree' AS model_type,
        {f1} AS f1_score, 0.0 AS roc_auc, 0.0 AS accuracy,
        {X.shape[1]} AS n_features, '' AS mlflow_run_id, current_timestamp() AS submitted_at""")
submit("event3", {"model": "DecisionTree", "f1": round(f1, 4), "note": "no MLflow"})
