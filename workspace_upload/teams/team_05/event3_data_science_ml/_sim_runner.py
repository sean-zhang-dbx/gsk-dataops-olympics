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
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import f1_score, accuracy_score, roc_auc_score

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(EXPERIMENT_PATH)
# COMMAND ----------
df = spark.table(f"{CATALOG}.default.heart_silver_correct").toPandas()

FEATURE_COLS = ["age", "sex", "cp", "trestbps", "chol", "fbs", "restecg", "thalach", "exang", "oldpeak", "slope", "ca", "thal", "age_chol", "hr_reserve", "high_risk"]
df["age_chol"] = df["age"] * df["chol"]
df["hr_reserve"] = 220 - df["age"] - df["thalach"]
df["high_risk"] = ((df["age"] > 55) & (df["chol"] > 240)).astype(int)
# COMMAND ----------
# Feature Store
df["patient_id"] = range(len(df))
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()
features_sdf = spark.createDataFrame(df[FEATURE_COLS + ["patient_id"]])
try:
    fe.create_table(
        name=f"{CATALOG}.default.heart_features",
        primary_keys=["patient_id"],
        df=features_sdf,
        description="Heart disease features with engineered columns"
    )
    print("Feature Store table created")
except Exception as e:
    features_sdf.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_features")
    print(f"Saved as regular table: {str(e)[:60]}")
# COMMAND ----------
X = df[FEATURE_COLS]
y = df["target"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
# COMMAND ----------
with mlflow.start_run(run_name=f"{TEAM_NAME}_rf"):
    mlflow.log_param("model_type", "RandomForest")
    mlflow.log_param("n_features", len(FEATURE_COLS))
    model = RandomForestClassifier(n_estimators=200, max_depth=8, random_state=42)
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
with mlflow.start_run(run_name=f"{TEAM_NAME}_gbt"):
    mlflow.log_param("model_type", "GradientBoosting")
    mlflow.log_param("n_features", len(FEATURE_COLS))
    model2 = GradientBoostingClassifier(n_estimators=150, learning_rate=0.1, random_state=42)
    model2.fit(X_train, y_train)
    preds2 = model2.predict(X_test)
    f1_2 = f1_score(y_test, preds2)
    auc_2 = roc_auc_score(y_test, model2.predict_proba(X_test)[:, 1])
    acc_2 = accuracy_score(y_test, preds2)
    mlflow.log_metric("f1_score", f1_2)
    mlflow.log_metric("accuracy", acc_2)
    mlflow.log_metric("roc_auc", auc_2)
    mlflow.sklearn.log_model(model2, "model")
    print(f"GBT: F1={f1_2:.4f}")
# COMMAND ----------
best_f1 = max(f1, f1_2)
best_model = "RandomForest" if f1 >= f1_2 else "GradientBoosting"
best_auc = auc if f1 >= f1_2 else auc_2
best_acc = acc if f1 >= f1_2 else acc_2

spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.event3_results AS
    SELECT '{TEAM_NAME}' AS team, '{best_model}' AS model_type,
        {best_f1} AS f1_score, {best_auc} AS roc_auc, {best_acc} AS accuracy,
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
submit("event3", {"model": best_model, "f1": round(best_f1, 4)})
print(f"team_05 Event 3 complete (FS + 2 models, best F1={best_f1:.4f})")
