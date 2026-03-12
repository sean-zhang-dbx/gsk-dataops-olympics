# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.default.heart_silver_correct AS SELECT * FROM {SHARED_CATALOG}.default.heart_disease")
# COMMAND ----------
import mlflow, mlflow.sklearn
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, accuracy_score, roc_auc_score
import pandas as pd, numpy as np
# COMMAND ----------
mlflow.set_tracking_uri("databricks")
exp_name = f"/Users/odl_instructor_2119401@databrickslabs.com/{TEAM_NAME}_heart_ml"
mlflow.set_experiment(exp_name)
df = spark.table(f"{CATALOG}.default.heart_silver_correct").toPandas()
X = df.drop("target", axis=1); y = df["target"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
# COMMAND ----------
# Run 1: LogisticRegression baseline
with mlflow.start_run(run_name=f"{TEAM_NAME}_lr"):
    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("n_features", X.shape[1])
    m = LogisticRegression(max_iter=1000)
    m.fit(X_train, y_train)
    mlflow.log_metric("f1_score", f1_score(y_test, m.predict(X_test)))
    mlflow.log_metric("accuracy", accuracy_score(y_test, m.predict(X_test)))
    mlflow.sklearn.log_model(m, "model")
# COMMAND ----------
# Run 2: RandomForest
with mlflow.start_run(run_name=f"{TEAM_NAME}_rf"):
    mlflow.log_param("model_type", "RandomForest")
    mlflow.log_param("n_features", X.shape[1])
    m = RandomForestClassifier(n_estimators=200, random_state=42)
    m.fit(X_train, y_train)
    f1_rf = f1_score(y_test, m.predict(X_test))
    mlflow.log_metric("f1_score", f1_rf)
    mlflow.log_metric("accuracy", accuracy_score(y_test, m.predict(X_test)))
    mlflow.sklearn.log_model(m, "model")
# COMMAND ----------
# Run 3: GradientBoosting (best)
with mlflow.start_run(run_name=f"{TEAM_NAME}_gb") as run:
    mlflow.log_param("model_type", "GradientBoosting")
    mlflow.log_param("n_features", X.shape[1])
    best_model = GradientBoostingClassifier(n_estimators=200, learning_rate=0.1, random_state=42)
    best_model.fit(X_train, y_train)
    preds = best_model.predict(X_test)
    f1 = f1_score(y_test, preds)
    acc = accuracy_score(y_test, preds)
    auc = roc_auc_score(y_test, best_model.predict_proba(X_test)[:, 1])
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("accuracy", acc)
    mlflow.log_metric("roc_auc", auc)
    mlflow.sklearn.log_model(best_model, "model")
    best_run_id = run.info.run_id
    print(f"GB: F1={f1:.4f}, Acc={acc:.4f}, AUC={auc:.4f}")
# COMMAND ----------
# Save results
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.event3_results AS
    SELECT '{TEAM_NAME}' AS team, 'GradientBoosting' AS model_type,
        {f1} AS f1_score, {auc} AS roc_auc, {acc} AS accuracy,
        {X.shape[1]} AS n_features, '{best_run_id}' AS mlflow_run_id, current_timestamp() AS submitted_at""")
# COMMAND ----------
# Register model
mlflow.set_registry_uri("databricks-uc")
try:
    mlflow.register_model(f"runs:/{best_run_id}/model", f"{CATALOG}.default.heart_disease_model")
    print("Model registered")
except Exception as e:
    print(f"Registration: {e}")
# COMMAND ----------
# Bonus: SHAP
try:
    import shap
    explainer = shap.TreeExplainer(best_model)
    shap_values = explainer.shap_values(X_test)
    importance = pd.DataFrame({"feature": X.columns, "mean_shap": np.abs(shap_values).mean(axis=0)}).sort_values("mean_shap", ascending=False)
    spark.createDataFrame(importance).write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_shap_importance")
    print("SHAP importance saved")
except Exception as e:
    print(f"SHAP: {e}")
# COMMAND ----------
# Bonus: Cross-validation
cv_scores = cross_val_score(best_model, X, y, cv=5, scoring="f1")
cv_df = pd.DataFrame({"fold": range(1, 6), "f1_score": cv_scores, "mean_f1": cv_scores.mean()})
spark.createDataFrame(cv_df).write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_cv_results")
print(f"CV F1: {cv_scores.mean():.4f} (+/- {cv_scores.std():.4f})")
# COMMAND ----------
submit("event3", {"model": "GradientBoosting", "f1": round(f1, 4), "bonus": ["shap", "cv"]})
print(f"team_05 Event 3 MAX SCORE complete")
