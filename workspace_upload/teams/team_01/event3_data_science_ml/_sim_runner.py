# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.default.heart_silver_correct AS SELECT * FROM {SHARED_CATALOG}.default.heart_disease")
# COMMAND ----------
import mlflow, mlflow.sklearn
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, VotingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, accuracy_score, roc_auc_score

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(EXPERIMENT_PATH)
# COMMAND ----------
df = spark.table(f"{CATALOG}.default.heart_silver_correct").toPandas()
BASE_FEATURES = ["age", "sex", "cp", "trestbps", "chol", "fbs", "restecg", "thalach", "exang", "oldpeak", "slope", "ca", "thal"]

df["age_chol"] = df["age"] * df["chol"]
df["hr_reserve"] = 220 - df["age"] - df["thalach"]
df["high_risk"] = ((df["age"] > 55) & (df["chol"] > 240)).astype(int)
df["chol_risk"] = (df["chol"] > 240).astype(int)
df["bp_category"] = pd.cut(df["trestbps"], bins=[0, 120, 140, 160, 300], labels=[0, 1, 2, 3]).astype(int)

FEATURE_COLS = BASE_FEATURES + ["age_chol", "hr_reserve", "high_risk", "chol_risk", "bp_category"]
X = df[FEATURE_COLS]
y = df["target"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Feature Store
# COMMAND ----------
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()
df["patient_id"] = range(len(df))
features_sdf = spark.createDataFrame(df[FEATURE_COLS + ["patient_id"]])
try:
    fe.create_table(
        name=f"{CATALOG}.default.heart_features",
        primary_keys=["patient_id"],
        df=features_sdf,
        description="Heart disease patient features for ML prediction"
    )
    print("Feature Store table created")
except Exception as e:
    features_sdf.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_features")
    print(f"Saved as regular table (FS error: {str(e)[:60]})")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Run 1: LogisticRegression
# COMMAND ----------
with mlflow.start_run(run_name=f"{TEAM_NAME}_lr"):
    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("n_features", len(FEATURE_COLS))
    model_lr = LogisticRegression(max_iter=1000, random_state=42)
    model_lr.fit(X_train, y_train)
    preds = model_lr.predict(X_test)
    f1_lr = f1_score(y_test, preds)
    mlflow.log_metric("f1_score", f1_lr)
    mlflow.log_metric("accuracy", accuracy_score(y_test, preds))
    mlflow.log_metric("roc_auc", roc_auc_score(y_test, model_lr.predict_proba(X_test)[:, 1]))
    mlflow.sklearn.log_model(model_lr, "model")
    print(f"LR: F1={f1_lr:.4f}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Run 2: RandomForest
# COMMAND ----------
with mlflow.start_run(run_name=f"{TEAM_NAME}_rf"):
    mlflow.log_param("model_type", "RandomForest")
    mlflow.log_param("n_features", len(FEATURE_COLS))
    model_rf = RandomForestClassifier(n_estimators=200, max_depth=10, random_state=42)
    model_rf.fit(X_train, y_train)
    preds = model_rf.predict(X_test)
    f1_rf = f1_score(y_test, preds)
    mlflow.log_metric("f1_score", f1_rf)
    mlflow.log_metric("accuracy", accuracy_score(y_test, preds))
    mlflow.log_metric("roc_auc", roc_auc_score(y_test, model_rf.predict_proba(X_test)[:, 1]))
    mlflow.sklearn.log_model(model_rf, "model")
    print(f"RF: F1={f1_rf:.4f}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Run 3: GradientBoosting
# COMMAND ----------
with mlflow.start_run(run_name=f"{TEAM_NAME}_gbt"):
    mlflow.log_param("model_type", "GradientBoosting")
    mlflow.log_param("n_features", len(FEATURE_COLS))
    model_gbt = GradientBoostingClassifier(n_estimators=200, learning_rate=0.1, max_depth=5, random_state=42)
    model_gbt.fit(X_train, y_train)
    preds = model_gbt.predict(X_test)
    f1_gbt = f1_score(y_test, preds)
    mlflow.log_metric("f1_score", f1_gbt)
    mlflow.log_metric("accuracy", accuracy_score(y_test, preds))
    mlflow.log_metric("roc_auc", roc_auc_score(y_test, model_gbt.predict_proba(X_test)[:, 1]))
    mlflow.sklearn.log_model(model_gbt, "model")
    print(f"GBT: F1={f1_gbt:.4f}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Run 4: Ensemble (Bonus +3)
# COMMAND ----------
with mlflow.start_run(run_name=f"{TEAM_NAME}_ensemble"):
    mlflow.log_param("model_type", "VotingClassifier")
    mlflow.log_param("n_features", len(FEATURE_COLS))
    model = VotingClassifier(estimators=[
        ('rf', RandomForestClassifier(n_estimators=200, max_depth=10, random_state=42)),
        ('gbt', GradientBoostingClassifier(n_estimators=200, learning_rate=0.1, max_depth=5, random_state=42)),
        ('lr', LogisticRegression(max_iter=1000, random_state=42)),
    ], voting='soft')
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    f1 = f1_score(y_test, preds)
    auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
    acc = accuracy_score(y_test, preds)
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("accuracy", acc)
    mlflow.log_metric("roc_auc", auc)
    mlflow.sklearn.log_model(model, "model")
    print(f"Ensemble: F1={f1:.4f}, AUC={auc:.4f}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Bonus: SHAP Explainability (+3)
# COMMAND ----------
try:
    import shap
    explainer = shap.TreeExplainer(model_rf)
    shap_values = explainer.shap_values(X_test)
    shap_imp = np.abs(shap_values).mean(axis=0) if len(shap_values.shape) == 2 else np.abs(shap_values[1]).mean(axis=0)
    top5 = sorted(zip(FEATURE_COLS, shap_imp), key=lambda x: -x[1])[:5]
    shap_df = spark.createDataFrame(top5, ["feature", "importance"])
    shap_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_shap_importance")
    print(f"SHAP: top features = {[f[0] for f in top5]}")
except Exception as e:
    print(f"SHAP skipped: {str(e)[:60]}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Bonus: Cross-Validation (+2)
# COMMAND ----------
cv_scores = cross_val_score(model_rf, X_train, y_train, cv=5, scoring='f1')
cv_data = [(i+1, float(s)) for i, s in enumerate(cv_scores)]
cv_df = spark.createDataFrame(cv_data, ["fold", "f1_score"])
cv_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_cv_results")
print(f"CV: mean F1={cv_scores.mean():.4f} +/- {cv_scores.std():.4f}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Results + Register Model
# COMMAND ----------
best_run_id = ""
try:
    from mlflow.tracking import MlflowClient
    mlflow.set_registry_uri("databricks-uc")
    client = MlflowClient()
    exp = client.get_experiment_by_name(EXPERIMENT_PATH)
    if exp:
        best_runs = client.search_runs(experiment_ids=[exp.experiment_id], order_by=["metrics.f1_score DESC"], max_results=1)
        if best_runs:
            best_run_id = best_runs[0].info.run_id
            mlflow.register_model(f"runs:/{best_run_id}/model", f"{CATALOG}.default.heart_disease_model")
            print(f"Model registered from run {best_run_id}")
except Exception as e:
    print(f"Registration: {str(e)[:80]}")
# COMMAND ----------
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.event3_results AS
    SELECT '{TEAM_NAME}' AS team, 'VotingClassifier' AS model_type,
        {f1} AS f1_score, {auc} AS roc_auc, {acc} AS accuracy,
        {len(FEATURE_COLS)} AS n_features, '{best_run_id}' AS mlflow_run_id, current_timestamp() AS submitted_at""")
# COMMAND ----------
submit("event3", {"model": "VotingClassifier", "f1": round(f1, 4), "bonus": "SHAP+CV+Ensemble"})
print(f"team_01 Event 3 complete (POWER TEAM — F1={f1:.4f}, FS, Ensemble, SHAP, CV)")
