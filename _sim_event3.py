# Databricks notebook source
# Event 3 Simulation: team_01 does full ML challenge + bonuses

TEAM_NAME = "team_01"
CATALOG = TEAM_NAME

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")

# COMMAND ----------

# %pip install shap --quiet

# COMMAND ----------

import pandas as pd
import numpy as np

df = spark.table("heart_silver").toPandas()
print(f"Shape: {df.shape}")
print(f"Disease rate: {df['target'].mean()*100:.1f}%")

# COMMAND ----------

# Feature engineering
df["hr_reserve"] = 220 - df["age"] - df["thalach"]
df["high_risk"] = ((df["age"] > 55) & (df["chol"] > 240)).astype(int)
df["chol_risk"] = (df["chol"] > 240).astype(int)
df["age_chol"] = df["age"] * df["chol"]
df["bp_hr_ratio"] = df["trestbps"] / (df["thalach"] + 1)

FEATURE_COLS = [
    "age", "sex", "cp", "trestbps", "chol", "fbs", "restecg",
    "thalach", "exang", "oldpeak", "slope", "ca", "thal",
    "hr_reserve", "high_risk", "chol_risk", "age_chol", "bp_hr_ratio",
]
print(f"Features: {len(FEATURE_COLS)}")

# COMMAND ----------

from sklearn.model_selection import train_test_split

X = df[FEATURE_COLS].fillna(0)
y = df["target"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
print(f"Train: {X_train.shape}, Test: {X_test.shape}")

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, VotingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, roc_auc_score, accuracy_score
from mlflow.models.signature import infer_signature

try:
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
except Exception:
    user = "organizer"
mlflow.set_experiment(f"/Users/{user}/{TEAM_NAME}_heart_ml")

models_to_try = {
    "RandomForest": RandomForestClassifier(n_estimators=200, max_depth=10, random_state=42),
    "GradientBoosting": GradientBoostingClassifier(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42),
    "LogisticRegression": LogisticRegression(max_iter=1000, C=1.0, random_state=42),
}

best_f1 = 0
best_model = None
best_run_id = None

for name, clf in models_to_try.items():
    with mlflow.start_run(run_name=name):
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        y_proba = clf.predict_proba(X_test)[:, 1]

        f1 = f1_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_proba)
        acc = accuracy_score(y_test, y_pred)

        mlflow.log_param("model_type", name)
        mlflow.log_param("n_features", len(FEATURE_COLS))
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("roc_auc", auc)
        mlflow.log_metric("accuracy", acc)

        sig = infer_signature(X_train, clf.predict(X_train))
        mlflow.sklearn.log_model(clf, "model", signature=sig)

        print(f"  {name}: F1={f1:.4f}  AUC={auc:.4f}  Acc={acc:.4f}")

        if f1 > best_f1:
            best_f1 = f1
            best_model = clf
            best_run_id = mlflow.active_run().info.run_id

print(f"\nBest: F1={best_f1:.4f}")

# COMMAND ----------

# === BONUS: Ensemble ===
with mlflow.start_run(run_name="VotingClassifier"):
    ensemble = VotingClassifier(
        estimators=[
            ("rf", RandomForestClassifier(n_estimators=200, max_depth=10, random_state=42)),
            ("gb", GradientBoostingClassifier(n_estimators=200, max_depth=5, random_state=42)),
            ("lr", LogisticRegression(max_iter=1000, random_state=42)),
        ],
        voting="soft",
    )
    ensemble.fit(X_train, y_train)
    y_pred_e = ensemble.predict(X_test)
    y_proba_e = ensemble.predict_proba(X_test)[:, 1]

    f1_e = f1_score(y_test, y_pred_e)
    auc_e = roc_auc_score(y_test, y_proba_e)

    mlflow.log_param("model_type", "VotingClassifier")
    mlflow.log_param("n_features", len(FEATURE_COLS))
    mlflow.log_metric("f1_score", f1_e)
    mlflow.log_metric("roc_auc", auc_e)

    sig = infer_signature(X_train, ensemble.predict(X_train))
    mlflow.sklearn.log_model(ensemble, "model", signature=sig)
    print(f"Ensemble: F1={f1_e:.4f}  AUC={auc_e:.4f}")

    if f1_e > best_f1:
        best_f1 = f1_e
        best_model = ensemble
        best_run_id = mlflow.active_run().info.run_id

# COMMAND ----------

# === BONUS: Cross-Validation ===
from sklearn.model_selection import cross_val_score

cv_scores = cross_val_score(best_model, X_train, y_train, cv=5, scoring="f1")
print(f"5-Fold CV F1: {cv_scores.mean():.4f} +/- {cv_scores.std():.4f}")

cv_df = pd.DataFrame({"fold": range(1, 6), "f1_score": cv_scores})
spark.createDataFrame(cv_df).write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.default.heart_cv_results"
)
print("CV results saved")

# COMMAND ----------

# === BONUS: SHAP (simplified - save feature importance) ===
if hasattr(best_model, 'feature_importances_'):
    importances = best_model.feature_importances_
elif hasattr(best_model, 'estimators_'):
    try:
        importances = np.mean([est.feature_importances_ for _, est in best_model.named_estimators_.items() if hasattr(est, 'feature_importances_')], axis=0)
    except Exception:
        importances = None
else:
    importances = None

if importances is not None:
    shap_df = pd.DataFrame({
        "feature": FEATURE_COLS,
        "importance": importances,
    }).sort_values("importance", ascending=False)
    spark.createDataFrame(shap_df).write.format("delta").mode("overwrite").saveAsTable(
        f"{CATALOG}.default.heart_shap_importance"
    )
    print("SHAP importance table saved")
else:
    print("Could not compute feature importances")

# COMMAND ----------

# === Model Registration ===
try:
    model_uri = f"runs:/{best_run_id}/model"
    model_name = f"{CATALOG}.default.heart_disease_model"
    mlflow.set_registry_uri("databricks-uc")
    mlflow.register_model(model_uri, model_name)
    print(f"Model registered: {model_name}")
except Exception as e:
    print(f"Model registration: {e}")

# COMMAND ----------

print("Event 3 simulation complete for team_01")
dbutils.notebook.exit("OK")
