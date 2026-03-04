# Databricks notebook source
# MAGIC %md
# MAGIC # Event 3: Answer Key — Complete ML Solution
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**

# COMMAND ----------

TEAM_NAME = "answer_key"
CATALOG = "team_01"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Data

# COMMAND ----------

import pandas as pd
import numpy as np

df = spark.table("heart_silver").toPandas()
print(f"Shape: {df.shape}")
print(f"\nTarget distribution:\n{df['target'].value_counts()}")
print(f"\nDisease rate: {df['target'].mean()*100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Feature Engineering

# COMMAND ----------

df["hr_reserve"] = 220 - df["age"] - df["thalach"]
df["high_risk"] = ((df["age"] > 55) & (df["chol"] > 240)).astype(int)
df["chol_risk"] = (df["chol"] > 240).astype(int)
df["age_chol"] = df["age"] * df["chol"]
df["bp_hr_ratio"] = df["trestbps"] / (df["thalach"] + 1)

print(f"Added 5 engineered features. New shape: {df.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Train/Test Split

# COMMAND ----------

from sklearn.model_selection import train_test_split

FEATURE_COLS = [
    "age", "sex", "cp", "trestbps", "chol", "fbs", "restecg",
    "thalach", "exang", "oldpeak", "slope", "ca", "thal",
    "hr_reserve", "high_risk", "chol_risk", "age_chol", "bp_hr_ratio",
]

X = df[FEATURE_COLS].fillna(0)
y = df["target"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"Train: {X_train.shape}, Test: {X_test.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Train Models + MLflow

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, VotingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, roc_auc_score, accuracy_score, classification_report
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

model = best_model
print(f"\nBest: F1={best_f1:.4f} (run_id={best_run_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Ensemble

# COMMAND ----------

with mlflow.start_run(run_name="Ensemble"):
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
    mlflow.log_metric("f1_score", f1_e)
    mlflow.log_metric("roc_auc", auc_e)

    sig = infer_signature(X_train, ensemble.predict(X_train))
    mlflow.sklearn.log_model(ensemble, "model", signature=sig)

    print(f"Ensemble: F1={f1_e:.4f}  AUC={auc_e:.4f}")
    if f1_e > best_f1:
        best_f1 = f1_e
        model = ensemble
        best_run_id = mlflow.active_run().info.run_id
        print("  -> New best!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Validation

# COMMAND ----------

from sklearn.model_selection import cross_val_score

cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring="f1")
print(f"5-Fold CV F1: {cv_scores.mean():.4f} +/- {cv_scores.std():.4f}")
print(f"Per-fold: {[f'{s:.4f}' for s in cv_scores]}")

cv_df = pd.DataFrame({"fold": range(1, 6), "f1_score": cv_scores})
spark.createDataFrame(cv_df).write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.default.heart_cv_results"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Report

# COMMAND ----------

print("=" * 60)
print(f"  ANSWER KEY — ML CHALLENGE")
print("=" * 60)
y_pred_final = model.predict(X_test)
y_proba_final = model.predict_proba(X_test)[:, 1]
f1_final = f1_score(y_test, y_pred_final)
auc_final = roc_auc_score(y_test, y_proba_final)
print(f"\n  Best F1: {f1_final:.4f}")
print(f"  AUC-ROC: {auc_final:.4f}")
print(f"\n{classification_report(y_test, y_pred_final)}")
print("=" * 60)
