# Databricks notebook source
# MAGIC %md
# MAGIC # Lightning Talk 3: Data Science / ML Demo
# MAGIC
# MAGIC **For organizers only — this is the 15-minute demo shown before Event 3.**
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Loading and exploring diabetes readmission data
# MAGIC 2. Feature engineering
# MAGIC 3. Training a model with MLflow tracking
# MAGIC 4. Registering the model in MLflow Model Registry

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load and Explore

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import f1_score, classification_report, roc_auc_score
import mlflow
import mlflow.sklearn
import plotly.express as px

df = spark.table("diabetes_readmission").toPandas()
print(f"Shape: {df.shape}")
print(f"\nTarget distribution:\n{df['readmission_risk'].value_counts(normalize=True)}")

display(spark.table("diabetes_readmission"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering

# COMMAND ----------

# Create interaction features
df["glucose_bmi"] = df["glucose"] * df["bmi"]
df["age_glucose"] = df["age"] * df["glucose"]
df["high_risk_flag"] = ((df["glucose"] > 140) & (df["bmi"] > 30)).astype(int)

feature_cols = ["pregnancies", "glucose", "blood_pressure", "skin_thickness",
                "insulin", "bmi", "diabetes_pedigree", "age",
                "glucose_bmi", "age_glucose", "high_risk_flag"]

X = df[feature_cols]
y = df["readmission_risk"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Train: {len(X_train)}, Test: {len(X_test)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Train with MLflow

# COMMAND ----------

mlflow.set_experiment("/dataops_olympics/demo_ml_challenge")

with mlflow.start_run(run_name="demo_gradient_boosting"):
    model = GradientBoostingClassifier(
        n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    f1 = f1_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_proba)

    mlflow.log_params({
        "model": "GradientBoosting",
        "n_estimators": 200,
        "max_depth": 5,
        "learning_rate": 0.1,
        "n_features": len(feature_cols)
    })
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("auc_roc", auc)
    mlflow.sklearn.log_model(model, "model")

    run_id = mlflow.active_run().info.run_id

    print(f"F1 Score: {f1:.4f}")
    print(f"AUC-ROC:  {auc:.4f}")
    print(f"\n{classification_report(y_test, y_pred)}")

# COMMAND ----------

# Feature importance
importances = model.feature_importances_
fig = px.bar(x=feature_cols, y=importances,
             title="Feature Importance — Readmission Risk Model",
             labels={"x": "Feature", "y": "Importance"},
             template="plotly_white")
fig.update_layout(xaxis_tickangle=-45)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Register Model

# COMMAND ----------

# Register the model in MLflow Model Registry
model_uri = f"runs:/{run_id}/model"
registered = mlflow.register_model(model_uri, "demo_readmission_model")

print(f"Model registered: {registered.name} v{registered.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Points for the Talk
# MAGIC
# MAGIC - **MLflow** tracks every experiment — parameters, metrics, artifacts, models
# MAGIC - **Feature engineering** is where you can differentiate (interaction features, domain knowledge)
# MAGIC - **Model Registry** is how you promote models to production
# MAGIC - **F1 Score** is the competition metric — it balances precision and recall
# MAGIC - The **Databricks Assistant** can suggest model improvements and feature ideas!
