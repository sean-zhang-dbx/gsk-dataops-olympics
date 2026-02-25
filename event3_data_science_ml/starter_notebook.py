# Databricks notebook source
# MAGIC %md
# MAGIC # Event 3: Data Science / ML — Model Accuracy Challenge
# MAGIC
# MAGIC ## Challenge: Build the Most Accurate Predictive Model
# MAGIC **Build Time: ~20 minutes**
# MAGIC
# MAGIC ### Objective
# MAGIC Build a classification model to predict **patient readmission risk** and register it with MLflow.
# MAGIC
# MAGIC ### How You Win
# MAGIC **Highest F1 score on the held-out test set wins Gold!**
# MAGIC
# MAGIC ### Rules
# MAGIC 1. Must use the provided `diabetes_readmission` dataset
# MAGIC 2. Must use MLflow to track your experiment
# MAGIC 3. Must evaluate on the standard test split (80/20, random_state=42)
# MAGIC 4. Must register your best model in MLflow Model Registry
# MAGIC 5. Any sklearn or Spark ML algorithm is allowed
# MAGIC 6. Feature engineering is encouraged!
# MAGIC
# MAGIC > **Tip:** Use the Databricks Assistant to help you try different models!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load and Explore the Data

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score, classification_report, confusion_matrix, roc_auc_score
import mlflow
import mlflow.sklearn

# Load the dataset
df = spark.table("diabetes_readmission").toPandas()

print(f"Dataset shape: {df.shape}")
print(f"\nTarget distribution:")
print(df["readmission_risk"].value_counts(normalize=True))

display(spark.table("diabetes_readmission"))

# COMMAND ----------

# TODO: Explore the data — distributions, correlations, missing values
# Hint: df.describe(), df.isnull().sum(), df.corr()

# YOUR EXPLORATION CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Feature Engineering
# MAGIC
# MAGIC **TODO:** Create new features that might improve prediction accuracy.
# MAGIC
# MAGIC Ideas:
# MAGIC - Interaction features (e.g., glucose x BMI)
# MAGIC - Binned features (e.g., age groups)
# MAGIC - Polynomial features
# MAGIC - Domain-specific features

# COMMAND ----------

# TODO: Feature Engineering — this is where you differentiate from other teams!
# Example:
# df["glucose_bmi_interaction"] = df["glucose"] * df["bmi"]
# df["high_risk_flag"] = ((df["glucose"] > 140) & (df["bmi"] > 30)).astype(int)

# YOUR FEATURE ENGINEERING CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Train/Test Split
# MAGIC
# MAGIC **IMPORTANT:** Use the exact split below so all teams are evaluated on the same test set.

# COMMAND ----------

# Define features and target
# TODO: Update feature_cols if you added new features
feature_cols = ["pregnancies", "glucose", "blood_pressure", "skin_thickness",
                "insulin", "bmi", "diabetes_pedigree", "age"]

X = df[feature_cols]
y = df["readmission_risk"]

# MANDATORY: Use this exact split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Training set: {X_train.shape[0]} samples")
print(f"Test set:     {X_test.shape[0]} samples")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Train Your Model with MLflow

# COMMAND ----------

TEAM_NAME = "_____"  # e.g., "team_01"
mlflow.set_experiment(f"/dataops_olympics/{TEAM_NAME}_ml_challenge")

# COMMAND ----------

# TODO: Train your model and log with MLflow
# You can run this cell multiple times to try different models/hyperparameters

from sklearn.ensemble import RandomForestClassifier  # Or your chosen model

with mlflow.start_run(run_name=f"{TEAM_NAME}_attempt_1"):

    # TODO: Define and train your model
    model = _____  # YOUR MODEL HERE
    model.fit(X_train, y_train)

    # Predict
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]

    # Calculate metrics
    f1 = f1_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_pred_proba)

    # Log parameters
    mlflow.log_params(_____)  # YOUR PARAMS (e.g., n_estimators, max_depth)

    # Log metrics
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("auc_roc", auc)

    # Log the model
    mlflow.sklearn.log_model(model, "model")

    print(f"F1 Score: {f1:.4f}")
    print(f"AUC-ROC:  {auc:.4f}")
    print(f"\n{classification_report(y_test, y_pred)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Register Your Best Model
# MAGIC
# MAGIC **TODO:** Register your best model in the MLflow Model Registry.
# MAGIC This is required for scoring!

# COMMAND ----------

# TODO: Register the model
# Option 1: From the MLflow UI — click "Register Model" on your best run
# Option 2: Programmatically:

# run_id = mlflow.active_run().info.run_id  # or get from MLflow UI
# model_uri = f"runs:/{run_id}/model"
# mlflow.register_model(model_uri, f"{TEAM_NAME}_readmission_model")

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Submission

# COMMAND ----------

print("=" * 60)
print(f"  ML CHALLENGE — FINAL SUBMISSION: {TEAM_NAME}")
print("=" * 60)

y_final_pred = model.predict(X_test)
y_final_proba = model.predict_proba(X_test)[:, 1]

final_f1 = f1_score(y_test, y_final_pred)
final_auc = roc_auc_score(y_test, y_final_proba)

print(f"\n  F1 Score:  {final_f1:.4f}")
print(f"  AUC-ROC:   {final_auc:.4f}")
print(f"\n{classification_report(y_test, y_final_pred)}")

cm = confusion_matrix(y_test, y_final_pred)
print(f"  Confusion Matrix:")
print(f"    TN={cm[0][0]}  FP={cm[0][1]}")
print(f"    FN={cm[1][0]}  TP={cm[1][1]}")

print(f"\n  F1 SCORE TO REPORT: {final_f1:.4f}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checklist
# MAGIC - [ ] Data explored and understood
# MAGIC - [ ] Feature engineering attempted
# MAGIC - [ ] Model trained with MLflow tracking
# MAGIC - [ ] F1 score calculated on test set
# MAGIC - [ ] Model registered in MLflow Model Registry
# MAGIC
# MAGIC > **Report your F1 score to the judges!**
