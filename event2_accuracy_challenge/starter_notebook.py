# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Event 2: Accuracy Challenge
# MAGIC
# MAGIC ## Challenge: Build the Most Accurate Predictive Model
# MAGIC **Time Limit: 20 minutes**
# MAGIC
# MAGIC ### Objective
# MAGIC Build a machine learning model to predict **patient readmission risk** using the diabetes/readmission dataset.
# MAGIC
# MAGIC ### Scoring
# MAGIC - **F1 Score** (max 15 pts): Proportional to best F1 score across all teams
# MAGIC - **Explainability Bonus** (max 5 pts): Judge-rated
# MAGIC   - Feature importance plot (+2 pts)
# MAGIC   - SHAP values or similar (+2 pts)
# MAGIC   - Clear model documentation (+1 pt)
# MAGIC - **Total**: max 20 pts
# MAGIC
# MAGIC ### Rules
# MAGIC 1. Must use the provided `diabetes_readmission` dataset
# MAGIC 2. Must use MLflow to track your experiment
# MAGIC 3. Must evaluate on the provided test split (80/20 split, random_state=42)
# MAGIC 4. Any sklearn or Spark ML algorithm is allowed
# MAGIC 5. Feature engineering is encouraged!
# MAGIC
# MAGIC > ⏱️ START YOUR TIMER NOW!

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
import plotly.express as px

# Load the dataset
df = spark.table("diabetes_readmission").toPandas()

print(f"Dataset shape: {df.shape}")
print(f"\nTarget distribution:")
print(df["readmission_risk"].value_counts(normalize=True))

display(spark.table("diabetes_readmission"))

# COMMAND ----------

# TODO: Explore the data - check for missing values, distributions, correlations
# Hint: df.describe(), df.isnull().sum(), df.corr()

# YOUR EXPLORATION CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Feature Engineering
# MAGIC
# MAGIC **TODO:** Create new features that might improve prediction accuracy.
# MAGIC
# MAGIC Ideas:
# MAGIC - Interaction features (e.g., glucose × BMI)
# MAGIC - Binned features (e.g., age groups)
# MAGIC - Polynomial features
# MAGIC - Domain-specific features

# COMMAND ----------

# TODO: Feature Engineering
# This is where you can differentiate from other teams!

# Example:
# df["glucose_bmi_interaction"] = df["glucose"] * df["bmi"]
# df["age_group"] = pd.cut(df["age"], bins=[0, 30, 45, 60, 100], labels=["young", "middle", "senior", "elderly"])

# YOUR FEATURE ENGINEERING CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Prepare Train/Test Split
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
print(f"Train target dist: {y_train.value_counts(normalize=True).to_dict()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Train Your Model with MLflow Tracking
# MAGIC
# MAGIC **TODO:** Train a classification model and log it with MLflow.
# MAGIC
# MAGIC Options to try:
# MAGIC - `RandomForestClassifier`
# MAGIC - `GradientBoostingClassifier`
# MAGIC - `XGBClassifier` (if xgboost installed)
# MAGIC - `LogisticRegression`
# MAGIC - Ensemble / Stacking

# COMMAND ----------

# TODO: Set up MLflow experiment
TEAM_NAME = "_____"  # e.g., "team_01"
mlflow.set_experiment(f"/dataops_olympics/{TEAM_NAME}_accuracy_challenge")

# COMMAND ----------

# TODO: Train your model and log with MLflow
# You can run this cell multiple times to try different models/hyperparameters

from sklearn.ensemble import RandomForestClassifier  # Or your chosen model

with mlflow.start_run(run_name=f"{TEAM_NAME}_attempt_1"):
    
    # TODO: Define and train your model
    model = _____  # YOUR MODEL HERE
    
    # Fit the model
    model.fit(X_train, y_train)
    
    # Predict
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    
    # Calculate metrics
    f1 = f1_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_pred_proba)
    
    # Log parameters
    mlflow.log_params(_____)  # YOUR PARAMS HERE (e.g., model hyperparameters)
    
    # Log metrics
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("auc_roc", auc)
    
    # Log the model
    mlflow.sklearn.log_model(model, "model")
    
    print(f"F1 Score: {f1:.4f}")
    print(f"AUC-ROC: {auc:.4f}")
    print(f"\nClassification Report:")
    print(classification_report(y_test, y_pred))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Explainability (Bonus Points!)
# MAGIC
# MAGIC **TODO:** Add model explainability for up to 5 bonus points.

# COMMAND ----------

# TODO: Feature Importance Plot (+2 pts)
# Hint: For tree-based models, use model.feature_importances_

# Example:
# importances = model.feature_importances_
# fig = px.bar(x=feature_cols, y=importances, title="Feature Importance")
# fig.show()

# YOUR CODE HERE


# COMMAND ----------

# TODO: SHAP Values or Partial Dependence (+2 pts)
# Hint: pip install shap, then:
# import shap
# explainer = shap.TreeExplainer(model)
# shap_values = explainer.shap_values(X_test)
# shap.summary_plot(shap_values, X_test)

# YOUR CODE HERE


# COMMAND ----------

# TODO: Model Documentation (+1 pt)
# Write a brief summary of your approach:
# - What model did you choose and why?
# - What features were most important?
# - What did you try that didn't work?
# - If you had more time, what would you do?

# YOUR DOCUMENTATION HERE (as comments or markdown)


# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Final Submission
# MAGIC
# MAGIC Run this cell to generate your final score.

# COMMAND ----------

print("=" * 60)
print(f"ACCURACY CHALLENGE — FINAL SUBMISSION: {TEAM_NAME}")
print("=" * 60)

# Final evaluation on test set
y_final_pred = model.predict(X_test)
y_final_proba = model.predict_proba(X_test)[:, 1]

final_f1 = f1_score(y_test, y_final_pred)
final_auc = roc_auc_score(y_test, y_final_proba)

print(f"\n📊 FINAL METRICS:")
print(f"   F1 Score:  {final_f1:.4f}")
print(f"   AUC-ROC:   {final_auc:.4f}")
print(f"\n📋 Classification Report:")
print(classification_report(y_test, y_final_pred))

# Confusion Matrix
cm = confusion_matrix(y_test, y_final_pred)
print(f"📈 Confusion Matrix:")
print(f"   TN={cm[0][0]}  FP={cm[0][1]}")
print(f"   FN={cm[1][0]}  TP={cm[1][1]}")

print(f"\n🏆 F1 Score to report to judges: {final_f1:.4f}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checklist
# MAGIC - [ ] Data explored and understood
# MAGIC - [ ] Feature engineering attempted
# MAGIC - [ ] Model trained with MLflow tracking
# MAGIC - [ ] F1 score calculated on test set
# MAGIC - [ ] Feature importance plotted (bonus)
# MAGIC - [ ] SHAP values computed (bonus)
# MAGIC - [ ] Model documentation written (bonus)
# MAGIC
# MAGIC > 🏁 **Report your F1 score to the judges!**
