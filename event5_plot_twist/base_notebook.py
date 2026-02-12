# Databricks notebook source
# MAGIC %md
# MAGIC # 🎭 Event 5: Plot Twist Finals
# MAGIC
# MAGIC ## Challenge: Adapt to Surprise Constraints!
# MAGIC **Time Limit: 20 minutes**
# MAGIC
# MAGIC ### Overview
# MAGIC Only the **Top 3 teams** (by cumulative score) compete in this event.
# MAGIC
# MAGIC You'll start with a working solution, then draw a **Plot Twist Card** that changes the rules.
# MAGIC You must adapt your solution in real-time and present to judges.
# MAGIC
# MAGIC ### Scoring
# MAGIC - **Adaptation Speed** (max 10 pts)
# MAGIC - **Solution Quality** (max 10 pts)
# MAGIC - **Presentation** (max 5 pts)
# MAGIC - **Total**: max 25 pts
# MAGIC
# MAGIC ### Base Solution
# MAGIC Below is a working end-to-end pipeline. Your Plot Twist card will require you to modify it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Base Pipeline: Heart Disease Prediction System

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, classification_report
from sklearn.preprocessing import StandardScaler
import mlflow
import mlflow.sklearn
import plotly.express as px

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Data Ingestion

# COMMAND ----------

# Load data
df = spark.table("heart_disease").toPandas()

print(f"Dataset: {df.shape[0]} rows, {df.shape[1]} columns")
print(f"Target distribution:\n{df['target'].value_counts()}")
display(spark.table("heart_disease"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Feature Engineering

# COMMAND ----------

# Basic feature engineering
feature_cols = ["age", "sex", "cp", "trestbps", "chol", "fbs", "restecg", 
                "thalach", "exang", "oldpeak", "slope", "ca", "thal"]

X = df[feature_cols]
y = df["target"]

# Scale features
scaler = StandardScaler()
X_scaled = pd.DataFrame(scaler.fit_transform(X), columns=feature_cols)

# Split
X_train, X_test, y_train, y_test = train_test_split(
    X_scaled, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Train: {len(X_train)}, Test: {len(X_test)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Model Training

# COMMAND ----------

TEAM_NAME = "_____"  # Your team name
mlflow.set_experiment(f"/dataops_olympics/{TEAM_NAME}_plot_twist")

with mlflow.start_run(run_name=f"{TEAM_NAME}_base"):
    model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    f1 = f1_score(y_test, y_pred)
    
    mlflow.log_params({"model": "RandomForest", "n_estimators": 100, "max_depth": 10})
    mlflow.log_metric("f1_score", f1)
    mlflow.sklearn.log_model(model, "model")
    
    print(f"Base Model F1: {f1:.4f}")
    print(classification_report(y_test, y_pred))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Visualization & Results

# COMMAND ----------

# Feature importance
importances = model.feature_importances_
fig = px.bar(
    x=feature_cols, y=importances,
    title="Feature Importance - Heart Disease Prediction",
    labels={"x": "Feature", "y": "Importance"}
)
fig.update_layout(template="plotly_white")
fig.show()

# COMMAND ----------

# Prediction distribution
results_df = pd.DataFrame({"actual": y_test.values, "predicted": y_pred})
fig2 = px.histogram(
    results_df.melt(), x="value", color="variable", barmode="group",
    title="Actual vs Predicted Distribution",
    labels={"value": "Class (0=Healthy, 1=Disease)", "count": "Count"}
)
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🎭 PLOT TWIST AREA
# MAGIC
# MAGIC **Draw your card, then modify the solution below!**
# MAGIC
# MAGIC The twist cards are in the `twist_cards.py` notebook.
# MAGIC Your judge will tell you which twist to apply.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Your Twist Adaptation
# MAGIC
# MAGIC **Twist drawn:** _____________________
# MAGIC
# MAGIC **Your approach:** _____________________

# COMMAND ----------

# TODO: Adapt your solution based on the twist card drawn
# Modify the code below to handle your plot twist

# YOUR ADAPTATION CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ### Presentation Notes
# MAGIC
# MAGIC **TODO:** Prepare a 3-minute presentation covering:
# MAGIC 1. What twist did you get?
# MAGIC 2. How did you adapt?
# MAGIC 3. What trade-offs did you make?
# MAGIC 4. What would you do differently with more time?

# COMMAND ----------

# Final results after twist adaptation
print("=" * 60)
print(f"  PLOT TWIST FINALS — TEAM: {TEAM_NAME}")
print("=" * 60)
print(f"  Base F1 Score:    {f1:.4f}")
# print(f"  Adapted F1 Score: {adapted_f1:.4f}")  # Uncomment after adaptation
print("=" * 60)
