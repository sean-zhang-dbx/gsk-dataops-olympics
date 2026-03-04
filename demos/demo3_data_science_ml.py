# Databricks notebook source
# MAGIC %md
# MAGIC # Lightning Talk 3: Machine Learning with MLflow
# MAGIC
# MAGIC **Duration: 15 minutes** | Instructor-led live demo
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Agenda
# MAGIC | Time | Section | What You'll See |
# MAGIC |------|---------|----------------|
# MAGIC | 0:00 | **The Problem** | Predicting patient readmission |
# MAGIC | 1:00 | **Explore** | Understand the data in 60 seconds |
# MAGIC | 3:00 | **Features** | Feature engineering that matters |
# MAGIC | 5:00 | **Train** | Model training with MLflow tracking |
# MAGIC | 9:00 | **Wow Moment** | MLflow experiment UI — compare runs |
# MAGIC | 11:00 | **Register** | Model Registry for production |
# MAGIC | 13:00 | **Feature Importance** | What the model learned |
# MAGIC | 14:00 | **Your Turn** | Preview of the practice notebook |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Problem
# MAGIC
# MAGIC > **Say this:** "Hospital readmissions cost the NHS and insurance companies billions.
# MAGIC > If we can predict which diabetes patients are likely to be readmitted, we can
# MAGIC > intervene early. That's what we're building — a readmission risk predictor."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Explore the Data (60 seconds)

# COMMAND ----------

spark.sql("USE CATALOG dataops_olympics")
spark.sql("USE SCHEMA default")

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, classification_report, roc_auc_score
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
df = spark.table("diabetes_readmission").toPandas()
print(f"Dataset: {df.shape[0]} patients × {df.shape[1]} features")
print(f"\nReadmission risk distribution:")
print(df["readmission_risk"].value_counts())
print(f"\nClass balance: {df['readmission_risk'].mean():.1%} positive")

# COMMAND ----------

display(spark.table("diabetes_readmission"))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "768 patients, 8 clinical features, one target: will they be readmitted?
# MAGIC > The classes are reasonably balanced. Now let's engineer some features."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering
# MAGIC
# MAGIC > **Say this:** "Raw features are rarely enough. Domain knowledge is your edge.
# MAGIC > A doctor would tell you: high glucose AND high BMI together is much worse
# MAGIC > than either alone. Let's encode that."

# COMMAND ----------

df["glucose_bmi"] = df["glucose"] * df["bmi"]
df["age_glucose"] = df["age"] * df["glucose"]
df["high_risk_flag"] = ((df["glucose"] > 140) & (df["bmi"] > 30)).astype(int)

feature_cols = [
    "pregnancies", "glucose", "blood_pressure", "skin_thickness",
    "insulin", "bmi", "diabetes_pedigree", "age",
    "glucose_bmi", "age_glucose", "high_risk_flag"
]

X = df[feature_cols]
y = df["readmission_risk"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"Train: {len(X_train)} | Test: {len(X_test)}")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "Three new features: an interaction term, an age-glucose interaction,
# MAGIC > and a binary flag for high-risk patients. This is where data science meets domain expertise."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Train with MLflow Tracking
# MAGIC
# MAGIC > **Say this:** "Here's where Databricks shines. MLflow tracks EVERYTHING automatically —
# MAGIC > your parameters, metrics, the model itself, even the code version. Let me show you
# MAGIC > three models in three runs."

# COMMAND ----------

_user = spark.sql("SELECT current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{_user}/demo_ml_lightning_talk")

# COMMAND ----------

# Run 1: Logistic Regression (baseline)
with mlflow.start_run(run_name="logistic_regression"):
    model_lr = LogisticRegression(max_iter=2000, random_state=42)
    model_lr.fit(X_train, y_train)
    y_pred = model_lr.predict(X_test)
    f1_lr = f1_score(y_test, y_pred)
    sig = infer_signature(X_train, y_pred)
    mlflow.log_params({"model": "LogisticRegression", "max_iter": 2000})
    mlflow.log_metric("f1_score", f1_lr)
    mlflow.sklearn.log_model(model_lr, "model", signature=sig, input_example=X_test[:3])
    print(f"Logistic Regression — F1: {f1_lr:.4f}")

# COMMAND ----------

# Run 2: Random Forest
with mlflow.start_run(run_name="random_forest"):
    model_rf = RandomForestClassifier(n_estimators=200, max_depth=8, random_state=42)
    model_rf.fit(X_train, y_train)
    y_pred = model_rf.predict(X_test)
    f1_rf = f1_score(y_test, y_pred)
    sig = infer_signature(X_train, y_pred)
    mlflow.log_params({"model": "RandomForest", "n_estimators": 200, "max_depth": 8})
    mlflow.log_metric("f1_score", f1_rf)
    mlflow.sklearn.log_model(model_rf, "model", signature=sig, input_example=X_test[:3])
    print(f"Random Forest — F1: {f1_rf:.4f}")

# COMMAND ----------

# Run 3: Gradient Boosting
with mlflow.start_run(run_name="gradient_boosting"):
    model_gb = GradientBoostingClassifier(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42)
    model_gb.fit(X_train, y_train)
    y_pred = model_gb.predict(X_test)
    y_proba = model_gb.predict_proba(X_test)[:, 1]
    f1_gb = f1_score(y_test, y_pred)
    auc_gb = roc_auc_score(y_test, y_proba)
    sig = infer_signature(X_train, y_pred)
    mlflow.log_params({"model": "GradientBoosting", "n_estimators": 200, "max_depth": 5, "learning_rate": 0.1})
    mlflow.log_metric("f1_score", f1_gb)
    mlflow.log_metric("auc_roc", auc_gb)
    mlflow.sklearn.log_model(model_gb, "model", signature=sig, input_example=X_test[:3])
    run_id = mlflow.active_run().info.run_id
    print(f"Gradient Boosting — F1: {f1_gb:.4f} | AUC: {auc_gb:.4f}")

# COMMAND ----------

print("=" * 50)
print("  MODEL COMPARISON")
print("=" * 50)
print(f"  Logistic Regression:  F1 = {f1_lr:.4f}")
print(f"  Random Forest:        F1 = {f1_rf:.4f}")
print(f"  Gradient Boosting:    F1 = {f1_gb:.4f}  (winner!)")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Wow Moment — MLflow Experiment UI
# MAGIC
# MAGIC > **LIVE UI DEMO (2 min):**
# MAGIC > 1. Click **Experiments** in the left sidebar
# MAGIC > 2. Open `demo_ml_lightning_talk`
# MAGIC > 3. Show the 3 runs side by side
# MAGIC > 4. Click **Compare** — show the metrics chart
# MAGIC > 5. Click into the best run — show logged params, metrics, model artifact
# MAGIC >
# MAGIC > **Say this:** "Every experiment you run is tracked forever. You can compare models,
# MAGIC > reproduce results, and go back to any previous run. No more 'model_final_v2_REAL.pkl'."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Register the Best Model

# COMMAND ----------

model_uri = f"runs:/{run_id}/model"

# Auto-detect a catalog that supports model registration (Unity Catalog requires 3-level names)
_model_name = "demo_readmission_model"
try:
    catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
    uc_catalog = next((c for c in catalogs if "sandbox" in c.lower()), None)
    if uc_catalog:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_catalog}.dataops_olympics")
        _model_name = f"{uc_catalog}.dataops_olympics.demo_readmission_model"
except Exception:
    pass

try:
    registered = mlflow.register_model(model_uri, _model_name)
    print(f"Registered: {registered.name} v{registered.version}")
except Exception as e:
    print(f"Model registration note: {str(e)[:120]}")
    print("The model is tracked in MLflow experiments — registration is optional for this demo.")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "The model is now in the Model Registry. In a real project, this is
# MAGIC > how you promote models from 'experiment' to 'staging' to 'production'. One click."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. What Did the Model Learn?

# COMMAND ----------

import pandas as _pd
importances = model_gb.feature_importances_
imp_df = _pd.DataFrame({"Feature": feature_cols, "Importance": importances}).sort_values("Importance", ascending=False)
display(spark.createDataFrame(imp_df))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "Glucose and the glucose-BMI interaction are the top predictors.
# MAGIC > That validates our feature engineering — the interaction term we created
# MAGIC > is more important than most raw features. This is why feature engineering wins competitions."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Turn!
# MAGIC
# MAGIC > **Say this:** "In the practice notebook, you'll train ONE model with MLflow.
# MAGIC > Just fill in 4 blanks. Then in the competition, you'll compete for the highest
# MAGIC > F1 score. Any sklearn model is fair game. Let's go!"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **MLflow tracks everything** — parameters, metrics, models, artifacts
# MAGIC 2. **Feature engineering** is where you differentiate (interaction features, domain flags)
# MAGIC 3. **Compare models** in the MLflow UI — no spreadsheets needed
# MAGIC 4. **Model Registry** promotes models to production with one click
# MAGIC 5. **F1 Score** is the competition metric — balance precision and recall
# MAGIC 6. **Use the Databricks Assistant** to suggest features, tune hyperparameters, and debug models
