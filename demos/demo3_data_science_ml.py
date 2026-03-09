# Databricks notebook source
# MAGIC %md
# MAGIC # Lightning Talk 3: Data Science — Feature Store & MLflow
# MAGIC
# MAGIC **Duration: 15 minutes** | Instructor-led live demo
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Agenda
# MAGIC | Time | Section | What You'll See |
# MAGIC |------|---------|----------------|
# MAGIC | 0:00 | **The Problem** | Predicting which patients get readmitted |
# MAGIC | 1:00 | **Explore** | Understand the data in 60 seconds |
# MAGIC | 3:00 | **Feature Store** | Why production ML needs governed features |
# MAGIC | 6:00 | **Train** | 3 models tracked side-by-side in MLflow |
# MAGIC | 9:00 | **Wow Moment** | MLflow Experiment UI — compare runs visually |
# MAGIC | 11:00 | **Register** | Model Registry → one click to production |
# MAGIC | 13:00 | **Feature Importance** | What the model actually learned |
# MAGIC | 14:00 | **Your Turn** | Preview of the practice notebook |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Problem
# MAGIC
# MAGIC Hospital readmissions cost the healthcare system billions every year. If we can predict
# MAGIC which patients are likely to come back within 30 days, we can intervene early — better care,
# MAGIC lower costs. That's what we're building: a readmission risk model, tracked and governed end-to-end.

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
from sklearn.metrics import f1_score, roc_auc_score
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
# MAGIC 768 patients, 8 clinical features, one target: will they be readmitted?
# MAGIC The key question — how do we turn raw columns into *governed, reusable* features?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering with Feature Store
# MAGIC
# MAGIC In most ML projects, feature engineering happens in a messy notebook that nobody can reproduce.
# MAGIC Databricks Feature Store changes that — features become **governed, versioned, and reusable**
# MAGIC across models and teams. Think of it as Unity Catalog for your features.

# COMMAND ----------

df["glucose_bmi"] = df["glucose"] * df["bmi"]
df["age_glucose"] = df["age"] * df["glucose"]
df["high_risk_flag"] = ((df["glucose"] > 140) & (df["bmi"] > 30)).astype(int)

feature_cols = [
    "pregnancies", "glucose", "blood_pressure", "skin_thickness",
    "insulin", "bmi", "diabetes_pedigree", "age",
    "glucose_bmi", "age_glucose", "high_risk_flag"
]

print(f"Engineered {len(feature_cols)} features (3 new: glucose_bmi, age_glucose, high_risk_flag)")

# COMMAND ----------

# MAGIC %md
# MAGIC We created 3 new features: a glucose-BMI interaction, an age-glucose interaction,
# MAGIC and a binary flag for high-risk patients. Now let's save these to the **Feature Store**
# MAGIC so they're governed and reusable.

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

feature_df = spark.createDataFrame(
    df[["pregnancies", "glucose", "blood_pressure", "skin_thickness",
        "insulin", "bmi", "diabetes_pedigree", "age",
        "glucose_bmi", "age_glucose", "high_risk_flag"]].assign(
        patient_id=range(len(df))
    )
)

try:
    spark.sql("DROP TABLE IF EXISTS dataops_olympics.default.demo_patient_features")
except Exception:
    pass

fe.create_table(
    name="dataops_olympics.default.demo_patient_features",
    primary_keys=["patient_id"],
    df=feature_df,
    description="Engineered features for diabetes readmission prediction — demo"
)
print("Feature Store table created: demo_patient_features")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE dataops_olympics.default.demo_patient_features

# COMMAND ----------

# MAGIC %md
# MAGIC Our features are now a governed Unity Catalog table. Any team can discover, reuse,
# MAGIC and build on these features. Feature Store also tracks **lineage** — you can see
# MAGIC which models use which features.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Train with MLflow Tracking
# MAGIC
# MAGIC Now the fun part. MLflow tracks **everything** — parameters, metrics, the model itself.
# MAGIC Let's train three models in three runs and see why this changes how you do ML.

# COMMAND ----------

X = df[feature_cols]
y = df["readmission_risk"]
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"Train: {len(X_train)} | Test: {len(X_test)}")

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
    mlflow.log_params({"model": "LogisticRegression", "max_iter": 2000, "features": len(feature_cols)})
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
    mlflow.log_params({"model": "RandomForest", "n_estimators": 200, "max_depth": 8, "features": len(feature_cols)})
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
    mlflow.log_params({"model": "GradientBoosting", "n_estimators": 200, "max_depth": 5,
                        "learning_rate": 0.1, "features": len(feature_cols)})
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
print(f"  Gradient Boosting:    F1 = {f1_gb:.4f}  ← winner!")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Wow Moment — MLflow Experiment UI
# MAGIC
# MAGIC Let's check out the MLflow UI:
# MAGIC 1. Click **Experiments** in the left sidebar
# MAGIC 2. Open `demo_ml_lightning_talk`
# MAGIC 3. See the 3 runs side by side — each row is a run
# MAGIC 4. Click **Compare** — metrics chart shows F1 scores
# MAGIC 5. Click into the best run — logged params, metrics, model artifact
# MAGIC
# MAGIC Every experiment is tracked forever. You can compare models, reproduce results,
# MAGIC and go back to any previous run. No more `model_final_v2_REAL.pkl` — this is how production ML teams work.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Register the Best Model
# MAGIC
# MAGIC The best model gets promoted to the **Model Registry**. From there, it's one click to production.
# MAGIC The registry tracks versions, lineage back to the training data, and who approved the deployment.

# COMMAND ----------

model_uri = f"runs:/{run_id}/model"
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
# MAGIC The model is now in the Unity Catalog Model Registry. In the competition,
# MAGIC your model will be scored automatically — the team with the highest F1 score wins!

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
# MAGIC Glucose and the glucose-BMI interaction are the top predictors. That validates our feature
# MAGIC engineering — the interaction term we created is more important than most raw features.
# MAGIC This is why Feature Store matters: once you discover a powerful feature, everyone on the team can use it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dataops_olympics.default.demo_patient_features;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Turn!
# MAGIC
# MAGIC In the practice notebook, you'll train ONE model with MLflow — just fill in 4 blanks.
# MAGIC Then in the competition, you'll have two options: save features as a regular table (5 pts)
# MAGIC or use Feature Store (8 pts). Go for Feature Store — the code is almost the same, but the points are better!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Feature Store** = governed, versioned, reusable features in Unity Catalog
# MAGIC 2. **MLflow tracks everything** — parameters, metrics, models, artifacts
# MAGIC 3. **Feature engineering** is where you win (interaction terms, domain flags)
# MAGIC 4. **Compare models** in the MLflow Experiment UI — no spreadsheets needed
# MAGIC 5. **Model Registry** promotes models to production with lineage tracking
# MAGIC 6. **F1 Score** is the competition metric — balance precision and recall
# MAGIC 7. **Use the Databricks Assistant** (`Cmd+I`) to suggest features and tune hyperparameters
