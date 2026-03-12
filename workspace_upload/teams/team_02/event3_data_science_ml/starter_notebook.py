# Databricks notebook source
# MAGIC %md
# MAGIC # Event 3: Data Science / ML — Model Accuracy Challenge
# MAGIC
# MAGIC ## Build the Most Accurate Heart Disease Predictor
# MAGIC **Time: 20 minutes** | **Max Points: 40 (+8 bonus)**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Scenario
# MAGIC
# MAGIC > The analytics from Event 2 revealed something alarming: **over half** the patients in the
# MAGIC > dataset have heart disease, and certain age groups are at dramatically higher risk.
# MAGIC >
# MAGIC > The hospital board has approved a new initiative: **build a predictive model** to flag
# MAGIC > patients at risk for heart disease *at the time of intake*, before test results come back.
# MAGIC > Early detection could save lives and reduce costs.
# MAGIC >
# MAGIC > You have the cleaned `heart_silver_correct` table from Event 1 with ~488 patient records.
# MAGIC >
# MAGIC > **Your job:** Train a classifier, track it with MLflow, and register the best model.
# MAGIC > Highest F1 score on the standardized test set wins.
# MAGIC
# MAGIC ### How This Works
# MAGIC
# MAGIC > **This notebook gives you a working baseline model.** Just click **Run All** to get a score.
# MAGIC >
# MAGIC > Then use **Databricks Assistant** (`Cmd+I` / `Ctrl+I`) to iteratively improve:
# MAGIC > - Add better features
# MAGIC > - Try different algorithms
# MAGIC > - Tune hyperparameters
# MAGIC >
# MAGIC > Each improvement → re-run → higher score. **The team with the best F1 wins!**
# MAGIC
# MAGIC ### Scoring Overview
# MAGIC
# MAGIC | Category | Points |
# MAGIC |----------|--------|
# MAGIC | Data Loading + EDA | 5 |
# MAGIC | Feature Engineering (regular table) | 5 |
# MAGIC | Feature Engineering (Feature Store) | **8** |
# MAGIC | Model Training + MLflow Logging | 10 |
# MAGIC | Model Performance (**F1 primary**) | 15 |
# MAGIC | Model Registration | 5 |
# MAGIC | Results Saved to Catalog | 2 |
# MAGIC | **Total (with Feature Store)** | **45** |
# MAGIC | Bonus: SHAP, Ensemble, Cross-Val | up to 8 |
# MAGIC
# MAGIC > **F1 score is the primary metric** (up to 15 pts). If you optimize for accuracy
# MAGIC > instead, you'll receive partial credit (capped at 8 pts). Log `f1_score` in MLflow!
# MAGIC
# MAGIC > **Feature Store gives +3 bonus pts** over a regular table. Both paths work!
# MAGIC
# MAGIC > **Vibe Coding:** Use **Databricks Assistant** (`Cmd+I`) to generate your ML code!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Team Configuration

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_submit

# COMMAND ----------

print(f"Working in: {CATALOG}.default")
print(f"MLflow Experiment: {EXPERIMENT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 0: Prepare Data
# MAGIC
# MAGIC Creates canonical tables in your team catalog from shared reference data.
# MAGIC Everyone starts from the same baseline.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.default.heart_silver_correct AS
    SELECT * FROM {SHARED_CATALOG}.default.heart_disease
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.default.heart_gold_correct AS
    SELECT
        CASE WHEN age < 40 THEN 'Under 40' WHEN age BETWEEN 40 AND 49 THEN '40-49' WHEN age BETWEEN 50 AND 59 THEN '50-59' ELSE '60+' END AS age_group,
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
        COUNT(*) AS patient_count,
        ROUND(AVG(chol), 1) AS avg_cholesterol,
        ROUND(AVG(trestbps), 1) AS avg_blood_pressure,
        ROUND(AVG(thalach), 1) AS avg_max_heart_rate
    FROM {CATALOG}.default.heart_silver_correct
    GROUP BY 1, 2
""")
print(f"  heart_silver_correct: {spark.table(f'{CATALOG}.default.heart_silver_correct').count()} rows")
print(f"  heart_gold_correct:   {spark.table(f'{CATALOG}.default.heart_gold_correct').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Load Data & EDA (5 pts)
# MAGIC
# MAGIC Load your `heart_silver_correct` table and explore it.
# MAGIC
# MAGIC **Feature columns:** `age`, `sex`, `cp`, `trestbps`, `chol`, `fbs`, `restecg`,
# MAGIC `thalach`, `exang`, `oldpeak`, `slope`, `ca`, `thal`
# MAGIC
# MAGIC **Target column:** `target` (1 = heart disease, 0 = healthy)

# COMMAND ----------

import pandas as pd

df = spark.table(f"{CATALOG}.default.heart_silver_correct").toPandas()
print(f"Dataset shape: {df.shape}")
print(f"\nTarget distribution:\n{df['target'].value_counts()}")
print(f"\nBasic statistics:")
df.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Feature Engineering (5 pts regular / 8 pts Feature Store)
# MAGIC
# MAGIC We start with one example feature. **Use Databricks Assistant to add more!**
# MAGIC
# MAGIC **Ask AI:** *"Add 3 more features to improve heart disease prediction:
# MAGIC hr_reserve (220 - age - thalach), high_risk flag (age > 55 AND chol > 240),
# MAGIC and chol_risk (1 if chol > 240)"*

# COMMAND ----------

FEATURE_COLS = [
    "age", "sex", "cp", "trestbps", "chol", "fbs", "restecg",
    "thalach", "exang", "oldpeak", "slope", "ca", "thal",
    # Engineered features — add more here!
    "age_chol",
]

df["age_chol"] = df["age"] * df["chol"]

# ──── ADD MORE FEATURES BELOW (use Cmd+I to ask AI) ────
# Example prompts:
#   "Add hr_reserve = 220 - age - thalach"
#   "Add a high_risk flag for age > 55 and chol > 240"
#   "Create interaction features for the top correlated columns"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Feature Table
# MAGIC
# MAGIC **Option A: Regular Delta Table (5 pts)** — runs by default below.
# MAGIC
# MAGIC **Option B: Feature Store (8 pts)** — for +3 bonus pts, ask AI:
# MAGIC *"Convert this to use FeatureEngineeringClient to create a Feature Store table
# MAGIC with patient_id as primary key"*

# COMMAND ----------

features_sdf = spark.createDataFrame(df[FEATURE_COLS + ["target"]])
features_sdf.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.default.heart_features"
)
print(f"Saved heart_features: {len(FEATURE_COLS)} features")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Train/Test Split
# MAGIC
# MAGIC > **These settings are fixed** so all teams are evaluated on the same test set.
# MAGIC > Do NOT change `test_size`, `random_state`, or `stratify`.

# COMMAND ----------

from sklearn.model_selection import train_test_split

X = df[FEATURE_COLS]
y = df["target"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"Train: {X_train.shape[0]} samples  |  Test: {X_test.shape[0]} samples")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Train Model + MLflow (10 pts)
# MAGIC
# MAGIC ### 4a: Baseline Model (runs out of the box)
# MAGIC
# MAGIC This gives you a working LogisticRegression baseline. Just run it!

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, roc_auc_score, accuracy_score

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(EXPERIMENT_PATH)

with mlflow.start_run(run_name=f"{TEAM_NAME}_baseline"):
    model = LogisticRegression(max_iter=1000, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    f1 = f1_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_proba)
    acc = accuracy_score(y_test, y_pred)

    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("n_features", len(FEATURE_COLS))
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("roc_auc", auc)
    mlflow.log_metric("accuracy", acc)
    mlflow.sklearn.log_model(model, "model")

    print(f"BASELINE — F1: {f1:.4f}  |  AUC: {auc:.4f}  |  Accuracy: {acc:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b: Improve with AI! (this is where you compete)
# MAGIC
# MAGIC Use **Databricks Assistant** (`Cmd+I`) to try better models. Each run is tracked in MLflow.
# MAGIC
# MAGIC **Prompt ideas:**
# MAGIC - *"Train a RandomForestClassifier with 200 trees and log to MLflow"*
# MAGIC - *"Train a GradientBoostingClassifier with tuned hyperparameters and log to MLflow"*
# MAGIC - *"Try XGBoost with learning_rate=0.1 and max_depth=5"*
# MAGIC - *"Build a VotingClassifier ensemble combining RF, GBT, and LR"* (bonus!)
# MAGIC
# MAGIC > **Tip:** Run this cell multiple times with different models.
# MAGIC > The scoring picks your **best F1 score** across all runs.

# COMMAND ----------

# ──── YOUR IMPROVED MODEL HERE ────
# Use Cmd+I (Databricks Assistant) to generate better model code.
# Make sure to:
#   1. Use mlflow.start_run(run_name="...")
#   2. Log params: model_type, n_features
#   3. Log metrics: f1_score, roc_auc, accuracy
#   4. Log the model: mlflow.sklearn.log_model(model, "model")
#
# Example prompt: "Train a GradientBoostingClassifier, log params/metrics/model to MLflow,
#   use EXPERIMENT_PATH, X_train, y_train, X_test, y_test, FEATURE_COLS"


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Register Best Model (5 pts)
# MAGIC
# MAGIC Finds your best MLflow run and registers it in Unity Catalog.

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

exp = client.get_experiment_by_name(EXPERIMENT_PATH)
if exp:
    best_runs = client.search_runs(
        experiment_ids=[exp.experiment_id],
        order_by=["metrics.f1_score DESC"],
        max_results=1,
    )
    if best_runs:
        best_run = best_runs[0]
        best_run_id = best_run.info.run_id
        best_f1 = best_run.data.metrics.get("f1_score", 0)
        print(f"Best run: {best_run_id}  |  F1: {best_f1:.4f}")

        model_name = f"{CATALOG}.default.heart_disease_model"
        try:
            mlflow.register_model(f"runs:/{best_run_id}/model", model_name)
            print(f"Model registered: {model_name}")
        except Exception as e:
            print(f"Registration note: {e}")
            print("This is OK — the organizer can verify via MLflow directly.")
    else:
        print("No runs found. Run Step 4 first!")
else:
    print(f"Experiment not found at {EXPERIMENT_PATH}. Run Step 4 first!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Save & Submit Results (2 pts)
# MAGIC
# MAGIC Computes your final metrics and saves to `event3_results`.
# MAGIC The scoring notebook reads this table automatically.

# COMMAND ----------

from datetime import datetime as _dt
from sklearn.metrics import f1_score, roc_auc_score, accuracy_score, classification_report, confusion_matrix

print("=" * 60)
print(f"  ML CHALLENGE — FINAL SUBMISSION: {TEAM_NAME}")
print("=" * 60)

try:
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    f1 = f1_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_proba)
    acc = accuracy_score(y_test, y_pred)

    print(f"\n  F1 Score:  {f1:.4f}")
    print(f"  AUC-ROC:   {auc:.4f}")
    print(f"  Accuracy:  {acc:.4f}")
    print(f"\n{classification_report(y_test, y_pred)}")

    cm = confusion_matrix(y_test, y_pred)
    print(f"  Confusion Matrix:  TN={cm[0][0]}  FP={cm[0][1]}  FN={cm[1][0]}  TP={cm[1][1]}")

    model_type = type(model).__name__
    n_features = len(FEATURE_COLS)
    _run_id = best_run_id if "best_run_id" in dir() else ""

    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.default.event3_results AS
        SELECT
            '{TEAM_NAME}' AS team,
            '{model_type}' AS model_type,
            CAST({f1} AS DOUBLE) AS f1_score,
            CAST({auc} AS DOUBLE) AS roc_auc,
            CAST({acc} AS DOUBLE) AS accuracy,
            CAST({n_features} AS INT) AS n_features,
            '{_run_id}' AS mlflow_run_id,
            CURRENT_TIMESTAMP() AS submitted_at
    """)

    print(f"\n  Results saved to {CATALOG}.default.event3_results")
    print(f"  >>> YOUR F1 SCORE: {f1:.4f} <<<")
except NameError:
    print("  ERROR: model, X_test, or y_test not defined. Run Steps 3-4 first!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Bonus Challenges (up to +8 pts)
# MAGIC
# MAGIC ### Bonus 1: SHAP Explainability (+3 pts)
# MAGIC
# MAGIC > **Ask AI:** *"Generate SHAP feature importance for my model using TreeExplainer,
# MAGIC > get top 5 features, and save as heart_shap_importance table with columns
# MAGIC > feature and importance"*
# MAGIC >
# MAGIC > You may need `%pip install shap` first.
# MAGIC
# MAGIC ### Bonus 2: Ensemble Model (+3 pts)
# MAGIC
# MAGIC > **Ask AI:** *"Build a VotingClassifier combining RandomForest, GradientBoosting,
# MAGIC > and LogisticRegression with soft voting. Log it to MLflow with model_type='VotingClassifier'"*
# MAGIC
# MAGIC ### Bonus 3: Cross-Validation Report (+2 pts)
# MAGIC
# MAGIC > **Ask AI:** *"Run 5-fold stratified cross-validation on my model, log mean and std F1,
# MAGIC > and save per-fold results as heart_cv_results table with columns fold and f1_score"*

# COMMAND ----------

# ──── BONUS CODE HERE ────
# Use Cmd+I to generate bonus challenge code


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## SUBMIT YOUR WORK
# MAGIC
# MAGIC **Run this cell when you're done!** It records your submission timestamp for the live scoreboard.

# COMMAND ----------

submit("event3")
