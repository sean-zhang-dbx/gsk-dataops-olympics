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
# MAGIC > You have the cleaned `heart_silver` table from Event 1 with ~488 patient records.
# MAGIC >
# MAGIC > **Your job:** Train a classifier, track it with MLflow, and register the best model.
# MAGIC > Highest F1 score on the standardized test set wins.
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

TEAM_NAME = "team_XX"  # <-- CHANGE THIS (e.g., "team_01")
CATALOG = TEAM_NAME

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")
print(f"Working in: {CATALOG}.default")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Load Data & EDA (5 pts)
# MAGIC
# MAGIC > Load your `heart_silver` table into a Pandas DataFrame.
# MAGIC > Show the shape, target distribution, and basic statistics.
# MAGIC >
# MAGIC > **Feature columns:** `age`, `sex`, `cp`, `trestbps`, `chol`, `fbs`, `restecg`,
# MAGIC > `thalach`, `exang`, `oldpeak`, `slope`, `ca`, `thal`
# MAGIC >
# MAGIC > **Target column:** `target` (1 = heart disease, 0 = healthy)

# COMMAND ----------

# YOUR CODE HERE — load data into pandas
# Prompt: "Load heart_silver from Spark into pandas, show shape, describe, and target distribution"


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Feature Engineering
# MAGIC
# MAGIC Create **at least 2 new features** that might improve prediction.
# MAGIC
# MAGIC **Feature ideas:**
# MAGIC - `age_chol` = age * chol (interaction)
# MAGIC - `hr_reserve` = 220 - age - thalach (heart rate reserve)
# MAGIC - `high_risk` = 1 if age > 55 AND chol > 240 (composite flag)
# MAGIC - `bp_category` = binned blood pressure (low/normal/high/very_high)
# MAGIC - `chol_risk` = 1 if chol > 240, 0 otherwise
# MAGIC
# MAGIC ### Choose Your Path:
# MAGIC
# MAGIC | Option | Points | What to do |
# MAGIC |--------|--------|------------|
# MAGIC | **Option A: Regular Table** | 5 pts | Add features to your DataFrame and save as `heart_features` Delta table |
# MAGIC | **Option B: Feature Store** | **8 pts** | Use `databricks.feature_engineering` to create a Feature Store table |
# MAGIC
# MAGIC > Feature Store earns **+3 bonus pts** because it enables feature reuse, lineage tracking,
# MAGIC > and online serving — production best practices.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option A: Regular Delta Table (5 pts)
# MAGIC
# MAGIC > Add features to your pandas DataFrame. Then save the feature table:
# MAGIC > ```python
# MAGIC > features_sdf = spark.createDataFrame(df[FEATURE_COLS + ["patient_id", "target"]])
# MAGIC > features_sdf.write.format("delta").mode("overwrite").saveAsTable(
# MAGIC >     f"{CATALOG}.default.heart_features"
# MAGIC > )
# MAGIC > ```

# COMMAND ----------

# YOUR CODE HERE — feature engineering (Option A: regular table)
# Prompt: "Add 3 new features: hr_reserve, high_risk flag, and chol_risk category"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Option B: Feature Store (8 pts)
# MAGIC
# MAGIC > Use Databricks Feature Engineering to create a proper feature table.
# MAGIC > This gives you lineage tracking, feature reuse, and online serving capabilities.
# MAGIC >
# MAGIC > ```python
# MAGIC > from databricks.feature_engineering import FeatureEngineeringClient
# MAGIC > fe = FeatureEngineeringClient()
# MAGIC >
# MAGIC > # Create a Spark DataFrame with patient_id as the primary key
# MAGIC > features_sdf = spark.createDataFrame(df[FEATURE_COLS + ["patient_id"]])
# MAGIC >
# MAGIC > # Create the feature table (patient_id is the lookup key)
# MAGIC > fe.create_table(
# MAGIC >     name=f"{CATALOG}.default.heart_features",
# MAGIC >     primary_keys=["patient_id"],
# MAGIC >     df=features_sdf,
# MAGIC >     description="Heart disease patient features for ML prediction"
# MAGIC > )
# MAGIC > ```
# MAGIC >
# MAGIC > *Docs: [Feature Engineering](https://docs.databricks.com/en/machine-learning/feature-store/index.html)*

# COMMAND ----------

# YOUR CODE HERE — feature engineering (Option B: Feature Store)
# Prompt: "Create a Feature Store table with patient features using FeatureEngineeringClient"


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Train/Test Split
# MAGIC
# MAGIC > **MUST use these exact settings** so all teams are evaluated on the same test set:
# MAGIC > - `test_size=0.2`
# MAGIC > - `random_state=42`
# MAGIC > - `stratify=y`
# MAGIC >
# MAGIC > Define `FEATURE_COLS` with all the columns you want to use (original + engineered).

# COMMAND ----------

# YOUR CODE HERE — split data
# IMPORTANT: random_state=42, test_size=0.2, stratify=y


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Train Model + MLflow (10 pts)
# MAGIC
# MAGIC > Train a classification model and log to MLflow:
# MAGIC >
# MAGIC > 1. Set experiment: `mlflow.set_experiment(f"/Users/<your_email>/{TEAM_NAME}_heart_ml")`
# MAGIC > 2. Start a run: `with mlflow.start_run(run_name="my_model"):`
# MAGIC > 3. Log parameters: model type, hyperparameters, feature count
# MAGIC > 4. Log metrics: `f1_score`, `roc_auc_score`, `accuracy_score`
# MAGIC > 5. Log the model: `mlflow.sklearn.log_model(model, "model", signature=signature)`
# MAGIC >
# MAGIC > **You can run this cell multiple times** — try different algorithms!
# MAGIC > - `RandomForestClassifier`
# MAGIC > - `GradientBoostingClassifier`
# MAGIC > - `LogisticRegression`
# MAGIC > - `VotingClassifier` (ensemble)
# MAGIC >
# MAGIC > The **best F1 score** is what gets scored.

# COMMAND ----------

# YOUR CODE HERE — train + log to MLflow
# Prompt: "Train a GradientBoostingClassifier, log params/metrics/model to MLflow"


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Register Best Model (5 pts)
# MAGIC
# MAGIC > Register your best model in MLflow Model Registry:
# MAGIC >
# MAGIC > ```python
# MAGIC > model_name = f"{CATALOG}.default.heart_disease_model"
# MAGIC > mlflow.register_model(f"runs:/{best_run_id}/model", model_name)
# MAGIC > ```
# MAGIC >
# MAGIC > If Unity Catalog registration fails due to permissions, that's OK.
# MAGIC > Just make sure the model is logged in MLflow — the organizer can verify.

# COMMAND ----------

# YOUR CODE HERE — register model


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Save & Submit Results (2 pts)
# MAGIC
# MAGIC > Save your results to `{CATALOG}.default.event3_results`. The scoring notebook
# MAGIC > reads this table automatically — **this is how we pick up your score for the dashboard**.
# MAGIC >
# MAGIC > This cell computes your metrics, displays them, and saves to your catalog.

# COMMAND ----------

from datetime import datetime as _dt

print("=" * 60)
print(f"  ML CHALLENGE — FINAL SUBMISSION: {TEAM_NAME}")
print("=" * 60)

try:
    from sklearn.metrics import f1_score, roc_auc_score, accuracy_score, classification_report, confusion_matrix
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

    # Save results to team catalog
    model_type = type(model).__name__
    n_features = len(FEATURE_COLS) if "FEATURE_COLS" in dir() else X_test.shape[1]
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
# MAGIC ## Bonus Challenges
# MAGIC
# MAGIC ### Bonus 1: SHAP Explainability (+3 pts)
# MAGIC
# MAGIC > Generate SHAP feature importance values and save the top-5 features.
# MAGIC > Save as a table called `heart_shap_importance` (columns: `feature`, `importance`).
# MAGIC >
# MAGIC > **Note:** You may need to install `shap` first:
# MAGIC > ```python
# MAGIC > %pip install shap
# MAGIC > ```
# MAGIC >
# MAGIC > Then:
# MAGIC > ```python
# MAGIC > import shap
# MAGIC > explainer = shap.TreeExplainer(model)
# MAGIC > shap_values = explainer.shap_values(X_test)
# MAGIC >
# MAGIC > # Get feature importances from mean absolute SHAP values
# MAGIC > import numpy as np
# MAGIC > shap_imp = np.abs(shap_values).mean(axis=0) if len(shap_values.shape) == 2 else np.abs(shap_values[1]).mean(axis=0)
# MAGIC > top5 = sorted(zip(FEATURE_COLS, shap_imp), key=lambda x: -x[1])[:5]
# MAGIC > shap_df = spark.createDataFrame(top5, ["feature", "importance"])
# MAGIC > shap_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_shap_importance")
# MAGIC > ```
# MAGIC
# MAGIC ### Bonus 2: Ensemble Model (+3 pts)
# MAGIC
# MAGIC > Build a `VotingClassifier` combining at least 3 different algorithms.
# MAGIC > Log it as a separate MLflow run. Compare F1 to your single-model run.
# MAGIC >
# MAGIC > ```python
# MAGIC > from sklearn.ensemble import VotingClassifier, RandomForestClassifier, GradientBoostingClassifier
# MAGIC > from sklearn.linear_model import LogisticRegression
# MAGIC > ensemble = VotingClassifier(estimators=[
# MAGIC >     ('rf', RandomForestClassifier(n_estimators=100, random_state=42)),
# MAGIC >     ('gb', GradientBoostingClassifier(n_estimators=100, random_state=42)),
# MAGIC >     ('lr', LogisticRegression(max_iter=1000, random_state=42)),
# MAGIC > ], voting='soft')
# MAGIC > ```
# MAGIC
# MAGIC ### Bonus 3: Cross-Validation Report (+2 pts)
# MAGIC
# MAGIC > Run 5-fold stratified cross-validation and log the mean and std of F1 scores.
# MAGIC > Save as a table `heart_cv_results` with columns: fold, f1_score.
# MAGIC >
# MAGIC > ```python
# MAGIC > from sklearn.model_selection import cross_val_score
# MAGIC > cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='f1')
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## SUBMIT YOUR WORK
# MAGIC
# MAGIC **Run this cell when you're done!** It records your submission timestamp for the live scoreboard.

# COMMAND ----------

from datetime import datetime as _dt

_event_name = "Event 3: Data Science"
_submit_ts = _dt.now()

spark.sql(f"""
    INSERT INTO dataops_olympics.default.event_submissions
    VALUES ('{TEAM_NAME}', '{_event_name}', '{_submit_ts}', NULL)
""")

print("=" * 60)
print(f"  SUBMITTED! {TEAM_NAME} — {_event_name}")
print(f"  Timestamp: {_submit_ts.strftime('%H:%M:%S.%f')}")
print(f"  Signal the judges that you are DONE!")
print("=" * 60)
