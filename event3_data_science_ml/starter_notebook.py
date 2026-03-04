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
# MAGIC | Feature Engineering | 5 |
# MAGIC | Model Training + MLflow Logging | 10 |
# MAGIC | Model Performance (F1-based) | 15 |
# MAGIC | Model Registration | 5 |
# MAGIC | **Total** | **40** |
# MAGIC | Bonus: SHAP, Ensemble, Cross-Val | up to 8 |
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
# MAGIC ## Step 2: Feature Engineering (5 pts)
# MAGIC
# MAGIC > Create **at least 2 new features** that might improve prediction.
# MAGIC >
# MAGIC > Ideas:
# MAGIC > - `age_chol` = age * chol (interaction)
# MAGIC > - `hr_reserve` = 220 - age - thalach (heart rate reserve)
# MAGIC > - `high_risk` = 1 if age > 55 AND chol > 240 (composite flag)
# MAGIC > - `bp_category` = binned blood pressure (low/normal/high/very_high)
# MAGIC > - `chol_risk` = 1 if chol > 240, 0 otherwise
# MAGIC >
# MAGIC > More features = more points (up to 5). Quality matters.

# COMMAND ----------

# YOUR CODE HERE — feature engineering
# Prompt: "Add 3 new features: hr_reserve, high_risk flag, and chol_risk category"


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
# MAGIC ## Final Submission
# MAGIC
# MAGIC Run this to display your final score.

# COMMAND ----------

print("=" * 60)
print(f"  ML CHALLENGE — FINAL SUBMISSION: {TEAM_NAME}")
print("=" * 60)

try:
    from sklearn.metrics import f1_score, roc_auc_score, classification_report, confusion_matrix
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    f1 = f1_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_proba)

    print(f"\n  F1 Score:  {f1:.4f}")
    print(f"  AUC-ROC:   {auc:.4f}")
    print(f"\n{classification_report(y_test, y_pred)}")

    cm = confusion_matrix(y_test, y_pred)
    print(f"  Confusion Matrix:  TN={cm[0][0]}  FP={cm[0][1]}  FN={cm[1][0]}  TP={cm[1][1]}")
    print(f"\n  >>> REPORT THIS F1 SCORE: {f1:.4f} <<<")
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
