# Databricks notebook source
# MAGIC %md
# MAGIC # Event 3: Data Science / ML — Model Accuracy Challenge
# MAGIC
# MAGIC ## Challenge: Build the Most Accurate Predictive Model
# MAGIC **Build Time: ~20 minutes**
# MAGIC
# MAGIC ### Objective
# MAGIC Build a classification model to predict **heart disease** using the **cleaned Silver table
# MAGIC from Event 1** and register it with MLflow.
# MAGIC
# MAGIC ### How You Win
# MAGIC **Highest F1 score on the held-out test set wins Gold!**
# MAGIC
# MAGIC ### Data — From YOUR Event 1 Pipeline!
# MAGIC
# MAGIC You'll use your **Silver table** from Event 1:
# MAGIC - `{TEAM_NAME}_heart_silver` — cleaned patient data with validated age, BP, cholesterol
# MAGIC - Target column: `target` (1 = heart disease, 0 = healthy)
# MAGIC - Features: age, sex, cp, trestbps, chol, fbs, restecg, thalach, exang, oldpeak, slope, ca, thal
# MAGIC
# MAGIC If your team used the SDP path, use `heart_silver`.
# MAGIC
# MAGIC ### Rules
# MAGIC 1. Must use your Silver table from Event 1
# MAGIC 2. Must use MLflow to track your experiment
# MAGIC 3. Must evaluate on the standard test split (80/20, random_state=42)
# MAGIC 4. Must register your best model in MLflow Model Registry
# MAGIC 5. Any sklearn or Spark ML algorithm is allowed
# MAGIC 6. Feature engineering is encouraged!
# MAGIC
# MAGIC > **Vibe Coding:** Use the Databricks Assistant (`Cmd+I`) to iterate on models faster!

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dataops_olympics;
# MAGIC USE SCHEMA default;

# COMMAND ----------

TEAM_NAME = "team_XX"  # <-- CHANGE THIS to your team name

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Load Your Event 1 Silver Table
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Load your `{TEAM_NAME}_heart_silver` table (or `heart_silver` for SDP path)
# MAGIC > into a Pandas DataFrame. Print the shape, column names, and the distribution
# MAGIC > of the `target` column (what % have heart disease?).
# MAGIC >
# MAGIC > This is the cleaned, deduplicated data from your Event 1 pipeline.

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Explore & Feature Engineer
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Explore the data to understand distributions, correlations, and potential issues.
# MAGIC > Then create **new features** that might improve prediction accuracy.
# MAGIC >
# MAGIC > Ideas:
# MAGIC > - Interaction features (e.g., `age * chol`, `trestbps * thalach`)
# MAGIC > - Binned features (e.g., age groups, cholesterol risk categories)
# MAGIC > - Ratio features (e.g., `oldpeak / thalach`)
# MAGIC > - High-risk flags (e.g., age > 55 AND chol > 250)
# MAGIC >
# MAGIC > This is where you differentiate from other teams!

# COMMAND ----------

# YOUR CODE HERE — explore the data


# COMMAND ----------

# YOUR CODE HERE — feature engineering


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Train/Test Split
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Define your feature columns and target (`target`).
# MAGIC > Split 80/20 using `random_state=42` and `stratify=y` so all teams
# MAGIC > are evaluated on the same test set.
# MAGIC >
# MAGIC > **Important:** If you added new features, include them in your feature list!

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Train Your Model with MLflow
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Train a classification model and log everything to MLflow:
# MAGIC > - Set the experiment to `/Users/{your_email}/{TEAM_NAME}_ml_challenge`
# MAGIC > - Log model type and hyperparameters
# MAGIC > - Log F1 score and AUC-ROC metrics
# MAGIC > - Log the model with an `infer_signature` (required for UC Model Registry)
# MAGIC >
# MAGIC > You can run this cell multiple times with different models/hyperparameters.
# MAGIC > Try RandomForest, GradientBoosting, LogisticRegression, or an ensemble.
# MAGIC >
# MAGIC > Print the F1 score, AUC-ROC, and classification report.

# COMMAND ----------

# YOUR CODE HERE — train and log with MLflow
# You can run this cell multiple times to try different approaches!


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Register Your Best Model
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Register your best model in the MLflow Model Registry.
# MAGIC > Find the best run from your experiment and call `mlflow.register_model()`.
# MAGIC >
# MAGIC > Note: Unity Catalog requires 3-level model names (catalog.schema.model_name).
# MAGIC > If registration fails due to permissions, that's OK — the model is still tracked.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Final Submission
# MAGIC
# MAGIC Run this cell to display your final score. Report the F1 score to the judges!

# COMMAND ----------

print("=" * 60)
print(f"  ML CHALLENGE — FINAL SUBMISSION: {TEAM_NAME}")
print("=" * 60)

try:
    y_final_pred = model.predict(X_test)
    y_final_proba = model.predict_proba(X_test)[:, 1]

    from sklearn.metrics import f1_score, roc_auc_score, classification_report, confusion_matrix
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
except NameError:
    print("  ERROR: 'model', 'X_test', or 'y_test' not defined.")
    print("  Make sure you ran Steps 3 and 4 first!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stretch Goals (Extra Credit)
# MAGIC
# MAGIC Finished early? Ask the Databricks Assistant to help you with these:
# MAGIC
# MAGIC 1. **SHAP Explainability** — "Add SHAP feature importance plots and log the chart to MLflow"
# MAGIC 2. **Ensemble Model** — "Create a VotingClassifier combining RandomForest, GradientBoosting, and LogisticRegression"
# MAGIC 3. **Cross-Validation** — "Replace the single split with 5-fold stratified cross-validation"
# MAGIC 4. **Hyperparameter Tuning** — "Use GridSearchCV to find the best RandomForest hyperparameters"
# MAGIC 5. **Threshold Tuning** — "Plot the precision-recall curve and find the optimal classification threshold"
