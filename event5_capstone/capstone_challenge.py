# Databricks notebook source
# MAGIC %md
# MAGIC # Event 5: Capstone — End-to-End Vibe Coding Build
# MAGIC
# MAGIC ## The Ultimate Challenge
# MAGIC **Time: 30 minutes** | **Scoring: Judges rate completeness, quality, presentation**
# MAGIC
# MAGIC ### The Scenario
# MAGIC A hospital administrator just walked in and said:
# MAGIC
# MAGIC > *"We have drug review data, clinical notes, AND that heart disease pipeline
# MAGIC > your team built earlier today. I need a complete analytics solution by end of day:
# MAGIC > extend your pipeline to include drug reviews, build a dashboard I can show the board,
# MAGIC > and some kind of intelligence layer — a predictive model, an AI assistant,
# MAGIC > or an app that helps our clinical staff. Can you do it?"*
# MAGIC
# MAGIC **You said yes. You have 30 minutes. The Databricks Assistant is your co-pilot.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What You Must Build
# MAGIC
# MAGIC | Component | Requirement | Points |
# MAGIC |-----------|-------------|--------|
# MAGIC | **1. Data Pipeline** | Extend your Event 1 pipeline with drug_reviews + clinical_notes | 8 pts |
# MAGIC | **2. Dashboard or Visualizations** | AI/BI Dashboard OR 4+ notebook visualizations | 7 pts |
# MAGIC | **3. Intelligence Layer** (pick one) | ML model OR GenAI agent OR interactive app | 7 pts |
# MAGIC | **Presentation** | 3-min demo to judges explaining what you built | 3 pts |
# MAGIC | **Total** | | **25 pts** |
# MAGIC
# MAGIC ### Your Data
# MAGIC - Your Event 1 tables: `{TEAM_NAME}_heart_silver`, `{TEAM_NAME}_heart_gold`
# MAGIC - `drug_reviews` table — 1,000 drug reviews with ratings, conditions, text
# MAGIC - `clinical_notes` table — 20 clinical notes across hospital departments
# MAGIC
# MAGIC ### Rules
# MAGIC - **Vibe coding encouraged** — use the Databricks Assistant as much as possible
# MAGIC - All team members can work simultaneously
# MAGIC - Use any Databricks features you've learned today
# MAGIC - You may split work: one person on pipeline, one on dashboard, one on ML/agent
# MAGIC
# MAGIC > **This is where everything comes together. Teams that mastered the Assistant
# MAGIC > in earlier events will have a massive advantage.**

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dataops_olympics;
# MAGIC USE SCHEMA default;

# COMMAND ----------

TEAM_NAME = "team_XX"  # <-- CHANGE THIS to your team name

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Component 1: Data Pipeline
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Extend your Event 1 Medallion pipeline to include the `drug_reviews` table:
# MAGIC >
# MAGIC > - **Bronze:** Ingest `drug_reviews` as-is with an ingestion timestamp
# MAGIC > - **Silver:** Clean the data — filter out rows with null drug names,
# MAGIC >   ratings outside 1-10. Add a `sentiment` column:
# MAGIC >   rating >= 7 = "positive", rating <= 3 = "negative", else "neutral"
# MAGIC > - **Gold:** Aggregate — average rating and review count by drug and condition
# MAGIC > - Add table and column comments for governance
# MAGIC >
# MAGIC > Your Event 1 heart tables should already exist. This extends the pipeline
# MAGIC > to a second data domain.

# COMMAND ----------

# YOUR CODE HERE — build the drug_reviews pipeline


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Component 2: Dashboard / Visualizations
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create visual analytics that a hospital administrator could understand.
# MAGIC >
# MAGIC > **Option A: AI/BI Dashboard** (in the Databricks UI)
# MAGIC > - Drug rating distribution (bar or histogram)
# MAGIC > - Top/bottom drugs by average rating
# MAGIC > - Review volume by condition
# MAGIC > - Heart disease metrics from your Gold table
# MAGIC > - Filters for drug name, condition, age group
# MAGIC >
# MAGIC > **Option B: Notebook Visualizations** (at least 4 Plotly charts)
# MAGIC > - Combine heart disease and drug review insights
# MAGIC > - Use your Gold tables for pre-aggregated views

# COMMAND ----------

# YOUR CODE HERE — create visualizations


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Component 3: Intelligence Layer (Pick One)
# MAGIC
# MAGIC ### Option A: ML Model
# MAGIC
# MAGIC > Train a model on your Silver heart data (from Event 1) to predict heart disease.
# MAGIC > Log with MLflow, report F1 score. If you already did this in Event 3,
# MAGIC > extend it with drug review features or improve the model.
# MAGIC
# MAGIC ### Option B: GenAI Agent
# MAGIC
# MAGIC > Build a clinical agent that answers questions across ALL your data:
# MAGIC > heart patients (Silver/Gold), drug reviews, and clinical notes.
# MAGIC > If you built one in Event 4, enhance it with the drug review pipeline output.
# MAGIC
# MAGIC ### Option C: Interactive App Prototype
# MAGIC
# MAGIC > Design a clinical decision support tool concept:
# MAGIC > - A doctor inputs patient age, cholesterol, BP
# MAGIC > - The tool queries your Silver table for similar patients
# MAGIC > - Shows drug recommendations from the reviews data
# MAGIC > - Displays relevant clinical notes

# COMMAND ----------

# YOUR CODE HERE — build your intelligence layer


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Presentation Prep (3 minutes)
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Prepare a 3-minute demo for the judges. Structure:
# MAGIC >
# MAGIC > 1. **Problem** (30 sec) — What does the hospital administrator need?
# MAGIC > 2. **Pipeline** (45 sec) — Show your Medallion architecture, data quality, governance
# MAGIC > 3. **Dashboard** (45 sec) — Walk through key insights
# MAGIC > 4. **Intelligence** (45 sec) — Demo your model/agent/app
# MAGIC > 5. **Wow factor** (15 sec) — What makes your solution special?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion Checklist
# MAGIC
# MAGIC - [ ] Drug reviews pipeline (Bronze -> Silver -> Gold) complete
# MAGIC - [ ] Governance comments on all new tables
# MAGIC - [ ] Dashboard published OR 4+ visualizations in notebook
# MAGIC - [ ] Intelligence layer working (ML model / agent / app)
# MAGIC - [ ] Presentation rehearsed
# MAGIC
# MAGIC > **Signal judges when ready to present!**
