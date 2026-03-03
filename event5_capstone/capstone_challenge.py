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
# MAGIC > *"We have drug review data and clinical notes from our departments.
# MAGIC > I need a complete analytics solution by end of day:
# MAGIC > a clean data pipeline, a dashboard I can show the board,
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
# MAGIC | **1. Data Pipeline** | Ingest, clean, create governed Delta tables | 8 pts |
# MAGIC | **2. Dashboard or Visualizations** | AI/BI Dashboard OR 4+ notebook visualizations | 7 pts |
# MAGIC | **3. Intelligence Layer** (pick one) | ML model OR GenAI agent OR interactive app | 7 pts |
# MAGIC | **Presentation** | 3-min demo to judges explaining what you built | 3 pts |
# MAGIC | **Total** | | **25 pts** |
# MAGIC
# MAGIC ### Your Data
# MAGIC - `drug_reviews` table — 1,000 drug reviews with ratings, conditions, text
# MAGIC - `clinical_notes` table — 20 clinical notes across hospital departments
# MAGIC - You may also use `heart_disease` data from earlier events
# MAGIC
# MAGIC ### Rules
# MAGIC - **Vibe coding encouraged** — use the Databricks Assistant as much as possible
# MAGIC - All team members can work simultaneously
# MAGIC - Use any Databricks features you've learned today
# MAGIC - You may split work: one person on pipeline, one on dashboard, one on ML/agent
# MAGIC
# MAGIC > **This is where everything comes together. Teams that mastered the Assistant in earlier events will have a massive advantage.**

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Component 1: Data Pipeline
# MAGIC
# MAGIC **Goal:** Create a clean, governed data pipeline for drug reviews.
# MAGIC
# MAGIC ### Requirements
# MAGIC - Ingest drug_reviews into a Bronze Delta table
# MAGIC - Create a Silver table with:
# MAGIC   - Data quality checks (no null drug names, rating between 1-10)
# MAGIC   - Derived columns (e.g., sentiment category based on rating, review length)
# MAGIC - Create a Gold table with business aggregates
# MAGIC - Add table/column comments for governance
# MAGIC
# MAGIC ### Suggested Assistant Prompt
# MAGIC > "Build a medallion pipeline for the `drug_reviews` table:
# MAGIC > Bronze = raw data with ingestion timestamp,
# MAGIC > Silver = cleaned with filters (rating 1-10, no null drug_name) and a sentiment column (rating>=7 positive, <=3 negative, else neutral),
# MAGIC > Gold = average rating and review count by drug and condition.
# MAGIC > Add table comments for governance."

# COMMAND ----------

TEAM_NAME = "_____"  # e.g., "team_01"

# --- BUILD YOUR PIPELINE HERE ---
# Use the Databricks Assistant to generate the code!


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Component 2: Dashboard / Visualizations
# MAGIC
# MAGIC **Goal:** Create visual analytics that a hospital administrator could understand.
# MAGIC
# MAGIC ### Option A: AI/BI Dashboard (in the Databricks UI)
# MAGIC Create a Lakeview Dashboard with:
# MAGIC - Drug rating distribution (bar or histogram)
# MAGIC - Top/bottom drugs by average rating
# MAGIC - Review volume by condition
# MAGIC - A filter for drug_name or condition
# MAGIC
# MAGIC ### Option B: Notebook Visualizations (at least 4 charts)
# MAGIC Create Plotly charts directly in this notebook.
# MAGIC
# MAGIC ### Suggested Assistant Prompt
# MAGIC > "Create 4 Plotly visualizations from the `drug_reviews` table:
# MAGIC > 1. Bar chart of average rating by drug, sorted descending
# MAGIC > 2. Pie chart of review distribution by condition
# MAGIC > 3. Histogram of ratings (1-10)
# MAGIC > 4. Heatmap showing average rating by drug and condition"

# COMMAND ----------

# --- BUILD YOUR DASHBOARD / VISUALIZATIONS HERE ---


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Component 3: Intelligence Layer (Pick One)
# MAGIC
# MAGIC Choose the option that best fits your team's skills.
# MAGIC
# MAGIC ### Option A: ML Model
# MAGIC Build a model that predicts something useful from the drug review data.
# MAGIC
# MAGIC Ideas:
# MAGIC - Predict drug rating from condition and review text features
# MAGIC - Classify reviews as positive/negative
# MAGIC - Predict which drug is most effective per condition
# MAGIC
# MAGIC Track with MLflow. Register your best model.
# MAGIC
# MAGIC **Suggested Assistant Prompt:**
# MAGIC > "Build a classification model that predicts whether a drug review is positive
# MAGIC > (rating >= 7) or not, using drug_name, condition, and useful_count as features.
# MAGIC > Use sklearn, log with MLflow, and show the F1 score."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Option B: GenAI Agent
# MAGIC Build an AI agent that can answer questions about drugs and clinical notes.
# MAGIC
# MAGIC Ideas:
# MAGIC - Drug recommendation agent ("What drug is best rated for hypertension?")
# MAGIC - Clinical notes summarizer
# MAGIC - Drug interaction checker
# MAGIC
# MAGIC **Suggested Assistant Prompt:**
# MAGIC > "Create a clinical_agent function that can answer questions about drug ratings
# MAGIC > and clinical notes. Route drug questions to SQL on drug_reviews, route clinical
# MAGIC > questions to clinical_notes. Include a system prompt and handle errors."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Option C: Interactive App / Genie
# MAGIC Set up a Genie space or simple interactive interface.
# MAGIC
# MAGIC Ideas:
# MAGIC - Genie space for drug reviews + clinical notes with good instructions
# MAGIC - Interactive widget-based drug lookup
# MAGIC
# MAGIC **Suggested Assistant Prompt:**
# MAGIC > "Set up a Genie space with drug_reviews and clinical_notes tables. Add instructions
# MAGIC > explaining that rating is 1-10, condition maps to medical conditions, and
# MAGIC > clinical notes have department and note_type fields."

# COMMAND ----------

# --- BUILD YOUR INTELLIGENCE LAYER HERE ---


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Presentation Prep (3 minutes)
# MAGIC
# MAGIC Prepare a quick demo for the judges. Cover:
# MAGIC
# MAGIC 1. **What you built** — Pipeline, dashboard, and which intelligence option
# MAGIC 2. **How you used AI** — What prompts worked? What did you have to fix manually?
# MAGIC 3. **What it does** — Live demo of your dashboard and intelligence layer
# MAGIC 4. **What you'd do next** — If you had another hour, what would you add?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Final Validation

# COMMAND ----------

print("=" * 60)
print(f"  CAPSTONE VALIDATION — {TEAM_NAME}")
print("=" * 60)

score = 0

# Component 1: Pipeline
pipeline_tables = []
for suffix in ["_drug_bronze", "_drug_silver", "_drug_gold", "_drugs_bronze", "_drugs_silver", "_drugs_gold", "_drug_reviews_bronze", "_drug_reviews_silver", "_drug_reviews_gold"]:
    try:
        tbl = f"{TEAM_NAME}{suffix}"
        cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {tbl}").collect()[0].cnt
        pipeline_tables.append((tbl, cnt))
    except:
        pass

if len(pipeline_tables) >= 2:
    print(f"  Pipeline: {len(pipeline_tables)} tables found")
    for tbl, cnt in pipeline_tables:
        print(f"    - {tbl}: {cnt} rows")
    score += min(len(pipeline_tables), 3) * 2  # up to 6 pts for tables
else:
    print("  Pipeline: Looking for tables...")
    # Try to find any team tables
    try:
        tables = spark.sql("SHOW TABLES").collect()
        team_tables = [t for t in tables if TEAM_NAME in t.tableName]
        if team_tables:
            print(f"    Found {len(team_tables)} team tables:")
            for t in team_tables:
                cnt = spark.sql(f"SELECT COUNT(*) FROM {t.tableName}").collect()[0][0]
                print(f"    - {t.tableName}: {cnt} rows")
                score += 2
    except:
        pass

if score == 0:
    print("  Pipeline: No tables found yet")

# Component 2: Visualizations (manual check by judges)
print(f"\n  Dashboard/Visualizations: [Judge will verify]")

# Component 3: Intelligence layer (manual check by judges)
print(f"  Intelligence Layer: [Judge will verify]")

print(f"\n  Automated Pipeline Score: {score}/8")
print(f"  Remaining points awarded by judges (dashboard + intelligence + presentation)")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Checklist
# MAGIC
# MAGIC **Pipeline (8 pts)**
# MAGIC - [ ] Bronze table created from drug_reviews
# MAGIC - [ ] Silver table with quality checks and derived columns
# MAGIC - [ ] Gold table with business aggregates
# MAGIC - [ ] Table/column comments added
# MAGIC
# MAGIC **Dashboard (7 pts)**
# MAGIC - [ ] At least 4 visualizations OR published AI/BI Dashboard
# MAGIC - [ ] Charts are clear and labeled
# MAGIC - [ ] Useful for a non-technical audience
# MAGIC
# MAGIC **Intelligence Layer (7 pts)**
# MAGIC - [ ] ML model with MLflow tracking, OR
# MAGIC - [ ] GenAI agent that answers questions, OR
# MAGIC - [ ] Genie space / interactive app
# MAGIC
# MAGIC **Presentation (3 pts)**
# MAGIC - [ ] 3-minute demo prepared
# MAGIC
# MAGIC > **SIGNAL JUDGES WHEN READY TO PRESENT!**
