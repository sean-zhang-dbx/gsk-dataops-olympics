# Databricks notebook source
# MAGIC %md
# MAGIC # Event 5: Capstone — Hospital Command Center
# MAGIC
# MAGIC ## Build the Dashboard the Board of Directors Will See
# MAGIC **Time: 30 minutes** | **Max Points: 30 (+5 bonus)**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Scenario
# MAGIC
# MAGIC > The hospital's Board of Directors meeting is in 30 minutes. The **Chief Medical Officer**
# MAGIC > is so impressed by your team's work today — the pipeline (Event 1), the analytics
# MAGIC > (Event 2), the predictive model (Event 3), and the AI agent (Event 4) — that she wants
# MAGIC > you to build a **"Hospital Command Center"**: a **business-user-friendly** interface
# MAGIC > that ties everything together into a story the board can understand.
# MAGIC >
# MAGIC > You have two options for the main deliverable:
# MAGIC > - **Option A: AI/BI Dashboard** (Lakeview) — 10 pts
# MAGIC > - **Option B: Databricks App** (Streamlit/Gradio/Dash) — **15 pts** (harder, more points)
# MAGIC >
# MAGIC > She also wants a **Genie space** so board members can ask follow-up questions
# MAGIC > in plain English. And she wants an **AI-generated executive briefing** she can
# MAGIC > read aloud in the first 30 seconds.
# MAGIC >
# MAGIC > **This is the grand finale. Every Databricks feature you've learned today comes together.**
# MAGIC
# MAGIC ### What You'll Build
# MAGIC
# MAGIC | # | Component | Databricks Feature | Points | Time |
# MAGIC |---|-----------|-------------------|--------|------|
# MAGIC | 1 | Verify & prepare all data | Unity Catalog | 3 | 2 min |
# MAGIC | 2 | AI executive briefing | `ai_query()` | 5 | 5 min |
# MAGIC | 3a | **Option A: AI/BI Dashboard** (5+ charts) | Lakeview Dashboard | **10** | 15 min |
# MAGIC | 3b | **Option B: Databricks App** (interactive) | Streamlit / Gradio / Dash | **15** | 15 min |
# MAGIC | 4 | Genie space (all tables) | AI/BI Genie | 5 | 3 min |
# MAGIC | 5 | Presentation to judges | Live demo | 7 | 5 min |
# MAGIC | | **Total (Dashboard path)** | | **30** | **30 min** |
# MAGIC | | **Total (App path)** | | **35** | **30 min** |
# MAGIC | | Bonus: Alerts, Subscriptions, Filters | | +5 | |
# MAGIC
# MAGIC ### Databricks Features Showcase
# MAGIC
# MAGIC This capstone uses: **Unity Catalog** · **AI/BI Dashboard** or **Databricks App** ·
# MAGIC **AI/BI Genie** · **`ai_query()`** · **Delta Lake** · **MLflow** · **Databricks Assistant**
# MAGIC
# MAGIC > **Vibe Coding:** Use **Databricks Assistant** (`Cmd+I`) for everything!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Team Configuration

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_submit

# COMMAND ----------

print(f"Team catalog: {CATALOG}.default")

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
# MAGIC ## Step 1: Verify Your Artifacts (3 pts, ~2 min)
# MAGIC
# MAGIC > Confirm that all tables from Events 1-4 are accessible.
# MAGIC > If anything is missing, run the fallback notebook first.

# COMMAND ----------

print("=" * 60)
print(f"  ARTIFACT CHECK — {TEAM_NAME}")
print("=" * 60)

checks = {
    "heart_silver_correct (Event 1)":       f"{CATALOG}.default.heart_silver_correct",
    "heart_gold_correct (Event 1)":         f"{CATALOG}.default.heart_gold_correct",
    "heart_ai_insights (Event 4)":  f"{CATALOG}.default.heart_ai_insights",
    "drug_ai_summary (Event 4)":    f"{CATALOG}.default.drug_ai_summary",
    "drug_reviews (shared)":        f"{SHARED_CATALOG}.default.drug_reviews",
    "clinical_notes (shared)":      f"{SHARED_CATALOG}.default.clinical_notes",
}

all_ok = True
for label, fqn in checks.items():
    try:
        cnt = spark.table(fqn).count()
        print(f"  [OK] {label:35s} → {fqn} ({cnt} rows)")
    except Exception as e:
        all_ok = False
        print(f"  [!!] {label:35s} → MISSING — {str(e)[:50]}")

if all_ok:
    print(f"\n  All artifacts present. Ready for the capstone!")
else:
    print(f"\n  WARNING: Some tables missing. Run fallback notebooks first:")
    print(f"  - event1_data_engineering/fallback_generate_tables (TEAM_NAME={TEAM_NAME})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create missing tables if needed
# MAGIC
# MAGIC If `heart_ai_insights` or `drug_ai_summary` don't exist (Event 4 not completed),
# MAGIC run the cells below to create them now.

# COMMAND ----------

try:
    spark.table(f"{CATALOG}.default.heart_ai_insights").count()
    print("heart_ai_insights already exists — skipping")
except Exception:
    print("Creating heart_ai_insights with ai_query()...")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.default.heart_ai_insights AS
        SELECT *,
            ai_query(
                'databricks-meta-llama-3-3-70b-instruct',
                CONCAT(
                    'You are a clinical analyst. Provide a 2-sentence insight for: ',
                    age_group, ' patients, ', diagnosis, ', n=', patient_count,
                    ', avg cholesterol=', avg_cholesterol, ' mg/dL',
                    ', avg BP=', avg_blood_pressure, ' mmHg',
                    ', avg max HR=', avg_max_heart_rate, ' bpm'
                )
            ) AS clinical_insight
        FROM {CATALOG}.default.heart_gold_correct
    """)
    print("  Created!")

# COMMAND ----------

try:
    spark.table(f"{CATALOG}.default.drug_ai_summary").count()
    print("drug_ai_summary already exists — skipping")
except Exception:
    print("Creating drug_ai_summary with ai_query()...")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.default.drug_ai_summary AS
        SELECT drug_name, ROUND(AVG(rating), 1) AS avg_rating, COUNT(*) AS reviews,
            ai_query(
                'databricks-meta-llama-3-3-70b-instruct',
                CONCAT('Summarize in one sentence: ', drug_name,
                       ', avg rating ', ROUND(AVG(rating), 1), '/10, ',
                       COUNT(*), ' reviews')
            ) AS ai_summary
        FROM {SHARED_CATALOG}.default.drug_reviews
        GROUP BY drug_name ORDER BY avg_rating DESC LIMIT 10
    """)
    print("  Created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: AI Executive Briefing (5 pts, ~5 min)
# MAGIC
# MAGIC > Generate an AI-powered executive briefing that the CMO can read aloud to the board.
# MAGIC > This should summarize the entire hospital data picture in 3-4 paragraphs.
# MAGIC >
# MAGIC > Save as `{CATALOG}.default.executive_briefing`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a: Gather the key metrics for the LLM

# COMMAND ----------

from pyspark.sql import functions as F

total_patients = spark.table(f"{CATALOG}.default.heart_silver_correct").count()
disease_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.default.heart_silver_correct WHERE target = 1").collect()[0][0]
disease_pct = round(disease_count * 100.0 / total_patients, 1)

gold_summary = spark.table(f"{CATALOG}.default.heart_gold_correct").toPandas().to_string(index=False)

top_drugs = spark.table(f"{CATALOG}.default.drug_ai_summary").select("drug_name", "avg_rating", "reviews").toPandas().to_string(index=False)

notes_count = spark.table(f"{SHARED_CATALOG}.default.clinical_notes").count()

print(f"Patients: {total_patients} ({disease_pct}% with heart disease)")
print(f"Clinical notes: {notes_count}")
print(f"\nGold table:\n{gold_summary}")
print(f"\nTop drugs:\n{top_drugs}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b: Generate the executive briefing with `ai_query()`

# COMMAND ----------

briefing_prompt = f"""You are writing a 4-paragraph executive briefing for a hospital Board of Directors meeting.

KEY METRICS:
- Total patients analyzed: {total_patients}
- Heart disease prevalence: {disease_pct}%
- Clinical notes reviewed: {notes_count} across hospital departments

PATIENT COHORT BREAKDOWN:
{gold_summary}

TOP DRUG REVIEWS:
{top_drugs}

Write a professional briefing that:
1. Opens with the headline finding (heart disease prevalence)
2. Highlights the highest-risk age groups and their clinical profiles
3. Summarizes drug review intelligence and any concerns
4. Closes with a recommendation for next steps

Tone: Professional, data-driven, suitable for hospital executives. Cite specific numbers."""

briefing_result = spark.sql(f"""
    SELECT ai_query(
        'databricks-meta-llama-3-3-70b-instruct',
        '{briefing_prompt.replace("'", "''")}'
    ) AS briefing
""").collect()[0]["briefing"]

print("=" * 70)
print("  EXECUTIVE BRIEFING")
print("=" * 70)
print(briefing_result)
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c: Save the briefing

# COMMAND ----------

from datetime import datetime

briefing_df = spark.createDataFrame([{
    "team": TEAM_NAME,
    "briefing_text": briefing_result,
    "total_patients": total_patients,
    "disease_prevalence_pct": disease_pct,
    "generated_at": datetime.now().isoformat(),
}])

briefing_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.default.executive_briefing"
)
print(f"Saved to {CATALOG}.default.executive_briefing")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Business Deliverable — Choose Your Path
# MAGIC
# MAGIC > **Pick ONE of the two options below.** The App path is worth more points
# MAGIC > because it requires coding, but is harder. The Dashboard path is quicker.
# MAGIC >
# MAGIC > | Path | Points | Difficulty |
# MAGIC > |------|--------|------------|
# MAGIC > | **Option A: AI/BI Dashboard** | 10 pts | Medium |
# MAGIC > | **Option B: Databricks App** | **15 pts** | Hard |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Option A: AI/BI Dashboard (10 pts, ~15 min)
# MAGIC
# MAGIC > Create a **Lakeview AI/BI Dashboard** from the Databricks UI with
# MAGIC > **at least 5 visualizations**.
# MAGIC
# MAGIC #### How to Create
# MAGIC
# MAGIC 1. Click **+ New** → **Dashboard** in the sidebar
# MAGIC 2. Name it: `{TEAM_NAME} — Hospital Command Center`
# MAGIC 3. Add **datasets** by writing SQL queries (see below)
# MAGIC 4. Drag visualizations onto the canvas
# MAGIC 5. **Publish** when done
# MAGIC
# MAGIC #### Required Charts (5 minimum for full points)
# MAGIC
# MAGIC | # | Chart | SQL Dataset | Chart Type |
# MAGIC |---|-------|-------------|------------|
# MAGIC | 1 | **Heart Disease by Age Group** | `SELECT * FROM {CATALOG}.default.heart_gold_correct` | Stacked bar (age_group × diagnosis) |
# MAGIC | 2 | **Risk Heatmap** | Avg cholesterol & BP by age group | Table with conditional formatting |
# MAGIC | 3 | **AI Clinical Insights** | `SELECT * FROM {CATALOG}.default.heart_ai_insights` | Table showing LLM-generated insights |
# MAGIC | 4 | **Top Drug Ratings** | `SELECT * FROM {CATALOG}.default.drug_ai_summary` | Horizontal bar chart |
# MAGIC | 5 | **Patient Distribution** | Gender × diagnosis breakdown from heart_silver_correct | Pie or donut chart |
# MAGIC
# MAGIC ### Copy-Paste SQL for Dashboard Datasets
# MAGIC
# MAGIC Use these queries when adding datasets to your dashboard:

# COMMAND ----------

# MAGIC %md
# MAGIC **Dataset 1: Heart Disease Cohorts**
# MAGIC ```sql
# MAGIC SELECT age_group, diagnosis, patient_count, avg_cholesterol,
# MAGIC        avg_blood_pressure, avg_max_heart_rate
# MAGIC FROM <your_catalog>.default.heart_gold_correct
# MAGIC ORDER BY age_group, diagnosis
# MAGIC ```
# MAGIC
# MAGIC **Dataset 2: AI Clinical Insights**
# MAGIC ```sql
# MAGIC SELECT age_group, diagnosis, patient_count, avg_cholesterol,
# MAGIC        avg_blood_pressure, clinical_insight
# MAGIC FROM <your_catalog>.default.heart_ai_insights
# MAGIC ```
# MAGIC
# MAGIC **Dataset 3: Top Drugs with AI Summary**
# MAGIC ```sql
# MAGIC SELECT drug_name, avg_rating, reviews, ai_summary
# MAGIC FROM <your_catalog>.default.drug_ai_summary
# MAGIC ORDER BY avg_rating DESC
# MAGIC ```
# MAGIC
# MAGIC **Dataset 4: Patient Demographics**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   CASE WHEN sex = 1 THEN 'Male' ELSE 'Female' END AS gender,
# MAGIC   CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
# MAGIC   COUNT(*) AS patient_count,
# MAGIC   ROUND(AVG(age), 1) AS avg_age,
# MAGIC   ROUND(AVG(chol), 1) AS avg_cholesterol
# MAGIC FROM <your_catalog>.default.heart_silver_correct
# MAGIC GROUP BY 1, 2
# MAGIC ```
# MAGIC
# MAGIC **Dataset 5: Executive Briefing**
# MAGIC ```sql
# MAGIC SELECT briefing_text, total_patients, disease_prevalence_pct, generated_at
# MAGIC FROM <your_catalog>.default.executive_briefing
# MAGIC ```
# MAGIC
# MAGIC **Dataset 6 (bonus): Clinical Notes by Department**
# MAGIC ```sql
# MAGIC SELECT department, note_type, COUNT(*) AS note_count
# MAGIC FROM dataops_olympics.default.clinical_notes
# MAGIC GROUP BY 1, 2
# MAGIC ```
# MAGIC
# MAGIC > **Replace `<your_catalog>` with your team name** (e.g., `team_01`).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard Layout Suggestion
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────┐
# MAGIC │  Hospital Command Center — {TEAM_NAME}                   │
# MAGIC ├──────────────────────┬───────────────────────────────────┤
# MAGIC │                      │                                   │
# MAGIC │  Heart Disease by    │   Patient Demographics            │
# MAGIC │  Age Group           │   (pie: gender × diagnosis)       │
# MAGIC │  (stacked bar)       │                                   │
# MAGIC │                      │                                   │
# MAGIC ├──────────────────────┴───────────────────────────────────┤
# MAGIC │                                                          │
# MAGIC │  AI Clinical Insights (table with LLM text)              │
# MAGIC │                                                          │
# MAGIC ├──────────────────────┬───────────────────────────────────┤
# MAGIC │                      │                                   │
# MAGIC │  Top Drug Ratings    │   Executive Briefing              │
# MAGIC │  (horizontal bar)    │   (text block from ai_query)      │
# MAGIC │                      │                                   │
# MAGIC └──────────────────────┴───────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC > **Tip:** Use Databricks Assistant in the dashboard editor!
# MAGIC > Click the AI icon to generate chart configurations from natural language.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### Option B: Databricks App (15 pts, ~15 min)
# MAGIC
# MAGIC > Build an **interactive web application** that hospital executives can use
# MAGIC > without any SQL or technical knowledge. Deploy it as a Databricks App.
# MAGIC
# MAGIC #### Why More Points?
# MAGIC > Apps require **coding, UI design, and deployment** — a deeper Databricks skill set.
# MAGIC > The result is a production-quality interface any business user can operate.
# MAGIC
# MAGIC #### What to Build
# MAGIC
# MAGIC Your app should include:
# MAGIC 1. **Patient overview** — key metrics (total patients, disease prevalence)
# MAGIC 2. **Interactive charts** — at least 3 visualizations (age distribution, risk heatmap, drug ratings)
# MAGIC 3. **AI insights panel** — display the `clinical_insight` / `ai_summary` from your tables
# MAGIC 4. **Executive briefing** — render the AI-generated briefing text
# MAGIC 5. **Filters/interactivity** — let users filter by age group, diagnosis, etc.
# MAGIC
# MAGIC #### Recommended Frameworks
# MAGIC
# MAGIC | Framework | Difficulty | Best For |
# MAGIC |-----------|-----------|----------|
# MAGIC | **Streamlit** | Easiest | Quick dashboards with Python |
# MAGIC | **Gradio** | Easy | ML model demos |
# MAGIC | **Dash (Plotly)** | Medium | Rich interactive dashboards |
# MAGIC
# MAGIC #### Quick Start (Streamlit)
# MAGIC
# MAGIC ```python
# MAGIC # app.py
# MAGIC import streamlit as st
# MAGIC from databricks import sql as dbsql
# MAGIC
# MAGIC st.set_page_config(page_title="Hospital Command Center", layout="wide")
# MAGIC st.title("Hospital Command Center")
# MAGIC
# MAGIC # Connect to your tables using Databricks SQL connector or Spark
# MAGIC # Display charts with st.bar_chart(), st.plotly_chart(), etc.
# MAGIC # Show AI insights in st.expander() panels
# MAGIC # Add filters with st.selectbox(), st.slider()
# MAGIC ```
# MAGIC
# MAGIC #### Deploying as a Databricks App
# MAGIC
# MAGIC 1. Create `app.yaml` with your app config
# MAGIC 2. Run `databricks apps create {TEAM_NAME}-hospital-app`
# MAGIC 3. Deploy with `databricks apps deploy {TEAM_NAME}-hospital-app --source-code-path .`
# MAGIC 4. Verify the app is running in the Databricks UI
# MAGIC
# MAGIC #### Scoring Rubric (App)
# MAGIC
# MAGIC | Criteria | Points |
# MAGIC |----------|--------|
# MAGIC | App deployed and accessible | 5 |
# MAGIC | At least 3 interactive visualizations | 4 |
# MAGIC | Reads from team's Unity Catalog tables | 3 |
# MAGIC | Includes AI-generated insights or briefing | 2 |
# MAGIC | Clean, professional UI | 1 |
# MAGIC | **Total** | **15** |

# COMMAND ----------

# MAGIC %md
# MAGIC #### Record Your App (if choosing Option B)

# COMMAND ----------

APP_NAME = ""  # e.g., "team_01-hospital-app"
if APP_NAME:
    print(f"App name: {APP_NAME}")
else:
    print("No app name entered — using Dashboard path (Option A).")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Genie Space — "Ask the Data" (5 pts, ~3 min)
# MAGIC
# MAGIC > Connect a Genie space so board members can ask follow-up questions in plain English.
# MAGIC >
# MAGIC > If you built one in Event 2, **update it** with all your tables.
# MAGIC > If not, create a new one now.
# MAGIC
# MAGIC ### Tables to Add
# MAGIC
# MAGIC | Table | Why |
# MAGIC |-------|-----|
# MAGIC | `{CATALOG}.default.heart_silver_correct` | Patient-level queries |
# MAGIC | `{CATALOG}.default.heart_gold_correct` | Aggregated cohort queries |
# MAGIC | `{CATALOG}.default.heart_ai_insights` | AI-generated insights |
# MAGIC | `{CATALOG}.default.drug_ai_summary` | Drug intelligence |
# MAGIC | `{CATALOG}.default.executive_briefing` | Executive summary |
# MAGIC
# MAGIC ### Instructions to Copy-Paste into Genie
# MAGIC
# MAGIC ```
# MAGIC DOMAIN: Hospital Command Center — comprehensive patient and drug analytics.
# MAGIC
# MAGIC DATA TABLES:
# MAGIC - heart_silver_correct: Patient-level clinical records (~488 rows).
# MAGIC   Key: target (1=heart disease, 0=healthy), age, sex (0=F,1=M), chol, trestbps, thalach.
# MAGIC - heart_gold_correct: Pre-aggregated by age_group and diagnosis. Columns: patient_count,
# MAGIC   avg_cholesterol, avg_blood_pressure, avg_max_heart_rate.
# MAGIC - heart_ai_insights: Same as heart_gold_correct plus clinical_insight (AI-generated text).
# MAGIC - drug_ai_summary: Top drugs with avg_rating, reviews count, and ai_summary text.
# MAGIC - executive_briefing: AI-generated 4-paragraph board briefing.
# MAGIC
# MAGIC RULES:
# MAGIC - "heart disease patients" means target = 1.
# MAGIC - Round all numbers to 1 decimal place.
# MAGIC - For drug questions, use drug_ai_summary.
# MAGIC - For patient cohort questions, prefer heart_gold_correct over heart_silver_correct.
# MAGIC - When asked for "insights" or "summary", include the clinical_insight or ai_summary text.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Record Your Genie Space ID
# MAGIC
# MAGIC After creating/updating the space, paste the ID below (from the URL).

# COMMAND ----------

GENIE_SPACE_ID = ""  # paste your Genie space ID here
if GENIE_SPACE_ID:
    print(f"Genie Space ID: {GENIE_SPACE_ID}")
    print(f"URL: https://{spark.conf.get('spark.databricks.workspaceUrl')}/genie/rooms/{GENIE_SPACE_ID}")
else:
    print("No Genie space ID entered yet.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Presentation (7 pts, ~5 min)
# MAGIC
# MAGIC > Prepare a **3-minute live demo** for the judges. The remaining 2 minutes are for Q&A.
# MAGIC
# MAGIC ### Presentation Structure
# MAGIC
# MAGIC | Segment | Time | What to Show |
# MAGIC |---------|------|-------------|
# MAGIC | **Opening** | 30 sec | Read the first paragraph of your executive briefing |
# MAGIC | **Dashboard Tour** | 90 sec | Walk through each chart, highlight the key finding |
# MAGIC | **Genie Demo** | 30 sec | Ask Genie a question live: "Which age group has the highest heart disease rate?" |
# MAGIC | **AI Insights** | 15 sec | Show the `ai_query()`-generated clinical insights |
# MAGIC | **Wow Factor** | 15 sec | What makes your solution special? |
# MAGIC
# MAGIC ### Scoring Rubric (Judges)
# MAGIC
# MAGIC | Criteria | Points |
# MAGIC |----------|--------|
# MAGIC | Business deliverable (dashboard or app) is functional and complete | 4 |
# MAGIC | Deliverable tells a coherent data story (not random charts) | 3 |
# MAGIC | Executive briefing is data-driven and professional | 2 |
# MAGIC | Genie space answers a live question correctly | 2 |
# MAGIC | Presentation is clear and within time | 2 |
# MAGIC | **Bonus: Published dashboard / deployed app** | +1 |
# MAGIC | **Bonus: Filters, interactivity, or parameters** | +2 |
# MAGIC | **Bonus: Scheduled email subscription (dashboard) or auth (app)** | +2 |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Bonus Challenges (+5 pts)
# MAGIC
# MAGIC ### Bonus 1: Publish the Dashboard (+1 pt)
# MAGIC
# MAGIC > Click **Share** → **Publish** in the dashboard editor. This creates a
# MAGIC > shareable link that anyone in the workspace can view.
# MAGIC
# MAGIC ### Bonus 2: Add Filters/Parameters (+2 pts)
# MAGIC
# MAGIC > Add interactive filters to your dashboard:
# MAGIC > - Age group dropdown (filters all charts)
# MAGIC > - Diagnosis filter (Heart Disease / Healthy / All)
# MAGIC >
# MAGIC > In the dashboard editor, click a chart → **Add filter** → select the field.
# MAGIC > Cross-filters let one chart filter others automatically.
# MAGIC
# MAGIC ### Bonus 3: Schedule a Dashboard Email (+2 pts)
# MAGIC
# MAGIC > Set up an automated email subscription:
# MAGIC > 1. Open the published dashboard
# MAGIC > 2. Click **Schedule** → **Add schedule**
# MAGIC > 3. Set frequency to "Daily" (or any schedule)
# MAGIC > 4. Add your email as a subscriber
# MAGIC >
# MAGIC > This demonstrates production-readiness to the board.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Final Checklist
# MAGIC
# MAGIC - [ ] All Event 1-4 tables verified and accessible
# MAGIC - [ ] Executive briefing generated with `ai_query()` and saved
# MAGIC - [ ] AI/BI Dashboard created with 5+ charts
# MAGIC - [ ] Dashboard named `{TEAM_NAME} — Hospital Command Center`
# MAGIC - [ ] Genie space created/updated with all tables + instructions
# MAGIC - [ ] Presentation rehearsed (3 min max)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## SUBMIT YOUR WORK
# MAGIC
# MAGIC **Run this cell when you're done!** It records your submission timestamp for the live scoreboard.
# MAGIC Then signal the judges that you're ready to present!

# COMMAND ----------

submit("event5")
