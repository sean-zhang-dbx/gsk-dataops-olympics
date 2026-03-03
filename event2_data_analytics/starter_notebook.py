# Databricks notebook source
# MAGIC %md
# MAGIC # Event 2: Data Analytics — Dashboard & Genie Challenge
# MAGIC
# MAGIC ## Challenge: Best Analytics & Natural Language Querying
# MAGIC **Build Time: ~20 minutes**
# MAGIC
# MAGIC ### Objective
# MAGIC 1. Build an **AI/BI (Lakeview) Dashboard** with key visualizations
# MAGIC 2. Set up a **Genie space** connected to your heart disease data
# MAGIC 3. Answer benchmark questions — the team with the most correct answers wins
# MAGIC
# MAGIC ### How You Win
# MAGIC **Most accurate answers to the benchmark question pool wins Gold!**
# MAGIC
# MAGIC ### Data — From YOUR Event 1 Pipeline!
# MAGIC
# MAGIC You'll use the **Silver and Gold tables you built in Event 1**:
# MAGIC - `{TEAM_NAME}_heart_silver` — cleaned patient data (deduplicated, validated)
# MAGIC - `{TEAM_NAME}_heart_gold` — aggregated metrics by age group and diagnosis
# MAGIC
# MAGIC If your team used the SDP path, your tables are:
# MAGIC - `heart_silver` and `heart_gold`
# MAGIC
# MAGIC > If Event 1 tables aren't ready, the organizer will provide reference tables.
# MAGIC
# MAGIC > **Vibe Coding:** Use the Databricks Assistant (`Cmd+I`) to generate SQL and visualizations!

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dataops_olympics;
# MAGIC USE SCHEMA default;

# COMMAND ----------

TEAM_NAME = "team_XX"  # <-- CHANGE THIS to your team name

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Verify Your Event 1 Output
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Confirm that your Silver and Gold tables from Event 1 exist.
# MAGIC > Display the row counts and a sample of each table.
# MAGIC > If using the SDP path, your tables may be named `heart_silver` / `heart_gold`
# MAGIC > (without the team prefix).

# COMMAND ----------

# YOUR CODE HERE — verify your Event 1 tables exist
# Prompt idea: "Show me the row counts and first 5 rows of my Silver and Gold tables"


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Analytical SQL Queries
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Write **5 SQL queries** against your Silver table to answer key clinical questions.
# MAGIC > These queries will help you build your dashboard AND answer benchmark questions.
# MAGIC >
# MAGIC > **Query 1:** What percentage of patients have heart disease (target = 1)?
# MAGIC >
# MAGIC > **Query 2:** What is the average age of patients WITH vs WITHOUT heart disease?
# MAGIC >
# MAGIC > **Query 3:** Distribution of chest pain types (cp column) by heart disease status.
# MAGIC > cp values: 0 = typical angina, 1 = atypical, 2 = non-anginal, 3 = asymptomatic.
# MAGIC >
# MAGIC > **Query 4:** Which age group (Under 40, 40-49, 50-59, 60+) has the highest heart disease rate?
# MAGIC >
# MAGIC > **Query 5:** Average cholesterol by sex (0=female, 1=male) and heart disease status.
# MAGIC >
# MAGIC > Use your `{TEAM_NAME}_heart_silver` table (or `heart_silver` for SDP path).

# COMMAND ----------

# YOUR CODE HERE — Query 1


# COMMAND ----------

# YOUR CODE HERE — Query 2


# COMMAND ----------

# YOUR CODE HERE — Query 3


# COMMAND ----------

# YOUR CODE HERE — Query 4


# COMMAND ----------

# YOUR CODE HERE — Query 5


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Create Visualizations (for your Dashboard)
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create **at least 3 Plotly visualizations** from your Silver/Gold tables:
# MAGIC >
# MAGIC > 1. A **bar chart** showing heart disease rate by age group
# MAGIC >    (you can query your Gold table directly for this!)
# MAGIC > 2. A **scatter plot** of max heart rate (`thalach`) vs age, colored by diagnosis
# MAGIC > 3. A third visualization of your choice (pie chart, heatmap, box plot, etc.)
# MAGIC >
# MAGIC > Use `plotly_white` template for clean styling.

# COMMAND ----------

# YOUR CODE HERE — Visualization 1


# COMMAND ----------

# YOUR CODE HERE — Visualization 2


# COMMAND ----------

# YOUR CODE HERE — Visualization 3


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Create an AI/BI Dashboard
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > In the Databricks UI (not this notebook), create a **Lakeview Dashboard**:
# MAGIC >
# MAGIC > 1. Click **+ New** in the sidebar, then **Dashboard**
# MAGIC > 2. Add a **Dataset** pointing to your Silver table (`{TEAM_NAME}_heart_silver`)
# MAGIC > 3. Add visualizations:
# MAGIC >    - At least 1 bar/column chart
# MAGIC >    - At least 1 counter/KPI widget (e.g., total patients, disease rate %)
# MAGIC >    - At least 1 table or detail view
# MAGIC > 4. Add **filters** (e.g., age group, sex, chest pain type)
# MAGIC > 5. **Publish** the dashboard
# MAGIC >
# MAGIC > You can also add your Gold table as a second dataset for pre-aggregated views.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Set Up a Genie Space
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > In the Databricks UI, create a **Genie space**:
# MAGIC >
# MAGIC > 1. Click **+ New** > **Genie space**
# MAGIC > 2. Name: `{TEAM_NAME} Heart Disease Analyst`
# MAGIC > 3. Add your tables as data sources:
# MAGIC >    - Your Silver table (`{TEAM_NAME}_heart_silver` or `heart_silver`)
# MAGIC >    - Your Gold table (`{TEAM_NAME}_heart_gold` or `heart_gold`)
# MAGIC > 4. Add instructions to help Genie understand your data:
# MAGIC >    - "target = 1 means the patient has heart disease, target = 0 means healthy"
# MAGIC >    - "cp is chest pain type: 0=typical angina, 1=atypical, 2=non-anginal, 3=asymptomatic"
# MAGIC >    - "sex: 1=male, 0=female"
# MAGIC >    - "thalach is the maximum heart rate achieved during exercise"
# MAGIC > 5. Test with sample questions

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Benchmark Questions
# MAGIC
# MAGIC The organizer will announce benchmark questions. Use your dashboard,
# MAGIC Genie space, and SQL skills to answer them as fast and accurately as possible.
# MAGIC
# MAGIC Record your answers below:

# COMMAND ----------

answers = {
    "Q1": "",
    "Q2": "",
    "Q3": "",
    "Q4": "",
    "Q5": "",
}

print("=" * 60)
print(f"  BENCHMARK ANSWERS — {TEAM_NAME}")
print("=" * 60)
for q, a in answers.items():
    print(f"  {q}: {a}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stretch Goals (Extra Credit)
# MAGIC
# MAGIC Finished early? Ask the Databricks Assistant to help you with these:
# MAGIC
# MAGIC 1. **Cross-table analysis** — Join your Silver table with `life_expectancy` or `drug_reviews`
# MAGIC 2. **Statistical tests** — Chi-squared test: is chest pain type statistically associated with heart disease?
# MAGIC 3. **Advanced dashboard** — Add drill-through filters, calculated fields, or conditional formatting
# MAGIC 4. **Genie optimization** — Write comprehensive instructions with example questions and column descriptions
# MAGIC 5. **AI Functions in SQL** — Use `ai_query()` to generate natural language summaries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion Checklist
# MAGIC
# MAGIC - [ ] At least 5 analytical SQL queries written
# MAGIC - [ ] At least 3 visualizations created
# MAGIC - [ ] AI/BI Dashboard published with filters
# MAGIC - [ ] Genie space set up and responding to questions
# MAGIC - [ ] Benchmark answers recorded
# MAGIC
# MAGIC > **RAISE YOUR HAND when your benchmark answers are ready!**
