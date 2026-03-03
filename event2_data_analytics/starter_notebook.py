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
# MAGIC ### Data
# MAGIC You'll use the heart disease tables from Event 1. If your team didn't complete Event 1,
# MAGIC the organizer will clone the reference tables to your workspace.
# MAGIC
# MAGIC > **Tip:** Use the Databricks Assistant (`Cmd+I`) to generate SQL and visualizations!
# MAGIC
# MAGIC ### Databricks Assistant — Prompt Gallery
# MAGIC
# MAGIC Try these prompts in the Assistant panel:
# MAGIC
# MAGIC | Task | Prompt to Try |
# MAGIC |------|--------------|
# MAGIC | **SQL query** | "Write a SQL query on `team_XX_heart_disease` that shows the heart disease rate by age group (Under 40, 40-49, 50-59, 60+)" |
# MAGIC | **Complex SQL** | "Write a query showing avg cholesterol and max heart rate by sex and heart disease status, with a total row" |
# MAGIC | **Plotly chart** | "Create a Plotly grouped bar chart from this SQL result showing patient count by age group, colored by heart disease status" |
# MAGIC | **Dashboard help** | "What visualizations should I include in a heart disease analytics dashboard for a hospital administrator?" |
# MAGIC | **Genie instructions** | "Write Genie space instructions for the heart_disease table explaining what each column means" |
# MAGIC | **Benchmark answers** | Paste a question and say "Write a SQL query to answer: [question]" |
# MAGIC
# MAGIC *You can paste SQL errors directly into the Assistant and ask "Fix this error" — it reads the context.*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Verify Your Data
# MAGIC
# MAGIC Make sure the heart disease tables from Event 1 are available.

# COMMAND ----------

TEAM_NAME = "_____"  # e.g., "team_01"

# Check available tables
display(spark.sql("SHOW TABLES"))

# COMMAND ----------

# Quick look at the data
display(spark.sql(f"SELECT * FROM {TEAM_NAME}_heart_disease LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Analytical SQL Queries
# MAGIC
# MAGIC **TODO:** Write SQL queries to understand the heart disease data.
# MAGIC These queries will help you build your dashboard AND answer benchmark questions.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query 1 - What percentage of patients have heart disease?
# MAGIC -- Hint: target = 1 means heart disease present
# MAGIC
# MAGIC SELECT
# MAGIC     _____
# MAGIC FROM _____

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query 2 - Average age of patients WITH vs WITHOUT heart disease
# MAGIC
# MAGIC SELECT
# MAGIC     _____
# MAGIC FROM _____
# MAGIC GROUP BY _____

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query 3 - Distribution of chest pain types by heart disease status
# MAGIC -- cp: 0 = typical angina, 1 = atypical, 2 = non-anginal, 3 = asymptomatic
# MAGIC
# MAGIC SELECT
# MAGIC     _____
# MAGIC FROM _____
# MAGIC GROUP BY _____

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query 4 - Which age group has the highest rate of heart disease?
# MAGIC
# MAGIC SELECT
# MAGIC     _____
# MAGIC FROM _____

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query 5 - Average cholesterol by sex and heart disease status
# MAGIC
# MAGIC SELECT
# MAGIC     _____
# MAGIC FROM _____

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Visualizations (for your Dashboard)
# MAGIC
# MAGIC **TODO:** Create at least 3 visualizations that you'll use in your AI/BI Dashboard.
# MAGIC
# MAGIC Ideas:
# MAGIC - Bar chart: Heart disease rate by age group
# MAGIC - Pie chart: Distribution of chest pain types
# MAGIC - Scatter plot: Max heart rate vs age, colored by heart disease
# MAGIC - Heatmap: Correlation between numeric features

# COMMAND ----------

import plotly.express as px

# TODO: Visualization 1 - Heart disease rate by age group
df_viz1 = spark.sql(f"""
    SELECT
        CASE
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END as age_group,
        target,
        COUNT(*) as count
    FROM {TEAM_NAME}_heart_disease
    GROUP BY 1, 2
    ORDER BY 1
""").toPandas()

fig1 = _____  # YOUR CODE HERE
fig1.show()

# COMMAND ----------

# TODO: Visualization 2 - Scatter plot: Max heart rate vs age
df_viz2 = spark.sql(f"""
    SELECT age, thalach as max_heart_rate, target
    FROM {TEAM_NAME}_heart_disease
""").toPandas()

fig2 = _____  # YOUR CODE HERE
fig2.show()

# COMMAND ----------

# TODO: Visualization 3 - Your choice!

# YOUR CODE HERE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create an AI/BI Dashboard
# MAGIC
# MAGIC **TODO:** Create a Lakeview Dashboard in the Databricks UI.
# MAGIC
# MAGIC ### Steps:
# MAGIC 1. Click **+ New** in the sidebar, then **Dashboard**
# MAGIC 2. Add a **Dataset** pointing to your heart disease table
# MAGIC 3. Add visualizations:
# MAGIC    - At least 1 bar/column chart
# MAGIC    - At least 1 counter/KPI widget
# MAGIC    - At least 1 table or detail view
# MAGIC 4. Add filters (e.g., filter by age group, sex, chest pain type)
# MAGIC 5. **Publish** the dashboard
# MAGIC
# MAGIC > This step is done in the Databricks UI, not in this notebook.
# MAGIC > Use the queries and visualizations above as a starting point.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Set Up a Genie Space
# MAGIC
# MAGIC **TODO:** Create a Genie space connected to your data.
# MAGIC
# MAGIC ### Steps:
# MAGIC 1. Click **+ New** in the sidebar, then **Genie space**
# MAGIC 2. Give it a name: `{TEAM_NAME} Heart Disease Analyst`
# MAGIC 3. Add your tables as data sources:
# MAGIC    - `{TEAM_NAME}_heart_disease` (or `heart_disease`)
# MAGIC    - `{TEAM_NAME}_heart_disease_gold` (if available from Event 1)
# MAGIC 4. Add instructions to help Genie understand your data:
# MAGIC    - "target = 1 means the patient has heart disease, target = 0 means healthy"
# MAGIC    - "cp is chest pain type: 0=typical angina, 1=atypical, 2=non-anginal, 3=asymptomatic"
# MAGIC    - "sex: 1=male, 0=female"
# MAGIC 5. Test with sample questions:
# MAGIC    - "How many patients have heart disease?"
# MAGIC    - "What is the average age of patients with heart disease?"
# MAGIC    - "Show me the distribution of chest pain types"
# MAGIC
# MAGIC > This step is done in the Databricks UI, not in this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Benchmark Questions
# MAGIC
# MAGIC The organizer will announce benchmark questions. Use your dashboard,
# MAGIC Genie space, and SQL skills to answer them as fast and accurately as possible.
# MAGIC
# MAGIC Record your answers below:

# COMMAND ----------

# Record your benchmark answers here
answers = {
    "Q1": "_____",
    "Q2": "_____",
    "Q3": "_____",
    "Q4": "_____",
    "Q5": "_____",
}

print("=" * 60)
print(f"  BENCHMARK ANSWERS — {TEAM_NAME}")
print("=" * 60)
for q, a in answers.items():
    print(f"  {q}: {a}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stretch Goals (Extra Credit)
# MAGIC
# MAGIC Finished early? Try these with the Databricks Assistant:
# MAGIC
# MAGIC 1. **Cross-table analysis** — Ask: *"Join heart_disease and life_expectancy tables and create a scatter plot comparing avg cholesterol by country life expectancy"*
# MAGIC 2. **Statistical tests** — Ask: *"Run a chi-squared test to determine if chest pain type is statistically associated with heart disease"*
# MAGIC 3. **Advanced dashboard** — Ask: *"What are the best dashboard design practices for healthcare data? Suggest a layout for my heart disease dashboard"*
# MAGIC 4. **Genie optimization** — Ask: *"Write comprehensive Genie instructions that include example questions, column descriptions, and common calculation patterns"*
# MAGIC 5. **AI Functions in SQL** — Ask: *"Use ai_query() to generate a natural language summary of the heart disease statistics"*

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
