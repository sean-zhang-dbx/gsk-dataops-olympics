# Databricks notebook source
# MAGIC %md
# MAGIC # Lightning Talk 2: Data Analytics — SQL, Dashboards & Genie
# MAGIC
# MAGIC **Duration: 15 minutes** | Instructor-led live demo
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Agenda
# MAGIC | Time | Section | What You'll See |
# MAGIC |------|---------|----------------|
# MAGIC | 0:00 | **The Problem** | Your CEO asks a question — how fast can you answer? |
# MAGIC | 1:00 | **SQL Power** | 4 analytical queries, each more insightful |
# MAGIC | 5:00 | **Visualizations** | Interactive charts from SQL results |
# MAGIC | 8:00 | **AI/BI Dashboard** | Quick tour of Lakeview dashboards (UI walkthrough) |
# MAGIC | 10:00 | **Wow Moment** | Genie — ask data questions in plain English |
# MAGIC | 14:00 | **Your Turn** | Preview of the practice notebook |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Problem
# MAGIC
# MAGIC Your medical director walks in: *"Which age group in our patient cohort has the highest heart disease rate?"*
# MAGIC You have 60 seconds. That's what analytics on Databricks looks like —
# MAGIC SQL first, viz second, Genie for when you need answers even faster.

# COMMAND ----------

spark.sql("USE CATALOG dataops_olympics")
spark.sql("USE SCHEMA default")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. SQL Analytics — Answer Real Questions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Question: What's the heart disease prevalence in our dataset?
# MAGIC SELECT
# MAGIC     CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END as diagnosis,
# MAGIC     COUNT(*) as patients,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pct
# MAGIC FROM heart_disease
# MAGIC GROUP BY target
# MAGIC ORDER BY target

# COMMAND ----------

# MAGIC %md
# MAGIC One query and we know the exact split. Notice the window function
# MAGIC `SUM(COUNT(*)) OVER()` — that's how you calculate percentages in SQL without a subquery.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Question: Which age group is most at risk?
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN age < 40 THEN '1. Under 40'
# MAGIC         WHEN age < 50 THEN '2. 40-49'
# MAGIC         WHEN age < 60 THEN '3. 50-59'
# MAGIC         ELSE '4. 60+'
# MAGIC     END as age_group,
# MAGIC     COUNT(*) as total,
# MAGIC     SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) as diseased,
# MAGIC     ROUND(AVG(CASE WHEN target = 1 THEN 1.0 ELSE 0.0 END) * 100, 1) as disease_rate_pct
# MAGIC FROM heart_disease
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Question: Does chest pain type predict heart disease?
# MAGIC SELECT
# MAGIC     CASE cp
# MAGIC         WHEN 0 THEN 'Typical Angina'
# MAGIC         WHEN 1 THEN 'Atypical Angina'
# MAGIC         WHEN 2 THEN 'Non-Anginal'
# MAGIC         WHEN 3 THEN 'Asymptomatic'
# MAGIC     END as chest_pain_type,
# MAGIC     COUNT(*) as patients,
# MAGIC     ROUND(AVG(CASE WHEN target = 1 THEN 1.0 ELSE 0.0 END) * 100, 1) as disease_rate_pct,
# MAGIC     ROUND(AVG(chol), 1) as avg_cholesterol,
# MAGIC     ROUND(AVG(thalach), 1) as avg_max_heart_rate
# MAGIC FROM heart_disease
# MAGIC GROUP BY cp
# MAGIC ORDER BY cp

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI Summary — this would be the top row of a dashboard
# MAGIC SELECT
# MAGIC     COUNT(*) as total_patients,
# MAGIC     SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) as with_disease,
# MAGIC     ROUND(AVG(age), 1) as avg_age,
# MAGIC     ROUND(AVG(chol), 1) as avg_cholesterol,
# MAGIC     ROUND(AVG(thalach), 1) as avg_max_heart_rate
# MAGIC FROM heart_disease

# COMMAND ----------

# MAGIC %md
# MAGIC Four queries, four different insights. Notice the CASE statements to make the output
# MAGIC human-readable — that's a pro tip for your dashboards too.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Interactive Visualizations
# MAGIC
# MAGIC SQL gives you numbers — charts give you understanding. In Databricks, just use `display()`
# MAGIC and click the chart icon to configure your visualization.

# COMMAND ----------

display(spark.sql("""
    SELECT age, thalach as max_heart_rate, chol as cholesterol,
           CASE WHEN target = 1 THEN 'Disease' ELSE 'Healthy' END as status
    FROM heart_disease
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Click the **chart icon** below the table → switch to a scatter plot.
# MAGIC Map age to X, max_heart_rate to Y, and color by status.
# MAGIC Notice the pattern: healthy patients tend to have higher max heart rates.

# COMMAND ----------

display(spark.sql("""
    SELECT
        CASE
            WHEN age < 40 THEN '1. Under 40'
            WHEN age < 50 THEN '2. 40-49'
            WHEN age < 60 THEN '3. 50-59'
            ELSE '4. 60+'
        END as age_group,
        CASE WHEN target = 1 THEN 'Disease' ELSE 'Healthy' END as status,
        COUNT(*) as patients
    FROM heart_disease
    GROUP BY 1, 2
    ORDER BY 1, 2
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. AI/BI Dashboards (Lakeview)
# MAGIC
# MAGIC In the competition, you'll build a dashboard in the Databricks UI. Here's how fast it is:
# MAGIC
# MAGIC 1. Click **+ New** → **Dashboard**
# MAGIC 2. Add dataset: `SELECT * FROM heart_disease`
# MAGIC 3. Drag in a Counter widget (total patients)
# MAGIC 4. Drag in a Bar chart (disease by age group)
# MAGIC 5. Add a filter on `target`
# MAGIC 6. Click **Publish**
# MAGIC
# MAGIC 5 clicks, 1 dashboard. No code required — but your SQL skills will make yours better.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Wow Moment — Genie (Natural Language Analytics)
# MAGIC
# MAGIC What if you didn't need to write SQL at all? What if you could just ask your data a question in English?
# MAGIC
# MAGIC **Genie demo:**
# MAGIC 1. Open an existing Genie space (or create: **+ New** → **Genie space**)
# MAGIC 2. Name: "Heart Disease Analyst"
# MAGIC 3. Add table: `heart_disease`
# MAGIC 4. Add instructions:
# MAGIC    - "target = 1 means heart disease, 0 means healthy"
# MAGIC    - "cp is chest pain type: 0=typical angina, 1=atypical, 2=non-anginal, 3=asymptomatic"
# MAGIC 5. Ask: **"What percentage of patients over 50 have heart disease?"**
# MAGIC 6. Check out the SQL that Genie generates
# MAGIC 7. Ask: **"Break that down by chest pain type"**
# MAGIC
# MAGIC Genie wrote the SQL for us. It understood "over 50" means `age > 50`, and "heart disease" means `target = 1`.
# MAGIC This is the future of data analytics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Turn! (Preview)
# MAGIC
# MAGIC In the practice notebook, you'll write 2 SQL queries and create a visualization (~10 minutes).
# MAGIC Then in the competition, you'll build a full AI/BI dashboard and a Genie space.
# MAGIC Pro tip: **build your Genie space well** — you'll reuse it in Event 4 when you build your AI agent.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **SQL is your superpower** — CASE, window functions, GROUP BY solve 90% of analytics
# MAGIC 2. **`display()`** creates interactive charts from any DataFrame -- click the chart icon
# MAGIC 3. **AI/BI Dashboards** (Lakeview) — drag-and-drop dashboards, no code needed
# MAGIC 4. **Genie** — ask questions in English, get SQL answers automatically
# MAGIC 5. **Use the Databricks Assistant** (`Cmd+I`) to write SQL queries from English descriptions
