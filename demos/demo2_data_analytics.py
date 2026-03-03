# Databricks notebook source
# MAGIC %md
# MAGIC # Lightning Talk 2: Data Analytics & AI/BI on Databricks
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
# MAGIC | 5:00 | **Plotly Viz** | Interactive charts from SQL results |
# MAGIC | 8:00 | **AI/BI Dashboard** | Quick tour of Lakeview dashboards (UI walkthrough) |
# MAGIC | 10:00 | **Wow Moment** | Genie — ask data questions in plain English |
# MAGIC | 14:00 | **Your Turn** | Preview of the practice notebook |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Problem
# MAGIC
# MAGIC > **Say this:** "Your medical director walks in and says: 'Which age group in our
# MAGIC > patient cohort has the highest heart disease rate?' You have 60 seconds. Go."
# MAGIC >
# MAGIC > "That's what analytics on Databricks looks like — SQL first, viz second, Genie for
# MAGIC > when you need answers even faster."

# COMMAND ----------

spark.sql("USE dataops_olympics")

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
# MAGIC > **Say this:** "One query. We now know the exact split. Notice the window function
# MAGIC > `SUM(COUNT(*)) OVER()` — that's how you calculate percentages in SQL without a subquery."

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
# MAGIC > **Say this:** "Four queries, four different insights. Notice how I used CASE statements
# MAGIC > to make the output human-readable. That's a tip for your dashboards too."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Interactive Visualizations with Plotly
# MAGIC
# MAGIC > **Say this:** "SQL gives you numbers. Charts give you understanding. Let me turn
# MAGIC > those query results into something visual."

# COMMAND ----------

import plotly.express as px

df_scatter = spark.sql("""
    SELECT age, thalach as max_heart_rate, chol as cholesterol,
           CASE WHEN target = 1 THEN 'Disease' ELSE 'Healthy' END as status
    FROM heart_disease
""").toPandas()

fig = px.scatter(df_scatter, x="age", y="max_heart_rate", color="status",
                 size="cholesterol", hover_data=["cholesterol"],
                 title="Heart Rate vs Age — Each Dot is a Patient",
                 labels={"age": "Age (years)", "max_heart_rate": "Max Heart Rate (bpm)"},
                 template="plotly_white", opacity=0.7,
                 color_discrete_map={"Disease": "#e74c3c", "Healthy": "#2ecc71"})
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "Look at the pattern. Healthy patients (green) tend to have higher
# MAGIC > max heart rates. Diseased patients cluster lower. The dot size is cholesterol.
# MAGIC > This is a 4-dimensional view in one chart."

# COMMAND ----------

df_bar = spark.sql("""
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
""").toPandas()

fig2 = px.bar(df_bar, x="age_group", y="patients", color="status", barmode="group",
              title="Patient Count by Age Group and Disease Status",
              labels={"age_group": "Age Group", "patients": "Patients"},
              template="plotly_white",
              color_discrete_map={"Disease": "#e74c3c", "Healthy": "#2ecc71"})
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. AI/BI Dashboards (Lakeview)
# MAGIC
# MAGIC > **Say this:** "In the competition, you'll build a dashboard in the Databricks UI.
# MAGIC > Let me show you how fast it is."
# MAGIC >
# MAGIC > **LIVE UI DEMO (2 min):**
# MAGIC > 1. Click **+ New** → **Dashboard**
# MAGIC > 2. Add dataset: `SELECT * FROM heart_disease`
# MAGIC > 3. Drag in a Counter widget (total patients)
# MAGIC > 4. Drag in a Bar chart (disease by age group)
# MAGIC > 5. Add a filter on `target`
# MAGIC > 6. Click **Publish**
# MAGIC >
# MAGIC > **Say this:** "5 clicks, 1 dashboard. No code required. But YOU will use the
# MAGIC > SQL skills you just learned to make yours better."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Wow Moment — Genie (Natural Language Analytics)
# MAGIC
# MAGIC > **Say this:** "What if you didn't need to write SQL at all? What if you could just
# MAGIC > ask your data a question in English?"
# MAGIC >
# MAGIC > **LIVE UI DEMO (3 min):**
# MAGIC > 1. Open an existing Genie space (or create: **+ New** → **Genie space**)
# MAGIC > 2. Name: "Heart Disease Analyst"
# MAGIC > 3. Add table: `heart_disease`
# MAGIC > 4. Add instructions:
# MAGIC >    - "target = 1 means heart disease, 0 means healthy"
# MAGIC >    - "cp is chest pain type: 0=typical angina, 1=atypical, 2=non-anginal, 3=asymptomatic"
# MAGIC > 5. Ask: **"What percentage of patients over 50 have heart disease?"**
# MAGIC > 6. Show the SQL that Genie generates
# MAGIC > 7. Ask: **"Break that down by chest pain type"**
# MAGIC >
# MAGIC > **Say this:** "Genie wrote the SQL for you. It understood 'over 50' means age > 50,
# MAGIC > it knew 'heart disease' means target = 1. This is the future of data analytics."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Turn! (Preview)
# MAGIC
# MAGIC > **Say this:** "In the practice notebook, you'll write 2 SQL queries and create
# MAGIC > 1 Plotly chart. It should take about 10 minutes. Then in the competition,
# MAGIC > you'll build a full dashboard and Genie space. Let's go!"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **SQL is your superpower** — CASE, window functions, GROUP BY solve 90% of analytics
# MAGIC 2. **Plotly** creates interactive charts from any DataFrame in 3 lines
# MAGIC 3. **AI/BI Dashboards** (Lakeview) — drag-and-drop dashboards, no code needed
# MAGIC 4. **Genie** — ask questions in English, get SQL answers automatically
# MAGIC 5. **Use the Databricks Assistant** (`Cmd+I`) to write SQL queries from English descriptions
