# Databricks notebook source
# MAGIC %md
# MAGIC # Lightning Talk 2: Data Analytics Demo
# MAGIC
# MAGIC **For organizers only — this is the 15-minute demo shown before Event 2.**
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Analytical SQL queries on heart disease data
# MAGIC 2. Creating visualizations for a dashboard
# MAGIC 3. How to set up a Genie space (live demo in UI)
# MAGIC
# MAGIC ## What to show live (in Databricks UI, not in this notebook):
# MAGIC - Create an AI/BI Dashboard with 3-4 widgets
# MAGIC - Create a Genie space and ask it questions
# MAGIC - Show how Genie generates SQL from natural language

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Analytical SQL Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Heart disease prevalence
# MAGIC SELECT
# MAGIC     CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END as status,
# MAGIC     COUNT(*) as patient_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
# MAGIC FROM demo_heart_disease
# MAGIC GROUP BY target

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Age distribution by heart disease status
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN age < 40 THEN 'Under 40'
# MAGIC         WHEN age < 50 THEN '40-49'
# MAGIC         WHEN age < 60 THEN '50-59'
# MAGIC         ELSE '60+'
# MAGIC     END as age_group,
# MAGIC     CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END as status,
# MAGIC     COUNT(*) as count
# MAGIC FROM demo_heart_disease
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Chest pain type analysis
# MAGIC SELECT
# MAGIC     CASE cp
# MAGIC         WHEN 0 THEN 'Typical Angina'
# MAGIC         WHEN 1 THEN 'Atypical Angina'
# MAGIC         WHEN 2 THEN 'Non-Anginal Pain'
# MAGIC         WHEN 3 THEN 'Asymptomatic'
# MAGIC     END as chest_pain_type,
# MAGIC     COUNT(*) as count,
# MAGIC     ROUND(AVG(CASE WHEN target = 1 THEN 1.0 ELSE 0.0 END) * 100, 1) as heart_disease_rate_pct
# MAGIC FROM demo_heart_disease
# MAGIC GROUP BY cp
# MAGIC ORDER BY cp

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Key KPIs
# MAGIC SELECT
# MAGIC     COUNT(*) as total_patients,
# MAGIC     SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) as with_disease,
# MAGIC     ROUND(AVG(age), 1) as avg_age,
# MAGIC     ROUND(AVG(chol), 1) as avg_cholesterol,
# MAGIC     ROUND(AVG(thalach), 1) as avg_max_heart_rate
# MAGIC FROM demo_heart_disease

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Visualizations (for Dashboard)

# COMMAND ----------

import plotly.express as px

# Scatter: Max heart rate vs Age, colored by disease status
df_scatter = spark.sql("""
    SELECT age, thalach as max_heart_rate,
           CASE WHEN target = 1 THEN 'Disease' ELSE 'Healthy' END as status
    FROM demo_heart_disease
""").toPandas()

fig = px.scatter(df_scatter, x="age", y="max_heart_rate", color="status",
                 title="Max Heart Rate vs Age by Disease Status",
                 labels={"age": "Age", "max_heart_rate": "Max Heart Rate (bpm)"},
                 template="plotly_white")
fig.show()

# COMMAND ----------

# Bar chart: Disease rate by age group
df_bar = spark.sql("""
    SELECT
        CASE
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END as age_group,
        ROUND(AVG(CASE WHEN target = 1 THEN 1.0 ELSE 0.0 END) * 100, 1) as disease_rate
    FROM demo_heart_disease
    GROUP BY 1
    ORDER BY 1
""").toPandas()

fig2 = px.bar(df_bar, x="age_group", y="disease_rate",
              title="Heart Disease Rate by Age Group",
              labels={"disease_rate": "Disease Rate (%)", "age_group": "Age Group"},
              template="plotly_white", color="disease_rate",
              color_continuous_scale="Reds")
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Genie Space Setup (Live Demo)
# MAGIC
# MAGIC **Show this live in the Databricks UI:**
# MAGIC
# MAGIC 1. Click **+ New** > **Genie space**
# MAGIC 2. Name: "Heart Disease Analyst"
# MAGIC 3. Add `demo_heart_disease` as data source
# MAGIC 4. Add instructions:
# MAGIC    - "target = 1 means heart disease, 0 means healthy"
# MAGIC    - "cp is chest pain type: 0=typical angina, 1=atypical, 2=non-anginal, 3=asymptomatic"
# MAGIC    - "sex: 1=male, 0=female"
# MAGIC 5. Ask Genie:
# MAGIC    - "How many patients have heart disease?"
# MAGIC    - "What age group has the highest disease rate?"
# MAGIC    - "Show me average cholesterol by sex"
# MAGIC
# MAGIC **Key point:** Genie writes SQL for you — no coding needed for ad-hoc analytics!
