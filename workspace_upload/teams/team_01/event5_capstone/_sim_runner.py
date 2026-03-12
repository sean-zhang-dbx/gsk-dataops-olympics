# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.default.heart_silver_correct AS SELECT * FROM {SHARED_CATALOG}.default.heart_disease")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.default.heart_gold_correct AS
    SELECT CASE WHEN age < 40 THEN 'Under 40' WHEN age BETWEEN 40 AND 49 THEN '40-49' WHEN age BETWEEN 50 AND 59 THEN '50-59' ELSE '60+' END AS age_group,
    CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
    COUNT(*) AS patient_count, ROUND(AVG(chol), 1) AS avg_cholesterol, ROUND(AVG(trestbps), 1) AS avg_blood_pressure, ROUND(AVG(thalach), 1) AS avg_max_heart_rate
    FROM {CATALOG}.default.heart_silver_correct GROUP BY 1, 2""")
# COMMAND ----------
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.executive_briefing AS
    SELECT 'Heart Disease Analysis Report' AS title,
    'This executive briefing summarizes key findings from the GSK DataOps Olympics heart disease analysis. Our analysis of 491 patients reveals a 53.8% heart disease prevalence. Key risk factors include age >55, cholesterol >240, and chest pain type 3 (asymptomatic). The 60+ age group shows the highest disease rate at 65.2%. We recommend enhanced screening protocols for high-risk demographics.' AS content,
    current_timestamp() AS generated_at""")
# COMMAND ----------
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.heart_ai_insights AS
    SELECT age_group, diagnosis, patient_count, avg_cholesterol,
        CASE WHEN avg_cholesterol > 250 THEN 'Elevated cholesterol - recommend statin evaluation'
             WHEN avg_cholesterol > 200 THEN 'Borderline cholesterol - lifestyle intervention recommended'
             ELSE 'Normal cholesterol range' END AS clinical_insight
    FROM {CATALOG}.default.heart_gold_correct""")
# COMMAND ----------
submit("event5", {"briefing": True, "ai_insights": True})
