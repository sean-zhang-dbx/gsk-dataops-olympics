# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
# Step 0: Prepare data
spark.sql(f"CREATE OR REPLACE TABLE {CATALOG}.default.heart_silver_correct AS SELECT * FROM {SHARED_CATALOG}.default.heart_disease")
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.heart_gold_correct AS
    SELECT CASE WHEN age < 40 THEN 'Under 40' WHEN age BETWEEN 40 AND 49 THEN '40-49' WHEN age BETWEEN 50 AND 59 THEN '50-59' ELSE '60+' END AS age_group,
    CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
    COUNT(*) AS patient_count, ROUND(AVG(chol), 1) AS avg_cholesterol, ROUND(AVG(trestbps), 1) AS avg_blood_pressure, ROUND(AVG(thalach), 1) AS avg_max_heart_rate
    FROM {CATALOG}.default.heart_silver_correct GROUP BY 1, 2""")
print(f"Step 0 done: silver={spark.table(f'{CATALOG}.default.heart_silver_correct').count()}, gold={spark.table(f'{CATALOG}.default.heart_gold_correct').count()}")
# COMMAND ----------
# Create event2_submissions table
spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.default.event2_submissions (
    team STRING, question_id STRING, answer STRING, method STRING, submitted_at TIMESTAMP
)""")
# COMMAND ----------
# Submit all 8 correct answers via SQL
from datetime import datetime
answers = [
    ("Q1", "264"), ("Q2", "Heart Disease: 56.1, Healthy: 49.5"), ("Q3", "46.8%"),
    ("Q4", "3 (asymptomatic)"), ("Q5", "253.8, 233.6"), ("Q6", "71"),
    ("Q7", "60+ (65.2%)"), ("Q8", "150.4")
]
for qid, ans in answers:
    spark.sql(f"""INSERT INTO {CATALOG}.default.event2_submissions VALUES ('{TEAM_NAME}', '{qid}', '{ans}', 'SQL', current_timestamp())""")
print(f"Submitted {len(answers)} answers")
# COMMAND ----------
submit("event2", {"answers": 8, "method": "SQL"})
