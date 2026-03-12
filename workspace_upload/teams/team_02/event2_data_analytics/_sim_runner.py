# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
spark.sql(f"CREATE OR REPLACE TABLE {CATALOG}.default.heart_silver_correct AS SELECT * FROM {SHARED_CATALOG}.default.heart_disease")
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.heart_gold_correct AS
    SELECT CASE WHEN age < 40 THEN 'Under 40' WHEN age BETWEEN 40 AND 49 THEN '40-49' WHEN age BETWEEN 50 AND 59 THEN '50-59' ELSE '60+' END AS age_group,
    CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
    COUNT(*) AS patient_count, ROUND(AVG(chol), 1) AS avg_cholesterol, ROUND(AVG(trestbps), 1) AS avg_blood_pressure, ROUND(AVG(thalach), 1) AS avg_max_heart_rate
    FROM {CATALOG}.default.heart_silver_correct GROUP BY 1, 2""")
# COMMAND ----------
spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.default.event2_submissions (
    team STRING, question_id STRING, answer STRING, method STRING, submitted_at TIMESTAMP)""")
# COMMAND ----------
# First attempt - wrong answers for Q5 and Q7
import time
wrong_answers = [
    ("Q1", "264"), ("Q2", "Heart Disease: 56.1, Healthy: 49.5"), ("Q3", "46.8%"),
    ("Q4", "3 (asymptomatic)"), ("Q5", "250, 230"), ("Q6", "71"),
    ("Q7", "50-59 (55%)"), ("Q8", "150.4")
]
for qid, ans in wrong_answers:
    spark.sql(f"INSERT INTO {CATALOG}.default.event2_submissions VALUES ('{TEAM_NAME}', '{qid}', '{ans}', 'SQL', current_timestamp())")
time.sleep(1)
# Second attempt - fix Q5 but Q7 still wrong
spark.sql(f"INSERT INTO {CATALOG}.default.event2_submissions VALUES ('{TEAM_NAME}', 'Q5', '253.8, 233.6', 'SQL', current_timestamp())")
print("Submitted 8 answers (Q5 fixed, Q7 still wrong)")
# COMMAND ----------
submit("event2", {"answers": 8, "method": "SQL", "note": "fixed Q5"})
