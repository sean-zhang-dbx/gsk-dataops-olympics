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
answers = [("Q1", "264"), ("Q2", "55, 50"), ("Q3", "50%"), ("Q4", "2"), ("Q5", "200, 200"), ("Q6", "264"), ("Q7", "40-49"), ("Q8", "160")]
for qid, ans in answers:
    spark.sql(f"INSERT INTO {CATALOG}.default.event2_submissions VALUES ('{TEAM_NAME}', '{qid}', '{ans}', 'SQL', current_timestamp())")
print("Submitted 8 answers (most wrong)")
# COMMAND ----------
submit("event2", {"answers": 8})
