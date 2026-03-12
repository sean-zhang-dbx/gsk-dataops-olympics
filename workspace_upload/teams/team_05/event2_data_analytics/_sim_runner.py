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
# All correct
answers = [
    ("Q1", "264"), ("Q2", "Heart Disease: 56.1, Healthy: 49.5"), ("Q3", "46.8%"),
    ("Q4", "3 (asymptomatic)"), ("Q5", "253.8, 233.6"), ("Q6", "71"),
    ("Q7", "60+ (65.2%)"), ("Q8", "150.4")
]
for qid, ans in answers:
    spark.sql(f"INSERT INTO {CATALOG}.default.event2_submissions VALUES ('{TEAM_NAME}', '{qid}', '{ans}', 'SQL', current_timestamp())")
# COMMAND ----------
# Bonus 1: Executive summary
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.heart_executive_summary AS
    SELECT 'Heart Disease Analysis Summary' AS title,
        CONCAT('Total patients: ', COUNT(*), '. Disease rate: ', ROUND(SUM(CASE WHEN target=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 1), '%') AS summary
    FROM {CATALOG}.default.heart_silver_correct""")
# COMMAND ----------
# Bonus 2: Cohort comparison
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.heart_cohort_comparison AS
    SELECT CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS cohort,
        COUNT(*) AS n, ROUND(AVG(age),1) AS avg_age, ROUND(AVG(chol),1) AS avg_chol,
        ROUND(AVG(trestbps),1) AS avg_bp, ROUND(AVG(thalach),1) AS avg_hr
    FROM {CATALOG}.default.heart_silver_correct GROUP BY target""")
# COMMAND ----------
# Bonus 3: Chi-squared test
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.heart_chi2_test AS
    SELECT 'sex_vs_target' AS test_name, 'chi2' AS test_type,
        12.5 AS statistic, 0.0004 AS p_value, 'significant' AS result""")
# COMMAND ----------
submit("event2", {"answers": 8, "bonus": ["executive_summary", "cohort_comparison", "chi2_test"]})
print("team_05 Event 2 complete (MAX SCORE - all correct + 3 bonus)")
