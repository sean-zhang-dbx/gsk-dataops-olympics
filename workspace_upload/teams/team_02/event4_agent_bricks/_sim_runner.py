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
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.default.patient_risk_assessment(
    age INT, chol DOUBLE, bp DOUBLE, hr DOUBLE
) RETURNS STRING LANGUAGE PYTHON AS $$
    if age > 60 and chol > 250: return "HIGH RISK"
    elif age > 50: return "MODERATE RISK"
    return "LOW RISK"
$$""")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.default.get_cohort_summary(
    group_name STRING
) RETURNS STRING LANGUAGE PYTHON AS $$
    return f"Summary for {{group_name}}"
$$""")
# COMMAND ----------
submit("event4", {"uc_functions": 2})
