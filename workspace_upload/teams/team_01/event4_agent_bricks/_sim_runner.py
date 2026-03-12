# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
# Step 0: Ensure tables exist
spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.default.heart_silver_correct AS SELECT * FROM {SHARED_CATALOG}.default.heart_disease")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.default.heart_gold_correct AS
    SELECT CASE WHEN age < 40 THEN 'Under 40' WHEN age BETWEEN 40 AND 49 THEN '40-49' WHEN age BETWEEN 50 AND 59 THEN '50-59' ELSE '60+' END AS age_group,
    CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
    COUNT(*) AS patient_count, ROUND(AVG(chol), 1) AS avg_cholesterol, ROUND(AVG(trestbps), 1) AS avg_blood_pressure, ROUND(AVG(thalach), 1) AS avg_max_heart_rate
    FROM {CATALOG}.default.heart_silver_correct GROUP BY 1, 2""")
# COMMAND ----------
# UC Function 1: patient_risk_assessment
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.default.patient_risk_assessment(
    age INT, chol DOUBLE, bp DOUBLE, hr DOUBLE
) RETURNS STRING
LANGUAGE PYTHON AS $$
    risk_score = 0
    if age > 55: risk_score += 2
    if chol > 240: risk_score += 2
    if bp > 140: risk_score += 2
    if hr < 120: risk_score += 1
    if risk_score >= 5: return "HIGH RISK"
    elif risk_score >= 3: return "MODERATE RISK"
    else: return "LOW RISK"
$$""")
# COMMAND ----------
# UC Function 2: get_cohort_summary
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.default.get_cohort_summary(
    age_group_filter STRING
) RETURNS STRING
LANGUAGE PYTHON AS $$
    return f"Cohort summary for {{age_group_filter}}: see heart_gold_correct table"
$$""")
# COMMAND ----------
# Test functions
result = spark.sql(f"SELECT {CATALOG}.default.patient_risk_assessment(55, 250, 145, 150)").collect()
print(f"Risk test: {result[0][0]}")
# COMMAND ----------
# Agent config table (registers resources)
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.agent_config (
    resource_type STRING, resource_id STRING, resource_name STRING, created_at TIMESTAMP
)""")
spark.sql(f"""INSERT INTO {CATALOG}.default.agent_config VALUES
    ('genie_space', '01f11db153f51e23b3e5934d2e6b9fbd', 'team_01 Heart Disease Analytics', current_timestamp())
""")
# COMMAND ----------
submit("event4", {"uc_functions": 2, "genie": True})
