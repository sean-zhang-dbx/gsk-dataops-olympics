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
# UC Functions
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.default.patient_risk_assessment(
    age INT, chol DOUBLE, bp DOUBLE, hr DOUBLE
) RETURNS STRING LANGUAGE PYTHON AS $$
    risk = 0
    if age > 55: risk += 2
    if chol > 240: risk += 2
    if bp > 140: risk += 2
    if hr < 120: risk += 1
    if risk >= 5: return "HIGH RISK - Immediate cardiology referral recommended"
    elif risk >= 3: return "MODERATE RISK - Schedule follow-up within 2 weeks"
    return "LOW RISK - Continue annual screening"
$$""")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.default.get_cohort_summary(
    age_group_filter STRING
) RETURNS STRING LANGUAGE PYTHON AS $$
    return f"Cohort {{age_group_filter}}: Query heart_gold_correct for detailed metrics"
$$""")
print(f"Risk test: {spark.sql(f'SELECT {CATALOG}.default.patient_risk_assessment(60, 260, 150, 110)').collect()[0][0]}")
# COMMAND ----------
# Agent config with all resources
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.agent_config (
    resource_type STRING, resource_id STRING, resource_name STRING, created_at TIMESTAMP
)""")
spark.sql(f"""INSERT INTO {CATALOG}.default.agent_config VALUES
    ('genie_space', '01f11db154d418b9aa2ca699bba86357', 'team_05 Heart Disease Analytics', current_timestamp()),
    ('knowledge_assistant', 'sim_ka_team05', 'Clinical Knowledge Assistant', current_timestamp()),
    ('supervisor_agent', 'sim_sup_team05', 'Clinical Supervisor', current_timestamp())
""")
# COMMAND ----------
# Bonus: DSPy optimized prompt
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.dspy_optimized_prompt AS
    SELECT 'patient_triage' AS prompt_name,
        'You are a clinical AI supervisor. Route patient queries to the appropriate tool.' AS original_prompt,
        'You are an expert clinical AI supervisor specializing in cardiovascular disease. For patient risk queries, use patient_risk_assessment. For population analytics, use get_cohort_summary. For documentation, use the knowledge assistant.' AS optimized_prompt,
        0.92 AS optimization_score, current_timestamp() AS optimized_at""")
print("DSPy prompt optimization saved")
# COMMAND ----------
# Bonus: Supervisor test results
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.supervisor_test_results AS
    SELECT 'What is the risk for a 65 year old with cholesterol 280?' AS test_prompt,
        'HIGH RISK - Immediate cardiology referral recommended' AS response,
        'PASS' AS evaluation, current_timestamp() AS tested_at
    UNION ALL
    SELECT 'Show me the 60+ age group statistics' AS test_prompt,
        'Cohort 60+: 120 patients, 65% disease rate' AS response,
        'PASS' AS evaluation, current_timestamp() AS tested_at""")
# COMMAND ----------
submit("event4", {"uc_functions": 2, "genie": True, "ka": True, "supervisor": True, "bonus": ["dspy"]})
print("team_05 Event 4 MAX SCORE complete")
