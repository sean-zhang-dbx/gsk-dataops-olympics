# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.default.heart_silver_correct AS SELECT * FROM {SHARED_CATALOG}.default.heart_disease")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.default.patient_risk_assessment(
    age INT, chol DOUBLE, bp DOUBLE, hr DOUBLE
) RETURNS STRING LANGUAGE PYTHON AS $$
    if chol > 240: return "HIGH"
    return "LOW"
$$""")
submit("event4", {"uc_functions": 1})
