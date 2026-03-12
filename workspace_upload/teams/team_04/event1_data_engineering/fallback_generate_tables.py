# Databricks notebook source
# MAGIC %md
# MAGIC # Event 1: Fallback — Generate Correct Tables
# MAGIC
# MAGIC **Run this if your team didn't finish the challenge.**
# MAGIC
# MAGIC This notebook generates correct Bronze, Silver, and Gold tables with full governance
# MAGIC metadata so you can proceed to the next events (Analytics, ML, GenAI).
# MAGIC
# MAGIC Just set your **TEAM_NAME** below and click **Run All**.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

RAW_PATH = f"/Volumes/{SHARED_CATALOG}/{SHARED_SCHEMA}/raw_data/heart_events/"
print(f"Generating tables for: {CATALOG}.default")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Raw Ingestion
# MAGIC Load all 5 NDJSON batch files from the shared volume.

# COMMAND ----------

df_bronze = spark.read.json(f"{RAW_PATH}*.json")

df_bronze.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{CATALOG}.default.heart_bronze")

bronze_count = spark.table(f"{CATALOG}.default.heart_bronze").count()
print(f"Bronze: {bronze_count} rows ingested")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Cleaned & Deduplicated
# MAGIC Apply data quality filters and remove duplicate `event_id`s.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_silver AS
    SELECT *, current_timestamp() AS ingested_at
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp) AS _rn
        FROM {CATALOG}.default.heart_bronze
    )
    WHERE _rn = 1
      AND age IS NOT NULL AND age BETWEEN 1 AND 120
      AND trestbps BETWEEN 50 AND 300
      AND chol >= 0
""")

silver_count = spark.table(f"{CATALOG}.default.heart_silver").count()
print(f"Silver: {silver_count} rows (removed {bronze_count - silver_count} dirty/duplicate rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Aggregated Summary
# MAGIC Group by age range and diagnosis for dashboards and analytics.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_gold AS
    SELECT
        CASE
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END AS age_group,
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
        COUNT(*) AS patient_count,
        ROUND(AVG(chol), 1) AS avg_cholesterol,
        ROUND(AVG(trestbps), 1) AS avg_blood_pressure,
        ROUND(AVG(thalach), 1) AS avg_max_heart_rate
    FROM {CATALOG}.default.heart_silver
    GROUP BY 1, 2
    ORDER BY 1, 2
""")

display(spark.table(f"{CATALOG}.default.heart_gold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance — Table & Column Comments

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {CATALOG}.default.heart_bronze
    SET TBLPROPERTIES ('comment' = 'Raw patient intake events from hospital EHR system — 5 NDJSON batch files, unmodified')
""")

spark.sql(f"""
    ALTER TABLE {CATALOG}.default.heart_silver
    SET TBLPROPERTIES ('comment' = 'Cleaned patient intake data — validated age/BP/cholesterol, deduplicated on event_id')
""")

spark.sql(f"""
    ALTER TABLE {CATALOG}.default.heart_gold
    SET TBLPROPERTIES ('comment' = 'Heart disease metrics aggregated by age group and diagnosis for dashboards')
""")

column_comments = {
    "heart_silver": {
        "age": "Patient age in years (validated 1-120)",
        "trestbps": "Resting blood pressure in mmHg (validated 50-300)",
        "chol": "Serum cholesterol in mg/dL (validated >= 0)",
        "target": "Diagnosis flag: 1 = heart disease present, 0 = healthy",
        "event_id": "Unique event identifier from source EHR system",
    },
    "heart_gold": {
        "age_group": "Age bucket: Under 40, 40-49, 50-59, 60+",
        "diagnosis": "Heart Disease or Healthy based on target flag",
        "patient_count": "Number of patients in this cohort",
        "avg_cholesterol": "Average serum cholesterol (mg/dL) for the cohort",
        "avg_blood_pressure": "Average resting blood pressure (mmHg) for the cohort",
        "avg_max_heart_rate": "Average maximum heart rate achieved during testing",
    },
}

for table, cols in column_comments.items():
    for col_name, comment in cols.items():
        try:
            spark.sql(f"ALTER TABLE {CATALOG}.default.{table} ALTER COLUMN {col_name} COMMENT '{comment}'")
        except Exception as e:
            print(f"  Skipped {table}.{col_name}: {e}")

print("All governance metadata applied!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Report

# COMMAND ----------

dq = spark.sql(f"""
    SELECT
        '{CATALOG}' AS team,
        (SELECT COUNT(*) FROM {CATALOG}.default.heart_bronze) AS bronze_rows,
        (SELECT COUNT(*) FROM {CATALOG}.default.heart_silver) AS silver_rows,
        (SELECT COUNT(*) FROM {CATALOG}.default.heart_gold) AS gold_rows,
        (SELECT COUNT(DISTINCT age_group) FROM {CATALOG}.default.heart_gold) AS age_groups,
        (SELECT COUNT(DISTINCT diagnosis) FROM {CATALOG}.default.heart_gold) AS diagnoses,
        ROUND(
            (SELECT COUNT(*) FROM {CATALOG}.default.heart_silver) * 100.0 /
            NULLIF((SELECT COUNT(*) FROM {CATALOG}.default.heart_bronze), 0), 1
        ) AS silver_pass_rate_pct
""")
display(dq)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

print("=" * 60)
print(f"  FALLBACK COMPLETE: {CATALOG}.default")
print("=" * 60)

for tbl in ["heart_bronze", "heart_silver", "heart_gold"]:
    cnt = spark.table(f"{CATALOG}.default.{tbl}").count()
    cols = spark.table(f"{CATALOG}.default.{tbl}").columns
    print(f"  {tbl:20s} | {cnt:5d} rows | {len(cols)} cols")

gold = spark.table(f"{CATALOG}.default.heart_gold")
age_groups = set(gold.select("age_group").distinct().toPandas()["age_group"])
print(f"\n  Gold age groups: {sorted(age_groups)}")
print(f"  Gold rows: {gold.count()}")
print(f"\n  Your data is ready for Events 2-5!")
