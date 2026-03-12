# Databricks notebook source
# MAGIC %md
# MAGIC # PANIC BUTTON: Fallback — Generate All Tables for a Team
# MAGIC
# MAGIC **Use when:** A team is stuck, corrupted their data, or can't proceed to the next event.
# MAGIC
# MAGIC This generates correct Bronze, Silver, and Gold tables so the team can continue
# MAGIC to Events 2-5 without being blocked.
# MAGIC
# MAGIC **Set the team name below and Run All.**

# COMMAND ----------

dbutils.widgets.text("TEAM_NAME", "team_XX", "Team to fix")
TEAM_NAME = dbutils.widgets.get("TEAM_NAME")

SHARED_CATALOG = "dataops_olympics"
SHARED_SCHEMA = "default"
RAW_PATH = f"/Volumes/{SHARED_CATALOG}/{SHARED_SCHEMA}/raw_data/heart_events/"

print(f"Generating fallback tables for: {TEAM_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create catalog if missing

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TEAM_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEAM_NAME}.default")
spark.sql(f"USE CATALOG {TEAM_NAME}")
spark.sql(f"USE SCHEMA default")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Raw Ingestion

# COMMAND ----------

import os

# Try NDJSON files from volume first
ndjson_path = f"/Volumes/{SHARED_CATALOG}/{SHARED_SCHEMA}/raw_data/heart_events/"
try:
    files = [f for f in os.listdir(ndjson_path) if f.endswith(".json")]
    if files:
        df_bronze = spark.read.json(f"{ndjson_path}/*.json")
        df_bronze.write.format("delta").mode("overwrite").saveAsTable(f"{TEAM_NAME}.default.heart_bronze")
        print(f"  Bronze from NDJSON: {df_bronze.count()} rows")
    else:
        raise FileNotFoundError("No JSON files")
except Exception:
    # Fallback: generate from shared heart_disease table
    print("  NDJSON not available, generating from shared table...")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TEAM_NAME}.default.heart_bronze AS
        SELECT
            CONCAT('EVT-', LPAD(CAST(ROW_NUMBER() OVER (ORDER BY age) AS STRING), 5, '0')) AS event_id,
            current_timestamp() AS event_timestamp,
            'fallback_generator' AS source_system,
            1 AS record_version,
            CONCAT('PT-', LPAD(CAST(ROW_NUMBER() OVER (ORDER BY age) AS STRING), 4, '0')) AS patient_id,
            *
        FROM {SHARED_CATALOG}.{SHARED_SCHEMA}.heart_disease
    """)
    cnt = spark.table(f"{TEAM_NAME}.default.heart_bronze").count()
    print(f"  Bronze (generated): {cnt} rows")

spark.sql(f"COMMENT ON TABLE {TEAM_NAME}.default.heart_bronze IS 'Raw patient intake events (fallback generated)'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Cleaned & Deduplicated

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {TEAM_NAME}.default.heart_silver AS
    SELECT event_id, event_timestamp, source_system, record_version, patient_id,
           age, sex, cp, trestbps, chol, fbs, restecg, thalach, exang,
           oldpeak, slope, ca, thal, target,
           current_timestamp() AS ingested_at
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp) AS _rn
        FROM {TEAM_NAME}.default.heart_bronze
    )
    WHERE _rn = 1
      AND age IS NOT NULL AND age BETWEEN 1 AND 120
      AND trestbps BETWEEN 50 AND 300
      AND chol >= 0
""")

cnt = spark.table(f"{TEAM_NAME}.default.heart_silver").count()
print(f"  Silver: {cnt} rows")

spark.sql(f"COMMENT ON TABLE {TEAM_NAME}.default.heart_silver IS 'Cleaned and deduplicated patient records (fallback generated)'")
spark.sql(f"ALTER TABLE {TEAM_NAME}.default.heart_silver ALTER COLUMN age COMMENT 'Patient age in years'")
spark.sql(f"ALTER TABLE {TEAM_NAME}.default.heart_silver ALTER COLUMN target COMMENT '1 = heart disease, 0 = healthy'")
spark.sql(f"ALTER TABLE {TEAM_NAME}.default.heart_silver ALTER COLUMN trestbps COMMENT 'Resting blood pressure in mmHg'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Aggregated

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {TEAM_NAME}.default.heart_gold AS
    SELECT
        CASE
            WHEN age < 40 THEN 'Under 40'
            WHEN age BETWEEN 40 AND 49 THEN '40-49'
            WHEN age BETWEEN 50 AND 59 THEN '50-59'
            ELSE '60+'
        END AS age_group,
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
        COUNT(*) AS patient_count,
        ROUND(AVG(chol), 1) AS avg_cholesterol,
        ROUND(AVG(trestbps), 1) AS avg_blood_pressure,
        ROUND(AVG(thalach), 1) AS avg_max_heart_rate
    FROM {TEAM_NAME}.default.heart_silver
    GROUP BY 1, 2
""")

cnt = spark.table(f"{TEAM_NAME}.default.heart_gold").count()
print(f"  Gold: {cnt} rows")

spark.sql(f"COMMENT ON TABLE {TEAM_NAME}.default.heart_gold IS 'Aggregated heart disease metrics by age group and diagnosis (fallback generated)'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

print("=" * 60)
print(f"  FALLBACK COMPLETE — {TEAM_NAME}")
print("=" * 60)

for tbl in ["heart_bronze", "heart_silver", "heart_gold"]:
    try:
        cnt = spark.table(f"{TEAM_NAME}.default.{tbl}").count()
        cols = len(spark.table(f"{TEAM_NAME}.default.{tbl}").columns)
        print(f"  {tbl:20s} {cnt:>5} rows, {cols} columns")
    except:
        print(f"  {tbl:20s} MISSING")

print("=" * 60)
print(f"  Team {TEAM_NAME} can now proceed to Events 2-5!")
