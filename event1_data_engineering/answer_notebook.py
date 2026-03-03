# Databricks notebook source
# MAGIC %md
# MAGIC # Event 1: Answer Key
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY — Do not share with participants!**
# MAGIC
# MAGIC This notebook contains complete working solutions for both the SDP and SQL paths.
# MAGIC The SDP pipeline code is shown for reference but must be run as a Pipeline, not interactively.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dataops_olympics;
# MAGIC USE SCHEMA default;

# COMMAND ----------

TEAM_NAME = "answer_key"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Data Exploration

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/dataops_olympics/default/raw_data/heart_events/'

# COMMAND ----------

df_all_raw = spark.read.json("/Volumes/dataops_olympics/default/raw_data/heart_events/*.json")
print(f"Total raw records: {df_all_raw.count()}")
print(f"Distinct event_ids: {df_all_raw.select('event_id').distinct().count()}")
print(f"Null ages: {df_all_raw.filter('age IS NULL').count()}")
print(f"Invalid BP (999 or -1): {df_all_raw.filter('trestbps NOT BETWEEN 50 AND 300').count()}")
print(f"Negative cholesterol: {df_all_raw.filter('chol < 0').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Path B: SQL Solution (Complete)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze — Raw Ingestion

# COMMAND ----------

df_bronze = spark.read.json("/Volumes/dataops_olympics/default/raw_data/heart_events/*.json")

df_bronze.write.format("delta").mode("overwrite").saveAsTable(
    f"dataops_olympics.default.{TEAM_NAME}_heart_bronze"
)
print(f"Bronze: {df_bronze.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver — Cleaned + Deduplicated

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE dataops_olympics.default.{TEAM_NAME}_heart_silver AS
    SELECT *, current_timestamp() as ingested_at
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp) as _rn
        FROM dataops_olympics.default.{TEAM_NAME}_heart_bronze
    )
    WHERE _rn = 1
      AND age IS NOT NULL AND age BETWEEN 1 AND 120
      AND trestbps BETWEEN 50 AND 300
      AND chol >= 0
""")

silver_count = spark.table(f"dataops_olympics.default.{TEAM_NAME}_heart_silver").count()
bronze_count = spark.table(f"dataops_olympics.default.{TEAM_NAME}_heart_bronze").count()
print(f"Silver: {silver_count} rows (removed {bronze_count - silver_count} dirty/duplicate rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold — Aggregated

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE dataops_olympics.default.{TEAM_NAME}_heart_gold AS
    SELECT
        CASE
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END as age_group,
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END as diagnosis,
        COUNT(*) as patient_count,
        ROUND(AVG(chol), 1) as avg_cholesterol,
        ROUND(AVG(trestbps), 1) as avg_blood_pressure,
        ROUND(AVG(thalach), 1) as avg_max_heart_rate
    FROM dataops_olympics.default.{TEAM_NAME}_heart_silver
    GROUP BY 1, 2
    ORDER BY 1, 2
""")

display(spark.table(f"dataops_olympics.default.{TEAM_NAME}_heart_gold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Governance — Table & Column Comments

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE dataops_olympics.default.{TEAM_NAME}_heart_bronze
    SET TBLPROPERTIES ('comment' = 'Raw patient intake events from hospital EHR — 5 NDJSON batches, unmodified')
""")

spark.sql(f"""
    ALTER TABLE dataops_olympics.default.{TEAM_NAME}_heart_silver
    SET TBLPROPERTIES ('comment' = 'Cleaned patient intake data — validated age/BP/cholesterol, deduplicated on event_id')
""")

spark.sql(f"""
    ALTER TABLE dataops_olympics.default.{TEAM_NAME}_heart_gold
    SET TBLPROPERTIES ('comment' = 'Heart disease metrics aggregated by age group for dashboards and Genie')
""")

for tbl in [f"{TEAM_NAME}_heart_silver"]:
    spark.sql(f"ALTER TABLE dataops_olympics.default.{tbl} ALTER COLUMN age COMMENT 'Patient age in years (validated 1-120)'")
    spark.sql(f"ALTER TABLE dataops_olympics.default.{tbl} ALTER COLUMN trestbps COMMENT 'Resting blood pressure in mmHg (validated 50-300)'")
    spark.sql(f"ALTER TABLE dataops_olympics.default.{tbl} ALTER COLUMN chol COMMENT 'Serum cholesterol in mg/dL (validated >= 0)'")
    spark.sql(f"ALTER TABLE dataops_olympics.default.{tbl} ALTER COLUMN target COMMENT 'Diagnosis: 1 = heart disease present, 0 = healthy'")
    spark.sql(f"ALTER TABLE dataops_olympics.default.{tbl} ALTER COLUMN event_id COMMENT 'Unique event identifier from source system'")

print("All governance comments applied!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Report

# COMMAND ----------

dq = spark.sql(f"""
    SELECT
        COUNT(*) as total_records,
        SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) as null_age,
        SUM(CASE WHEN age IS NOT NULL AND age NOT BETWEEN 1 AND 120 THEN 1 ELSE 0 END) as invalid_age,
        SUM(CASE WHEN trestbps NOT BETWEEN 50 AND 300 THEN 1 ELSE 0 END) as invalid_bp,
        SUM(CASE WHEN chol < 0 THEN 1 ELSE 0 END) as negative_chol,
        COUNT(*) - COUNT(DISTINCT event_id) as duplicate_events,
        ROUND(
            (COUNT(*) - SUM(CASE WHEN age IS NULL OR (age NOT BETWEEN 1 AND 120) OR trestbps NOT BETWEEN 50 AND 300 OR chol < 0 THEN 1 ELSE 0 END))
            * 100.0 / COUNT(*), 1
        ) as clean_pct
    FROM dataops_olympics.default.{TEAM_NAME}_heart_bronze
""")
display(dq)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Path A: SDP Pipeline Code (Reference)
# MAGIC
# MAGIC This is the same code as `sdp_pipeline_template.py`. It must be run as a Pipeline,
# MAGIC not interactively. Shown here for reference.
# MAGIC
# MAGIC - **Bronze & Silver** = Streaming Tables (incremental ingestion)
# MAGIC - **Gold** = Materialized View (auto-refreshing aggregation)
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC # Bronze — Streaming Table via Auto Loader
# MAGIC @dlt.table(comment="Raw patient intake events — streaming ingestion via Auto Loader")
# MAGIC def heart_bronze():
# MAGIC     return (spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .option("cloudFiles.inferColumnTypes", "true")
# MAGIC         .load("/Volumes/dataops_olympics/default/raw_data/heart_events/"))
# MAGIC
# MAGIC # Silver — Streaming Table with DQ expectations
# MAGIC @dlt.table(comment="Cleaned patient intake data — validated and deduplicated")
# MAGIC @dlt.expect_or_drop("valid_age", "age IS NOT NULL AND age BETWEEN 1 AND 120")
# MAGIC @dlt.expect_or_drop("valid_blood_pressure", "trestbps BETWEEN 50 AND 300")
# MAGIC @dlt.expect_or_drop("non_negative_cholesterol", "chol >= 0")
# MAGIC @dlt.expect("has_event_id", "event_id IS NOT NULL")
# MAGIC def heart_silver():
# MAGIC     return (dlt.read_stream("heart_bronze")
# MAGIC         .dropDuplicates(["event_id"])
# MAGIC         .withColumn("ingested_at", current_timestamp()))
# MAGIC
# MAGIC # Gold — Materialized View (non-streaming read)
# MAGIC @dlt.table(comment="Heart disease metrics by age group — materialized view")
# MAGIC def heart_gold():
# MAGIC     return (dlt.read("heart_silver")
# MAGIC         .withColumn("age_group",
# MAGIC             when(col("age") < 40, "Under 40")
# MAGIC             .when(col("age") < 50, "40-49")
# MAGIC             .when(col("age") < 60, "50-59")
# MAGIC             .otherwise("60+"))
# MAGIC         .withColumn("diagnosis",
# MAGIC             when(col("target") == 1, "Heart Disease").otherwise("Healthy"))
# MAGIC         .groupBy("age_group", "diagnosis")
# MAGIC         .agg(
# MAGIC             count("*").alias("patient_count"),
# MAGIC             round(avg("chol"), 1).alias("avg_cholesterol"),
# MAGIC             round(avg("trestbps"), 1).alias("avg_blood_pressure"),
# MAGIC             round(avg("thalach"), 1).alias("avg_max_heart_rate")))
# MAGIC ```
# MAGIC
# MAGIC ### SDP Advantages Demonstrated
# MAGIC
# MAGIC | Feature | SDP (Path A) | SQL (Path B) |
# MAGIC |---------|-------------|-------------|
# MAGIC | **Ingestion** | Auto Loader (streaming, incremental) | Batch `spark.read.json` |
# MAGIC | **Table Types** | Streaming Tables (Bronze/Silver) + Materialized View (Gold) | Regular Delta Tables |
# MAGIC | **Data Quality** | Automatic — SDP expectations track pass/fail rates | Manual — must write COUNT/CASE queries |
# MAGIC | **Governance** | Comments in code = infrastructure-as-code | ALTER TABLE after the fact |
# MAGIC | **Lineage** | Pipeline UI shows full dependency graph | No automatic lineage |
# MAGIC | **Monitoring** | Built-in event log with metrics history | None |
