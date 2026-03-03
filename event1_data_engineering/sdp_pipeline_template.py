# Databricks notebook source
# MAGIC %md
# MAGIC # SDP Pipeline — Heart Disease Intake Events
# MAGIC
# MAGIC **This notebook is meant to be run as a Spark Declarative Pipeline (SDP), NOT interactively.**
# MAGIC
# MAGIC ### How to Use
# MAGIC 1. Copy this notebook to your own workspace folder
# MAGIC 2. Update `TEAM_NAME` below
# MAGIC 3. Go to **Workflows → Pipelines → Create Pipeline**
# MAGIC    - Pipeline name: `{TEAM_NAME}_heart_pipeline`
# MAGIC    - Source code: select this notebook
# MAGIC    - Destination: catalog = `dataops_olympics`, schema = `default`
# MAGIC 4. Click **Start** to run the pipeline
# MAGIC
# MAGIC ### What This Creates
# MAGIC - `heart_bronze` — Raw ingestion of all 5 NDJSON batch files
# MAGIC - `heart_silver` — Cleaned data with data quality expectations
# MAGIC - `heart_gold` — Materialized view with aggregated heart disease metrics

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, count, avg, round as _round, when, current_timestamp, row_number
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Raw Ingestion

# COMMAND ----------

@dlt.table(
    comment="Raw patient intake events from hospital EHR system — 5 NDJSON batches, unmodified"
)
def heart_bronze():
    return (
        spark.read
        .json("/Volumes/dataops_olympics/default/raw_data/heart_events/*.json")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Cleaned + Deduplicated
# MAGIC
# MAGIC Data quality expectations:
# MAGIC - `valid_age`: age must be between 1 and 120 (DROP rows that fail)
# MAGIC - `valid_blood_pressure`: trestbps must be between 50 and 300 (DROP rows that fail)
# MAGIC - `non_negative_cholesterol`: chol must be >= 0 (DROP rows that fail)
# MAGIC - `has_event_id`: event_id must not be null (WARN only)

# COMMAND ----------

@dlt.table(
    comment="Cleaned patient intake data — validated age, BP, cholesterol; deduplicated on event_id"
)
@dlt.expect_or_drop("valid_age", "age IS NOT NULL AND age BETWEEN 1 AND 120")
@dlt.expect_or_drop("valid_blood_pressure", "trestbps BETWEEN 50 AND 300")
@dlt.expect_or_drop("non_negative_cholesterol", "chol >= 0")
@dlt.expect("has_event_id", "event_id IS NOT NULL")
def heart_silver():
    bronze = dlt.read("heart_bronze")
    w = Window.partitionBy("event_id").orderBy("event_timestamp")
    return (
        bronze
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .withColumn("ingested_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Aggregated Materialized View

# COMMAND ----------

@dlt.table(
    comment="Heart disease metrics by age group — auto-refreshing materialized view for dashboards and Genie"
)
def heart_gold():
    return (
        dlt.read("heart_silver")
        .withColumn(
            "age_group",
            when(col("age") < 40, "Under 40")
            .when(col("age") < 50, "40-49")
            .when(col("age") < 60, "50-59")
            .otherwise("60+")
        )
        .withColumn(
            "diagnosis",
            when(col("target") == 1, "Heart Disease").otherwise("Healthy")
        )
        .groupBy("age_group", "diagnosis")
        .agg(
            count("*").alias("patient_count"),
            _round(avg("chol"), 1).alias("avg_cholesterol"),
            _round(avg("trestbps"), 1).alias("avg_blood_pressure"),
            _round(avg("thalach"), 1).alias("avg_max_heart_rate"),
        )
    )
