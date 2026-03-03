# Databricks notebook source
# MAGIC %md
# MAGIC # SDP Pipeline — Heart Disease Intake Events
# MAGIC
# MAGIC **This notebook is meant to be run as a Spark Declarative Pipeline (SDP), NOT interactively.**
# MAGIC
# MAGIC ### How to Use
# MAGIC 1. Go to **Workflows → Pipelines → Create Pipeline**
# MAGIC    - Pipeline name: `{TEAM_NAME}_heart_pipeline`
# MAGIC    - Source code: select this notebook
# MAGIC    - Destination: catalog = `dataops_olympics`, schema = `default`
# MAGIC 2. Click **Start** to run the pipeline
# MAGIC
# MAGIC ### What This Creates
# MAGIC - `heart_bronze` — **Streaming table** — Raw ingestion via Auto Loader
# MAGIC - `heart_silver` — **Streaming table** — Cleaned data with DQ expectations
# MAGIC - `heart_gold` — **Materialized view** — Aggregated heart disease metrics

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, count, avg, round as _round, when, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Streaming Table (Auto Loader)

# COMMAND ----------

@dlt.table(
    comment="Raw patient intake events from hospital EHR system — streaming ingestion via Auto Loader"
)
def heart_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/dataops_olympics/default/raw_data/heart_events/")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Streaming Table (Cleaned + Deduplicated)
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
    return (
        dlt.read_stream("heart_bronze")
        .dropDuplicates(["event_id"])
        .withColumn("ingested_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Materialized View (Aggregated)

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
