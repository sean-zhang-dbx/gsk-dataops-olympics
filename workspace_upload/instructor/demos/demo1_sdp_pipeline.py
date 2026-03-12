# Databricks notebook source
# MAGIC %md
# MAGIC # SDP Pipeline: Heart Disease Medallion (Demo)
# MAGIC
# MAGIC Bronze → Silver → Gold using **Spark Declarative Pipelines**.
# MAGIC This notebook is run as a **Pipeline** (Workflows → Pipelines), not interactively.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import *

# COMMAND ----------

@dp.table(comment="Raw patient intake events — Auto Loader ingestion from NDJSON")
def demo_sdp_bronze():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/dataops_olympics/default/raw_data/heart_events/"))

# COMMAND ----------

@dp.table(comment="Cleaned patient data — validated and deduplicated on event_id")
@dp.expect_or_drop("valid_age", "age IS NOT NULL AND age BETWEEN 1 AND 120")
@dp.expect_or_drop("valid_blood_pressure", "trestbps BETWEEN 50 AND 300")
@dp.expect("non_negative_cholesterol", "chol >= 0")
def demo_sdp_silver():
    return (spark.readStream.table("demo_sdp_bronze")
        .dropDuplicates(["event_id"])
        .withColumn("ingested_at", current_timestamp()))

# COMMAND ----------

@dp.table(comment="Heart disease metrics by age group — for dashboards and Genie")
def demo_sdp_gold():
    return (spark.read.table("demo_sdp_silver")
        .withColumn("age_group",
            when(col("age") < 40, "Under 40")
            .when(col("age") < 50, "40-49")
            .when(col("age") < 60, "50-59")
            .otherwise("60+"))
        .withColumn("diagnosis",
            when(col("target") == 1, "Heart Disease").otherwise("Healthy"))
        .groupBy("age_group", "diagnosis")
        .agg(
            count("*").alias("patient_count"),
            round(avg("chol"), 1).alias("avg_cholesterol"),
            round(avg("trestbps"), 1).alias("avg_blood_pressure"),
            round(avg("thalach"), 1).alias("avg_max_heart_rate")))
