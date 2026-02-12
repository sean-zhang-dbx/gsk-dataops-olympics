# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 Event 1B: Declarative Pipelines Challenge
# MAGIC
# MAGIC ## Challenge: Build a Medallion Architecture with Declarative Pipelines
# MAGIC **Time Limit: 20 minutes**
# MAGIC
# MAGIC ### What are Declarative Pipelines?
# MAGIC Declarative Pipelines (formerly Delta Live Tables / DLT) let you define **what** your data
# MAGIC pipeline should produce rather than **how** to execute it. Databricks manages orchestration,
# MAGIC error handling, data quality, and optimization automatically.
# MAGIC
# MAGIC ### Objective
# MAGIC Build a **Bronze → Silver → Gold** medallion architecture using Declarative Pipelines:
# MAGIC 1. **Bronze**: Raw ingestion with schema evolution
# MAGIC 2. **Silver**: Cleaned, validated, enriched data with **Expectations** (data quality rules)
# MAGIC 3. **Gold**: Aggregated business-level tables
# MAGIC
# MAGIC ### Scoring
# MAGIC | Criteria | Points |
# MAGIC |----------|--------|
# MAGIC | Bronze layer ingests all batches | 3 pts |
# MAGIC | Silver layer with ≥3 Expectations | 4 pts |
# MAGIC | Gold layer with ≥2 aggregate tables | 3 pts |
# MAGIC | Pipeline runs end-to-end | 3 pts |
# MAGIC | Bonus: Change Data Feed enabled | 2 pts |
# MAGIC | **Total** | **15 pts** |
# MAGIC
# MAGIC ### Dataset
# MAGIC - Heart disease CSV files (3 batches simulating incremental arrival)
# MAGIC - Located in your Unity Catalog Volume or `/tmp/dataops_olympics/raw/`
# MAGIC
# MAGIC > ⏱️ START YOUR TIMER NOW!

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to Run This Notebook
# MAGIC
# MAGIC ### Option A: As a Declarative Pipeline (Recommended)
# MAGIC 1. Go to **Workflows → Delta Live Tables** (or **Pipelines**)
# MAGIC 2. Click **Create Pipeline**
# MAGIC 3. Set this notebook as the source
# MAGIC 4. Choose **Serverless** or your cluster
# MAGIC 5. Click **Start**
# MAGIC
# MAGIC ### Option B: Preview in Notebook Mode
# MAGIC You can also run individual cells to test logic before running as a pipeline.
# MAGIC When running as a regular notebook, the `dlt` module won't be available,
# MAGIC so we provide a preview mode below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TEAM_NAME = "_____"  # e.g., "team_01"

# Data source paths — update based on your setup
# Option 1: From Unity Catalog Volume (recommended)
# DATA_SOURCE = f"/Volumes/dataops_olympics/competition/raw_data/"

# Option 2: From local file system (fallback)
DATA_SOURCE = "/tmp/dataops_olympics/raw/heart_disease/"

# Option 3: From repo data/ folder (if uploaded)
# DATA_SOURCE = "file:./data/"

print(f"Team: {TEAM_NAME}")
print(f"Data source: {DATA_SOURCE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🥉 BRONZE LAYER: Raw Ingestion
# MAGIC
# MAGIC The Bronze layer ingests raw data as-is, preserving the original format.
# MAGIC It should handle schema evolution and track ingestion metadata.

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Define Bronze Table
# MAGIC
# MAGIC Use `@dlt.table` to define a streaming or batch ingestion from CSV files.
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import current_timestamp, input_file_name
# MAGIC
# MAGIC @dlt.table(
# MAGIC     name="bronze_heart_disease",
# MAGIC     comment="Raw heart disease data ingested from CSV batches"
# MAGIC )
# MAGIC def bronze_heart_disease():
# MAGIC     return (
# MAGIC         spark.read.format("csv")
# MAGIC         .option("header", "true")
# MAGIC         .option("inferSchema", "true")
# MAGIC         .load(DATA_SOURCE + "*.csv")  # Read all batch files
# MAGIC         .withColumn("_ingested_at", current_timestamp())
# MAGIC         .withColumn("_source_file", input_file_name())
# MAGIC     )
# MAGIC ```

# COMMAND ----------

# Preview mode (run as regular notebook to test your logic)
# This cell simulates what DLT would do

from pyspark.sql.functions import current_timestamp, input_file_name

# TODO: Read all CSV batch files
# The DLT version uses @dlt.table decorator; here we test the logic directly

df_bronze = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"file:{DATA_SOURCE}*.csv")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

print(f"Bronze records: {df_bronze.count()}")
print(f"Source files: {df_bronze.select('_source_file').distinct().count()}")
display(df_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🥈 SILVER LAYER: Cleaned & Validated Data
# MAGIC
# MAGIC The Silver layer applies:
# MAGIC - **Data quality rules** via DLT Expectations
# MAGIC - **Type casting** and **null handling**
# MAGIC - **Deduplication**
# MAGIC - **Enrichment** (derived columns)
# MAGIC
# MAGIC ### DLT Expectations
# MAGIC Expectations are declarative data quality rules:
# MAGIC ```python
# MAGIC @dlt.expect("description", "condition")          # Warn on failure
# MAGIC @dlt.expect_or_drop("description", "condition")  # Drop bad rows
# MAGIC @dlt.expect_or_fail("description", "condition")  # Fail pipeline
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Define Silver Table with Expectations
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     name="silver_heart_disease",
# MAGIC     comment="Cleaned and validated heart disease data"
# MAGIC )
# MAGIC @dlt.expect("valid_age", "age BETWEEN 0 AND 120")
# MAGIC @dlt.expect_or_drop("valid_blood_pressure", "trestbps BETWEEN 50 AND 300")
# MAGIC @dlt.expect("valid_cholesterol", "chol > 0")
# MAGIC # TODO: Add more expectations!
# MAGIC # @dlt.expect_or_drop("_____", "_____")
# MAGIC def silver_heart_disease():
# MAGIC     return (
# MAGIC         dlt.read("bronze_heart_disease")
# MAGIC         .filter("age > 0")  # Remove known bad records
# MAGIC         .withColumn("age_group",
# MAGIC             F.when(F.col("age") < 40, "Under 40")
# MAGIC              .when(F.col("age") < 50, "40-49")
# MAGIC              .when(F.col("age") < 60, "50-59")
# MAGIC              .otherwise("60+"))
# MAGIC         .withColumn("risk_category",
# MAGIC             F.when(
# MAGIC                 (F.col("cp") == 3) & (F.col("trestbps") > 140),
# MAGIC                 "High Risk"
# MAGIC             ).otherwise("Standard"))
# MAGIC         .dropDuplicates(["age", "sex", "cp", "trestbps", "chol", "thalach"])
# MAGIC     )
# MAGIC ```

# COMMAND ----------

# Preview mode - test your silver layer logic
from pyspark.sql import functions as F

# Simulate reading from bronze
df_silver = (
    df_bronze
    # TODO: Apply data quality filters (these simulate DLT expectations)
    .filter("age > 0 AND age < 120")            # valid_age
    .filter("trestbps BETWEEN 50 AND 300")       # valid_blood_pressure
    .filter("chol > 0")                          # valid_cholesterol
    
    # TODO: Add more quality filters
    # .filter("_____")
    
    # Enrichment: Add derived columns
    .withColumn("age_group",
        F.when(F.col("age") < 40, "Under 40")
         .when(F.col("age") < 50, "40-49")
         .when(F.col("age") < 60, "50-59")
         .otherwise("60+"))
    .withColumn("risk_category",
        F.when(
            (F.col("cp") == 3) & (F.col("trestbps") > 140),
            "High Risk"
        ).otherwise("Standard"))
    
    # TODO: Add more enrichment columns
    # .withColumn("_____", _____)
    
    # Deduplication
    .dropDuplicates(["age", "sex", "cp", "trestbps", "chol", "thalach"])
)

# Show quality stats
total_bronze = df_bronze.count()
total_silver = df_silver.count()
dropped = total_bronze - total_silver
print(f"Bronze records: {total_bronze}")
print(f"Silver records: {total_silver}")
print(f"Dropped by quality rules: {dropped} ({dropped/total_bronze:.1%})")
display(df_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🥇 GOLD LAYER: Business-Level Aggregates
# MAGIC
# MAGIC The Gold layer creates analytics-ready tables optimized for specific use cases.
# MAGIC
# MAGIC ### TODO: Create at least 2 Gold tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 1: Risk Summary by Age Group
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     name="gold_risk_by_age",
# MAGIC     comment="Heart disease risk statistics aggregated by age group"
# MAGIC )
# MAGIC def gold_risk_by_age():
# MAGIC     return (
# MAGIC         dlt.read("silver_heart_disease")
# MAGIC         .groupBy("age_group")
# MAGIC         .agg(
# MAGIC             F.count("*").alias("total_patients"),
# MAGIC             F.sum("target").alias("disease_count"),
# MAGIC             F.round(F.avg("target"), 3).alias("disease_rate"),
# MAGIC             F.round(F.avg("chol"), 1).alias("avg_cholesterol"),
# MAGIC             F.round(F.avg("trestbps"), 1).alias("avg_blood_pressure"),
# MAGIC         )
# MAGIC     )
# MAGIC ```

# COMMAND ----------

# Preview: Gold Table 1
df_gold_1 = (
    df_silver
    .groupBy("age_group")
    .agg(
        F.count("*").alias("total_patients"),
        F.sum("target").alias("disease_count"),
        F.round(F.avg("target"), 3).alias("disease_rate"),
        F.round(F.avg("chol"), 1).alias("avg_cholesterol"),
        F.round(F.avg("trestbps"), 1).alias("avg_blood_pressure"),
    )
    .orderBy("age_group")
)

print("🥇 Gold Table 1: Risk Summary by Age Group")
display(df_gold_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 2: TODO — Create Your Own!
# MAGIC
# MAGIC Ideas:
# MAGIC - Disease rate by chest pain type
# MAGIC - High-risk patient list
# MAGIC - Key metrics dashboard table
# MAGIC - Daily/batch ingestion summary

# COMMAND ----------

# TODO: Preview your Gold Table 2
# df_gold_2 = (
#     df_silver
#     .groupBy("_____")
#     .agg(
#         _____
#     )
# )
# display(df_gold_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📝 Full DLT Pipeline Definition
# MAGIC
# MAGIC When you're ready, **uncomment the code below** and run this notebook as a
# MAGIC Delta Live Tables pipeline. This is the "real" version.

# COMMAND ----------

# =============================================================
# UNCOMMENT THIS ENTIRE CELL when running as a DLT Pipeline
# =============================================================

# import dlt
# from pyspark.sql import functions as F
# 
# DATA_SOURCE = "/tmp/dataops_olympics/raw/heart_disease/"
#
# # ---- BRONZE ----
# @dlt.table(
#     name="bronze_heart_disease",
#     comment="Raw heart disease data from CSV batches",
#     table_properties={"quality": "bronze"}
# )
# def bronze_heart_disease():
#     return (
#         spark.read.format("csv")
#         .option("header", "true")
#         .option("inferSchema", "true")
#         .load(DATA_SOURCE + "*.csv")
#         .withColumn("_ingested_at", F.current_timestamp())
#         .withColumn("_source_file", F.input_file_name())
#     )
#
# # ---- SILVER ----
# @dlt.table(
#     name="silver_heart_disease",
#     comment="Cleaned and validated heart disease data",
#     table_properties={"quality": "silver"}
# )
# @dlt.expect("valid_age", "age BETWEEN 1 AND 120")
# @dlt.expect_or_drop("valid_blood_pressure", "trestbps BETWEEN 50 AND 300")
# @dlt.expect("valid_cholesterol", "chol > 0")
# @dlt.expect("valid_heart_rate", "thalach BETWEEN 40 AND 220")
# def silver_heart_disease():
#     return (
#         dlt.read("bronze_heart_disease")
#         .filter("age > 0")
#         .withColumn("age_group",
#             F.when(F.col("age") < 40, "Under 40")
#              .when(F.col("age") < 50, "40-49")
#              .when(F.col("age") < 60, "50-59")
#              .otherwise("60+"))
#         .withColumn("risk_category",
#             F.when(
#                 (F.col("cp") == 3) & (F.col("trestbps") > 140),
#                 "High Risk"
#             ).otherwise("Standard"))
#         .dropDuplicates(["age", "sex", "cp", "trestbps", "chol", "thalach"])
#     )
#
# # ---- GOLD ----
# @dlt.table(
#     name="gold_risk_by_age",
#     comment="Risk statistics by age group",
#     table_properties={"quality": "gold"}
# )
# def gold_risk_by_age():
#     return (
#         dlt.read("silver_heart_disease")
#         .groupBy("age_group")
#         .agg(
#             F.count("*").alias("total_patients"),
#             F.sum("target").alias("disease_count"),
#             F.round(F.avg("target"), 3).alias("disease_rate"),
#             F.round(F.avg("chol"), 1).alias("avg_cholesterol"),
#             F.round(F.avg("trestbps"), 1).alias("avg_blood_pressure"),
#         )
#     )
#
# @dlt.table(
#     name="gold_risk_by_chest_pain",
#     comment="Risk statistics by chest pain type",
#     table_properties={"quality": "gold"}
# )
# def gold_risk_by_chest_pain():
#     return (
#         dlt.read("silver_heart_disease")
#         .withColumn("chest_pain_type",
#             F.when(F.col("cp") == 0, "Typical Angina")
#              .when(F.col("cp") == 1, "Atypical Angina")
#              .when(F.col("cp") == 2, "Non-Anginal")
#              .otherwise("Asymptomatic"))
#         .groupBy("chest_pain_type")
#         .agg(
#             F.count("*").alias("total_patients"),
#             F.round(F.avg("target"), 3).alias("disease_rate"),
#             F.round(F.avg("age"), 1).alias("avg_age"),
#         )
#     )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Completion Checklist
# MAGIC
# MAGIC - [ ] Bronze layer reads all CSV batches
# MAGIC - [ ] Silver layer has ≥ 3 DLT Expectations
# MAGIC - [ ] Silver layer has derived/enriched columns
# MAGIC - [ ] Gold layer has ≥ 2 aggregate tables
# MAGIC - [ ] Pipeline runs end-to-end (or preview mode validated)
# MAGIC - [ ] Bonus: Change Data Feed enabled
# MAGIC
# MAGIC > 🏁 **Signal judges when your pipeline DAG is green!**

# COMMAND ----------

# Validation (preview mode)
print("=" * 60)
print("DECLARATIVE PIPELINE VALIDATION (Preview Mode)")
print("=" * 60)

score = 0

# Check Bronze
if df_bronze.count() > 0:
    print(f"  ✅ Bronze: {df_bronze.count()} records from {df_bronze.select('_source_file').distinct().count()} files")
    score += 3
else:
    print(f"  ❌ Bronze: no records")

# Check Silver
if df_silver.count() > 0:
    dropped_pct = (df_bronze.count() - df_silver.count()) / df_bronze.count()
    print(f"  ✅ Silver: {df_silver.count()} records ({dropped_pct:.1%} dropped by quality rules)")
    score += 4
else:
    print(f"  ❌ Silver: no records")

# Check Gold
try:
    if df_gold_1.count() > 0:
        print(f"  ✅ Gold Table 1: {df_gold_1.count()} aggregate rows")
        score += 3
except:
    print(f"  ❌ Gold Table 1: not created")

print(f"\nPreview Score: {score}/10")
print("(Full score requires running as actual DLT pipeline)")
print("=" * 60)
