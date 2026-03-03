# Databricks notebook source
# MAGIC %md
# MAGIC # Lightning Talk 1: Data Engineering on Databricks
# MAGIC
# MAGIC **Duration: 15 minutes** | Instructor-led live demo
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Agenda
# MAGIC | Time | Section | What You'll See |
# MAGIC |------|---------|----------------|
# MAGIC | 0:00 | **The Problem** | Why pipelines matter in pharma |
# MAGIC | 1:00 | **Ingest** | CSV + JSON → Spark DataFrames from UC Volumes |
# MAGIC | 4:00 | **Delta Lake** | Write to Delta, explain ACID + time travel |
# MAGIC | 7:00 | **Governance** | Table & column comments in Unity Catalog |
# MAGIC | 9:00 | **SDP Pipeline** | Bronze → Silver → Gold with Spark Declarative Pipelines |
# MAGIC | 13:00 | **Wow Moment** | Data quality expectations + time travel |
# MAGIC | 14:00 | **Your Turn** | Preview of the practice notebook |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Problem
# MAGIC
# MAGIC > **Say this:** "Imagine you're at GSK and a new clinical trial dataset lands on your desk —
# MAGIC > a messy CSV with 500 patient records. Your job: get it into a governed, queryable table
# MAGIC > that the analytics team can trust. That's what we're building in the next 14 minutes."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Lives in Unity Catalog Volumes
# MAGIC
# MAGIC > **Say this:** "First — where does your raw data live? In Databricks, we use
# MAGIC > Unity Catalog Volumes. Think of it as governed cloud storage that's part of your catalog.
# MAGIC > No more dumping files in /tmp or local paths."

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dataops_olympics;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's see what's in our Volume
# MAGIC LIST '/Volumes/dataops_olympics/default/raw_data/heart_disease/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Raw Data (CSV from Volume)
# MAGIC
# MAGIC > **Say this:** "Step one — read the file. Spark handles CSV, JSON, Parquet, and more.
# MAGIC > One line of code, and notice the path starts with /Volumes — this is governed storage."

# COMMAND ----------

csv_path = "/Volumes/dataops_olympics/default/raw_data/heart_disease/heart.csv"

df_heart = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(csv_path))

print(f"Loaded {df_heart.count()} rows x {len(df_heart.columns)} columns")
display(df_heart.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "That's it. One line. Spark inferred all the data types — age is integer,
# MAGIC > cholesterol is integer, oldpeak is double. No pandas, no manual parsing."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Delta Lake
# MAGIC
# MAGIC > **Say this:** "Now the important part. We don't save as CSV or Parquet — we save as
# MAGIC > **Delta Lake**. Why? Three words: ACID transactions, time travel, schema enforcement.
# MAGIC > Your table will never be in a half-written, corrupted state."

# COMMAND ----------

df_heart.write.format("delta").mode("overwrite").saveAsTable("dataops_olympics.default.demo_heart_bronze")
print("Saved as Delta table: demo_heart_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL dataops_olympics.default.demo_heart_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "Notice the format says 'delta'. This table now supports versioning,
# MAGIC > ACID transactions, and schema evolution. Every write creates a new version."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Governance — Document Your Data
# MAGIC
# MAGIC > **Say this:** "At GSK, data without documentation is data nobody trusts.
# MAGIC > Unity Catalog lets you add comments right on the table and columns.
# MAGIC > This is how a data engineer earns the trust of the analytics team."

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dataops_olympics.default.demo_heart_bronze SET TBLPROPERTIES (
# MAGIC   'comment' = 'UCI Heart Disease dataset: 500 patients, 14 clinical features, target=1 means disease present'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE dataops_olympics.default.demo_heart_bronze ALTER COLUMN age COMMENT 'Patient age in years (29-77)';
# MAGIC ALTER TABLE dataops_olympics.default.demo_heart_bronze ALTER COLUMN chol COMMENT 'Serum cholesterol in mg/dL';
# MAGIC ALTER TABLE dataops_olympics.default.demo_heart_bronze ALTER COLUMN target COMMENT 'Diagnosis: 1 = heart disease, 0 = healthy';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE dataops_olympics.default.demo_heart_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "Look — every column now has a description. When your colleague opens
# MAGIC > this table next month, they know exactly what 'target' means without asking you."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Spark Declarative Pipelines (The Modern Way)
# MAGIC
# MAGIC > **Say this:** "Now here's the modern approach to building medallion pipelines.
# MAGIC > Instead of writing imperative Spark code, we **declare** our tables and let
# MAGIC > Databricks handle the orchestration. This is called Spark Declarative Pipelines —
# MAGIC > it used to be called Delta Live Tables (DLT)."
# MAGIC
# MAGIC > "With SDP, you define WHAT you want, not HOW to run it. You also get
# MAGIC > built-in data quality expectations — like unit tests for your data."
# MAGIC
# MAGIC Let me show you what an SDP pipeline notebook looks like:

# COMMAND ----------

# MAGIC %md
# MAGIC ### SDP Pipeline Definition (What it looks like)
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC # BRONZE: Raw ingestion from Volume
# MAGIC @dlt.table(comment="Raw heart disease data from UCI dataset")
# MAGIC def heart_bronze():
# MAGIC     return (spark.read
# MAGIC         .format("csv")
# MAGIC         .option("header", "true")
# MAGIC         .option("inferSchema", "true")
# MAGIC         .load("/Volumes/dataops_olympics/default/raw_data/heart_disease/heart.csv"))
# MAGIC
# MAGIC # SILVER: Cleaned with data quality expectations
# MAGIC @dlt.table(comment="Cleaned heart disease data with quality checks")
# MAGIC @dlt.expect_or_drop("valid_age", "age BETWEEN 1 AND 120")
# MAGIC @dlt.expect_or_drop("valid_bp", "trestbps BETWEEN 50 AND 300")
# MAGIC @dlt.expect("valid_cholesterol", "chol BETWEEN 50 AND 600")
# MAGIC def heart_silver():
# MAGIC     return dlt.read("heart_bronze").withColumn("ingested_at", current_timestamp())
# MAGIC
# MAGIC # GOLD: Business-ready aggregation
# MAGIC @dlt.table(comment="Heart disease metrics by age group")
# MAGIC def heart_gold():
# MAGIC     return (dlt.read("heart_silver")
# MAGIC         .withColumn("age_group",
# MAGIC             when(col("age") < 40, "1. Under 40")
# MAGIC             .when(col("age") < 50, "2. 40-49")
# MAGIC             .when(col("age") < 60, "3. 50-59")
# MAGIC             .otherwise("4. 60+"))
# MAGIC         .groupBy("age_group", "target")
# MAGIC         .agg(
# MAGIC             count("*").alias("patients"),
# MAGIC             round(avg("chol"), 1).alias("avg_cholesterol"),
# MAGIC             round(avg("trestbps"), 1).alias("avg_blood_pressure"),
# MAGIC             round(avg("thalach"), 1).alias("avg_max_heart_rate")))
# MAGIC ```
# MAGIC
# MAGIC > **Say this:** "Notice three things:
# MAGIC > 1. Each table is a Python function decorated with `@dlt.table`
# MAGIC > 2. Silver has `@dlt.expect` — these are data quality rules. If age is invalid, the row is dropped.
# MAGIC > 3. No `write` calls — the pipeline handles all the orchestration."

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's Build the Medallion with SQL (Interactive Demo)
# MAGIC
# MAGIC > **Say this:** "Since we can't run an SDP pipeline interactively in this notebook,
# MAGIC > let me show you the equivalent medallion flow in SQL — same logic, same result.
# MAGIC > In the competition, you'll create an actual SDP pipeline."

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SILVER: Clean the data (same logic as @dlt.expect_or_drop)
# MAGIC CREATE OR REPLACE TABLE dataops_olympics.default.demo_heart_silver AS
# MAGIC SELECT *, current_timestamp() as ingested_at
# MAGIC FROM dataops_olympics.default.demo_heart_bronze
# MAGIC WHERE age BETWEEN 1 AND 120
# MAGIC   AND trestbps BETWEEN 50 AND 300
# MAGIC   AND chol BETWEEN 50 AND 600

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM dataops_olympics.default.demo_heart_bronze) as bronze_rows,
# MAGIC   (SELECT COUNT(*) FROM dataops_olympics.default.demo_heart_silver) as silver_rows,
# MAGIC   (SELECT COUNT(*) FROM dataops_olympics.default.demo_heart_bronze) -
# MAGIC   (SELECT COUNT(*) FROM dataops_olympics.default.demo_heart_silver) as rows_filtered

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GOLD: Business-ready aggregation
# MAGIC CREATE OR REPLACE TABLE dataops_olympics.default.demo_heart_gold AS
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN age < 40 THEN '1. Under 40'
# MAGIC         WHEN age < 50 THEN '2. 40-49'
# MAGIC         WHEN age < 60 THEN '3. 50-59'
# MAGIC         ELSE '4. 60+'
# MAGIC     END as age_group,
# MAGIC     CASE WHEN target = 1 THEN 'Disease' ELSE 'Healthy' END as status,
# MAGIC     COUNT(*) as patients,
# MAGIC     ROUND(AVG(chol), 1) as avg_cholesterol,
# MAGIC     ROUND(AVG(trestbps), 1) as avg_blood_pressure,
# MAGIC     ROUND(AVG(thalach), 1) as avg_max_heart_rate
# MAGIC FROM dataops_olympics.default.demo_heart_silver
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dataops_olympics.default.demo_heart_gold

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "Three tables. Raw → Clean → Aggregated. That's the Medallion
# MAGIC > Architecture. With SDP, Databricks manages dependencies, retries, and scheduling.
# MAGIC > You just declare the tables and the quality rules."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Wow Moment — Delta Time Travel
# MAGIC
# MAGIC > **Say this:** "Here's the magic trick. Let's say someone accidentally overwrites
# MAGIC > your Silver table with bad data..."

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE dataops_olympics.default.demo_heart_silver
# MAGIC SELECT *, current_timestamp() as ingested_at FROM dataops_olympics.default.demo_heart_bronze WHERE age < 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as current_rows FROM dataops_olympics.default.demo_heart_silver

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "Disaster, right? Your table is empty. But with Delta Lake, every version
# MAGIC > is saved. Watch this..."

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY dataops_olympics.default.demo_heart_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE dataops_olympics.default.demo_heart_silver TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as restored_rows FROM dataops_olympics.default.demo_heart_silver

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "One command: RESTORE TABLE. Your data is back. No backup needed,
# MAGIC > no panicking. That's Delta Lake. Now it's your turn — open the practice notebook!"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dataops_olympics.default.demo_heart_bronze;
# MAGIC DROP TABLE IF EXISTS dataops_olympics.default.demo_heart_silver;
# MAGIC DROP TABLE IF EXISTS dataops_olympics.default.demo_heart_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways for Participants
# MAGIC
# MAGIC 1. **Unity Catalog Volumes** — governed cloud storage for raw files (`/Volumes/catalog/schema/volume/`)
# MAGIC 2. **Spark reads any format** — CSV, JSON, Parquet, etc. with one line
# MAGIC 3. **Delta Lake** = ACID transactions + time travel + schema enforcement
# MAGIC 4. **Unity Catalog governance** = table/column comments so your data is self-documenting
# MAGIC 5. **Spark Declarative Pipelines** = declare tables with `@dlt.table`, add quality rules with `@dlt.expect`
# MAGIC 6. **Medallion Architecture** = Bronze (raw) → Silver (clean) → Gold (aggregated)
# MAGIC 7. **Use the Databricks Assistant** (`Cmd+I`) to generate all of this code from English prompts
