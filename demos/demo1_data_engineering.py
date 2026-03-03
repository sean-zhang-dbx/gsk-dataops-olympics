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
# MAGIC | 1:00 | **Ingest** | CSV + JSON → Spark DataFrames |
# MAGIC | 4:00 | **Delta Lake** | Write to Delta, explain ACID + time travel |
# MAGIC | 7:00 | **Governance** | Table & column comments in Unity Catalog |
# MAGIC | 9:00 | **Medallion** | Bronze → Silver → Gold in 3 SQL statements |
# MAGIC | 13:00 | **Wow Moment** | Time travel — query yesterday's data today |
# MAGIC | 14:00 | **Your Turn** | Preview of the practice notebook |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Problem
# MAGIC
# MAGIC > **Say this:** "Imagine you're at GSK and a new clinical trial dataset lands on your desk —
# MAGIC > a messy CSV with 500 patient records. Your job: get it into a governed, queryable table
# MAGIC > that the analytics team can trust. That's what we're building in the next 14 minutes."

# COMMAND ----------

spark.sql("USE dataops_olympics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest Raw Data (CSV)
# MAGIC
# MAGIC > **Say this:** "Step one — read the file. Spark handles CSV, JSON, Parquet, and more.
# MAGIC > Three options, one line of code."

# COMMAND ----------

csv_path = "file:/tmp/dataops_olympics/raw/heart_disease/heart.csv"

df_heart = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(csv_path))

print(f"Loaded {df_heart.count()} rows × {len(df_heart.columns)} columns")
display(df_heart.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "That's it. One line. Spark inferred all the data types — age is integer,
# MAGIC > cholesterol is integer, oldpeak is double. No pandas, no manual parsing."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest JSON Data
# MAGIC
# MAGIC > **Say this:** "What about JSON? Same pattern, different format option.
# MAGIC > This file is a JSON array, so we add multiLine."

# COMMAND ----------

json_path = "file:/tmp/dataops_olympics/raw/life_expectancy/life_expectancy_sample.json"

df_life = (spark.read
    .format("json")
    .option("multiLine", "true")
    .load(json_path))

print(f"Loaded {df_life.count()} rows × {len(df_life.columns)} columns")
display(df_life.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Delta Lake
# MAGIC
# MAGIC > **Say this:** "Now the important part. We don't save as CSV or Parquet — we save as
# MAGIC > **Delta Lake**. Why? Three words: ACID transactions, time travel, schema enforcement.
# MAGIC > Your table will never be in a half-written, corrupted state."

# COMMAND ----------

df_heart.write.format("delta").mode("overwrite").saveAsTable("demo_heart_bronze")
print("Saved as Delta table: demo_heart_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's see what we just created
# MAGIC DESCRIBE DETAIL demo_heart_bronze

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
# MAGIC ALTER TABLE demo_heart_bronze SET TBLPROPERTIES (
# MAGIC   'comment' = 'UCI Heart Disease dataset: 500 patients, 14 clinical features, target=1 means disease present'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE demo_heart_bronze ALTER COLUMN age COMMENT 'Patient age in years (29-77)';
# MAGIC ALTER TABLE demo_heart_bronze ALTER COLUMN chol COMMENT 'Serum cholesterol in mg/dL';
# MAGIC ALTER TABLE demo_heart_bronze ALTER COLUMN target COMMENT 'Diagnosis: 1 = heart disease, 0 = healthy';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now anyone can discover what this data means
# MAGIC DESCRIBE TABLE demo_heart_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "Look — every column now has a description. When your colleague opens
# MAGIC > this table next month, they know exactly what 'target' means without asking you."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Medallion Architecture (Bronze → Silver → Gold)
# MAGIC
# MAGIC > **Say this:** "The standard pattern in data engineering is the Medallion Architecture.
# MAGIC > Bronze is raw data. Silver is cleaned. Gold is business-ready aggregations.
# MAGIC > Let me build all three layers in 60 seconds."

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SILVER: Clean the data (remove invalid records)
# MAGIC CREATE OR REPLACE TABLE demo_heart_silver AS
# MAGIC SELECT *
# MAGIC FROM demo_heart_bronze
# MAGIC WHERE age BETWEEN 1 AND 120
# MAGIC   AND trestbps BETWEEN 50 AND 300
# MAGIC   AND chol BETWEEN 50 AND 600

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM demo_heart_bronze) as bronze_rows,
# MAGIC   (SELECT COUNT(*) FROM demo_heart_silver) as silver_rows,
# MAGIC   (SELECT COUNT(*) FROM demo_heart_bronze) - (SELECT COUNT(*) FROM demo_heart_silver) as rows_removed

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GOLD: Business-ready aggregation
# MAGIC CREATE OR REPLACE TABLE demo_heart_gold AS
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
# MAGIC FROM demo_heart_silver
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_heart_gold

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "Three SQL statements. Raw → Clean → Aggregated. That's the Medallion
# MAGIC > Architecture. In production, this runs on a schedule. Today, you'll build it by hand."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Wow Moment — Delta Time Travel
# MAGIC
# MAGIC > **Say this:** "Here's the magic trick. Let's say someone accidentally overwrites
# MAGIC > your Silver table with bad data..."

# COMMAND ----------

# MAGIC %sql
# MAGIC -- "Oops" — overwrite with garbage
# MAGIC INSERT OVERWRITE demo_heart_silver SELECT * FROM demo_heart_bronze WHERE age < 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The table looks empty!
# MAGIC SELECT COUNT(*) as current_rows FROM demo_heart_silver

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "Disaster, right? Your table is empty. But with Delta Lake, every version
# MAGIC > is saved. Watch this..."

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time travel: see the version history
# MAGIC DESCRIBE HISTORY demo_heart_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore to the previous version
# MAGIC RESTORE TABLE demo_heart_silver TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data is back!
# MAGIC SELECT COUNT(*) as restored_rows FROM demo_heart_silver

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "One command: RESTORE TABLE. Your data is back. No backup needed,
# MAGIC > no panicking. That's Delta Lake. Now it's your turn — open the practice notebook!"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS demo_heart_bronze;
# MAGIC DROP TABLE IF EXISTS demo_heart_silver;
# MAGIC DROP TABLE IF EXISTS demo_heart_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways for Participants
# MAGIC
# MAGIC 1. **Spark reads any format** — CSV, JSON, Parquet, etc. with one line
# MAGIC 2. **Delta Lake** = ACID transactions + time travel + schema enforcement
# MAGIC 3. **Unity Catalog governance** = table/column comments so your data is self-documenting
# MAGIC 4. **Medallion Architecture** = Bronze (raw) → Silver (clean) → Gold (aggregated)
# MAGIC 5. **Use the Databricks Assistant** (`Cmd+I`) to generate all of this code from English prompts
