# Databricks notebook source
# MAGIC %md
# MAGIC # Event 1: Data Engineering — Speed Sprint
# MAGIC
# MAGIC ## Challenge: Fastest End-to-End Pipeline
# MAGIC **Build Time: ~20 minutes**
# MAGIC
# MAGIC ### Objective
# MAGIC Build a complete data pipeline using **Spark Declarative Pipelines (SDP)** that:
# MAGIC 1. Ingests CSV and JSON healthcare data from Unity Catalog Volumes
# MAGIC 2. Creates governed Delta tables with Unity Catalog
# MAGIC 3. Adds table and column comments for governance
# MAGIC 4. Builds a Medallion flow (Bronze → Silver → Gold) with data quality expectations
# MAGIC
# MAGIC ### How You Win
# MAGIC **First team to complete all steps wins Gold!**
# MAGIC
# MAGIC ### Datasets (in Unity Catalog Volumes)
# MAGIC - **Heart Disease** (`/Volumes/dataops_olympics/default/raw_data/heart_disease/heart.csv`) — 500 rows CSV
# MAGIC - **Life Expectancy** (`/Volumes/dataops_olympics/default/raw_data/life_expectancy/life_expectancy_sample.json`) — 100 rows JSON
# MAGIC
# MAGIC > **Tip:** Use the Databricks Assistant (`Cmd+I`) to generate code from prompts!
# MAGIC
# MAGIC ### Databricks Assistant — Prompt Gallery
# MAGIC
# MAGIC Try these prompts in the Assistant panel to speed through the challenge:
# MAGIC
# MAGIC | Task | Prompt to Try |
# MAGIC |------|--------------|
# MAGIC | **Read CSV** | "Read the CSV at `/Volumes/dataops_olympics/default/raw_data/heart_disease/heart.csv` with headers and infer schema" |
# MAGIC | **Create Delta** | "Write this DataFrame as a Delta table called `dataops_olympics.default.team_XX_heart_disease`" |
# MAGIC | **Add governance** | "Add a table comment and column comments for age, chol, and target" |
# MAGIC | **SDP Pipeline** | "Create a Spark Declarative Pipeline notebook with Bronze, Silver (with expectations), and Gold tables for heart disease data" |
# MAGIC | **Explain code** | Select any cell and ask "Explain what this code does" |

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dataops_olympics;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Ingest CSV Data (Heart Disease)
# MAGIC
# MAGIC **TODO:** Read the Heart Disease CSV file from the Volume and create a Delta table.

# COMMAND ----------

TEAM_NAME = "_____"  # e.g., "team_01"

csv_path = "/Volumes/dataops_olympics/default/raw_data/heart_disease/heart.csv"

# TODO: Read the heart disease CSV into a Spark DataFrame
# Hint: spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_path)
df_heart = _____  # YOUR CODE HERE

display(df_heart)

# COMMAND ----------

# TODO: Write as a Delta table with fully qualified name
df_heart.write.format("delta").mode("overwrite").saveAsTable(f"dataops_olympics.default.{TEAM_NAME}_heart_disease")
print(f"Created table: dataops_olympics.default.{TEAM_NAME}_heart_disease")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Ingest JSON Data (Life Expectancy)
# MAGIC
# MAGIC **TODO:** Read the Life Expectancy JSON file and create a Delta table.

# COMMAND ----------

json_path = "/Volumes/dataops_olympics/default/raw_data/life_expectancy/life_expectancy_sample.json"

# TODO: Read the JSON file — Hint: use .option("multiLine", "true") for JSON arrays
df_life = _____  # YOUR CODE HERE

df_life.write.format("delta").mode("overwrite").saveAsTable(f"dataops_olympics.default.{TEAM_NAME}_life_expectancy")
display(df_life)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Add Governance — Table & Column Comments
# MAGIC
# MAGIC **TODO:** Add descriptive comments to your tables and key columns.
# MAGIC This is how data teams document their assets in Unity Catalog.

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE dataops_olympics.default.{TEAM_NAME}_heart_disease
    SET TBLPROPERTIES ('comment' = '______')
""")

spark.sql(f"ALTER TABLE dataops_olympics.default.{TEAM_NAME}_heart_disease ALTER COLUMN age COMMENT '______'")
spark.sql(f"ALTER TABLE dataops_olympics.default.{TEAM_NAME}_heart_disease ALTER COLUMN chol COMMENT '______'")
spark.sql(f"ALTER TABLE dataops_olympics.default.{TEAM_NAME}_heart_disease ALTER COLUMN target COMMENT '______'")

print("Governance comments added!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Build a Medallion Pipeline
# MAGIC
# MAGIC ### Option A: Spark Declarative Pipelines (Recommended)
# MAGIC
# MAGIC Create a **new notebook** with the SDP pipeline definition below, then create a Pipeline
# MAGIC in the Databricks UI to run it.
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC TEAM_NAME = "team_XX"  # Change this!
# MAGIC
# MAGIC @dlt.table(comment="Raw heart disease data from UCI dataset")
# MAGIC def heart_bronze():
# MAGIC     return (spark.read
# MAGIC         .format("csv")
# MAGIC         .option("header", "true")
# MAGIC         .option("inferSchema", "true")
# MAGIC         .load("/Volumes/dataops_olympics/default/raw_data/heart_disease/heart.csv"))
# MAGIC
# MAGIC @dlt.table(comment="Cleaned heart disease data")
# MAGIC @dlt.expect_or_drop("valid_age", "age BETWEEN 1 AND 120")
# MAGIC @dlt.expect_or_drop("valid_bp", "trestbps BETWEEN 50 AND 300")
# MAGIC @dlt.expect("valid_cholesterol", "chol BETWEEN 50 AND 600")
# MAGIC def heart_silver():
# MAGIC     return dlt.read("heart_bronze").withColumn("ingested_at", current_timestamp())
# MAGIC
# MAGIC @dlt.table(comment="Heart disease metrics by age group")
# MAGIC def heart_gold():
# MAGIC     return (dlt.read("heart_silver")
# MAGIC         .withColumn("age_group",
# MAGIC             when(col("age") < 40, "Under 40")
# MAGIC             .when(col("age") < 50, "40-49")
# MAGIC             .when(col("age") < 60, "50-59")
# MAGIC             .otherwise("60+"))
# MAGIC         .groupBy("age_group", "target")
# MAGIC         .agg(
# MAGIC             count("*").alias("patient_count"),
# MAGIC             round(avg("chol"), 1).alias("avg_cholesterol"),
# MAGIC             round(avg("trestbps"), 1).alias("avg_blood_pressure"),
# MAGIC             round(avg("thalach"), 1).alias("avg_max_heart_rate")))
# MAGIC ```
# MAGIC
# MAGIC ### Option B: Interactive SQL (If Pipeline not available)
# MAGIC
# MAGIC If you can't create an SDP pipeline, build the medallion with SQL:

# COMMAND ----------

# Silver table — clean the data (same logic as SDP expectations)
spark.sql(f"""
    CREATE OR REPLACE TABLE dataops_olympics.default.{TEAM_NAME}_heart_disease_silver AS
    SELECT *
    FROM dataops_olympics.default.{TEAM_NAME}_heart_disease
    WHERE age BETWEEN 1 AND 120
      AND trestbps BETWEEN 50 AND 300
      AND chol BETWEEN 50 AND 600
""")

silver_count = spark.table(f"dataops_olympics.default.{TEAM_NAME}_heart_disease_silver").count()
print(f"Silver table: {silver_count} rows (cleaned from raw)")

# COMMAND ----------

# Gold table — aggregated view for analytics
spark.sql(f"""
    CREATE OR REPLACE TABLE dataops_olympics.default.{TEAM_NAME}_heart_disease_gold AS
    SELECT
        CASE
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END as age_group,
        target as heart_disease,
        COUNT(*) as patient_count,
        AVG(chol) as avg_cholesterol,
        AVG(trestbps) as avg_blood_pressure,
        AVG(thalach) as avg_max_heart_rate
    FROM dataops_olympics.default.{TEAM_NAME}_heart_disease_silver
    GROUP BY 1, 2
    ORDER BY 1, 2
""")

display(spark.table(f"dataops_olympics.default.{TEAM_NAME}_heart_disease_gold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Your Pipeline
# MAGIC
# MAGIC Run this cell to validate your work and signal completion!

# COMMAND ----------

print("=" * 60)
print(f"  DATA ENGINEERING VALIDATION — {TEAM_NAME}")
print("=" * 60)

checks = []
score = 0

try:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM dataops_olympics.default.{TEAM_NAME}_heart_disease").collect()[0].cnt
    checks.append(f"  Bronze (heart_disease): {cnt} rows")
    score += 1
except Exception as e:
    checks.append(f"  Bronze (heart_disease): MISSING - {e}")

try:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM dataops_olympics.default.{TEAM_NAME}_life_expectancy").collect()[0].cnt
    checks.append(f"  Bronze (life_expectancy): {cnt} rows")
    score += 1
except Exception as e:
    checks.append(f"  Bronze (life_expectancy): MISSING - {e}")

try:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM dataops_olympics.default.{TEAM_NAME}_heart_disease_silver").collect()[0].cnt
    checks.append(f"  Silver (cleaned): {cnt} rows")
    score += 1
except Exception as e:
    checks.append(f"  Silver table: MISSING - {e}")

try:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM dataops_olympics.default.{TEAM_NAME}_heart_disease_gold").collect()[0].cnt
    checks.append(f"  Gold (aggregated): {cnt} rows")
    score += 1
except Exception as e:
    checks.append(f"  Gold table: MISSING - {e}")

try:
    props = spark.sql(f"DESCRIBE TABLE EXTENDED dataops_olympics.default.{TEAM_NAME}_heart_disease").collect()
    has_comment = any("comment" in str(row).lower() and row[1] and len(str(row[1])) > 5 for row in props)
    if has_comment:
        checks.append("  Governance: Comments configured")
        score += 1
    else:
        checks.append("  Governance: Comments NOT found")
except Exception as e:
    checks.append(f"  Governance: Could not verify - {e}")

for c in checks:
    print(c)

print(f"\nChecks passed: {score}/5")
if score == 5:
    print("\nALL CHECKS PASSED! RAISE YOUR HAND!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stretch Goals (Extra Credit)
# MAGIC
# MAGIC Finished early? Try these with the Databricks Assistant:
# MAGIC
# MAGIC 1. **Create an actual SDP Pipeline** — Create a new notebook with the `@dlt.table` code from Step 4, then go to Workflows > Pipelines > Create Pipeline
# MAGIC 2. **Liquid Clustering** — Ask: *"Alter the Silver table to use liquid clustering on age and target columns"*
# MAGIC 3. **Change Data Feed** — Ask: *"Enable Change Data Feed on the Silver table and show me how to query the changes"*
# MAGIC 4. **Time Travel** — Ask: *"Show me how to query the previous version of my Silver table using Delta time travel"*
# MAGIC 5. **AI Functions** — Ask: *"Use ai_classify() in SQL to classify heart disease risk as 'High', 'Medium', 'Low' based on the cholesterol and age columns"*
