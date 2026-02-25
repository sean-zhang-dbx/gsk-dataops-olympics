# Databricks notebook source
# MAGIC %md
# MAGIC # Lightning Talk 1: Data Engineering Demo
# MAGIC
# MAGIC **For organizers only — this is the 15-minute demo shown before Event 1.**
# MAGIC
# MAGIC This notebook demonstrates the complete data engineering pipeline that
# MAGIC teams will replicate during the build session.
# MAGIC
# MAGIC ## What We'll Show
# MAGIC 1. Ingesting CSV and JSON data with Spark
# MAGIC 2. Creating Delta tables in Unity Catalog
# MAGIC 3. Adding governance (table/column comments)
# MAGIC 4. Building a Bronze -> Silver -> Gold medallion flow

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Ingestion — CSV

# COMMAND ----------

# Read CSV data
csv_path = "file:/tmp/dataops_olympics/raw/heart_disease/heart.csv"
df_heart = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_path)

print(f"Heart Disease: {df_heart.count()} rows, {len(df_heart.columns)} columns")
display(df_heart.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Ingestion — JSON

# COMMAND ----------

json_path = "file:/tmp/dataops_olympics/raw/life_expectancy/life_expectancy_sample.json"
df_life = spark.read.format("json").option("multiLine", "true").load(json_path)

print(f"Life Expectancy: {df_life.count()} rows, {len(df_life.columns)} columns")
display(df_life.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Delta Tables with Governance

# COMMAND ----------

# Write as Delta tables
df_heart.write.format("delta").mode("overwrite").saveAsTable("demo_heart_disease")
df_life.write.format("delta").mode("overwrite").saveAsTable("demo_life_expectancy")

# Add governance — table comments
spark.sql("""
    ALTER TABLE demo_heart_disease
    SET TBLPROPERTIES ('comment' = 'UCI Heart Disease dataset — 500 patient records with 14 clinical features for heart disease prediction')
""")

# Add governance — column comments
spark.sql("ALTER TABLE demo_heart_disease ALTER COLUMN age COMMENT 'Patient age in years'")
spark.sql("ALTER TABLE demo_heart_disease ALTER COLUMN sex COMMENT 'Sex: 1 = male, 0 = female'")
spark.sql("ALTER TABLE demo_heart_disease ALTER COLUMN cp COMMENT 'Chest pain type: 0=typical angina, 1=atypical, 2=non-anginal, 3=asymptomatic'")
spark.sql("ALTER TABLE demo_heart_disease ALTER COLUMN chol COMMENT 'Serum cholesterol in mg/dl'")
spark.sql("ALTER TABLE demo_heart_disease ALTER COLUMN target COMMENT 'Heart disease diagnosis: 1=disease present, 0=healthy'")

print("Delta tables created with governance!")
display(spark.sql("DESCRIBE TABLE EXTENDED demo_heart_disease"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Medallion Architecture — Bronze -> Silver -> Gold

# COMMAND ----------

# Bronze: Already done (raw data as-is)
print("Bronze: demo_heart_disease (raw ingested data)")

# Silver: Cleaned data
spark.sql("""
    CREATE OR REPLACE TABLE demo_heart_disease_silver AS
    SELECT *
    FROM demo_heart_disease
    WHERE age > 0 AND age < 120
      AND trestbps > 50 AND trestbps < 300
      AND chol > 50 AND chol < 600
""")

silver_count = spark.table("demo_heart_disease_silver").count()
print(f"Silver: demo_heart_disease_silver ({silver_count} rows — cleaned)")

# Gold: Aggregated for analytics
spark.sql("""
    CREATE OR REPLACE TABLE demo_heart_disease_gold AS
    SELECT
        CASE
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END as age_group,
        target as heart_disease,
        COUNT(*) as patient_count,
        ROUND(AVG(chol), 1) as avg_cholesterol,
        ROUND(AVG(trestbps), 1) as avg_blood_pressure,
        ROUND(AVG(thalach), 1) as avg_max_heart_rate
    FROM demo_heart_disease_silver
    GROUP BY 1, 2
    ORDER BY 1, 2
""")

print("Gold: demo_heart_disease_gold (aggregated)")
display(spark.table("demo_heart_disease_gold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC **Key points to emphasize during the talk:**
# MAGIC - Spark reads CSV/JSON with one line of code
# MAGIC - Delta Lake is the default table format — ACID, time travel, schema enforcement
# MAGIC - Unity Catalog provides governance — comments describe your data for other users
# MAGIC - Medallion architecture (Bronze -> Silver -> Gold) is the standard pattern
# MAGIC - The Databricks Assistant can help you write all of this code!
