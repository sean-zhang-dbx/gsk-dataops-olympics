# Databricks notebook source
# MAGIC %md
# MAGIC # Event 1 Simulation Runner — team_01
# MAGIC Simulates a team completing Event 1: Data Engineering (SQL path, max points)

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_submit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Bronze — Read all NDJSON from volume

# COMMAND ----------

bronze_df = spark.read.format("json").load(f"{VOLUME_PATH}/heart_events/")
bronze_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_bronze")
bronze_count = spark.table(f"{CATALOG}.default.heart_bronze").count()
print(f"Bronze: {bronze_count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Silver — Clean, filter, deduplicate

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_df = spark.table(f"{CATALOG}.default.heart_bronze")

# Filter nulls and bad BP
silver_df = silver_df.filter(
    F.col("age").isNotNull() &
    (F.col("trestbps") > 0) &
    (F.col("trestbps") < 300)
)

# Deduplicate by event_id (keep latest)
w = Window.partitionBy("event_id").orderBy(F.col("event_timestamp").desc())
silver_df = silver_df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

silver_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_silver")
silver_count = spark.table(f"{CATALOG}.default.heart_silver").count()
print(f"Silver: {silver_count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Gold — Aggregate

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_gold AS
    SELECT
        CASE WHEN age < 40 THEN 'Under 40' WHEN age BETWEEN 40 AND 49 THEN '40-49'
             WHEN age BETWEEN 50 AND 59 THEN '50-59' ELSE '60+' END AS age_group,
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
        COUNT(*) AS patient_count,
        ROUND(AVG(chol), 1) AS avg_cholesterol,
        ROUND(AVG(trestbps), 1) AS avg_blood_pressure,
        ROUND(AVG(thalach), 1) AS avg_max_heart_rate
    FROM {CATALOG}.default.heart_silver
    GROUP BY 1, 2
""")
gold_count = spark.table(f"{CATALOG}.default.heart_gold").count()
print(f"Gold: {gold_count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Governance — Comments

# COMMAND ----------

spark.sql(f"COMMENT ON TABLE {CATALOG}.default.heart_bronze IS 'Raw heart event data from NDJSON volume ingestion'")
spark.sql(f"COMMENT ON TABLE {CATALOG}.default.heart_silver IS 'Cleaned patient data - nulls removed, BP validated, deduplicated by event_id'")
spark.sql(f"COMMENT ON TABLE {CATALOG}.default.heart_gold IS 'Aggregated heart disease metrics by age group and diagnosis'")

spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver ALTER COLUMN age COMMENT 'Patient age in years'")
spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver ALTER COLUMN trestbps COMMENT 'Resting blood pressure (mm Hg)'")
spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver ALTER COLUMN chol COMMENT 'Serum cholesterol in mg/dl'")
spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver ALTER COLUMN target COMMENT 'Heart disease diagnosis (1=disease, 0=healthy)'")
print("Governance comments applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Submit

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: UC Tags

# COMMAND ----------

spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver SET TAGS ('domain' = 'cardiology', 'quality_tier' = 'silver', 'pii' = 'false')")
spark.sql(f"ALTER TABLE {CATALOG}.default.heart_gold SET TAGS ('domain' = 'cardiology', 'quality_tier' = 'gold')")
print("UC tags applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: AI Gold Table

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.heart_gold_ai AS
    SELECT *, CASE WHEN avg_cholesterol > 250 AND avg_blood_pressure > 140 THEN 'High Risk'
        WHEN avg_cholesterol > 200 OR avg_blood_pressure > 130 THEN 'Moderate Risk' ELSE 'Low Risk' END AS cardiovascular_risk
    FROM {CATALOG}.default.heart_gold""")
print(f"Bonus heart_gold_ai: {spark.table(f'{CATALOG}.default.heart_gold_ai').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Submit

# COMMAND ----------

submit("event1", {"tables": ["heart_bronze", "heart_silver", "heart_gold", "heart_gold_ai"]})

# COMMAND ----------

print(f"EVENT 1 COMPLETE for {TEAM_NAME} (POWER TEAM — max score + bonus)")
print(f"  Bronze: {bronze_count} rows")
print(f"  Silver: {silver_count} rows")
print(f"  Gold:   {gold_count} rows")
