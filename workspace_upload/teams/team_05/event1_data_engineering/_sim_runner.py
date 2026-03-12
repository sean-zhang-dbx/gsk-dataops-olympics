# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# Bronze - all data
bronze_df = spark.read.format("json").load(f"{VOLUME_PATH}/heart_events/")
bronze_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_bronze")
print(f"Bronze: {spark.table(f'{CATALOG}.default.heart_bronze').count()} rows")
# COMMAND ----------
# Silver - proper cleaning + dedup
silver_df = spark.table(f"{CATALOG}.default.heart_bronze").filter(F.col("age").isNotNull() & (F.col("trestbps") > 0) & (F.col("trestbps") < 300))
w = Window.partitionBy("event_id").orderBy(F.col("event_timestamp").desc())
silver_df = silver_df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
silver_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_silver")
print(f"Silver: {spark.table(f'{CATALOG}.default.heart_silver').count()} rows")
# COMMAND ----------
# Gold - correct aggregation
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.heart_gold AS
    SELECT CASE WHEN age < 40 THEN 'Under 40' WHEN age BETWEEN 40 AND 49 THEN '40-49' WHEN age BETWEEN 50 AND 59 THEN '50-59' ELSE '60+' END AS age_group,
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
        COUNT(*) AS patient_count, ROUND(AVG(chol), 1) AS avg_cholesterol, ROUND(AVG(trestbps), 1) AS avg_blood_pressure, ROUND(AVG(thalach), 1) AS avg_max_heart_rate
    FROM {CATALOG}.default.heart_silver GROUP BY 1, 2""")
print(f"Gold: {spark.table(f'{CATALOG}.default.heart_gold').count()} rows")
# COMMAND ----------
# Governance - table and column comments
spark.sql(f"COMMENT ON TABLE {CATALOG}.default.heart_bronze IS 'Raw heart event data from NDJSON'")
spark.sql(f"COMMENT ON TABLE {CATALOG}.default.heart_silver IS 'Cleaned and deduplicated patient data'")
spark.sql(f"COMMENT ON TABLE {CATALOG}.default.heart_gold IS 'Aggregated metrics by age group and diagnosis'")
spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver ALTER COLUMN age COMMENT 'Patient age in years'")
spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver ALTER COLUMN trestbps COMMENT 'Resting blood pressure (mm Hg)'")
spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver ALTER COLUMN chol COMMENT 'Serum cholesterol in mg/dl'")
spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver ALTER COLUMN target COMMENT 'Heart disease diagnosis (1=disease, 0=healthy)'")
print("Governance applied (no bonus)")
# COMMAND ----------
submit("event1", {"tables": ["heart_bronze", "heart_silver", "heart_gold"]})
print("team_05 Event 1 complete (full pipeline, no bonus)")
