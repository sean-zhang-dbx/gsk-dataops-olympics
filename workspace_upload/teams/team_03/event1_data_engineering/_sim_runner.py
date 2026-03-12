# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
from pyspark.sql import functions as F
# Bronze
bronze_df = spark.read.format("json").load(f"{VOLUME_PATH}/heart_events/")
bronze_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_bronze")
# COMMAND ----------
# Silver - filtered but NOT deduped
silver_df = spark.table(f"{CATALOG}.default.heart_bronze").filter(F.col("age").isNotNull() & (F.col("trestbps") > 0) & (F.col("trestbps") < 300))
silver_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_silver")
print(f"Silver (no dedup): {spark.table(f'{CATALOG}.default.heart_silver').count()} rows")
# COMMAND ----------
# Gold
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.heart_gold AS
    SELECT CASE WHEN age < 40 THEN 'Under 40' WHEN age BETWEEN 40 AND 49 THEN '40-49' WHEN age BETWEEN 50 AND 59 THEN '50-59' ELSE '60+' END AS age_group,
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
        COUNT(*) AS patient_count, ROUND(AVG(chol), 1) AS avg_cholesterol, ROUND(AVG(trestbps), 1) AS avg_blood_pressure, ROUND(AVG(thalach), 1) AS avg_max_heart_rate
    FROM {CATALOG}.default.heart_silver GROUP BY 1, 2""")
# COMMAND ----------
spark.sql(f"COMMENT ON TABLE {CATALOG}.default.heart_silver IS 'Patient data filtered'")
# COMMAND ----------
submit("event1", {"tables": ["heart_bronze", "heart_silver", "heart_gold"]})
print("team_03 Event 1 complete (no dedup, partial comments)")
