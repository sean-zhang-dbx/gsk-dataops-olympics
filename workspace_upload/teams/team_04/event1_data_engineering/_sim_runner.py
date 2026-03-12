# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
# Bronze - only loaded 3 of 5 batches (simulates struggle with path)
spark.read.format("json").load(f"{VOLUME_PATH}/heart_events/intake_batch_001.json").write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_bronze")
spark.read.format("json").load(f"{VOLUME_PATH}/heart_events/intake_batch_002.json").write.format("delta").mode("append").saveAsTable(f"{CATALOG}.default.heart_bronze")
spark.read.format("json").load(f"{VOLUME_PATH}/heart_events/intake_batch_003.json").write.format("delta").mode("append").saveAsTable(f"{CATALOG}.default.heart_bronze")
print(f"Bronze (3 batches): {spark.table(f'{CATALOG}.default.heart_bronze').count()} rows")
# COMMAND ----------
# Silver - basic filter only
from pyspark.sql import functions as F
spark.table(f"{CATALOG}.default.heart_bronze").filter(F.col("age").isNotNull()).write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_silver")
print(f"Silver: {spark.table(f'{CATALOG}.default.heart_silver').count()} rows")
# COMMAND ----------
# No gold - ran out of time
# COMMAND ----------
submit("event1", {"tables": ["heart_bronze", "heart_silver"]})
print("team_04 Event 1 complete (struggled - no gold)")
