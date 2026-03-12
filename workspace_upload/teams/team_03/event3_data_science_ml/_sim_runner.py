# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.default.heart_silver_correct AS SELECT * FROM {SHARED_CATALOG}.default.heart_disease")
# Team struggled, only loaded data but couldn't get ML working
print("team_03: only managed to load data, no ML model trained")
submit("event3", {"note": "only EDA"})
