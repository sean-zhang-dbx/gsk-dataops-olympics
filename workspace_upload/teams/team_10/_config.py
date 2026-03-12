# Databricks notebook source
# MAGIC %md
# MAGIC # Team Configuration
# MAGIC
# MAGIC Your team name and catalog are pre-assigned. Just **Run All** — no changes needed.

# COMMAND ----------

TEAM_NAME = "team_10"
CATALOG = TEAM_NAME
SHARED_CATALOG = "dataops_olympics"
SHARED_SCHEMA = "default"
VOLUME_PATH = f"/Volumes/{SHARED_CATALOG}/{SHARED_SCHEMA}/raw_data"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")
print(f"Team: {TEAM_NAME}  |  Catalog: {CATALOG}.default  |  Shared: {SHARED_CATALOG}.default")
