# Databricks notebook source
# MAGIC %md
# MAGIC # Plot Twist 1: Schema Change — Heal the Pipeline!
# MAGIC
# MAGIC ## The Scenario
# MAGIC Your data source just pushed a schema change. New columns appeared,
# MAGIC some columns were renamed, and your data engineering pipeline is broken.
# MAGIC
# MAGIC **You have 10 minutes to heal it.**
# MAGIC
# MAGIC ## What Changed
# MAGIC - Column `trestbps` was renamed to `resting_bp`
# MAGIC - Column `chol` was renamed to `cholesterol`
# MAGIC - New column `bmi` was added (Body Mass Index)
# MAGIC - New column `smoking_status` was added (0=never, 1=former, 2=current)
# MAGIC - Column `thal` was removed (deprecated from source)
# MAGIC
# MAGIC ## Your Task
# MAGIC 1. Read the new schema data (provided below)
# MAGIC 2. Update your pipeline to handle the schema change
# MAGIC 3. Ensure your Silver and Gold tables still work
# MAGIC 4. No downstream queries should break

# COMMAND ----------

# MAGIC %md
# MAGIC ## The New Data (Changed Schema)

# COMMAND ----------

import pandas as pd
import numpy as np

np.random.seed(99)

# Generate the "changed schema" version of heart disease data
df_original = spark.table("heart_disease").toPandas()

df_new = df_original.copy()
df_new = df_new.rename(columns={
    "trestbps": "resting_bp",
    "chol": "cholesterol"
})
df_new["bmi"] = np.round(np.random.uniform(18.5, 40.0, len(df_new)), 1)
df_new["smoking_status"] = np.random.choice([0, 1, 2], len(df_new), p=[0.4, 0.35, 0.25])
df_new = df_new.drop(columns=["thal"])

# Save as the "new source"
new_path = "/Volumes/dataops_olympics/default/raw_data/heart_disease/heart_v2.csv"
df_new.to_csv(new_path, index=False)

sdf_new = spark.createDataFrame(df_new)
sdf_new.write.format("delta").mode("overwrite").saveAsTable("heart_disease_v2")

print("New schema data loaded!")
print(f"\nOld columns: {sorted(df_original.columns.tolist())}")
print(f"New columns: {sorted(df_new.columns.tolist())}")
print(f"\nRemoved: thal")
print(f"Renamed: trestbps -> resting_bp, chol -> cholesterol")
print(f"Added:   bmi, smoking_status")

display(sdf_new.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Fix
# MAGIC
# MAGIC **TODO:** Update your pipeline to work with the new schema.
# MAGIC
# MAGIC Hints:
# MAGIC - Use `ALTER TABLE ... RENAME COLUMN` or create a view with aliases
# MAGIC - Use `MERGE INTO` or `INSERT INTO ... SELECT` to handle schema evolution
# MAGIC - Delta Lake supports schema evolution: `.option("mergeSchema", "true")`
# MAGIC - Consider creating a compatibility view that maps old names to new names

# COMMAND ----------

TEAM_NAME = "_____"

# TODO: Heal your pipeline here!
# The goal is to make your Silver and Gold tables work with the new data.

# YOUR CODE HERE


# COMMAND ----------

# Validation: Check that downstream tables still work
print("=" * 60)
print("  SCHEMA CHANGE VALIDATION")
print("=" * 60)

try:
    gold = spark.sql(f"SELECT * FROM {TEAM_NAME}_heart_disease_gold LIMIT 5")
    print(f"  Gold table: OK ({gold.count()} rows)")
except Exception as e:
    print(f"  Gold table: BROKEN — {e}")

print("=" * 60)
