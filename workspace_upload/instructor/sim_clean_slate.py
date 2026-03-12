# Databricks notebook source
# MAGIC %md
# MAGIC # Simulation Clean Slate
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**
# MAGIC
# MAGIC Drops team catalogs and resets shared scoring tables to prepare for a fresh simulation run.
# MAGIC Does NOT drop the `dataops_olympics` catalog itself or the raw data volume.

# COMMAND ----------

SHARED_CATALOG = "dataops_olympics"
SIM_TEAMS = [f"team_{i:02d}" for i in range(1, 6)]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Drop Team Catalogs

# COMMAND ----------

for team in SIM_TEAMS:
    try:
        spark.sql(f"DROP CATALOG IF EXISTS {team} CASCADE")
        print(f"  [OK] Dropped catalog: {team}")
    except Exception as e:
        print(f"  [SKIP] {team}: {str(e)[:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Recreate Team Catalogs

# COMMAND ----------

for team in SIM_TEAMS:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {team}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {team}.default")
    print(f"  [OK] Created catalog: {team}.default")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Reset Shared Scoring Tables

# COMMAND ----------

scoring_tables = [
    "event_submissions",
    "olympics_leaderboard",
    "registered_teams",
    "event1_scores",
    "event2_scores",
    "event3_scores",
    "event2_answer_key",
]

for table in scoring_tables:
    fqn = f"{SHARED_CATALOG}.default.{table}"
    try:
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        print(f"  [OK] Dropped: {fqn}")
    except Exception as e:
        print(f"  [SKIP] {fqn}: {str(e)[:60]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Recreate Core Shared Tables

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SHARED_CATALOG}.default.event_submissions (
        team_name STRING, event_name STRING, submission_type STRING,
        asset_reference STRING, submitted_at STRING, submitted_by STRING
    )
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SHARED_CATALOG}.default.olympics_leaderboard (
        team STRING, event STRING, category STRING,
        points DOUBLE, max_points DOUBLE, scored_at TIMESTAMP
    )
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SHARED_CATALOG}.default.registered_teams (
        team STRING
    )
""")
print("  Core shared tables recreated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Clean MLflow Experiments

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()
for team in SIM_TEAMS:
    exp_path = f"/Shared/{team}_heart_ml"
    try:
        exp = client.get_experiment_by_name(exp_path)
        if exp:
            client.delete_experiment(exp.experiment_id)
            print(f"  [OK] Deleted MLflow experiment: {exp_path}")
        else:
            print(f"  [SKIP] No experiment at: {exp_path}")
    except Exception as e:
        print(f"  [SKIP] {exp_path}: {str(e)[:60]}")

# COMMAND ----------

print("=" * 60)
print("  CLEAN SLATE COMPLETE")
print(f"  Teams reset: {', '.join(SIM_TEAMS)}")
print(f"  Shared catalog preserved: {SHARED_CATALOG}")
print("=" * 60)
