# Databricks notebook source
# MAGIC %md
# MAGIC # Simulation Orchestrator — Events 1-3
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**
# MAGIC
# MAGIC Runs a full end-to-end simulation with 5 teams across Events 1-3.
# MAGIC Each step is called via `dbutils.notebook.run()` with error handling.
# MAGIC
# MAGIC ### Execution Order
# MAGIC 1. Clean slate (drop catalogs, reset tables)
# MAGIC 2. Instructor setup (shared data, team catalogs)
# MAGIC 3. Generate Event 2 answer key
# MAGIC 4. Event 1 simulation + scoring
# MAGIC 5. Event 2 simulation + scoring
# MAGIC 6. Event 3 simulation + scoring
# MAGIC 7. Scoreboard generation

# COMMAND ----------

BASE = "/Workspace/Shared/gsk-dataops-olympics"
SIM_TEAMS = ["team_01", "team_02", "team_03", "team_04", "team_05"]

def run_notebook(path, timeout=300, args=None):
    """Run a notebook with error handling."""
    label = path.split("/")[-1]
    print(f"  Running: {label} ...", end=" ")
    try:
        result = dbutils.notebook.run(path, timeout_seconds=timeout, arguments=args or {})
        print(f"DONE ({result or 'ok'})")
        return True
    except Exception as e:
        print(f"FAILED: {str(e)[:100]}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Clean Slate

# COMMAND ----------

print("=" * 60)
print("  STEP 1: CLEAN SLATE")
print("=" * 60)
run_notebook(f"{BASE}/instructor/sim_clean_slate", timeout=300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Instructor Setup

# COMMAND ----------

print("=" * 60)
print("  STEP 2: INSTRUCTOR SETUP (shared data + team catalogs)")
print("=" * 60)

for team in SIM_TEAMS:
    run_notebook(f"{BASE}/instructor/00_setup_and_data", timeout=600,
                 args={"TEAM_NAME": team, "TEAM_MEMBERS": ""})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate Event 2 Answer Key

# COMMAND ----------

print("=" * 60)
print("  STEP 3: GENERATE EVENT 2 ANSWER KEY")
print("=" * 60)
run_notebook(f"{BASE}/instructor/scoring/event2_benchmark_questions", timeout=300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Event 1 — Data Engineering

# COMMAND ----------

print("=" * 60)
print("  STEP 4: EVENT 1 SIMULATIONS")
print("=" * 60)

for team in SIM_TEAMS:
    run_notebook(f"{BASE}/teams/{team}/event1_data_engineering/_sim_runner", timeout=300)

# COMMAND ----------

print("=" * 60)
print("  STEP 4b: SCORE EVENT 1")
print("=" * 60)
run_notebook(f"{BASE}/instructor/scoring/event1_scoring", timeout=300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Event 2 — Data Analytics

# COMMAND ----------

print("=" * 60)
print("  STEP 5: EVENT 2 SIMULATIONS")
print("=" * 60)

for team in SIM_TEAMS:
    run_notebook(f"{BASE}/teams/{team}/event2_data_analytics/_sim_runner", timeout=300)

# COMMAND ----------

print("=" * 60)
print("  STEP 5b: SCORE EVENT 2")
print("=" * 60)
run_notebook(f"{BASE}/instructor/scoring/event2_scoring", timeout=300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Event 3 — Data Science / ML

# COMMAND ----------

print("=" * 60)
print("  STEP 6: EVENT 3 SIMULATIONS")
print("=" * 60)

for team in SIM_TEAMS:
    run_notebook(f"{BASE}/teams/{team}/event3_data_science_ml/_sim_runner", timeout=600)

# COMMAND ----------

print("=" * 60)
print("  STEP 6b: SCORE EVENT 3")
print("=" * 60)
run_notebook(f"{BASE}/instructor/scoring/event3_scoring", timeout=300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Scoreboard

# COMMAND ----------

print("=" * 60)
print("  STEP 7: GENERATE SCOREBOARD")
print("=" * 60)
run_notebook(f"{BASE}/instructor/scoring/scoreboard", timeout=300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

print("=" * 60)
print("  SIMULATION COMPLETE — VALIDATION")
print("=" * 60)

print("\n  Event Submissions:")
display(spark.sql("SELECT team_name, event_name, COUNT(*) AS submissions FROM dataops_olympics.default.event_submissions GROUP BY 1, 2 ORDER BY 1, 2"))

print("\n  Leaderboard Summary:")
display(spark.sql("""
    SELECT team, SUM(points) AS total_points, COUNT(DISTINCT event) AS events
    FROM dataops_olympics.default.olympics_leaderboard
    GROUP BY team ORDER BY total_points DESC
"""))

print("\n  Event 1 Scores:")
try:
    display(spark.table("dataops_olympics.default.event1_scores"))
except Exception:
    print("  (not found)")

print("\n  Event 2 Scores:")
try:
    display(spark.table("dataops_olympics.default.event2_scores"))
except Exception:
    print("  (not found)")

print("\n  Event 3 Scores:")
try:
    display(spark.table("dataops_olympics.default.event3_scores"))
except Exception:
    print("  (not found)")

print("\n" + "=" * 60)
print("  ALL DONE")
print("=" * 60)
