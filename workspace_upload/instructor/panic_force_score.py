# Databricks notebook source
# MAGIC %md
# MAGIC # PANIC BUTTON: Force Score — Manually Assign Points
# MAGIC
# MAGIC **Use when:** Automated scoring fails, a team deserves partial credit,
# MAGIC or you need to adjust a score after manual review.
# MAGIC
# MAGIC This inserts/overwrites a score entry directly into the leaderboard.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set the team, event, category, and points below.
# MAGIC
# MAGIC **Valid events:**
# MAGIC - `Event 1: Data Engineering`
# MAGIC - `Event 2: Data Analytics`
# MAGIC - `Event 3: Data Science`
# MAGIC - `Event 4: GenAI Agents`
# MAGIC - `Event 5: Capstone`
# MAGIC
# MAGIC **Valid categories per event:**
# MAGIC - Event 1: Bronze, Silver, Gold, DQ, Governance, Bonus
# MAGIC - Event 2: Build_SQL, Build_Genie, Benchmark, Speed, Bonus
# MAGIC - Event 3: EDA, Features, MLflow, Performance, Registration, Bonus
# MAGIC - Event 4: UC_Functions, Genie, Knowledge_Assistant, Supervisor, Test_Prompts, Bonus
# MAGIC - Event 5: Artifacts, Briefing, Dashboard, Genie, Presentation, Bonus

# COMMAND ----------

dbutils.widgets.text("TEAM", "team_XX", "Team Name")
dbutils.widgets.text("EVENT", "Event 1: Data Engineering", "Event Name")
dbutils.widgets.text("CATEGORY", "Bronze", "Category")
dbutils.widgets.text("POINTS", "0", "Points to Award")
dbutils.widgets.text("MAX_POINTS", "10", "Max Possible Points")
dbutils.widgets.text("REASON", "", "Reason for Override")

TEAM = dbutils.widgets.get("TEAM")
EVENT = dbutils.widgets.get("EVENT")
CATEGORY = dbutils.widgets.get("CATEGORY")
POINTS = float(dbutils.widgets.get("POINTS"))
MAX_POINTS = float(dbutils.widgets.get("MAX_POINTS"))
REASON = dbutils.widgets.get("REASON")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Score Override

# COMMAND ----------

LB = "dataops_olympics.default.olympics_leaderboard"
RT = "dataops_olympics.default.registered_teams"

# Ensure team is registered
if spark.sql(f"SELECT 1 FROM {RT} WHERE team = '{TEAM}'").count() == 0:
    spark.sql(f"INSERT INTO {RT} VALUES ('{TEAM}')")
    print(f"  Registered {TEAM}")

# Delete existing score for this team/event/category
spark.sql(f"""
    DELETE FROM {LB}
    WHERE team = '{TEAM}' AND event = '{EVENT}' AND category = '{CATEGORY}'
""")

# Insert new score
from datetime import datetime
now = datetime.now()
spark.sql(f"""
    INSERT INTO {LB} VALUES (
        '{TEAM}', '{EVENT}', '{CATEGORY}', {POINTS}, {MAX_POINTS}, '{now}'
    )
""")

print("=" * 60)
print(f"  FORCE SCORE APPLIED")
print(f"  Team:     {TEAM}")
print(f"  Event:    {EVENT}")
print(f"  Category: {CATEGORY}")
print(f"  Points:   {POINTS}/{MAX_POINTS}")
print(f"  Reason:   {REASON or '(none)'}")
print(f"  Time:     {now}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — Current Scores for This Team

# COMMAND ----------

display(spark.sql(f"""
    SELECT event, category, points, max_points, scored_at
    FROM {LB}
    WHERE team = '{TEAM}'
    ORDER BY event, category
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Override (Optional)
# MAGIC
# MAGIC Paste multiple overrides below to apply them all at once.
# MAGIC Format: `team, event, category, points, max_points`

# COMMAND ----------

BATCH_OVERRIDES = """
# Uncomment and edit lines below:
# team_01, Event 1: Data Engineering, Bronze, 10, 10
# team_01, Event 1: Data Engineering, Silver, 8, 15
# team_02, Event 2: Data Analytics, Benchmark, 5, 8
"""

from datetime import datetime

lines = [l.strip() for l in BATCH_OVERRIDES.strip().split("\n") if l.strip() and not l.strip().startswith("#")]

if lines:
    print("=" * 60)
    print("  BATCH OVERRIDE")
    print("=" * 60)
    for line in lines:
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 5:
            print(f"  [SKIP] Bad format: {line}")
            continue
        t, e, c, p, m = parts
        try:
            spark.sql(f"DELETE FROM {LB} WHERE team = '{t}' AND event = '{e}' AND category = '{c}'")
            spark.sql(f"INSERT INTO {LB} VALUES ('{t}', '{e}', '{c}', {float(p)}, {float(m)}, '{datetime.now()}')")
            if spark.sql(f"SELECT 1 FROM {RT} WHERE team = '{t}'").count() == 0:
                spark.sql(f"INSERT INTO {RT} VALUES ('{t}')")
            print(f"  [OK] {t} | {e} | {c} = {p}/{m}")
        except Exception as ex:
            print(f"  [FAIL] {line} -> {str(ex)[:60]}")
    print("=" * 60)
else:
    print("  No batch overrides configured.")
