# Databricks notebook source
# MAGIC %md
# MAGIC # Event 5: Self-Check
# MAGIC
# MAGIC Run this notebook to verify your Capstone artifacts before submission.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_submit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisite Tables (from Events 1-4)

# COMMAND ----------

print("=" * 60)
print(f"  EVENT 5 SELF-CHECK — {TEAM_NAME}")
print("=" * 60)
print()

prereqs = {
    "heart_silver_correct (Event 1)":  f"{CATALOG}.default.heart_silver_correct",
    "heart_gold_correct (Event 1)":    f"{CATALOG}.default.heart_gold_correct",
    "event3_results (Event 3)": f"{CATALOG}.default.event3_results",
}

for label, fqn in prereqs.items():
    try:
        cnt = spark.table(fqn).count()
        print(f"  [PASS] {label:35s} — {cnt} rows")
    except Exception as e:
        print(f"  [FAIL] {label:35s} — MISSING")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Capstone Artifacts

# COMMAND ----------

# --- Executive Briefing ---
try:
    briefing = spark.table(f"{CATALOG}.default.executive_briefing")
    cnt = briefing.count()
    if cnt > 0:
        row = briefing.first()
        text_len = len(str(row["briefing_text"])) if row["briefing_text"] else 0
        print(f"  [PASS] executive_briefing exists — {cnt} row(s), briefing length: {text_len} chars")
    else:
        print(f"  [FAIL] executive_briefing exists but is empty")
except Exception as e:
    print(f"  [FAIL] executive_briefing not found — complete Step 2 in the capstone notebook: {e}")

# COMMAND ----------

# --- AI Insights ---
try:
    insights = spark.table(f"{CATALOG}.default.heart_ai_insights")
    cnt = insights.count()
    cols = set(insights.columns)
    if "clinical_insight" in cols:
        print(f"  [PASS] heart_ai_insights exists — {cnt} rows with clinical_insight column")
    else:
        print(f"  [WARN] heart_ai_insights exists ({cnt} rows) but missing clinical_insight column")
except Exception as e:
    print(f"  [FAIL] heart_ai_insights not found: {e}")

# COMMAND ----------

# --- Drug AI Summary ---
try:
    drugs = spark.table(f"{CATALOG}.default.drug_ai_summary")
    cnt = drugs.count()
    cols = set(drugs.columns)
    if "ai_summary" in cols:
        print(f"  [PASS] drug_ai_summary exists — {cnt} rows with ai_summary column")
    else:
        print(f"  [WARN] drug_ai_summary exists ({cnt} rows) but missing ai_summary column")
except Exception as e:
    print(f"  [FAIL] drug_ai_summary not found: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Check Reminders
# MAGIC
# MAGIC The following are verified manually by judges during your presentation:
# MAGIC
# MAGIC - **Dashboard or App**: Is it functional, with 5+ charts, and tells a coherent data story?
# MAGIC - **Genie Space**: Can it answer a live follow-up question from the board?
# MAGIC - **Presentation**: Clear, within 3 minutes, covers all key findings?
# MAGIC
# MAGIC ### Bonus items the judges look for:
# MAGIC - Published dashboard or deployed app
# MAGIC - Interactive filters or parameters
# MAGIC - Scheduled email subscription or app authentication

# COMMAND ----------

# MAGIC %md
# MAGIC ## Submission Status

# COMMAND ----------

check_submission("event5")
