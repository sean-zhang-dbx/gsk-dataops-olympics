# Databricks notebook source
# MAGIC %md
# MAGIC # Event 2: Self-Check
# MAGIC
# MAGIC Run this notebook to verify your Event 2 artifacts before submission.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_submit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisite Tables (from Event 1)

# COMMAND ----------

print("=" * 60)
print(f"  EVENT 2 SELF-CHECK — {TEAM_NAME}")
print("=" * 60)
print()

for table_name in ["heart_silver_correct", "heart_gold_correct"]:
    try:
        cnt = spark.table(f"{CATALOG}.default.{table_name}").count()
        print(f"  [PASS] {table_name} exists — {cnt} rows")
    except Exception as e:
        print(f"  [FAIL] {table_name} missing — run event1 fallback first: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event 2 Submissions Table

# COMMAND ----------

try:
    subs = spark.table(f"{CATALOG}.default.event2_submissions")
    sub_count = subs.filter(f"team = '{TEAM_NAME}'").count()
    if sub_count > 0:
        print(f"  [PASS] event2_submissions exists — {sub_count} answers submitted")
    else:
        print(f"  [WARN] event2_submissions exists but no answers found for {TEAM_NAME}")
except Exception as e:
    print(f"  [FAIL] event2_submissions table not found — run the starter notebook first: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Submitted Answers

# COMMAND ----------

try:
    latest = spark.sql(f"""
        SELECT question_id, answer, method, submitted_at,
            ROW_NUMBER() OVER (PARTITION BY question_id ORDER BY submitted_at DESC) AS rn
        FROM {CATALOG}.default.event2_submissions
        WHERE team = '{TEAM_NAME}'
    """).filter("rn = 1").drop("rn").orderBy("question_id")

    answered = latest.count()
    print(f"  Questions answered: {answered} / 8")
    print()
    if answered > 0:
        for row in latest.collect():
            print(f"  {row['question_id']} [{row['method']:5s}]: {row['answer']}")
    else:
        print("  No answers submitted yet. Use submit_answer() in the starter notebook.")
except Exception as e:
    print(f"  Could not retrieve answers: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Table Checks

# COMMAND ----------

for table_name, desc in [
    ("heart_executive_summary", "Bonus 1: AI-Powered Summary"),
    ("heart_cohort_comparison", "Bonus 2: Cohort Analysis"),
    ("heart_chi2_test", "Bonus 3: Chi-Squared Test"),
]:
    try:
        cnt = spark.table(f"{CATALOG}.default.{table_name}").count()
        print(f"  [PASS] {desc} — {table_name} exists ({cnt} rows)")
    except Exception:
        print(f"  [    ] {desc} — {table_name} not found (optional)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Check Reminder
# MAGIC
# MAGIC The scoring notebook also checks your **Genie space** setup.
# MAGIC Make sure you have:
# MAGIC - Created a Genie space with `heart_silver_correct` and `heart_gold_correct`
# MAGIC - Added general instructions and sample questions
# MAGIC - Tested it with a simple question
# MAGIC
# MAGIC The organizer will verify Genie spaces manually during scoring.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Submission Status

# COMMAND ----------

check_submission("event2")
