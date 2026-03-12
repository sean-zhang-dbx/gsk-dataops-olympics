# Databricks notebook source
# MAGIC %md
# MAGIC # Event 4: Self-Check
# MAGIC
# MAGIC Run this notebook to verify your Event 4 artifacts before submission.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_submit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking UC Function Tools

# COMMAND ----------

print("=" * 60)
print(f"  EVENT 4 SELF-CHECK — {TEAM_NAME}")
print("=" * 60)
print()

# --- UC Functions ---
for func_name in ["patient_risk_assessment", "get_cohort_summary"]:
    try:
        funcs = spark.sql(f"SHOW FUNCTIONS IN {CATALOG}.default LIKE '{func_name}'").collect()
        if funcs:
            print(f"  [PASS] UC function exists: {CATALOG}.default.{func_name}")
        else:
            print(f"  [FAIL] UC function not found: {CATALOG}.default.{func_name}")
    except Exception as e:
        print(f"  [FAIL] Could not check function {func_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Config Table

# COMMAND ----------

try:
    config = spark.table(f"{CATALOG}.default.agent_config")
    config_rows = config.collect()
    if config_rows:
        print(f"  [PASS] agent_config exists — {len(config_rows)} resource(s) registered:")
        for row in config_rows:
            print(f"         {row['resource_type']:25s} — ID: {row['resource_id']}")
    else:
        print(f"  [WARN] agent_config exists but is empty — register your Genie, KA, and Supervisor IDs")

    # Check specific resources
    resource_types = [row["resource_type"] for row in config_rows]
    for expected in ["genie_space", "knowledge_assistant", "supervisor_agent"]:
        if expected in resource_types:
            print(f"  [PASS] {expected} registered in agent_config")
        else:
            print(f"  [FAIL] {expected} not found in agent_config — paste the ID in the starter notebook")

except Exception as e:
    print(f"  [FAIL] agent_config table not found — run the starter notebook first: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Supervisor Test Results

# COMMAND ----------

try:
    test_results = spark.table(f"{CATALOG}.default.supervisor_test_results")
    test_count = test_results.count()
    if test_count > 0:
        success_count = test_results.filter("status = 'success'").count()
        print(f"  [PASS] supervisor_test_results exists — {test_count} prompts tested, {success_count} successful")
    else:
        print(f"  [WARN] supervisor_test_results exists but is empty — run Step 6 in the starter notebook")
except Exception as e:
    print(f"  [FAIL] supervisor_test_results not found — complete Step 6 in the starter notebook: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Checks

# COMMAND ----------

# DSPy optimized prompt
try:
    dspy_table = spark.table(f"{CATALOG}.default.dspy_optimized_prompt")
    cnt = dspy_table.count()
    print(f"  [PASS] Bonus: DSPy optimized prompt saved ({cnt} rows)")
except Exception:
    print(f"  [    ] Bonus: dspy_optimized_prompt not found (optional)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Check Reminders
# MAGIC
# MAGIC The following items are verified manually by the organizer during scoring:
# MAGIC
# MAGIC - **Genie Space**: Verify it is accessible and configured with the right tables and instructions
# MAGIC - **Knowledge Assistant**: Verify it is ONLINE and can answer questions about clinical documents
# MAGIC - **Supervisor Agent**: Verify it is ONLINE and correctly routes to sub-agents
# MAGIC - **MCP Server** (bonus): Verify the Databricks App is deployed and the UC HTTP connection exists

# COMMAND ----------

# MAGIC %md
# MAGIC ## Submission Status

# COMMAND ----------

check_submission("event4")
