# Databricks notebook source
# MAGIC %md
# MAGIC # Event 1: Self-Check
# MAGIC
# MAGIC Run this notebook to verify your Event 1 artifacts before submission.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_submit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking Event 1 Artifacts

# COMMAND ----------

print("=" * 60)
print(f"  EVENT 1 SELF-CHECK — {TEAM_NAME}")
print("=" * 60)
print()

# --- Bronze Table ---
try:
    bronze = spark.table(f"{CATALOG}.default.heart_bronze")
    bronze_count = bronze.count()
    bronze_cols = bronze.columns
    print(f"  [PASS] heart_bronze exists — {bronze_count} rows, {len(bronze_cols)} columns")
    if bronze_count < 100:
        print(f"         Warning: row count seems low (expected ~500)")
except Exception as e:
    print(f"  [FAIL] heart_bronze — {e}")

# COMMAND ----------

# --- Silver Table ---
try:
    silver = spark.table(f"{CATALOG}.default.heart_silver")
    silver_count = silver.count()
    silver_cols = silver.columns
    print(f"  [PASS] heart_silver exists — {silver_count} rows, {len(silver_cols)} columns")
    if silver_count < 100:
        print(f"         Warning: row count seems low (expected ~485)")
except Exception as e:
    print(f"  [FAIL] heart_silver — {e}")

# COMMAND ----------

# --- Gold Table ---
try:
    gold = spark.table(f"{CATALOG}.default.heart_gold")
    gold_count = gold.count()
    gold_cols = set(gold.columns)
    print(f"  [PASS] heart_gold exists — {gold_count} rows")
    expected_cols = {"age_group", "diagnosis", "patient_count", "avg_cholesterol", "avg_blood_pressure", "avg_max_heart_rate"}
    missing = expected_cols - gold_cols
    if missing:
        print(f"         Warning: missing expected columns: {missing}")
    else:
        print(f"         All expected columns present")
except Exception as e:
    print(f"  [FAIL] heart_gold — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks (Silver)

# COMMAND ----------

try:
    from pyspark.sql.functions import col, count, sum as _sum, when

    silver = spark.table(f"{CATALOG}.default.heart_silver")

    # Check for null ages
    null_ages = silver.filter(col("age").isNull()).count()
    if null_ages == 0:
        print(f"  [PASS] No null ages in silver")
    else:
        print(f"  [FAIL] Found {null_ages} null ages in silver — these should be filtered out")

    # Check for invalid blood pressure
    bad_bp = silver.filter(~col("trestbps").between(50, 300)).count()
    if bad_bp == 0:
        print(f"  [PASS] No invalid blood pressure values in silver")
    else:
        print(f"  [FAIL] Found {bad_bp} invalid BP values in silver — these should be filtered out")

    # Check for negative cholesterol
    neg_chol = silver.filter(col("chol") < 0).count()
    if neg_chol == 0:
        print(f"  [PASS] No negative cholesterol values in silver")
    else:
        print(f"  [FAIL] Found {neg_chol} negative chol values in silver — these should be filtered out")

    # Check deduplication
    if "event_id" in silver.columns:
        total = silver.count()
        distinct = silver.select("event_id").distinct().count()
        if total == distinct:
            print(f"  [PASS] No duplicate event_ids in silver")
        else:
            print(f"  [FAIL] Found {total - distinct} duplicate event_ids in silver")
    else:
        print(f"  [WARN] event_id column not found — cannot check deduplication")

except Exception as e:
    print(f"  [SKIP] Could not run DQ checks: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance Checks

# COMMAND ----------

for table_name in ["heart_bronze", "heart_silver", "heart_gold"]:
    try:
        desc = spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.default.{table_name}").collect()
        # Look for a non-empty table comment
        has_comment = False
        for row in desc:
            if row[0] == "Comment" and row[1] and len(str(row[1]).strip()) > 0:
                has_comment = True
                break
        if has_comment:
            print(f"  [PASS] {table_name} has a table comment")
        else:
            print(f"  [FAIL] {table_name} has no table comment — add one with ALTER TABLE ... SET TBLPROPERTIES('comment' = '...')")
    except Exception as e:
        print(f"  [SKIP] {table_name} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Submission Status

# COMMAND ----------

check_submission("event1")
