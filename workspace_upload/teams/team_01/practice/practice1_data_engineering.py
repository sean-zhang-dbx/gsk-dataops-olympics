# Databricks notebook source
# MAGIC %md
# MAGIC # Practice 1: Data Engineering
# MAGIC
# MAGIC **Time: ~10 minutes** | Use the Databricks Assistant to generate all your code from the business requirements.
# MAGIC
# MAGIC You just saw the lightning talk — now try it yourself!
# MAGIC Each exercise describes **what** needs to happen. Use `Cmd+I` to prompt the Assistant.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What You'll Do
# MAGIC 1. Read a CSV file from a Unity Catalog Volume
# MAGIC 2. Save it as a Delta table
# MAGIC 3. Add a governance comment
# MAGIC 4. Create a Silver (cleaned) table with data quality filters
# MAGIC 5. Run the validation check

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Run this cell first (no changes needed)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dataops_olympics;
# MAGIC USE SCHEMA default;

# COMMAND ----------

TEAM_NAME = "practice"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Read a CSV File from a Volume
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Read the heart disease CSV file located at
# MAGIC > `/Volumes/dataops_olympics/default/raw_data/heart_disease/heart.csv`
# MAGIC > into a Spark DataFrame. The file has a header row and you should let Spark
# MAGIC > infer the column types automatically.
# MAGIC >
# MAGIC > Print the row count and display the first 5 rows.

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Save as a Delta Table
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Save the DataFrame from Exercise 1 as a Delta table named
# MAGIC > `practice_heart_disease` in `dataops_olympics.default`.
# MAGIC > If the table already exists, overwrite it.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Add Governance Metadata
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Add a **table comment** to `practice_heart_disease` describing what the data is.
# MAGIC > Also add **column comments** to at least the `age` and `target` columns
# MAGIC > explaining what they mean (target 1 = heart disease, 0 = healthy).

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Create a Silver Table (Cleaned Data)
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create a cleaned Silver table called `practice_heart_silver` from the Bronze table.
# MAGIC > Keep only rows where:
# MAGIC > - `age` is between 1 and 120
# MAGIC > - `trestbps` (resting blood pressure) is between 50 and 300
# MAGIC >
# MAGIC > In an SDP pipeline, this would be handled by expectations that automatically
# MAGIC > drop invalid rows. Here, use a SQL WHERE clause.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — Run this to check your work!

# COMMAND ----------

print("=" * 55)
print("  PRACTICE 1 — VALIDATION")
print("=" * 55)

score = 0

try:
    cnt = spark.table(f"dataops_olympics.default.{TEAM_NAME}_heart_disease").count()
    if cnt > 400:
        print(f"  [PASS] Bronze table: {cnt} rows")
        score += 1
    else:
        print(f"  [FAIL] Bronze table has only {cnt} rows")
except Exception as e:
    print(f"  [FAIL] Bronze table not found: {e}")

try:
    detail = spark.sql(f"DESCRIBE DETAIL dataops_olympics.default.{TEAM_NAME}_heart_disease").collect()[0]
    fmt = detail["format"]
    if fmt == "delta":
        print(f"  [PASS] Format is Delta")
        score += 1
    else:
        print(f"  [FAIL] Format is {fmt}, expected delta")
except Exception as e:
    print(f"  [FAIL] Could not check format: {e}")

try:
    props = spark.sql(f"DESCRIBE TABLE EXTENDED dataops_olympics.default.{TEAM_NAME}_heart_disease").collect()
    has_comment = any("comment" in str(row).lower() and row[1] and len(str(row[1])) > 5
                      for row in props)
    if has_comment:
        print(f"  [PASS] Governance comment found")
        score += 1
    else:
        print(f"  [FAIL] No governance comment")
except Exception as e:
    print(f"  [FAIL] Could not check governance: {e}")

try:
    cnt = spark.table(f"dataops_olympics.default.{TEAM_NAME}_heart_silver").count()
    if cnt > 0:
        print(f"  [PASS] Silver table: {cnt} rows")
        score += 1
    else:
        print(f"  [FAIL] Silver table is empty")
except Exception as e:
    print(f"  [FAIL] Silver table not found: {e}")

print(f"\n  Score: {score}/4")
if score == 4:
    print("\n  ALL PASSED! You're ready for the competition!")
elif score >= 2:
    print("\n  Good progress! Fix the remaining items and re-run.")
else:
    print("\n  Ask the Databricks Assistant for help — paste the error message!")
print("=" * 55)
