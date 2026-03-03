# Databricks notebook source
# MAGIC %md
# MAGIC # Practice 1: Data Engineering
# MAGIC
# MAGIC **Time: ~10 minutes** | Fill in the blanks, run each cell, check your work at the end.
# MAGIC
# MAGIC You just saw the lightning talk — now try it yourself!
# MAGIC Fill in the `_____` blanks below. Use the **Databricks Assistant** (`Cmd+I`) if you get stuck.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What You'll Do
# MAGIC 1. Read a CSV file into a Spark DataFrame
# MAGIC 2. Save it as a Delta table
# MAGIC 3. Add a governance comment
# MAGIC 4. Create a Silver (cleaned) table
# MAGIC 5. Run the validation check

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Run this cell first (no changes needed)

# COMMAND ----------

spark.sql("USE dataops_olympics")
TEAM_NAME = "practice"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Read a CSV File
# MAGIC
# MAGIC Fill in the blank to read the heart disease CSV.
# MAGIC
# MAGIC **Hint:** The format is `"csv"` and you need `header` and `inferSchema` options.

# COMMAND ----------

csv_path = "file:/tmp/dataops_olympics/raw/heart_disease/heart.csv"

# FILL IN: Replace _____ with the correct format string
df_heart = (spark.read
    .format("_____")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(csv_path))

print(f"Loaded {df_heart.count()} rows")
display(df_heart.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Save as a Delta Table
# MAGIC
# MAGIC Fill in the blank to write the DataFrame as a Delta table.
# MAGIC
# MAGIC **Hint:** The format is `"delta"` and the mode is `"overwrite"`.

# COMMAND ----------

# FILL IN: Replace _____ with the correct format
df_heart.write.format("_____").mode("overwrite").saveAsTable(f"{TEAM_NAME}_heart_disease")

print(f"Created table: {TEAM_NAME}_heart_disease")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Add a Governance Comment
# MAGIC
# MAGIC Fill in the blank with a description of what this table contains.
# MAGIC
# MAGIC **Hint:** Describe what the data is (e.g., "Patient records for heart disease analysis")

# COMMAND ----------

# FILL IN: Replace _____ with a meaningful table description
spark.sql(f"""
    ALTER TABLE {TEAM_NAME}_heart_disease
    SET TBLPROPERTIES ('comment' = '_____')
""")

spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN age COMMENT 'Patient age in years'")
spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN target COMMENT 'Diagnosis: 1=disease, 0=healthy'")

print("Governance comments added!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Create a Silver Table (Cleaned Data)
# MAGIC
# MAGIC Fill in the WHERE clause to filter out bad records.
# MAGIC
# MAGIC **Hint:** Keep only rows where:
# MAGIC - `age` is between 1 and 120
# MAGIC - `trestbps` (blood pressure) is between 50 and 300

# COMMAND ----------

# FILL IN: Replace _____ with the correct filter conditions
spark.sql(f"""
    CREATE OR REPLACE TABLE {TEAM_NAME}_heart_silver AS
    SELECT *
    FROM {TEAM_NAME}_heart_disease
    WHERE age BETWEEN 1 AND 120
      AND _____
""")

silver_count = spark.table(f"{TEAM_NAME}_heart_silver").count()
print(f"Silver table created: {silver_count} rows (cleaned)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — Run this to check your work!

# COMMAND ----------

print("=" * 55)
print("  PRACTICE 1 — VALIDATION")
print("=" * 55)

score = 0

# Check 1: Bronze table exists
try:
    cnt = spark.table(f"{TEAM_NAME}_heart_disease").count()
    if cnt > 400:
        print(f"  [PASS] Bronze table: {cnt} rows")
        score += 1
    else:
        print(f"  [FAIL] Bronze table has only {cnt} rows")
except:
    print("  [FAIL] Bronze table not found")

# Check 2: Format is Delta
try:
    detail = spark.sql(f"DESCRIBE DETAIL {TEAM_NAME}_heart_disease").collect()[0]
    fmt = detail["format"]
    if fmt == "delta":
        print(f"  [PASS] Format is Delta")
        score += 1
    else:
        print(f"  [FAIL] Format is {fmt}, expected delta")
except:
    print("  [FAIL] Could not check format")

# Check 3: Governance comment exists
try:
    props = spark.sql(f"DESCRIBE TABLE EXTENDED {TEAM_NAME}_heart_disease").collect()
    has_comment = any("comment" in str(row).lower() and row[1] and len(str(row[1])) > 5
                      for row in props)
    if has_comment:
        print(f"  [PASS] Governance comment found")
        score += 1
    else:
        print(f"  [FAIL] No governance comment")
except:
    print("  [FAIL] Could not check governance")

# Check 4: Silver table
try:
    cnt = spark.table(f"{TEAM_NAME}_heart_silver").count()
    if cnt > 0:
        print(f"  [PASS] Silver table: {cnt} rows")
        score += 1
    else:
        print(f"  [FAIL] Silver table is empty")
except:
    print("  [FAIL] Silver table not found")

print(f"\n  Score: {score}/4")
if score == 4:
    print("\n  ALL PASSED! You're ready for the competition!")
elif score >= 2:
    print("\n  Good progress! Fix the remaining items and re-run.")
else:
    print("\n  Ask the Databricks Assistant for help — paste the error message!")
print("=" * 55)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answers (for reference — try without peeking!)
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answers</summary>
# MAGIC
# MAGIC - **Exercise 1:** `"csv"`
# MAGIC - **Exercise 2:** `"delta"`
# MAGIC - **Exercise 3:** Any meaningful description, e.g., `"UCI Heart Disease dataset — 500 patients with clinical features for heart disease prediction"`
# MAGIC - **Exercise 4:** `trestbps BETWEEN 50 AND 300`
# MAGIC
# MAGIC </details>
