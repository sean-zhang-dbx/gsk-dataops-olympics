# Databricks notebook source
# MAGIC %md
# MAGIC # Practice 2: Data Analytics
# MAGIC
# MAGIC **Time: ~10 minutes** | Fill in the blanks, run each cell, check your work at the end.
# MAGIC
# MAGIC You just saw the lightning talk — now try it yourself!
# MAGIC Fill in the `_____` blanks below. Use the **Databricks Assistant** (`Cmd+I`) if you get stuck.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What You'll Do
# MAGIC 1. Write a SQL query to find heart disease prevalence
# MAGIC 2. Write a SQL query to analyze by age group
# MAGIC 3. Create a Plotly bar chart
# MAGIC 4. Run the validation check

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Run this cell first

# COMMAND ----------

spark.sql("USE CATALOG dataops_olympics")
spark.sql("USE SCHEMA default")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Heart Disease Prevalence
# MAGIC
# MAGIC Write a query that counts patients by diagnosis status.
# MAGIC
# MAGIC **Hint:** `target = 1` means heart disease, `target = 0` means healthy.
# MAGIC Fill in the GROUP BY column.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FILL IN: Replace _____ with the column to group by
# MAGIC SELECT
# MAGIC     CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END as diagnosis,
# MAGIC     COUNT(*) as patients
# MAGIC FROM heart_disease
# MAGIC GROUP BY _____

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Disease Rate by Age Group
# MAGIC
# MAGIC Write a query that shows the disease rate for each age group.
# MAGIC
# MAGIC **Hint:** Use `AVG(CASE WHEN target = 1 THEN 1.0 ELSE 0.0 END)` to calculate the rate.
# MAGIC Fill in the aggregation expression.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FILL IN: Replace _____ with the disease rate calculation
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN age < 40 THEN '1. Under 40'
# MAGIC         WHEN age < 50 THEN '2. 40-49'
# MAGIC         WHEN age < 60 THEN '3. 50-59'
# MAGIC         ELSE '4. 60+'
# MAGIC     END as age_group,
# MAGIC     COUNT(*) as total_patients,
# MAGIC     ROUND(_____ * 100, 1) as disease_rate_pct
# MAGIC FROM heart_disease
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Create a Plotly Bar Chart
# MAGIC
# MAGIC Create a grouped bar chart showing patient count by age group and disease status.
# MAGIC
# MAGIC **Hint:** Use `px.bar(df, x="age_group", y="patients", color="status", barmode="group")`

# COMMAND ----------

import plotly.express as px

df_chart = spark.sql("""
    SELECT
        CASE
            WHEN age < 40 THEN '1. Under 40'
            WHEN age < 50 THEN '2. 40-49'
            WHEN age < 60 THEN '3. 50-59'
            ELSE '4. 60+'
        END as age_group,
        CASE WHEN target = 1 THEN 'Disease' ELSE 'Healthy' END as status,
        COUNT(*) as patients
    FROM heart_disease
    GROUP BY 1, 2
    ORDER BY 1, 2
""").toPandas()

# FILL IN: Replace _____ with the correct Plotly function call
# Use px.bar with x="age_group", y="patients", color="status", barmode="group"
fig = _____

fig.update_layout(template="plotly_white",
                  title="Heart Disease by Age Group")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — Run this to check your work!

# COMMAND ----------

print("=" * 55)
print("  PRACTICE 2 — VALIDATION")
print("=" * 55)

score = 0

# Check 1: Can query heart_disease
try:
    cnt = spark.sql("SELECT COUNT(*) as cnt FROM heart_disease").collect()[0].cnt
    if cnt > 400:
        print(f"  [PASS] heart_disease table accessible ({cnt} rows)")
        score += 1
    else:
        print(f"  [FAIL] heart_disease has only {cnt} rows")
except Exception as e:
    print(f"  [FAIL] Cannot query heart_disease: {e}")

# Check 2: Can compute prevalence
try:
    result = spark.sql("""
        SELECT target, COUNT(*) as cnt
        FROM heart_disease GROUP BY target ORDER BY target
    """).collect()
    if len(result) == 2:
        print(f"  [PASS] Prevalence query works: {result[0].cnt} healthy, {result[1].cnt} diseased")
        score += 1
except Exception:
    print("  [FAIL] Prevalence query failed")

# Check 3: Can compute by age group
try:
    result = spark.sql("""
        SELECT
            CASE WHEN age < 40 THEN 'Under 40' WHEN age < 50 THEN '40-49'
                 WHEN age < 60 THEN '50-59' ELSE '60+' END as ag,
            ROUND(AVG(CASE WHEN target=1 THEN 1.0 ELSE 0.0 END)*100,1) as rate
        FROM heart_disease GROUP BY 1
    """).collect()
    if len(result) == 4:
        print(f"  [PASS] Age group analysis works ({len(result)} groups)")
        score += 1
except Exception:
    print("  [FAIL] Age group query failed")

# Check 4: Plotly is available
try:
    import plotly.express as px
    print(f"  [PASS] Plotly imported successfully")
    score += 1
except Exception:
    print("  [FAIL] Plotly not available")

print(f"\n  Score: {score}/4")
if score == 4:
    print("\n  ALL PASSED! You're ready for the competition!")
elif score >= 2:
    print("\n  Good progress! Fix the remaining items and re-run.")
else:
    print("\n  Ask the Databricks Assistant for help!")
print("=" * 55)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answers (for reference — try without peeking!)
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answers</summary>
# MAGIC
# MAGIC - **Exercise 1:** `target` (the column to GROUP BY)
# MAGIC - **Exercise 2:** `AVG(CASE WHEN target = 1 THEN 1.0 ELSE 0.0 END)`
# MAGIC - **Exercise 3:** `px.bar(df_chart, x="age_group", y="patients", color="status", barmode="group")`
# MAGIC
# MAGIC </details>
