# Databricks notebook source
# MAGIC %md
# MAGIC # Practice 2: Data Analytics
# MAGIC
# MAGIC **Time: ~10 minutes** | Use the Databricks Assistant to generate all your code from the business requirements.
# MAGIC
# MAGIC You just saw the lightning talk — now try it yourself!
# MAGIC Each exercise describes **what** needs to happen. Use `Cmd+I` to prompt the Assistant.
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
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Write a SQL query on the `heart_disease` table that counts the number
# MAGIC > of patients grouped by diagnosis status.
# MAGIC > The `target` column indicates: 1 = heart disease, 0 = healthy.
# MAGIC > Show the diagnosis label ("Heart Disease" or "Healthy") and the patient count.

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Disease Rate by Age Group
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Write a SQL query that shows the **heart disease rate (as a percentage)**
# MAGIC > for each age group: "Under 40", "40-49", "50-59", "60+".
# MAGIC >
# MAGIC > For each group, show the total number of patients and the disease rate
# MAGIC > rounded to 1 decimal place. Order by age group.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Create a Plotly Bar Chart
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create a **grouped bar chart** using Plotly Express that shows the number
# MAGIC > of patients in each age group, split by heart disease status (Disease vs Healthy).
# MAGIC > Use `barmode="group"` and the `plotly_white` template.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — Run this to check your work!

# COMMAND ----------

print("=" * 55)
print("  PRACTICE 2 — VALIDATION")
print("=" * 55)

score = 0

try:
    cnt = spark.sql("SELECT COUNT(*) as cnt FROM heart_disease").collect()[0].cnt
    if cnt > 400:
        print(f"  [PASS] heart_disease table accessible ({cnt} rows)")
        score += 1
    else:
        print(f"  [FAIL] heart_disease has only {cnt} rows")
except Exception as e:
    print(f"  [FAIL] Cannot query heart_disease: {e}")

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
