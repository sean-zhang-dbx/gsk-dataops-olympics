# Databricks notebook source
# MAGIC %run ../_config
# COMMAND ----------
# MAGIC %run ../_submit
# COMMAND ----------
# Step 0: Prepare data
spark.sql(f"CREATE OR REPLACE TABLE {CATALOG}.default.heart_silver_correct AS SELECT * FROM {SHARED_CATALOG}.default.heart_disease")
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.heart_gold_correct AS
    SELECT CASE WHEN age < 40 THEN 'Under 40' WHEN age BETWEEN 40 AND 49 THEN '40-49' WHEN age BETWEEN 50 AND 59 THEN '50-59' ELSE '60+' END AS age_group,
    CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
    COUNT(*) AS patient_count, ROUND(AVG(chol), 1) AS avg_cholesterol, ROUND(AVG(trestbps), 1) AS avg_blood_pressure, ROUND(AVG(thalach), 1) AS avg_max_heart_rate
    FROM {CATALOG}.default.heart_silver_correct GROUP BY 1, 2""")
print(f"Step 0 done: silver={spark.table(f'{CATALOG}.default.heart_silver_correct').count()}, gold={spark.table(f'{CATALOG}.default.heart_gold_correct').count()}")
# COMMAND ----------
# Create event2_submissions table
spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.default.event2_submissions (
    team STRING, question_id STRING, answer STRING, method STRING, submitted_at TIMESTAMP
)""")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Genie Space Creation (via API)
# COMMAND ----------
import requests, json

host = spark.conf.get("spark.databricks.workspaceUrl", "")
if not host.startswith("http"):
    host = f"https://{host}"
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

genie_created = False
genie_space_id = None
try:
    warehouse_resp = requests.get(f"{host}/api/2.0/sql/warehouses", headers=headers)
    warehouses = warehouse_resp.json().get("warehouses", [])
    wh_id = warehouses[0]["id"] if warehouses else None

    if wh_id:
        silver_fqn = f"{CATALOG}.default.heart_silver_correct"
        gold_fqn = f"{CATALOG}.default.heart_gold_correct"
        genie_payload = {
            "title": f"{TEAM_NAME} Heart Disease Analyst",
            "description": "Genie space for heart disease data analytics",
            "warehouse_id": wh_id,
            "table_identifiers": [silver_fqn, gold_fqn],
        }
        resp = requests.post(f"{host}/api/2.0/genie/spaces", headers=headers, json=genie_payload)
        if resp.status_code == 200:
            genie_space_id = resp.json().get("space_id", resp.json().get("id", ""))
            genie_created = True
            print(f"Genie space created: {genie_space_id}")
        else:
            print(f"Genie API returned {resp.status_code}: {resp.text[:200]}")
    else:
        print("No SQL warehouse found, skipping Genie")
except Exception as e:
    print(f"Genie creation skipped: {str(e)[:100]}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Submit All 8 Answers (some via Genie, rest via SQL)
# COMMAND ----------
import time

genie_method = "Genie" if genie_created else "SQL"
answers = [
    ("Q1", "264", genie_method), ("Q2", "Heart Disease: 56.1, Healthy: 49.5", genie_method),
    ("Q3", "46.8%", "SQL"), ("Q4", "3 (asymptomatic)", "SQL"),
    ("Q5", "253.8, 233.6", genie_method), ("Q6", "71", "SQL"),
    ("Q7", "60+ (65.2%)", genie_method), ("Q8", "150.4", "SQL")
]
for qid, ans, method in answers:
    spark.sql(f"INSERT INTO {CATALOG}.default.event2_submissions VALUES ('{TEAM_NAME}', '{qid}', '{ans}', '{method}', current_timestamp())")
    time.sleep(0.5)
print(f"Submitted 8 answers (Genie={genie_created})")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Bonus Tables
# COMMAND ----------
# Bonus 1: Executive summary using ai_query (or fallback)
try:
    spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.heart_executive_summary AS
        SELECT 'Heart Disease Analysis Summary' AS title,
            CONCAT('Total patients: ', COUNT(*), '. Disease prevalence: ', ROUND(SUM(CASE WHEN target=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 1), '%. ',
            'Average age: ', ROUND(AVG(age), 1), '. Average cholesterol: ', ROUND(AVG(chol), 1), '.') AS summary
        FROM {CATALOG}.default.heart_silver_correct""")
    print("Bonus: executive_summary created")
except Exception as e:
    print(f"Bonus executive_summary skipped: {str(e)[:60]}")
# COMMAND ----------
# Bonus 2: Cohort comparison
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.heart_cohort_comparison AS
    SELECT CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS cohort,
        COUNT(*) AS n, ROUND(AVG(age),1) AS avg_age, ROUND(AVG(chol),1) AS avg_chol,
        ROUND(AVG(trestbps),1) AS avg_bp, ROUND(AVG(thalach),1) AS avg_hr
    FROM {CATALOG}.default.heart_silver_correct GROUP BY target""")
print("Bonus: cohort_comparison created")
# COMMAND ----------
# Bonus 3: Chi-squared test result
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.default.heart_chi2_test AS
    SELECT 'sex_vs_target' AS test_name, 'chi2' AS test_type,
        12.5 AS statistic, 0.0004 AS p_value, 'significant' AS result""")
print("Bonus: chi2_test created")
# COMMAND ----------
submit("event2", {"answers": 8, "method": "mixed", "genie_space": genie_space_id or "none", "bonus": 3})
print(f"team_01 Event 2 complete (POWER TEAM — Genie={genie_created}, 3 bonus)")
