# Databricks notebook source
# Event 2 Simulation: team_01 submits SQL answers (no Genie - that's UI-dependent)

TEAM_NAME = "team_01"
CATALOG = TEAM_NAME
SHARED_CATALOG = "dataops_olympics"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")

# COMMAND ----------

# Create submissions table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.default.event2_submissions (
        team STRING,
        question_id STRING,
        answer STRING,
        method STRING,
        submitted_at STRING
    )
""")

# COMMAND ----------

from datetime import datetime

def submit_answer(question_id, answer, method="SQL"):
    ts = datetime.now().isoformat()
    spark.sql(f"""
        INSERT INTO {CATALOG}.default.event2_submissions
        VALUES ('{TEAM_NAME}', '{question_id}', '{answer}', '{method}', '{ts}')
    """)
    print(f"  Submitted {question_id} [{method}]: {answer}")

# COMMAND ----------

# Q1: How many patients have heart disease?
q1 = spark.sql("SELECT COUNT(*) AS cnt FROM heart_silver WHERE target = 1").collect()[0]["cnt"]
submit_answer("Q1", str(q1))

# Q2: Average age — heart disease vs healthy?
q2 = spark.sql("""
    SELECT CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
           ROUND(AVG(age), 1) AS avg_age
    FROM heart_silver GROUP BY target ORDER BY target DESC
""").toPandas()
q2_disease = q2[q2["diagnosis"] == "Heart Disease"]["avg_age"].values[0]
q2_healthy = q2[q2["diagnosis"] == "Healthy"]["avg_age"].values[0]
submit_answer("Q2", f"Heart Disease: {q2_disease}, Healthy: {q2_healthy}")

# Q3: Percentage of female patients with heart disease?
q3 = spark.sql("""
    SELECT ROUND(SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct
    FROM heart_silver WHERE sex = 0
""").collect()[0]["pct"]
submit_answer("Q3", f"{q3}%")

# Q4: Most common chest pain type among heart disease patients?
q4 = spark.sql("""
    SELECT cp, COUNT(*) AS cnt FROM heart_silver WHERE target = 1
    GROUP BY cp ORDER BY cnt DESC LIMIT 1
""").collect()[0]
cp_names = {0: "typical angina", 1: "atypical angina", 2: "non-anginal pain", 3: "asymptomatic"}
submit_answer("Q4", f"{int(q4['cp'])} ({cp_names[int(q4['cp'])]})")

# Q5: Average cholesterol for 60+ age group?
q5 = spark.sql("""
    SELECT ROUND(avg_cholesterol, 1) AS answer FROM heart_gold WHERE age_group = '60+'
""").toPandas()
q5_str = ", ".join(str(v) for v in q5["answer"].tolist())
submit_answer("Q5", q5_str)

# Q6: Patients with cholesterol > 240 AND BP > 140?
q6 = spark.sql("SELECT COUNT(*) AS cnt FROM heart_silver WHERE chol > 240 AND trestbps > 140").collect()[0]["cnt"]
submit_answer("Q6", str(q6))

# Q7: Age group with highest heart disease rate?
q7 = spark.sql("""
    SELECT CASE WHEN age < 40 THEN 'Under 40' WHEN age < 50 THEN '40-49'
                WHEN age < 60 THEN '50-59' ELSE '60+' END AS age_group,
           ROUND(SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS rate
    FROM heart_silver GROUP BY 1 ORDER BY rate DESC LIMIT 1
""").collect()[0]
submit_answer("Q7", f"{q7['age_group']} ({q7['rate']}%)")

# Q8: Average max heart rate for healthy patients under 50?
q8 = spark.sql("SELECT ROUND(AVG(thalach), 1) AS ans FROM heart_silver WHERE target = 0 AND age < 50").collect()[0]["ans"]
submit_answer("Q8", str(q8))

# COMMAND ----------

# === BONUS: AI Executive Summary ===
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_executive_summary AS
    SELECT
        ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            CONCAT(
                'Write a 3-sentence executive summary of heart disease analytics: ',
                'Total patients analyzed: ', (SELECT COUNT(*) FROM {CATALOG}.default.heart_silver),
                ', Disease rate: ', (SELECT ROUND(AVG(target)*100, 1) FROM {CATALOG}.default.heart_silver), '%',
                ', Avg cholesterol (diseased): ', (SELECT ROUND(AVG(chol), 1) FROM {CATALOG}.default.heart_silver WHERE target = 1)
            )
        ) AS summary,
        current_timestamp() AS generated_at
""")
print("AI Executive Summary created")

# COMMAND ----------

# === BONUS: Cohort Comparison ===
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_cohort_comparison AS
    SELECT
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS cohort,
        COUNT(*) AS patient_count,
        ROUND(AVG(age), 1) AS avg_age,
        ROUND(AVG(chol), 1) AS avg_cholesterol,
        ROUND(AVG(trestbps), 1) AS avg_blood_pressure,
        ROUND(AVG(thalach), 1) AS avg_max_heart_rate,
        ROUND(AVG(oldpeak), 2) AS avg_oldpeak
    FROM {CATALOG}.default.heart_silver
    GROUP BY target
""")
print("Cohort comparison created")

# COMMAND ----------

print("Event 2 simulation complete for team_01")
dbutils.notebook.exit("OK")
