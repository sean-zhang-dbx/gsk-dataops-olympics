# Databricks notebook source
# Event 4 Simulation: team_01 does GenAI agent + AI functions + bonuses

TEAM_NAME = "team_01"
CATALOG = TEAM_NAME
SHARED_CATALOG = "dataops_olympics"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")

# COMMAND ----------

# === AI Functions: heart_ai_insights ===
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_ai_insights AS
    SELECT *,
        ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            CONCAT(
                'You are a clinical analyst. Provide a 2-sentence insight for this cohort: ',
                age_group, ' patients, ', diagnosis, ', n=', patient_count,
                ', avg cholesterol=', avg_cholesterol, ' mg/dL',
                ', avg BP=', avg_blood_pressure, ' mmHg',
                ', avg max HR=', avg_max_heart_rate, ' bpm'
            )
        ) AS clinical_insight
    FROM {CATALOG}.default.heart_gold
""")
print(f"heart_ai_insights: {spark.table(f'{CATALOG}.default.heart_ai_insights').count()} rows")

# COMMAND ----------

# === AI Functions: drug_ai_summary ===
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.drug_ai_summary AS
    SELECT drug_name, ROUND(AVG(rating), 1) AS avg_rating, COUNT(*) AS reviews,
        ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            CONCAT('Summarize this drug in one sentence: ', drug_name,
                   ', avg rating ', ROUND(AVG(rating), 1), '/10, ',
                   COUNT(*), ' reviews')
        ) AS ai_summary
    FROM {SHARED_CATALOG}.default.drug_reviews
    GROUP BY drug_name
    ORDER BY avg_rating DESC
    LIMIT 5
""")
print(f"drug_ai_summary: {spark.table(f'{CATALOG}.default.drug_ai_summary').count()} rows")

# COMMAND ----------

# === Agent Function (demonstrate routing) ===
def clinical_agent(question):
    q = question.lower()
    if any(kw in q for kw in ["drug", "medication", "rating", "review"]):
        row = spark.sql(f"""
            SELECT drug_name, avg_rating FROM {CATALOG}.default.drug_ai_summary
            ORDER BY avg_rating DESC LIMIT 1
        """).collect()[0]
        return f"Highest-rated drug: {row['drug_name']} ({row['avg_rating']}/10)"
    elif any(kw in q for kw in ["heart", "patient", "disease"]):
        row = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.default.heart_silver WHERE target = 1").collect()[0]
        return f"{row['cnt']} patients have heart disease"
    elif any(kw in q for kw in ["clinical note", "department"]):
        row = spark.sql(f"""
            SELECT department, COUNT(*) AS cnt FROM {SHARED_CATALOG}.default.clinical_notes
            GROUP BY department ORDER BY cnt DESC LIMIT 1
        """).collect()[0]
        return f"Most notes in {row['department']} ({row['cnt']})"
    return "I can answer questions about heart disease, drug reviews, and clinical notes."

# Test the agent
test_prompts = [
    "How many patients have heart disease?",
    "Which drug has the highest rating?",
    "What department has the most clinical notes?",
]
audit_log = []
for p in test_prompts:
    answer = clinical_agent(p)
    audit_log.append({"question": p, "answer": answer, "status": "success"})
    print(f"  Q: {p}\n  A: {answer}\n")

# COMMAND ----------

# === BONUS: Safety Guardrails (audit log) ===
import pandas as pd
spark.createDataFrame(pd.DataFrame(audit_log)).write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.default.agent_audit_log"
)
print("Agent audit log saved")

# COMMAND ----------

print("Event 4 simulation complete for team_01")
dbutils.notebook.exit("OK")
