# Databricks notebook source
# MAGIC %md
# MAGIC # Event 2: Benchmark Questions & Answer Key
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY — Do not share with participants!**
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Pre-computes correct answers for all 8 benchmark questions
# MAGIC 2. Prints a formatted answer key to reference during the race
# MAGIC 3. Provides a speed-tracking template
# MAGIC
# MAGIC Run this **before** the event using any team's data (or the shared reference).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Use any team catalog that has correct Silver/Gold tables, or the answer_key catalog.

# COMMAND ----------

REFERENCE_CATALOG = "team_01"  # Use any team with correct Silver/Gold tables
SCHEMA = "default"

spark.sql(f"USE CATALOG {REFERENCE_CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Answer Key

# COMMAND ----------

import json

questions = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1: How many patients in the dataset have heart disease?

# COMMAND ----------

q1 = spark.sql("""
    SELECT COUNT(*) AS answer
    FROM heart_silver_correct
    WHERE target = 1
""").collect()[0]["answer"]

questions["Q1"] = {
    "question": "How many patients in the dataset have heart disease?",
    "answer": str(q1),
    "type": "exact_number",
    "table": "heart_silver_correct",
    "difficulty": "easy",
}
print(f"Q1 Answer: {q1}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2: Average age — heart disease vs healthy?

# COMMAND ----------

q2_df = spark.sql("""
    SELECT
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
        ROUND(AVG(age), 1) AS avg_age
    FROM heart_silver_correct
    GROUP BY target
    ORDER BY target DESC
""").toPandas()

q2_disease = q2_df[q2_df["diagnosis"] == "Heart Disease"]["avg_age"].values[0]
q2_healthy = q2_df[q2_df["diagnosis"] == "Healthy"]["avg_age"].values[0]

questions["Q2"] = {
    "question": "What is the average age of patients WITH heart disease vs WITHOUT? (give both numbers)",
    "answer": f"Heart Disease: {q2_disease}, Healthy: {q2_healthy}",
    "accept": [str(q2_disease), str(q2_healthy)],
    "type": "two_numbers",
    "table": "heart_silver_correct",
    "difficulty": "easy",
}
print(f"Q2 Answer: Heart Disease = {q2_disease}, Healthy = {q2_healthy}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3: What percentage of female patients have heart disease?

# COMMAND ----------

q3 = spark.sql("""
    SELECT ROUND(
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
    ) AS pct
    FROM heart_silver_correct
    WHERE sex = 0
""").collect()[0]["pct"]

questions["Q3"] = {
    "question": "What percentage of female patients (sex=0) have heart disease?",
    "answer": f"{q3}%",
    "accept": [str(q3)],
    "type": "percentage",
    "table": "heart_silver_correct",
    "difficulty": "medium",
}
print(f"Q3 Answer: {q3}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4: Most common chest pain type among heart disease patients?

# COMMAND ----------

q4_df = spark.sql("""
    SELECT cp, COUNT(*) AS cnt
    FROM heart_silver_correct
    WHERE target = 1
    GROUP BY cp
    ORDER BY cnt DESC
    LIMIT 1
""").collect()[0]

cp_names = {0: "typical angina", 1: "atypical angina", 2: "non-anginal pain", 3: "asymptomatic"}
q4_cp = int(q4_df["cp"])
q4_name = cp_names[q4_cp]

questions["Q4"] = {
    "question": "Which chest pain type (cp) is most common among heart disease patients?",
    "answer": f"{q4_cp} ({q4_name})",
    "accept": [str(q4_cp), q4_name],
    "type": "category",
    "table": "heart_silver_correct",
    "difficulty": "medium",
}
print(f"Q4 Answer: cp = {q4_cp} ({q4_name}), count = {q4_df['cnt']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q5: Average cholesterol for patients in the 60+ age group?

# COMMAND ----------

q5 = spark.sql("""
    SELECT ROUND(avg_cholesterol, 1) AS answer
    FROM heart_gold_correct
    WHERE age_group = '60+'
""").toPandas()

q5_values = q5["answer"].tolist()
q5_str = ", ".join(str(v) for v in q5_values)

questions["Q5"] = {
    "question": "What is the average cholesterol for patients in the 60+ age group? (both diagnoses)",
    "answer": q5_str,
    "accept": [str(v) for v in q5_values],
    "type": "number_or_list",
    "table": "heart_gold_correct",
    "difficulty": "easy",
}
print(f"Q5 Answer: {q5_str}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q6: Patients with cholesterol > 240 AND resting BP > 140?

# COMMAND ----------

q6 = spark.sql("""
    SELECT COUNT(*) AS answer
    FROM heart_silver_correct
    WHERE chol > 240 AND trestbps > 140
""").collect()[0]["answer"]

questions["Q6"] = {
    "question": "How many patients have BOTH cholesterol > 240 AND resting blood pressure > 140?",
    "answer": str(q6),
    "type": "exact_number",
    "table": "heart_silver_correct",
    "difficulty": "medium",
}
print(f"Q6 Answer: {q6}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q7: Which age group has the highest heart disease rate?

# COMMAND ----------

q7_df = spark.sql("""
    SELECT
        CASE
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END AS age_group,
        ROUND(SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS disease_rate
    FROM heart_silver_correct
    GROUP BY 1
    ORDER BY disease_rate DESC
    LIMIT 1
""").collect()[0]

q7_group = q7_df["age_group"]
q7_rate = q7_df["disease_rate"]

questions["Q7"] = {
    "question": "Which age group has the highest heart disease rate (% of patients with target=1)?",
    "answer": f"{q7_group} ({q7_rate}%)",
    "accept": [q7_group, str(q7_rate)],
    "type": "category_with_number",
    "table": "heart_silver_correct",
    "difficulty": "hard",
}
print(f"Q7 Answer: {q7_group} at {q7_rate}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q8: Average max heart rate for healthy patients under 50?

# COMMAND ----------

q8 = spark.sql("""
    SELECT ROUND(AVG(thalach), 1) AS answer
    FROM heart_silver_correct
    WHERE target = 0 AND age < 50
""").collect()[0]["answer"]

questions["Q8"] = {
    "question": "What is the average max heart rate (thalach) for healthy patients under 50?",
    "answer": str(q8),
    "type": "number",
    "table": "heart_silver_correct",
    "difficulty": "medium",
}
print(f"Q8 Answer: {q8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Formatted Answer Key
# MAGIC
# MAGIC Print this and keep it handy during the race.

# COMMAND ----------

print("=" * 72)
print("  EVENT 2: BENCHMARK RACE — ANSWER KEY")
print("=" * 72)
for qid in sorted(questions.keys()):
    q = questions[qid]
    diff = q["difficulty"].upper()
    print(f"\n  {qid} [{diff}]: {q['question']}")
    print(f"  >>> ANSWER: {q['answer']}")
    print(f"      Table: {q['table']} | Accept: {q.get('accept', [q['answer']])}")
print("\n" + "=" * 72)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Speed Tracking Template
# MAGIC
# MAGIC Fill this in during the race — record which team answers first per question.

# COMMAND ----------

TEAMS = [f"team_{i:02d}" for i in range(1, 16)]  # team_01 through team_15

speed_winners = {
    "Q1": "",  # team name of first correct answer
    "Q2": "",
    "Q3": "",
    "Q4": "",
    "Q5": "",
    "Q6": "",
    "Q7": "",
    "Q8": "",
}

team_answers = {team: {f"Q{i}": {"answer": "", "method": ""} for i in range(1, 9)} for team in TEAMS}

print("Speed Tracking — fill in team names during the race:")
for q in sorted(speed_winners.keys()):
    print(f"  {q}: first correct = {speed_winners[q] or '________'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Answer Key

# COMMAND ----------

import pandas as pd

key_rows = []
for qid, q in questions.items():
    key_rows.append({
        "question_id": qid,
        "question": q["question"],
        "answer": q["answer"],
        "difficulty": q["difficulty"],
        "table_used": q["table"],
    })

key_df = spark.createDataFrame(pd.DataFrame(key_rows))
key_df.write.format("delta").mode("overwrite").saveAsTable(
    "dataops_olympics_instructor.default.event2_answer_key"
)
print("Answer key saved to dataops_olympics_instructor.default.event2_answer_key")
