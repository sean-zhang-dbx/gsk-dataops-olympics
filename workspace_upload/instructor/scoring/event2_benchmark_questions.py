# Databricks notebook source
# MAGIC %md
# MAGIC # Event 2: Benchmark Questions & Answer Key
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY — Do not share with participants!**
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Pre-computes correct answers for all 8 benchmark questions
# MAGIC 2. Prints the questions exactly as you will read them aloud (with format hints)
# MAGIC 3. Prints a formatted answer key to reference during the race
# MAGIC
# MAGIC Run this **before** the event using any team's data (or the shared reference).
# MAGIC
# MAGIC ### Design Rules
# MAGIC - Every question returns a **single value** (one number or one word)
# MAGIC - Every question specifies the **exact format** expected (e.g. "Answer as a whole number")
# MAGIC - Every question includes an **example** of what the answer looks like

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

REFERENCE_CATALOG = "team_01"
SCHEMA = "default"

spark.sql(f"USE CATALOG {REFERENCE_CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Answer Key

# COMMAND ----------

questions = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1: Patient count with heart disease
# MAGIC
# MAGIC **Read aloud:** "How many patients in the dataset have heart disease (target = 1)?
# MAGIC Answer as a **whole number**. Example: `312`"

# COMMAND ----------

q1 = spark.sql("""
    SELECT COUNT(*) AS answer
    FROM heart_silver_correct
    WHERE target = 1
""").collect()[0]["answer"]

questions["Q1"] = {
    "question": "How many patients in the dataset have heart disease (target = 1)? Answer as a whole number. Example: 312",
    "answer": str(q1),
    "type": "exact_number",
    "table": "heart_silver_correct",
    "difficulty": "easy",
}
print(f"Q1 Answer: {q1}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2: Average age of heart disease patients
# MAGIC
# MAGIC **Read aloud:** "What is the average age of patients WITH heart disease (target = 1)?
# MAGIC Round to 1 decimal place. Answer as a **single number**. Example: `52.3`"

# COMMAND ----------

q2 = spark.sql("""
    SELECT ROUND(AVG(age), 1) AS answer
    FROM heart_silver_correct
    WHERE target = 1
""").collect()[0]["answer"]

questions["Q2"] = {
    "question": "What is the average age of patients WITH heart disease (target = 1)? Round to 1 decimal place. Answer as a single number. Example: 52.3",
    "answer": str(q2),
    "type": "number",
    "table": "heart_silver_correct",
    "difficulty": "easy",
}
print(f"Q2 Answer: {q2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3: Heart disease rate among female patients
# MAGIC
# MAGIC **Read aloud:** "What percentage of female patients (sex = 0) have heart disease?
# MAGIC Round to 1 decimal place. Answer as a **number without the % sign**. Example: `63.2`"

# COMMAND ----------

q3 = spark.sql("""
    SELECT ROUND(
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
    ) AS answer
    FROM heart_silver_correct
    WHERE sex = 0
""").collect()[0]["answer"]

questions["Q3"] = {
    "question": "What percentage of female patients (sex = 0) have heart disease? Round to 1 decimal place. Answer as a number without the % sign. Example: 63.2",
    "answer": str(q3),
    "type": "percentage",
    "table": "heart_silver_correct",
    "difficulty": "medium",
}
print(f"Q3 Answer: {q3}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4: Most common chest pain type in heart disease patients
# MAGIC
# MAGIC **Read aloud:** "What is the `cp` value (0, 1, 2, or 3) of the most common chest pain type
# MAGIC among heart disease patients (target = 1)?
# MAGIC Answer as a **single digit**. Example: `1`"

# COMMAND ----------

q4 = spark.sql("""
    SELECT cp
    FROM heart_silver_correct
    WHERE target = 1
    GROUP BY cp
    ORDER BY COUNT(*) DESC
    LIMIT 1
""").collect()[0]["cp"]

cp_names = {0: "typical angina", 1: "atypical angina", 2: "non-anginal pain", 3: "asymptomatic"}

questions["Q4"] = {
    "question": "What is the cp value (0, 1, 2, or 3) of the most common chest pain type among heart disease patients (target = 1)? Answer as a single digit. Example: 1",
    "answer": str(int(q4)),
    "type": "exact_number",
    "table": "heart_silver_correct",
    "difficulty": "medium",
}
print(f"Q4 Answer: {int(q4)} ({cp_names[int(q4)]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q5: Average cholesterol for 60+ heart disease patients
# MAGIC
# MAGIC **Read aloud:** "In the heart_gold_correct table, what is the average cholesterol for
# MAGIC **Heart Disease** patients in the **60+** age group?
# MAGIC Round to 1 decimal place. Answer as a **single number**. Example: `241.5`"

# COMMAND ----------

q5 = spark.sql("""
    SELECT ROUND(avg_cholesterol, 1) AS answer
    FROM heart_gold_correct
    WHERE age_group = '60+' AND diagnosis = 'Heart Disease'
""").collect()[0]["answer"]

questions["Q5"] = {
    "question": "In the heart_gold_correct table, what is the average cholesterol for Heart Disease patients in the 60+ age group? Round to 1 decimal place. Answer as a single number. Example: 241.5",
    "answer": str(q5),
    "type": "number",
    "table": "heart_gold_correct",
    "difficulty": "easy",
}
print(f"Q5 Answer: {q5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q6: High-risk patient count
# MAGIC
# MAGIC **Read aloud:** "How many patients have BOTH cholesterol above 240 (chol > 240)
# MAGIC AND resting blood pressure above 140 (trestbps > 140)?
# MAGIC Answer as a **whole number**. Example: `45`"

# COMMAND ----------

q6 = spark.sql("""
    SELECT COUNT(*) AS answer
    FROM heart_silver_correct
    WHERE chol > 240 AND trestbps > 140
""").collect()[0]["answer"]

questions["Q6"] = {
    "question": "How many patients have BOTH cholesterol above 240 (chol > 240) AND resting blood pressure above 140 (trestbps > 140)? Answer as a whole number. Example: 45",
    "answer": str(q6),
    "type": "exact_number",
    "table": "heart_silver_correct",
    "difficulty": "medium",
}
print(f"Q6 Answer: {q6}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q7: Age group with highest disease rate
# MAGIC
# MAGIC **Read aloud:** "Which age group has the highest heart disease rate?
# MAGIC Use these groups: Under 40, 40-49, 50-59, 60+.
# MAGIC Answer with the **age group name only**. Example: `50-59`"

# COMMAND ----------

q7 = spark.sql("""
    SELECT
        CASE
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END AS age_group
    FROM heart_silver_correct
    GROUP BY 1
    ORDER BY SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) DESC
    LIMIT 1
""").collect()[0]["age_group"]

questions["Q7"] = {
    "question": "Which age group has the highest heart disease rate? Use these groups: Under 40, 40-49, 50-59, 60+. Answer with the age group name only. Example: 50-59",
    "answer": q7,
    "type": "category",
    "table": "heart_silver_correct",
    "difficulty": "hard",
}
print(f"Q7 Answer: {q7}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q8: Average max heart rate for healthy patients under 50
# MAGIC
# MAGIC **Read aloud:** "What is the average maximum heart rate (thalach) for healthy patients
# MAGIC (target = 0) who are under 50 years old (age < 50)?
# MAGIC Round to 1 decimal place. Answer as a **single number**. Example: `145.7`"

# COMMAND ----------

q8 = spark.sql("""
    SELECT ROUND(AVG(thalach), 1) AS answer
    FROM heart_silver_correct
    WHERE target = 0 AND age < 50
""").collect()[0]["answer"]

questions["Q8"] = {
    "question": "What is the average maximum heart rate (thalach) for healthy patients (target = 0) who are under 50 years old (age < 50)? Round to 1 decimal place. Answer as a single number. Example: 145.7",
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
print("\n" + "=" * 72)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Questions to Read Aloud (Copy-Paste for Slides/Chat)
# MAGIC
# MAGIC Use this formatted list to announce each question during the race.

# COMMAND ----------

print("=" * 72)
print("  QUESTIONS TO READ ALOUD DURING THE RACE")
print("=" * 72)
for qid in sorted(questions.keys()):
    q = questions[qid]
    print(f"\n  {qid}: {q['question']}")
print("\n" + "=" * 72)

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
    "dataops_olympics.default.event2_answer_key"
)
print("Answer key saved to dataops_olympics.default.event2_answer_key")
