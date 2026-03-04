# Databricks notebook source
# MAGIC %md
# MAGIC # Event 4: Answer Key — Clinical AI Agent
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**

# COMMAND ----------

CATALOG = "team_01"
SHARED_CATALOG = "dataops_olympics"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")

# COMMAND ----------

# MAGIC %md
# MAGIC ## System Prompt

# COMMAND ----------

system_prompt = """You are a clinical data analyst AI assistant for a hospital system.

AVAILABLE DATA SOURCES:
1. heart_silver ({CATALOG}.default): ~488 patient records with clinical measurements.
   Key columns: age, sex (0=female, 1=male), cp (chest pain: 0=typical angina,
   1=atypical, 2=non-anginal, 3=asymptomatic), trestbps (resting BP mmHg),
   chol (cholesterol mg/dL), thalach (max heart rate), target (1=heart disease, 0=healthy).

2. heart_gold ({CATALOG}.default): ~8 aggregated rows.
   Columns: age_group, diagnosis, patient_count, avg_cholesterol, avg_blood_pressure, avg_max_heart_rate.

3. drug_reviews (dataops_olympics.default): 1000 drug reviews.
   Columns: drug_name, condition, rating (1-10), review_text, date.

4. clinical_notes (dataops_olympics.default): 20 clinical notes.
   Columns: note_id, department, note_type, note_text, author, created_date.

RULES:
- Always cite the data source and row counts in your response.
- Never provide specific medical advice or treatment recommendations.
- Round numbers to 1 decimal place.
- When asked about heart disease, target=1 means disease present.
- When asked about drugs, use average rating as the quality metric.
"""

print(f"System prompt: {len(system_prompt)} chars")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Function

# COMMAND ----------

def clinical_agent(question: str) -> str:
    q = question.lower()

    if any(kw in q for kw in ["drug", "medication", "rating", "review", "prescription"]):
        return _handle_drug_query(question)
    elif any(kw in q for kw in ["clinical note", "department", "note type", "hospital note"]):
        return _handle_notes_query(question)
    elif any(kw in q for kw in ["age group", "age_group", "cohort"]):
        return _handle_gold_query(question)
    elif any(kw in q for kw in ["heart", "patient", "disease", "cardiac", "cholesterol", "blood pressure", "chest pain"]):
        return _handle_heart_query(question)
    else:
        return f"I can answer questions about: (1) heart disease patients, (2) drug reviews/ratings, (3) clinical notes by department. Please rephrase your question."


def _handle_heart_query(question: str) -> str:
    q = question.lower()

    if "how many" in q and "heart disease" in q:
        row = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.default.heart_silver WHERE target = 1").collect()[0]
        return f"There are {row['cnt']} patients with heart disease in the dataset (source: heart_silver)."

    if "chest pain" in q and ("common" in q or "most" in q):
        rows = spark.sql(f"""
            SELECT cp, COUNT(*) AS cnt FROM {CATALOG}.default.heart_silver
            WHERE target = 1 GROUP BY cp ORDER BY cnt DESC
        """).collect()
        cp_names = {0: "typical angina", 1: "atypical angina", 2: "non-anginal pain", 3: "asymptomatic"}
        top = rows[0]
        return f"The most common chest pain type among heart disease patients is cp={top['cp']} ({cp_names.get(top['cp'], '?')}), with {top['cnt']} patients (source: heart_silver)."

    if "disease rate" in q or "highest" in q and "rate" in q:
        return _handle_gold_query(question)

    row = spark.sql(f"""
        SELECT COUNT(*) AS total,
            SUM(CASE WHEN target=1 THEN 1 ELSE 0 END) AS disease,
            ROUND(AVG(age), 1) AS avg_age
        FROM {CATALOG}.default.heart_silver
    """).collect()[0]
    return f"Dataset has {row['total']} patients, {row['disease']} with heart disease ({row['disease']*100//row['total']}%), avg age {row['avg_age']} (source: heart_silver)."


def _handle_drug_query(question: str) -> str:
    q = question.lower()

    if "highest" in q and "rating" in q:
        row = spark.sql(f"""
            SELECT drug_name, ROUND(AVG(rating), 1) AS avg_rating, COUNT(*) AS reviews
            FROM {SHARED_CATALOG}.default.drug_reviews
            GROUP BY drug_name ORDER BY avg_rating DESC LIMIT 1
        """).collect()[0]
        return f"The highest-rated drug is {row['drug_name']} with avg rating {row['avg_rating']}/10 ({row['reviews']} reviews) (source: drug_reviews)."

    rows = spark.sql(f"""
        SELECT drug_name, ROUND(AVG(rating), 1) AS avg_rating, COUNT(*) AS reviews
        FROM {SHARED_CATALOG}.default.drug_reviews
        GROUP BY drug_name ORDER BY avg_rating DESC LIMIT 5
    """).collect()
    result = "Top 5 drugs by rating:\n"
    for r in rows:
        result += f"  - {r['drug_name']}: {r['avg_rating']}/10 ({r['reviews']} reviews)\n"
    return result + "(source: drug_reviews)"


def _handle_notes_query(question: str) -> str:
    q = question.lower()

    if "most" in q and ("department" in q or "note" in q):
        row = spark.sql(f"""
            SELECT department, COUNT(*) AS cnt
            FROM {SHARED_CATALOG}.default.clinical_notes
            GROUP BY department ORDER BY cnt DESC LIMIT 1
        """).collect()[0]
        return f"The department with the most clinical notes is {row['department']} with {row['cnt']} notes (source: clinical_notes)."

    rows = spark.sql(f"""
        SELECT department, COUNT(*) AS cnt
        FROM {SHARED_CATALOG}.default.clinical_notes
        GROUP BY department ORDER BY cnt DESC
    """).collect()
    result = "Clinical notes by department:\n"
    for r in rows:
        result += f"  - {r['department']}: {r['cnt']} notes\n"
    return result + "(source: clinical_notes)"


def _handle_gold_query(question: str) -> str:
    rows = spark.sql(f"""
        SELECT age_group,
            SUM(CASE WHEN diagnosis='Heart Disease' THEN patient_count ELSE 0 END) AS disease,
            SUM(patient_count) AS total
        FROM {CATALOG}.default.heart_gold
        GROUP BY age_group
    """).collect()

    best_group = None
    best_rate = 0
    for r in rows:
        rate = r["disease"] / r["total"] * 100 if r["total"] > 0 else 0
        if rate > best_rate:
            best_rate = rate
            best_group = r["age_group"]

    return f"The age group with the highest heart disease rate is {best_group} at {best_rate:.1f}% (source: heart_gold)."

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI Functions

# COMMAND ----------

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
display(spark.table(f"{CATALOG}.default.heart_ai_insights"))

# COMMAND ----------

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
display(spark.table(f"{CATALOG}.default.drug_ai_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Prompts

# COMMAND ----------

test_prompts = [
    "How many patients in the dataset have heart disease?",
    "What is the most common chest pain type among heart disease patients?",
    "Which drug has the highest average rating?",
    "What department has the most clinical notes?",
    "Which age group has the highest heart disease rate?",
]

for i, p in enumerate(test_prompts, 1):
    print(f"\nT{i}: {p}")
    print(f"  → {clinical_agent(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: Genie Space Integration
# MAGIC
# MAGIC > If you have a Genie space from Event 2, integrate it as an agent tool.
# MAGIC > The code below demonstrates the pattern — replace GENIE_SPACE_ID with yours.

# COMMAND ----------

# Genie integration example (requires valid Genie space ID from Event 2)
# Uncomment and configure to earn +3 bonus points

# import requests, json, time
# GENIE_SPACE_ID = "YOUR_GENIE_SPACE_ID"
# host = spark.conf.get("spark.databricks.workspaceUrl")
# token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
# headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
#
# def ask_genie(question: str) -> str:
#     resp = requests.post(
#         f"https://{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation",
#         headers=headers, json={"content": question}
#     )
#     conv = resp.json()
#     conv_id, msg_id = conv["conversation_id"], conv["message_id"]
#     for _ in range(15):
#         time.sleep(2)
#         r = requests.get(
#             f"https://{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conv_id}/messages/{msg_id}",
#             headers=headers
#         )
#         msg = r.json()
#         if msg.get("status") == "COMPLETED":
#             for att in msg.get("attachments", []):
#                 if att.get("text", {}).get("content"):
#                     return att["text"]["content"]
#             return "Genie returned no text result"
#     return "Genie timed out"
#
# genie_questions = ["How many patients have heart disease?", "What is the average age of patients?"]
# genie_log = [{"question": q, "genie_answer": ask_genie(q)} for q in genie_questions]
# spark.createDataFrame(genie_log).write.format("delta").mode("overwrite") \
#     .saveAsTable(f"{CATALOG}.default.genie_agent_log")
