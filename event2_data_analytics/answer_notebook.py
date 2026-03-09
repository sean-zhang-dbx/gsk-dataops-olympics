# Databricks notebook source
# MAGIC %md
# MAGIC # Event 2: Answer Key — Complete Solutions
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY — Do not share with participants!**
# MAGIC
# MAGIC This notebook contains complete SQL solutions for:
# MAGIC - All 3 practice queries from the Build Phase
# MAGIC - All 8 benchmark questions with expected output

# COMMAND ----------

CATALOG = "team_01"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")
print(f"Using: {CATALOG}.default")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Practice Query Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practice 1: Heart Disease Prevalence

# COMMAND ----------

display(spark.sql("""
    SELECT
        COUNT(*) AS total_patients,
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS with_heart_disease,
        SUM(CASE WHEN target = 0 THEN 1 ELSE 0 END) AS healthy,
        ROUND(SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS disease_pct
    FROM heart_silver
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practice 2: Age Profile by Diagnosis

# COMMAND ----------

display(spark.sql("""
    SELECT
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
        COUNT(*) AS patient_count,
        ROUND(AVG(age), 1) AS avg_age,
        MIN(age) AS min_age,
        MAX(age) AS max_age
    FROM heart_silver
    GROUP BY target
    ORDER BY target DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practice 3: Chest Pain Distribution

# COMMAND ----------

display(spark.sql("""
    SELECT
        CASE
            WHEN cp = 0 THEN '0: Typical Angina'
            WHEN cp = 1 THEN '1: Atypical Angina'
            WHEN cp = 2 THEN '2: Non-Anginal Pain'
            WHEN cp = 3 THEN '3: Asymptomatic'
        END AS chest_pain_type,
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS heart_disease,
        SUM(CASE WHEN target = 0 THEN 1 ELSE 0 END) AS healthy,
        COUNT(*) AS total
    FROM heart_silver
    GROUP BY cp
    ORDER BY cp
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Benchmark Question Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1: How many patients have heart disease?

# COMMAND ----------

display(spark.sql("""
    SELECT COUNT(*) AS patients_with_heart_disease
    FROM heart_silver
    WHERE target = 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2: Average age — heart disease vs healthy?

# COMMAND ----------

display(spark.sql("""
    SELECT
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
        ROUND(AVG(age), 1) AS avg_age
    FROM heart_silver
    GROUP BY target
    ORDER BY target DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3: Percentage of female patients with heart disease?

# COMMAND ----------

display(spark.sql("""
    SELECT
        COUNT(*) AS total_female,
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS female_with_disease,
        ROUND(SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS disease_pct
    FROM heart_silver
    WHERE sex = 0
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4: Most common chest pain type among heart disease patients?

# COMMAND ----------

display(spark.sql("""
    SELECT
        cp,
        CASE
            WHEN cp = 0 THEN 'typical angina'
            WHEN cp = 1 THEN 'atypical angina'
            WHEN cp = 2 THEN 'non-anginal pain'
            WHEN cp = 3 THEN 'asymptomatic'
        END AS chest_pain_name,
        COUNT(*) AS patient_count
    FROM heart_silver
    WHERE target = 1
    GROUP BY cp
    ORDER BY patient_count DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q5: Average cholesterol for 60+ age group?

# COMMAND ----------

display(spark.sql("""
    SELECT age_group, diagnosis, avg_cholesterol
    FROM heart_gold
    WHERE age_group = '60+'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q6: Patients with cholesterol > 240 AND BP > 140?

# COMMAND ----------

display(spark.sql("""
    SELECT COUNT(*) AS high_risk_patients
    FROM heart_silver
    WHERE chol > 240 AND trestbps > 140
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q7: Age group with highest heart disease rate?

# COMMAND ----------

display(spark.sql("""
    SELECT
        CASE
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END AS age_group,
        COUNT(*) AS total,
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS with_disease,
        ROUND(SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS disease_rate_pct
    FROM heart_silver
    GROUP BY 1
    ORDER BY disease_rate_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q8: Average max heart rate for healthy patients under 50?

# COMMAND ----------

display(spark.sql("""
    SELECT ROUND(AVG(thalach), 1) AS avg_max_heart_rate
    FROM heart_silver
    WHERE target = 0 AND age < 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Genie Prompt Examples
# MAGIC
# MAGIC These are the natural language prompts a well-configured Genie should handle:
# MAGIC
# MAGIC | Question | Expected Genie Prompt |
# MAGIC |----------|----------------------|
# MAGIC | Q1 | "How many patients have heart disease?" |
# MAGIC | Q2 | "What is the average age for heart disease patients versus healthy?" |
# MAGIC | Q3 | "What percentage of female patients have heart disease?" |
# MAGIC | Q4 | "What chest pain type is most common in heart disease patients?" |
# MAGIC | Q5 | "What is the average cholesterol for patients aged 60 and over?" |
# MAGIC | Q6 | "How many patients have cholesterol above 240 and blood pressure above 140?" |
# MAGIC | Q7 | "Which age group has the highest rate of heart disease?" |
# MAGIC | Q8 | "Average maximum heart rate for healthy patients younger than 50?" |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Create "Perfect" Organizer Genie Space
# MAGIC
# MAGIC Programmatically creates a fully-configured Genie space using the REST API.
# MAGIC This serves as the organizer's reference — a gold-standard Genie space that
# MAGIC correctly answers all 8 benchmark questions.

# COMMAND ----------

import requests, json, uuid

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Find a SQL warehouse
wh_resp = requests.get(f"https://{host}/api/2.0/sql/warehouses", headers=headers).json()
wh_id = None
for w in wh_resp.get("warehouses", []):
    if w.get("state") in ("RUNNING", "STOPPED") and w.get("warehouse_type") in ("PRO", "CLASSIC"):
        wh_id = w["id"]
        break
if not wh_id and wh_resp.get("warehouses"):
    wh_id = wh_resp["warehouses"][0]["id"]
print(f"Using warehouse: {wh_id}")

# COMMAND ----------

serialized_space = json.dumps({
    "version": 2,
    "config": {
        "sample_questions": [
            {"id": uuid.uuid4().hex, "question": ["How many patients have heart disease?"]},
            {"id": uuid.uuid4().hex, "question": ["What is the average age of heart disease patients versus healthy patients?"]},
            {"id": uuid.uuid4().hex, "question": ["What percentage of female patients have heart disease?"]},
            {"id": uuid.uuid4().hex, "question": ["Which chest pain type is most common among heart disease patients?"]},
            {"id": uuid.uuid4().hex, "question": ["What is the average cholesterol for patients in the 60+ age group?"]},
            {"id": uuid.uuid4().hex, "question": ["How many patients have both cholesterol above 240 and blood pressure above 140?"]},
            {"id": uuid.uuid4().hex, "question": ["Which age group has the highest heart disease rate?"]},
            {"id": uuid.uuid4().hex, "question": ["What is the average max heart rate for healthy patients under 50?"]},
        ]
    },
    "data_sources": {
        "tables": sorted([
            {
                "identifier": f"{CATALOG}.default.heart_gold",
                "description": [
                    "Pre-aggregated cohort metrics grouped by age_group and diagnosis. "
                    "~8 rows (4 age groups x 2 diagnoses). Columns: age_group (Under 40, 40-49, 50-59, 60+), "
                    "diagnosis (Heart Disease or Healthy), patient_count, avg_cholesterol, "
                    "avg_blood_pressure, avg_max_heart_rate. Prefer this table for aggregated questions."
                ],
            },
            {
                "identifier": f"{CATALOG}.default.heart_silver",
                "description": [
                    "Patient-level clinical records. ~488 rows after quality filtering. Each row is one patient. "
                    "Key columns: age (years), sex (0=female, 1=male), cp (chest pain: 0=typical angina, "
                    "1=atypical angina, 2=non-anginal pain, 3=asymptomatic), trestbps (resting BP mmHg), "
                    "chol (cholesterol mg/dL), fbs (fasting blood sugar >120: 1=true), restecg (0=normal, "
                    "1=ST-T abnormality, 2=LVH), thalach (max heart rate in exercise), exang (exercise angina: "
                    "1=yes), oldpeak (ST depression), slope, ca (vessels 0-3), thal, "
                    "target (DIAGNOSIS: 1=heart disease, 0=healthy). Use for patient-level analysis."
                ],
            },
        ], key=lambda t: t["identifier"]),
    },
    "instructions": {
        "text_instructions": [{
            "id": uuid.uuid4().hex,
            "content": [
                "DOMAIN: Heart disease patient analytics from a hospital intake system.\n\n"
                "RULES:\n"
                "- 'heart disease patients' means target = 1. 'healthy patients' means target = 0.\n"
                "- 'disease rate' or 'prevalence' = COUNT(target=1) / COUNT(*) * 100.\n"
                "- Always round percentages and averages to 1 decimal place.\n"
                "- Age groups: Under 40 (age < 40), 40-49 (40 <= age < 50), 50-59 (50 <= age < 60), 60+ (age >= 60).\n"
                "- For aggregated questions by age group or diagnosis, prefer heart_gold.\n"
                "- For patient-level filtering and counting, use heart_silver.\n"
                "- sex: 0 = female, 1 = male.\n"
                "- cp (chest pain): 0 = typical angina, 1 = atypical angina, 2 = non-anginal pain, 3 = asymptomatic.\n"
                "- thalach = max heart rate, trestbps = resting blood pressure, chol = cholesterol.\n"
                "- fbs = fasting blood sugar > 120 mg/dL (1=true, 0=false).\n"
                "- exang = exercise-induced angina (1=yes, 0=no).\n"
                "- oldpeak = ST depression induced by exercise.\n"
                "- restecg: 0=normal, 1=ST-T wave abnormality, 2=left ventricular hypertrophy."
            ]
        }],
        "example_question_sqls": [
            {
                "id": uuid.uuid4().hex,
                "question": ["What percentage of patients have heart disease?"],
                "sql": [
                    "SELECT COUNT(*) AS total_patients, "
                    "SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS with_disease, "
                    "ROUND(SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS disease_pct "
                    "FROM heart_silver"
                ],
            },
            {
                "id": uuid.uuid4().hex,
                "question": ["What is the average cholesterol by age group?"],
                "sql": [
                    "SELECT age_group, diagnosis, avg_cholesterol, avg_blood_pressure, avg_max_heart_rate "
                    "FROM heart_gold ORDER BY age_group"
                ],
            },
            {
                "id": uuid.uuid4().hex,
                "question": ["How many female patients have heart disease?"],
                "sql": [
                    "SELECT COUNT(*) AS female_heart_disease "
                    "FROM heart_silver WHERE sex = 0 AND target = 1"
                ],
            },
            {
                "id": uuid.uuid4().hex,
                "question": ["Which age group has the highest heart disease rate?"],
                "sql": [
                    "SELECT CASE WHEN age < 40 THEN 'Under 40' WHEN age < 50 THEN '40-49' "
                    "WHEN age < 60 THEN '50-59' ELSE '60+' END AS age_group, "
                    "ROUND(SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS disease_rate_pct "
                    "FROM heart_silver GROUP BY 1 ORDER BY disease_rate_pct DESC"
                ],
            },
            {
                "id": uuid.uuid4().hex,
                "question": ["How many patients have cholesterol above 240 and blood pressure above 140?"],
                "sql": [
                    "SELECT COUNT(*) AS high_risk_patients "
                    "FROM heart_silver WHERE chol > 240 AND trestbps > 140"
                ],
            },
            {
                "id": uuid.uuid4().hex,
                "question": ["What is the average max heart rate for healthy patients under 50?"],
                "sql": [
                    "SELECT ROUND(AVG(thalach), 1) AS avg_max_heart_rate "
                    "FROM heart_silver WHERE target = 0 AND age < 50"
                ],
            },
        ],
    },
})

payload = {
    "title": "Organizer — Heart Disease Analyst (Reference)",
    "description": (
        "Organizer's gold-standard Genie space for Event 2. "
        "Fully configured with instructions, example SQLs, and sample questions "
        "covering all 8 benchmark questions."
    ),
    "serialized_space": serialized_space,
    "warehouse_id": wh_id,
}

resp = requests.post(f"https://{host}/api/2.0/genie/spaces", headers=headers, json=payload)
if resp.status_code == 200:
    data = resp.json()
    ORGANIZER_GENIE_ID = data.get("space_id", "")
    print(f"Organizer Genie Space created: {ORGANIZER_GENIE_ID}")
    print(f"URL: https://{host}/genie/rooms/{ORGANIZER_GENIE_ID}")
else:
    print(f"Error {resp.status_code}: {resp.text[:500]}")
    ORGANIZER_GENIE_ID = ""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Organizer Genie Space
# MAGIC
# MAGIC Verify it can handle all 8 benchmark questions correctly.

# COMMAND ----------

import time

def ask_genie(space_id, question):
    """Send a question to a Genie Space and return the answer."""
    resp = requests.post(
        f"https://{host}/api/2.0/genie/spaces/{space_id}/start-conversation",
        headers=headers, json={"content": question}
    ).json()
    conv_id = resp.get("conversation_id", "")
    msg_id = resp.get("message_id", "")
    if not conv_id:
        return f"ERROR: {resp}"
    for _ in range(20):
        time.sleep(3)
        r = requests.get(
            f"https://{host}/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}",
            headers=headers
        ).json()
        if r.get("status") == "COMPLETED":
            for att in r.get("attachments", []):
                if att.get("text", {}).get("content"):
                    return att["text"]["content"]
                if att.get("query", {}).get("query"):
                    return f"SQL: {att['query']['query'][:200]}"
            return "Completed (no text)"
        if r.get("status") == "FAILED":
            return f"FAILED: {r.get('error', '')}"
    return "TIMEOUT"

if ORGANIZER_GENIE_ID:
    test_questions = [
        ("Q1", "How many patients have heart disease?"),
        ("Q2", "What is the average age of heart disease patients versus healthy?"),
        ("Q3", "What percentage of female patients have heart disease?"),
        ("Q4", "Which chest pain type is most common among heart disease patients?"),
        ("Q5", "What is the average cholesterol for patients in the 60+ age group?"),
        ("Q6", "How many patients have cholesterol above 240 and blood pressure above 140?"),
        ("Q7", "Which age group has the highest heart disease rate?"),
        ("Q8", "Average max heart rate for healthy patients under 50?"),
    ]
    print(f"Testing organizer Genie space {ORGANIZER_GENIE_ID}...\n")
    for qid, q in test_questions:
        answer = ask_genie(ORGANIZER_GENIE_ID, q)
        print(f"  {qid}: {q}")
        print(f"      -> {answer[:200]}\n")
else:
    print("No organizer Genie space to test")
