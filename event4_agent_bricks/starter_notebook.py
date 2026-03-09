# Databricks notebook source
# MAGIC %md
# MAGIC # Event 4: Agent Bricks — Clinical AI Supervisor
# MAGIC
# MAGIC ## Build a Production-Grade AI Supervisor with Databricks Agent Bricks
# MAGIC **Time: 25 minutes** | **Max Points: 40 (+8 bonus)**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Scenario
# MAGIC
# MAGIC > Your hospital's data platform is mature — the pipeline is running (Event 1),
# MAGIC > analytics are flowing (Event 2), and the ML model is flagging at-risk patients (Event 3).
# MAGIC >
# MAGIC > The **Director of Clinical Operations** wants a single AI assistant that *any* staff
# MAGIC > member can chat with. But this isn't a hackathon prototype — it needs to be a
# MAGIC > **production-grade Supervisor Agent** built with Databricks Agent Bricks:
# MAGIC >
# MAGIC > - A **Genie Space** for SQL analytics on patient data
# MAGIC > - A **Knowledge Assistant** for clinical documentation Q&A
# MAGIC > - **UC Function tools** for AI-powered risk assessment
# MAGIC > - A **Supervisor Agent** that routes questions to the right sub-agent
# MAGIC >
# MAGIC > Each component is a real, deployed Databricks resource — not just Python code.
# MAGIC
# MAGIC ### Scoring Overview
# MAGIC
# MAGIC | Category | Points |
# MAGIC |----------|--------|
# MAGIC | UC Function Tools | 10 |
# MAGIC | Genie Space | 8 |
# MAGIC | Knowledge Assistant | 7 |
# MAGIC | Supervisor Agent | 10 |
# MAGIC | Test Prompt Evaluation | 5 |
# MAGIC | **Total** | **40** |
# MAGIC | Bonus: MCP Server | +5 |
# MAGIC | Bonus: Routing Instructions | +3 |
# MAGIC | Bonus: DSPy Prompt Optimization | +3 |
# MAGIC
# MAGIC ### Data Sources
# MAGIC
# MAGIC | Table | Location | Description |
# MAGIC |-------|----------|-------------|
# MAGIC | `heart_silver` | `{TEAM_NAME}.default` | ~488 patient clinical records |
# MAGIC | `heart_gold` | `{TEAM_NAME}.default` | ~8 aggregated cohort metrics |
# MAGIC | `drug_reviews` | `dataops_olympics.default` | 1,000 drug reviews |
# MAGIC | `clinical_notes` | `dataops_olympics.default` | 20 hospital notes |
# MAGIC
# MAGIC ### Clinical Documents (for Knowledge Assistant)
# MAGIC
# MAGIC ```
# MAGIC /Volumes/dataops_olympics/default/raw_data/clinical_docs/
# MAGIC   ├── heart_disease_protocol.txt
# MAGIC   ├── patient_intake_policy.txt
# MAGIC   ├── data_quality_standards.txt
# MAGIC   ├── cardiology_guidelines.txt
# MAGIC   └── ... (10 documents)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Team Configuration

# COMMAND ----------

TEAM_NAME = "team_XX"  # <-- CHANGE THIS
CATALOG = TEAM_NAME
SHARED_CATALOG = "dataops_olympics"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")
print(f"Team catalog: {CATALOG}.default")
print(f"Shared data:  {SHARED_CATALOG}.default")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 1: Explore Your Data Sources
# MAGIC
# MAGIC > Verify you can access all 4 tables from previous events.

# COMMAND ----------

for table, catalog in [
    ("heart_silver", CATALOG),
    ("heart_gold", CATALOG),
    ("drug_reviews", SHARED_CATALOG),
    ("clinical_notes", SHARED_CATALOG),
]:
    try:
        cnt = spark.table(f"{catalog}.default.{table}").count()
        print(f"  {catalog}.default.{table}: {cnt} rows")
    except Exception as e:
        print(f"  {catalog}.default.{table}: ERROR - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 2: Create UC Function Tools (10 pts)
# MAGIC
# MAGIC > Create two **Unity Catalog functions** that your Supervisor Agent will use as tools.
# MAGIC > These are real, registered functions in your catalog — not just Python code.
# MAGIC >
# MAGIC > **Function 1: `patient_risk_assessment`** — AI-powered cardiovascular risk classifier
# MAGIC > that uses `ai_query()` to analyze patient metrics and return LOW / MEDIUM / HIGH.
# MAGIC >
# MAGIC > **Function 2: `get_cohort_summary`** — Queries the Gold table and returns a formatted
# MAGIC > summary for an age group.
# MAGIC >
# MAGIC > *Tip: Use Databricks Assistant (`Cmd+I`) to help write these!*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Function 1: AI-powered risk assessment
# MAGIC -- This function calls an LLM to classify cardiovascular risk
# MAGIC -- YOUR CODE HERE — create patient_risk_assessment function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Function 2: Cohort summary lookup
# MAGIC -- This function queries heart_gold and returns formatted text
# MAGIC -- YOUR CODE HERE — create get_cohort_summary function

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Your Functions

# COMMAND ----------

# Test your functions
try:
    result = spark.sql(f"SELECT {CATALOG}.default.patient_risk_assessment(55, 250.0, 145.0, 150.0) AS risk").collect()
    print(f"  Risk assessment: {result[0]['risk']}")
except Exception as e:
    print(f"  patient_risk_assessment: {e}")

try:
    result = spark.sql(f"SELECT {CATALOG}.default.get_cohort_summary('50-59') AS summary").collect()
    print(f"  Cohort summary: {result[0]['summary']}")
except Exception as e:
    print(f"  get_cohort_summary: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 3: Create a Genie Space (8 pts)
# MAGIC
# MAGIC > Create a **Genie Space** that lets staff explore patient data with natural language.
# MAGIC >
# MAGIC > ### Instructions (in the Databricks UI):
# MAGIC >
# MAGIC > 1. Go to **AI/BI → Genie Spaces → New Genie Space**
# MAGIC > 2. Name it: **`{TEAM_NAME}_clinical_analytics`**
# MAGIC > 3. Add tables:
# MAGIC >    - `{TEAM_NAME}.default.heart_silver`
# MAGIC >    - `{TEAM_NAME}.default.heart_gold`
# MAGIC > 4. Add **General Instructions**:
# MAGIC >    ```
# MAGIC >    You are a clinical data analyst. The heart_silver table has patient-level records
# MAGIC >    with age, sex, cp (chest pain type: 0=typical angina, 1=atypical, 2=non-anginal,
# MAGIC >    3=asymptomatic), trestbps (resting BP), chol (cholesterol), thalach (max heart rate),
# MAGIC >    target (1=heart disease, 0=healthy). heart_gold is aggregated by age_group and diagnosis.
# MAGIC >    Always include row counts. Round to 1 decimal.
# MAGIC >    ```
# MAGIC > 5. Add **3+ Sample Questions**:
# MAGIC >    - "How many patients have heart disease?"
# MAGIC >    - "What is the average cholesterol by age group?"
# MAGIC >    - "Which chest pain type is most common among heart disease patients?"
# MAGIC > 6. Click **Save** — copy the **Genie Space ID** from the URL
# MAGIC
# MAGIC > ### After creating the Genie Space, paste the ID below and test it:

# COMMAND ----------

GENIE_SPACE_ID = ""  # <-- PASTE YOUR GENIE SPACE ID HERE

# Save to config table for scoring
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.default.agent_config
    (resource_type STRING, resource_id STRING, resource_name STRING, created_at TIMESTAMP)
""")
if GENIE_SPACE_ID:
    spark.sql(f"""
        MERGE INTO {CATALOG}.default.agent_config AS t
        USING (SELECT 'genie_space' AS resource_type, '{GENIE_SPACE_ID}' AS resource_id,
               '{TEAM_NAME}_clinical_analytics' AS resource_name, current_timestamp() AS created_at) AS s
        ON t.resource_type = s.resource_type
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Genie Space ID saved: {GENIE_SPACE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Your Genie Space via API

# COMMAND ----------

import requests, time, json

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def ask_genie(question: str) -> str:
    """Send a question to the Genie Space and return the answer."""
    if not GENIE_SPACE_ID:
        return "No Genie Space ID configured"
    resp = requests.post(
        f"https://{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation",
        headers=headers, json={"content": question}
    ).json()
    conv_id, msg_id = resp["conversation_id"], resp["message_id"]
    for _ in range(20):
        time.sleep(3)
        r = requests.get(
            f"https://{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conv_id}/messages/{msg_id}",
            headers=headers
        ).json()
        if r.get("status") == "COMPLETED":
            for att in r.get("attachments", []):
                if att.get("text", {}).get("content"):
                    return att["text"]["content"]
            return "Genie returned no text"
    return "Genie timed out"

if GENIE_SPACE_ID:
    print("Testing Genie Space...")
    print(f"  Q: How many patients have heart disease?")
    print(f"  A: {ask_genie('How many patients have heart disease?')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 4: Create a Knowledge Assistant (7 pts)
# MAGIC
# MAGIC > Create a **Knowledge Assistant** that answers questions about clinical documentation.
# MAGIC >
# MAGIC > ### Instructions (in the Databricks UI):
# MAGIC >
# MAGIC > 1. Go to **AI/BI → Agent Evaluation → Create → Knowledge Assistant**
# MAGIC > 2. Name it: **`{TEAM_NAME}_clinical_docs`**
# MAGIC > 3. **Volume path**: `/Volumes/dataops_olympics/default/raw_data/clinical_docs`
# MAGIC > 4. **Description**: "Answers questions about hospital clinical protocols, patient intake
# MAGIC >    procedures, data quality standards, and cardiology guidelines."
# MAGIC > 5. **Instructions**: "Always cite the specific document when answering. If you are unsure,
# MAGIC >    say so. Do not provide medical advice."
# MAGIC > 6. Click **Create** — wait for status to become **ONLINE** (2–5 min)
# MAGIC > 7. Copy the **Tile ID** from the URL or settings
# MAGIC
# MAGIC > ### After creating the KA, paste the Tile ID below:

# COMMAND ----------

KA_TILE_ID = ""  # <-- PASTE YOUR KA TILE ID HERE

if KA_TILE_ID:
    spark.sql(f"""
        MERGE INTO {CATALOG}.default.agent_config AS t
        USING (SELECT 'knowledge_assistant' AS resource_type, '{KA_TILE_ID}' AS resource_id,
               '{TEAM_NAME}_clinical_docs' AS resource_name, current_timestamp() AS created_at) AS s
        ON t.resource_type = s.resource_type
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"KA Tile ID saved: {KA_TILE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 5: Build the Supervisor Agent (10 pts)
# MAGIC
# MAGIC > Wire everything together into a **Supervisor Agent** that routes questions.
# MAGIC >
# MAGIC > ### Instructions (in the Databricks UI):
# MAGIC >
# MAGIC > 1. Go to **AI/BI → Agent Evaluation → Create → Supervisor Agent**
# MAGIC > 2. Name it: **`{TEAM_NAME}_clinical_supervisor`**
# MAGIC > 3. **Add agents:**
# MAGIC >    - **clinical_analytics** → Your Genie Space (select it from the dropdown)
# MAGIC >      - Description: "SQL analytics on patient heart data — heart disease rates, cholesterol stats, demographic breakdowns"
# MAGIC >    - **clinical_docs** → Your Knowledge Assistant
# MAGIC >      - Description: "Answers questions about clinical protocols, intake procedures, and data quality standards from hospital documents"
# MAGIC >    - **risk_assessment** → Your UC Function `{CATALOG}.default.patient_risk_assessment`
# MAGIC >      - Description: "Classifies cardiovascular risk as LOW, MEDIUM, or HIGH given patient age, cholesterol, blood pressure, and heart rate"
# MAGIC > 4. **Add routing instructions** (copy this template):
# MAGIC >    ```
# MAGIC >    Route queries as follows:
# MAGIC >    - Data/analytics questions (counts, averages, breakdowns, comparisons) → clinical_analytics
# MAGIC >    - Policy/procedure/protocol questions → clinical_docs
# MAGIC >    - Risk assessment requests (classify risk, assess patient) → risk_assessment
# MAGIC >
# MAGIC >    If the query needs multiple agents, chain them:
# MAGIC >    1. First gather data (clinical_analytics)
# MAGIC >    2. Then provide context (clinical_docs) or assess risk (risk_assessment)
# MAGIC >
# MAGIC >    Always be factual and cite the data source.
# MAGIC >    ```
# MAGIC > 5. Click **Create** — wait for status to become **ONLINE** (2–5 min)

# COMMAND ----------

MAS_TILE_ID = ""  # <-- PASTE YOUR SUPERVISOR TILE ID HERE

if MAS_TILE_ID:
    spark.sql(f"""
        MERGE INTO {CATALOG}.default.agent_config AS t
        USING (SELECT 'supervisor_agent' AS resource_type, '{MAS_TILE_ID}' AS resource_id,
               '{TEAM_NAME}_clinical_supervisor' AS resource_name, current_timestamp() AS created_at) AS s
        ON t.resource_type = s.resource_type
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Supervisor Tile ID saved: {MAS_TILE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 6: Test Your Supervisor (5 pts)
# MAGIC
# MAGIC > Send 5 test prompts to your Supervisor's serving endpoint and save the results.
# MAGIC > You can also test in the **AI Playground** (select your Supervisor endpoint).

# COMMAND ----------

MAS_ENDPOINT = f"agents-{MAS_TILE_ID}" if MAS_TILE_ID else ""

test_prompts = [
    "How many patients in the dataset have heart disease?",
    "What does the patient intake policy say about data validation?",
    "Assess the cardiovascular risk for a 62-year-old with cholesterol 280, BP 155, and heart rate 140.",
    "Which age group has the highest heart disease rate?",
    "What are the data quality standards for clinical measurements?",
]

results = []
for i, prompt in enumerate(test_prompts, 1):
    print(f"\n--- Test {i} ---")
    print(f"Q: {prompt}")
    if not MAS_ENDPOINT:
        print("  [SKIP] No Supervisor endpoint configured")
        results.append({"prompt_id": i, "question": prompt, "answer": "NOT_CONFIGURED", "status": "skipped"})
        continue
    try:
        resp = requests.post(
            f"https://{host}/serving-endpoints/{MAS_ENDPOINT}/invocations",
            headers=headers,
            json={"messages": [{"role": "user", "content": prompt}]},
            timeout=60,
        ).json()
        answer = resp.get("choices", [{}])[0].get("message", {}).get("content", str(resp))
        print(f"A: {answer[:500]}")
        results.append({"prompt_id": i, "question": prompt, "answer": answer[:2000], "status": "success"})
    except Exception as e:
        print(f"  ERROR: {e}")
        results.append({"prompt_id": i, "question": prompt, "answer": str(e)[:500], "status": "error"})

# COMMAND ----------

# Save test results for scoring
import pandas as pd
from datetime import datetime

if results:
    df = pd.DataFrame(results)
    df["team"] = TEAM_NAME
    df["tested_at"] = datetime.now().isoformat()
    spark.createDataFrame(df).write.format("delta").mode("overwrite").saveAsTable(
        f"{CATALOG}.default.supervisor_test_results"
    )
    print(f"Saved {len(results)} test results to {CATALOG}.default.supervisor_test_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bonus Challenges
# MAGIC
# MAGIC ### Bonus 1: MCP Server Integration (+5 pts)
# MAGIC
# MAGIC > Build an **MCP server** as a Databricks App and connect it to your Supervisor.
# MAGIC > A scaffold is provided in `event4_agent_bricks/mcp_server/`.
# MAGIC >
# MAGIC > **Steps:**
# MAGIC > 1. Deploy the MCP server app (FastAPI with `/mcp` endpoint)
# MAGIC > 2. Create a **UC HTTP Connection** pointing to the app:
# MAGIC >    ```sql
# MAGIC >    CREATE CONNECTION {TEAM_NAME}_mcp_server TYPE HTTP
# MAGIC >    OPTIONS (
# MAGIC >      host 'https://{TEAM_NAME}-mcp.{workspace}.databricksapps.com',
# MAGIC >      port '443',
# MAGIC >      base_path '/mcp',
# MAGIC >      is_mcp_connection 'true'
# MAGIC >    );
# MAGIC >    ```
# MAGIC > 3. Add the connection as an agent in your Supervisor:
# MAGIC >    - Name: `clinical_actions`
# MAGIC >    - Connection: `{TEAM_NAME}_mcp_server`
# MAGIC >    - Description: "Performs clinical actions: approve referrals, schedule follow-ups"
# MAGIC
# MAGIC ### Bonus 2: Advanced Routing Instructions (+3 pts)
# MAGIC
# MAGIC > Write detailed routing instructions (200+ chars) with explicit rules for:
# MAGIC > - When to use each agent
# MAGIC > - How to chain agents for complex queries
# MAGIC > - Guardrails (no medical advice, cite sources)
# MAGIC >
# MAGIC > *Better instructions = better routing = better agent performance!*
# MAGIC
# MAGIC ### Bonus 3: DSPy Prompt Optimization (+3 pts)
# MAGIC
# MAGIC > Use **[DSPy](https://dspy.ai/)** to *automatically optimize* the prompt in your
# MAGIC > `patient_risk_assessment` UC function. Instead of hand-crafting the prompt,
# MAGIC > let DSPy find the best one by evaluating against labeled examples.
# MAGIC >
# MAGIC > **What to do:**
# MAGIC > 1. Define a DSPy signature for risk classification
# MAGIC > 2. Create a few labeled examples (training data)
# MAGIC > 3. Use a DSPy optimizer (e.g., `MIPROv2` or `BootstrapFewShot`) to find the best prompt
# MAGIC > 4. Update your UC function with the optimized prompt
# MAGIC > 5. Save the optimized prompt to `{CATALOG}.default.dspy_optimized_prompt`
# MAGIC >
# MAGIC > *DSPy treats prompts as programs — it optimizes them the way you'd tune model hyperparameters!*
# MAGIC >
# MAGIC > See the example below to get started.

# COMMAND ----------

# MAGIC %md
# MAGIC #### DSPy Example: Optimizing Risk Classification Prompt

# COMMAND ----------

# MAGIC %pip install dspy -q

# COMMAND ----------

# Restart Python after pip install
dbutils.library.restartPython()

# COMMAND ----------

# === DSPy Prompt Optimization Example ===
# This shows how to use DSPy to find a better prompt for risk classification.
# Instead of hand-crafting, DSPy searches for the optimal prompt automatically.

import dspy

TEAM_NAME = "team_XX"  # <-- re-set after restartPython
CATALOG = TEAM_NAME

# Connect DSPy to the Databricks Foundation Model API
lm = dspy.LM("databricks/databricks-meta-llama-3-3-70b-instruct")
dspy.configure(lm=lm)


class RiskClassification(dspy.Signature):
    """Classify cardiovascular risk based on patient vitals."""
    age: int = dspy.InputField(desc="Patient age in years")
    cholesterol: float = dspy.InputField(desc="Serum cholesterol in mg/dL")
    blood_pressure: float = dspy.InputField(desc="Resting blood pressure in mmHg")
    heart_rate: float = dspy.InputField(desc="Max heart rate achieved in bpm")
    risk_level: str = dspy.OutputField(desc="Exactly one of: LOW, MEDIUM, or HIGH")
    explanation: str = dspy.OutputField(desc="One sentence clinical explanation")


classify_risk = dspy.ChainOfThought(RiskClassification)

# Training examples with known labels
trainset = [
    dspy.Example(age=35, cholesterol=180, blood_pressure=120, heart_rate=160,
                 risk_level="LOW", explanation="Young patient with normal vitals.").with_inputs("age", "cholesterol", "blood_pressure", "heart_rate"),
    dspy.Example(age=52, cholesterol=230, blood_pressure=138, heart_rate=145,
                 risk_level="MEDIUM", explanation="Middle-aged with borderline cholesterol and BP.").with_inputs("age", "cholesterol", "blood_pressure", "heart_rate"),
    dspy.Example(age=68, cholesterol=290, blood_pressure=165, heart_rate=130,
                 risk_level="HIGH", explanation="Elderly with elevated cholesterol and hypertension.").with_inputs("age", "cholesterol", "blood_pressure", "heart_rate"),
    dspy.Example(age=45, cholesterol=210, blood_pressure=125, heart_rate=155,
                 risk_level="LOW", explanation="Normal range vitals for age.").with_inputs("age", "cholesterol", "blood_pressure", "heart_rate"),
    dspy.Example(age=61, cholesterol=260, blood_pressure=155, heart_rate=140,
                 risk_level="HIGH", explanation="Over 60 with high cholesterol and BP above 150.").with_inputs("age", "cholesterol", "blood_pressure", "heart_rate"),
]


def risk_metric(example, pred, trace=None):
    return pred.risk_level.strip().upper() == example.risk_level.strip().upper()


# Quick test before optimization
result = classify_risk(age=62, cholesterol=280, blood_pressure=155, heart_rate=140)
print(f"Before optimization: {result.risk_level} — {result.explanation}")

# Optimize with BootstrapFewShot (fast, good for small datasets)
optimizer = dspy.BootstrapFewShot(metric=risk_metric, max_bootstrapped_demos=3)
optimized_classifier = optimizer.compile(classify_risk, trainset=trainset)

# Test after optimization
result2 = optimized_classifier(age=62, cholesterol=280, blood_pressure=155, heart_rate=140)
print(f"After optimization:  {result2.risk_level} — {result2.explanation}")

# Save optimized prompt for scoring
optimized_prompt = lm.history[-1]["prompt"] if hasattr(lm, "history") else str(optimized_classifier)
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.dspy_optimized_prompt AS
    SELECT
        'patient_risk_assessment' AS function_name,
        '{optimized_prompt[:2000].replace("'", "''")}' AS optimized_prompt,
        current_timestamp() AS created_at
""")
print(f"\nOptimized prompt saved to {CATALOG}.default.dspy_optimized_prompt")
