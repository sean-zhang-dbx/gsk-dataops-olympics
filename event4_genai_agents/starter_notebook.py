# Databricks notebook source
# MAGIC %md
# MAGIC # Event 4: GenAI / Agents — Clinical AI Challenge
# MAGIC
# MAGIC ## Build the Smartest Clinical AI Agent
# MAGIC **Time: 20 minutes** | **Max Points: 40 (+8 bonus)**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Scenario
# MAGIC
# MAGIC > The hospital wants a **clinical AI agent** that can answer questions about
# MAGIC > patient data, drug information, and clinical notes using natural language.
# MAGIC >
# MAGIC > Your agent must:
# MAGIC > 1. Route questions to the right data source
# MAGIC > 2. Use `ai_query()` for intelligent analysis
# MAGIC > 3. Handle multiple question types gracefully
# MAGIC > 4. Be evaluated against 5 test prompts
# MAGIC
# MAGIC ### Scoring Overview
# MAGIC
# MAGIC | Category | Points |
# MAGIC |----------|--------|
# MAGIC | Data Exploration | 3 |
# MAGIC | System Prompt | 5 |
# MAGIC | Agent Function (routing + SQL) | 12 |
# MAGIC | AI Functions (`ai_query`) | 10 |
# MAGIC | Test Prompt Evaluation | 10 |
# MAGIC | **Total** | **40** |
# MAGIC | Bonus: Semantic Search, Safety, Multi-step | up to 8 |
# MAGIC
# MAGIC ### Data Sources
# MAGIC
# MAGIC | Table | Location | Rows | Description |
# MAGIC |-------|----------|------|-------------|
# MAGIC | `heart_silver` | `{TEAM_NAME}.default` | ~488 | Patient clinical data |
# MAGIC | `heart_gold` | `{TEAM_NAME}.default` | ~8 | Aggregated metrics |
# MAGIC | `drug_reviews` | `dataops_olympics.default` | 1,000 | Drug names, ratings, conditions |
# MAGIC | `clinical_notes` | `dataops_olympics.default` | 20 | Hospital department notes |
# MAGIC
# MAGIC > **Vibe Coding:** Use **Databricks Assistant** (`Cmd+I`) for prompt engineering!

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
# MAGIC ## Step 1: Explore Data Sources (3 pts)
# MAGIC
# MAGIC > Display row counts and samples from all 4 tables.

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

display(spark.table(f"{SHARED_CATALOG}.default.drug_reviews").limit(5))

# COMMAND ----------

display(spark.table(f"{SHARED_CATALOG}.default.clinical_notes").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: System Prompt (5 pts)
# MAGIC
# MAGIC > Define a system prompt for your clinical AI agent. It should describe:
# MAGIC > - The agent's role and capabilities
# MAGIC > - Available data sources and what each contains
# MAGIC > - Column semantics (target, cp, sex, thalach, etc.)
# MAGIC > - Behavioral rules (be factual, cite data, don't give medical advice)
# MAGIC >
# MAGIC > Minimum 200 characters. More detailed = better agent performance.

# COMMAND ----------

# YOUR CODE HERE — define system_prompt
system_prompt = """
You are a clinical data analyst AI for a hospital. You have access to:
... (describe your data sources and rules)
"""
print(f"System prompt length: {len(system_prompt)} chars")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Build Your Agent Function (12 pts)
# MAGIC
# MAGIC > Create a `clinical_agent(question: str) -> str` function that:
# MAGIC >
# MAGIC > 1. **Detects the topic** from the question keywords
# MAGIC > 2. **Routes to the right table** and runs SQL:
# MAGIC >    - Heart/patient/cardiac → `{CATALOG}.default.heart_silver` or `heart_gold`
# MAGIC >    - Drug/medication/rating → `{SHARED_CATALOG}.default.drug_reviews`
# MAGIC >    - Clinical/note/department → `{SHARED_CATALOG}.default.clinical_notes`
# MAGIC > 3. **Returns a formatted answer** with data
# MAGIC > 4. **Handles unknown questions** gracefully
# MAGIC >
# MAGIC > Use Databricks Assistant to help build this!

# COMMAND ----------

# YOUR CODE HERE — define clinical_agent function
# Prompt: "Create a clinical_agent function that routes questions to heart_silver,
# drug_reviews, or clinical_notes based on keywords, runs SQL, and returns formatted answers"

def clinical_agent(question: str) -> str:
    """Route clinical questions to the right data source and return answers."""
    # YOUR IMPLEMENTATION HERE
    return "Not implemented yet"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: AI Functions with `ai_query()` (10 pts)
# MAGIC
# MAGIC > Enhance your agent with `ai_query()` to generate intelligent analysis.
# MAGIC >
# MAGIC > Create a table `heart_ai_insights` that uses an LLM to analyze each Gold cohort:
# MAGIC >
# MAGIC > ```sql
# MAGIC > CREATE OR REPLACE TABLE heart_ai_insights AS
# MAGIC > SELECT *,
# MAGIC >   ai_query(
# MAGIC >     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC >     CONCAT(
# MAGIC >       'You are a clinical analyst. Analyze this patient cohort and provide ',
# MAGIC >       'a 2-sentence clinical insight: ',
# MAGIC >       age_group, ' patients, ', diagnosis, ', n=', patient_count,
# MAGIC >       ', avg cholesterol=', avg_cholesterol, ' mg/dL',
# MAGIC >       ', avg BP=', avg_blood_pressure, ' mmHg',
# MAGIC >       ', avg max HR=', avg_max_heart_rate, ' bpm'
# MAGIC >     )
# MAGIC >   ) AS clinical_insight
# MAGIC > FROM heart_gold
# MAGIC > ```
# MAGIC >
# MAGIC > Also create a `drug_ai_summary` table summarizing the top drugs:
# MAGIC >
# MAGIC > ```sql
# MAGIC > CREATE OR REPLACE TABLE drug_ai_summary AS
# MAGIC > SELECT drug_name, ROUND(AVG(rating), 1) AS avg_rating, COUNT(*) AS reviews,
# MAGIC >   ai_query(
# MAGIC >     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC >     CONCAT('Summarize this drug in one sentence: ', drug_name,
# MAGIC >            ', avg rating ', ROUND(AVG(rating), 1), '/10, ',
# MAGIC >            COUNT(*), ' reviews')
# MAGIC >   ) AS ai_summary
# MAGIC > FROM dataops_olympics.default.drug_reviews
# MAGIC > GROUP BY drug_name
# MAGIC > ORDER BY avg_rating DESC
# MAGIC > LIMIT 5
# MAGIC > ```

# COMMAND ----------

# YOUR CODE HERE — create heart_ai_insights using ai_query()


# COMMAND ----------

# YOUR CODE HERE — create drug_ai_summary using ai_query()


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Test Your Agent (10 pts)
# MAGIC
# MAGIC Your agent will be evaluated on these 5 test prompts.
# MAGIC Each correct, data-backed response earns 2 pts.

# COMMAND ----------

test_prompts = [
    "How many patients in the dataset have heart disease?",
    "What is the most common chest pain type among heart disease patients?",
    "Which drug has the highest average rating?",
    "What department has the most clinical notes?",
    "Which age group has the highest heart disease rate?",
]

print("=" * 60)
print(f"  AGENT EVALUATION — {TEAM_NAME}")
print("=" * 60)

agent_responses = {}
for i, prompt in enumerate(test_prompts, 1):
    print(f"\n--- Test {i} ---")
    print(f"Q: {prompt}")
    try:
        response = clinical_agent(prompt)
        agent_responses[f"T{i}"] = response
        print(f"A: {response[:500]}")
    except Exception as e:
        agent_responses[f"T{i}"] = f"ERROR: {e}"
        print(f"ERROR: {e}")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Bonus Challenges
# MAGIC
# MAGIC ### Bonus 1: Semantic Search over Clinical Notes (+3 pts)
# MAGIC
# MAGIC > Use sentence-transformers + ChromaDB to build a semantic search over clinical notes.
# MAGIC > Save the search function as `search_notes(query, k=3)`.
# MAGIC >
# MAGIC > ```python
# MAGIC > # pip install chromadb sentence-transformers
# MAGIC > from sentence_transformers import SentenceTransformer
# MAGIC > import chromadb
# MAGIC >
# MAGIC > model = SentenceTransformer('all-MiniLM-L6-v2')
# MAGIC > client = chromadb.Client()
# MAGIC > collection = client.create_collection("clinical_notes")
# MAGIC >
# MAGIC > notes = spark.table(f"{SHARED_CATALOG}.default.clinical_notes").toPandas()
# MAGIC > collection.add(
# MAGIC >     documents=notes["note_text"].tolist(),
# MAGIC >     ids=[str(i) for i in range(len(notes))],
# MAGIC >     metadatas=[{"dept": d} for d in notes["department"].tolist()]
# MAGIC > )
# MAGIC >
# MAGIC > def search_notes(query, k=3):
# MAGIC >     results = collection.query(query_texts=[query], n_results=k)
# MAGIC >     return results["documents"][0]
# MAGIC > ```
# MAGIC
# MAGIC ### Bonus 2: Safety Guardrails (+2 pts)
# MAGIC
# MAGIC > Add safety checks to your agent:
# MAGIC > - Refuse to give specific medical advice ("I cannot provide medical advice...")
# MAGIC > - Add confidence scores to responses
# MAGIC > - Detect and handle off-topic questions
# MAGIC > - Log all queries to a `{CATALOG}.default.agent_audit_log` table
# MAGIC
# MAGIC ### Bonus 3: Multi-Step Reasoning (+3 pts)
# MAGIC
# MAGIC > Handle complex questions that require querying multiple tables:
# MAGIC > - "Compare heart disease rates with the top-rated drug for cardiac conditions"
# MAGIC > - "Which department's notes mention the most common chest pain type?"
# MAGIC > - Break down into sub-queries, combine results, return a coherent answer
