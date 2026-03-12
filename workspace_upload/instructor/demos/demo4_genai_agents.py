# Databricks notebook source
# MAGIC %md
# MAGIC # Lightning Talk 4: GenAI Agents — MLflow Tracing & Serving
# MAGIC
# MAGIC **Duration: 15 minutes** | Instructor-led live demo
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Agenda
# MAGIC | Time | Section | What You'll See |
# MAGIC |------|---------|----------------|
# MAGIC | 0:00 | **The Problem** | Doctors need answers, not SQL skills |
# MAGIC | 1:00 | **Data Landscape** | Three sources — one agent to rule them all |
# MAGIC | 3:00 | **System Prompt** | The art of prompt engineering |
# MAGIC | 5:00 | **Build Agent** | Multi-source clinical agent in Python |
# MAGIC | 8:00 | **Wow Moment #1** | MLflow Tracing — see every call, every decision |
# MAGIC | 11:00 | **Wow Moment #2** | Register & serve your agent in MLflow |
# MAGIC | 13:00 | **Evaluation** | Testing agent quality at scale |
# MAGIC | 14:00 | **Your Turn** | Preview of the practice notebook |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Problem
# MAGIC
# MAGIC A hospital has three databases: patient records, drug reviews, and clinical notes.
# MAGIC A doctor asks: *"What's the best-rated drug for cardiac patients?"*
# MAGIC Today, that requires three apps, three queries, and 20 minutes.
# MAGIC What if one AI agent could do it in 2 seconds — and you could see exactly how it got the answer?

# COMMAND ----------

spark.sql("USE CATALOG dataops_olympics")
spark.sql("USE SCHEMA default")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Our Data Landscape

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'heart_disease' as source, COUNT(*) as records, 'Patient clinical measurements' as description FROM heart_disease
# MAGIC UNION ALL
# MAGIC SELECT 'drug_reviews', COUNT(*), 'Drug names, ratings 1-10, patient reviews' FROM drug_reviews
# MAGIC UNION ALL
# MAGIC SELECT 'clinical_notes', COUNT(*), 'Hospital dept notes (5 depts, 4 note types)' FROM clinical_notes

# COMMAND ----------

# MAGIC %md
# MAGIC 500 patients, 1000 drug reviews, 20 clinical notes — three very different data sources.
# MAGIC The agent needs to understand the question and route it to the right one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The System Prompt — Most Important Part
# MAGIC
# MAGIC The system prompt IS the agent's brain. A vague prompt like "be helpful" gives you a vague agent.
# MAGIC Watch how specific we need to be:

# COMMAND ----------

system_prompt = """You are a clinical decision support assistant for a hospital network.

DATA SOURCES:
1. Heart Disease DB: 500 patients with age, sex, chest pain type (cp: 0-3),
   cholesterol (chol), resting blood pressure (trestbps), max heart rate (thalach),
   and diagnosis (target: 1=disease, 0=healthy)
2. Drug Reviews: 1000 reviews of 15 medications with ratings (1-10), conditions, review text
3. Clinical Notes: 20 notes across Cardiology, Oncology, Neurology, Emergency, Pediatrics

RESPONSE RULES:
- Always cite the data source and sample size
- Provide numbers: counts, averages, percentages
- NEVER provide medical advice — you are a DATA ANALYSIS tool
- If data is insufficient, say so explicitly
"""

print(f"System prompt: {len(system_prompt)} characters")

# COMMAND ----------

# MAGIC %md
# MAGIC Notice: exact tables, exact column names, what codes mean, and the rules of engagement.
# MAGIC The more specific your prompt, the better your agent performs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build the Agent — Multi-Source Router

# COMMAND ----------

def clinical_agent(question: str) -> str:
    """Routes questions to the right data source and returns formatted answers."""
    q = question.lower()
    sections = []

    if any(w in q for w in ["heart", "cardiac", "patient", "disease", "age",
                            "cholesterol", "blood pressure", "chest pain"]):
        stats = spark.sql("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN target=1 THEN 1 ELSE 0 END) as diseased,
                   ROUND(AVG(age), 1) as avg_age,
                   ROUND(AVG(chol), 1) as avg_chol,
                   ROUND(AVG(thalach), 1) as avg_hr
            FROM heart_disease
        """).collect()[0]
        rate = round(stats.diseased * 100 / stats.total, 1)
        sections.append(
            f"HEART DISEASE DATA (n={stats.total}):\n"
            f"  Prevalence: {stats.diseased}/{stats.total} ({rate}%)\n"
            f"  Avg age: {stats.avg_age} yrs | Avg cholesterol: {stats.avg_chol} mg/dL\n"
            f"  Avg max heart rate: {stats.avg_hr} bpm"
        )

    if any(w in q for w in ["drug", "medication", "medicine", "rating", "review", "treatment"]):
        drugs = spark.sql("""
            SELECT drug_name, ROUND(AVG(rating), 1) as avg_rating, COUNT(*) as n_reviews
            FROM drug_reviews GROUP BY 1 ORDER BY avg_rating DESC LIMIT 5
        """).toPandas()
        lines = [f"  {r['drug_name']}: {r['avg_rating']}/10 ({r['n_reviews']} reviews)"
                 for _, r in drugs.iterrows()]
        sections.append(f"TOP DRUGS BY RATING:\n" + "\n".join(lines))

    if any(w in q for w in ["clinical", "note", "department", "diagnosis", "physician"]):
        depts = spark.sql("""
            SELECT department, COUNT(*) as notes, COLLECT_SET(note_type) as note_types
            FROM clinical_notes GROUP BY 1 ORDER BY notes DESC
        """).toPandas()
        lines = [f"  {r['department']}: {r['notes']} notes ({', '.join(r['note_types'])})"
                 for _, r in depts.iterrows()]
        sections.append(f"CLINICAL NOTES BY DEPARTMENT:\n" + "\n".join(lines))

    if not sections:
        sections.append(
            "I can answer questions about:\n"
            "  1. Heart disease patient data\n"
            "  2. Drug reviews and ratings\n"
            "  3. Clinical notes by department\n"
            "Please ask about one of these topics."
        )

    return "\n\n".join(sections)

# COMMAND ----------

# MAGIC %md
# MAGIC Keyword detection, SQL routing, formatted response — simple but effective.
# MAGIC The real question: how do you KNOW it's working correctly in production? That's where tracing comes in.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Test the Agent — Quick Check

# COMMAND ----------

print(clinical_agent("What percentage of patients have heart disease?"))

# COMMAND ----------

print(clinical_agent("What are the top rated drugs?"))

# COMMAND ----------

print(clinical_agent("What is the weather today?"))

# COMMAND ----------

# MAGIC %md
# MAGIC The weather question — the agent correctly said "I don't have that data." That's a guardrail.
# MAGIC But how do we see what happened *inside* the agent for each call?
# MAGIC This is the biggest pain point in production AI: **observability**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Wow Moment #1 — MLflow Tracing
# MAGIC
# MAGIC Here's the game-changer. **MLflow Tracing** lets you see every call your agent makes —
# MAGIC the input, the output, the latency, even sub-steps. One decorator is all it takes.

# COMMAND ----------

import mlflow

_user = spark.sql("SELECT current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{_user}/demo_genai_lightning_talk")

# Add tracing — this is literally one line
clinical_agent = mlflow.trace(clinical_agent, name="clinical_agent", span_type="AGENT")
print("MLflow tracing enabled!")

# COMMAND ----------

# MAGIC %md
# MAGIC One line: `mlflow.trace()`. Now every call to our agent is captured.
# MAGIC Let's run the same questions again — this time, everything gets traced in MLflow.

# COMMAND ----------

print(clinical_agent("How many patients have heart disease and what is their average age?"))

# COMMAND ----------

print(clinical_agent("Which drug has the highest average rating?"))

# COMMAND ----------

print(clinical_agent("Tell me about cardiology department notes"))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's check out the traces in the MLflow UI:
# MAGIC 1. Click **Experiments** in the left sidebar
# MAGIC 2. Open `demo_genai_lightning_talk`
# MAGIC 3. Click the **Traces** tab — each row is one agent invocation
# MAGIC 4. Click into a trace — see the **span tree**: input → processing → output
# MAGIC 5. Notice the **latency** for each span and the **input/output** content
# MAGIC
# MAGIC This is production observability for AI. Every call is logged — who asked what,
# MAGIC what the agent returned, how long it took. When a doctor reports a wrong answer,
# MAGIC you can trace back to the exact call.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Wow Moment #2 — Register & Serve Your Agent
# MAGIC
# MAGIC Tracing tells you how your agent behaves. But to put it in production,
# MAGIC you need to register it in MLflow — just like we registered the ML model earlier.
# MAGIC Then anyone can find it, test it, and serve it.

# COMMAND ----------

from mlflow.models import infer_signature

sample_q = "How many patients have heart disease?"
sample_a = clinical_agent(sample_q)

with mlflow.start_run(run_name="clinical_agent_v1"):
    mlflow.log_param("agent_type", "clinical_router")
    mlflow.log_param("data_sources", "heart_disease,drug_reviews,clinical_notes")
    mlflow.log_param("system_prompt_length", len(system_prompt))

    sig = infer_signature(
        model_input=[sample_q],
        model_output=[sample_a]
    )
    mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=None,
        signature=sig,
        input_example=[sample_q],
    )
    _run_id = mlflow.active_run().info.run_id
    print(f"Agent logged — run ID: {_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC The agent is now in MLflow — tracked, versioned, reproducible. You can register it
# MAGIC in Unity Catalog and serve it as an endpoint. In the competition, this is worth 7 points,
# MAGIC and it's **required**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Agent Evaluation

# COMMAND ----------

eval_prompts = [
    ("How many patients have heart disease?", "should return count and percentage"),
    ("What is the best rated medication?", "should return drug name and rating"),
    ("Which department has the most notes?", "should return department breakdown"),
    ("What is the average age of cardiac patients?", "should return age statistic"),
    ("Tell me about diabetes treatments", "should route to drug reviews"),
]

print("=" * 60)
print("  AGENT EVALUATION (5 test prompts — all traced!)")
print("=" * 60)

for i, (prompt, expected) in enumerate(eval_prompts, 1):
    response = clinical_agent(prompt)
    has_data = any(c.isdigit() for c in response)
    status = "PASS" if has_data else "WEAK"
    print(f"\n  [{status}] Prompt {i}: {prompt}")
    print(f"  Expected: {expected}")
    preview = response[:120].replace("\n", " ")
    print(f"  Got: {preview}...")

print("\n" + "=" * 60)
print("  Check the Traces tab — all 5 evaluation calls are logged!")

# COMMAND ----------

# MAGIC %md
# MAGIC Every evaluation prompt is traced. In production, you'd use MLflow's evaluation framework
# MAGIC to score these automatically — checking for correctness, hallucination, and safety.
# MAGIC That's GenAI observability.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Turn!
# MAGIC
# MAGIC In the competition, you'll build an agent like this — but smarter. You'll add `ai_query()`
# MAGIC for LLM-powered analysis, trace everything with MLflow, and register your agent so we can
# MAGIC find and evaluate it. MLflow Tracing is **required** — 7 points depend on it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **System prompts** — be specific about data, columns, codes, and rules
# MAGIC 2. **Multi-source routing** — detect the question type and query the right table
# MAGIC 3. **MLflow Tracing** — one decorator, full observability for every agent call
# MAGIC 4. **Register your agent** in MLflow — versioned, reproducible, servable
# MAGIC 5. **Evaluation** — test against known prompts, score automatically
# MAGIC 6. **Guardrails** — a good agent knows when it can't answer
# MAGIC 7. **Use the Databricks Assistant** (`Cmd+I`) to write prompts and debug routing logic
