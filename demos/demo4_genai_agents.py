# Databricks notebook source
# MAGIC %md
# MAGIC # Lightning Talk 4: GenAI / Agents Demo
# MAGIC
# MAGIC **For organizers only — this is the 15-minute demo shown before Event 4.**
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Building an AI agent that answers clinical questions
# MAGIC 2. Using Genie as a tool for structured data queries
# MAGIC 3. Semantic search over clinical notes
# MAGIC 4. Agent evaluation against test prompts
# MAGIC
# MAGIC ## What to show live (in Databricks UI):
# MAGIC - AgentBricks interface for building agents
# MAGIC - Connecting Genie as a tool
# MAGIC - Running evaluation

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Explore Available Data

# COMMAND ----------

# Clinical notes
display(spark.sql("SELECT patient_id, department, note_type, LEFT(text, 100) as preview FROM clinical_notes LIMIT 10"))

# COMMAND ----------

# Drug reviews summary
display(spark.sql("""
    SELECT drug_name, condition, COUNT(*) as reviews, ROUND(AVG(rating), 1) as avg_rating
    FROM drug_reviews
    GROUP BY drug_name, condition
    ORDER BY reviews DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build a Clinical Agent

# COMMAND ----------

# System prompt — this is the key to a good agent
system_prompt = """You are a clinical decision support assistant for a hospital system.

You have access to three data sources:
1. Heart Disease Database: 500 patients with clinical measurements (age, sex, chest pain type,
   cholesterol, blood pressure, max heart rate, etc.) and heart disease diagnosis (target: 1=disease, 0=healthy)
2. Drug Reviews: 1000 patient reviews of 15 medications with ratings (1-10), conditions treated, and review text
3. Clinical Notes: 20 clinical notes from various hospital departments

When answering questions:
- Always cite specific data when available (e.g., "Based on 500 patient records...")
- Provide quantitative answers when possible (counts, averages, percentages)
- Flag any limitations in the data
- Suggest follow-up actions when clinically appropriate
- Never provide actual medical advice — you are a data analysis tool, not a doctor
"""

print(f"System prompt: {len(system_prompt)} characters")

# COMMAND ----------

def demo_clinical_agent(question: str) -> str:
    """Demo clinical agent with multi-source data access."""

    q_lower = question.lower()
    response_parts = []

    # Heart disease data queries
    if any(w in q_lower for w in ["heart", "cardiac", "patient", "disease", "age", "cholesterol"]):
        stats = spark.sql("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) as diseased,
                ROUND(AVG(age), 1) as avg_age,
                ROUND(AVG(chol), 1) as avg_chol,
                ROUND(AVG(thalach), 1) as avg_hr
            FROM heart_disease
        """).collect()[0]

        response_parts.append(
            f"Heart Disease Database ({stats.total} patients):\n"
            f"  Disease prevalence: {stats.diseased}/{stats.total} ({stats.diseased*100/stats.total:.1f}%)\n"
            f"  Average age: {stats.avg_age} years\n"
            f"  Average cholesterol: {stats.avg_chol} mg/dl\n"
            f"  Average max heart rate: {stats.avg_hr} bpm"
        )

    # Drug review queries
    if any(w in q_lower for w in ["drug", "medication", "treatment", "rating", "review"]):
        drugs = spark.sql("""
            SELECT drug_name, ROUND(AVG(rating), 1) as rating, COUNT(*) as reviews
            FROM drug_reviews GROUP BY drug_name ORDER BY rating DESC LIMIT 5
        """).toPandas()

        drug_info = "\n".join(f"  {r['drug_name']}: {r['rating']}/10 ({r['reviews']} reviews)"
                              for _, r in drugs.iterrows())
        response_parts.append(f"Drug Reviews (Top 5 by rating):\n{drug_info}")

    # Clinical notes queries
    if any(w in q_lower for w in ["clinical", "note", "department", "diagnosis"]):
        notes = spark.sql("""
            SELECT department, COUNT(*) as count
            FROM clinical_notes GROUP BY department ORDER BY count DESC
        """).toPandas()

        note_info = "\n".join(f"  {r['department']}: {r['count']} notes" for _, r in notes.iterrows())
        response_parts.append(f"Clinical Notes by Department:\n{note_info}")

    if not response_parts:
        response_parts.append("I can help with questions about heart disease patients, drug reviews, and clinical notes.")

    return f"Q: {question}\n\n" + "\n\n".join(response_parts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Test the Agent

# COMMAND ----------

print(demo_clinical_agent("What percentage of patients have heart disease?"))

# COMMAND ----------

print(demo_clinical_agent("What are the top rated medications?"))

# COMMAND ----------

print(demo_clinical_agent("How many clinical notes are there by department?"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Agent Evaluation

# COMMAND ----------

eval_prompts = [
    "How many patients in the database have heart disease?",
    "What is the average age of patients with heart disease?",
    "Which drug has the highest rating?",
    "What department has the most clinical notes?",
    "What percentage of patients are over 60?",
]

print("=" * 60)
print("  AGENT EVALUATION DEMO")
print("=" * 60)

for i, prompt in enumerate(eval_prompts, 1):
    print(f"\n--- Prompt {i} ---")
    response = demo_clinical_agent(prompt)
    print(response[:500])

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Points for the Talk
# MAGIC
# MAGIC - **System prompts** define agent behavior — be specific about data sources and response format
# MAGIC - **Genie as a tool** lets agents query structured data without writing SQL
# MAGIC - **AgentBricks** provides the framework for building, testing, and deploying agents
# MAGIC - **Evaluation** measures agent quality against known-good answers
# MAGIC - Teams should focus on: prompt quality, data routing logic, and response formatting
# MAGIC - The **Databricks Assistant** can help write prompts and debug agent logic!
