# Databricks notebook source
# MAGIC %md
# MAGIC # Practice 4: GenAI Agents
# MAGIC
# MAGIC **Time: ~10 minutes** | Fill in the blanks, run each cell, check your work at the end.
# MAGIC
# MAGIC You just saw the lightning talk — now try it yourself!
# MAGIC Fill in the `_____` blanks below. Use the **Databricks Assistant** (`Cmd+I`) if you get stuck.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What You'll Do
# MAGIC 1. Explore the clinical data sources
# MAGIC 2. Write a system prompt for a clinical agent
# MAGIC 3. Build a simple agent that routes questions to the right data
# MAGIC 4. Test the agent with sample questions
# MAGIC 5. Run the validation check

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Run this cell first

# COMMAND ----------

spark.sql("USE CATALOG dataops_olympics")
spark.sql("USE SCHEMA default")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Explore the Data
# MAGIC
# MAGIC Run these cells to see what data your agent will have access to.
# MAGIC (No blanks here — just run and observe.)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'heart_disease' as source, COUNT(*) as records FROM heart_disease
# MAGIC UNION ALL
# MAGIC SELECT 'drug_reviews', COUNT(*) FROM drug_reviews
# MAGIC UNION ALL
# MAGIC SELECT 'clinical_notes', COUNT(*) FROM clinical_notes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top drugs by rating
# MAGIC SELECT drug_name, ROUND(AVG(rating), 1) as avg_rating, COUNT(*) as reviews
# MAGIC FROM drug_reviews GROUP BY 1 ORDER BY 2 DESC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Write a System Prompt
# MAGIC
# MAGIC Fill in the blank to describe what data the agent has access to.
# MAGIC
# MAGIC **Hint:** Mention the three tables: heart_disease, drug_reviews, clinical_notes.
# MAGIC Include what each table contains and any important column meanings.

# COMMAND ----------

# FILL IN: Replace _____ with a description of the agent's data sources
system_prompt = """You are a clinical decision support assistant.

You have access to:
_____

When answering:
- Cite data sources and sample sizes
- Provide numbers when possible
- Never give medical advice — you are a data tool
"""

print(f"System prompt: {len(system_prompt)} characters")
print(system_prompt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Build a Simple Agent
# MAGIC
# MAGIC Fill in the SQL query to get heart disease statistics when the user asks
# MAGIC about cardiac topics.
# MAGIC
# MAGIC **Hint:** Query the `heart_disease` table for COUNT, AVG(age), and AVG(chol).

# COMMAND ----------

def my_agent(question: str) -> str:
    """Simple clinical agent that routes questions to the right data."""

    q = question.lower()
    answer_parts = []

    # Route 1: Heart disease questions
    if any(w in q for w in ["heart", "cardiac", "patient", "disease", "cholesterol"]):
        # FILL IN: Replace _____ with a SQL query that returns total patients,
        # number with disease, and average age from the heart_disease table
        stats = spark.sql("""_____""").collect()[0]

        answer_parts.append(
            f"Heart Disease Data:\n"
            f"  Total patients: {stats[0]}\n"
            f"  With disease: {stats[1]}\n"
            f"  Average age: {stats[2]}"
        )

    # Route 2: Drug review questions
    if any(w in q for w in ["drug", "medication", "rating", "review"]):
        drugs = spark.sql("""
            SELECT drug_name, ROUND(AVG(rating), 1) as avg_rating, COUNT(*) as n
            FROM drug_reviews GROUP BY 1 ORDER BY avg_rating DESC LIMIT 3
        """).toPandas()

        lines = [f"  {r['drug_name']}: {r['avg_rating']}/10 ({r['n']} reviews)"
                 for _, r in drugs.iterrows()]
        answer_parts.append("Top Drugs:\n" + "\n".join(lines))

    # Route 3: Clinical notes questions
    if any(w in q for w in ["clinical", "note", "department"]):
        depts = spark.sql("""
            SELECT department, COUNT(*) as n FROM clinical_notes GROUP BY 1 ORDER BY n DESC
        """).toPandas()

        lines = [f"  {r['department']}: {r['n']} notes" for _, r in depts.iterrows()]
        answer_parts.append("Clinical Notes:\n" + "\n".join(lines))

    if not answer_parts:
        answer_parts.append("I can answer questions about heart disease patients, drug reviews, and clinical notes.")

    return f"Q: {question}\n\n" + "\n\n".join(answer_parts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Test Your Agent
# MAGIC
# MAGIC Run these cells and verify the agent returns meaningful answers.

# COMMAND ----------

print(my_agent("How many patients have heart disease?"))

# COMMAND ----------

print(my_agent("What are the top rated drugs?"))

# COMMAND ----------

print(my_agent("Which departments have clinical notes?"))

# COMMAND ----------

# Test the guardrail — should say it can't help
print(my_agent("What time is it?"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — Run this to check your work!

# COMMAND ----------

print("=" * 55)
print("  PRACTICE 4 — VALIDATION")
print("=" * 55)

score = 0

# Check 1: System prompt is meaningful
try:
    assert len(system_prompt) > 100, "System prompt too short"
    assert "heart" in system_prompt.lower() or "patient" in system_prompt.lower(), \
        "Prompt should mention heart disease data"
    print(f"  [PASS] System prompt defined ({len(system_prompt)} chars)")
    score += 1
except AssertionError as e:
    print(f"  [FAIL] System prompt issue: {e}")
except Exception:
    print("  [FAIL] System prompt not defined")

# Check 2: Agent answers heart disease questions
try:
    resp = my_agent("How many patients have heart disease?")
    assert any(c.isdigit() for c in resp), "Response should contain numbers"
    assert "heart" in resp.lower() or "patient" in resp.lower() or "disease" in resp.lower()
    print(f"  [PASS] Agent answers heart disease questions with data")
    score += 1
except Exception as e:
    print(f"  [FAIL] Heart disease routing: {e}")

# Check 3: Agent answers drug questions
try:
    resp = my_agent("What are the best rated drugs?")
    assert any(c.isdigit() for c in resp), "Response should contain ratings"
    print(f"  [PASS] Agent answers drug questions with data")
    score += 1
except Exception as e:
    print(f"  [FAIL] Drug routing: {e}")

# Check 4: Agent handles unknown questions gracefully
try:
    resp = my_agent("What is the meaning of life?")
    assert "heart" in resp.lower() or "drug" in resp.lower() or "clinical" in resp.lower() or "can" in resp.lower()
    print(f"  [PASS] Agent handles unknown questions gracefully")
    score += 1
except Exception as e:
    print(f"  [FAIL] Unknown question handling: {e}")

print(f"\n  Score: {score}/4")
if score == 4:
    print("\n  ALL PASSED! You're ready for the competition!")
elif score >= 2:
    print("\n  Good progress! Fix the remaining items and re-run.")
else:
    print("\n  Ask the Databricks Assistant for help!")
print("=" * 55)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answers (for reference — try without peeking!)
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answers</summary>
# MAGIC
# MAGIC **Exercise 2** — System prompt should mention something like:
# MAGIC ```
# MAGIC 1. heart_disease table: 500 patients with age, cholesterol, blood pressure, target (1=disease, 0=healthy)
# MAGIC 2. drug_reviews table: 1000 drug reviews with ratings 1-10 and conditions
# MAGIC 3. clinical_notes table: 20 clinical notes from hospital departments
# MAGIC ```
# MAGIC
# MAGIC **Exercise 3** — SQL query:
# MAGIC ```sql
# MAGIC SELECT COUNT(*) as total,
# MAGIC        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) as diseased,
# MAGIC        ROUND(AVG(age), 1) as avg_age
# MAGIC FROM heart_disease
# MAGIC ```
# MAGIC
# MAGIC </details>
