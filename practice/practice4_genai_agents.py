# Databricks notebook source
# MAGIC %md
# MAGIC # Practice 4: GenAI Agents
# MAGIC
# MAGIC **Time: ~10 minutes** | Use the Databricks Assistant to generate all your code from the business requirements.
# MAGIC
# MAGIC You just saw the lightning talk — now try it yourself!
# MAGIC Each exercise describes **what** needs to happen. Use `Cmd+I` to prompt the Assistant.
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
# MAGIC (No code to write here — just run and observe.)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'heart_disease' as source, COUNT(*) as records FROM heart_disease
# MAGIC UNION ALL
# MAGIC SELECT 'drug_reviews', COUNT(*) FROM drug_reviews
# MAGIC UNION ALL
# MAGIC SELECT 'clinical_notes', COUNT(*) FROM clinical_notes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT drug_name, ROUND(AVG(rating), 1) as avg_rating, COUNT(*) as reviews
# MAGIC FROM drug_reviews GROUP BY 1 ORDER BY 2 DESC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Write a System Prompt
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create a `system_prompt` string variable that instructs a clinical AI assistant.
# MAGIC > The prompt should:
# MAGIC > - Describe the agent's role (clinical decision support)
# MAGIC > - List the 3 data sources it has access to:
# MAGIC >   - `heart_disease`: 500 patients with clinical features (age, cholesterol, BP, target=1 means disease)
# MAGIC >   - `drug_reviews`: 1,000 reviews with drug names, ratings (1-10), and conditions
# MAGIC >   - `clinical_notes`: 20 notes from hospital departments
# MAGIC > - Include behavioral rules: cite data, give numbers, never give medical advice
# MAGIC >
# MAGIC > The prompt should be at least 100 characters long.

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Build a Simple Agent
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create a function called `my_agent(question: str) -> str` that:
# MAGIC >
# MAGIC > 1. If the question mentions "heart", "cardiac", "patient", or "cholesterol":
# MAGIC >    query the `heart_disease` table for total patients, count with disease, and avg age
# MAGIC >
# MAGIC > 2. If the question mentions "drug", "medication", or "rating":
# MAGIC >    query `drug_reviews` for the top 3 drugs by average rating
# MAGIC >
# MAGIC > 3. If the question mentions "clinical", "note", or "department":
# MAGIC >    query `clinical_notes` for note count by department
# MAGIC >
# MAGIC > 4. If no keywords match, return a helpful message saying what topics it can answer
# MAGIC >
# MAGIC > Format the response as a readable string with the question at the top.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Test Your Agent
# MAGIC
# MAGIC Run these cells to verify the agent returns meaningful answers.

# COMMAND ----------

print(my_agent("How many patients have heart disease?"))

# COMMAND ----------

print(my_agent("What are the top rated drugs?"))

# COMMAND ----------

print(my_agent("Which departments have clinical notes?"))

# COMMAND ----------

print(my_agent("What time is it?"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — Run this to check your work!

# COMMAND ----------

print("=" * 55)
print("  PRACTICE 4 — VALIDATION")
print("=" * 55)

score = 0

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

try:
    resp = my_agent("How many patients have heart disease?")
    assert any(c.isdigit() for c in resp), "Response should contain numbers"
    assert "heart" in resp.lower() or "patient" in resp.lower() or "disease" in resp.lower()
    print(f"  [PASS] Agent answers heart disease questions with data")
    score += 1
except Exception as e:
    print(f"  [FAIL] Heart disease routing: {e}")

try:
    resp = my_agent("What are the best rated drugs?")
    assert any(c.isdigit() for c in resp), "Response should contain ratings"
    print(f"  [PASS] Agent answers drug questions with data")
    score += 1
except Exception as e:
    print(f"  [FAIL] Drug routing: {e}")

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
