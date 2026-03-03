# Databricks notebook source
# MAGIC %md
# MAGIC # Event 4: GenAI / Agents — Agent Building Challenge
# MAGIC
# MAGIC ## Challenge: Build the Best Clinical AI Agent
# MAGIC **Build Time: ~25 minutes**
# MAGIC
# MAGIC ### Objective
# MAGIC Build an AI agent that can answer clinical questions by:
# MAGIC 1. Querying your **Event 1 Silver/Gold tables** for patient analytics
# MAGIC 2. Leveraging **drug review** and **clinical notes** data for context
# MAGIC 3. Writing effective system prompts
# MAGIC 4. Being evaluated against a set of test prompts
# MAGIC
# MAGIC ### How You Win
# MAGIC **Best agent evaluation score against test prompts wins Gold!**
# MAGIC
# MAGIC ### Data — From YOUR Pipeline + Additional Sources
# MAGIC
# MAGIC - `{TEAM_NAME}_heart_silver` — Your cleaned patient data from Event 1
# MAGIC - `{TEAM_NAME}_heart_gold` — Your aggregated metrics from Event 1
# MAGIC - `drug_reviews` table — 1,000 drug review records with ratings
# MAGIC - `clinical_notes` table — 20 clinical notes across hospital departments
# MAGIC
# MAGIC (SDP path: `heart_silver` and `heart_gold`)
# MAGIC
# MAGIC > **Vibe Coding:** Use the Databricks Assistant (`Cmd+I`) for prompt engineering and agent code!

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dataops_olympics;
# MAGIC USE SCHEMA default;

# COMMAND ----------

TEAM_NAME = "team_XX"  # <-- CHANGE THIS to your team name

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Explore Your Data Sources
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Explore all 4 data sources your agent will have access to.
# MAGIC > For each table, display the row count, column names, and a sample.
# MAGIC >
# MAGIC > Tables:
# MAGIC > - Your Silver table (`{TEAM_NAME}_heart_silver` or `heart_silver`)
# MAGIC > - Your Gold table (`{TEAM_NAME}_heart_gold` or `heart_gold`)
# MAGIC > - `drug_reviews` — drug names, ratings (1-10), conditions
# MAGIC > - `clinical_notes` — departments, note types, clinical text

# COMMAND ----------

# YOUR CODE HERE — explore all 4 data sources


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Set Up a Genie Space
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > In the Databricks UI, create a Genie space (or reuse from Event 2):
# MAGIC >
# MAGIC > 1. Click **+ New** > **Genie space**
# MAGIC > 2. Name: `{TEAM_NAME} Clinical Agent`
# MAGIC > 3. Add your Silver/Gold tables, `drug_reviews`, and `clinical_notes`
# MAGIC > 4. Add instructions explaining column meanings (target, cp, sex, thalach, etc.)
# MAGIC > 5. Test with a few sample questions

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Build Your Agent
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create a `clinical_agent(question: str) -> str` function that:
# MAGIC >
# MAGIC > 1. **Detects the topic** from the question (heart data, drugs, clinical notes)
# MAGIC > 2. **Routes to the right table** and runs an appropriate SQL query:
# MAGIC >    - Heart/cardiac/patient questions -> query your Silver table for stats
# MAGIC >    - Drug/medication/rating questions -> query `drug_reviews` for top drugs
# MAGIC >    - Clinical/note/department questions -> query `clinical_notes` for summaries
# MAGIC >    - Age group questions -> query your Gold table directly
# MAGIC > 3. **Formats the response** as a readable string with data citations
# MAGIC > 4. **Handles unknown questions** gracefully with a helpful message
# MAGIC >
# MAGIC > Also define a `system_prompt` variable (100+ chars) describing the agent's role,
# MAGIC > available data sources, and behavioral rules.

# COMMAND ----------

# YOUR CODE HERE — define system_prompt


# COMMAND ----------

# YOUR CODE HERE — define clinical_agent function


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Test Your Agent

# COMMAND ----------

# Test with these prompts
test_prompts = [
    "How many patients in the dataset have heart disease?",
    "What is the most common chest pain type among patients with heart disease?",
    "Which drug has the highest average rating?",
    "What department has the most clinical notes?",
    "Which age group has the highest heart disease rate?",
]

print("=" * 60)
print(f"  AGENT TEST — {TEAM_NAME}")
print("=" * 60)

for i, prompt in enumerate(test_prompts, 1):
    print(f"\n--- Test {i} ---")
    print(f"Q: {prompt}")
    try:
        response = clinical_agent(prompt)
        print(f"A: {response[:300]}")
    except Exception as e:
        print(f"ERROR: {e}")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Enhance Your Agent (Creativity Points!)
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Improve your agent with advanced features. Ideas:
# MAGIC >
# MAGIC > - **Foundation Model API** — Use `databricks-meta-llama-3-1-70b-instruct` to generate
# MAGIC >   natural language responses from SQL results
# MAGIC > - **Semantic search** over clinical notes with ChromaDB
# MAGIC > - **Multi-step reasoning** — break complex questions into sub-queries across tables
# MAGIC > - **Safety guardrails** — refuse to give medical advice, add confidence scores
# MAGIC > - **Conversation memory** — handle follow-up questions like "What about for women only?"
# MAGIC > - **Connect to your Genie space** for complex SQL generation

# COMMAND ----------

# YOUR CODE HERE — enhance your agent


# COMMAND ----------

# MAGIC %md
# MAGIC ## Checklist
# MAGIC - [ ] Data sources explored
# MAGIC - [ ] Genie space set up with relevant data
# MAGIC - [ ] Agent function created and tested
# MAGIC - [ ] System prompt crafted
# MAGIC - [ ] Agent handles multiple question types
# MAGIC - [ ] Agent tested against sample prompts
# MAGIC
# MAGIC > **Signal judges when ready for agent evaluation!**
