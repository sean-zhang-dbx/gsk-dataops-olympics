# Databricks notebook source
# MAGIC %md
# MAGIC # Event 4: GenAI / Agents — Agent Building Challenge
# MAGIC
# MAGIC ## Challenge: Build the Best Clinical AI Agent
# MAGIC **Build Time: ~25 minutes**
# MAGIC
# MAGIC ### Objective
# MAGIC Build an AI agent that can answer clinical questions by:
# MAGIC 1. Using a **Genie space** as a tool for structured data queries
# MAGIC 2. Leveraging clinical notes and drug review data for context
# MAGIC 3. Writing effective system prompts
# MAGIC 4. Being evaluated against a set of test prompts
# MAGIC
# MAGIC ### How You Win
# MAGIC **Best agent evaluation score against test prompts wins Gold!**
# MAGIC
# MAGIC ### Available Data
# MAGIC - `clinical_notes` table — 20 clinical notes across departments
# MAGIC - `drug_reviews` table — 1,000 drug review records
# MAGIC - `heart_disease` table — Patient clinical data (from Events 1-2)
# MAGIC - Your Genie space from Event 2 (if set up)
# MAGIC
# MAGIC > **Tip:** Use the Databricks Assistant (`Cmd+I`) for prompt engineering and agent code!
# MAGIC
# MAGIC ### Databricks Assistant — Prompt Gallery
# MAGIC
# MAGIC Try these prompts in the Assistant panel:
# MAGIC
# MAGIC | Task | Prompt to Try |
# MAGIC |------|--------------|
# MAGIC | **System prompt** | "Write a system prompt for a clinical decision support agent that has access to heart disease data, drug reviews, and clinical notes" |
# MAGIC | **SQL routing** | "Write a function that takes a question string and determines which table to query: heart_disease, drug_reviews, or clinical_notes" |
# MAGIC | **Agent function** | "Create a clinical_agent function that routes questions to the right SQL query and returns a formatted answer" |
# MAGIC | **Semantic search** | "Set up ChromaDB to index clinical notes and search by semantic similarity" |
# MAGIC | **Error handling** | "Add error handling and safety guardrails to this agent function — refuse to give medical advice if data is insufficient" |
# MAGIC | **Genie setup** | "Write Genie space instructions for tables: heart_disease, drug_reviews, clinical_notes — include column descriptions and common question patterns" |
# MAGIC
# MAGIC *For Agent mode (if skills are installed): try "Build a Genie space with heart_disease and drug_reviews tables".*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Explore Your Data Sources

# COMMAND ----------

TEAM_NAME = "_____"  # e.g., "team_01"

# Clinical notes
df_notes = spark.table("clinical_notes").toPandas()
print(f"Clinical Notes: {len(df_notes)} records")
print(f"Departments: {df_notes['department'].unique().tolist()}")
display(spark.table("clinical_notes"))

# COMMAND ----------

# Drug reviews
df_drugs = spark.table("drug_reviews").toPandas()
print(f"Drug Reviews: {len(df_drugs)} records")
print(f"Top drugs: {df_drugs['drug_name'].value_counts().head().to_dict()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Set Up a Genie Space (if not already done)
# MAGIC
# MAGIC If you set up a Genie space in Event 2, you can reuse it here.
# MAGIC Otherwise, create one now:
# MAGIC
# MAGIC 1. Click **+ New** > **Genie space**
# MAGIC 2. Name: `{TEAM_NAME} Clinical Agent`
# MAGIC 3. Add data sources: `heart_disease`, `drug_reviews`, `clinical_notes`
# MAGIC 4. Add instructions:
# MAGIC    - "target = 1 means heart disease present"
# MAGIC    - "rating is 1-10 for drug reviews, higher is better"
# MAGIC    - "Use department and note_type to filter clinical notes"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Build Your Agent
# MAGIC
# MAGIC **TODO:** Create an AI agent that can answer clinical questions.
# MAGIC
# MAGIC ### Option A: Using Databricks Foundation Model API + Tools
# MAGIC This approach uses the Foundation Model API with function calling.

# COMMAND ----------

# TODO: Define your agent's system prompt
# This is crucial — a good prompt makes a huge difference!

system_prompt = """
You are a clinical decision support agent for a hospital.
You help doctors and nurses make informed decisions about patient care.

You have access to:
- Heart disease patient data (500 patients with clinical measurements)
- Drug review data (1000 reviews with ratings and conditions)
- Clinical notes from various hospital departments

When answering questions:
- Be specific and cite data when possible
- Flag any potential risks or concerns
- Suggest follow-up actions when appropriate

TODO: Customize this prompt for your team's approach!
"""

print("System prompt defined!")
print(f"Length: {len(system_prompt)} characters")

# COMMAND ----------

# TODO: Create a simple agent function that can answer clinical questions
# This is a basic framework — enhance it to score higher!

def clinical_agent(question: str) -> str:
    """
    Clinical AI agent that answers questions using available data.

    TODO: Enhance this function to:
    1. Determine if the question needs data lookup (SQL) or note search
    2. Query the appropriate data source
    3. Formulate a helpful response
    """

    response = f"Question: {question}\n\n"

    # TODO: Add data lookup logic
    # Hint: Use spark.sql() to query tables based on the question
    # Hint: Use keyword matching or an LLM to determine which table to query

    # Example: If question mentions "heart" or "cardiac", query heart_disease table
    if any(word in question.lower() for word in ["heart", "cardiac", "chest pain", "cholesterol"]):
        # TODO: Write a relevant SQL query based on the question
        result = spark.sql("""
            SELECT COUNT(*) as total_patients,
                   SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) as with_disease,
                   AVG(age) as avg_age
            FROM heart_disease
        """).collect()[0]

        response += f"Heart Disease Data:\n"
        response += f"  Total patients: {result.total_patients}\n"
        response += f"  With heart disease: {result.with_disease}\n"
        response += f"  Average age: {result.avg_age:.1f}\n\n"

    # Example: If question mentions "drug" or "medication", query drug_reviews
    if any(word in question.lower() for word in ["drug", "medication", "medicine", "treatment"]):
        result = spark.sql("""
            SELECT drug_name, AVG(rating) as avg_rating, COUNT(*) as review_count
            FROM drug_reviews
            GROUP BY drug_name
            ORDER BY avg_rating DESC
            LIMIT 5
        """).toPandas()

        response += f"Top Rated Drugs:\n"
        for _, row in result.iterrows():
            response += f"  {row['drug_name']}: {row['avg_rating']:.1f}/10 ({row['review_count']} reviews)\n"
        response += "\n"

    # TODO: Add more data lookup patterns
    # TODO: Add clinical notes search
    # TODO: Use an LLM to generate the final response

    response += "---\nTODO: Enhance this agent with better logic and LLM integration!"

    return response

# COMMAND ----------

# Test your agent
print(clinical_agent("What is the average age of patients with heart disease?"))

# COMMAND ----------

print(clinical_agent("What are the best rated medications for diabetes?"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Enhance Your Agent (Creativity Points!)
# MAGIC
# MAGIC Ideas to improve your agent:
# MAGIC - **Use Foundation Model API** for natural language generation
# MAGIC - **Add semantic search** over clinical notes (ChromaDB)
# MAGIC - **Connect to Genie** for complex SQL queries
# MAGIC - **Add multi-step reasoning** (break complex questions into sub-queries)
# MAGIC - **Add safety guardrails** (refuse to give medical advice without data)

# COMMAND ----------

# TODO: Enhance your agent here!
#
# Option: Use Foundation Model API
# from databricks.sdk import WorkspaceClient
# w = WorkspaceClient()
# response = w.serving_endpoints.query(
#     name="databricks-meta-llama-3-1-70b-instruct",
#     messages=[
#         {"role": "system", "content": system_prompt},
#         {"role": "user", "content": question}
#     ]
# )

# YOUR ENHANCED AGENT CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Agent Evaluation
# MAGIC
# MAGIC The organizer will run an evaluation script against your agent.
# MAGIC Test with these sample prompts to prepare:

# COMMAND ----------

test_prompts = [
    "How many patients in the dataset have heart disease?",
    "What is the most common chest pain type among patients with heart disease?",
    "Which drug has the highest average rating?",
    "What department has the most clinical notes?",
    "Is there a correlation between age and heart disease in this data?",
]

print("=" * 60)
print(f"  AGENT EVALUATION — {TEAM_NAME}")
print("=" * 60)

for i, prompt in enumerate(test_prompts, 1):
    print(f"\n--- Test Prompt {i} ---")
    print(f"Q: {prompt}")
    try:
        response = clinical_agent(prompt)
        print(f"A: {response[:300]}")
    except Exception as e:
        print(f"ERROR: {e}")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stretch Goals (Extra Credit)
# MAGIC
# MAGIC Finished early? Try these with the Databricks Assistant:
# MAGIC
# MAGIC 1. **RAG with ChromaDB** — Ask: *"Set up ChromaDB to index all clinical notes, embed them with sentence-transformers, and add semantic search to the agent"*
# MAGIC 2. **Multi-step reasoning** — Ask: *"Enhance the agent to break complex questions into sub-queries — e.g., 'Are heart disease patients on well-rated drugs?' queries both tables"*
# MAGIC 3. **Safety guardrails** — Ask: *"Add input validation and safety guardrails: refuse inappropriate questions, add confidence scores, cite data sources in responses"*
# MAGIC 4. **Foundation Model API** — Ask: *"Connect the agent to the Databricks Foundation Model API (meta-llama) to generate natural language responses from the SQL results"*
# MAGIC 5. **Agent evaluation** — Ask: *"Create an evaluation framework that scores agent responses on accuracy, relevance, and completeness for each test prompt"*
# MAGIC 6. **Conversation memory** — Ask: *"Add conversation history so the agent can handle follow-up questions like 'What about for women only?'"*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checklist
# MAGIC - [ ] Data sources explored (clinical notes, drug reviews, heart disease)
# MAGIC - [ ] Genie space set up with relevant data
# MAGIC - [ ] Agent function created and tested
# MAGIC - [ ] System prompt crafted
# MAGIC - [ ] Agent handles multiple question types
# MAGIC - [ ] Agent tested against sample prompts
# MAGIC
# MAGIC > **Signal judges when ready for agent evaluation!**
