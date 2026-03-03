# Databricks notebook source
# MAGIC %md
# MAGIC # Plot Twist 3: Multi-Agent — Build a Team of Agents!
# MAGIC
# MAGIC ## The Scenario
# MAGIC Your single clinical agent from Event 4 works well, but the hospital wants
# MAGIC specialized agents that collaborate. You need to extend your system into a
# MAGIC **multi-agent architecture** with specialized roles.
# MAGIC
# MAGIC **You have 10 minutes to build a multi-agent system.**
# MAGIC
# MAGIC ## The Architecture
# MAGIC You need at least 2 specialized agents that work together:
# MAGIC
# MAGIC | Agent | Role | Data Sources |
# MAGIC |-------|------|-------------|
# MAGIC | **Clinical Agent** | Answers questions about patients and clinical notes | heart_disease, clinical_notes |
# MAGIC | **Pharmacy Agent** | Answers questions about drugs and treatments | drug_reviews |
# MAGIC | **Router (optional)** | Routes questions to the right specialist agent | N/A |
# MAGIC
# MAGIC ## Your Task
# MAGIC 1. Create a second specialized agent (Pharmacy Agent)
# MAGIC 2. Create a routing mechanism that decides which agent handles each question
# MAGIC 3. Show the multi-agent system handling different question types
# MAGIC 4. Demonstrate that specialized agents give better answers than a single generalist

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Define Specialized Agents

# COMMAND ----------

TEAM_NAME = "_____"

# Agent 1: Clinical Specialist
def clinical_specialist(question: str) -> str:
    """Handles questions about patients, diagnoses, and clinical data."""

    # Query patient data
    result = spark.sql("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) as diseased,
               AVG(age) as avg_age,
               AVG(chol) as avg_chol
        FROM heart_disease
    """).collect()[0]

    response = f"[Clinical Specialist]\n"
    response += f"Patient Database: {result.total} patients\n"
    response += f"Heart Disease Cases: {result.diseased}\n"
    response += f"Average Age: {result.avg_age:.1f}\n"

    # TODO: Add clinical notes search
    # TODO: Use LLM for better responses
    # TODO: Add more specific query logic based on the question

    return response


# Agent 2: Pharmacy Specialist
def pharmacy_specialist(question: str) -> str:
    """Handles questions about drugs, treatments, and reviews."""

    # TODO: Implement the pharmacy agent
    # Hint: Query drug_reviews table based on the question
    # Hint: Look for drug names, conditions, ratings in the question

    result = spark.sql("""
        SELECT drug_name, AVG(rating) as avg_rating, COUNT(*) as reviews
        FROM drug_reviews
        GROUP BY drug_name
        ORDER BY avg_rating DESC
        LIMIT 5
    """).toPandas()

    response = f"[Pharmacy Specialist]\n"
    response += f"Top Medications by Rating:\n"
    for _, row in result.iterrows():
        response += f"  {row['drug_name']}: {row['avg_rating']:.1f}/10 ({row['reviews']} reviews)\n"

    # YOUR ENHANCED LOGIC HERE

    return response

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build a Router

# COMMAND ----------

# TODO: Create a router that decides which agent handles each question
def route_question(question: str) -> str:
    """
    Routes a question to the appropriate specialist agent.

    TODO: Make this smarter! Ideas:
    - Use keyword matching (simple)
    - Use an LLM to classify the question (advanced)
    - Use embeddings similarity to determine topic (advanced)
    """
    q_lower = question.lower()

    # Simple keyword-based routing
    clinical_keywords = ["patient", "heart", "disease", "cardiac", "chest pain",
                         "age", "cholesterol", "blood pressure", "diagnosis", "clinical"]
    pharmacy_keywords = ["drug", "medication", "medicine", "treatment", "side effect",
                         "rating", "review", "prescription", "dosage"]

    clinical_score = sum(1 for kw in clinical_keywords if kw in q_lower)
    pharmacy_score = sum(1 for kw in pharmacy_keywords if kw in q_lower)

    if clinical_score > pharmacy_score:
        return "clinical"
    elif pharmacy_score > clinical_score:
        return "pharmacy"
    else:
        return "both"  # Ask both agents

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Multi-Agent Orchestrator

# COMMAND ----------

def multi_agent_system(question: str) -> str:
    """Orchestrates multiple specialist agents to answer questions."""

    route = route_question(question)

    response = f"Question: {question}\n"
    response += f"Routing: -> {route}\n"
    response += "=" * 50 + "\n\n"

    if route == "clinical" or route == "both":
        response += clinical_specialist(question) + "\n"

    if route == "pharmacy" or route == "both":
        response += pharmacy_specialist(question) + "\n"

    # TODO: Add a synthesis step that combines both agents' responses
    # TODO: Use an LLM to create a unified answer from multiple specialists

    return response

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test the Multi-Agent System

# COMMAND ----------

test_questions = [
    "What percentage of patients have heart disease?",
    "What is the best rated drug for hypertension?",
    "How does age affect heart disease risk, and what drugs are commonly prescribed?",
    "Which department has the most clinical notes?",
    "Compare the effectiveness of Metformin across different conditions",
]

print("=" * 60)
print(f"  MULTI-AGENT SYSTEM TEST — {TEAM_NAME}")
print("=" * 60)

for i, q in enumerate(test_questions, 1):
    print(f"\n{'='*60}")
    print(f"  Test {i}")
    print(f"{'='*60}")
    print(multi_agent_system(q))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring Criteria
# MAGIC
# MAGIC | Criteria | Points |
# MAGIC |----------|--------|
# MAGIC | Two functional specialist agents | 3 |
# MAGIC | Working router mechanism | 2 |
# MAGIC | Cross-agent questions handled | 2 |
# MAGIC | Quality of responses | 2 |
# MAGIC | Presentation / explanation | 1 |
# MAGIC | **Total** | **10** |
