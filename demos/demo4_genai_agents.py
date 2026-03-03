# Databricks notebook source
# MAGIC %md
# MAGIC # Lightning Talk 4: GenAI Agents on Databricks
# MAGIC
# MAGIC **Duration: 15 minutes** | Instructor-led live demo
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Agenda
# MAGIC | Time | Section | What You'll See |
# MAGIC |------|---------|----------------|
# MAGIC | 0:00 | **The Problem** | Doctors need answers, not SQL |
# MAGIC | 1:00 | **Data Landscape** | Three data sources, one agent |
# MAGIC | 3:00 | **System Prompt** | The art of prompt engineering |
# MAGIC | 5:00 | **Build Agent** | Multi-source clinical agent in Python |
# MAGIC | 8:00 | **Test Agent** | Live Q&A with the agent |
# MAGIC | 10:00 | **Wow Moment** | Semantic search over clinical notes |
# MAGIC | 13:00 | **Evaluation** | How to measure agent quality |
# MAGIC | 14:00 | **Your Turn** | Preview of the practice notebook |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Problem
# MAGIC
# MAGIC > **Say this:** "A hospital has three databases: patient records, drug reviews, and
# MAGIC > clinical notes. A doctor asks: 'What's the best-rated drug for my patient's condition?'
# MAGIC > Today, that requires opening three apps, writing three queries, and stitching the answer
# MAGIC > together. What if one AI agent could do all of that?"

# COMMAND ----------

spark.sql("USE dataops_olympics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Our Data Landscape

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Three data sources, three different types of information
# MAGIC SELECT 'heart_disease' as source, COUNT(*) as records, '14 clinical measurements per patient' as description FROM heart_disease
# MAGIC UNION ALL
# MAGIC SELECT 'drug_reviews', COUNT(*), '15 drugs, ratings 1-10, patient reviews' FROM drug_reviews
# MAGIC UNION ALL
# MAGIC SELECT 'clinical_notes', COUNT(*), '5 departments, 4 note types' FROM clinical_notes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Peek at clinical notes
# MAGIC SELECT note_id, department, note_type, LEFT(text, 80) as preview FROM clinical_notes LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drug review landscape
# MAGIC SELECT drug_name, condition,
# MAGIC        COUNT(*) as reviews, ROUND(AVG(rating), 1) as avg_rating
# MAGIC FROM drug_reviews
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY reviews DESC
# MAGIC LIMIT 8

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "500 patients, 1000 drug reviews, 20 clinical notes. Three very different
# MAGIC > data formats. Our agent needs to understand the question and route it to the right source."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The System Prompt — Most Important Part
# MAGIC
# MAGIC > **Say this:** "The system prompt IS the agent. It defines personality, capabilities,
# MAGIC > data access, and guardrails. A great system prompt is specific, not vague."

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
- Flag limitations (e.g., "based on 500 patients, not population-level")
- NEVER provide medical advice — you are a DATA ANALYSIS tool
- If data is insufficient, say so explicitly
"""

print("System prompt defined!")
print(f"Length: {len(system_prompt)} characters")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "Notice I'm very specific: I listed the exact tables, the exact column
# MAGIC > names, the data types, and what the codes mean. The more specific your prompt,
# MAGIC > the better the agent performs. 'Be helpful' is a bad prompt. This is a good one."

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
            SELECT drug_name,
                   ROUND(AVG(rating), 1) as avg_rating,
                   COUNT(*) as n_reviews
            FROM drug_reviews GROUP BY 1 ORDER BY avg_rating DESC LIMIT 5
        """).toPandas()

        lines = [f"  {r['drug_name']}: {r['avg_rating']}/10 ({r['n_reviews']} reviews)"
                 for _, r in drugs.iterrows()]
        sections.append(f"TOP DRUGS BY RATING:\n" + "\n".join(lines))

    if any(w in q for w in ["clinical", "note", "department", "diagnosis", "physician"]):
        depts = spark.sql("""
            SELECT department, COUNT(*) as notes,
                   COLLECT_SET(note_type) as note_types
            FROM clinical_notes GROUP BY 1 ORDER BY notes DESC
        """).toPandas()

        lines = [f"  {r['department']}: {r['notes']} notes ({', '.join(r['note_types'])})"
                 for _, r in depts.iterrows()]
        sections.append(f"CLINICAL NOTES BY DEPARTMENT:\n" + "\n".join(lines))

    if not sections:
        sections.append(
            "I can answer questions about:\n"
            "  - Heart disease patient data (age, cholesterol, diagnosis)\n"
            "  - Drug reviews and ratings\n"
            "  - Clinical notes by department\n"
            "Please ask a specific question about one of these topics."
        )

    return f"Q: {question}\n\n" + "\n\n".join(sections)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "The agent does three things: keyword detection, SQL routing, and
# MAGIC > formatted response. It's simple but effective. In the competition, you'll make
# MAGIC > this smarter — better routing, LLM-generated answers, maybe semantic search."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Test the Agent — Live Q&A

# COMMAND ----------

print(clinical_agent("What percentage of patients have heart disease?"))

# COMMAND ----------

print(clinical_agent("What are the top rated drugs?"))

# COMMAND ----------

print(clinical_agent("Which departments have the most clinical notes?"))

# COMMAND ----------

print(clinical_agent("What is the weather today?"))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "Notice what happened with the weather question — the agent correctly
# MAGIC > said 'I don't have that data.' That's a guardrail. A good agent knows its limits."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Wow Moment — Semantic Search over Clinical Notes
# MAGIC
# MAGIC > **Say this:** "What if someone asks about 'cardiac symptoms' but the note says
# MAGIC > 'cardiology condition'? Keyword matching would miss that. Semantic search understands
# MAGIC > meaning, not just words."

# COMMAND ----------

try:
    import chromadb
    from sentence_transformers import SentenceTransformer

    notes_df = spark.table("clinical_notes").toPandas()

    embedder = SentenceTransformer("all-MiniLM-L6-v2")

    client = chromadb.Client()
    try:
        client.delete_collection("clinical_notes")
    except:
        pass
    collection = client.create_collection("clinical_notes")

    collection.add(
        documents=notes_df["text"].tolist(),
        ids=notes_df["note_id"].tolist(),
        metadatas=[{"dept": d, "type": t}
                   for d, t in zip(notes_df["department"], notes_df["note_type"])]
    )

    results = collection.query(query_texts=["cardiac symptoms and treatment plan"], n_results=3)

    print("Semantic Search: 'cardiac symptoms and treatment plan'\n")
    for i, (doc, meta) in enumerate(zip(results["documents"][0], results["metadatas"][0])):
        print(f"  Result {i+1} [{meta['dept']} / {meta['type']}]:")
        print(f"    {doc[:120]}...")
        print()

    print("The search found relevant notes by MEANING, not just keywords!")

except ImportError:
    print("ChromaDB/sentence-transformers not installed.")
    print("Run: %pip install chromadb sentence-transformers")
    print("\nThis is an optional enhancement for the competition!")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "We searched for 'cardiac symptoms' and found notes from Cardiology
# MAGIC > even though the exact phrase doesn't appear. That's the power of embeddings.
# MAGIC > In the competition, you can add this to your agent as a bonus."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Agent Evaluation Framework

# COMMAND ----------

eval_prompts = [
    ("How many patients have heart disease?", "should return count and percentage"),
    ("What is the best rated medication?", "should return drug name and rating"),
    ("Which department has the most notes?", "should return department breakdown"),
    ("What is the average age of cardiac patients?", "should return age statistic"),
    ("Tell me about diabetes treatments", "should route to drug reviews"),
]

print("=" * 60)
print("  AGENT EVALUATION (5 test prompts)")
print("=" * 60)

for i, (prompt, expected) in enumerate(eval_prompts, 1):
    response = clinical_agent(prompt)
    has_data = any(c.isdigit() for c in response)
    status = "PASS" if has_data else "WEAK"
    print(f"\n  [{status}] Prompt {i}: {prompt}")
    print(f"  Expected: {expected}")
    preview = response.split('\n\n', 1)[1][:150] if '\n\n' in response else response[:150]
    print(f"  Got: {preview}...")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Say this:** "Evaluation is how you know if your agent is actually good.
# MAGIC > We test it against known questions and check if the answers contain real data.
# MAGIC > In production, you'd use Mosaic AI Agent Evaluation for this."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Turn!
# MAGIC
# MAGIC > **Say this:** "In the practice notebook, you'll write a system prompt and build a
# MAGIC > simple 2-source agent. Fill in 4 blanks, 10 minutes. Then in the competition,
# MAGIC > you'll go all-out: semantic search, LLM integration, multi-step reasoning. Let's go!"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **System prompts** define agent behavior — be specific about data, rules, and format
# MAGIC 2. **Multi-source routing** — detect what the user needs and query the right table
# MAGIC 3. **Guardrails** — a good agent knows when it can't answer
# MAGIC 4. **Semantic search** (ChromaDB + embeddings) finds information by meaning, not keywords
# MAGIC 5. **Evaluation** — test your agent against known prompts to measure quality
# MAGIC 6. **Use the Databricks Assistant** to write prompts, debug routing logic, and add features
