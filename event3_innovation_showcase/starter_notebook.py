# Databricks notebook source
# MAGIC %md
# MAGIC # 💡 Event 3: Innovation Showcase
# MAGIC
# MAGIC ## Challenge: Most Creative Use of AI on Databricks
# MAGIC **Time Limit: 30 minutes**
# MAGIC
# MAGIC ### Objective
# MAGIC Build an **AI-powered clinical decision support** application using Databricks.
# MAGIC
# MAGIC Your application should:
# MAGIC 1. Use clinical notes data (provided)
# MAGIC 2. Implement semantic search over clinical notes
# MAGIC 3. Use an LLM to generate insights
# MAGIC 4. Demonstrate a useful clinical workflow
# MAGIC 5. Present a live demo to judges
# MAGIC
# MAGIC ### Scoring
# MAGIC - **Creativity** (max 10 pts)
# MAGIC - **Functionality** (max 10 pts)
# MAGIC - **Usefulness** (max 5 pts)
# MAGIC - **Live Demo Quality** (max 5 pts)
# MAGIC - **Total**: max 30 pts
# MAGIC
# MAGIC ### Available Resources
# MAGIC - `clinical_notes` table (20 clinical notes across departments)
# MAGIC - `drug_reviews` table (1000 drug review records)
# MAGIC - `heart_disease` table (patient clinical data)
# MAGIC - Python libraries: chromadb, sentence-transformers, openai
# MAGIC
# MAGIC > ⏱️ START YOUR TIMER NOW!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Explore the Clinical Data

# COMMAND ----------

# Load clinical notes
df_notes = spark.table("clinical_notes").toPandas()
print(f"Clinical Notes: {len(df_notes)} records")
print(f"Departments: {df_notes['department'].unique().tolist()}")
print(f"Note types: {df_notes['note_type'].unique().tolist()}")

display(spark.table("clinical_notes"))

# COMMAND ----------

# Load drug reviews for context
df_drugs = spark.table("drug_reviews").toPandas()
print(f"Drug Reviews: {len(df_drugs)} records")
print(f"Drugs: {df_drugs['drug_name'].unique().tolist()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build a Semantic Search Engine
# MAGIC
# MAGIC **TODO:** Create a vector store from clinical notes so you can search by meaning.
# MAGIC
# MAGIC We'll use **ChromaDB** (open-source vector database) as a Free Edition alternative
# MAGIC to Databricks Vector Search.

# COMMAND ----------

# Option A: ChromaDB with sentence-transformers (works on Free Edition)
import chromadb
from sentence_transformers import SentenceTransformer

# Initialize the embedding model (runs locally, no API key needed)
print("Loading embedding model (this may take a minute)...")
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

# Create ChromaDB collection
chroma_client = chromadb.Client()

# Delete collection if it exists (for re-runs)
try:
    chroma_client.delete_collection("clinical_notes")
except:
    pass

collection = chroma_client.create_collection(
    name="clinical_notes",
    metadata={"hnsw:space": "cosine"}
)

print("✅ Vector store initialized")

# COMMAND ----------

# TODO: Add clinical notes to the vector store
# Hint: Use collection.add(documents=..., ids=..., metadatas=...)

documents = df_notes["text"].tolist()
ids = df_notes["patient_id"].tolist()
metadatas = [
    {"department": row["department"], "note_type": row["note_type"], "date": row["date"]}
    for _, row in df_notes.iterrows()
]

# YOUR CODE HERE - Add documents to ChromaDB
collection.add(
    documents=_____,
    ids=_____,
    metadatas=_____
)

print(f"✅ Added {collection.count()} documents to vector store")

# COMMAND ----------

# TODO: Test semantic search
# Try searching for something like "diabetes management" or "cardiac symptoms"

query = "_____"  # YOUR SEARCH QUERY

results = collection.query(
    query_texts=[query],
    n_results=3
)

print(f"Search results for: '{query}'\n")
for i, (doc, meta, dist) in enumerate(zip(
    results["documents"][0], results["metadatas"][0], results["distances"][0]
)):
    print(f"--- Result {i+1} (similarity: {1-dist:.3f}) ---")
    print(f"Department: {meta['department']} | Type: {meta['note_type']}")
    print(f"Text: {doc[:200]}...")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Build an LLM-Powered Clinical Assistant
# MAGIC
# MAGIC **TODO:** Create a function that:
# MAGIC 1. Takes a clinical question
# MAGIC 2. Searches relevant notes (RAG pattern)
# MAGIC 3. Generates an AI response
# MAGIC
# MAGIC ### Option A: Use Databricks AI Functions (if available)
# MAGIC ### Option B: Use a local/free LLM approach (fallback)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option B: Rule-Based + Template Approach (Free Edition Guaranteed)
# MAGIC
# MAGIC This approach works without any API keys or external LLM services.

# COMMAND ----------

def clinical_assistant(question: str, top_k: int = 3) -> str:
    """
    AI-powered clinical decision support assistant.
    Uses semantic search (RAG) to find relevant notes and generate insights.
    """
    
    # Step 1: Semantic search for relevant notes
    results = collection.query(query_texts=[question], n_results=top_k)
    
    relevant_notes = results["documents"][0]
    metadatas = results["metadatas"][0]
    distances = results["distances"][0]
    
    # Step 2: Extract key information from relevant notes
    # TODO: Enhance this with more sophisticated NLP or LLM calls
    
    response = f"📋 Clinical Decision Support Response\n"
    response += f"{'='*50}\n"
    response += f"Query: {question}\n\n"
    response += f"Found {len(relevant_notes)} relevant clinical records:\n\n"
    
    for i, (note, meta, dist) in enumerate(zip(relevant_notes, metadatas, distances)):
        similarity = 1 - dist
        response += f"--- Record {i+1} (Relevance: {similarity:.1%}) ---\n"
        response += f"Department: {meta['department']} | Type: {meta['note_type']} | Date: {meta['date']}\n"
        response += f"Summary: {note[:300]}\n\n"
    
    # Step 3: Generate recommendations based on findings
    # TODO: This is where you can add LLM integration or more sophisticated logic
    response += f"{'='*50}\n"
    response += f"💡 Key Observations:\n"
    
    # Simple keyword-based analysis (replace with LLM for better results)
    keywords = {
        "diabetes": "Consider HbA1c monitoring, lifestyle modifications, and medication adherence review.",
        "hypertension": "Monitor blood pressure regularly. Consider ACE inhibitor or ARB therapy.",
        "cardiac": "Evaluate with ECG and cardiac biomarkers. Consider cardiology referral.",
        "cancer": "Ensure staging is complete. Discuss treatment options with oncology team.",
        "pain": "Assess pain scale, consider multimodal analgesia approach.",
        "infection": "Review culture results, ensure appropriate antibiotic coverage.",
        "depression": "Screen with PHQ-9, consider SSRI therapy and CBT referral.",
    }
    
    recommendations = []
    for keyword, rec in keywords.items():
        if keyword in question.lower() or any(keyword in note.lower() for note in relevant_notes):
            recommendations.append(f"  • {rec}")
    
    if recommendations:
        response += "\n".join(recommendations)
    else:
        response += "  • Review relevant clinical notes above for detailed patient information.\n"
        response += "  • Consider consulting with the relevant department specialist."
    
    return response

# COMMAND ----------

# TODO: Test your clinical assistant with different queries
# Try questions like:
# - "What patients have diabetes management issues?"
# - "Find cardiac emergencies"
# - "Patients needing medication adjustment"

query = "_____"  # YOUR QUESTION HERE
response = clinical_assistant(query)
print(response)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Build Additional Features (Creativity Points!)
# MAGIC
# MAGIC Ideas to earn more points:
# MAGIC - **Drug interaction checker**: Cross-reference drugs from notes with drug_reviews
# MAGIC - **Department analytics dashboard**: Visualize patient distribution
# MAGIC - **Risk scoring**: Combine clinical notes with heart disease predictions
# MAGIC - **Treatment timeline**: Extract and visualize treatment sequences
# MAGIC - **Quality metrics**: Analyze follow-up compliance

# COMMAND ----------

# TODO: Build your creative feature here!
# This is your chance to stand out from other teams.

# Example: Drug Interaction Analyzer
def analyze_drug_mentions(notes_df, reviews_df):
    """Cross-reference drugs mentioned in notes with review data."""
    # YOUR CODE HERE
    pass

# Example: Department Analytics
def department_dashboard(notes_df):
    """Create visualizations of clinical note patterns."""
    # YOUR CODE HERE
    pass

# YOUR CREATIVE FEATURE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Prepare Your Live Demo
# MAGIC
# MAGIC **TODO:** Prepare a 3-minute live demo for the judges showing:
# MAGIC 1. How your system searches clinical notes
# MAGIC 2. What insights it generates
# MAGIC 3. How it could help clinical decision-making
# MAGIC 4. What makes your approach creative/unique

# COMMAND ----------

# Demo script - Run these cells during your presentation

print("=" * 60)
print("  CLINICAL DECISION SUPPORT SYSTEM")
print(f"  Team: _____")
print("=" * 60)

# Demo Query 1: Department-specific
print("\n🔍 Demo 1: Finding diabetes patients needing attention")
print(clinical_assistant("diabetes management medication adjustment"))

# COMMAND ----------

# Demo Query 2: Emergency scenarios
print("\n🔍 Demo 2: Cardiac emergency protocols")
print(clinical_assistant("chest pain cardiac emergency"))

# COMMAND ----------

# Demo Query 3: Cross-department insights
print("\n🔍 Demo 3: Patients requiring follow-up")
print(clinical_assistant("follow up appointment recheck monitoring"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Completion Checklist
# MAGIC
# MAGIC - [ ] Semantic search engine built and working
# MAGIC - [ ] Clinical notes indexed in vector store
# MAGIC - [ ] AI assistant answers clinical questions
# MAGIC - [ ] At least one creative additional feature
# MAGIC - [ ] Live demo prepared (3 queries ready)
# MAGIC
# MAGIC > 🏁 **Signal judges when ready for your live demo!**
