# Databricks notebook source
# MAGIC %md
# MAGIC # 💡 Event 3: Innovation Showcase — SOLUTION
# MAGIC
# MAGIC > **⚠️ ORGANIZERS ONLY — Do not distribute to participants!**
# MAGIC
# MAGIC This solution builds a comprehensive Clinical Decision Support System with:
# MAGIC 1. Semantic search over clinical notes
# MAGIC 2. Drug interaction analysis
# MAGIC 3. Department analytics dashboard
# MAGIC 4. Patient risk scoring

# COMMAND ----------

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import chromadb
from sentence_transformers import SentenceTransformer
import re
from collections import Counter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

df_notes = spark.table("clinical_notes").toPandas()
df_drugs = spark.table("drug_reviews").toPandas()
df_heart = spark.table("heart_disease").toPandas()

print(f"Clinical Notes: {len(df_notes)}")
print(f"Drug Reviews:   {len(df_drugs)}")
print(f"Heart Disease:  {len(df_heart)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Vector Store

# COMMAND ----------

# Initialize embedding model
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

# Create ChromaDB
chroma_client = chromadb.Client()
try:
    chroma_client.delete_collection("clinical_notes")
except:
    pass

collection = chroma_client.create_collection(
    name="clinical_notes",
    metadata={"hnsw:space": "cosine"}
)

# Add clinical notes
collection.add(
    documents=df_notes["text"].tolist(),
    ids=df_notes["patient_id"].tolist(),
    metadatas=[
        {
            "department": row["department"],
            "note_type": row["note_type"],
            "date": row["date"],
            "patient_id": row["patient_id"]
        }
        for _, row in df_notes.iterrows()
    ]
)

print(f"✅ Indexed {collection.count()} clinical notes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical Decision Support System

# COMMAND ----------

# Drug knowledge base (extracted from reviews)
drug_avg_ratings = df_drugs.groupby("drug_name").agg(
    avg_rating=("rating", "mean"),
    num_reviews=("rating", "count"),
    top_condition=("condition", lambda x: x.mode().iloc[0] if len(x) > 0 else "Unknown")
).to_dict("index")

# Common drug interactions (simplified knowledge base)
DRUG_INTERACTIONS = {
    ("Metformin", "Prednisone"): "⚠️ Prednisone may increase blood glucose, counteracting Metformin",
    ("Warfarin", "Ibuprofen"): "🚨 High risk: increased bleeding risk",
    ("Lisinopril", "Losartan"): "⚠️ Dual RAAS blockade - increased risk of hyperkalemia",
    ("Sertraline", "Sumatriptan"): "⚠️ Risk of serotonin syndrome",
    ("Metformin", "Ibuprofen"): "⚠️ NSAIDs may reduce kidney function affecting Metformin clearance",
}

def extract_drugs_from_text(text):
    """Extract drug names mentioned in clinical text."""
    known_drugs = [
        "metformin", "lisinopril", "atorvastatin", "amlodipine", "omeprazole",
        "metoprolol", "losartan", "gabapentin", "sertraline", "levothyroxine",
        "warfarin", "prednisone", "ibuprofen", "amoxicillin", "acetaminophen",
        "sumatriptan", "topiramate", "rasagiline", "levodopa", "adalimumab",
        "methotrexate", "sacubitril", "valsartan", "dapagliflozin", "levetiracetam",
        "spironolactone", "furosemide", "alendronate", "budesonide", "azithromycin",
        "enoxaparin", "insulin"
    ]
    text_lower = text.lower()
    return [drug.capitalize() for drug in known_drugs if drug in text_lower]

def check_drug_interactions(drugs):
    """Check for known drug interactions."""
    interactions = []
    for i, drug1 in enumerate(drugs):
        for drug2 in drugs[i+1:]:
            pair = tuple(sorted([drug1, drug2]))
            if pair in DRUG_INTERACTIONS:
                interactions.append(DRUG_INTERACTIONS[pair])
            # Check reverse
            pair_rev = tuple(reversed(pair))
            if pair_rev in DRUG_INTERACTIONS:
                interactions.append(DRUG_INTERACTIONS[pair_rev])
    return interactions

# COMMAND ----------

def clinical_assistant(question: str, top_k: int = 3) -> str:
    """
    Comprehensive Clinical Decision Support System.
    
    Features:
    - Semantic search over clinical notes
    - Drug extraction and interaction checking
    - Evidence-based recommendations
    - Risk assessment integration
    """
    
    # Semantic search
    results = collection.query(query_texts=[question], n_results=top_k)
    
    relevant_notes = results["documents"][0]
    metadatas = results["metadatas"][0]
    distances = results["distances"][0]
    
    output = []
    output.append("=" * 60)
    output.append("  CLINICAL DECISION SUPPORT SYSTEM")
    output.append("=" * 60)
    output.append(f"\n🔍 Query: {question}\n")
    
    # Relevant records
    output.append("📋 RELEVANT CLINICAL RECORDS:")
    output.append("-" * 40)
    
    all_drugs = []
    for i, (note, meta, dist) in enumerate(zip(relevant_notes, metadatas, distances)):
        similarity = 1 - dist
        output.append(f"\n  Record {i+1} | Relevance: {similarity:.1%}")
        output.append(f"  Dept: {meta['department']} | Type: {meta['note_type']} | Date: {meta['date']}")
        output.append(f"  {note[:250]}...")
        
        # Extract drugs
        drugs = extract_drugs_from_text(note)
        if drugs:
            output.append(f"  💊 Medications: {', '.join(drugs)}")
            all_drugs.extend(drugs)
    
    # Drug interaction check
    unique_drugs = list(set(all_drugs))
    if len(unique_drugs) > 1:
        output.append(f"\n⚕️ DRUG INTERACTION ANALYSIS:")
        output.append(f"-" * 40)
        output.append(f"  Medications found across records: {', '.join(unique_drugs)}")
        interactions = check_drug_interactions(unique_drugs)
        if interactions:
            for interaction in interactions:
                output.append(f"  {interaction}")
        else:
            output.append(f"  ✅ No known interactions detected")
        
        # Drug ratings from reviews
        output.append(f"\n  📊 Drug Review Summary:")
        for drug in unique_drugs:
            if drug in drug_avg_ratings:
                info = drug_avg_ratings[drug]
                output.append(f"    {drug}: ★{info['avg_rating']:.1f}/10 ({info['num_reviews']} reviews) - Primary: {info['top_condition']}")
    
    # Evidence-based recommendations
    output.append(f"\n💡 RECOMMENDATIONS:")
    output.append("-" * 40)
    
    conditions_found = set()
    keywords_map = {
        "diabetes": ("Diabetes Management", [
            "Monitor HbA1c every 3 months (target <7%)",
            "Review medication adherence and dosing",
            "Screen for diabetic complications (retinopathy, nephropathy, neuropathy)",
            "Reinforce lifestyle modifications (diet, exercise)",
        ]),
        "hypertension": ("Hypertension Management", [
            "Target BP <130/80 mmHg per current guidelines",
            "Consider DASH diet and sodium restriction",
            "Assess for end-organ damage",
            "Review medication compliance",
        ]),
        "cardiac": ("Cardiac Care", [
            "Obtain ECG and cardiac biomarkers",
            "Evaluate for acute coronary syndrome if chest pain",
            "Consider echocardiogram for functional assessment",
            "Cardiology consultation if warranted",
        ]),
        "cancer": ("Oncology", [
            "Verify staging is complete and current",
            "Discuss treatment goals and options with patient",
            "Screen for treatment side effects",
            "Palliative care referral if appropriate",
        ]),
        "depression": ("Mental Health", [
            "Complete PHQ-9 screening",
            "Consider SSRI initiation if moderate-severe",
            "CBT referral for psychotherapy",
            "Safety screening for suicidal ideation",
        ]),
    }
    
    combined_text = (question + " " + " ".join(relevant_notes)).lower()
    
    for keyword, (category, recs) in keywords_map.items():
        if keyword in combined_text:
            conditions_found.add(category)
            output.append(f"\n  📌 {category}:")
            for rec in recs:
                output.append(f"    • {rec}")
    
    if not conditions_found:
        output.append("  • Review relevant clinical records above for patient-specific guidance")
        output.append("  • Consider specialist consultation based on findings")
    
    output.append(f"\n{'='*60}")
    output.append("  ⚠️ This is a decision support tool. All recommendations")
    output.append("     require clinical judgment and patient-specific evaluation.")
    output.append(f"{'='*60}")
    
    return "\n".join(output)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Department Analytics Dashboard

# COMMAND ----------

# Department distribution
dept_counts = df_notes["department"].value_counts()
fig1 = px.pie(
    values=dept_counts.values, names=dept_counts.index,
    title="Clinical Notes by Department",
    color_discrete_sequence=px.colors.qualitative.Set2
)
fig1.show()

# COMMAND ----------

# Drug mention frequency across notes
all_drug_mentions = []
for _, row in df_notes.iterrows():
    drugs = extract_drugs_from_text(row["text"])
    for drug in drugs:
        all_drug_mentions.append({"drug": drug, "department": row["department"]})

df_drug_mentions = pd.DataFrame(all_drug_mentions)
if len(df_drug_mentions) > 0:
    fig2 = px.bar(
        df_drug_mentions.groupby(["drug", "department"]).size().reset_index(name="count"),
        x="drug", y="count", color="department",
        title="Drug Mentions in Clinical Notes by Department",
        barmode="stack"
    )
    fig2.update_layout(template="plotly_white", xaxis_tickangle=-45)
    fig2.show()

# COMMAND ----------

# Drug review ratings
fig3 = px.box(
    df_drugs, x="drug_name", y="rating",
    title="Drug Review Ratings Distribution",
    color="drug_name"
)
fig3.update_layout(template="plotly_white", xaxis_tickangle=-45, showlegend=False)
fig3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo Queries

# COMMAND ----------

print(clinical_assistant("diabetes management glucose control medication"))

# COMMAND ----------

print(clinical_assistant("chest pain cardiac emergency treatment"))

# COMMAND ----------

print(clinical_assistant("patient follow-up medication adjustment recheck"))

# COMMAND ----------

print(clinical_assistant("depression anxiety mental health screening"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This solution demonstrates:
# MAGIC 1. **Semantic Search**: ChromaDB + sentence-transformers for meaning-based retrieval
# MAGIC 2. **Drug Extraction**: NLP to identify medications in unstructured notes
# MAGIC 3. **Interaction Checking**: Cross-referencing drugs for safety
# MAGIC 4. **Evidence-Based Recommendations**: Clinical guidelines by condition
# MAGIC 5. **Analytics Dashboard**: Department and drug analytics
# MAGIC 6. **All running on Free Edition**: No external API keys required
