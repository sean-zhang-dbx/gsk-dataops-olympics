# Databricks notebook source
# Event 5 Capstone Simulation: team_01 does executive briefing + artifact verification

TEAM_NAME = "team_01"
CATALOG = TEAM_NAME
SHARED_CATALOG = "dataops_olympics"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")

# COMMAND ----------

# === Step 1: Verify Artifacts ===
print("=" * 60)
print(f"  ARTIFACT CHECK — {TEAM_NAME}")
print("=" * 60)

checks = {
    "heart_silver (Event 1)": f"{CATALOG}.default.heart_silver",
    "heart_gold (Event 1)": f"{CATALOG}.default.heart_gold",
    "heart_ai_insights (Event 4)": f"{CATALOG}.default.heart_ai_insights",
    "drug_ai_summary (Event 4)": f"{CATALOG}.default.drug_ai_summary",
    "drug_reviews (shared)": f"{SHARED_CATALOG}.default.drug_reviews",
    "clinical_notes (shared)": f"{SHARED_CATALOG}.default.clinical_notes",
}

all_ok = True
for label, fqn in checks.items():
    try:
        cnt = spark.table(fqn).count()
        print(f"  [OK] {label:35s} -> {cnt} rows")
    except Exception as e:
        all_ok = False
        print(f"  [!!] {label:35s} -> MISSING")

# COMMAND ----------

# === Step 2: AI Executive Briefing ===
from pyspark.sql import functions as F

total_patients = spark.table(f"{CATALOG}.default.heart_silver").count()
disease_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.default.heart_silver WHERE target = 1").collect()[0][0]
disease_pct = round(disease_count * 100.0 / total_patients, 1)

gold_summary = spark.table(f"{CATALOG}.default.heart_gold").toPandas().to_string(index=False)
top_drugs = spark.table(f"{CATALOG}.default.drug_ai_summary").select("drug_name", "avg_rating", "reviews").toPandas().to_string(index=False)
notes_count = spark.table(f"{SHARED_CATALOG}.default.clinical_notes").count()

print(f"Patients: {total_patients} ({disease_pct}% with heart disease)")

# COMMAND ----------

briefing_prompt = f"""You are writing a 4-paragraph executive briefing for a hospital Board of Directors meeting.

KEY METRICS:
- Total patients analyzed: {total_patients}
- Heart disease prevalence: {disease_pct}%
- Clinical notes reviewed: {notes_count} across hospital departments

PATIENT COHORT BREAKDOWN:
{gold_summary}

TOP DRUG REVIEWS:
{top_drugs}

Write a professional briefing that:
1. Opens with the headline finding (heart disease prevalence)
2. Highlights the highest-risk age groups and their clinical profiles
3. Summarizes drug review intelligence and any concerns
4. Closes with a recommendation for next steps

Tone: Professional, data-driven, suitable for hospital executives. Cite specific numbers."""

briefing_result = spark.sql(f"""
    SELECT ai_query(
        'databricks-meta-llama-3-3-70b-instruct',
        '{briefing_prompt.replace("'", "''")}'
    ) AS briefing
""").collect()[0]["briefing"]

print("=" * 70)
print("  EXECUTIVE BRIEFING")
print("=" * 70)
print(briefing_result[:500] + "..." if len(briefing_result) > 500 else briefing_result)

# COMMAND ----------

# Save the briefing
from datetime import datetime

briefing_df = spark.createDataFrame([{
    "team": TEAM_NAME,
    "briefing_text": briefing_result,
    "total_patients": total_patients,
    "disease_prevalence_pct": float(disease_pct),
    "generated_at": datetime.now().isoformat(),
}])

briefing_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.default.executive_briefing"
)
print(f"Saved to {CATALOG}.default.executive_briefing")

# COMMAND ----------

# Note: Dashboard and Genie creation are UI-dependent (cannot be automated via notebook)
# The scoring will reflect what can be auto-scored vs manual scoring

print("Event 5 simulation complete for team_01")
print("Dashboard and Genie must be created via UI (manual steps)")
dbutils.notebook.exit("OK")
