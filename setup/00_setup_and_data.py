# Databricks notebook source
# MAGIC %md
# MAGIC # DataOps Olympics — One-Click Setup
# MAGIC
# MAGIC **Run All cells** to set up everything. That's it.
# MAGIC
# MAGIC This notebook will:
# MAGIC 1. Clean up any previous Olympics data (tables in the schema)
# MAGIC 2. Install required Python libraries
# MAGIC 3. Create a Unity Catalog Volume for raw data files
# MAGIC 4. Upload bundled data files from the `data/` folder to the Volume
# MAGIC 5. Create all Delta tables
# MAGIC 6. Install Agent Skills for the Databricks Assistant
# MAGIC 7. Validate everything is ready
# MAGIC
# MAGIC **Estimated time: ~5 minutes**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG_NAME = "dataops_olympics"
SCHEMA_NAME  = "default"
VOLUME_NAME  = "raw_data"
VOLUME_PATH  = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Clean Up Previous Runs

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dataops_olympics;
# MAGIC USE SCHEMA default;

# COMMAND ----------

print("=" * 60)
print("  CLEANUP — Removing previous DataOps Olympics tables")
print("=" * 60)

try:
    tables = spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{SCHEMA_NAME}").collect()
    for t in tables:
        tbl_name = t.tableName
        try:
            spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{tbl_name}")
            print(f"  Dropped table: {tbl_name}")
        except Exception as e:
            print(f"  Could not drop {tbl_name}: {e}")
except Exception as e:
    print(f"  No existing tables to clean: {e}")

print("\n  Cleanup complete.")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Install Libraries

# COMMAND ----------

# MAGIC %pip install plotly chromadb sentence-transformers --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

CATALOG_NAME = "dataops_olympics"
SCHEMA_NAME  = "default"
VOLUME_NAME  = "raw_data"
VOLUME_PATH  = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Volume for Raw Data

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}")
print(f"  Volume ready: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Stage Data Files to Volume
# MAGIC
# MAGIC Copies bundled CSV/JSON files from the repo `data/` folder to the UC Volume.

# COMMAND ----------

import os, shutil, json
import pandas as pd
import numpy as np

sub_dirs = ["heart_disease", "diabetes", "life_expectancy", "drug_reviews", "clinical_notes"]
for d in sub_dirs:
    os.makedirs(f"{VOLUME_PATH}/{d}", exist_ok=True)

_nb_dir = os.getcwd()
REPO_DATA_DIR = None

candidates = [
    os.path.join(_nb_dir, "..", "data"),
    os.path.join(_nb_dir, "data"),
]

try:
    user_email = spark.sql("SELECT current_user()").collect()[0][0]
    candidates += [
        f"/Workspace/Repos/{user_email}/gsk-dataops-olympics/data",
        f"/Workspace/Users/{user_email}/gsk-dataops-olympics/data",
    ]
except Exception as e:
    print(f"  Could not detect user email: {e}")

for c in candidates:
    if os.path.isdir(c) and os.path.isfile(os.path.join(c, "heart_disease.csv")):
        REPO_DATA_DIR = os.path.abspath(c)
        break

print(f"  Repo data/ found: {REPO_DATA_DIR or 'NOT FOUND (will generate synthetic data)'}")

# COMMAND ----------

if REPO_DATA_DIR:
    copy_map = {
        "heart_disease.csv":           f"{VOLUME_PATH}/heart_disease/heart.csv",
        "heart_disease_batch_1.csv":   f"{VOLUME_PATH}/heart_disease/heart_disease_batch_1.csv",
        "heart_disease_batch_2.csv":   f"{VOLUME_PATH}/heart_disease/heart_disease_batch_2.csv",
        "heart_disease_batch_3.csv":   f"{VOLUME_PATH}/heart_disease/heart_disease_batch_3.csv",
        "diabetes_readmission.csv":    f"{VOLUME_PATH}/diabetes/diabetes.csv",
        "life_expectancy.csv":         f"{VOLUME_PATH}/life_expectancy/life_expectancy.csv",
        "life_expectancy_sample.json": f"{VOLUME_PATH}/life_expectancy/life_expectancy_sample.json",
        "drug_reviews.csv":            f"{VOLUME_PATH}/drug_reviews/drug_reviews.csv",
        "clinical_notes.json":         f"{VOLUME_PATH}/clinical_notes/clinical_notes.json",
    }
    copied = 0
    for src_name, dst_path in copy_map.items():
        src_path = os.path.join(REPO_DATA_DIR, src_name)
        if os.path.isfile(src_path):
            shutil.copy2(src_path, dst_path)
            copied += 1
    print(f"  Copied {copied}/{len(copy_map)} files from repo data/ to Volume")
else:
    print("  Generating synthetic data (repo data/ not found)...")

    np.random.seed(42)

    # Heart Disease
    heart_path = f"{VOLUME_PATH}/heart_disease/heart.csv"
    n = 500
    df_h = pd.DataFrame({
        "age": np.random.randint(29, 77, n), "sex": np.random.randint(0, 2, n),
        "cp": np.random.randint(0, 4, n), "trestbps": np.random.randint(94, 200, n),
        "chol": np.random.randint(126, 564, n), "fbs": np.random.randint(0, 2, n),
        "restecg": np.random.randint(0, 3, n), "thalach": np.random.randint(71, 202, n),
        "exang": np.random.randint(0, 2, n),
        "oldpeak": np.round(np.random.uniform(0, 6.2, n), 1),
        "slope": np.random.randint(0, 3, n), "ca": np.random.randint(0, 4, n),
        "thal": np.random.choice([3, 6, 7], n), "target": np.random.randint(0, 2, n),
    })
    df_h.to_csv(heart_path, index=False)
    for batch in range(1, 4):
        b = df_h.sample(50, random_state=batch)
        b.loc[b.sample(3, random_state=batch).index, "age"] = -1
        b.loc[b.sample(2, random_state=batch+10).index, "trestbps"] = 999
        b.to_csv(f"{VOLUME_PATH}/heart_disease/heart_disease_batch_{batch}.csv", index=False)
    print(f"    Generated heart_disease.csv + 3 batches")

    # Diabetes / Readmission
    n = 768
    df_d = pd.DataFrame({
        "pregnancies": np.random.randint(0, 17, n),
        "glucose": np.random.randint(44, 199, n),
        "blood_pressure": np.random.randint(24, 122, n),
        "skin_thickness": np.random.randint(7, 99, n),
        "insulin": np.random.randint(14, 846, n),
        "bmi": np.round(np.random.uniform(18.2, 67.1, n), 1),
        "diabetes_pedigree": np.round(np.random.uniform(0.078, 2.42, n), 3),
        "age": np.random.randint(21, 81, n),
    })
    risk = ((df_d["glucose"] > 140).astype(float) * 0.3 +
            (df_d["bmi"] > 30).astype(float) * 0.25 +
            (df_d["age"] > 45).astype(float) * 0.15 +
            (df_d["diabetes_pedigree"] > 1.0).astype(float) * 0.15 +
            (df_d["pregnancies"] > 5).astype(float) * 0.1 +
            np.random.uniform(0, 0.3, n))
    df_d["readmission_risk"] = (risk > 0.45).astype(int)
    df_d.to_csv(f"{VOLUME_PATH}/diabetes/diabetes.csv", index=False)
    print(f"    Generated diabetes.csv")

    # Life Expectancy
    countries = ["India","United States","United Kingdom","Germany","France","Brazil",
                 "Japan","China","Australia","South Africa","Nigeria","Mexico",
                 "Canada","Italy","Spain","Russia","South Korea","Indonesia","Turkey","Thailand"]
    rows = []
    for c in countries:
        base = np.random.uniform(55, 82)
        for y in range(2000, 2024):
            rows.append({
                "country": c, "year": y,
                "life_expectancy": round(base + (y-2000)*0.2 + np.random.normal(0,1), 1),
                "adult_mortality": round(np.random.uniform(50,350),1),
                "infant_deaths": np.random.randint(0,500),
                "alcohol_consumption": round(np.random.uniform(0.01,17),2),
                "health_expenditure_pct": round(np.random.uniform(1,18),2),
                "hepatitis_b_coverage": round(np.random.uniform(10,99),1),
                "measles_cases": np.random.randint(0,50000),
                "bmi": round(np.random.uniform(18,65),1),
                "under_five_deaths": np.random.randint(0,300),
                "polio_coverage": round(np.random.uniform(30,99),1),
                "total_expenditure": round(np.random.uniform(1,18),2),
                "diphtheria_coverage": round(np.random.uniform(30,99),1),
                "hiv_aids": round(np.random.uniform(0.1,30),1),
                "gdp_per_capita": round(np.random.uniform(200,80000),2),
                "population": np.random.randint(100000,1500000000),
                "schooling": round(np.random.uniform(2,20),1),
                "status": np.random.choice(["Developing","Developed"], p=[0.7,0.3]),
            })
    df_l = pd.DataFrame(rows)
    df_l.to_csv(f"{VOLUME_PATH}/life_expectancy/life_expectancy.csv", index=False)
    df_l.head(100).to_json(f"{VOLUME_PATH}/life_expectancy/life_expectancy_sample.json", orient="records", indent=2)
    print(f"    Generated life_expectancy.csv + JSON sample")

    # Drug Reviews
    drugs = ["Metformin","Lisinopril","Atorvastatin","Amlodipine","Omeprazole",
             "Metoprolol","Losartan","Gabapentin","Hydrochlorothiazide","Sertraline",
             "Levothyroxine","Acetaminophen","Ibuprofen","Amoxicillin","Prednisone"]
    conditions = ["Type 2 Diabetes","Hypertension","High Cholesterol","Chest Pain",
                  "GERD","Heart Failure","Blood Pressure","Nerve Pain","Fluid Retention",
                  "Depression","Hypothyroidism","Pain","Inflammation","Bacterial Infection","Asthma"]
    review_texts = [
        "This medication has been very effective for me with minimal side effects.",
        "I experienced some dizziness at first but it went away after a week.",
        "Great improvement in my condition since starting this drug.",
        "Side effects were too severe, had to switch medications.",
        "My doctor recommended this and I am glad they did. Feeling much better.",
        "The generic version works just as well for me.",
        "I have been on this for 3 months now with good results.",
        "Excellent medication with very few side effects in my experience.",
        "Works well in combination with my other medications.",
    ]
    n = 1000
    df_r = pd.DataFrame({
        "drug_name": np.random.choice(drugs, n),
        "condition": np.random.choice(conditions, n),
        "review": np.random.choice(review_texts, n),
        "rating": np.random.randint(1, 11, n),
        "date": pd.date_range("2020-01-01", periods=n, freq="8h").strftime("%Y-%m-%d").tolist(),
        "useful_count": np.random.randint(0, 200, n),
    })
    df_r.to_csv(f"{VOLUME_PATH}/drug_reviews/drug_reviews.csv", index=False)
    print(f"    Generated drug_reviews.csv")

    # Clinical Notes
    notes = []
    depts = ["Cardiology","Oncology","Neurology","Emergency","Pediatrics"]
    note_types = ["Admission Note","Progress Note","Discharge Summary","Consultation"]
    for i in range(20):
        notes.append({
            "note_id": f"NOTE_{i+1:03d}",
            "patient_id": f"PAT_{np.random.randint(100,999)}",
            "department": depts[i % len(depts)],
            "note_type": note_types[i % len(note_types)],
            "date": f"2024-{(i%12)+1:02d}-{(i%28)+1:02d}",
            "text": f"Patient presents with symptoms consistent with {depts[i%len(depts)].lower()} condition. "
                    f"Vitals stable. Treatment plan discussed with patient and family. "
                    f"Follow-up scheduled in {np.random.choice([1,2,3,4])} weeks.",
            "physician": f"Dr. {'Smith Jones Patel Chen Williams'.split()[i%5]}",
        })
    with open(f"{VOLUME_PATH}/clinical_notes/clinical_notes.json", "w") as f:
        json.dump(notes, f, indent=2)
    print(f"    Generated clinical_notes.json")

print("\n  Data staging complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Delta Tables

# COMMAND ----------

print("Creating Delta tables...\n")

df = pd.read_csv(f"{VOLUME_PATH}/heart_disease/heart.csv")
sdf = spark.createDataFrame(df)
sdf.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.heart_disease")
print(f"  heart_disease: {sdf.count()} rows")

# COMMAND ----------

df = pd.read_csv(f"{VOLUME_PATH}/diabetes/diabetes.csv")
sdf = spark.createDataFrame(df)
sdf.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.diabetes_readmission")
print(f"  diabetes_readmission: {sdf.count()} rows")

# COMMAND ----------

df = pd.read_csv(f"{VOLUME_PATH}/life_expectancy/life_expectancy.csv")
sdf = spark.createDataFrame(df)
sdf.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.life_expectancy")
print(f"  life_expectancy: {sdf.count()} rows")

# COMMAND ----------

df = pd.read_csv(f"{VOLUME_PATH}/drug_reviews/drug_reviews.csv")
sdf = spark.createDataFrame(df)
sdf.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.drug_reviews")
print(f"  drug_reviews: {sdf.count()} rows")

# COMMAND ----------

import json as _json
with open(f"{VOLUME_PATH}/clinical_notes/clinical_notes.json", "r") as _f:
    _notes = _json.load(_f)
import pandas as _pd
_df_notes = _pd.DataFrame(_notes)
if "note_id" not in _df_notes.columns:
    _df_notes.insert(0, "note_id", [f"NOTE_{i+1:03d}" for i in range(len(_df_notes))])
sdf = spark.createDataFrame(_df_notes)
sdf.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.clinical_notes")
print(f"  clinical_notes: {sdf.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Install Agent Skills for Databricks Assistant
# MAGIC
# MAGIC Downloads skills from the [ai-dev-kit](https://github.com/databricks-solutions/ai-dev-kit)
# MAGIC repo and installs them into your workspace so the **Databricks Assistant Agent mode**
# MAGIC can build dashboards, pipelines, Genie spaces, and more.

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/ai-dev-kit
# MAGIC git clone --depth 1 https://github.com/databricks-solutions/ai-dev-kit.git /tmp/ai-dev-kit 2>&1 | tail -1

# COMMAND ----------

import os, shutil

user_email = spark.sql("SELECT current_user()").collect()[0][0]
skills_target = f"/Workspace/Users/{user_email}/.assistant/skills"

SKILLS_TO_INSTALL = [
    "databricks-spark-declarative-pipelines",
    "databricks-unity-catalog",
    "databricks-aibi-dashboards",
    "databricks-genie",
    "databricks-agent-bricks",
    "databricks-model-serving",
    "databricks-dbsql",
    "databricks-vector-search",
    "databricks-mlflow-evaluation",
    "databricks-jobs",
    "databricks-docs",
]

skills_source = "/tmp/ai-dev-kit/databricks-skills"

print("=" * 60)
print("  AGENT SKILLS — Installing to Databricks Assistant")
print("=" * 60)
print(f"  Target: {skills_target}")
print()

installed = 0
for skill_name in SKILLS_TO_INSTALL:
    src = os.path.join(skills_source, skill_name)
    dst = os.path.join(skills_target, skill_name)
    if not os.path.isdir(src):
        print(f"  SKIP  {skill_name} (not found in repo)")
        continue
    try:
        if os.path.exists(dst):
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
        installed += 1
        print(f"  OK    {skill_name}")
    except Exception as e:
        print(f"  FAIL  {skill_name}: {e}")

print(f"\n  Installed {installed}/{len(SKILLS_TO_INSTALL)} skills")
print(f"\n  To use: open the Assistant sidebar → switch to 'Agent' mode")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Validation

# COMMAND ----------

print("=" * 60)
print("  SETUP COMPLETE — VALIDATION REPORT")
print("=" * 60)

expected_tables = {
    "heart_disease": 500,
    "diabetes_readmission": 768,
    "life_expectancy": 480,
    "drug_reviews": 1000,
    "clinical_notes": 20,
}

all_ok = True
print("\n  TABLES:")
for tbl, expected in expected_tables.items():
    try:
        actual = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG_NAME}.{SCHEMA_NAME}.{tbl}").collect()[0].cnt
        status = "OK" if actual >= expected * 0.9 else "LOW"
        if status != "OK":
            all_ok = False
        print(f"    {tbl:30s} {actual:>6,} rows  [{status}]")
    except Exception as e:
        print(f"    {tbl:30s}  ERROR: {str(e)[:50]}")
        all_ok = False

print(f"\n  VOLUME FILES:")
import os
total_files = 0
for root, dirs, files in os.walk(VOLUME_PATH):
    for f in files:
        total_files += 1
print(f"    {total_files} files in {VOLUME_PATH}")

print(f"\n  KEY FILE PATHS FOR EVENTS:")
print(f"    Heart CSV:            {VOLUME_PATH}/heart_disease/heart.csv")
print(f"    Heart Batches:        {VOLUME_PATH}/heart_disease/heart_disease_batch_*.csv")
print(f"    Life Expectancy JSON: {VOLUME_PATH}/life_expectancy/life_expectancy_sample.json")
print(f"    Drug Reviews CSV:     {VOLUME_PATH}/drug_reviews/drug_reviews.csv")
print(f"    Clinical Notes JSON:  {VOLUME_PATH}/clinical_notes/clinical_notes.json")

print(f"\n  CATALOG.SCHEMA: {CATALOG_NAME}.{SCHEMA_NAME}")

print(f"\n{'=' * 60}")
if all_ok:
    print("  READY FOR DATAOPS OLYMPICS!")
else:
    print("  ISSUES DETECTED — check table errors above")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Reference Card
# MAGIC
# MAGIC ### Available Tables
# MAGIC | Table | Rows | Description | Used In |
# MAGIC |-------|------|-------------|---------|
# MAGIC | `heart_disease` | ~500 | UCI heart disease clinical data | Events 1, 2, Warm-Up |
# MAGIC | `diabetes_readmission` | 768 | Diabetes readmission risk | Event 3 |
# MAGIC | `life_expectancy` | ~480 | WHO health indicators by country/year | Event 1 |
# MAGIC | `drug_reviews` | 1,000 | Drug review ratings and text | Events 4, 5 |
# MAGIC | `clinical_notes` | 20 | Synthetic clinical notes | Events 4, 5 |
# MAGIC
# MAGIC ### Raw Files (in Unity Catalog Volume)
# MAGIC | File | Path |
# MAGIC |------|------|
# MAGIC | Heart Disease CSV | `/Volumes/dataops_olympics/default/raw_data/heart_disease/heart.csv` |
# MAGIC | Heart Batches 1-3 | `/Volumes/dataops_olympics/default/raw_data/heart_disease/heart_disease_batch_*.csv` |
# MAGIC | Life Expectancy JSON | `/Volumes/dataops_olympics/default/raw_data/life_expectancy/life_expectancy_sample.json` |
# MAGIC | Drug Reviews CSV | `/Volumes/dataops_olympics/default/raw_data/drug_reviews/drug_reviews.csv` |
# MAGIC | Clinical Notes JSON | `/Volumes/dataops_olympics/default/raw_data/clinical_notes/clinical_notes.json` |
# MAGIC
# MAGIC ### Useful SQL
# MAGIC ```sql
# MAGIC USE CATALOG dataops_olympics;
# MAGIC USE SCHEMA default;
# MAGIC SHOW TABLES;
# MAGIC DESCRIBE TABLE heart_disease;
# MAGIC SELECT * FROM heart_disease LIMIT 10;
# MAGIC DESCRIBE HISTORY heart_disease;
# MAGIC LIST '/Volumes/dataops_olympics/default/raw_data/';
# MAGIC ```
