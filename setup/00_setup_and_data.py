# Databricks notebook source
# MAGIC %md
# MAGIC # DataOps Olympics — One-Click Setup
# MAGIC
# MAGIC **Run All cells** to set up everything. That's it.
# MAGIC
# MAGIC This notebook will:
# MAGIC 1. Clean up any previous Olympics data (tables, schema, staging files)
# MAGIC 2. Install required Python libraries
# MAGIC 3. Create catalog/schema (Unity Catalog or hive_metastore fallback)
# MAGIC 4. Stage bundled data files from the `data/` folder
# MAGIC 5. Create all Delta tables
# MAGIC 6. Validate everything is ready
# MAGIC
# MAGIC **Estimated time: ~3 minutes**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Edit the values below if needed, then **Run All**.

# COMMAND ----------

CATALOG_NAME = "hive_metastore"   # Change to a UC catalog name if available
SCHEMA_NAME  = "dataops_olympics"
NUM_TEAMS    = 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Clean Up Previous Runs

# COMMAND ----------

print("=" * 60)
print("  CLEANUP — Removing previous DataOps Olympics data")
print("=" * 60)

# Drop the schema (and all tables in it)
if CATALOG_NAME == "hive_metastore":
    try:
        tables = spark.sql(f"SHOW TABLES IN {SCHEMA_NAME}").collect()
        for t in tables:
            spark.sql(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.{t.tableName}")
            print(f"  Dropped table: {SCHEMA_NAME}.{t.tableName}")
        spark.sql(f"DROP DATABASE IF EXISTS {SCHEMA_NAME}")
        print(f"  Dropped database: {SCHEMA_NAME}")
    except Exception as e:
        print(f"  No existing database to clean: {SCHEMA_NAME}")
else:
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME} CASCADE")
        print(f"  Dropped schema: {CATALOG_NAME}.{SCHEMA_NAME}")
    except Exception as e:
        print(f"  No existing schema to clean: {CATALOG_NAME}.{SCHEMA_NAME}")

# Clear staging files
import shutil, os
staging_dir = "/tmp/dataops_olympics"
if os.path.exists(staging_dir):
    shutil.rmtree(staging_dir)
    print(f"  Cleared staging directory: {staging_dir}")
else:
    print(f"  Staging directory already clean: {staging_dir}")

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

# Re-import config after Python restart
CATALOG_NAME = "hive_metastore"
SCHEMA_NAME  = "dataops_olympics"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Schema

# COMMAND ----------

if CATALOG_NAME != "hive_metastore":
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
        spark.sql(f"USE CATALOG {CATALOG_NAME}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
        spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
        USING_UC = True
        print(f"  Using Unity Catalog: {CATALOG_NAME}.{SCHEMA_NAME}")
    except Exception as e:
        print(f"  Unity Catalog not available, falling back to hive_metastore")
        CATALOG_NAME = "hive_metastore"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA_NAME}")
        spark.sql(f"USE {SCHEMA_NAME}")
        USING_UC = False
else:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA_NAME}")
    spark.sql(f"USE {SCHEMA_NAME}")
    USING_UC = False
    print(f"  Using hive_metastore.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Stage Data Files
# MAGIC
# MAGIC Copies bundled CSV/JSON files from the repo `data/` folder to a local staging path
# MAGIC so all event notebooks have a consistent file location.

# COMMAND ----------

import os, shutil, json
import pandas as pd
import numpy as np

LOCAL_DATA_DIR = "/tmp/dataops_olympics/raw"

os.makedirs(f"{LOCAL_DATA_DIR}/heart_disease", exist_ok=True)
os.makedirs(f"{LOCAL_DATA_DIR}/diabetes", exist_ok=True)
os.makedirs(f"{LOCAL_DATA_DIR}/life_expectancy", exist_ok=True)
os.makedirs(f"{LOCAL_DATA_DIR}/drug_reviews", exist_ok=True)
os.makedirs(f"{LOCAL_DATA_DIR}/clinical_notes", exist_ok=True)

# Auto-detect the repo data/ folder
_nb_dir = os.getcwd()
REPO_DATA_DIR = None

candidates = [
    os.path.join(_nb_dir, "..", "data"),
    os.path.join(_nb_dir, "data"),
]

# Also check common Workspace/Repos paths
try:
    user_email = spark.sql("SELECT current_user()").collect()[0][0]
    candidates += [
        f"/Workspace/Repos/{user_email}/gsk-dataops-olympics/data",
        f"/Workspace/Users/{user_email}/gsk-dataops-olympics/data",
    ]
except:
    pass

for c in candidates:
    if os.path.isdir(c) and os.path.isfile(os.path.join(c, "heart_disease.csv")):
        REPO_DATA_DIR = os.path.abspath(c)
        break

print(f"  Repo data/ found: {REPO_DATA_DIR or 'NOT FOUND (will generate synthetic data)'}")

# COMMAND ----------

# Copy bundled data to staging, or generate if not found
if REPO_DATA_DIR:
    copy_map = {
        "heart_disease.csv":           f"{LOCAL_DATA_DIR}/heart_disease/heart.csv",
        "heart_disease_batch_1.csv":   f"{LOCAL_DATA_DIR}/heart_disease/heart_disease_batch_1.csv",
        "heart_disease_batch_2.csv":   f"{LOCAL_DATA_DIR}/heart_disease/heart_disease_batch_2.csv",
        "heart_disease_batch_3.csv":   f"{LOCAL_DATA_DIR}/heart_disease/heart_disease_batch_3.csv",
        "diabetes_readmission.csv":    f"{LOCAL_DATA_DIR}/diabetes/diabetes.csv",
        "life_expectancy.csv":         f"{LOCAL_DATA_DIR}/life_expectancy/life_expectancy.csv",
        "life_expectancy_sample.json": f"{LOCAL_DATA_DIR}/life_expectancy/life_expectancy_sample.json",
        "drug_reviews.csv":            f"{LOCAL_DATA_DIR}/drug_reviews/drug_reviews.csv",
        "clinical_notes.json":         f"{LOCAL_DATA_DIR}/clinical_notes/clinical_notes.json",
    }
    copied = 0
    for src_name, dst_path in copy_map.items():
        src_path = os.path.join(REPO_DATA_DIR, src_name)
        if os.path.isfile(src_path):
            shutil.copy2(src_path, dst_path)
            copied += 1
    print(f"  Copied {copied}/{len(copy_map)} files from repo data/ to staging")
else:
    print("  Generating synthetic data (repo data/ not found)...")

np.random.seed(42)

# Heart Disease
heart_path = f"{LOCAL_DATA_DIR}/heart_disease/heart.csv"
if not os.path.isfile(heart_path):
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
        b.to_csv(f"{LOCAL_DATA_DIR}/heart_disease/heart_disease_batch_{batch}.csv", index=False)
    print(f"    Generated heart_disease.csv + 3 batches")

# Diabetes / Readmission
diab_path = f"{LOCAL_DATA_DIR}/diabetes/diabetes.csv"
if not os.path.isfile(diab_path):
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
    df_d.to_csv(diab_path, index=False)
    print(f"    Generated diabetes.csv")

# Life Expectancy
life_path = f"{LOCAL_DATA_DIR}/life_expectancy/life_expectancy.csv"
if not os.path.isfile(life_path):
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
    df_l.to_csv(life_path, index=False)
    df_l.head(100).to_json(
        f"{LOCAL_DATA_DIR}/life_expectancy/life_expectancy_sample.json", orient="records", indent=2)
    print(f"    Generated life_expectancy.csv + JSON sample")

# Drug Reviews
reviews_path = f"{LOCAL_DATA_DIR}/drug_reviews/drug_reviews.csv"
if not os.path.isfile(reviews_path):
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
    df_r.to_csv(reviews_path, index=False)
    print(f"    Generated drug_reviews.csv")

# Clinical Notes
notes_path = f"{LOCAL_DATA_DIR}/clinical_notes/clinical_notes.json"
if not os.path.isfile(notes_path):
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
    with open(notes_path, "w") as f:
        json.dump(notes, f, indent=2)
    print(f"    Generated clinical_notes.json")

print("\n  Data staging complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Delta Tables

# COMMAND ----------

print("Creating Delta tables...\n")

# Heart Disease
df = pd.read_csv(f"{LOCAL_DATA_DIR}/heart_disease/heart.csv")
sdf = spark.createDataFrame(df)
sdf.write.format("delta").mode("overwrite").saveAsTable("heart_disease")
cnt = sdf.count()
print(f"  heart_disease: {cnt} rows")

# COMMAND ----------

# Diabetes / Readmission
df = pd.read_csv(f"{LOCAL_DATA_DIR}/diabetes/diabetes.csv")
sdf = spark.createDataFrame(df)
sdf.write.format("delta").mode("overwrite").saveAsTable("diabetes_readmission")
cnt = sdf.count()
print(f"  diabetes_readmission: {cnt} rows")

# COMMAND ----------

# Life Expectancy
df = pd.read_csv(f"{LOCAL_DATA_DIR}/life_expectancy/life_expectancy.csv")
sdf = spark.createDataFrame(df)
sdf.write.format("delta").mode("overwrite").saveAsTable("life_expectancy")
cnt = sdf.count()
print(f"  life_expectancy: {cnt} rows")

# COMMAND ----------

# Drug Reviews
df = pd.read_csv(f"{LOCAL_DATA_DIR}/drug_reviews/drug_reviews.csv")
sdf = spark.createDataFrame(df)
sdf.write.format("delta").mode("overwrite").saveAsTable("drug_reviews")
cnt = sdf.count()
print(f"  drug_reviews: {cnt} rows")

# COMMAND ----------

# Clinical Notes
sdf = spark.read.json(f"file:{LOCAL_DATA_DIR}/clinical_notes/clinical_notes.json")
sdf.write.format("delta").mode("overwrite").saveAsTable("clinical_notes")
cnt = sdf.count()
print(f"  clinical_notes: {cnt} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Validation

# COMMAND ----------

import os

print("=" * 60)
print("  SETUP COMPLETE — VALIDATION REPORT")
print("=" * 60)

# Check all tables
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
        actual = spark.sql(f"SELECT COUNT(*) as cnt FROM {tbl}").collect()[0].cnt
        status = "OK" if actual >= expected * 0.9 else "LOW"
        if status != "OK":
            all_ok = False
        print(f"    {tbl:30s} {actual:>6,} rows  [{status}]")
    except Exception as e:
        print(f"    {tbl:30s}  ERROR: {str(e)[:40]}")
        all_ok = False

# Check staging files
print(f"\n  STAGING FILES:")
total_files = 0
for root, dirs, files in os.walk("/tmp/dataops_olympics/raw"):
    for f in files:
        total_files += 1
print(f"    {total_files} files in /tmp/dataops_olympics/raw/")

# Key file paths for participants
print(f"\n  KEY FILE PATHS FOR EVENTS:")
print(f"    Heart CSV:           file:/tmp/dataops_olympics/raw/heart_disease/heart.csv")
print(f"    Heart Batches:       file:/tmp/dataops_olympics/raw/heart_disease/heart_disease_batch_*.csv")
print(f"    Life Expectancy JSON:file:/tmp/dataops_olympics/raw/life_expectancy/life_expectancy_sample.json")
print(f"    Drug Reviews CSV:    file:/tmp/dataops_olympics/raw/drug_reviews/drug_reviews.csv")
print(f"    Clinical Notes JSON: file:/tmp/dataops_olympics/raw/clinical_notes/clinical_notes.json")

print(f"\n  SCHEMA: {CATALOG_NAME}.{SCHEMA_NAME}" if CATALOG_NAME != "hive_metastore" else f"\n  SCHEMA: {SCHEMA_NAME} (hive_metastore)")

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
# MAGIC ### Raw Files (for ingestion events)
# MAGIC | File | Path |
# MAGIC |------|------|
# MAGIC | Heart Disease CSV | `file:/tmp/dataops_olympics/raw/heart_disease/heart.csv` |
# MAGIC | Heart Batches 1-3 | `file:/tmp/dataops_olympics/raw/heart_disease/heart_disease_batch_*.csv` |
# MAGIC | Life Expectancy JSON | `file:/tmp/dataops_olympics/raw/life_expectancy/life_expectancy_sample.json` |
# MAGIC | Drug Reviews CSV | `file:/tmp/dataops_olympics/raw/drug_reviews/drug_reviews.csv` |
# MAGIC | Clinical Notes JSON | `file:/tmp/dataops_olympics/raw/clinical_notes/clinical_notes.json` |
# MAGIC
# MAGIC ### Useful SQL
# MAGIC ```sql
# MAGIC SHOW TABLES;
# MAGIC DESCRIBE TABLE heart_disease;
# MAGIC SELECT * FROM heart_disease LIMIT 10;
# MAGIC DESCRIBE HISTORY heart_disease;
# MAGIC ```
# MAGIC
# MAGIC ### Key Python Imports
# MAGIC ```python
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC from sklearn.model_selection import train_test_split
# MAGIC from sklearn.ensemble import RandomForestClassifier
# MAGIC from sklearn.metrics import f1_score, classification_report
# MAGIC import mlflow
# MAGIC import mlflow.sklearn
# MAGIC import plotly.express as px
# MAGIC ```
