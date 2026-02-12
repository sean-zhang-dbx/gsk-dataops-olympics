# Databricks notebook source
# MAGIC %md
# MAGIC # DataOps Olympics — Environment Setup & Data Loading
# MAGIC
# MAGIC **Run this notebook FIRST** before starting any competition events.
# MAGIC
# MAGIC This notebook will:
# MAGIC 1. Verify your Databricks environment
# MAGIC 2. Install required libraries
# MAGIC 3. Set up the catalog/schema (Unity Catalog or hive_metastore fallback)
# MAGIC 4. Load bundled data files into Delta tables
# MAGIC 5. (Optional) Set up Unity Catalog Volumes
# MAGIC 6. Validate everything is ready
# MAGIC
# MAGIC ### Data Source
# MAGIC All data files are **bundled in this repository** under the `data/` folder.
# MAGIC No internet downloads required at runtime.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Environment Verification

# COMMAND ----------

import sys
import importlib

print("=" * 60)
print("  DATABRICKS ENVIRONMENT CHECK")
print("=" * 60)

print(f"\n  Python version: {sys.version.split()[0]}")

try:
    print(f"  Spark version:  {spark.version}")
except:
    raise RuntimeError("Spark not available — are you on a Databricks cluster?")

# Check key libraries
libs = {
    "pandas": "pandas",
    "numpy": "numpy",
    "sklearn": "scikit-learn",
    "mlflow": "mlflow",
    "plotly": "plotly",
    "delta": "delta-spark",
}

for import_name, pip_name in libs.items():
    try:
        mod = importlib.import_module(import_name)
        ver = getattr(mod, "__version__", "✓")
        print(f"  ✅ {import_name}: {ver}")
    except ImportError:
        print(f"  ⚠️  {import_name}: not installed")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Install Additional Libraries

# COMMAND ----------

%pip install plotly chromadb sentence-transformers --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configuration

# COMMAND ----------

# -------------------------------------------------------------------
# EDIT THESE if needed
# -------------------------------------------------------------------
CATALOG_NAME = "dataops_olympics"
SCHEMA_NAME  = "competition"
NUM_TEAMS    = 10

# Path to the bundled data/ folder inside this repo
# When imported via Databricks Repos, the repo root is the notebook's CWD.
import os, pathlib

# Auto-detect repo root (works in Repos and Workspace)
_nb_dir = os.getcwd()
REPO_DATA_DIR = os.path.join(_nb_dir, "..", "data")
if not os.path.isdir(REPO_DATA_DIR):
    # Fallback: maybe the repo was cloned to /Workspace/...
    REPO_DATA_DIR = "/Workspace/Repos/" + os.environ.get("USER", "user") + "/gsk-dataops-olympics/data"
if not os.path.isdir(REPO_DATA_DIR):
    REPO_DATA_DIR = None  # Will use /tmp fallback

# Local staging area
LOCAL_DATA_DIR = "/tmp/dataops_olympics/raw"

print(f"Catalog:       {CATALOG_NAME}")
print(f"Schema:        {SCHEMA_NAME}")
print(f"Repo data dir: {REPO_DATA_DIR or '(not found, will generate)'}")
print(f"Staging dir:   {LOCAL_DATA_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Catalog / Schema

# COMMAND ----------

# Try Unity Catalog first, fall back to hive_metastore
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
    spark.sql(f"USE CATALOG {CATALOG_NAME}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
    spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
    USING_UNITY_CATALOG = True
    print(f"✅ Using Unity Catalog: {CATALOG_NAME}.{SCHEMA_NAME}")
except Exception as e:
    print(f"⚠️  Unity Catalog not available — falling back to hive_metastore")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA_NAME}")
    spark.sql(f"USE {SCHEMA_NAME}")
    USING_UNITY_CATALOG = False
    print(f"✅ Using hive_metastore.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Stage Data Files
# MAGIC
# MAGIC Data is bundled in the `data/` folder of this repository.
# MAGIC We copy it to a local staging path so all notebooks have a consistent path,
# MAGIC and (optionally) upload to a Unity Catalog Volume.

# COMMAND ----------

import shutil, os, json

os.makedirs(f"{LOCAL_DATA_DIR}/heart_disease", exist_ok=True)
os.makedirs(f"{LOCAL_DATA_DIR}/diabetes", exist_ok=True)
os.makedirs(f"{LOCAL_DATA_DIR}/life_expectancy", exist_ok=True)
os.makedirs(f"{LOCAL_DATA_DIR}/drug_reviews", exist_ok=True)
os.makedirs(f"{LOCAL_DATA_DIR}/clinical_notes", exist_ok=True)

if REPO_DATA_DIR and os.path.isdir(REPO_DATA_DIR):
    # Copy from bundled repo data/ to local staging
    src = REPO_DATA_DIR
    files_copied = 0
    
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
    
    for src_name, dst_path in copy_map.items():
        src_path = os.path.join(src, src_name)
        if os.path.isfile(src_path):
            shutil.copy2(src_path, dst_path)
            files_copied += 1
    
    print(f"✅ Copied {files_copied} files from repo data/ to staging")
else:
    print("⚠️  Repo data/ folder not found — generating data at runtime...")
    print("   (This is fine, but importing via Repos is recommended)")
    exec(open("/dev/null").read())  # placeholder; generation logic below

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Data (fallback if repo data/ not found)
# MAGIC
# MAGIC This cell only runs if the bundled data files weren't detected.
# MAGIC It creates equivalent synthetic datasets.

# COMMAND ----------

import pandas as pd
import numpy as np

np.random.seed(42)

# ---------- Heart Disease ----------
heart_path = f"{LOCAL_DATA_DIR}/heart_disease/heart.csv"
if not os.path.isfile(heart_path) or os.path.getsize(heart_path) < 100:
    n = 500
    df_heart = pd.DataFrame({
        "age": np.random.randint(29, 77, n), "sex": np.random.randint(0, 2, n),
        "cp": np.random.randint(0, 4, n), "trestbps": np.random.randint(94, 200, n),
        "chol": np.random.randint(126, 564, n), "fbs": np.random.randint(0, 2, n),
        "restecg": np.random.randint(0, 3, n), "thalach": np.random.randint(71, 202, n),
        "exang": np.random.randint(0, 2, n),
        "oldpeak": np.round(np.random.uniform(0, 6.2, n), 1),
        "slope": np.random.randint(0, 3, n), "ca": np.random.randint(0, 4, n),
        "thal": np.random.choice([3, 6, 7], n), "target": np.random.randint(0, 2, n),
    })
    df_heart.to_csv(heart_path, index=False)
    print(f"  Generated heart_disease.csv: {n} rows")

    # Batch files for DLT event
    for batch in range(1, 4):
        batch_df = df_heart.sample(50, random_state=batch)
        # Inject quality issues
        batch_df.loc[batch_df.sample(3, random_state=batch).index, "age"] = -1
        batch_df.loc[batch_df.sample(2, random_state=batch+10).index, "trestbps"] = 999
        batch_df.to_csv(f"{LOCAL_DATA_DIR}/heart_disease/heart_disease_batch_{batch}.csv", index=False)
        print(f"  Generated heart_disease_batch_{batch}.csv: 50 rows")

# ---------- Diabetes / Readmission ----------
diab_path = f"{LOCAL_DATA_DIR}/diabetes/diabetes.csv"
if not os.path.isfile(diab_path) or os.path.getsize(diab_path) < 100:
    n = 768
    df_diab = pd.DataFrame({
        "pregnancies": np.random.randint(0, 17, n),
        "glucose": np.random.randint(44, 199, n),
        "blood_pressure": np.random.randint(24, 122, n),
        "skin_thickness": np.random.randint(7, 99, n),
        "insulin": np.random.randint(14, 846, n),
        "bmi": np.round(np.random.uniform(18.2, 67.1, n), 1),
        "diabetes_pedigree": np.round(np.random.uniform(0.078, 2.42, n), 3),
        "age": np.random.randint(21, 81, n),
    })
    risk = ((df_diab["glucose"] > 140).astype(float) * 0.3 +
            (df_diab["bmi"] > 30).astype(float) * 0.25 +
            (df_diab["age"] > 45).astype(float) * 0.15 +
            (df_diab["diabetes_pedigree"] > 1.0).astype(float) * 0.15 +
            (df_diab["pregnancies"] > 5).astype(float) * 0.1 +
            np.random.uniform(0, 0.3, n))
    df_diab["readmission_risk"] = (risk > 0.45).astype(int)
    df_diab.to_csv(diab_path, index=False)
    print(f"  Generated diabetes.csv: {n} rows")

# ---------- Life Expectancy ----------
life_path = f"{LOCAL_DATA_DIR}/life_expectancy/life_expectancy.csv"
if not os.path.isfile(life_path) or os.path.getsize(life_path) < 100:
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
    df_life = pd.DataFrame(rows)
    df_life.to_csv(life_path, index=False)
    df_life.head(100).to_json(
        f"{LOCAL_DATA_DIR}/life_expectancy/life_expectancy_sample.json", orient="records", indent=2)
    print(f"  Generated life_expectancy.csv: {len(rows)} rows + JSON sample")

# ---------- Drug Reviews ----------
reviews_path = f"{LOCAL_DATA_DIR}/drug_reviews/drug_reviews.csv"
if not os.path.isfile(reviews_path) or os.path.getsize(reviews_path) < 100:
    drugs = ["Metformin","Lisinopril","Atorvastatin","Amlodipine","Omeprazole",
             "Metoprolol","Losartan","Gabapentin","Hydrochlorothiazide","Sertraline",
             "Levothyroxine","Acetaminophen","Ibuprofen","Amoxicillin","Prednisone"]
    conditions = ["Type 2 Diabetes","Hypertension","High Cholesterol","Chest Pain",
                  "GERD","Heart Failure","Blood Pressure","Nerve Pain","Fluid Retention",
                  "Depression","Hypothyroidism","Pain","Inflammation","Bacterial Infection","Asthma"]
    reviews_list = [
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
    df_rev = pd.DataFrame({
        "drug_name": np.random.choice(drugs, n),
        "condition": np.random.choice(conditions, n),
        "review": np.random.choice(reviews_list, n),
        "rating": np.random.randint(1, 11, n),
        "date": pd.date_range("2020-01-01", periods=n, freq="8h").strftime("%Y-%m-%d").tolist(),
        "useful_count": np.random.randint(0, 200, n),
    })
    df_rev.to_csv(reviews_path, index=False)
    print(f"  Generated drug_reviews.csv: {n} rows")

# ---------- Clinical Notes ----------
notes_path = f"{LOCAL_DATA_DIR}/clinical_notes/clinical_notes.json"
if not os.path.isfile(notes_path) or os.path.getsize(notes_path) < 100:
    print("  ⚠️  clinical_notes.json not found — expected from repo data/")

print("\n✅ Data staging complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. (Optional) Upload to Unity Catalog Volume
# MAGIC
# MAGIC **Volumes** are the modern way to manage files in Databricks.
# MAGIC This stores data under `/Volumes/catalog/schema/volume_name/`.

# COMMAND ----------

if USING_UNITY_CATALOG:
    try:
        spark.sql(f"""
            CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.raw_data
            COMMENT 'Raw data files for DataOps Olympics'
        """)
        VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/raw_data"
        
        # Upload key files to the Volume
        import glob
        uploaded = 0
        for local_file in glob.glob(f"{LOCAL_DATA_DIR}/**/*.*", recursive=True):
            rel = os.path.relpath(local_file, LOCAL_DATA_DIR)
            dest = f"{VOLUME_PATH}/{rel}"
            try:
                dbutils.fs.cp(f"file:{local_file}", dest)
                uploaded += 1
            except:
                pass
        
        print(f"✅ UC Volume created: {VOLUME_PATH}")
        print(f"   Uploaded {uploaded} files")
        print(f"   Files are now accessible at {VOLUME_PATH}/")
    except Exception as e:
        print(f"⚠️  Volume creation skipped: {str(e)[:80]}")
else:
    print("ℹ️  Unity Catalog not available — skipping Volume creation")
    print(f"   Data is available at file:{LOCAL_DATA_DIR}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create Delta Tables

# COMMAND ----------

import pandas as pd

# ---- Heart Disease ----
df = pd.read_csv(f"{LOCAL_DATA_DIR}/heart_disease/heart.csv")
sdf = spark.createDataFrame(df)
sdf.write.format("delta").mode("overwrite").saveAsTable("heart_disease")
print(f"✅ heart_disease: {sdf.count()} rows")

# COMMAND ----------

# ---- Diabetes / Readmission ----
df = pd.read_csv(f"{LOCAL_DATA_DIR}/diabetes/diabetes.csv")
sdf = spark.createDataFrame(df)
sdf.write.format("delta").mode("overwrite").saveAsTable("diabetes_readmission")
print(f"✅ diabetes_readmission: {sdf.count()} rows")

# COMMAND ----------

# ---- Life Expectancy ----
df = pd.read_csv(f"{LOCAL_DATA_DIR}/life_expectancy/life_expectancy.csv")
sdf = spark.createDataFrame(df)
sdf.write.format("delta").mode("overwrite").saveAsTable("life_expectancy")
print(f"✅ life_expectancy: {sdf.count()} rows")

# COMMAND ----------

# ---- Drug Reviews ----
df = pd.read_csv(f"{LOCAL_DATA_DIR}/drug_reviews/drug_reviews.csv")
sdf = spark.createDataFrame(df)
sdf.write.format("delta").mode("overwrite").saveAsTable("drug_reviews")
print(f"✅ drug_reviews: {sdf.count()} rows")

# COMMAND ----------

# ---- Clinical Notes ----
sdf = spark.read.json(f"file:{LOCAL_DATA_DIR}/clinical_notes/clinical_notes.json")
sdf.write.format("delta").mode("overwrite").saveAsTable("clinical_notes")
print(f"✅ clinical_notes: {sdf.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Final Validation

# COMMAND ----------

print("=" * 60)
print("  SETUP VALIDATION")
print("=" * 60)

tables = spark.sql("SHOW TABLES").collect()
print(f"\n  📊 Tables created: {len(tables)}")

all_good = True
for table in tables:
    tbl_name = table.tableName
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {tbl_name}").collect()[0].cnt
        print(f"     ✅ {tbl_name}: {count:,} rows")
    except:
        print(f"     ❌ {tbl_name}: ERROR")
        all_good = False

print(f"\n  🗄️  Unity Catalog: {'Yes' if USING_UNITY_CATALOG else 'No (hive_metastore)'}")
print(f"  📁 Staging path:  file:{LOCAL_DATA_DIR}/")

# List raw files
total_bytes = 0
file_count = 0
for root, dirs, files in os.walk(LOCAL_DATA_DIR):
    for f in files:
        fpath = os.path.join(root, f)
        size = os.path.getsize(fpath)
        total_bytes += size
        file_count += 1

print(f"  📄 Raw files: {file_count} ({total_bytes:,} bytes)")

print(f"\n{'='*60}")
if all_good:
    print("  🎉 SETUP COMPLETE! Ready for DataOps Olympics!")
else:
    print("  ⚠️  Some issues detected — check table errors above")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Reference Card
# MAGIC
# MAGIC ### Useful SQL
# MAGIC ```sql
# MAGIC -- List all tables
# MAGIC SHOW TABLES;
# MAGIC
# MAGIC -- Explore a table
# MAGIC DESCRIBE TABLE heart_disease;
# MAGIC SELECT * FROM heart_disease LIMIT 10;
# MAGIC
# MAGIC -- Check Delta history
# MAGIC DESCRIBE HISTORY heart_disease;
# MAGIC
# MAGIC -- Check table details (format, size, location)
# MAGIC DESCRIBE DETAIL heart_disease;
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
# MAGIC
# MAGIC ### Available Tables
# MAGIC | Table | Rows | Description |
# MAGIC |-------|------|-------------|
# MAGIC | `heart_disease` | ~500 | UCI heart disease clinical data |
# MAGIC | `diabetes_readmission` | 768 | Diabetes readmission risk prediction |
# MAGIC | `life_expectancy` | ~480 | WHO health indicators by country/year |
# MAGIC | `drug_reviews` | 1,000 | Drug review ratings and text |
# MAGIC | `clinical_notes` | 20 | Synthetic clinical notes (for NLP) |
# MAGIC
# MAGIC ### Raw Files (for DLT/ingestion events)
# MAGIC | File | Path |
# MAGIC |------|------|
# MAGIC | Heart Disease CSV | `file:/tmp/dataops_olympics/raw/heart_disease/heart.csv` |
# MAGIC | Heart Batch 1-3 | `file:/tmp/dataops_olympics/raw/heart_disease/heart_disease_batch_*.csv` |
# MAGIC | Life Expectancy JSON | `file:/tmp/dataops_olympics/raw/life_expectancy/life_expectancy_sample.json` |
# MAGIC | Drug Reviews CSV | `file:/tmp/dataops_olympics/raw/drug_reviews/drug_reviews.csv` |
# MAGIC | Clinical Notes JSON | `file:/tmp/dataops_olympics/raw/clinical_notes/clinical_notes.json` |
