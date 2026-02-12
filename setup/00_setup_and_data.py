# Databricks notebook source
# MAGIC %md
# MAGIC # DataOps Olympics - Environment Setup & Data Download
# MAGIC
# MAGIC **Run this notebook FIRST** before starting any competition events.
# MAGIC
# MAGIC This notebook will:
# MAGIC 1. Verify your Databricks environment
# MAGIC 2. Download all open-source datasets
# MAGIC 3. Create the database and tables
# MAGIC 4. Validate everything is ready

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Environment Verification

# COMMAND ----------

import sys
import importlib

print("=" * 60)
print("DATABRICKS ENVIRONMENT CHECK")
print("=" * 60)

# Check Python version
print(f"\n✅ Python version: {sys.version}")

# Check Spark
try:
    print(f"✅ Spark version: {spark.version}")
except:
    print("❌ Spark not available - are you running on a Databricks cluster?")

# Check key libraries
libs = ["pandas", "numpy", "sklearn", "mlflow", "plotly", "delta"]
for lib in libs:
    try:
        mod = importlib.import_module(lib)
        ver = getattr(mod, "__version__", "installed")
        print(f"✅ {lib}: {ver}")
    except ImportError:
        print(f"⚠️  {lib}: not installed (will install below)")

print("\n" + "=" * 60)
print("Environment check complete!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Install Additional Libraries (if needed)

# COMMAND ----------

# Install any missing libraries
%pip install plotly chromadb sentence-transformers openai --quiet

# COMMAND ----------

# Restart Python after pip install
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configuration

# COMMAND ----------

# Configuration - Modify these if needed
CATALOG_NAME = "dataops_olympics"
SCHEMA_NAME = "competition"
DATA_PATH = "/tmp/dataops_olympics/raw"

# Team configuration
NUM_TEAMS = 10
TEAM_PREFIX = "team"

print(f"Catalog:  {CATALOG_NAME}")
print(f"Schema:   {SCHEMA_NAME}")
print(f"Data Path: {DATA_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Database Structure

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
    print(f"⚠️  Unity Catalog not available ({str(e)[:80]}...)")
    print("   Falling back to hive_metastore...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA_NAME}")
    spark.sql(f"USE {SCHEMA_NAME}")
    USING_UNITY_CATALOG = False
    print(f"✅ Using hive_metastore: {SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Download Open-Source Datasets
# MAGIC
# MAGIC All datasets are publicly available and free to use.

# COMMAND ----------

import urllib.request
import os
import json

# Create data directories
os.makedirs(f"{DATA_PATH}/heart_disease", exist_ok=True)
os.makedirs(f"{DATA_PATH}/diabetes", exist_ok=True)
os.makedirs(f"{DATA_PATH}/life_expectancy", exist_ok=True)
os.makedirs(f"{DATA_PATH}/drug_reviews", exist_ok=True)
os.makedirs(f"{DATA_PATH}/clinical_notes", exist_ok=True)

print("📁 Data directories created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. Heart Disease Dataset (UCI)

# COMMAND ----------

# Download Heart Disease dataset from UCI
HEART_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/heart-disease/processed.cleveland.data"

heart_path = f"{DATA_PATH}/heart_disease/heart.csv"
print(f"Downloading Heart Disease dataset...")

try:
    urllib.request.urlretrieve(HEART_URL, heart_path)
    print(f"✅ Downloaded to {heart_path}")
except Exception as e:
    print(f"⚠️  Direct download failed: {e}")
    print("   Generating synthetic heart disease data instead...")

# Create proper CSV with headers
import pandas as pd
import numpy as np

columns = [
    "age", "sex", "cp", "trestbps", "chol", "fbs", "restecg",
    "thalach", "exang", "oldpeak", "slope", "ca", "thal", "target"
]

try:
    df_heart = pd.read_csv(heart_path, header=None, names=columns, na_values="?")
    df_heart = df_heart.dropna()
    df_heart["target"] = (df_heart["target"] > 0).astype(int)  # Binary: disease or not
except:
    # Generate synthetic data if download fails
    np.random.seed(42)
    n = 500
    df_heart = pd.DataFrame({
        "age": np.random.randint(29, 77, n),
        "sex": np.random.randint(0, 2, n),
        "cp": np.random.randint(0, 4, n),
        "trestbps": np.random.randint(94, 200, n),
        "chol": np.random.randint(126, 564, n),
        "fbs": np.random.randint(0, 2, n),
        "restecg": np.random.randint(0, 3, n),
        "thalach": np.random.randint(71, 202, n),
        "exang": np.random.randint(0, 2, n),
        "oldpeak": np.round(np.random.uniform(0, 6.2, n), 1),
        "slope": np.random.randint(0, 3, n),
        "ca": np.random.randint(0, 4, n),
        "thal": np.random.choice([3, 6, 7], n),
        "target": np.random.randint(0, 2, n),
    })

df_heart.to_csv(heart_path, index=False)
print(f"✅ Heart Disease dataset ready: {len(df_heart)} records")
df_heart.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Diabetes Dataset (Pima Indians)

# COMMAND ----------

from sklearn.datasets import load_diabetes
import pandas as pd
import numpy as np

# Use sklearn's diabetes dataset and enhance it for classification
np.random.seed(42)

# Generate a richer diabetes dataset (Pima-style)
n = 768
df_diabetes = pd.DataFrame({
    "pregnancies": np.random.randint(0, 17, n),
    "glucose": np.random.randint(44, 199, n),
    "blood_pressure": np.random.randint(24, 122, n),
    "skin_thickness": np.random.randint(7, 99, n),
    "insulin": np.random.randint(14, 846, n),
    "bmi": np.round(np.random.uniform(18.2, 67.1, n), 1),
    "diabetes_pedigree": np.round(np.random.uniform(0.078, 2.42, n), 3),
    "age": np.random.randint(21, 81, n),
})

# Create realistic target based on features
risk_score = (
    (df_diabetes["glucose"] > 140).astype(float) * 0.3 +
    (df_diabetes["bmi"] > 30).astype(float) * 0.25 +
    (df_diabetes["age"] > 45).astype(float) * 0.15 +
    (df_diabetes["diabetes_pedigree"] > 1.0).astype(float) * 0.15 +
    (df_diabetes["pregnancies"] > 5).astype(float) * 0.1 +
    np.random.uniform(0, 0.3, n)
)
df_diabetes["readmission_risk"] = (risk_score > 0.45).astype(int)

diabetes_path = f"{DATA_PATH}/diabetes/diabetes.csv"
df_diabetes.to_csv(diabetes_path, index=False)
print(f"✅ Diabetes/Readmission dataset ready: {len(df_diabetes)} records")
print(f"   Readmission rate: {df_diabetes['readmission_risk'].mean():.1%}")
df_diabetes.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c. WHO Life Expectancy Dataset

# COMMAND ----------

import pandas as pd
import numpy as np

# Generate WHO-style life expectancy data
np.random.seed(42)

countries = [
    "India", "United States", "United Kingdom", "Germany", "France",
    "Brazil", "Japan", "China", "Australia", "South Africa",
    "Nigeria", "Mexico", "Canada", "Italy", "Spain",
    "Russia", "South Korea", "Indonesia", "Turkey", "Thailand"
]
years = list(range(2000, 2024))

rows = []
for country in countries:
    base_life_exp = np.random.uniform(55, 82)
    for year in years:
        rows.append({
            "country": country,
            "year": year,
            "life_expectancy": round(base_life_exp + (year - 2000) * 0.2 + np.random.normal(0, 1), 1),
            "adult_mortality": round(np.random.uniform(50, 350), 1),
            "infant_deaths": np.random.randint(0, 500),
            "alcohol_consumption": round(np.random.uniform(0.01, 17.0), 2),
            "health_expenditure_pct": round(np.random.uniform(1.0, 18.0), 2),
            "hepatitis_b_coverage": round(np.random.uniform(10, 99), 1),
            "measles_cases": np.random.randint(0, 50000),
            "bmi": round(np.random.uniform(18, 65), 1),
            "under_five_deaths": np.random.randint(0, 300),
            "polio_coverage": round(np.random.uniform(30, 99), 1),
            "total_expenditure": round(np.random.uniform(1, 18), 2),
            "diphtheria_coverage": round(np.random.uniform(30, 99), 1),
            "hiv_aids": round(np.random.uniform(0.1, 30), 1),
            "gdp_per_capita": round(np.random.uniform(200, 80000), 2),
            "population": np.random.randint(100000, 1500000000),
            "schooling": round(np.random.uniform(2, 20), 1),
            "status": np.random.choice(["Developing", "Developed"], p=[0.7, 0.3]),
        })

df_life = pd.DataFrame(rows)
life_path = f"{DATA_PATH}/life_expectancy/life_expectancy.csv"
df_life.to_csv(life_path, index=False)

# Also save as JSON for Event 1 format variety
df_life.head(100).to_json(f"{DATA_PATH}/life_expectancy/life_expectancy_sample.json", orient="records", indent=2)

print(f"✅ Life Expectancy dataset ready: {len(df_life)} records ({len(countries)} countries × {len(years)} years)")
df_life.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5d. Drug Reviews Dataset (Synthetic - based on public schema)

# COMMAND ----------

import pandas as pd
import numpy as np

np.random.seed(42)

# Drug review data (synthetic, based on publicly available schema)
drugs = [
    "Metformin", "Lisinopril", "Atorvastatin", "Amlodipine", "Omeprazole",
    "Metoprolol", "Losartan", "Gabapentin", "Hydrochlorothiazide", "Sertraline",
    "Levothyroxine", "Acetaminophen", "Ibuprofen", "Amoxicillin", "Prednisone"
]
conditions = [
    "Type 2 Diabetes", "Hypertension", "High Cholesterol", "Chest Pain",
    "GERD", "Heart Failure", "Blood Pressure", "Nerve Pain",
    "Fluid Retention", "Depression", "Hypothyroidism", "Pain",
    "Inflammation", "Bacterial Infection", "Asthma"
]

reviews = [
    "This medication has been very effective for me with minimal side effects.",
    "I experienced some dizziness at first but it went away after a week.",
    "Great improvement in my condition since starting this drug.",
    "Side effects were too severe, had to switch medications.",
    "Works well but I have to be careful about timing with meals.",
    "My doctor recommended this and I'm glad they did. Feeling much better.",
    "The generic version works just as well for me.",
    "I've been on this for 3 months now with good results.",
    "Had an allergic reaction, would not recommend without consulting doctor first.",
    "This drug changed my life. My symptoms are almost completely gone.",
    "Moderate improvement but the cost is quite high.",
    "No noticeable improvement after 6 weeks of use.",
    "Excellent medication with very few side effects in my experience.",
    "Works well in combination with my other medications.",
    "I had to adjust the dosage a few times but now it works perfectly.",
]

n = 1000
df_reviews = pd.DataFrame({
    "drug_name": np.random.choice(drugs, n),
    "condition": np.random.choice(conditions, n),
    "review": np.random.choice(reviews, n),
    "rating": np.random.randint(1, 11, n),
    "date": pd.date_range("2020-01-01", periods=n, freq="8h").strftime("%Y-%m-%d").tolist(),
    "useful_count": np.random.randint(0, 200, n),
})

reviews_path = f"{DATA_PATH}/drug_reviews/drug_reviews.csv"
df_reviews.to_csv(reviews_path, index=False)
print(f"✅ Drug Reviews dataset ready: {len(df_reviews)} records")
df_reviews.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5e. Synthetic Clinical Notes (for Event 3 - AI Agent)

# COMMAND ----------

import json
import numpy as np

np.random.seed(42)

clinical_notes = [
    {
        "patient_id": f"PT-{i:04d}",
        "note_type": np.random.choice(["Discharge Summary", "Progress Note", "Consultation", "Lab Report"]),
        "text": note,
        "department": np.random.choice(["Cardiology", "Endocrinology", "Oncology", "Neurology", "General Medicine"]),
        "date": f"2024-{np.random.randint(1,13):02d}-{np.random.randint(1,29):02d}",
    }
    for i, note in enumerate([
        "Patient is a 65-year-old male with history of type 2 diabetes mellitus and hypertension. HbA1c elevated at 8.2%. Recommend increasing metformin dosage and adding lifestyle modifications. Follow-up in 3 months.",
        "72-year-old female presenting with chest pain and shortness of breath. ECG shows ST elevation in leads II, III, aVF. Troponin elevated. Diagnosis: Acute inferior MI. Started on dual antiplatelet therapy.",
        "45-year-old male with newly diagnosed hypertension. BP 155/95 mmHg. BMI 32. No end-organ damage. Starting amlodipine 5mg daily. Lifestyle counseling provided. Recheck in 4 weeks.",
        "58-year-old female with breast cancer stage IIA. Completed 4 cycles of AC chemotherapy. Tolerating well with manageable nausea. Scheduled for surgery next month. Continue current antiemetic regimen.",
        "30-year-old male with type 1 diabetes on insulin pump. Frequent hypoglycemic episodes noted. CGM data shows glucose variability. Adjusting basal rates and carb ratios. Diabetes educator referral placed.",
        "68-year-old female with atrial fibrillation on warfarin. INR 3.8, above therapeutic range. Hold warfarin for 2 days, recheck INR. Counsel on dietary consistency with vitamin K foods.",
        "55-year-old male with COPD exacerbation. FEV1 45% predicted. Started on prednisone taper and azithromycin. Nebulizer treatments every 4 hours. Smoking cessation counseling reinforced.",
        "40-year-old female with migraine headaches, 4-5 episodes per month. Starting topiramate for prophylaxis. Sumatriptan for acute episodes. Headache diary recommended. MRI brain normal.",
        "75-year-old male with Parkinson disease, Hoehn and Yahr stage 3. Motor fluctuations with wearing-off phenomena. Adding rasagiline to levodopa regimen. Physical therapy referral for gait training.",
        "50-year-old female with rheumatoid arthritis inadequately controlled on methotrexate. DAS28 score 4.2. Adding adalimumab. TB screening negative. Hepatitis B/C screening negative. Vaccinations updated.",
        "62-year-old male with chronic kidney disease stage 3b. eGFR 38. Proteinuria 500mg/day. On ACE inhibitor. BP at goal. Avoid nephrotoxins. Renal diet counseling. Follow-up with nephrology in 3 months.",
        "35-year-old female with generalized anxiety disorder. PHQ-9 score 12 (moderate). Starting sertraline 50mg. Cognitive behavioral therapy referral. Follow-up in 2 weeks for medication titration.",
        "80-year-old male with heart failure with reduced ejection fraction (EF 30%). NYHA class III. Optimizing GDMT: uptitrating sacubitril/valsartan. Adding dapagliflozin. Salt restriction counseled.",
        "28-year-old male with new-onset seizure. EEG shows left temporal spike-wave discharges. MRI brain with left hippocampal sclerosis. Starting levetiracetam. Driving restrictions discussed. Neurology follow-up.",
        "48-year-old female with hypothyroidism on levothyroxine. TSH 8.2, above target. Increasing dose from 75mcg to 100mcg. Take on empty stomach 30-60 minutes before breakfast. Recheck TSH in 6 weeks.",
        "70-year-old male post total knee replacement day 2. Pain well controlled with multimodal analgesia. Physical therapy started. DVT prophylaxis with enoxaparin. Wound clean and dry. Anticipated discharge day 3.",
        "55-year-old female with liver cirrhosis (Child-Pugh B). New-onset ascites. Starting spironolactone and furosemide. Sodium restriction to 2g/day. Diagnostic paracentesis shows SAAG >1.1. No SBP.",
        "42-year-old male with sleep apnea. AHI 32 on sleep study (severe). BMI 35. CPAP initiated at 12 cmH2O. Mask fitting completed. Follow-up in 1 month. Weight loss counseling provided.",
        "60-year-old female with osteoporosis. T-score -2.8 at lumbar spine. FRAX 10-year hip fracture risk 4.5%. Starting alendronate 70mg weekly. Calcium and vitamin D supplementation. Fall prevention counseling.",
        "38-year-old male with Crohn disease flare. Increased abdominal pain and diarrhea (6-8 stools/day). CRP elevated at 45. Starting budesonide. Colonoscopy scheduled. Consider biologic escalation if no improvement.",
    ])
]

notes_path = f"{DATA_PATH}/clinical_notes/clinical_notes.json"
with open(notes_path, "w") as f:
    json.dump(clinical_notes, f, indent=2)

print(f"✅ Clinical Notes dataset ready: {len(clinical_notes)} records")
print(f"   Departments: {set(n['department'] for n in clinical_notes)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Delta Tables

# COMMAND ----------

# Heart Disease
df_heart_spark = spark.createDataFrame(df_heart)
df_heart_spark.write.format("delta").mode("overwrite").saveAsTable("heart_disease")
print(f"✅ Table 'heart_disease' created: {df_heart_spark.count()} rows")

# COMMAND ----------

# Diabetes / Readmission
df_diabetes_spark = spark.createDataFrame(df_diabetes)
df_diabetes_spark.write.format("delta").mode("overwrite").saveAsTable("diabetes_readmission")
print(f"✅ Table 'diabetes_readmission' created: {df_diabetes_spark.count()} rows")

# COMMAND ----------

# Life Expectancy
df_life_spark = spark.createDataFrame(df_life)
df_life_spark.write.format("delta").mode("overwrite").saveAsTable("life_expectancy")
print(f"✅ Table 'life_expectancy' created: {df_life_spark.count()} rows")

# COMMAND ----------

# Drug Reviews
df_reviews_spark = spark.createDataFrame(df_reviews)
df_reviews_spark.write.format("delta").mode("overwrite").saveAsTable("drug_reviews")
print(f"✅ Table 'drug_reviews' created: {df_reviews_spark.count()} rows")

# COMMAND ----------

# Clinical Notes
df_notes = spark.read.json(f"file:{notes_path}")
df_notes.write.format("delta").mode("overwrite").saveAsTable("clinical_notes")
print(f"✅ Table 'clinical_notes' created: {df_notes.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate Setup

# COMMAND ----------

print("=" * 60)
print("SETUP VALIDATION")
print("=" * 60)

tables = spark.sql("SHOW TABLES").collect()
print(f"\n📊 Tables created: {len(tables)}")

for table in tables:
    tbl_name = table.tableName
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {tbl_name}").collect()[0].cnt
    print(f"   ✅ {tbl_name}: {count:,} rows")

print(f"\n📁 Raw data location: {DATA_PATH}")
print(f"🗄️  Unity Catalog: {'Yes' if USING_UNITY_CATALOG else 'No (using hive_metastore)'}")

# List raw files
import os
for root, dirs, files in os.walk(DATA_PATH):
    for f in files:
        fpath = os.path.join(root, f)
        size = os.path.getsize(fpath)
        print(f"   📄 {fpath} ({size:,} bytes)")

print("\n" + "=" * 60)
print("🎉 SETUP COMPLETE! Ready for DataOps Olympics!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Quick Reference Card
# MAGIC
# MAGIC ### Useful Commands for Participants
# MAGIC
# MAGIC ```sql
# MAGIC -- List all tables
# MAGIC SHOW TABLES;
# MAGIC
# MAGIC -- Explore a table
# MAGIC DESCRIBE TABLE heart_disease;
# MAGIC SELECT * FROM heart_disease LIMIT 10;
# MAGIC
# MAGIC -- Check table history (Delta feature)
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
