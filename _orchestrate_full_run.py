# Databricks notebook source
# MAGIC %md
# MAGIC # Full Orchestration: Clear, Setup, Simulate, Score
# MAGIC
# MAGIC Runs the entire DataOps Olympics end-to-end for 2 teams.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: CLEAR EVERYTHING

# COMMAND ----------

catalogs_to_drop = ["team_01", "team_02"]
shared_tables_to_drop = [
    "olympics_leaderboard", "registered_teams",
    "event1_scores", "event2_scores", "event3_scores", "event4_scores", "event5_scores",
    "event2_genie_benchmark", "event2_answer_key",
    "heart_disease", "diabetes_readmission", "life_expectancy", "drug_reviews", "clinical_notes",
]

for cat in catalogs_to_drop:
    try:
        spark.sql(f"DROP CATALOG IF EXISTS {cat} CASCADE")
        print(f"  Dropped catalog: {cat}")
    except Exception as e:
        print(f"  Skip {cat}: {e}")

for tbl in shared_tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS dataops_olympics.default.{tbl}")
        print(f"  Dropped table: dataops_olympics.default.{tbl}")
    except Exception as e:
        print(f"  Skip {tbl}: {e}")

print("\nAll data cleared!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Shared Resources + Team Catalogs

# COMMAND ----------

SHARED_CATALOG = "dataops_olympics"
SHARED_SCHEMA = "default"
VOLUME_NAME = "raw_data"
VOLUME_PATH = f"/Volumes/{SHARED_CATALOG}/{SHARED_SCHEMA}/{VOLUME_NAME}"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {SHARED_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SHARED_CATALOG}.{SHARED_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SHARED_CATALOG}.{SHARED_SCHEMA}.{VOLUME_NAME}")

for team in ["team_01", "team_02"]:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {team}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {team}.default")
    print(f"  Created catalog: {team}")
print(f"  Shared resources ready: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Stage Data Files

# COMMAND ----------

import os, shutil, json
import pandas as pd
import numpy as np

sub_dirs = ["heart_disease", "heart_events", "diabetes", "life_expectancy", "drug_reviews", "clinical_notes"]
for d in sub_dirs:
    os.makedirs(f"{VOLUME_PATH}/{d}", exist_ok=True)

np.random.seed(42)
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
df_h.to_csv(f"{VOLUME_PATH}/heart_disease/heart.csv", index=False)
for batch in range(1, 4):
    b = df_h.sample(50, random_state=batch)
    b.loc[b.sample(3, random_state=batch).index, "age"] = -1
    b.loc[b.sample(2, random_state=batch+10).index, "trestbps"] = 999
    b.to_csv(f"{VOLUME_PATH}/heart_disease/heart_disease_batch_{batch}.csv", index=False)

import random as _rnd, datetime as _dtt
_rnd.seed(42)
_rows = df_h.to_dict("records")
_rnd.shuffle(_rows)
_base_time = _dtt.datetime(2024, 11, 15, 8, 0, 0)
_evt = 0
for _bi in range(5):
    _batch = _rows[_bi*100:(_bi+1)*100]
    _bt = _base_time + _dtt.timedelta(hours=_bi*3)
    _lines = []
    for _i, _r in enumerate(_batch):
        _evt += 1
        rec = {"event_id": f"EVT-{_evt:05d}",
               "event_timestamp": (_bt + _dtt.timedelta(seconds=_rnd.randint(0, 10800))).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "source_system": _rnd.choice(["hospital_ehr", "clinic_intake", "emergency_dept"]),
               "record_version": 1, "patient_id": f"PT-{_evt:04d}"}
        rec.update({k: (float(v) if isinstance(v, float) else int(v)) for k, v in _r.items()})
        if _bi == 2 and _i < 5:
            rec["age"] = None if _i < 3 else rec["age"]
            if _i >= 3: rec["trestbps"] = 999
        if _bi == 4 and _i < 4:
            rec["age"] = None if _i < 2 else rec["age"]
            if _i >= 2: rec["trestbps"] = -1
        _lines.append(json.dumps(rec))
    with open(f"{VOLUME_PATH}/heart_events/intake_batch_{_bi+1:03d}.json", "w") as _f:
        _f.write("\n".join(_lines) + "\n")
print(f"  Heart data + 5 NDJSON batches generated")

n = 768
df_d = pd.DataFrame({
    "pregnancies": np.random.randint(0, 17, n), "glucose": np.random.randint(44, 199, n),
    "blood_pressure": np.random.randint(24, 122, n), "skin_thickness": np.random.randint(7, 99, n),
    "insulin": np.random.randint(14, 846, n), "bmi": np.round(np.random.uniform(18.2, 67.1, n), 1),
    "diabetes_pedigree": np.round(np.random.uniform(0.078, 2.42, n), 3),
    "age": np.random.randint(21, 81, n),
})
risk = ((df_d["glucose"] > 140).astype(float) * 0.3 + (df_d["bmi"] > 30).astype(float) * 0.25 +
        (df_d["age"] > 45).astype(float) * 0.15 + (df_d["diabetes_pedigree"] > 1.0).astype(float) * 0.15 +
        (df_d["pregnancies"] > 5).astype(float) * 0.1 + np.random.uniform(0, 0.3, n))
df_d["readmission_risk"] = (risk > 0.45).astype(int)
df_d.to_csv(f"{VOLUME_PATH}/diabetes/diabetes.csv", index=False)

countries = ["India","United States","United Kingdom","Germany","France","Brazil",
             "Japan","China","Australia","South Africa","Nigeria","Mexico",
             "Canada","Italy","Spain","Russia","South Korea","Indonesia","Turkey","Thailand"]
rows_le = []
for c in countries:
    base = np.random.uniform(55, 82)
    for y in range(2000, 2024):
        rows_le.append({"country": c, "year": y,
            "life_expectancy": round(base + (y-2000)*0.2 + np.random.normal(0,1), 1),
            "adult_mortality": round(np.random.uniform(50,350),1),
            "infant_deaths": np.random.randint(0,500),
            "alcohol_consumption": round(np.random.uniform(0.01,17),2),
            "health_expenditure_pct": round(np.random.uniform(1,18),2),
            "bmi": round(np.random.uniform(18,65),1),
            "gdp_per_capita": round(np.random.uniform(200,80000),2),
            "population": np.random.randint(100000,1500000000),
            "schooling": round(np.random.uniform(2,20),1),
            "status": np.random.choice(["Developing","Developed"], p=[0.7,0.3]),
        })
df_l = pd.DataFrame(rows_le)
df_l.to_csv(f"{VOLUME_PATH}/life_expectancy/life_expectancy.csv", index=False)

drugs = ["Metformin","Lisinopril","Atorvastatin","Amlodipine","Omeprazole",
         "Metoprolol","Losartan","Gabapentin","Hydrochlorothiazide","Sertraline",
         "Levothyroxine","Acetaminophen","Ibuprofen","Amoxicillin","Prednisone"]
conditions = ["Type 2 Diabetes","Hypertension","High Cholesterol","Chest Pain",
              "GERD","Heart Failure","Blood Pressure","Nerve Pain","Fluid Retention",
              "Depression","Hypothyroidism","Pain","Inflammation","Bacterial Infection","Asthma"]
review_texts = ["This medication has been very effective for me with minimal side effects.",
    "I experienced some dizziness at first but it went away after a week.",
    "Great improvement in my condition since starting this drug.",
    "Side effects were too severe, had to switch medications.",
    "My doctor recommended this and I am glad they did. Feeling much better.",
    "The generic version works just as well for me.",
    "I have been on this for 3 months now with good results.",
    "Excellent medication with very few side effects in my experience.",
    "Works well in combination with my other medications."]
n = 1000
df_r = pd.DataFrame({"drug_name": np.random.choice(drugs, n), "condition": np.random.choice(conditions, n),
    "review": np.random.choice(review_texts, n), "rating": np.random.randint(1, 11, n),
    "date": pd.date_range("2020-01-01", periods=n, freq="8h").strftime("%Y-%m-%d").tolist(),
    "useful_count": np.random.randint(0, 200, n)})
df_r.to_csv(f"{VOLUME_PATH}/drug_reviews/drug_reviews.csv", index=False)

notes = []
depts = ["Cardiology","Oncology","Neurology","Emergency","Pediatrics"]
note_types = ["Admission Note","Progress Note","Discharge Summary","Consultation"]
for i in range(20):
    notes.append({"note_id": f"NOTE_{i+1:03d}", "patient_id": f"PAT_{np.random.randint(100,999)}",
        "department": depts[i % len(depts)], "note_type": note_types[i % len(note_types)],
        "date": f"2024-{(i%12)+1:02d}-{(i%28)+1:02d}",
        "text": f"Patient presents with symptoms consistent with {depts[i%len(depts)].lower()} condition. Vitals stable. Treatment plan discussed.",
        "physician": f"Dr. {'Smith Jones Patel Chen Williams'.split()[i%5]}"})
with open(f"{VOLUME_PATH}/clinical_notes/clinical_notes.json", "w") as f:
    json.dump(notes, f, indent=2)

print("All data staged to Volume!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Shared Reference Tables

# COMMAND ----------

spark.sql(f"USE CATALOG {SHARED_CATALOG}")
spark.sql(f"USE SCHEMA {SHARED_SCHEMA}")

sdf = spark.createDataFrame(df_h)
sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SHARED_CATALOG}.{SHARED_SCHEMA}.heart_disease")
print(f"  heart_disease: {sdf.count()} rows")

sdf = spark.createDataFrame(df_d)
sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SHARED_CATALOG}.{SHARED_SCHEMA}.diabetes_readmission")
print(f"  diabetes_readmission: {sdf.count()} rows")

sdf = spark.createDataFrame(df_l)
sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SHARED_CATALOG}.{SHARED_SCHEMA}.life_expectancy")
print(f"  life_expectancy: {sdf.count()} rows")

sdf = spark.createDataFrame(df_r)
sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SHARED_CATALOG}.{SHARED_SCHEMA}.drug_reviews")
print(f"  drug_reviews: {sdf.count()} rows")

import pandas as _pd_notes
_df_notes = _pd_notes.DataFrame(notes)
sdf = spark.createDataFrame(_df_notes)
sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SHARED_CATALOG}.{SHARED_SCHEMA}.clinical_notes")
print(f"  clinical_notes: {sdf.count()} rows")

print("Setup complete for team_01 and team_02!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Answer Key for Event 2

# COMMAND ----------

spark.sql("USE CATALOG dataops_olympics")
spark.sql("USE SCHEMA default")

spark.sql("""
    CREATE TABLE IF NOT EXISTS event2_answer_key (
        question_id STRING, question STRING, answer STRING
    )
""")
spark.sql("DELETE FROM event2_answer_key WHERE 1=1")

answer_data = [
    ("Q1", "How many patients have heart disease (target=1)?", ""),
    ("Q2", "What is the average age of patients with heart disease?", ""),
    ("Q3", "Which chest pain type (cp) is most common among heart disease patients?", ""),
    ("Q4", "What is the average cholesterol of healthy patients (target=0)?", ""),
    ("Q5", "How many patients are over 60 years old?", ""),
    ("Q6", "What percentage of female patients (sex=0) have heart disease?", ""),
    ("Q7", "What is the average max heart rate (thalach) for the 50-59 age group?", ""),
    ("Q8", "Which age group has the highest heart disease rate?", ""),
]

for qid, q, _ in answer_data:
    spark.sql(f"INSERT INTO event2_answer_key VALUES ('{qid}', '{q.replace(chr(39), chr(39)+chr(39))}', '')")

hd = spark.table("heart_disease").toPandas()
import pandas as pd
answers = {}
answers["Q1"] = str(int((hd["target"] == 1).sum()))
answers["Q2"] = str(round(hd[hd["target"] == 1]["age"].mean(), 1))
cp_counts = hd[hd["target"] == 1]["cp"].value_counts()
answers["Q3"] = str(int(cp_counts.index[0]))
answers["Q4"] = str(round(hd[hd["target"] == 0]["chol"].mean(), 1))
answers["Q5"] = str(int((hd["age"] > 60).sum()))
female = hd[hd["sex"] == 0]
answers["Q6"] = str(round(female["target"].mean() * 100, 1))
grp = hd[(hd["age"] >= 50) & (hd["age"] < 60)]
answers["Q7"] = str(round(grp["thalach"].mean(), 1))

def _age_group(age):
    if age < 40: return "Under 40"
    elif age < 50: return "40-49"
    elif age < 60: return "50-59"
    else: return "60+"

hd["age_group"] = hd["age"].apply(_age_group)
rates = hd.groupby("age_group")["target"].mean()
answers["Q8"] = rates.idxmax()

for qid, ans in answers.items():
    spark.sql(f"UPDATE event2_answer_key SET answer = '{ans}' WHERE question_id = '{qid}'")

print("Answer key populated:")
display(spark.table("event2_answer_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # EVENT 1: Data Engineering
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event 1 — team_01: SQL path + ALL bonuses (strong team)

# COMMAND ----------

CATALOG = "team_01"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA default")
RAW = "/Volumes/dataops_olympics/default/raw_data/heart_events/"

df_bronze = spark.read.json(f"{RAW}*.json")
df_bronze.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_bronze")
print(f"Bronze: {df_bronze.count()} rows")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_silver AS
    SELECT *, current_timestamp() as ingested_at
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp) as _rn
          FROM {CATALOG}.default.heart_bronze)
    WHERE _rn = 1 AND age IS NOT NULL AND age BETWEEN 1 AND 120
      AND trestbps BETWEEN 50 AND 300 AND chol >= 0
""")
print(f"Silver: {spark.table(f'{CATALOG}.default.heart_silver').count()} rows")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_gold AS
    SELECT
        CASE WHEN age < 40 THEN 'Under 40' WHEN age < 50 THEN '40-49'
             WHEN age < 60 THEN '50-59' ELSE '60+' END as age_group,
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END as diagnosis,
        COUNT(*) as patient_count,
        ROUND(AVG(chol), 1) as avg_cholesterol,
        ROUND(AVG(trestbps), 1) as avg_blood_pressure,
        ROUND(AVG(thalach), 1) as avg_max_heart_rate
    FROM {CATALOG}.default.heart_silver GROUP BY 1, 2 ORDER BY 1, 2
""")
print(f"Gold: {spark.table(f'{CATALOG}.default.heart_gold').count()} rows")

spark.sql(f"ALTER TABLE {CATALOG}.default.heart_bronze SET TBLPROPERTIES ('comment' = 'Raw patient intake events — 5 NDJSON batches')")
spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver SET TBLPROPERTIES ('comment' = 'Cleaned patient data — deduplicated, validated')")
spark.sql(f"ALTER TABLE {CATALOG}.default.heart_gold SET TBLPROPERTIES ('comment' = 'Heart disease metrics by age group')")
for col, comm in [("age", "Patient age (1-120)"), ("trestbps", "Resting BP mmHg (50-300)"),
                  ("chol", "Cholesterol mg/dL"), ("target", "1=disease, 0=healthy"), ("event_id", "Unique event ID")]:
    spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver ALTER COLUMN {col} COMMENT '{comm}'")
print("Governance applied")

try:
    spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver CLUSTER BY (age, target)")
    print("Liquid Clustering applied")
except Exception as e:
    print(f"LC: {e}")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_gold_ai AS
    SELECT *, ai_query('databricks-meta-llama-3-3-70b-instruct',
        CONCAT('Classify cardiovascular risk as LOW, MEDIUM, or HIGH for: ',
               age_group, ' patients, ', diagnosis, ', avg chol=', avg_cholesterol,
               ', avg BP=', avg_blood_pressure, '. Respond with ONLY: LOW, MEDIUM, or HIGH')
    ) AS cardiovascular_risk FROM {CATALOG}.default.heart_gold LIMIT 5
""")
print(f"AI Functions: {spark.table(f'{CATALOG}.default.heart_gold_ai').count()} rows")
print("team_01 Event 1 COMPLETE — SQL path + all bonuses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event 1 — team_02: SQL path, partial governance, NO bonuses (weaker team)

# COMMAND ----------

CATALOG = "team_02"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA default")

df_bronze = spark.read.json(f"{RAW}*.json")
df_bronze.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_bronze")
print(f"Bronze: {df_bronze.count()} rows")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_silver AS
    SELECT *, current_timestamp() as ingested_at
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp) as _rn
          FROM {CATALOG}.default.heart_bronze)
    WHERE _rn = 1 AND age IS NOT NULL AND age BETWEEN 1 AND 120
      AND trestbps BETWEEN 50 AND 300 AND chol >= 0
""")
print(f"Silver: {spark.table(f'{CATALOG}.default.heart_silver').count()} rows")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_gold AS
    SELECT
        CASE WHEN age < 40 THEN 'Under 40' WHEN age < 50 THEN '40-49'
             WHEN age < 60 THEN '50-59' ELSE '60+' END as age_group,
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END as diagnosis,
        COUNT(*) as patient_count,
        ROUND(AVG(chol), 1) as avg_cholesterol,
        ROUND(AVG(trestbps), 1) as avg_blood_pressure,
        ROUND(AVG(thalach), 1) as avg_max_heart_rate
    FROM {CATALOG}.default.heart_silver GROUP BY 1, 2 ORDER BY 1, 2
""")
print(f"Gold: {spark.table(f'{CATALOG}.default.heart_gold').count()} rows")

spark.sql(f"ALTER TABLE {CATALOG}.default.heart_bronze SET TBLPROPERTIES ('comment' = 'Raw events')")
print("team_02 Event 1 COMPLETE — SQL path, partial governance, no bonuses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score Event 1

# COMMAND ----------

spark.sql("USE CATALOG dataops_olympics")
spark.sql("USE SCHEMA default")

import pandas as _pd
import sys
sys.path.insert(0, "/Workspace/Users/sean.zhang@databricks.com/gsk-dataops-olympics")

TEAMS = ["team_01", "team_02"]
SCHEMA = "default"

EXPECTED_BRONZE_COLS = {"event_id", "event_timestamp", "source_system", "patient_id", "age", "sex", "cp", "trestbps", "chol", "target"}
EXPECTED_GOLD_COLS = {"age_group", "diagnosis", "patient_count"}
EXPECTED_GOLD_AGG_COLS = {"avg_cholesterol", "avg_blood_pressure", "avg_max_heart_rate"}

def _fqn(catalog, table): return f"{catalog}.{SCHEMA}.{table}"
def _table_exists(catalog, table):
    try:
        rows = spark.sql(f"SHOW TABLES IN {catalog}.{SCHEMA} LIKE '{table}'").collect()
        return len(rows) > 0
    except Exception:
        return False
def _is_sdp_table(catalog, table):
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES {_fqn(catalog, table)}").collect()
        return any("pipeline" in str(r[0]).lower() for r in rows)
    except: return False
def _count_comments(catalog, table):
    try:
        desc = spark.sql(f"DESCRIBE TABLE EXTENDED {_fqn(catalog, table)}").collect()
        has_tbl, col_cnt, in_detail = False, 0, False
        for r in desc:
            if r[0] and r[0].strip() == "# Detailed Table Information": in_detail = True; continue
            if in_detail and r[0] and r[0].strip().lower() == "comment" and r[1] and len(str(r[1]).strip()) > 3: has_tbl = True
            if not in_detail and r[0] and r[0].strip() not in ("", "#") and r[2] and len(str(r[2]).strip()) > 3: col_cnt += 1
        return has_tbl, col_cnt
    except: return False, 0
def _has_liquid_clustering(catalog, table):
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {_fqn(catalog, table)}").collect()[0]
        cc = detail["clusteringColumns"]
        return cc is not None and len(cc) > 0
    except: return False

def score_e1(team_name):
    catalog = team_name
    scores = {"team": team_name, "bronze": 0, "silver": 0, "gold": 0, "dq": 0, "governance": 0, "bonus": 0, "total": 0, "path": "?"}
    is_sdp = (_table_exists(catalog, "heart_bronze") and _is_sdp_table(catalog, "heart_bronze")) or \
             (_table_exists(catalog, "heart_silver") and _is_sdp_table(catalog, "heart_silver"))
    scores["path"] = "SDP" if is_sdp else "SQL"
    if _table_exists(catalog, "heart_bronze"):
        try:
            bc = spark.sql(f"SELECT COUNT(*) AS c FROM {_fqn(catalog, 'heart_bronze')}").collect()[0][0]
            bcols = set(r[0].lower() for r in spark.sql(f"DESCRIBE TABLE {_fqn(catalog, 'heart_bronze')}").collect() if r[0] and not r[0].startswith("#"))
            if bc >= 495: scores["bronze"] += 5
            elif bc >= 400: scores["bronze"] += 4
            elif bc > 0: scores["bronze"] += 2
            if bc >= 498: scores["bronze"] += 3
            elif bc >= 300: scores["bronze"] += 1
            if EXPECTED_BRONZE_COLS.issubset(bcols): scores["bronze"] += 2
            elif len(EXPECTED_BRONZE_COLS - bcols) <= 2: scores["bronze"] += 1
        except Exception: pass
    if _table_exists(catalog, "heart_silver"):
        try:
            sc = spark.sql(f"SELECT COUNT(*) AS c FROM {_fqn(catalog, 'heart_silver')}").collect()[0][0]
            dc = spark.sql(f"SELECT COUNT(DISTINCT event_id) AS c FROM {_fqn(catalog, 'heart_silver')}").collect()[0][0]
            if is_sdp:
                scores["silver"] += 10
                if 480 <= sc <= 495: scores["silver"] += 3
                elif sc < 498: scores["silver"] += 1
                if dc == sc: scores["silver"] += 2
            else:
                if sc > 0: scores["silver"] += 4
                if 480 <= sc <= 495: scores["silver"] += 2
                elif sc < 498: scores["silver"] += 1
                if dc == sc: scores["silver"] += 2
        except Exception: pass
    if _table_exists(catalog, "heart_gold"):
        try:
            gc = spark.sql(f"SELECT COUNT(*) AS c FROM {_fqn(catalog, 'heart_gold')}").collect()[0][0]
            gcols = set(r[0].lower() for r in spark.sql(f"DESCRIBE TABLE {_fqn(catalog, 'heart_gold')}").collect() if r[0] and not r[0].startswith("#"))
            if is_sdp:
                if gc > 0: scores["gold"] += 7
                if EXPECTED_GOLD_COLS.issubset(gcols) and EXPECTED_GOLD_AGG_COLS.issubset(gcols): scores["gold"] += 5
                elif EXPECTED_GOLD_COLS.issubset(gcols): scores["gold"] += 3
                ht, _ = _count_comments(catalog, "heart_gold")
                if ht: scores["gold"] += 3
            else:
                if gc > 0: scores["gold"] += 4
                if EXPECTED_GOLD_COLS.issubset(gcols) and any("avg" in c for c in gcols): scores["gold"] += 2
                elif any("avg" in c for c in gcols): scores["gold"] += 1
                ht, _ = _count_comments(catalog, "heart_gold")
                if ht: scores["gold"] += 2
        except Exception: pass
    if is_sdp: scores["dq"] += 5
    else:
        if _table_exists(catalog, "heart_bronze") and _table_exists(catalog, "heart_silver"):
            try:
                sc_dq = spark.sql(f"SELECT COUNT(*) AS c FROM {_fqn(catalog, 'heart_silver')}").collect()[0][0]
                bc_dq = spark.sql(f"SELECT COUNT(*) AS c FROM {_fqn(catalog, 'heart_bronze')}").collect()[0][0]
                if sc_dq < bc_dq: scores["dq"] += 2
            except Exception: pass
    best_tc, best_cc = False, 0
    for t in ["heart_silver", "heart_bronze", "heart_gold"]:
        if _table_exists(catalog, t):
            tc, cc = _count_comments(catalog, t)
            if tc: best_tc = True
            best_cc = max(best_cc, cc)
    if is_sdp:
        if best_tc: scores["governance"] += 2
        if best_cc >= 3: scores["governance"] += 3
        elif best_cc > 0: scores["governance"] += best_cc
    else:
        if best_tc: scores["governance"] += 1
        if best_cc >= 3: scores["governance"] += 2
        elif best_cc > 0: scores["governance"] += 1
    if _table_exists(catalog, "heart_silver") and _has_liquid_clustering(catalog, "heart_silver"):
        scores["bonus"] += 3
    if _table_exists(catalog, "heart_gold_ai"):
        try:
            ai_count = spark.sql(f"SELECT COUNT(*) AS c FROM {_fqn(catalog, 'heart_gold_ai')}").collect()[0][0]
            ai_cols = [r[0].lower() for r in spark.sql(f"DESCRIBE TABLE {_fqn(catalog, 'heart_gold_ai')}").collect() if r[0] and not r[0].startswith("#")]
            if "cardiovascular_risk" in ai_cols and ai_count > 0:
                scores["bonus"] += 2
            elif ai_count > 0:
                scores["bonus"] += 1
        except Exception:
            pass
    scores["total"] = sum(scores[k] for k in ["bronze", "silver", "gold", "dq", "governance", "bonus"])
    return scores

results_e1 = [score_e1(t) for t in TEAMS]
for r in results_e1:
    print(f"  {r['team']}: {r['total']}/55 ({r['path']}) B:{r['bronze']} S:{r['silver']} G:{r['gold']} DQ:{r['dq']} Gov:{r['governance']} Bonus:{r['bonus']}")

df_e1 = _pd.DataFrame([{"Team": r["team"], "Path": r["path"], "Bronze": r["bronze"], "Silver": r["silver"],
    "Gold": r["gold"], "DQ": r["dq"], "Governance": r["governance"], "Bonus": r["bonus"], "Total": r["total"]}
    for r in results_e1]).sort_values("Total", ascending=False)
spark.createDataFrame(df_e1).write.format("delta").mode("overwrite").saveAsTable("dataops_olympics.default.event1_scores")

_LB = "dataops_olympics.default.olympics_leaderboard"
_RT = "dataops_olympics.default.registered_teams"
spark.sql(f"CREATE TABLE IF NOT EXISTS {_LB} (team STRING, event STRING, category STRING, points DOUBLE, max_points DOUBLE, scored_at TIMESTAMP)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {_RT} (team STRING)")
from datetime import datetime as _dt
_now = _dt.now()
for r in results_e1:
    _t = r["team"]
    if spark.sql(f"SELECT 1 FROM {_RT} WHERE team = '{_t}'").count() == 0:
        spark.sql(f"INSERT INTO {_RT} VALUES ('{_t}')")
    for cat, pts, mx in [("Bronze", r["bronze"], 10), ("Silver", r["silver"], 15), ("Gold", r["gold"], 15),
                         ("DQ", r["dq"], 5), ("Governance", r["governance"], 5), ("Bonus", r["bonus"], 5)]:
        spark.sql(f"INSERT INTO {_LB} VALUES ('{_t}', 'Event 1: Data Engineering', '{cat}', {pts}, {mx}, '{_now}')")
print("Event 1 scored and leaderboard updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # EVENT 2: Data Analytics
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event 2 — team_01: All correct + bonuses

# COMMAND ----------

CATALOG = "team_01"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA default")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.default.event2_submissions
    (team STRING, question_id STRING, answer STRING, method STRING, submitted_at STRING)""")

from datetime import datetime as _dt2
_ts = _dt2.now().isoformat()

_answers = spark.table("dataops_olympics.default.event2_answer_key").toPandas()
for _, row in _answers.iterrows():
    qid, ans = row["question_id"], row["answer"]
    spark.sql(f"INSERT INTO {CATALOG}.default.event2_submissions VALUES ('{CATALOG}', '{qid}', '{ans}', 'SQL', '{_ts}')")
print(f"team_01: submitted all 8 answers (correct)")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_executive_summary AS
    SELECT 'team_01' AS team, ai_query('databricks-meta-llama-3-3-70b-instruct',
        'Write a 3-sentence executive summary about heart disease patient data with these stats: '
        || (SELECT CONCAT('total_cohorts=', COUNT(*), ', disease_cohorts=', SUM(CASE WHEN diagnosis='Heart Disease' THEN 1 ELSE 0 END), ', total_patients=', SUM(patient_count))
            FROM {CATALOG}.default.heart_gold)
    ) AS summary, current_timestamp() AS generated_at
""")
print("Bonus: executive summary created")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_cohort_comparison AS
    SELECT CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS cohort,
           COUNT(*) AS patient_count, ROUND(AVG(age),1) AS avg_age,
           ROUND(AVG(chol),1) AS avg_cholesterol, ROUND(AVG(trestbps),1) AS avg_bp,
           ROUND(AVG(thalach),1) AS avg_max_hr
    FROM {CATALOG}.default.heart_silver GROUP BY target
""")
print("Bonus: cohort comparison created")
print("team_01 Event 2 COMPLETE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event 2 — team_02: 5/8 correct, no bonuses

# COMMAND ----------

CATALOG = "team_02"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA default")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.default.event2_submissions
    (team STRING, question_id STRING, answer STRING, method STRING, submitted_at STRING)""")

_answers_df = spark.table("dataops_olympics.default.event2_answer_key").toPandas()
for idx, (_, row) in enumerate(_answers_df.iterrows()):
    qid, correct_ans = row["question_id"], row["answer"]
    if idx < 5:
        ans = correct_ans
    else:
        ans = "wrong answer"
    spark.sql(f"INSERT INTO {CATALOG}.default.event2_submissions VALUES ('{CATALOG}', '{qid}', '{ans}', 'SQL', '{_ts}')")
print("team_02: submitted 8 answers (5 correct, 3 wrong)")
print("team_02 Event 2 COMPLETE — no bonuses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score Event 2

# COMMAND ----------

spark.sql("USE CATALOG dataops_olympics")
spark.sql("USE SCHEMA default")

def score_e2(team_name):
    scores = {"team": team_name, "build_sql": 0, "build_genie": 0, "benchmark": 0, "speed_bonus": 0, "bonus": 0, "total": 0}
    try:
        subs = spark.sql(f"""
            SELECT question_id, answer, method,
                ROW_NUMBER() OVER (PARTITION BY question_id ORDER BY submitted_at DESC) AS rn
            FROM {team_name}.default.event2_submissions WHERE team = '{team_name}'
        """).filter("rn = 1").drop("rn").toPandas()
        scores["build_sql"] = min(len(subs), 3)
    except:
        subs = _pd.DataFrame()
    ak = spark.table("event2_answer_key").toPandas()
    for _, row in ak.iterrows():
        qid = row["question_id"]
        correct = str(row["answer"]).strip()
        sub_row = subs[subs["question_id"] == qid] if len(subs) > 0 else _pd.DataFrame()
        if len(sub_row) > 0:
            team_ans = str(sub_row.iloc[0]["answer"]).strip()
            try:
                if abs(float(team_ans) - float(correct)) <= 0.5:
                    scores["benchmark"] += 2; continue
            except: pass
            if team_ans.lower() == correct.lower():
                scores["benchmark"] += 2; continue
    for tbl, pts in [("heart_executive_summary", 3), ("heart_cohort_comparison", 2), ("heart_chi2_test", 3)]:
        if _table_exists(team_name, tbl): scores["bonus"] += pts
    scores["total"] = sum(scores[k] for k in ["build_sql", "build_genie", "benchmark", "speed_bonus", "bonus"])
    return scores

results_e2 = [score_e2(t) for t in TEAMS]
for r in results_e2:
    print(f"  {r['team']}: {r['total']}/48 SQL:{r['build_sql']} Bench:{r['benchmark']} Bonus:{r['bonus']}")

df_e2 = _pd.DataFrame([{"Team": r["team"], "Build_SQL": r["build_sql"], "Build_Genie": r["build_genie"],
    "Benchmark": r["benchmark"], "Speed": r["speed_bonus"], "Bonus": r["bonus"], "Total": r["total"]}
    for r in results_e2]).sort_values("Total", ascending=False)
spark.createDataFrame(df_e2).write.format("delta").mode("overwrite").saveAsTable("dataops_olympics.default.event2_scores")

_now = _dt.now()
for r in results_e2:
    _t = r["team"]
    for cat, pts, mx in [("Build_SQL", r["build_sql"], 3), ("Build_Genie", r["build_genie"], 5),
                         ("Benchmark", r["benchmark"], 24), ("Speed", r["speed_bonus"], 8), ("Bonus", r["bonus"], 8)]:
        spark.sql(f"INSERT INTO {_LB} VALUES ('{_t}', 'Event 2: Data Analytics', '{cat}', {pts}, {mx}, '{_now}')")
print("Event 2 scored and leaderboard updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # EVENT 3: Data Science / ML
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event 3 — team_01: Full ML pipeline + all bonuses

# COMMAND ----------

import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, VotingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score
import mlflow, mlflow.sklearn
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

CATALOG = "team_01"
df = spark.table(f"{CATALOG}.default.heart_silver").toPandas()
feature_cols = ["age", "sex", "cp", "trestbps", "chol", "fbs", "restecg", "thalach", "exang", "oldpeak", "slope", "ca", "thal"]
df["hr_reserve"] = df["thalach"] - df["age"]
df["high_risk"] = ((df["chol"] > 240) & (df["trestbps"] > 140)).astype(int)
df["age_chol"] = df["age"] * df["chol"]
feature_cols += ["hr_reserve", "high_risk", "age_chol"]
X = df[feature_cols].fillna(0)
y = df["target"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

user_email = spark.sql("SELECT current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{user_email}/team_01_heart_ml")

models = [
    ("RandomForest", RandomForestClassifier(n_estimators=200, max_depth=8, random_state=42)),
    ("GradientBoosting", GradientBoostingClassifier(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42)),
    ("LogisticRegression", LogisticRegression(max_iter=2000, random_state=42)),
]
best_f1 = 0; best_model = None; best_run_id = None
for name, clf in models:
    with mlflow.start_run(run_name=name):
        clf.fit(X_train, y_train)
        preds = clf.predict(X_test)
        f1 = f1_score(y_test, preds)
        sig = infer_signature(X_train, preds)
        mlflow.log_params({"model_type": name, "n_features": len(feature_cols)})
        mlflow.log_metric("f1_score", f1)
        mlflow.sklearn.log_model(clf, "model", signature=sig, input_example=X_test[:3])
        print(f"  {name}: F1={f1:.4f}")
        if f1 > best_f1:
            best_f1 = f1; best_model = clf; best_run_id = mlflow.active_run().info.run_id

with mlflow.start_run(run_name="VotingEnsemble"):
    vc = VotingClassifier(estimators=[(n, c) for n, c in models], voting="soft")
    vc.fit(X_train, y_train)
    preds = vc.predict(X_test)
    f1_vc = f1_score(y_test, preds)
    sig = infer_signature(X_train, preds)
    mlflow.log_params({"model_type": "VotingClassifier", "n_features": len(feature_cols)})
    mlflow.log_metric("f1_score", f1_vc)
    mlflow.sklearn.log_model(vc, "model", signature=sig, input_example=X_test[:3])
    print(f"  VotingEnsemble: F1={f1_vc:.4f}")
    if f1_vc > best_f1:
        best_f1 = f1_vc; best_run_id = mlflow.active_run().info.run_id

try:
    mlflow.register_model(f"runs:/{best_run_id}/model", f"{CATALOG}.default.heart_disease_model")
    print(f"  Model registered: {CATALOG}.default.heart_disease_model")
except Exception as e:
    print(f"  Registration note: {str(e)[:80]}")

cv = cross_val_score(best_model, X, y, cv=5, scoring="f1")
cv_df = _pd.DataFrame({"fold": list(range(1, 6)), "f1_score": cv})
spark.createDataFrame(cv_df).write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_cv_results")
print("  Bonus: CV results saved")

importances = best_model.feature_importances_ if hasattr(best_model, "feature_importances_") else [0]*len(feature_cols)
imp_df = _pd.DataFrame({"feature": feature_cols, "importance": importances}).sort_values("importance", ascending=False)
spark.createDataFrame(imp_df).write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_shap_importance")
print("  Bonus: SHAP importance saved")
print(f"team_01 Event 3 COMPLETE — best F1={best_f1:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event 3 — team_02: Basic ML, 1 model, no bonuses

# COMMAND ----------

CATALOG = "team_02"
df2 = spark.table(f"{CATALOG}.default.heart_silver").toPandas()
base_cols = ["age", "sex", "cp", "trestbps", "chol", "fbs", "restecg", "thalach", "exang", "oldpeak", "slope", "ca", "thal"]
X2 = df2[base_cols].fillna(0)
y2 = df2["target"]
X2_train, X2_test, y2_train, y2_test = train_test_split(X2, y2, test_size=0.2, random_state=42, stratify=y2)

mlflow.set_experiment(f"/Users/{user_email}/team_02_heart_ml")
with mlflow.start_run(run_name="RandomForest"):
    rf2 = RandomForestClassifier(n_estimators=100, random_state=42)
    rf2.fit(X2_train, y2_train)
    preds2 = rf2.predict(X2_test)
    f1_2 = f1_score(y2_test, preds2)
    sig2 = infer_signature(X2_train, preds2)
    mlflow.log_params({"model_type": "RandomForest", "n_features": len(base_cols)})
    mlflow.log_metric("f1_score", f1_2)
    mlflow.sklearn.log_model(rf2, "model", signature=sig2, input_example=X2_test[:3])
    print(f"  team_02 RF: F1={f1_2:.4f}")

print("team_02 Event 3 COMPLETE — 1 model, no bonuses, no registration")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score Event 3

# COMMAND ----------

spark.sql("USE CATALOG dataops_olympics")
spark.sql("USE SCHEMA default")

def _get_best_f1(team_name):
    best_f1, model_type, n_runs = 0.0, "unknown", 0
    best_run = None
    for path in [f"/Users/{user_email}/{team_name}_heart_ml"]:
        try:
            exp = client.get_experiment_by_name(path)
            if not exp: continue
            runs = client.search_runs(experiment_ids=[exp.experiment_id], order_by=["metrics.f1_score DESC"], max_results=20)
            n_runs += len(runs)
            for run in runs:
                f1 = run.data.metrics.get("f1_score", 0)
                if f1 > best_f1: best_f1 = f1; best_run = run; model_type = run.data.params.get("model_type", "unknown")
        except: continue
    return best_f1, model_type, n_runs, best_run

def score_e3(team_name):
    catalog = team_name
    scores = {"team": team_name, "eda": 0, "features": 0, "mlflow": 0, "performance": 0, "registration": 0, "bonus": 0, "total": 0, "f1_score": 0.0}
    if _table_exists(catalog, "heart_silver"): scores["eda"] = 5
    best_f1, model_type, n_runs, best_run = _get_best_f1(team_name)
    scores["f1_score"] = best_f1
    if best_run:
        n_feat = int(best_run.data.params.get("n_features", 13))
        scores["features"] = 5 if n_feat > 15 else (3 if n_feat > 13 else 1)
    if n_runs >= 3: scores["mlflow"] = 10
    elif n_runs >= 2: scores["mlflow"] = 7
    elif n_runs >= 1: scores["mlflow"] = 5
    if best_f1 >= 0.90: scores["performance"] = 15
    elif best_f1 >= 0.85: scores["performance"] = 12
    elif best_f1 >= 0.80: scores["performance"] = 10
    elif best_f1 >= 0.70: scores["performance"] = 7
    elif best_f1 > 0: scores["performance"] = 4
    try:
        models = spark.sql(f"SHOW MODELS IN {catalog}.default").collect()
        if len(models) > 0: scores["registration"] = 5
        elif n_runs > 0: scores["registration"] = 2
    except:
        if n_runs > 0: scores["registration"] = 2
    if _table_exists(catalog, "heart_shap_importance"): scores["bonus"] += 3
    if best_run:
        try:
            exp = client.get_experiment_by_name(f"/Users/{user_email}/{team_name}_heart_ml")
            if exp:
                runs = client.search_runs(experiment_ids=[exp.experiment_id])
                if any(r.data.params.get("model_type", "").lower() in ("votingclassifier", "ensemble") for r in runs):
                    scores["bonus"] += 3
        except: pass
    if _table_exists(catalog, "heart_cv_results"): scores["bonus"] += 2
    scores["total"] = sum(scores[k] for k in ["eda", "features", "mlflow", "performance", "registration", "bonus"])
    return scores

results_e3 = [score_e3(t) for t in TEAMS]
for r in results_e3:
    print(f"  {r['team']}: {r['total']}/48 F1={r['f1_score']:.4f} EDA:{r['eda']} Feat:{r['features']} ML:{r['mlflow']} Perf:{r['performance']} Reg:{r['registration']} Bonus:{r['bonus']}")

df_e3 = _pd.DataFrame([{"Team": r["team"], "EDA": r["eda"], "Features": r["features"], "MLflow": r["mlflow"],
    "Performance": r["performance"], "Registration": r["registration"], "Bonus": r["bonus"],
    "Total": r["total"], "F1": r["f1_score"]} for r in results_e3]).sort_values("Total", ascending=False)
spark.createDataFrame(df_e3).write.format("delta").mode("overwrite").saveAsTable("dataops_olympics.default.event3_scores")

_now = _dt.now()
for r in results_e3:
    _t = r["team"]
    for cat, pts, mx in [("EDA", r["eda"], 5), ("Features", r["features"], 5), ("MLflow", r["mlflow"], 10),
                         ("Performance", r["performance"], 15), ("Registration", r["registration"], 5), ("Bonus", r["bonus"], 8)]:
        spark.sql(f"INSERT INTO {_LB} VALUES ('{_t}', 'Event 3: Data Science', '{cat}', {pts}, {mx}, '{_now}')")
print("Event 3 scored and leaderboard updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # EVENT 4: GenAI / Agents
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event 4 — team_01: Full agent + AI functions + audit log

# COMMAND ----------

CATALOG = "team_01"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA default")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_ai_insights AS
    SELECT *, ai_query('databricks-meta-llama-3-3-70b-instruct',
        CONCAT('Provide a 1-sentence clinical insight for: age_group=', age_group,
               ', diagnosis=', diagnosis, ', avg_chol=', avg_cholesterol,
               ', avg_bp=', avg_blood_pressure, ', patients=', patient_count)
    ) AS clinical_insight FROM {CATALOG}.default.heart_gold
""")
print(f"heart_ai_insights: {spark.table(f'{CATALOG}.default.heart_ai_insights').count()} rows")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.drug_ai_summary AS
    SELECT drug_name, ROUND(AVG(rating), 1) AS avg_rating, COUNT(*) AS review_count,
        ai_query('databricks-meta-llama-3-3-70b-instruct',
            CONCAT('Summarize drug reviews for ', drug_name, ' (avg rating: ', ROUND(AVG(rating),1),
                   ', ', COUNT(*), ' reviews). One sentence.')
        ) AS ai_summary
    FROM dataops_olympics.default.drug_reviews GROUP BY drug_name
""")
print(f"drug_ai_summary: {spark.table(f'{CATALOG}.default.drug_ai_summary').count()} rows")

audit_data = [
    ("What heart disease rates exist?", "Based on gold table: 8 age-group cohorts analyzed", "success"),
    ("Summarize drug reviews for Metformin", "Metformin has avg rating 5.2 across 67 reviews", "success"),
    ("Show clinical notes for cardiology", "Found 4 cardiology notes in clinical_notes table", "success"),
]
audit_df = _pd.DataFrame(audit_data, columns=["question", "answer", "status"])
spark.createDataFrame(audit_df).write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.agent_audit_log")
print("Bonus: agent audit log created")
print("team_01 Event 4 COMPLETE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event 4 — team_02: Partial agent, only heart insights

# COMMAND ----------

CATALOG = "team_02"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA default")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_ai_insights AS
    SELECT *, ai_query('databricks-meta-llama-3-3-70b-instruct',
        CONCAT('Clinical insight: age_group=', age_group, ', diagnosis=', diagnosis,
               ', avg_chol=', avg_cholesterol)
    ) AS clinical_insight FROM {CATALOG}.default.heart_gold
""")
print(f"team_02 heart_ai_insights: {spark.table(f'{CATALOG}.default.heart_ai_insights').count()} rows")
print("team_02 Event 4 COMPLETE — partial agent, no drug summary, no audit log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score Event 4

# COMMAND ----------

spark.sql("USE CATALOG dataops_olympics")
spark.sql("USE SCHEMA default")
SHARED_CATALOG = "dataops_olympics"

def _table_count(catalog, table):
    try: return spark.sql(f"SELECT COUNT(*) AS c FROM {catalog}.default.{table}").collect()[0][0]
    except Exception: return 0
def _has_column(catalog, table, col):
    try:
        cols = [r[0].lower() for r in spark.sql(f"DESCRIBE TABLE {catalog}.default.{table}").collect() if r[0] and not r[0].startswith("#")]
        return col.lower() in cols
    except Exception: return False

def score_e4(team_name):
    catalog = team_name
    scores = {"team": team_name, "exploration": 0, "system_prompt": 0, "agent_function": 0, "ai_functions": 0, "test_prompts": 0, "bonus": 0, "total": 0}
    tables_acc = sum(1 for t, c in [("heart_silver", catalog), ("heart_gold", catalog),
                     ("drug_reviews", SHARED_CATALOG), ("clinical_notes", SHARED_CATALOG)] if _table_exists(c, t))
    scores["exploration"] = 3 if tables_acc >= 4 else (2 if tables_acc >= 2 else (1 if tables_acc >= 1 else 0))
    has_ai = _table_exists(catalog, "heart_ai_insights") or _table_exists(catalog, "drug_ai_summary")
    scores["system_prompt"] = 5 if has_ai else (2 if _table_exists(catalog, "heart_gold") else 0)
    ag = 0
    if _table_exists(catalog, "heart_ai_insights") and _table_count(catalog, "heart_ai_insights") > 0: ag += 4
    if _table_exists(catalog, "drug_ai_summary") and _table_count(catalog, "drug_ai_summary") > 0: ag += 4
    if _table_exists(catalog, "agent_audit_log"): ag += 2
    if ag >= 8: ag = min(ag + 2, 12)
    scores["agent_function"] = ag
    ai = 0
    if _table_exists(catalog, "heart_ai_insights") and _has_column(catalog, "heart_ai_insights", "clinical_insight") and _table_count(catalog, "heart_ai_insights") > 0: ai += 5
    if _table_exists(catalog, "drug_ai_summary") and _has_column(catalog, "drug_ai_summary", "ai_summary") and _table_count(catalog, "drug_ai_summary") > 0: ai += 5
    scores["ai_functions"] = ai
    scores["test_prompts"] = 10 if ai >= 8 else (7 if ai >= 5 else (5 if ag >= 6 else (2 if ag > 0 else 0)))
    if _table_exists(catalog, "genie_agent_log") and _table_count(catalog, "genie_agent_log") > 0: scores["bonus"] += 3
    if _table_exists(catalog, "agent_audit_log"): scores["bonus"] += 2
    if _table_exists(catalog, "heart_ai_insights") and _table_exists(catalog, "drug_ai_summary"):
        if _table_count(catalog, "heart_ai_insights") > 0 and _table_count(catalog, "drug_ai_summary") > 0:
            scores["bonus"] += 3
    scores["total"] = sum(scores[k] for k in ["exploration", "system_prompt", "agent_function", "ai_functions", "test_prompts", "bonus"])
    return scores

results_e4 = [score_e4(t) for t in TEAMS]
for r in results_e4:
    print(f"  {r['team']}: {r['total']}/48 Exp:{r['exploration']} Prompt:{r['system_prompt']} Agent:{r['agent_function']} AI:{r['ai_functions']} Test:{r['test_prompts']} Bonus:{r['bonus']}")

df_e4 = _pd.DataFrame([{"Team": r["team"], "Exploration": r["exploration"], "SystemPrompt": r["system_prompt"],
    "AgentFunction": r["agent_function"], "AIFunctions": r["ai_functions"], "TestPrompts": r["test_prompts"],
    "Bonus": r["bonus"], "Total": r["total"]} for r in results_e4]).sort_values("Total", ascending=False)
spark.createDataFrame(df_e4).write.format("delta").mode("overwrite").saveAsTable("dataops_olympics.default.event4_scores")

_now = _dt.now()
for r in results_e4:
    _t = r["team"]
    for cat, pts, mx in [("Exploration", r["exploration"], 3), ("SystemPrompt", r["system_prompt"], 5),
                         ("AgentFunction", r["agent_function"], 12), ("AIFunctions", r["ai_functions"], 10),
                         ("TestPrompts", r["test_prompts"], 10), ("Bonus", r["bonus"], 8)]:
        spark.sql(f"INSERT INTO {_LB} VALUES ('{_t}', 'Event 4: GenAI Agents', '{cat}', {pts}, {mx}, '{_now}')")
print("Event 4 scored and leaderboard updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # EVENT 5: Capstone
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event 5 — team_01: Full capstone with executive briefing

# COMMAND ----------

CATALOG = "team_01"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA default")

metrics = {}
for tbl in ["heart_silver", "heart_gold", "heart_ai_insights", "drug_ai_summary"]:
    metrics[tbl] = _table_count(CATALOG, tbl)

prompt = f"""You are a hospital data analyst. Write a comprehensive executive briefing (300+ words) covering:
1. Patient population: {metrics.get('heart_silver', 0)} patients analyzed
2. Heart disease prevalence from gold table ({metrics.get('heart_gold', 0)} cohorts)
3. AI-powered insights from {metrics.get('heart_ai_insights', 0)} clinical analyses
4. Drug review analysis covering {metrics.get('drug_ai_summary', 0)} medications
Include specific recommendations for hospital leadership."""

briefing = spark.sql(f"""
    SELECT ai_query('databricks-meta-llama-3-3-70b-instruct', "{prompt.replace('"', "''")}") AS text
""").collect()[0][0]

import pandas as _pd5
from datetime import datetime as _dt5
b_df = _pd5.DataFrame([{
    "team": CATALOG, "briefing_text": briefing,
    "total_patients": int(metrics.get("heart_silver", 0)),
    "disease_prevalence_pct": 0.0,
    "generated_at": _dt5.now().isoformat()
}])
spark.createDataFrame(b_df).write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.executive_briefing")
print(f"Executive briefing: {len(briefing)} chars")
print("team_01 Event 5 COMPLETE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event 5 — team_02: Minimal capstone, short briefing

# COMMAND ----------

CATALOG = "team_02"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA default")

brief2 = spark.sql(f"""
    SELECT ai_query('databricks-meta-llama-3-3-70b-instruct',
        'Write a brief hospital summary about heart disease patients.')
    AS text
""").collect()[0][0]

b_df2 = _pd5.DataFrame([{"team": CATALOG, "briefing_text": brief2,
    "total_patients": _table_count(CATALOG, "heart_silver"),
    "disease_prevalence_pct": 0.0, "generated_at": _dt5.now().isoformat()}])
spark.createDataFrame(b_df2).write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.executive_briefing")
print(f"team_02 briefing: {len(brief2)} chars")
print("team_02 Event 5 COMPLETE — minimal capstone")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score Event 5

# COMMAND ----------

spark.sql("USE CATALOG dataops_olympics")
spark.sql("USE SCHEMA default")

def score_e5(team_name):
    catalog = team_name
    scores = {"team": team_name, "artifacts": 0, "briefing": 0, "dashboard": 0, "genie": 0, "presentation": 0, "bonus": 0, "total": 0}
    found = sum(1 for t in ["heart_silver", "heart_gold", "heart_ai_insights", "drug_ai_summary"] if _table_exists(catalog, t))
    scores["artifacts"] = 3 if found >= 4 else (2 if found >= 2 else (1 if found >= 1 else 0))
    if _table_exists(catalog, "executive_briefing"):
        try:
            b = spark.sql(f"SELECT briefing_text FROM {catalog}.default.executive_briefing LIMIT 1").collect()[0][0]
            scores["briefing"] = 5 if b and len(b) > 200 else (3 if b and len(b) > 50 else 1)
        except Exception: scores["briefing"] = 2
    if _table_exists(catalog, "executive_briefing"):
        try:
            cols = [r[0].lower() for r in spark.sql(f"DESCRIBE TABLE {catalog}.default.executive_briefing").collect() if r[0] and not r[0].startswith("#")]
            if "disease_prevalence_pct" in cols and "total_patients" in cols: scores["bonus"] += 2
        except Exception: pass
    scores["total"] = sum(scores[k] for k in ["artifacts", "briefing", "dashboard", "genie", "presentation", "bonus"])
    return scores

results_e5 = [score_e5(t) for t in TEAMS]
for r in results_e5:
    print(f"  {r['team']}: {r['total']}/35 Art:{r['artifacts']} Brief:{r['briefing']} Dash:{r['dashboard']} Genie:{r['genie']} Pres:{r['presentation']} Bonus:{r['bonus']}")

df_e5 = _pd.DataFrame([{"Team": r["team"], "Artifacts": r["artifacts"], "Briefing": r["briefing"],
    "Dashboard": r["dashboard"], "Genie": r["genie"], "Presentation": r["presentation"],
    "Bonus": r["bonus"], "Total": r["total"]} for r in results_e5]).sort_values("Total", ascending=False)
spark.createDataFrame(df_e5).write.format("delta").mode("overwrite").saveAsTable("dataops_olympics.default.event5_scores")

_now = _dt.now()
for r in results_e5:
    _t = r["team"]
    for cat, pts, mx in [("Artifacts", r["artifacts"], 3), ("Briefing", r["briefing"], 5),
                         ("Dashboard", r["dashboard"], 10), ("Genie", r["genie"], 5),
                         ("Presentation", r["presentation"], 7), ("Bonus", r["bonus"], 5)]:
        spark.sql(f"INSERT INTO {_LB} VALUES ('{_t}', 'Event 5: Capstone', '{cat}', {pts}, {mx}, '{_now}')")
print("Event 5 scored and leaderboard updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # FINAL SCOREBOARD
# MAGIC ---

# COMMAND ----------

print("=" * 70)
print("  DATAOPS OLYMPICS — FINAL STANDINGS")
print("=" * 70)

final = spark.sql(f"""
    WITH best AS (
        SELECT team, event, category, MAX(points) AS points
        FROM {_LB} GROUP BY team, event, category
    )
    SELECT team, ROUND(SUM(points), 1) AS total, COUNT(DISTINCT event) AS events
    FROM best GROUP BY team ORDER BY total DESC
""").toPandas()

for i, row in final.iterrows():
    medal = {0: "GOLD", 1: "SILVER", 2: "BRONZE"}.get(i, "")
    print(f"  {i+1}. [{medal:6s}] {row['team']:12s} | {row['total']:6.1f} pts | {int(row['events'])} events")

print("=" * 70)
print("\nPer-event breakdown:")
display(spark.sql(f"""
    WITH best AS (
        SELECT team, event, category, MAX(points) AS pts, MAX(max_points) AS mx
        FROM {_LB} GROUP BY team, event, category
    )
    SELECT team, event, ROUND(SUM(pts), 1) AS points, ROUND(SUM(mx), 1) AS max_pts
    FROM best GROUP BY team, event ORDER BY team, event
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh AI/BI Dashboard

# COMMAND ----------

dbutils.notebook.run(
    "/Workspace/Users/sean.zhang@databricks.com/gsk-dataops-olympics/scoring/scoreboard",
    timeout_seconds=600
)
print("Dashboard refreshed!")
