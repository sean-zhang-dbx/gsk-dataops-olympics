# Databricks notebook source
# MAGIC %md
# MAGIC # Event 4: Agent Bricks — Answer Key
# MAGIC
# MAGIC **Reference implementation for the Clinical AI Supervisor.**
# MAGIC
# MAGIC This notebook creates all Agent Bricks resources programmatically:
# MAGIC 1. Clinical documents in a shared Volume (for KA)
# MAGIC 2. UC Function tools (patient risk assessment + cohort summary)
# MAGIC 3. Genie Space on patient heart data
# MAGIC 4. Knowledge Assistant on clinical documents
# MAGIC 5. Supervisor Agent wiring everything together
# MAGIC 6. Test prompts with saved results
# MAGIC
# MAGIC **Run this notebook end-to-end** to set up the answer key, then run `scoring.py` to verify.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TEAM_NAME = "answer_key"
CATALOG = TEAM_NAME
SHARED_CATALOG = "dataops_olympics"
RAW_DATA_PATH = f"/Volumes/{SHARED_CATALOG}/default/raw_data/heart_events/"
CLINICAL_DOCS_PATH = f"/Volumes/{SHARED_CATALOG}/default/raw_data/clinical_docs"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.default")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")
print(f"Catalog: {CATALOG} | Shared: {SHARED_CATALOG}")

# COMMAND ----------

import requests, time, json

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def api_get(path):
    return requests.get(f"https://{host}{path}", headers=headers)

def api_post(path, body):
    return requests.post(f"https://{host}{path}", headers=headers, json=body)

def api_patch(path, body):
    return requests.patch(f"https://{host}{path}", headers=headers, json=body)

def api_delete(path):
    return requests.delete(f"https://{host}{path}", headers=headers)

print(f"Host: {host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Prepare Heart Data (if not already present)
# MAGIC
# MAGIC Teams have `heart_silver` and `heart_gold` from Events 1–3.
# MAGIC For the answer key, we create them from the raw data.

# COMMAND ----------

try:
    silver_count = spark.table(f"{CATALOG}.default.heart_silver").count()
    print(f"heart_silver already exists ({silver_count} rows)")
except Exception:
    print("Creating heart_silver from raw data...")
    raw = spark.read.format("json").load(RAW_DATA_PATH)
    keep_cols = ["age", "sex", "cp", "trestbps", "chol", "fbs", "restecg",
                 "thalach", "exang", "oldpeak", "slope", "ca", "thal", "target"]
    existing = [c for c in keep_cols if c in raw.columns]
    raw = raw.select(*existing).dropDuplicates().na.drop()
    raw.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_silver")
    print(f"  Created heart_silver: {raw.count()} rows")

try:
    gold_count = spark.table(f"{CATALOG}.default.heart_gold").count()
    print(f"heart_gold already exists ({gold_count} rows)")
except Exception:
    print("Creating heart_gold from heart_silver...")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.default.heart_gold AS
        SELECT
            CASE
                WHEN age < 40 THEN '30-39'
                WHEN age < 50 THEN '40-49'
                WHEN age < 60 THEN '50-59'
                WHEN age < 70 THEN '60-69'
                ELSE '70+'
            END AS age_group,
            CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
            COUNT(*) AS patient_count,
            ROUND(AVG(chol), 1) AS avg_cholesterol,
            ROUND(AVG(trestbps), 1) AS avg_resting_bp,
            ROUND(AVG(thalach), 1) AS avg_max_hr,
            ROUND(AVG(oldpeak), 2) AS avg_oldpeak,
            ROUND(AVG(CAST(fbs AS DOUBLE)) * 100, 1) AS pct_high_fasting_sugar
        FROM {CATALOG}.default.heart_silver
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
    cnt = spark.table(f"{CATALOG}.default.heart_gold").count()
    print(f"  Created heart_gold: {cnt} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Generate Clinical Documents
# MAGIC
# MAGIC Create 10 clinical text files in the shared Volume for the Knowledge Assistant.

# COMMAND ----------

import os

os.makedirs(CLINICAL_DOCS_PATH, exist_ok=True)

clinical_documents = {
    "heart_disease_protocol.txt": """HEART DISEASE SCREENING AND MANAGEMENT PROTOCOL
Version 3.2 — Effective January 2025

1. SCREENING CRITERIA
All patients aged 40+ presenting with ANY of:
- Chest pain (any type), resting BP > 130 mmHg, cholesterol > 200 mg/dL,
  fasting blood sugar > 120 mg/dL, or family history of cardiovascular disease.

2. RISK CLASSIFICATION
HIGH RISK: Age > 60 AND (cholesterol > 250 OR resting BP > 150 OR max HR during stress > 150)
MEDIUM RISK: Age 45-60 AND any 2 abnormal markers
LOW RISK: Age < 45 AND fewer than 2 abnormal markers

3. DIAGNOSTIC PATHWAY
a) Initial triage: Resting ECG + blood panel (fasting glucose, lipid profile)
b) If HIGH risk → immediate cardiology consult + stress echocardiogram
c) If MEDIUM risk → scheduled stress test within 2 weeks
d) If LOW risk → lifestyle counseling + 6-month follow-up

4. TREATMENT GUIDELINES
- Statins for cholesterol > 240 mg/dL (target < 200)
- ACE inhibitors for BP > 140/90 (target < 130/80)
- Beta-blockers for tachycardia (resting HR > 100)
- Aspirin therapy for confirmed CAD patients
- Cardiac rehabilitation for all post-event patients

5. FOLLOW-UP SCHEDULE
HIGH risk: Monthly for 3 months, then quarterly
MEDIUM risk: Quarterly for 1 year
LOW risk: Annual screening
""",
    "patient_intake_policy.txt": """PATIENT INTAKE AND DATA COLLECTION POLICY
Department of Cardiology — DataOps General Hospital

1. REQUIRED DATA POINTS (ALL patients)
- Demographics: age, sex (0=female, 1=male)
- Vitals: resting blood pressure (trestbps), max heart rate achieved (thalach)
- Lab results: serum cholesterol (chol), fasting blood sugar (fbs: 1 if > 120 mg/dL)
- ECG: resting ECG results (restecg: 0=normal, 1=ST-T abnormality, 2=LVH)
- Symptoms: chest pain type (cp: 0=typical angina, 1=atypical, 2=non-anginal, 3=asymptomatic)

2. STRESS TEST DATA (when ordered)
- Exercise-induced angina (exang: 0=no, 1=yes)
- ST depression induced by exercise (oldpeak: numeric)
- Slope of peak exercise ST segment (slope: 0=upsloping, 1=flat, 2=downsloping)

3. IMAGING DATA (when ordered)
- Number of major vessels colored by fluoroscopy (ca: 0-3)
- Thalassemia status (thal: 1=normal, 2=fixed defect, 3=reversible defect)

4. DATA QUALITY REQUIREMENTS
- All numeric values must be within physiologically valid ranges
- Missing values must be documented with reason codes
- Duplicate records must be flagged and reconciled within 24 hours
- Data entry must be completed within 4 hours of patient encounter

5. CONSENT AND PRIVACY
- Written consent required before data collection
- All PHI encrypted at rest and in transit
- Data retention: 7 years minimum per regulatory requirements
""",
    "data_quality_standards.txt": """DATA QUALITY STANDARDS FOR CLINICAL MEASUREMENTS
Quality Assurance Department — DataOps General Hospital

1. VALID RANGES FOR CLINICAL MEASUREMENTS
| Measurement      | Min  | Max  | Unit    |
|------------------|------|------|---------|
| Age              | 18   | 120  | years   |
| Resting BP       | 60   | 220  | mmHg    |
| Cholesterol      | 100  | 600  | mg/dL   |
| Max Heart Rate   | 60   | 220  | bpm     |
| ST Depression    | 0.0  | 7.0  | mm      |
| Fasting BS       | 0    | 1    | boolean |

2. OUTLIER DETECTION RULES
- Z-score > 3.0 from population mean → flag for review
- Cholesterol > 400 → automatic physician notification
- Resting BP > 200 OR < 70 → immediate clinical alert
- Max HR > 200 → verify equipment calibration

3. COMPLETENESS REQUIREMENTS
- Bronze layer: >= 95% field completeness
- Silver layer: 100% for required fields after cleansing
- Gold layer: Aggregations must cover all demographic groups

4. TIMELINESS
- Lab results: loaded within 2 hours of completion
- Vital signs: real-time or within 15 minutes
- Imaging results: within 24 hours

5. AUDIT TRAIL
- All data modifications logged with timestamp, user, and reason
- Monthly data quality reports submitted to Chief Data Officer
- Quarterly audits by external data quality team
""",
    "cardiology_guidelines.txt": """CARDIOLOGY CLINICAL PRACTICE GUIDELINES
Based on ACC/AHA 2024 Recommendations — Adapted for DataOps General Hospital

1. CHEST PAIN CLASSIFICATION
Type 0 - Typical Angina: Substernal chest discomfort with characteristic quality and
  duration, provoked by exertion or emotional stress, relieved by rest or nitroglycerin.
Type 1 - Atypical Angina: Meets 2 of 3 criteria for typical angina.
Type 2 - Non-Anginal Pain: Meets 1 or fewer criteria.
Type 3 - Asymptomatic: No chest pain symptoms (often incidental finding).

2. HEART DISEASE RISK FACTORS (Modifiable)
- Hypertension (resting BP > 140/90)
- Hyperlipidemia (total cholesterol > 240, LDL > 160)
- Diabetes (fasting blood sugar > 126 mg/dL repeatedly)
- Smoking, obesity (BMI > 30), sedentary lifestyle
- Excessive alcohol consumption

3. HEART DISEASE RISK FACTORS (Non-Modifiable)
- Age (men > 45, women > 55)
- Sex (men at higher risk before age 55)
- Family history of premature CVD

4. ECG INTERPRETATION GUIDE
- Normal (restecg=0): No significant abnormalities
- ST-T Abnormality (restecg=1): Possible ischemia, recommend stress test
- Left Ventricular Hypertrophy (restecg=2): Evaluate for hypertension, aortic stenosis

5. STRESS TEST INTERPRETATION
- Exercise-induced angina (exang=1): Suggests significant coronary obstruction
- ST depression (oldpeak): Higher values indicate more severe ischemia
  - < 1.0 mm: Equivocal
  - 1.0 - 2.0 mm: Mildly positive
  - > 2.0 mm: Strongly positive
- Slope interpretation:
  - Upsloping (0): Generally benign
  - Flat (1): Concerning, suggests ischemia
  - Downsloping (2): Most concerning, high-risk finding
""",
    "emergency_cardiac_procedures.txt": """EMERGENCY CARDIAC CARE PROCEDURES
Emergency Department Protocol — DataOps General Hospital

1. STEMI (ST-Elevation Myocardial Infarction) PROTOCOL
- Door-to-balloon time target: < 90 minutes
- Immediate: 12-lead ECG within 10 minutes of arrival
- Administer: Aspirin 325mg, Heparin bolus, Nitroglycerin sublingual
- Activate cardiac catheterization lab if STEMI confirmed
- Monitor: Continuous telemetry, pulse oximetry, repeat ECG at 15 min

2. NSTEMI / UNSTABLE ANGINA PROTOCOL
- Risk stratification using TIMI or GRACE score
- High risk: Early invasive strategy (cardiac cath within 24 hours)
- Low risk: Conservative management with serial troponins
- Anticoagulation: Enoxaparin or Heparin infusion

3. CARDIAC ARREST (ACLS)
- Begin CPR immediately, apply defibrillator
- Shockable rhythm (VF/VT): Defibrillate, Epinephrine q3-5min, Amiodarone
- Non-shockable (PEA/Asystole): Epinephrine q3-5min, identify reversible causes
- Post-ROSC: Targeted temperature management, cardiac catheterization if indicated

4. TRIAGE CRITERIA FOR CARDIAC PATIENTS
- Level 1 (Immediate): Active STEMI, cardiac arrest, cardiogenic shock
- Level 2 (Emergent): Chest pain with ECG changes, unstable vital signs
- Level 3 (Urgent): Chest pain without ECG changes, stable hemodynamics
- Level 4 (Less Urgent): Chronic chest pain, follow-up concerns

5. HANDOFF DOCUMENTATION
All cardiac emergencies require structured handoff using SBAR format:
- Situation, Background, Assessment, Recommendation
""",
    "medication_formulary.txt": """CARDIOVASCULAR MEDICATION FORMULARY
Pharmacy Department — DataOps General Hospital

1. ANTIHYPERTENSIVES
| Drug Class       | Example          | Starting Dose  | Target        |
|------------------|------------------|----------------|---------------|
| ACE Inhibitor    | Lisinopril       | 10mg daily     | BP < 130/80   |
| ARB              | Losartan         | 50mg daily     | BP < 130/80   |
| Beta-Blocker     | Metoprolol       | 25mg BID       | HR 55-65 bpm  |
| CCB              | Amlodipine       | 5mg daily      | BP < 130/80   |
| Thiazide         | HCTZ             | 12.5mg daily   | BP < 130/80   |

2. LIPID-LOWERING AGENTS
| Drug             | Starting Dose    | LDL Target     | Monitoring    |
|------------------|------------------|----------------|---------------|
| Atorvastatin     | 20mg daily       | < 100 mg/dL    | LFTs q6mo     |
| Rosuvastatin     | 10mg daily       | < 70 mg/dL*    | LFTs q6mo     |
| Ezetimibe        | 10mg daily       | Adjunct        | Lipid panel   |
*For high-risk patients

3. ANTIPLATELET / ANTICOAGULANT
- Aspirin 81mg daily for secondary prevention
- Clopidogrel 75mg daily if aspirin-intolerant
- Warfarin with INR target 2.0-3.0 for AF patients
- DOACs (Rivaroxaban, Apixaban) preferred over Warfarin for new AF

4. CONTRAINDICATIONS AND INTERACTIONS
- ACE Inhibitors: Contraindicated in bilateral renal artery stenosis
- Beta-Blockers: Caution in severe asthma, decompensated heart failure
- Statins + Grapefruit: Increased toxicity risk
- Warfarin: Extensive drug-food interactions, monitor INR closely

5. PRESCRIBING RULES
- All cardiovascular medications require attending physician approval
- Generic substitution permitted unless clinically inappropriate
- 90-day supply for stable chronic conditions
""",
    "quality_improvement_program.txt": """CARDIAC CARE QUALITY IMPROVEMENT PROGRAM
Office of Quality and Patient Safety — DataOps General Hospital

1. KEY PERFORMANCE INDICATORS (KPIs)
- 30-day readmission rate for heart failure: Target < 15%
- Door-to-balloon time for STEMI: Target < 90 minutes
- Appropriate statin prescribing at discharge: Target > 95%
- Blood pressure control rate (< 140/90): Target > 75%
- Patient satisfaction score (HCAHPS cardiac): Target > 85th percentile

2. DATA COLLECTION AND REPORTING
- Monthly dashboards generated from the analytics pipeline
- Quarterly physician report cards with peer benchmarking
- Annual outcomes report submitted to CMS and ACC NCDR

3. ROOT CAUSE ANALYSIS TRIGGERS
- Any cardiac-related mortality within 48 hours of admission
- Door-to-balloon time exceeding 120 minutes
- Medication error involving anticoagulants
- Wrong-site procedure or retained surgical item

4. IMPROVEMENT INITIATIVES (Current Year)
a) AI-Assisted Risk Stratification: Deploy ML model to predict 30-day readmission
b) Clinical Decision Support: Integrate risk scores into EHR order entry
c) Patient Education: Standardized discharge instructions with teach-back
d) Telemedicine Follow-up: Virtual visits at 48-hour and 2-week post-discharge

5. BENCHMARKING
- Compare against ACC NCDR national benchmarks
- Participate in AHA Get With The Guidelines program
- Internal benchmarking across hospital campuses
""",
    "research_ethics_policy.txt": """RESEARCH ETHICS AND DATA USE POLICY
Institutional Review Board — DataOps General Hospital

1. DATA ACCESS FOR RESEARCH
- All research using patient data requires IRB approval
- De-identified datasets can be used for quality improvement without full IRB review
- The heart disease dataset used in analytics is fully de-identified per HIPAA Safe Harbor

2. MACHINE LEARNING MODEL GOVERNANCE
- All ML models affecting clinical decisions require:
  a) Bias assessment across age, sex, and demographic groups
  b) Clinical validation with prospective cohort
  c) Ongoing monitoring for model drift
  d) Human-in-the-loop for high-stakes predictions
- Model performance must be reported separately for each subgroup

3. AI IN CLINICAL DECISION SUPPORT
- AI recommendations are advisory only; clinicians make final decisions
- AI-generated risk scores must be explainable to patients
- Automated alerts from AI models require clinical validation threshold > 0.85 AUC

4. DATA SHARING AGREEMENTS
- External collaborators: BAA required, encrypted transfer only
- Multi-site studies: Federated learning preferred over data centralization
- Public datasets: Only fully de-identified, IRB-approved extracts

5. RESPONSIBLE AI PRINCIPLES
- Transparency: Document all model inputs, outputs, and limitations
- Fairness: Regular bias audits, equitable performance across populations
- Accountability: Named clinical champion for each deployed model
- Privacy: Minimum necessary data principle, differential privacy where feasible
""",
    "telehealth_cardiology_protocol.txt": """TELEHEALTH CARDIOLOGY PROTOCOL
Digital Health Department — DataOps General Hospital

1. ELIGIBLE ENCOUNTERS
- Follow-up visits for stable chronic conditions
- Post-discharge check-ins (48-hour and 2-week)
- Medication management and titration
- Remote monitoring review (implantable devices, wearables)

2. NOT ELIGIBLE (Require In-Person)
- Initial evaluation with chest pain
- Stress testing or cardiac imaging
- Invasive procedures or surgical consultations
- Hemodynamically unstable patients

3. REMOTE MONITORING DATA INTEGRATION
- Apple Watch / Fitbit: Heart rate, irregular rhythm notifications
- Blood pressure cuffs: Daily readings uploaded to patient portal
- Implantable devices: Continuous monitoring via vendor platforms
- Integration with EHR: Automated data feeds to clinical dashboard

4. CLINICAL WORKFLOW
a) Pre-visit: Nurse reviews remote monitoring data, prepares summary
b) Visit: Video consultation with cardiologist (15-30 minutes)
c) Post-visit: Updated care plan, prescriptions via e-prescribe
d) Documentation: Structured note in EHR within 24 hours

5. QUALITY METRICS
- Patient no-show rate: Target < 10% (vs 20% for in-person)
- Time to follow-up after discharge: Target < 48 hours
- Patient satisfaction with telehealth: Target > 4.5/5.0
- Clinical outcomes: Non-inferior to in-person for stable patients
""",
    "infection_control_cardiac_unit.txt": """INFECTION CONTROL PROTOCOLS — CARDIAC CARE UNIT
Infection Prevention Department — DataOps General Hospital

1. CATHETER-ASSOCIATED INFECTIONS
- Central line bundle compliance target: 100%
- Bundle elements: Hand hygiene, maximal barrier precautions, chlorhexidine
  skin prep, optimal catheter site selection, daily review of line necessity
- Target: Zero CLABSI (Central Line-Associated Bloodstream Infections)

2. SURGICAL SITE INFECTIONS (Cardiac Surgery)
- Preoperative: Nasal decolonization, chlorhexidine bathing
- Intraoperative: Antibiotic prophylaxis within 60 minutes of incision
- Postoperative: Sterile dressing for 48 hours, daily wound assessment
- Target SSI rate: < 1% for CABG procedures

3. HAND HYGIENE
- Compliance monitoring via electronic dispensers and direct observation
- Target compliance: > 95% for all cardiac unit staff
- Before patient contact, after patient contact, before aseptic procedures

4. ISOLATION PRECAUTIONS
- Contact precautions for MRSA/VRE colonized patients
- Droplet precautions for respiratory infections
- Enhanced precautions for immunocompromised cardiac transplant patients

5. REPORTING
- Real-time dashboard for hand hygiene compliance
- Monthly infection rate reports to Infection Control Committee
- Immediate notification for any healthcare-associated infection cluster
""",
}

written = 0
for filename, content in clinical_documents.items():
    filepath = f"{CLINICAL_DOCS_PATH}/{filename}"
    try:
        with open(filepath, "w") as f:
            f.write(content.strip())
        written += 1
    except Exception as e:
        print(f"  Error writing {filename}: {e}")

print(f"Generated {written}/{len(clinical_documents)} clinical documents in {CLINICAL_DOCS_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create UC Function Tools (10 pts)
# MAGIC
# MAGIC Two Unity Catalog functions that the Supervisor Agent will use as tools.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION patient_risk_assessment(
# MAGIC   age INT, chol DOUBLE, bp DOUBLE, hr DOUBLE
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Classifies cardiovascular risk as LOW, MEDIUM, or HIGH using AI analysis of patient vitals.'
# MAGIC RETURN
# MAGIC   SELECT ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     CONCAT(
# MAGIC       'You are a cardiovascular risk classifier. Based on these patient metrics, classify the risk as exactly one of: LOW, MEDIUM, or HIGH. ',
# MAGIC       'Then provide a brief 1-2 sentence explanation. ',
# MAGIC       'Patient: Age=', CAST(age AS STRING),
# MAGIC       ', Cholesterol=', CAST(chol AS STRING), ' mg/dL',
# MAGIC       ', Resting BP=', CAST(bp AS STRING), ' mmHg',
# MAGIC       ', Max Heart Rate=', CAST(hr AS STRING), ' bpm. ',
# MAGIC       'Risk classification rules: HIGH if age>60 AND (chol>250 OR bp>150), ',
# MAGIC       'MEDIUM if age 45-60 AND any 2 abnormal markers (chol>200, bp>130, hr>100), ',
# MAGIC       'LOW otherwise. ',
# MAGIC       'Respond with format: RISK_LEVEL: explanation'
# MAGIC     )
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_cohort_summary(
# MAGIC   target_age_group STRING
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Returns a formatted summary of patient cohort statistics for a given age group from the Gold table.'
# MAGIC RETURN
# MAGIC   SELECT CONCAT(
# MAGIC     'Age Group: ', age_group,
# MAGIC     ' | Diagnosis: ', diagnosis,
# MAGIC     ' | Patients: ', CAST(patient_count AS STRING),
# MAGIC     ' | Avg Cholesterol: ', CAST(avg_cholesterol AS STRING), ' mg/dL',
# MAGIC     ' | Avg BP: ', CAST(avg_resting_bp AS STRING), ' mmHg',
# MAGIC     ' | Avg Max HR: ', CAST(avg_max_hr AS STRING), ' bpm'
# MAGIC   )
# MAGIC   FROM heart_gold
# MAGIC   WHERE age_group = target_age_group
# MAGIC   LIMIT 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify UC Functions

# COMMAND ----------

print("Testing patient_risk_assessment...")
try:
    r = spark.sql(f"SELECT {CATALOG}.default.patient_risk_assessment(62, 280, 155, 140) AS risk").collect()
    print(f"  Result: {r[0]['risk']}")
except Exception as e:
    print(f"  Error: {e}")

print("\nTesting get_cohort_summary...")
try:
    r = spark.sql(f"SELECT {CATALOG}.default.get_cohort_summary('50-59') AS summary").collect()
    print(f"  Result: {r[0]['summary']}")
except Exception as e:
    print(f"  Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Genie Space (8 pts)
# MAGIC
# MAGIC Create a Genie Space on the team's heart data tables.

# COMMAND ----------

# Find a SQL warehouse to use
wh_resp = api_get("/api/2.0/sql/warehouses")
warehouses = wh_resp.json().get("warehouses", [])
wh_id = None
for w in warehouses:
    if w.get("state") in ("RUNNING", "STOPPED") and w.get("warehouse_type") in ("PRO", "CLASSIC"):
        wh_id = w["id"]
        break
if not wh_id and warehouses:
    wh_id = warehouses[0]["id"]
print(f"Using warehouse: {wh_id}")

# COMMAND ----------

import uuid

serialized_space = json.dumps({
    "version": 2,
    "config": {
        "sample_questions": [
            {"id": uuid.uuid4().hex, "question": ["How many patients have heart disease?"]},
            {"id": uuid.uuid4().hex, "question": ["What is the average cholesterol by age group?"]},
            {"id": uuid.uuid4().hex, "question": ["Which chest pain type is most common among heart disease patients?"]},
        ]
    },
    "data_sources": {
        "tables": sorted([
            {"identifier": f"{CATALOG}.default.heart_silver",
             "description": ["Patient-level clinical records with age, sex, chest pain type, blood pressure, cholesterol, heart rate, and heart disease diagnosis"]},
            {"identifier": f"{CATALOG}.default.heart_gold",
             "description": ["Aggregated cohort metrics by age group and diagnosis"]},
        ], key=lambda t: t["identifier"])
    },
    "instructions": {
        "text_instructions": [{
            "id": uuid.uuid4().hex,
            "content": [
                "You are a clinical data analyst. The heart_silver table has patient-level records with: "
                "age, sex (0=female, 1=male), cp (chest pain: 0=typical angina, 1=atypical, 2=non-anginal, 3=asymptomatic), "
                "trestbps (resting BP), chol (cholesterol mg/dL), fbs (fasting blood sugar >120: 1=true), "
                "restecg (0=normal, 1=ST-T abnormality, 2=LVH), thalach (max heart rate), "
                "exang (exercise angina: 1=yes), oldpeak (ST depression), slope, ca, thal, "
                "target (1=heart disease, 0=healthy). "
                "heart_gold has aggregated stats by age_group and diagnosis. "
                "Always include row counts. Round to 1 decimal place."
            ]
        }]
    },
})

genie_payload = {
    "title": f"{TEAM_NAME}_clinical_analytics",
    "description": (
        "Natural language analytics on patient heart disease data. "
        "Contains patient-level clinical records and aggregated cohort metrics."
    ),
    "serialized_space": serialized_space,
    "warehouse_id": wh_id,
}

resp = api_post("/api/2.0/genie/spaces", genie_payload)
if resp.status_code == 200:
    genie_data = resp.json()
    GENIE_SPACE_ID = genie_data.get("space_id", genie_data.get("id", ""))
    print(f"Genie Space created: {GENIE_SPACE_ID}")
else:
    print(f"Genie creation returned {resp.status_code}: {resp.text[:500]}")
    GENIE_SPACE_ID = ""
    print("Set GENIE_SPACE_ID manually if created via UI")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Knowledge Assistant (7 pts)
# MAGIC
# MAGIC > **Note:** The KA creation API is an internal Databricks API.
# MAGIC > If the programmatic approach fails, create via UI:
# MAGIC > 1. Go to **Agents** → **Knowledge Assistant** → **Build**
# MAGIC > 2. Name: `{TEAM_NAME}_clinical_docs`
# MAGIC > 3. Source: `/Volumes/dataops_olympics/default/raw_data/clinical_docs`
# MAGIC > 4. Then paste the tile ID below.

# COMMAND ----------

KA_TILE_ID = ""

# The KA API is in Beta and may not be available on all workspaces.
# Try the documented API, otherwise fall back to UI creation.
try:
    ka_create_payload = {
        "name": f"{TEAM_NAME}_clinical_docs",
        "description": (
            "Answers questions about hospital clinical protocols, patient intake "
            "procedures, data quality standards, and cardiology guidelines."
        ),
        "instructions": (
            "Always cite the specific document when answering. If you are unsure, "
            "say so. Do not provide medical advice."
        ),
    }
    resp = api_post("/api/2.0/knowledge-assistants", ka_create_payload)
    if resp.status_code == 200:
        data = resp.json()
        KA_TILE_ID = data.get("tile_id", data.get("id", data.get("knowledge_assistant_id", "")))
        print(f"KA created via API: {KA_TILE_ID}")
        # Add knowledge source
        if KA_TILE_ID:
            ks_payload = {
                "name": "clinical_documents",
                "type": "UC_VOLUME",
                "volume_path": CLINICAL_DOCS_PATH,
                "description": "Clinical protocols and guidelines from DataOps General Hospital",
            }
            api_post(f"/api/2.0/knowledge-assistants/{KA_TILE_ID}/knowledge-sources", ks_payload)
    else:
        print(f"KA API returned {resp.status_code} — create via UI instead")
except Exception as e:
    print(f"KA API error: {e}")

if not KA_TILE_ID:
    print(f"\n--- MANUAL STEP REQUIRED ---")
    print(f"1. Go to Agents → Knowledge Assistant → Build")
    print(f"2. Name: {TEAM_NAME}_clinical_docs")
    print(f"3. Source: {CLINICAL_DOCS_PATH}")
    print(f"4. Paste the tile ID in the next cell")

# COMMAND ----------

# If created via UI, paste the tile ID here:
# KA_TILE_ID = "paste-tile-id-here"

if KA_TILE_ID:
    print(f"KA Tile ID: {KA_TILE_ID}")
    ka_endpoint = f"agents-{KA_TILE_ID}"
    print(f"KA Endpoint: {ka_endpoint}")
else:
    ka_endpoint = ""
    print("KA not configured — Supervisor will be created without it")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Build the Supervisor Agent (10 pts)
# MAGIC
# MAGIC Wire Genie + KA + UC Function into a Supervisor Agent.
# MAGIC
# MAGIC > **Note:** Like the KA, the Supervisor creation API is internal.
# MAGIC > Fallback: create via UI at **Agents** → **Supervisor Agent** → **Build**.

# COMMAND ----------

MAS_TILE_ID = ""

agents_config = []

if GENIE_SPACE_ID:
    agents_config.append({
        "name": "clinical_analytics",
        "genie_space_id": GENIE_SPACE_ID,
        "description": (
            "SQL analytics on patient heart data — heart disease rates, cholesterol stats, "
            "demographic breakdowns, and cohort comparisons. Use for any data/analytics question."
        ),
    })

if KA_TILE_ID:
    agents_config.append({
        "name": "clinical_docs",
        "ka_tile_id": KA_TILE_ID,
        "description": (
            "Answers questions about clinical protocols, intake procedures, data quality "
            "standards, medication formulary, and cardiology guidelines from hospital documents."
        ),
    })

agents_config.append({
    "name": "risk_assessment",
    "uc_function_name": f"{CATALOG}.default.patient_risk_assessment",
    "description": (
        "Classifies cardiovascular risk as LOW, MEDIUM, or HIGH given patient age, "
        "cholesterol, blood pressure, and heart rate. Use when asked to assess or classify risk."
    ),
})

routing_instructions = f"""Route queries as follows:
- Data/analytics questions (counts, averages, breakdowns, comparisons) → clinical_analytics (Genie)
- Policy/procedure/protocol/guidelines questions → clinical_docs (Knowledge Assistant)
- Risk assessment requests (classify risk, assess patient, evaluate vitals) → risk_assessment (UC Function)

If a query needs multiple agents, chain them:
1. First gather data (clinical_analytics)
2. Then provide context (clinical_docs) or assess risk (risk_assessment)

Always be factual and cite the data source. Never provide direct medical advice.
When unsure which agent to use, prefer clinical_analytics for quantitative questions
and clinical_docs for qualitative or policy questions."""

mas_payload = {
    "name": f"{TEAM_NAME}_clinical_supervisor",
    "description": (
        "A multi-agent supervisor for clinical operations that coordinates "
        "patient data analytics, document Q&A, and AI-powered risk assessment."
    ),
    "agents": agents_config,
    "instructions": routing_instructions,
}

print(f"\n--- MANUAL STEP REQUIRED ---")
print(f"Create Supervisor Agent via UI:")
print(f"1. Go to Agents → Supervisor Agent → Build")
print(f"2. Name: {TEAM_NAME}_clinical_supervisor")
print(f"3. Add agents: {[a['name'] for a in agents_config]}")
print(f"4. Add routing instructions (copy from variable above)")
print(f"5. Paste the tile ID in the next cell")

# COMMAND ----------

# If created via UI, paste the tile ID here:
# MAS_TILE_ID = "paste-tile-id-here"

if MAS_TILE_ID:
    print(f"Supervisor Tile ID: {MAS_TILE_ID}")
    mas_endpoint = f"agents-{MAS_TILE_ID}"
    print(f"Supervisor Endpoint: {mas_endpoint}")
else:
    mas_endpoint = ""
    print("Supervisor not configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Wait for Provisioning
# MAGIC
# MAGIC KA and Supervisor endpoints can take 2–10 minutes to become ONLINE.

# COMMAND ----------

def wait_for_endpoint(endpoint_name, timeout_minutes=15):
    """Poll a serving endpoint until it's READY or timeout."""
    if not endpoint_name:
        return "NOT_CONFIGURED"
    print(f"Waiting for {endpoint_name}...", end="")
    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        try:
            r = api_get(f"/api/2.0/serving-endpoints/{endpoint_name}")
            if r.status_code == 200:
                state = r.json().get("state", {})
                ready = state.get("ready", "NOT_READY")
                config_update = state.get("config_update", "NOT_UPDATING")
                if ready == "READY":
                    print(f" ONLINE")
                    return "ONLINE"
                print(f".", end="")
            else:
                print(f"?", end="")
        except Exception:
            print(f"!", end="")
        time.sleep(15)
    print(f" TIMEOUT after {timeout_minutes} min")
    return "TIMEOUT"

if ka_endpoint:
    ka_status = wait_for_endpoint(ka_endpoint)
else:
    ka_status = "NOT_CONFIGURED"

if mas_endpoint:
    mas_status = wait_for_endpoint(mas_endpoint)
else:
    mas_status = "NOT_CONFIGURED"

print(f"\nKA: {ka_status} | Supervisor: {mas_status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Test Your Supervisor (5 pts)
# MAGIC
# MAGIC Send 5 test prompts and save results.

# COMMAND ----------

test_prompts = [
    "How many patients in the dataset have heart disease?",
    "What does the patient intake policy say about data validation?",
    "Assess the cardiovascular risk for a 62-year-old with cholesterol 280, BP 155, and heart rate 140.",
    "Which age group has the highest heart disease rate?",
    "What are the data quality standards for clinical measurements?",
]

results = []
for i, prompt in enumerate(test_prompts, 1):
    print(f"\n{'='*60}")
    print(f"Test {i}: {prompt}")
    print(f"{'='*60}")
    if not mas_endpoint:
        print("  [SKIP] No Supervisor endpoint")
        results.append({"prompt_id": i, "question": prompt, "answer": "NOT_CONFIGURED", "status": "skipped"})
        continue
    try:
        resp = requests.post(
            f"https://{host}/serving-endpoints/{mas_endpoint}/invocations",
            headers=headers,
            json={"messages": [{"role": "user", "content": prompt}]},
            timeout=120,
        )
        if resp.status_code == 200:
            data = resp.json()
            answer = data.get("choices", [{}])[0].get("message", {}).get("content", str(data))
            print(f"  Answer: {answer[:500]}")
            results.append({"prompt_id": i, "question": prompt, "answer": answer[:2000], "status": "success"})
        else:
            print(f"  HTTP {resp.status_code}: {resp.text[:300]}")
            results.append({"prompt_id": i, "question": prompt, "answer": f"HTTP_{resp.status_code}", "status": "error"})
    except Exception as e:
        print(f"  Error: {e}")
        results.append({"prompt_id": i, "question": prompt, "answer": str(e)[:500], "status": "error"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Save Results

# COMMAND ----------

import pandas as pd
from datetime import datetime

if results:
    df = pd.DataFrame(results)
    df["team"] = TEAM_NAME
    df["tested_at"] = datetime.now().isoformat()
    spark.createDataFrame(df).write.format("delta").mode("overwrite").saveAsTable(
        f"{CATALOG}.default.supervisor_test_results"
    )
    print(f"Saved {len(results)} test results to {CATALOG}.default.supervisor_test_results")
    display(spark.table(f"{CATALOG}.default.supervisor_test_results"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Save Agent Config (for Scoring)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.default.agent_config
    (resource_type STRING, resource_id STRING, resource_name STRING, created_at TIMESTAMP)
""")

configs = [
    ("genie_space", GENIE_SPACE_ID, f"{TEAM_NAME}_clinical_analytics"),
    ("knowledge_assistant", KA_TILE_ID, f"{TEAM_NAME}_clinical_docs"),
    ("supervisor_agent", MAS_TILE_ID, f"{TEAM_NAME}_clinical_supervisor"),
]

for rtype, rid, rname in configs:
    if rid:
        spark.sql(f"""
            MERGE INTO {CATALOG}.default.agent_config AS t
            USING (SELECT '{rtype}' AS resource_type, '{rid}' AS resource_id,
                   '{rname}' AS resource_name, current_timestamp() AS created_at) AS s
            ON t.resource_type = s.resource_type
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"  {rtype}: {rid}")
    else:
        print(f"  {rtype}: NOT SET")

display(spark.table(f"{CATALOG}.default.agent_config"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: DSPy Prompt Optimization (+3 pts)
# MAGIC
# MAGIC Use DSPy to optimize the risk classification prompt instead of hand-tuning.

# COMMAND ----------

# MAGIC %pip install dspy -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

TEAM_NAME = "answer_key"
CATALOG = TEAM_NAME
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")

import dspy

lm = dspy.LM("databricks/databricks-meta-llama-3-3-70b-instruct")
dspy.configure(lm=lm)


class RiskClassification(dspy.Signature):
    """Classify cardiovascular risk based on patient vitals."""
    age: int = dspy.InputField(desc="Patient age in years")
    cholesterol: float = dspy.InputField(desc="Serum cholesterol in mg/dL")
    blood_pressure: float = dspy.InputField(desc="Resting blood pressure in mmHg")
    heart_rate: float = dspy.InputField(desc="Max heart rate achieved in bpm")
    risk_level: str = dspy.OutputField(desc="Exactly one of: LOW, MEDIUM, or HIGH")
    explanation: str = dspy.OutputField(desc="One sentence clinical explanation")


classify_risk = dspy.ChainOfThought(RiskClassification)

trainset = [
    dspy.Example(age=35, cholesterol=180, blood_pressure=120, heart_rate=160,
                 risk_level="LOW", explanation="Young patient with normal vitals.").with_inputs("age", "cholesterol", "blood_pressure", "heart_rate"),
    dspy.Example(age=52, cholesterol=230, blood_pressure=138, heart_rate=145,
                 risk_level="MEDIUM", explanation="Middle-aged with borderline cholesterol and BP.").with_inputs("age", "cholesterol", "blood_pressure", "heart_rate"),
    dspy.Example(age=68, cholesterol=290, blood_pressure=165, heart_rate=130,
                 risk_level="HIGH", explanation="Elderly with elevated cholesterol and hypertension.").with_inputs("age", "cholesterol", "blood_pressure", "heart_rate"),
    dspy.Example(age=45, cholesterol=210, blood_pressure=125, heart_rate=155,
                 risk_level="LOW", explanation="Normal range vitals for age.").with_inputs("age", "cholesterol", "blood_pressure", "heart_rate"),
    dspy.Example(age=61, cholesterol=260, blood_pressure=155, heart_rate=140,
                 risk_level="HIGH", explanation="Over 60 with high cholesterol and BP above 150.").with_inputs("age", "cholesterol", "blood_pressure", "heart_rate"),
]


def risk_metric(example, pred, trace=None):
    return pred.risk_level.strip().upper() == example.risk_level.strip().upper()


optimizer = dspy.BootstrapFewShot(metric=risk_metric, max_bootstrapped_demos=3)
optimized_classifier = optimizer.compile(classify_risk, trainset=trainset)

result = optimized_classifier(age=62, cholesterol=280, blood_pressure=155, heart_rate=140)
print(f"Optimized result: {result.risk_level} — {result.explanation}")

optimized_prompt = lm.history[-1]["prompt"] if hasattr(lm, "history") else str(optimized_classifier)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.dspy_optimized_prompt AS
    SELECT
        'patient_risk_assessment' AS function_name,
        '{optimized_prompt[:2000].replace("'", "''")}' AS optimized_prompt,
        current_timestamp() AS created_at
""")
print(f"Saved to {CATALOG}.default.dspy_optimized_prompt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
{'='*60}
Event 4: Agent Bricks — Summary for {TEAM_NAME}
{'='*60}
Heart Data:       {CATALOG}.default.heart_silver, heart_gold
Clinical Docs:    {CLINICAL_DOCS_PATH} ({len(clinical_documents)} files)
UC Functions:     patient_risk_assessment, get_cohort_summary
Genie Space:      {GENIE_SPACE_ID or 'NOT SET'}
Knowledge Asst:   {KA_TILE_ID or 'NOT SET'} (endpoint: {ka_endpoint or 'N/A'})
Supervisor:       {MAS_TILE_ID or 'NOT SET'} (endpoint: {mas_endpoint or 'N/A'})
Test Results:     {sum(1 for r in results if r['status'] == 'success')}/{len(results)} successful
{'='*60}
""")
