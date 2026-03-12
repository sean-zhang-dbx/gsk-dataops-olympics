# Databricks notebook source
# MAGIC %md
# MAGIC # Event 1: Data Engineering — Speed Sprint
# MAGIC
# MAGIC ## Build a Production-Grade Medallion Pipeline
# MAGIC **Time: 25 minutes** | **Max Points: 50 (+5 bonus)**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Scenario
# MAGIC
# MAGIC > A hospital's patient intake system sends **newline-delimited JSON (NDJSON)** files
# MAGIC > to a Unity Catalog Volume every few hours. Five batches have arrived today.
# MAGIC > Some records have data quality issues — missing ages, impossible blood pressure
# MAGIC > readings, and duplicate events.
# MAGIC >
# MAGIC > **Your job:** Build a Medallion pipeline (Bronze → Silver → Gold) that ingests,
# MAGIC > cleans, and aggregates this data into a governed, analytics-ready table.
# MAGIC
# MAGIC ### Choose Your Implementation
# MAGIC
# MAGIC The business logic is **the same** for both paths — only the implementation differs:
# MAGIC
# MAGIC | | **Path A: Spark Declarative Pipelines (SDP)** | **Path B: Interactive SQL/PySpark** |
# MAGIC |---|---|---|
# MAGIC | **Max Points** | **50 pts** | **31 pts** |
# MAGIC | **How** | Create a `pipeline` notebook → run via Workflows → Pipelines | Write code directly in this notebook |
# MAGIC | **Ingestion** | Auto Loader (streaming, incremental) | Batch `spark.read.json` |
# MAGIC | **Bronze & Silver** | Streaming Tables | Regular Delta Tables |
# MAGIC | **Gold** | Materialized View (auto-refreshes) | Regular Delta Table |
# MAGIC | **DQ Metrics** | Automatic — SDP expectations track pass/fail rates | Manual — write your own DQ queries |
# MAGIC | **Governance** | Comments defined in pipeline code | ALTER TABLE after creation |
# MAGIC | **Lineage** | Pipeline UI shows full dependency graph | No automatic lineage |
# MAGIC
# MAGIC ### Raw Data Location
# MAGIC ```
# MAGIC /Volumes/dataops_olympics/default/raw_data/heart_events/
# MAGIC   ├── intake_batch_001.json   (100 records)
# MAGIC   ├── intake_batch_002.json   (100 records)
# MAGIC   ├── intake_batch_003.json   (106 records — includes dirty rows + duplicates)
# MAGIC   ├── intake_batch_004.json   (100 records)
# MAGIC   └── intake_batch_005.json   (104 records — includes dirty rows + duplicates)
# MAGIC   TOTAL: 510 raw records
# MAGIC ```
# MAGIC
# MAGIC > **Vibe Coding:** Use **Databricks Assistant** (`Cmd+I`) to generate your code!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Team Configuration

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_submit

# COMMAND ----------

RAW_DATA_PATH = f"/Volumes/{SHARED_CATALOG}/{SHARED_SCHEMA}/raw_data/heart_events/"
print(f"Working in: {CATALOG}.default")
print(f"Raw data:   {RAW_DATA_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Pipeline Overview
# MAGIC
# MAGIC ```
# MAGIC  NDJSON Files          Bronze               Silver               Gold
# MAGIC ┌──────────┐     ┌───────────────┐    ┌───────────────┐    ┌───────────────┐
# MAGIC │ batch_001│     │               │    │  Validated    │    │  Aggregated   │
# MAGIC │ batch_002│────>│  Raw ingest   │───>│  Cleaned      │───>│  By age_group │
# MAGIC │ batch_003│     │  510 rows     │    │  Deduplicated │    │  & diagnosis  │
# MAGIC │ batch_004│     │  (all records)│    │  491 rows     │    │  8 rows       │
# MAGIC │ batch_005│     └───────────────┘    └───────────────┘    └───────────────┘
# MAGIC └──────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 0: Explore the Raw Data
# MAGIC
# MAGIC Before building anything, understand what you're working with.

# COMMAND ----------

display(spark.sql(f"LIST '{RAW_DATA_PATH}'"))

# COMMAND ----------

df_peek = spark.read.json(f"{RAW_DATA_PATH}intake_batch_001.json")
print(f"Schema of a single batch:")
df_peek.printSchema()
print(f"Records in batch 1: {df_peek.count()}")

# COMMAND ----------

df_all_raw = spark.read.json(f"{RAW_DATA_PATH}*.json")
print(f"Total raw records across all 5 batches: {df_all_raw.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Profiling — Spot the Problems

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as _sum, when, lit

profile = df_all_raw.select(
    count("*").alias("total_rows"),
    _sum(when(col("age").isNull(), 1).otherwise(0)).alias("null_ages"),
    _sum(when(~col("age").between(1, 120), 1).otherwise(0)).alias("invalid_ages"),
    _sum(when(col("trestbps").isin(999, -1) | ~col("trestbps").between(50, 300), 1).otherwise(0)).alias("invalid_bp"),
    _sum(when(col("chol") < 0, 1).otherwise(0)).alias("negative_chol"),
)
display(profile)

dup_count = df_all_raw.count() - df_all_raw.dropDuplicates(["event_id"]).count()
print(f"Duplicate event_ids: {dup_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 1: Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Read **all 5 NDJSON batch files** from the Volume path and save them
# MAGIC > as a single Delta table called `heart_bronze` in your team catalog.
# MAGIC >
# MAGIC > **No cleaning** — Bronze is the raw landing zone. All 510 records must be present.
# MAGIC >
# MAGIC > *SDP Path: This is the `heart_bronze` streaming table — uses Auto Loader.*
# MAGIC
# MAGIC ### Expected Output: `{TEAM_NAME}.default.heart_bronze`
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | `event_id` | STRING | Unique event identifier (e.g., `EVT-00001`) |
# MAGIC | `event_timestamp` | STRING | ISO 8601 timestamp |
# MAGIC | `source_system` | STRING | `hospital_ehr`, `clinic_intake`, or `emergency_dept` |
# MAGIC | `record_version` | BIGINT | Always 1 |
# MAGIC | `patient_id` | STRING | Patient identifier (e.g., `PT-0001`) |
# MAGIC | `age` | BIGINT | Patient age — **nullable** (some records have NULL) |
# MAGIC | `sex` | BIGINT | 0 = female, 1 = male |
# MAGIC | `cp` | BIGINT | Chest pain type (0–3) |
# MAGIC | `trestbps` | BIGINT | Resting blood pressure in mmHg |
# MAGIC | `chol` | BIGINT | Serum cholesterol in mg/dL |
# MAGIC | `fbs` | BIGINT | Fasting blood sugar > 120 mg/dL (0/1) |
# MAGIC | `restecg` | BIGINT | Resting ECG results (0–2) |
# MAGIC | `thalach` | BIGINT | Maximum heart rate achieved |
# MAGIC | `exang` | BIGINT | Exercise-induced angina (0/1) |
# MAGIC | `oldpeak` | DOUBLE | ST depression induced by exercise |
# MAGIC | `slope` | BIGINT | Slope of peak exercise ST segment (0–2) |
# MAGIC | `ca` | BIGINT | Number of major vessels (0–3) |
# MAGIC | `thal` | BIGINT | Thalassemia (3, 6, or 7) |
# MAGIC | `target` | BIGINT | Diagnosis: 1 = heart disease, 0 = healthy |
# MAGIC
# MAGIC **Expected row count: 510**

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 2: Silver Layer — Clean & Deduplicate
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create a Silver table called `heart_silver` from the Bronze table.
# MAGIC > Apply these data quality rules:
# MAGIC >
# MAGIC > 1. **Remove** rows where `age` is NULL
# MAGIC > 2. **Remove** rows where `age` is outside the valid range of 1–120
# MAGIC > 3. **Remove** rows where `trestbps` (resting blood pressure) is not between 50 and 300
# MAGIC > 4. **Remove** rows where `chol` (cholesterol) is negative
# MAGIC > 5. **Deduplicate** on `event_id` — if multiple rows share the same `event_id`,
# MAGIC >    keep only the one with the **earliest** `event_timestamp`
# MAGIC > 6. (Optional) Add an `ingested_at` column with the current timestamp
# MAGIC >
# MAGIC > *SDP Path: This is the `heart_silver` streaming table with `@dp.expect_or_drop` expectations.*
# MAGIC
# MAGIC ### Expected Output: `{TEAM_NAME}.default.heart_silver`
# MAGIC
# MAGIC Same columns as Bronze (all 19 clinical columns including `event_id`, `event_timestamp`,
# MAGIC `source_system`, `record_version`, `patient_id`), optionally plus `ingested_at`.
# MAGIC
# MAGIC **Data quality after filtering:**
# MAGIC - No rows with `age IS NULL`
# MAGIC - No rows with `age` outside 1–120
# MAGIC - No rows with `trestbps` outside 50–300
# MAGIC - No rows with `chol < 0`
# MAGIC - No duplicate `event_id` values
# MAGIC
# MAGIC **Expected row count: 491** (510 raw − 10 duplicates − 9 dirty unique records)

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 3: Gold Layer — Aggregate for Analytics
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create a Gold table called `heart_gold` from the Silver table.
# MAGIC > Aggregate heart disease metrics **by age group and diagnosis**.
# MAGIC >
# MAGIC > *SDP Path: This is the `heart_gold` materialized view via `@dp.table` with a batch read.*
# MAGIC
# MAGIC ### Expected Output: `{TEAM_NAME}.default.heart_gold`
# MAGIC
# MAGIC | Column | Type | Definition |
# MAGIC |--------|------|-----------|
# MAGIC | `age_group` | STRING | Bucket ages: `Under 40` (age < 40), `40-49` (40 ≤ age < 50), `50-59` (50 ≤ age < 60), `60+` (age ≥ 60) |
# MAGIC | `diagnosis` | STRING | Map `target`: 1 → `Heart Disease`, 0 → `Healthy` |
# MAGIC | `patient_count` | BIGINT | `COUNT(*)` per group |
# MAGIC | `avg_cholesterol` | DOUBLE | `ROUND(AVG(chol), 1)` |
# MAGIC | `avg_blood_pressure` | DOUBLE | `ROUND(AVG(trestbps), 1)` |
# MAGIC | `avg_max_heart_rate` | DOUBLE | `ROUND(AVG(thalach), 1)` |
# MAGIC
# MAGIC **Expected row count: 8** (4 age groups × 2 diagnoses)
# MAGIC
# MAGIC > **Column names must match exactly** — the scoring engine compares your table against
# MAGIC > the answer key row-by-row. Use the exact column names above.

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SDP Path — Build Your Pipeline with AI
# MAGIC
# MAGIC **If you chose Path A (SDP)**, here's your workflow:
# MAGIC
# MAGIC ### Step A: Read the requirements
# MAGIC Open the **`sdp_pipeline_template`** notebook (in this same folder).
# MAGIC It contains the business requirements for all 3 layers + an SDP syntax reference.
# MAGIC
# MAGIC ### Step B: Create ONE pipeline file with AI
# MAGIC 1. Create a **new notebook** called `pipeline` (Python or SQL — your choice)
# MAGIC 2. Use the **Databricks Assistant** (`Cmd+I`) to generate the entire pipeline
# MAGIC    in that **single file** — all 3 layers (Bronze, Silver, Gold) together
# MAGIC 3. The result should be one self-contained file with all your SDP definitions
# MAGIC
# MAGIC ### Step C: Run it as a pipeline
# MAGIC 1. Go to **Workflows → Pipelines → Create Pipeline**
# MAGIC    - Pipeline name: `{TEAM_NAME}_heart_pipeline`
# MAGIC    - Source: select your `pipeline` notebook
# MAGIC    - **Destination catalog: `{TEAM_NAME}`** (your team catalog!)
# MAGIC    - **Destination schema: `default`**
# MAGIC 2. Click **Validate** first (dry-run to catch errors)
# MAGIC 3. Click **Start** to execute the full pipeline
# MAGIC
# MAGIC ### What you get
# MAGIC - `heart_bronze` — **Streaming Table** (Auto Loader ingestion)
# MAGIC - `heart_silver` — **Streaming Table** (cleaned with DQ expectations)
# MAGIC - `heart_gold` — **Materialized View** (auto-refreshing aggregation)
# MAGIC - **Automatic DQ metrics** in the Pipeline UI
# MAGIC - **Full lineage graph** showing Bronze → Silver → Gold
# MAGIC
# MAGIC ### Step D: Come back here
# MAGIC After the pipeline completes, return to this notebook for Steps 4–6
# MAGIC (governance, DQ report, validation).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 4: Governance — Add Metadata
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Every production table needs documentation. Add table-level and column-level
# MAGIC > comments so that other teams can understand the data without asking you.
# MAGIC >
# MAGIC > **For each of your 3 tables (Bronze, Silver, Gold):**
# MAGIC > - Add a **table comment** describing what the table contains and its purpose
# MAGIC >
# MAGIC > **For the Silver table, add column comments on at least these columns:**
# MAGIC > - `age` — patient age in years, validated 1–120
# MAGIC > - `trestbps` — resting blood pressure in mmHg, validated 50–300
# MAGIC > - `chol` — serum cholesterol in mg/dL, validated >= 0
# MAGIC > - `target` — diagnosis: 1 = heart disease present, 0 = healthy
# MAGIC > - `event_id` — unique event identifier from the source system
# MAGIC >
# MAGIC > *SDP Path: If you defined `comment=` in your `@dp.table()` decorators,
# MAGIC > your table comments are already applied. You may still want to add column comments.*

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.default.heart_silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 5: Data Quality Report
# MAGIC
# MAGIC ### SDP Path (automatic)
# MAGIC
# MAGIC Your DQ metrics are already captured by the pipeline! Check the Pipeline UI
# MAGIC for expectation pass/fail rates.
# MAGIC
# MAGIC ### SQL Path (manual)
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Write a query against your **Bronze** table that produces a data quality report:
# MAGIC >
# MAGIC > - Total record count
# MAGIC > - Count of null ages
# MAGIC > - Count of invalid ages (outside 1–120)
# MAGIC > - Count of invalid blood pressure readings (outside 50–300)
# MAGIC > - Count of negative cholesterol values
# MAGIC > - Count of duplicate event_ids
# MAGIC > - Percentage of clean records

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 6: Validation
# MAGIC
# MAGIC Run this to check your tables against the official answer key.
# MAGIC The organizer will run the official `scoring.py` separately.
# MAGIC
# MAGIC > For a **detailed self-check** with schema validation and row-by-row comparison,
# MAGIC > open and run the **`self_check`** notebook in this folder.

# COMMAND ----------

_ANSWER_BRONZE = f"{SHARED_CATALOG}.{SHARED_SCHEMA}.event1_answer_bronze"
_ANSWER_SILVER = f"{SHARED_CATALOG}.{SHARED_SCHEMA}.event1_answer_silver"
_ANSWER_GOLD = f"{SHARED_CATALOG}.{SHARED_SCHEMA}.event1_answer_gold"

_exp_b = spark.table(_ANSWER_BRONZE).count()
_exp_s = spark.table(_ANSWER_SILVER).count()
_exp_g = spark.table(_ANSWER_GOLD).count()

print("=" * 60)
print(f"  EVENT 1 VALIDATION — {TEAM_NAME}")
print(f"  Catalog: {CATALOG}.default")
print(f"  Answer key: Bronze={_exp_b}, Silver={_exp_s}, Gold={_exp_g}")
print("=" * 60)

for tbl_name, answer, exp_cnt, required_cols in [
    ("heart_bronze", _ANSWER_BRONZE, _exp_b,
     {"event_id", "event_timestamp", "source_system", "patient_id", "age", "sex", "cp", "trestbps", "chol", "target"}),
    ("heart_silver", _ANSWER_SILVER, _exp_s,
     {"event_id", "age", "sex", "cp", "trestbps", "chol", "target"}),
    ("heart_gold", _ANSWER_GOLD, _exp_g,
     {"age_group", "diagnosis", "patient_count", "avg_cholesterol", "avg_blood_pressure", "avg_max_heart_rate"}),
]:
    print()
    try:
        df = spark.table(f"{CATALOG}.default.{tbl_name}")
        cnt = df.count()
        cols = set(c.lower() for c in df.columns)
        missing_cols = required_cols - cols

        if missing_cols:
            print(f"  [{tbl_name}] SCHEMA: missing columns {missing_cols}")
        else:
            print(f"  [{tbl_name}] SCHEMA: OK — all required columns present")

        if cnt == exp_cnt:
            print(f"  [{tbl_name}] ROWS: {cnt} — exact match!")
        else:
            print(f"  [{tbl_name}] ROWS: {cnt} (expected {exp_cnt}) — off by {abs(cnt - exp_cnt)}")

        if "event_id" in cols and "event_id" in [c.lower() for c in spark.table(answer).columns]:
            missing = spark.sql(f"SELECT COUNT(*) FROM (SELECT event_id FROM {answer} EXCEPT SELECT event_id FROM {CATALOG}.default.{tbl_name})").collect()[0][0]
            match_pct = (exp_cnt - missing) / exp_cnt * 100
            if missing == 0:
                print(f"  [{tbl_name}] DATA: 100% match against answer key")
            else:
                print(f"  [{tbl_name}] DATA: {match_pct:.1f}% match — {missing} event_ids missing from your table")
        elif tbl_name == "heart_gold" and not missing_cols and cnt > 0:
            gold_match = spark.sql(f"""
                SELECT COUNT(*) FROM {answer} a
                JOIN {CATALOG}.default.{tbl_name} t
                ON a.age_group = t.age_group AND a.diagnosis = t.diagnosis
                WHERE a.patient_count = t.patient_count
            """).collect()[0][0]
            print(f"  [{tbl_name}] DATA: {gold_match}/{exp_cnt} rows match answer key patient_counts")

    except Exception as e:
        print(f"  [{tbl_name}] NOT FOUND — {str(e)[:80]}")

print()
print("=" * 60)
print("  Run the self_check notebook for detailed scoring breakdown.")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Stretch Goals (Extra Credit)
# MAGIC
# MAGIC Finished early? Ask the Databricks Assistant to help you with these:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1. Unity Catalog Tags (+3 pts)
# MAGIC
# MAGIC Tag your tables with metadata so other teams can discover and classify them.
# MAGIC Add **at least 3 tags** across your heart tables.
# MAGIC
# MAGIC > **Both paths (SDP and SQL):** Works on all table types, including streaming tables.
# MAGIC >
# MAGIC > ```sql
# MAGIC > ALTER TABLE heart_silver SET TAGS ('domain' = 'cardiology', 'pii' = 'true', 'quality_tier' = 'silver');
# MAGIC > ALTER TABLE heart_gold   SET TAGS ('domain' = 'cardiology', 'quality_tier' = 'gold');
# MAGIC > ```
# MAGIC >
# MAGIC > *Why Tags? In large organizations, Unity Catalog tags enable data discovery,
# MAGIC > compliance tracking (PII classification), and access governance. They're searchable
# MAGIC > in the Catalog Explorer and can drive automated policies.*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. AI Functions — `ai_query()` (+2 pts)
# MAGIC
# MAGIC Use `ai_query()` to generate a natural language cardiovascular risk assessment for each
# MAGIC patient cohort in your Gold table. The LLM combines multiple health metrics (cholesterol,
# MAGIC blood pressure, heart rate) into a qualitative risk judgment — something a simple `CASE WHEN` can't do.
# MAGIC
# MAGIC > **Both paths (SDP and SQL):** Create a new table called **`heart_gold_ai`** from your
# MAGIC > Gold table. Gold only has ~8 rows, so this is fast and cheap.
# MAGIC >
# MAGIC > **Requirements for scoring:**
# MAGIC > - Table name: **`heart_gold_ai`**
# MAGIC > - Must include a column called **`cardiovascular_risk`**
# MAGIC > - Use `ai_query()` to classify risk as Low, Moderate, or High
# MAGIC >
# MAGIC > ```sql
# MAGIC > CREATE OR REPLACE TABLE heart_gold_ai AS
# MAGIC > SELECT *,
# MAGIC >     ai_query(
# MAGIC >         'databricks-meta-llama-3-3-70b-instruct',
# MAGIC >         CONCAT(
# MAGIC >             'Patient cohort: ', patient_count, ' patients, ', age_group, ', ', diagnosis,
# MAGIC >             '. Avg cholesterol: ', avg_cholesterol,
# MAGIC >             ', avg BP: ', avg_blood_pressure,
# MAGIC >             ', avg max heart rate: ', avg_max_heart_rate,
# MAGIC >             '. Classify cardiovascular risk as exactly one of: Low, Moderate, High. Reply with one word only.'
# MAGIC >         )
# MAGIC >     ) as cardiovascular_risk
# MAGIC > FROM heart_gold
# MAGIC > ```
# MAGIC >
# MAGIC > *Why Gold? Only ~8 rows — `ai_query` calls an LLM per row, so fewer rows = faster + cheaper.
# MAGIC > Unlike `ai_classify`, `ai_query` lets the LLM reason across multiple metrics simultaneously.*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Time Travel (no pts, just cool)
# MAGIC
# MAGIC > **SQL Path only** (streaming tables don't support overwrite):
# MAGIC > Overwrite your Silver table with bad data, then restore the previous version using
# MAGIC > `RESTORE TABLE heart_silver TO VERSION AS OF <version_number>`

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## SUBMIT YOUR WORK
# MAGIC
# MAGIC **Run this cell when you're done!** It records your submission timestamp for speed tracking.
# MAGIC The first team to submit gets a speed bonus on the live scoreboard.

# COMMAND ----------

submit("event1")
