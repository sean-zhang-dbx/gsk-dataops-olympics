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
# MAGIC   ├── intake_batch_001.json   (100 records, clean)
# MAGIC   ├── intake_batch_002.json   (100 records, clean)
# MAGIC   ├── intake_batch_003.json   (100 records, ~8 dirty + 1 duplicate)
# MAGIC   ├── intake_batch_004.json   (100 records, clean)
# MAGIC   └── intake_batch_005.json   (100 records, ~4 dirty + 2 duplicates)
# MAGIC ```
# MAGIC
# MAGIC > **Vibe Coding:** Use **Databricks Assistant** (`Cmd+I`) to generate your code!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Team Configuration
# MAGIC
# MAGIC **CHANGE THIS** to your team name. Your team name is also your catalog name.
# MAGIC All your tables will be written to `{TEAM_NAME}.default.*`

# COMMAND ----------

TEAM_NAME = "team_XX"  # <-- CHANGE THIS (e.g., "team_01")
CATALOG = TEAM_NAME
SHARED_CATALOG = "dataops_olympics"
RAW_DATA_PATH = f"/Volumes/{SHARED_CATALOG}/default/raw_data/heart_events/"

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")
print(f"Working in: {CATALOG}.default")
print(f"Raw data:   {RAW_DATA_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Pipeline Overview
# MAGIC
# MAGIC ```
# MAGIC  NDJSON Files          Bronze              Silver              Gold
# MAGIC ┌──────────┐     ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
# MAGIC │ batch_001│     │              │    │  Validated   │    │  Aggregated  │
# MAGIC │ batch_002│────>│  Raw ingest  │───>│  Cleaned     │───>│  By age_group│
# MAGIC │ batch_003│     │  ~500 rows   │    │  Deduplicated│    │  & diagnosis │
# MAGIC │ batch_004│     │              │    │  ~485 rows   │    │  ~8 rows     │
# MAGIC │ batch_005│     └──────────────┘    └──────────────┘    └──────────────┘
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
# MAGIC > **No cleaning** — Bronze is the raw landing zone. All ~500 records should be present.
# MAGIC >
# MAGIC > *SDP Path: This is the `heart_bronze` streaming table — uses Auto Loader.*

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
# MAGIC > 6. Add an `ingested_at` column with the current timestamp
# MAGIC >
# MAGIC > *SDP Path: This is the `heart_silver` streaming table with `@dp.expect_or_drop` expectations.*

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
# MAGIC > Aggregate heart disease metrics **by age group and diagnosis**:
# MAGIC >
# MAGIC > | Column | Definition |
# MAGIC > |--------|-----------|
# MAGIC > | `age_group` | Bucket ages: "Under 40", "40-49", "50-59", "60+" |
# MAGIC > | `diagnosis` | Map `target`: 1 = "Heart Disease", 0 = "Healthy" |
# MAGIC > | `patient_count` | Number of patients in each group |
# MAGIC > | `avg_cholesterol` | Average of `chol`, rounded to 1 decimal |
# MAGIC > | `avg_blood_pressure` | Average of `trestbps`, rounded to 1 decimal |
# MAGIC > | `avg_max_heart_rate` | Average of `thalach`, rounded to 1 decimal |
# MAGIC >
# MAGIC > *SDP Path: This is the `heart_gold` materialized view via `@dp.table` with a batch read.*

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
# MAGIC Run this to check your work and get a preliminary score.
# MAGIC The organizer will run the official `scoring.py` separately.

# COMMAND ----------

print("=" * 60)
print(f"  EVENT 1 VALIDATION — {TEAM_NAME}")
print(f"  Catalog: {CATALOG}.default")
print("=" * 60)
score = 0

try:
    cnt = spark.table(f"{CATALOG}.default.heart_bronze").count()
    if cnt >= 490:
        print(f"  [PASS] Bronze table: {cnt} rows")
        score += 5
    else:
        print(f"  [WARN] Bronze table: {cnt} rows (expected ~500)")
        score += 2
except Exception as e:
    print(f"  [FAIL] Bronze table missing: {e}")

try:
    s_cnt = spark.table(f"{CATALOG}.default.heart_silver").count()
    if s_cnt < cnt:
        print(f"  [PASS] Silver table: {s_cnt} rows (filtered {cnt - s_cnt} bad/duplicate rows)")
        score += 4
    else:
        print(f"  [WARN] Silver table: {s_cnt} rows (no filtering detected)")
        score += 1
except Exception as e:
    print(f"  [FAIL] Silver table missing: {e}")

try:
    g = spark.table(f"{CATALOG}.default.heart_gold")
    g_cnt = g.count()
    cols = set(g.columns)
    has_agg = "patient_count" in cols or "avg_cholesterol" in cols
    if g_cnt > 0 and has_agg:
        print(f"  [PASS] Gold table: {g_cnt} rows with aggregations")
        score += 4
    elif g_cnt > 0:
        print(f"  [WARN] Gold table: {g_cnt} rows but missing expected aggregation columns")
        score += 2
    else:
        print(f"  [WARN] Gold table is empty")
except Exception as e:
    print(f"  [FAIL] Gold table missing: {e}")

try:
    desc = spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.default.heart_silver").collect()
    has_tbl_comment = any("comment" in str(r).lower() and r[1] and len(str(r[1])) > 5 for r in desc)
    col_comments = sum(1 for r in desc if r[2] and len(str(r[2])) > 5 and r[0] not in ["", "#"])
    if has_tbl_comment:
        print(f"  [PASS] Table comment found on Silver")
        score += 2
    else:
        print(f"  [FAIL] No table comment on Silver")
    if col_comments >= 2:
        print(f"  [PASS] {col_comments} column comments found")
        score += 3
    else:
        print(f"  [WARN] Only {col_comments} column comments (need 3+)")
        score += col_comments
except Exception as e:
    print(f"  [FAIL] Cannot verify governance: {e}")

print(f"\n  PRELIMINARY SCORE: {score}/~20 (SQL path estimate)")
print(f"  NOTE: SDP path scores up to 50 pts — run the official scoring.py!")
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

from datetime import datetime as _dt

_event_name = "Event 1: Data Engineering"
_submit_ts = _dt.now()

spark.sql(f"""
    INSERT INTO dataops_olympics.default.event_submissions
    VALUES ('{TEAM_NAME}', '{_event_name}', '{_submit_ts}', NULL)
""")

print("=" * 60)
print(f"  SUBMITTED! {TEAM_NAME} — {_event_name}")
print(f"  Timestamp: {_submit_ts.strftime('%H:%M:%S.%f')}")
print(f"  Signal the judges that you are DONE!")
print("=" * 60)
