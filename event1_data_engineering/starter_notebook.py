# Databricks notebook source
# MAGIC %md
# MAGIC # Event 1: Data Engineering — Speed Sprint
# MAGIC
# MAGIC ## Build a Production-Grade Medallion Pipeline
# MAGIC **Time: 25 minutes** | **Max Points: 50**
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
# MAGIC | **How** | Use the `sdp_pipeline_template` notebook via Workflows → Pipelines | Write code directly in this notebook |
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

# MAGIC %sql
# MAGIC USE CATALOG dataops_olympics;
# MAGIC USE SCHEMA default;

# COMMAND ----------

TEAM_NAME = "team_XX"  # <-- CHANGE THIS to your team name (e.g., "team_01")

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

# MAGIC %sql
# MAGIC LIST '/Volumes/dataops_olympics/default/raw_data/heart_events/'

# COMMAND ----------

df_peek = spark.read.json("/Volumes/dataops_olympics/default/raw_data/heart_events/intake_batch_001.json")
print(f"Schema of a single batch:")
df_peek.printSchema()
print(f"Records in batch 1: {df_peek.count()}")

# COMMAND ----------

df_all_raw = spark.read.json("/Volumes/dataops_olympics/default/raw_data/heart_events/*.json")
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
# MAGIC > Read **all 5 NDJSON batch files** from the Volume path
# MAGIC > `/Volumes/dataops_olympics/default/raw_data/heart_events/` and save them
# MAGIC > as a single Delta table called `{TEAM_NAME}_heart_bronze`.
# MAGIC >
# MAGIC > **No cleaning** — Bronze is the raw landing zone. All ~500 records should be present.
# MAGIC >
# MAGIC > *SDP Path: This is the `heart_bronze` streaming table in `sdp_pipeline_template` — uses Auto Loader.*

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 2: Silver Layer — Clean & Deduplicate
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create a Silver table called `{TEAM_NAME}_heart_silver` from the Bronze table.
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
# MAGIC > Create a Gold table called `{TEAM_NAME}_heart_gold` from the Silver table.
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
# MAGIC > *SDP Path: This is the `heart_gold` materialized view via `@dp.materialized_view`.*

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SDP Path — Build Your Pipeline with AI
# MAGIC
# MAGIC **If you chose Path A (SDP)**, here's your workflow:
# MAGIC
# MAGIC ### Step A: Fill in the template with AI
# MAGIC 1. Open the **`sdp_pipeline_template`** notebook (in this same folder)
# MAGIC 2. Read the business requirements for each layer (Bronze, Silver, Gold)
# MAGIC 3. Use **Databricks Assistant** (`Cmd+I`) to generate the code — Python or SQL!
# MAGIC    - The template shows syntax hints for both languages
# MAGIC    - The SDP Python API: `from pyspark import pipelines as dp`
# MAGIC    - Streaming tables: `@dp.table` / `CREATE OR REFRESH STREAMING TABLE`
# MAGIC    - Materialized views: `@dp.materialized_view` / `CREATE OR REFRESH MATERIALIZED VIEW`
# MAGIC    - Expectations: `@dp.expect_or_drop(...)` / `CONSTRAINT ... ON VIOLATION DROP ROW`
# MAGIC
# MAGIC ### Step B: Create and run the pipeline
# MAGIC 1. Go to **Workflows → Pipelines → Create Pipeline**
# MAGIC    - Pipeline name: `{TEAM_NAME}_heart_pipeline`
# MAGIC    - Source: select the `sdp_pipeline_template` notebook
# MAGIC    - Target catalog: `dataops_olympics`
# MAGIC    - Target schema: `default`
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
# MAGIC ### Step C: Come back here
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
# MAGIC > *SDP Path: If you defined `comment=` in your `@dp.table()` / `@dp.materialized_view()`
# MAGIC > decorators, your table comments are already applied. You may still want to add column comments.*

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

# Verify governance — check that comments were applied
display(spark.sql(f"DESCRIBE TABLE EXTENDED dataops_olympics.default.{TEAM_NAME}_heart_silver"))

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
print("=" * 60)
score = 0

try:
    cnt = spark.table(f"dataops_olympics.default.{TEAM_NAME}_heart_bronze").count()
    if cnt >= 490:
        print(f"  [PASS] Bronze table: {cnt} rows")
        score += 5
    else:
        print(f"  [WARN] Bronze table: {cnt} rows (expected ~500)")
        score += 2
except Exception as e:
    print(f"  [FAIL] Bronze table missing: {e}")

try:
    s_cnt = spark.table(f"dataops_olympics.default.{TEAM_NAME}_heart_silver").count()
    if s_cnt < cnt:
        print(f"  [PASS] Silver table: {s_cnt} rows (filtered {cnt - s_cnt} bad/duplicate rows)")
        score += 4
    else:
        print(f"  [WARN] Silver table: {s_cnt} rows (no filtering detected)")
        score += 1
except Exception as e:
    print(f"  [FAIL] Silver table missing: {e}")

try:
    g = spark.table(f"dataops_olympics.default.{TEAM_NAME}_heart_gold")
    g_cnt = g.count()
    cols = set(g.columns)
    has_agg = "patient_count" in cols or "avg_cholesterol" in cols or "count" in [c.lower() for c in cols]
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
    desc = spark.sql(f"DESCRIBE TABLE EXTENDED dataops_olympics.default.{TEAM_NAME}_heart_silver").collect()
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
# MAGIC 1. **Genie Space** — Create a Genie space on your Gold table and ask "Which age group has the highest heart disease rate?"
# MAGIC 2. **Liquid Clustering** — Optimize your Silver table with `CLUSTER BY (age, target)`
# MAGIC 3. **Change Data Feed** — Enable CDF on Silver to track row-level changes
# MAGIC 4. **Time Travel** — Overwrite Silver with bad data, then restore the previous version
# MAGIC 5. **AI Functions** — Use `ai_classify()` to categorize cholesterol levels as Normal/Borderline/High
