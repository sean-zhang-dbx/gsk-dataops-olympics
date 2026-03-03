# Databricks notebook source
# MAGIC %md
# MAGIC # Event 1: Data Engineering — Speed Sprint
# MAGIC
# MAGIC ## Challenge: Build a Production-Grade Data Pipeline
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
# MAGIC > **Your job:** Build a Medallion pipeline (Bronze -> Silver -> Gold) that ingests,
# MAGIC > cleans, and aggregates this data into a governed, analytics-ready table.
# MAGIC
# MAGIC ### Two Pathways (Choose One!)
# MAGIC
# MAGIC | | **Path A: Spark Declarative Pipelines (SDP)** | **Path B: Interactive SQL** |
# MAGIC |---|---|---|
# MAGIC | **Max Points** | **50 pts** | **31 pts** |
# MAGIC | **Difficulty** | Harder — create a pipeline in Workflows UI | Easier — run SQL in this notebook |
# MAGIC | **DQ Metrics** | Automatic from SDP expectations | Manual SQL computation |
# MAGIC | **Gold Table** | Materialized View (auto-refreshes) | Regular Delta Table |
# MAGIC | **Governance** | Comments defined in pipeline code | ALTER TABLE after creation |
# MAGIC
# MAGIC ### Vibe Coding Rules
# MAGIC
# MAGIC > Use **Databricks Assistant** (`Cmd+I` or the chat panel) to generate your code!
# MAGIC > Each step describes the **business requirements** — your job is to prompt the
# MAGIC > Assistant to write the correct implementation. The better your prompts, the faster you go.
# MAGIC
# MAGIC ### Raw Data Location
# MAGIC ```
# MAGIC /Volumes/dataops_olympics/default/raw_data/heart_events/
# MAGIC   ├── intake_batch_001.json   (100 records, clean)
# MAGIC   ├── intake_batch_002.json   (100 records, clean)
# MAGIC   ├── intake_batch_003.json   (100 records, 8 dirty + 1 duplicate)
# MAGIC   ├── intake_batch_004.json   (100 records, clean)
# MAGIC   └── intake_batch_005.json   (100 records, 4 dirty + 2 duplicates)
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dataops_olympics;
# MAGIC USE SCHEMA default;

# COMMAND ----------

TEAM_NAME = "team_XX"  # <-- CHANGE THIS to your team name (e.g., "team_01")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 0: Explore the Raw Data
# MAGIC
# MAGIC Before building anything, understand what you're working with.
# MAGIC Run these cells to explore the data and spot quality issues.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List the raw files in the Volume
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
# MAGIC
# MAGIC Run this cell to find the dirty data you need to clean in Silver.

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
# MAGIC ## Step 1: Bronze — Ingest All Raw Data
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Read **all 5 NDJSON batch files** from the Volume path
# MAGIC > `/Volumes/dataops_olympics/default/raw_data/heart_events/` and save them
# MAGIC > as a single Delta table called `{TEAM_NAME}_heart_bronze` in
# MAGIC > `dataops_olympics.default`. No cleaning — Bronze is the raw landing zone.
# MAGIC >
# MAGIC > The table should contain all ~500 records from all 5 files.

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!
# Prompt idea: "Read all JSON files from /Volumes/dataops_olympics/default/raw_data/heart_events/
#               and save as a Delta table called {TEAM_NAME}_heart_bronze"


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Choose Your Path!
# MAGIC
# MAGIC ### **Path A: Spark Declarative Pipelines (SDP) — 50 pts max**
# MAGIC
# MAGIC 1. Open the **`sdp_pipeline_template`** notebook (in this same folder)
# MAGIC 2. Go to **Workflows -> Pipelines -> Create Pipeline**
# MAGIC    - Pipeline name: `{TEAM_NAME}_heart_pipeline`
# MAGIC    - Source: select the `sdp_pipeline_template` notebook
# MAGIC    - Target catalog: `dataops_olympics`
# MAGIC    - Target schema: `default`
# MAGIC 3. Click **Start** and watch the pipeline run!
# MAGIC
# MAGIC After the pipeline completes, your tables will appear as:
# MAGIC - `dataops_olympics.default.heart_bronze`
# MAGIC - `dataops_olympics.default.heart_silver`
# MAGIC - `dataops_olympics.default.heart_gold` (materialized view!)
# MAGIC
# MAGIC **Then skip to Step 4 (Governance).**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Path B: Interactive SQL — 31 pts max**
# MAGIC
# MAGIC Continue below to build Silver and Gold with SQL/PySpark in this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3B: Silver — Clean the Data (SQL Path)
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create a Silver table called `{TEAM_NAME}_heart_silver` from the Bronze table.
# MAGIC > The Silver table must:
# MAGIC >
# MAGIC > 1. **Remove** rows where `age` is NULL
# MAGIC > 2. **Remove** rows where `age` is outside the valid range of 1-120
# MAGIC > 3. **Remove** rows where `trestbps` (blood pressure) is not between 50 and 300
# MAGIC > 4. **Remove** rows where `chol` (cholesterol) is negative
# MAGIC > 5. **Deduplicate** on `event_id` — if multiple rows share the same `event_id`,
# MAGIC >    keep only the one with the earliest `event_timestamp`
# MAGIC > 6. Add an `ingested_at` column with the current timestamp

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!
# Prompt idea: "Create a Silver table from my Bronze table that removes nulls,
#               invalid values, and duplicates per the requirements above"


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3B (cont): Gold — Aggregate for Analytics (SQL Path)
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create a Gold table called `{TEAM_NAME}_heart_gold` from the Silver table.
# MAGIC > It should aggregate heart disease metrics **by age group and diagnosis**:
# MAGIC >
# MAGIC > - `age_group`: bucket ages into "Under 40", "40-49", "50-59", "60+"
# MAGIC > - `diagnosis`: map `target` column — 1 = "Heart Disease", 0 = "Healthy"
# MAGIC > - `patient_count`: number of patients in each group
# MAGIC > - `avg_cholesterol`: average of `chol` column, rounded to 1 decimal
# MAGIC > - `avg_blood_pressure`: average of `trestbps` column, rounded to 1 decimal
# MAGIC > - `avg_max_heart_rate`: average of `thalach` column, rounded to 1 decimal

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!
# Prompt idea: "Create a Gold aggregation table that groups heart disease data by
#               age bucket and diagnosis with averages for cholesterol, BP, and heart rate"


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Governance — Add Metadata
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
# MAGIC > - `age` — what it means and its valid range
# MAGIC > - `trestbps` — what it measures and its valid range
# MAGIC > - `chol` — what it measures
# MAGIC > - `target` — what 0 and 1 mean
# MAGIC > - `event_id` — what it represents
# MAGIC >
# MAGIC > **SDP Path:** Your comments should already be in the pipeline code via
# MAGIC > the `comment` parameter in your SDP table definitions. Verify by running DESCRIBE below.

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!
# Prompt idea: "Add table comments and column comments to my heart disease pipeline tables.
#               Use ALTER TABLE SET TBLPROPERTIES for table comments and
#               ALTER TABLE ALTER COLUMN COMMENT for column comments."


# COMMAND ----------

# Verify your governance — run this to check your comments were applied
display(spark.sql(f"DESCRIBE TABLE EXTENDED dataops_olympics.default.{TEAM_NAME}_heart_silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Data Quality Metrics
# MAGIC
# MAGIC ### SDP Path — Your DQ metrics are FREE!
# MAGIC
# MAGIC After your pipeline runs, check the Pipeline UI for expectation results.
# MAGIC You can also query the event log programmatically:
# MAGIC
# MAGIC ```python
# MAGIC display(spark.sql("SELECT * FROM event_log('YOUR_PIPELINE_ID') WHERE event_type = 'flow_progress'"))
# MAGIC ```
# MAGIC
# MAGIC ### SQL Path — Compute DQ metrics manually
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Write a query against your **Bronze** table that produces a data quality report with:
# MAGIC > - Total record count
# MAGIC > - Count of null ages
# MAGIC > - Count of invalid ages (outside 1-120)
# MAGIC > - Count of invalid blood pressure readings (outside 50-300)
# MAGIC > - Count of negative cholesterol values
# MAGIC > - Count of duplicate event_ids
# MAGIC > - Percentage of clean records

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!
# Prompt idea: "Write a data quality report query that counts all the different types of
#               dirty data in my Bronze table"


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Validation — Run This!
# MAGIC
# MAGIC This cell checks your work and calculates your preliminary score.
# MAGIC The organizer will run the official scoring script separately.

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
# MAGIC ## Stretch Goals (Extra Credit)
# MAGIC
# MAGIC Finished early? Ask the Databricks Assistant to help you with these:
# MAGIC
# MAGIC 1. **Create a Genie space** on your Gold table — "Which age group has the highest heart disease rate?"
# MAGIC 2. **Liquid Clustering** — Optimize your Silver table with `CLUSTER BY (age, target)`
# MAGIC 3. **Change Data Feed** — Enable CDF on Silver so you can track row-level changes
# MAGIC 4. **Time Travel** — Overwrite Silver with bad data, then restore it to the previous version
# MAGIC 5. **AI Functions** — Use `ai_classify()` to categorize cholesterol levels as Normal/Borderline/High
