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
# MAGIC > **Your job:** Build a Medallion pipeline (Bronze → Silver → Gold) that ingests,
# MAGIC > cleans, and aggregates this data into a governed, analytics-ready table.
# MAGIC
# MAGIC ### Two Pathways (Choose One!)
# MAGIC
# MAGIC | | **Path A: Spark Declarative Pipelines (SDP)** | **Path B: Interactive SQL** |
# MAGIC |---|---|---|
# MAGIC | **Max Points** | **50 pts** | **31 pts** |
# MAGIC | **Difficulty** | Harder — create a pipeline in Workflows UI | Easier — run SQL in this notebook |
# MAGIC | **DQ Metrics** | Automatic from `@dlt.expect` | Manual SQL computation |
# MAGIC | **Gold Table** | Materialized View (auto-refreshes) | Regular Delta Table |
# MAGIC | **Governance** | Comments defined in pipeline code | ALTER TABLE after creation |
# MAGIC
# MAGIC > **Tip:** Use the Databricks Assistant (`Cmd+I`) to generate code!
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

TEAM_NAME = "_____"  # e.g., "team_01" — SET THIS FIRST!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 0: Explore the Raw Data
# MAGIC
# MAGIC Before building anything, understand what you're working with.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List the raw files in the Volume
# MAGIC LIST '/Volumes/dataops_olympics/default/raw_data/heart_events/'

# COMMAND ----------

# Preview one batch — notice the NDJSON format (one JSON object per line)
df_peek = spark.read.json("/Volumes/dataops_olympics/default/raw_data/heart_events/intake_batch_001.json")
print(f"Schema of a single batch:")
df_peek.printSchema()
print(f"Records in batch 1: {df_peek.count()}")

# COMMAND ----------

# Read ALL batches at once using a wildcard path
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

# Check for duplicate event_ids
dup_count = df_all_raw.count() - df_all_raw.dropDuplicates(["event_id"]).count()
print(f"Duplicate event_ids: {dup_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Bronze — Ingest All Raw Data
# MAGIC
# MAGIC Load all 5 NDJSON files into a single Bronze Delta table. **No cleaning yet** — Bronze is the raw landing zone.

# COMMAND ----------

# TODO: Read all NDJSON files from the heart_events folder and save as Bronze table
# Hint: spark.read.json("/Volumes/dataops_olympics/default/raw_data/heart_events/*.json")

df_bronze = _____  # YOUR CODE HERE

df_bronze.write.format("delta").mode("overwrite").saveAsTable(
    f"dataops_olympics.default.{TEAM_NAME}_heart_bronze"
)
print(f"Bronze table created: {TEAM_NAME}_heart_bronze ({df_bronze.count()} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Choose Your Path!
# MAGIC
# MAGIC ### **Path A: Spark Declarative Pipelines (SDP) — 50 pts max**
# MAGIC
# MAGIC 1. Open the **`sdp_pipeline_template`** notebook (in this same folder)
# MAGIC 2. Update `TEAM_NAME` at the top
# MAGIC 3. Customize the expectations and Gold aggregation
# MAGIC 4. Go to **Workflows → Pipelines → Create Pipeline**
# MAGIC    - Name: `{TEAM_NAME}_heart_pipeline`
# MAGIC    - Source: point to your copy of the SDP template notebook
# MAGIC    - Target catalog: `dataops_olympics`
# MAGIC    - Target schema: `default`
# MAGIC 5. Click **Start** and watch the pipeline run!
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
# MAGIC Continue below to build Silver and Gold with SQL in this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3B: Silver — Clean the Data (SQL Path)
# MAGIC
# MAGIC **TODO:** Create a Silver table that:
# MAGIC 1. Removes rows with NULL age
# MAGIC 2. Removes rows with invalid blood pressure (not between 50 and 300)
# MAGIC 3. Removes rows with negative cholesterol
# MAGIC 4. Deduplicates on `event_id` (keep the first occurrence)
# MAGIC 5. Adds an `ingested_at` timestamp column

# COMMAND ----------

# TODO: Fill in the WHERE clause and deduplication logic
spark.sql(f"""
    CREATE OR REPLACE TABLE dataops_olympics.default.{TEAM_NAME}_heart_silver AS
    SELECT *, current_timestamp() as ingested_at
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp) as _rn
        FROM dataops_olympics.default.{TEAM_NAME}_heart_bronze
    )
    WHERE _rn = 1
      AND _____   -- TODO: age filter
      AND _____   -- TODO: blood pressure filter
      AND _____   -- TODO: cholesterol filter
""")

silver_count = spark.table(f"dataops_olympics.default.{TEAM_NAME}_heart_silver").count()
bronze_count = spark.table(f"dataops_olympics.default.{TEAM_NAME}_heart_bronze").count()
print(f"Silver: {silver_count} rows (filtered {bronze_count - silver_count} dirty/duplicate rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3B (cont): Gold — Aggregate for Analytics (SQL Path)
# MAGIC
# MAGIC **TODO:** Create a Gold table with heart disease metrics aggregated by age group.
# MAGIC
# MAGIC The Gold table should have these columns:
# MAGIC - `age_group`: "Under 40", "40-49", "50-59", "60+"
# MAGIC - `diagnosis`: "Heart Disease" or "Healthy" (based on `target`)
# MAGIC - `patient_count`: count of patients
# MAGIC - `avg_cholesterol`: average cholesterol
# MAGIC - `avg_blood_pressure`: average resting blood pressure
# MAGIC - `avg_max_heart_rate`: average max heart rate (thalach)

# COMMAND ----------

# TODO: Fill in the Gold aggregation query
spark.sql(f"""
    CREATE OR REPLACE TABLE dataops_olympics.default.{TEAM_NAME}_heart_gold AS
    SELECT
        CASE
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END as age_group,
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END as diagnosis,
        _____  -- TODO: aggregations (COUNT, AVG for chol, trestbps, thalach)
    FROM dataops_olympics.default.{TEAM_NAME}_heart_silver
    GROUP BY 1, 2
    ORDER BY 1, 2
""")

display(spark.table(f"dataops_olympics.default.{TEAM_NAME}_heart_gold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Governance — Add Metadata
# MAGIC
# MAGIC Every production table needs documentation. Add comments so your colleagues
# MAGIC understand what the data means.
# MAGIC
# MAGIC **SDP Path:** Your comments should already be in the pipeline code (`@dlt.table(comment="...")`).
# MAGIC Verify them below.
# MAGIC
# MAGIC **SQL Path:** Add comments with ALTER TABLE.

# COMMAND ----------

# SQL Path: Add governance metadata
# TODO: Fill in meaningful descriptions

spark.sql(f"""
    ALTER TABLE dataops_olympics.default.{TEAM_NAME}_heart_bronze
    SET TBLPROPERTIES ('comment' = '_____')
""")

spark.sql(f"""
    ALTER TABLE dataops_olympics.default.{TEAM_NAME}_heart_silver
    SET TBLPROPERTIES ('comment' = '_____')
""")

# Column comments on Silver
spark.sql(f"ALTER TABLE dataops_olympics.default.{TEAM_NAME}_heart_silver ALTER COLUMN age COMMENT 'Patient age in years (validated 1-120)'")
spark.sql(f"ALTER TABLE dataops_olympics.default.{TEAM_NAME}_heart_silver ALTER COLUMN trestbps COMMENT 'Resting blood pressure in mmHg (validated 50-300)'")
spark.sql(f"ALTER TABLE dataops_olympics.default.{TEAM_NAME}_heart_silver ALTER COLUMN target COMMENT 'Diagnosis: 1 = heart disease present, 0 = healthy'")

print("Governance comments added!")

# COMMAND ----------

# Verify governance
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
# MAGIC # Find your pipeline's event log (replace with your pipeline ID)
# MAGIC display(spark.sql("SELECT * FROM event_log('YOUR_PIPELINE_ID') WHERE event_type = 'flow_progress'"))
# MAGIC ```
# MAGIC
# MAGIC ### SQL Path — Compute DQ metrics manually

# COMMAND ----------

# Data Quality Report (works for both paths — run on your Bronze table)
dq_report = spark.sql(f"""
    SELECT
        COUNT(*) as total_records,
        SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) as null_age_count,
        SUM(CASE WHEN age IS NOT NULL AND age NOT BETWEEN 1 AND 120 THEN 1 ELSE 0 END) as invalid_age_count,
        SUM(CASE WHEN trestbps NOT BETWEEN 50 AND 300 THEN 1 ELSE 0 END) as invalid_bp_count,
        SUM(CASE WHEN chol < 0 THEN 1 ELSE 0 END) as negative_chol_count,
        COUNT(*) - COUNT(DISTINCT event_id) as duplicate_event_ids,
        ROUND(
            (COUNT(*) - SUM(CASE WHEN age IS NULL OR (age NOT BETWEEN 1 AND 120) OR trestbps NOT BETWEEN 50 AND 300 OR chol < 0 THEN 1 ELSE 0 END))
            * 100.0 / COUNT(*), 1
        ) as clean_record_pct
    FROM dataops_olympics.default.{TEAM_NAME}_heart_bronze
""")

print("=" * 60)
print("  DATA QUALITY REPORT")
print("=" * 60)
display(dq_report)

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

# Bronze checks
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

# Silver checks
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

# Gold checks
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

# Governance checks
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
# MAGIC Finished early? Try these with the Databricks Assistant:
# MAGIC
# MAGIC 1. **Create a Genie space** on your Gold table — "Which age group has the highest heart disease rate?"
# MAGIC 2. **Liquid Clustering** — `ALTER TABLE ... CLUSTER BY (age, target)`
# MAGIC 3. **Change Data Feed** — Enable CDF on Silver: `ALTER TABLE ... SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`
# MAGIC 4. **Time Travel** — Overwrite Silver with bad data, then `RESTORE TABLE ... TO VERSION AS OF 0`
# MAGIC 5. **AI Functions** — `SELECT ai_classify(chol, ARRAY('Normal', 'Borderline', 'High')) FROM ...`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answers
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answers (try without peeking first!)</summary>
# MAGIC
# MAGIC **Step 1 (Bronze):**
# MAGIC ```python
# MAGIC df_bronze = spark.read.json("/Volumes/dataops_olympics/default/raw_data/heart_events/*.json")
# MAGIC ```
# MAGIC
# MAGIC **Step 3B Silver WHERE clause:**
# MAGIC ```sql
# MAGIC AND age IS NOT NULL AND age BETWEEN 1 AND 120
# MAGIC AND trestbps BETWEEN 50 AND 300
# MAGIC AND chol >= 0
# MAGIC ```
# MAGIC
# MAGIC **Step 3B Gold aggregations:**
# MAGIC ```sql
# MAGIC COUNT(*) as patient_count,
# MAGIC ROUND(AVG(chol), 1) as avg_cholesterol,
# MAGIC ROUND(AVG(trestbps), 1) as avg_blood_pressure,
# MAGIC ROUND(AVG(thalach), 1) as avg_max_heart_rate
# MAGIC ```
# MAGIC
# MAGIC </details>
