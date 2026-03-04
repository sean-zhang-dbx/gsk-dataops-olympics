# Databricks notebook source
# MAGIC %md
# MAGIC # SDP Pipeline — Heart Disease Intake Events
# MAGIC
# MAGIC **Your goal: create ONE file (`pipeline.py` or `pipeline.sql`) containing the entire pipeline.**
# MAGIC
# MAGIC This notebook is your **instructions sheet**. Read the requirements below, then:
# MAGIC
# MAGIC 1. **Create a new notebook** called `pipeline` (Python or SQL — your choice)
# MAGIC 2. Use the **Databricks Assistant** (`Cmd+I`) to generate all the code in that single file
# MAGIC 3. Go to **Workflows → Pipelines → Create Pipeline**
# MAGIC    - Pipeline name: `{TEAM_NAME}_heart_pipeline`
# MAGIC    - Source: select your `pipeline` notebook
# MAGIC    - Destination: catalog = `dataops_olympics`, schema = `default`
# MAGIC 4. Click **Validate** (dry-run) → then **Start**
# MAGIC 5. Return to the **starter notebook** for governance and validation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pipeline Architecture
# MAGIC
# MAGIC ```
# MAGIC  NDJSON Files          Bronze                Silver                 Gold
# MAGIC ┌──────────┐     ┌───────────────┐    ┌────────────────┐    ┌──────────────────┐
# MAGIC │ 5 batch  │────>│ STREAMING     │───>│ STREAMING      │───>│ MATERIALIZED     │
# MAGIC │ files    │     │ TABLE         │    │ TABLE          │    │ VIEW             │
# MAGIC │ ~500 rows│     │ (Auto Loader) │    │ (DQ + Dedup)   │    │ (Aggregated)     │
# MAGIC └──────────┘     └───────────────┘    └────────────────┘    └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Layer 1: Bronze — Streaming Table (Raw Ingestion)
# MAGIC
# MAGIC > Create a **streaming table** called `heart_bronze` that ingests all NDJSON files
# MAGIC > from `/Volumes/dataops_olympics/default/raw_data/heart_events/` using Auto Loader.
# MAGIC >
# MAGIC > No transformations — Bronze is the raw landing zone.
# MAGIC >
# MAGIC > Table comment: *"Raw patient intake events from hospital EHR system"*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Layer 2: Silver — Streaming Table (Cleaned + Deduplicated)
# MAGIC
# MAGIC > Create a **streaming table** called `heart_silver` that reads from `heart_bronze`
# MAGIC > as a stream and applies these rules:
# MAGIC >
# MAGIC > **Expectations (drop invalid rows):**
# MAGIC > - `valid_age` — age must not be NULL and must be between 1 and 120
# MAGIC > - `valid_blood_pressure` — trestbps must be between 50 and 300
# MAGIC > - `non_negative_cholesterol` — chol must be >= 0
# MAGIC >
# MAGIC > **Expectation (warn only):**
# MAGIC > - `has_event_id` — event_id must not be NULL
# MAGIC >
# MAGIC > **Deduplication:** Drop duplicate rows on `event_id`
# MAGIC >
# MAGIC > **Extra column:** Add `ingested_at` = current timestamp
# MAGIC >
# MAGIC > Table comment: *"Cleaned patient intake data — validated and deduplicated on event_id"*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Layer 3: Gold — Materialized View (Aggregated)
# MAGIC
# MAGIC > Create a **materialized view** called `heart_gold` from `heart_silver`
# MAGIC > (non-streaming read). Aggregate by age group and diagnosis:
# MAGIC >
# MAGIC > | Column | Definition |
# MAGIC > |--------|-----------|
# MAGIC > | `age_group` | "Under 40", "40-49", "50-59", "60+" |
# MAGIC > | `diagnosis` | target 1 = "Heart Disease", 0 = "Healthy" |
# MAGIC > | `patient_count` | Count per group |
# MAGIC > | `avg_cholesterol` | Average of `chol`, round to 1 decimal |
# MAGIC > | `avg_blood_pressure` | Average of `trestbps`, round to 1 decimal |
# MAGIC > | `avg_max_heart_rate` | Average of `thalach`, round to 1 decimal |
# MAGIC >
# MAGIC > Table comment: *"Heart disease metrics by age group — materialized view for dashboards and Genie"*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SDP Syntax Reference
# MAGIC
# MAGIC ### Python (`from pyspark import pipelines as dp`)
# MAGIC
# MAGIC | What | Syntax |
# MAGIC |------|--------|
# MAGIC | Streaming table | `@dp.table(comment="...")` with `spark.readStream` |
# MAGIC | Materialized view | `@dp.materialized_view(comment="...")` with `spark.table()` |
# MAGIC | Auto Loader | `spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load(path)` |
# MAGIC | Read upstream (stream) | `spark.readStream.table("table_name")` |
# MAGIC | Read upstream (batch) | `spark.table("table_name")` |
# MAGIC | Expect + drop | `@dp.expect_or_drop("name", "SQL constraint")` |
# MAGIC | Expect + warn | `@dp.expect("name", "SQL constraint")` |
# MAGIC
# MAGIC ### SQL
# MAGIC
# MAGIC | What | Syntax |
# MAGIC |------|--------|
# MAGIC | Streaming table | `CREATE OR REFRESH STREAMING TABLE name ...` |
# MAGIC | Materialized view | `CREATE OR REFRESH MATERIALIZED VIEW name ...` |
# MAGIC | Auto Loader | `SELECT * FROM STREAM read_files(path, format => 'json')` |
# MAGIC | Read upstream (stream) | `FROM STREAM(table_name)` |
# MAGIC | Read upstream (batch) | `FROM table_name` |
# MAGIC | Expect + drop | `CONSTRAINT name EXPECT (expr) ON VIOLATION DROP ROW` |
# MAGIC | Expect + warn | `CONSTRAINT name EXPECT (expr)` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Docs
# MAGIC - [SDP Programming Guide (Spark)](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
# MAGIC - [Transform Data with Pipelines (Databricks)](https://docs.databricks.com/aws/en/ldp/transform)
# MAGIC - [Pipeline Expectations (Databricks)](https://docs.databricks.com/aws/en/ldp/expectations)
