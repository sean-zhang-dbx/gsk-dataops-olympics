# Databricks notebook source
# MAGIC %md
# MAGIC # SDP Pipeline Template — Heart Disease Intake Events
# MAGIC
# MAGIC **This notebook runs as a Spark Declarative Pipeline (SDP), NOT interactively.**
# MAGIC
# MAGIC ### How to Use
# MAGIC 1. Use the **Databricks Assistant** (`Cmd+I`) to fill in the code cells below
# MAGIC 2. Go to **Workflows → Pipelines → Create Pipeline**
# MAGIC    - Pipeline name: `{TEAM_NAME}_heart_pipeline`
# MAGIC    - Source: select **this notebook**
# MAGIC    - Destination: catalog = `dataops_olympics`, schema = `default`
# MAGIC 3. Click **Start** to run the pipeline
# MAGIC
# MAGIC ### What You'll Build
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
# MAGIC ### Choose Python or SQL
# MAGIC
# MAGIC You can implement each layer in **Python or SQL** — pick whichever you prefer!
# MAGIC Both work in the same pipeline notebook.
# MAGIC
# MAGIC **Python API reference:**
# MAGIC - Streaming table: `@dp.table` with `spark.readStream`
# MAGIC - Materialized view: `@dp.materialized_view` with `spark.table()`
# MAGIC - Expectations: `@dp.expect_or_drop("name", "constraint")`
# MAGIC - Read upstream: `spark.readStream.table("table_name")` or `spark.table("table_name")`
# MAGIC
# MAGIC **SQL reference:**
# MAGIC - Streaming table: `CREATE OR REFRESH STREAMING TABLE name AS SELECT * FROM STREAM(...)`
# MAGIC - Materialized view: `CREATE OR REFRESH MATERIALIZED VIEW name AS SELECT ...`
# MAGIC - Expectations: `CONSTRAINT name EXPECT (expr) ON VIOLATION DROP ROW`
# MAGIC - Read upstream: `STREAM(table_name)` for streaming, or just `table_name` for batch

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Bronze — Streaming Table (Raw Ingestion via Auto Loader)
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create a **streaming table** called `heart_bronze` that ingests all NDJSON files
# MAGIC > from `/Volumes/dataops_olympics/default/raw_data/heart_events/` using Auto Loader
# MAGIC > (`cloudFiles` format).
# MAGIC >
# MAGIC > No transformations — this is the raw landing zone. Auto Loader will automatically
# MAGIC > detect and process new files as they arrive.
# MAGIC >
# MAGIC > Add a table comment: "Raw patient intake events from hospital EHR system"
# MAGIC
# MAGIC ### Python Hint
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC
# MAGIC @dp.table(comment="...")
# MAGIC def heart_bronze():
# MAGIC     return spark.readStream.format("cloudFiles").option(...).load(...)
# MAGIC ```
# MAGIC
# MAGIC ### SQL Hint
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE heart_bronze
# MAGIC COMMENT '...'
# MAGIC AS SELECT * FROM STREAM read_files('...', format => 'json')
# MAGIC ```

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!
# Choose Python or SQL. Delete whichever you don't use.


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Silver — Streaming Table (Cleaned + Deduplicated)
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create a **streaming table** called `heart_silver` that reads from `heart_bronze`
# MAGIC > as a stream and applies data quality rules:
# MAGIC >
# MAGIC > **Expectations (drop invalid rows):**
# MAGIC > - `valid_age` — age must not be NULL and must be between 1 and 120
# MAGIC > - `valid_blood_pressure` — trestbps must be between 50 and 300
# MAGIC > - `non_negative_cholesterol` — chol must be >= 0
# MAGIC >
# MAGIC > **Expectation (warn only, keep rows):**
# MAGIC > - `has_event_id` — event_id must not be NULL
# MAGIC >
# MAGIC > **Deduplication:**
# MAGIC > - Drop duplicate rows based on `event_id`
# MAGIC >
# MAGIC > **Additional column:**
# MAGIC > - Add `ingested_at` with the current timestamp
# MAGIC >
# MAGIC > Table comment: "Cleaned patient intake data — validated and deduplicated on event_id"
# MAGIC
# MAGIC ### Python Hint
# MAGIC ```python
# MAGIC @dp.table(comment="...")
# MAGIC @dp.expect_or_drop("name", "constraint")
# MAGIC @dp.expect("name", "constraint")
# MAGIC def heart_silver():
# MAGIC     return spark.readStream.table("heart_bronze").dropDuplicates([...])...
# MAGIC ```
# MAGIC
# MAGIC ### SQL Hint
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE heart_silver (
# MAGIC   CONSTRAINT valid_age EXPECT (age IS NOT NULL AND age BETWEEN 1 AND 120) ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT ... EXPECT (...) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT '...'
# MAGIC AS SELECT *, current_timestamp() AS ingested_at
# MAGIC FROM STREAM(heart_bronze)
# MAGIC ```

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Gold — Materialized View (Aggregated for Analytics)
# MAGIC
# MAGIC ### Business Requirement
# MAGIC
# MAGIC > Create a **materialized view** called `heart_gold` that reads from `heart_silver`
# MAGIC > (non-streaming read) and aggregates heart disease metrics by age group and diagnosis:
# MAGIC >
# MAGIC > | Column | Definition |
# MAGIC > |--------|-----------|
# MAGIC > | `age_group` | Bucket ages: "Under 40", "40-49", "50-59", "60+" |
# MAGIC > | `diagnosis` | Map `target`: 1 = "Heart Disease", 0 = "Healthy" |
# MAGIC > | `patient_count` | Count of patients in each group |
# MAGIC > | `avg_cholesterol` | Average of `chol`, rounded to 1 decimal |
# MAGIC > | `avg_blood_pressure` | Average of `trestbps`, rounded to 1 decimal |
# MAGIC > | `avg_max_heart_rate` | Average of `thalach`, rounded to 1 decimal |
# MAGIC >
# MAGIC > Table comment: "Heart disease metrics by age group — materialized view for dashboards and Genie"
# MAGIC
# MAGIC ### Python Hint
# MAGIC ```python
# MAGIC @dp.materialized_view(comment="...")
# MAGIC def heart_gold():
# MAGIC     return spark.table("heart_silver").withColumn(...).groupBy(...).agg(...)
# MAGIC ```
# MAGIC
# MAGIC ### SQL Hint
# MAGIC ```sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW heart_gold
# MAGIC COMMENT '...'
# MAGIC AS SELECT
# MAGIC   CASE WHEN age < 40 THEN 'Under 40' ... END AS age_group,
# MAGIC   ...
# MAGIC FROM heart_silver
# MAGIC GROUP BY ...
# MAGIC ```

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I) to generate!


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Configure the pipeline** — Go to **Workflows → Pipelines → Create Pipeline**
# MAGIC    - Source: this notebook
# MAGIC    - Destination catalog: `dataops_olympics`, schema: `default`
# MAGIC 2. **Validate first** — Click the dropdown next to Start and select **Validate** to dry-run
# MAGIC 3. **Start the pipeline** — Click **Start** to execute
# MAGIC 4. **Check data quality** — The Pipeline UI shows expectation pass/fail metrics automatically
# MAGIC 5. **Return to the starter notebook** to complete governance and validation steps
