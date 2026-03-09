# Databricks notebook source
# MAGIC %md
# MAGIC # Lightning Talk 1: Data Engineering — SDP & Delta Lake
# MAGIC
# MAGIC **Duration: 15 minutes** | Instructor-led live demo
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Agenda
# MAGIC | Time | Section | What You'll See |
# MAGIC |------|---------|----------------|
# MAGIC | 0:00 | **The Problem** | Why pipelines matter in pharma |
# MAGIC | 1:00 | **Ingest** | CSV + JSON → Spark DataFrames from UC Volumes |
# MAGIC | 4:00 | **Delta Lake** | Write to Delta, explain ACID + time travel |
# MAGIC | 7:00 | **Governance** | Table & column comments in Unity Catalog |
# MAGIC | 9:00 | **SDP Pipeline** | Bronze → Silver → Gold with Spark Declarative Pipelines |
# MAGIC | 13:00 | **Wow Moment** | Data quality expectations + time travel |
# MAGIC | 14:00 | **Your Turn** | Preview of the practice notebook |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Problem
# MAGIC
# MAGIC A new clinical trial dataset just landed — a messy CSV with 500 patient records.
# MAGIC The goal: get it into a **governed, queryable table** that the analytics team can trust.
# MAGIC That's what we'll build in the next 14 minutes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Lives in Unity Catalog Volumes
# MAGIC
# MAGIC Where does raw data live? In Databricks, we use **Unity Catalog Volumes** — governed cloud
# MAGIC storage that's part of your catalog. No more dumping files in `/tmp` or random local paths.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dataops_olympics;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's see what's in our Volume
# MAGIC LIST '/Volumes/dataops_olympics/default/raw_data/heart_disease/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Raw Data (CSV from Volume)
# MAGIC
# MAGIC Step one — read the file. Spark handles CSV, JSON, Parquet, and more.
# MAGIC Notice the path starts with `/Volumes` — that's governed storage.

# COMMAND ----------

csv_path = "/Volumes/dataops_olympics/default/raw_data/heart_disease/heart.csv"

df_heart = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(csv_path))

print(f"Loaded {df_heart.count()} rows x {len(df_heart.columns)} columns")
display(df_heart.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC That's it — one line. Spark inferred all the data types automatically (age → integer,
# MAGIC cholesterol → integer, oldpeak → double). No pandas, no manual parsing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Delta Lake
# MAGIC
# MAGIC We don't save as CSV or Parquet — we save as **Delta Lake**.
# MAGIC Why? Three words: **ACID transactions, time travel, schema enforcement.**
# MAGIC Your table will never be in a half-written, corrupted state.

# COMMAND ----------

df_heart.write.format("delta").mode("overwrite").saveAsTable("dataops_olympics.default.demo_heart_bronze")
print("Saved as Delta table: demo_heart_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL dataops_olympics.default.demo_heart_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the format says `delta`. This table now supports versioning,
# MAGIC ACID transactions, and schema evolution. Every write creates a new version.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Governance — Document Your Data
# MAGIC
# MAGIC Data without documentation is data nobody trusts. Unity Catalog lets you add comments
# MAGIC right on the table and columns — this is how a data engineer earns the trust of the analytics team.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dataops_olympics.default.demo_heart_bronze SET TBLPROPERTIES (
# MAGIC   'comment' = 'UCI Heart Disease dataset: 500 patients, 14 clinical features, target=1 means disease present'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE dataops_olympics.default.demo_heart_bronze ALTER COLUMN age COMMENT 'Patient age in years (29-77)';
# MAGIC ALTER TABLE dataops_olympics.default.demo_heart_bronze ALTER COLUMN chol COMMENT 'Serum cholesterol in mg/dL';
# MAGIC ALTER TABLE dataops_olympics.default.demo_heart_bronze ALTER COLUMN target COMMENT 'Diagnosis: 1 = heart disease, 0 = healthy';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE dataops_olympics.default.demo_heart_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Every column now has a description. When a colleague opens this table next month,
# MAGIC they know exactly what `target` means without asking you.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Spark Declarative Pipelines — The Real Thing
# MAGIC
# MAGIC The modern approach to medallion pipelines: instead of writing imperative Spark code,
# MAGIC we **declare** our tables and let Databricks handle everything.
# MAGIC This is **Spark Declarative Pipelines (SDP)**.
# MAGIC
# MAGIC We have a separate pipeline notebook (`demo1_sdp_pipeline`) that defines
# MAGIC Bronze → Silver → Gold. Let's create and run it right now.

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Pipeline Notebook (`demo1_sdp_pipeline`)
# MAGIC
# MAGIC Open the `demo1_sdp_pipeline` notebook in this folder to see the full code. Here's the gist:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC # BRONZE — Auto Loader streams NDJSON from the Volume
# MAGIC @dp.table(comment="Raw patient intake events — Auto Loader ingestion from NDJSON")
# MAGIC def demo_sdp_bronze():
# MAGIC     return (spark.readStream.format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .option("cloudFiles.inferColumnTypes", "true")
# MAGIC         .load("/Volumes/dataops_olympics/default/raw_data/heart_events/"))
# MAGIC
# MAGIC # SILVER — Data quality expectations drop bad rows automatically
# MAGIC @dp.table(comment="Cleaned patient data — validated and deduplicated on event_id")
# MAGIC @dp.expect_or_drop("valid_age", "age IS NOT NULL AND age BETWEEN 1 AND 120")
# MAGIC @dp.expect_or_drop("valid_blood_pressure", "trestbps BETWEEN 50 AND 300")
# MAGIC @dp.expect("non_negative_cholesterol", "chol >= 0")
# MAGIC def demo_sdp_silver():
# MAGIC     return (spark.readStream.table("demo_sdp_bronze")
# MAGIC         .dropDuplicates(["event_id"])
# MAGIC         .withColumn("ingested_at", current_timestamp()))
# MAGIC
# MAGIC # GOLD — Batch aggregation by age group and diagnosis
# MAGIC @dp.table(comment="Heart disease metrics by age group — for dashboards and Genie")
# MAGIC def demo_sdp_gold():
# MAGIC     return (spark.read.table("demo_sdp_silver")
# MAGIC         .withColumn("age_group", ...)
# MAGIC         .groupBy("age_group", "diagnosis")
# MAGIC         .agg(count("*").alias("patient_count"), ...))
# MAGIC ```
# MAGIC
# MAGIC Three things to notice:
# MAGIC 1. `@dp.table` + `readStream` creates **streaming tables** (Bronze/Silver)
# MAGIC 2. `@dp.expect_or_drop` enforces data quality — invalid rows are dropped automatically
# MAGIC 3. **No `.write()` calls** — the pipeline handles all orchestration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create & Start the Pipeline

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary

w = WorkspaceClient()

_cwd = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
_pipeline_notebook = f"{_cwd}/demo1_sdp_pipeline"

for p in w.pipelines.list_pipelines():
    if p.name == "demo_sdp_heart_pipeline":
        w.pipelines.delete(pipeline_id=p.pipeline_id)
        print(f"Cleaned up previous demo pipeline: {p.pipeline_id}")

pipeline = w.pipelines.create(
    name="demo_sdp_heart_pipeline",
    catalog="dataops_olympics",
    target="default",
    libraries=[PipelineLibrary(notebook=NotebookLibrary(path=_pipeline_notebook))],
    development=True,
    serverless=True,
)
_pipeline_id = pipeline.pipeline_id
print(f"Pipeline created: {_pipeline_id}")
print(f"Notebook: {_pipeline_notebook}")

# COMMAND ----------

w.pipelines.start_update(pipeline_id=_pipeline_id, full_refresh=True)
print("Pipeline started!")
print("Open Workflows → Pipelines → 'demo_sdp_heart_pipeline' to watch the DAG run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wait for Pipeline to Complete
# MAGIC
# MAGIC This typically takes 1–3 minutes. Check the pipeline UI for the DAG visualization,
# MAGIC data quality metrics, and lineage. Run the cell below to poll for completion.

# COMMAND ----------

import time

for i in range(40):
    pipe = w.pipelines.get(pipeline_id=_pipeline_id)
    state = pipe.state.value if pipe.state else "UNKNOWN"
    if state == "IDLE":
        print("Pipeline completed successfully!")
        break
    elif state == "FAILED":
        print("Pipeline FAILED — check the pipeline UI for details.")
        break
    else:
        if i % 2 == 0:
            print(f"  Pipeline state: {state}...")
        time.sleep(10)
else:
    print("Still running — check the pipeline UI or re-run this cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### SDP Results — Query the Pipeline Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM dataops_olympics.default.demo_sdp_bronze) as bronze_rows,
# MAGIC   (SELECT COUNT(*) FROM dataops_olympics.default.demo_sdp_silver) as silver_rows

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dataops_olympics.default.demo_sdp_gold ORDER BY age_group, diagnosis

# COMMAND ----------

# MAGIC %md
# MAGIC Three tables: **Raw → Clean → Aggregated.** That's the Medallion Architecture built with SDP.
# MAGIC Databricks managed dependencies, data quality enforcement, and orchestration —
# MAGIC check the pipeline UI for the DAG, quality metrics, and lineage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Wow Moment — Delta Time Travel
# MAGIC
# MAGIC Here's the magic trick. What if someone accidentally overwrites our Bronze table with bad data?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Oops — someone runs this by accident
# MAGIC INSERT OVERWRITE dataops_olympics.default.demo_heart_bronze
# MAGIC SELECT * FROM dataops_olympics.default.demo_heart_bronze WHERE age < 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as current_rows FROM dataops_olympics.default.demo_heart_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC The table is empty — disaster. But with Delta Lake, every version is saved. Watch this...

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY dataops_olympics.default.demo_heart_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE dataops_olympics.default.demo_heart_bronze TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as restored_rows FROM dataops_olympics.default.demo_heart_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC One command: `RESTORE TABLE`. Data is back. No backup needed, no panicking. That's Delta Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dataops_olympics.default.demo_heart_bronze;

# COMMAND ----------

try:
    w.pipelines.delete(pipeline_id=_pipeline_id)
    print(f"Deleted pipeline: {_pipeline_id}")
except Exception as e:
    print(f"Pipeline cleanup: {e}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dataops_olympics.default.demo_sdp_bronze;
# MAGIC DROP TABLE IF EXISTS dataops_olympics.default.demo_sdp_silver;
# MAGIC DROP TABLE IF EXISTS dataops_olympics.default.demo_sdp_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Turn!
# MAGIC
# MAGIC In the practice notebook, you'll write a simple Bronze → Silver pipeline — fill in 4 blanks, ~10 minutes.
# MAGIC Then in the competition, you'll build the full Medallion with SDP and data quality expectations.
# MAGIC The tables you create here flow into every subsequent event — analytics, ML, and GenAI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Unity Catalog Volumes** — governed cloud storage for raw files (`/Volumes/catalog/schema/volume/`)
# MAGIC 2. **Spark reads any format** — CSV, JSON, Parquet, etc. with one line
# MAGIC 3. **Delta Lake** = ACID transactions + time travel + schema enforcement
# MAGIC 4. **Unity Catalog governance** = table/column comments so your data is self-documenting
# MAGIC 5. **Spark Declarative Pipelines (SDP)** = declare tables + built-in data quality expectations
# MAGIC 6. **Medallion Architecture** = Bronze (raw) → Silver (clean) → Gold (aggregated)
# MAGIC 7. **Use the Databricks Assistant** (`Cmd+I`) to generate all of this code from English prompts
