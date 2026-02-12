# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ Bonus Challenges: Modern Databricks Features
# MAGIC
# MAGIC These are **optional bonus challenges** teams can tackle for extra credit,
# MAGIC or use as standalone learning exercises. Each section is independent.
# MAGIC
# MAGIC ### Feature Highlights
# MAGIC | Feature | What It Does | Section |
# MAGIC |---------|-------------|---------|
# MAGIC | **Liquid Clustering** | Replaces partitioning + Z-ordering | Section 1 |
# MAGIC | **Change Data Feed** | Track row-level changes in Delta | Section 2 |
# MAGIC | **Unity Catalog Volumes** | Managed file storage | Section 3 |
# MAGIC | **Predictive Optimization** | Auto-optimize tables | Section 4 |
# MAGIC | **AI Functions** | LLM calls directly in SQL | Section 5 |
# MAGIC | **Delta Sharing** | Cross-org data sharing | Section 6 |
# MAGIC
# MAGIC **Bonus Points**: 2 pts per completed section (max 12 bonus pts)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 1: Liquid Clustering
# MAGIC
# MAGIC **Liquid Clustering** replaces traditional partitioning and Z-ordering with a
# MAGIC single, adaptive optimization strategy. It automatically reorganizes data layout
# MAGIC based on your query patterns — no manual tuning needed.
# MAGIC
# MAGIC ### Why it matters
# MAGIC - No more choosing partition columns upfront
# MAGIC - No more running `OPTIMIZE ... ZORDER BY ...`
# MAGIC - Adapts as query patterns change
# MAGIC - Works with any column combination

# COMMAND ----------

# Load heart disease data
df_heart = spark.table("heart_disease")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Traditional Approach (OLD)
# MAGIC ```sql
# MAGIC -- Old way: static partitioning + manual Z-ordering
# MAGIC CREATE TABLE heart_disease_old
# MAGIC PARTITIONED BY (age_group)
# MAGIC AS SELECT * FROM heart_disease;
# MAGIC
# MAGIC OPTIMIZE heart_disease_old ZORDER BY (chol, trestbps);
# MAGIC -- Problem: If queries change to filter by 'sex', need to re-ZORDER!
# MAGIC ```
# MAGIC
# MAGIC ### Liquid Clustering Approach (NEW)

# COMMAND ----------

# TODO: Create a table with Liquid Clustering
# Hint: Use CLUSTER BY instead of PARTITION BY

TEAM_NAME = "_____"  # Your team name

spark.sql(f"""
    CREATE OR REPLACE TABLE {TEAM_NAME}_heart_lc
    CLUSTER BY (age, chol, trestbps)
    AS SELECT * FROM heart_disease
""")

print("✅ Liquid Clustered table created!")

# COMMAND ----------

# Verify clustering
spark.sql(f"DESCRIBE DETAIL {TEAM_NAME}_heart_lc").display()

# COMMAND ----------

# Run OPTIMIZE — Liquid Clustering handles the rest
spark.sql(f"OPTIMIZE {TEAM_NAME}_heart_lc")
print("✅ Table optimized with Liquid Clustering")

# COMMAND ----------

# Compare query performance (conceptual — small data won't show big difference)
# On large datasets, liquid clustering dramatically reduces data scanning

spark.sql(f"""
    SELECT age, AVG(chol) as avg_chol, COUNT(*) as cnt
    FROM {TEAM_NAME}_heart_lc
    WHERE age BETWEEN 50 AND 60
      AND chol > 200
    GROUP BY age
    ORDER BY age
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### What makes Liquid Clustering special?
# MAGIC
# MAGIC 1. **Incremental**: Only reorganizes new/modified data, not the whole table
# MAGIC 2. **Adaptive**: Query patterns inform clustering decisions
# MAGIC 3. **Zero tuning**: No partition pruning, no ZORDER column selection
# MAGIC 4. **Compatible**: Works with all Delta features (time travel, CDF, etc.)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 2: Change Data Feed (CDF)
# MAGIC
# MAGIC **Change Data Feed** tracks row-level changes (inserts, updates, deletes)
# MAGIC in a Delta table. Essential for building **incremental ETL pipelines**.

# COMMAND ----------

# Create a CDF-enabled table
spark.sql(f"""
    CREATE OR REPLACE TABLE {TEAM_NAME}_heart_cdf
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    AS SELECT * FROM heart_disease
""")

# Check current version
version_before = spark.sql(f"DESCRIBE HISTORY {TEAM_NAME}_heart_cdf").select("version").first()[0]
print(f"✅ CDF-enabled table created at version {version_before}")

# COMMAND ----------

# Make some changes to generate CDF records

# Update: Increase cholesterol for patients > 70
spark.sql(f"""
    UPDATE {TEAM_NAME}_heart_cdf
    SET chol = chol + 10
    WHERE age > 70
""")

# Delete: Remove records with suspicious data
spark.sql(f"""
    DELETE FROM {TEAM_NAME}_heart_cdf
    WHERE trestbps > 250 OR chol > 500
""")

# Insert: Add new records
spark.sql(f"""
    INSERT INTO {TEAM_NAME}_heart_cdf
    VALUES (55, 1, 2, 130, 250, 0, 1, 160, 0, 1.5, 1, 0, 3, 0)
""")

print("✅ Changes applied (UPDATE, DELETE, INSERT)")

# COMMAND ----------

# TODO: Read the Change Data Feed to see what changed!
# This is the key feature — you can see exactly what rows were modified

version_after = spark.sql(f"DESCRIBE HISTORY {TEAM_NAME}_heart_cdf").select("version").first()[0]

df_changes = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", version_before + 1)
    .table(f"{TEAM_NAME}_heart_cdf")
)

print(f"Changes between version {version_before} and {version_after}:")
print(f"Total change records: {df_changes.count()}")

# Show the change types
df_changes.groupBy("_change_type").count().display()

# COMMAND ----------

# View actual changes
print("📝 Sample changes:")
display(df_changes.select("age", "chol", "trestbps", "target", "_change_type", "_commit_version", "_commit_timestamp").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDF Use Cases
# MAGIC - **Incremental ETL**: Only process new/changed rows downstream
# MAGIC - **Audit logging**: Track who changed what and when
# MAGIC - **CDC replication**: Replicate changes to other systems
# MAGIC - **ML feature freshness**: Detect when training data changes

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 3: Unity Catalog Volumes
# MAGIC
# MAGIC **Volumes** provide managed file storage within Unity Catalog.
# MAGIC Think of them as cloud storage paths governed by UC permissions.
# MAGIC
# MAGIC No more remembering `/mnt/`, `/dbfs/`, or `wasbs://` paths!

# COMMAND ----------

# TODO: Create a Volume to store raw data files
# Note: This requires Unity Catalog to be available

try:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS dataops_olympics.competition.raw_data
        COMMENT 'Raw data files for DataOps Olympics competition'
    """)
    print("✅ Volume created: dataops_olympics.competition.raw_data")
    VOLUME_PATH = "/Volumes/dataops_olympics/competition/raw_data"
    
    # Copy data files to the volume
    dbutils.fs.cp("file:/tmp/dataops_olympics/raw/heart_disease/heart.csv", 
                   f"{VOLUME_PATH}/heart_disease.csv")
    print(f"✅ Data copied to Volume")
    
    # List files in volume
    display(dbutils.fs.ls(VOLUME_PATH))
    
except Exception as e:
    print(f"⚠️  Unity Catalog Volumes not available: {str(e)[:100]}")
    print("   This feature requires Unity Catalog. Skipping...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Volume Benefits
# MAGIC - **Governed**: Permissions managed through Unity Catalog
# MAGIC - **Discoverable**: Browse files in Catalog Explorer
# MAGIC - **Portable**: Standard `/Volumes/catalog/schema/volume/` paths
# MAGIC - **Shareable**: Works with Delta Sharing

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 4: Predictive Optimization
# MAGIC
# MAGIC **Predictive Optimization** automatically runs OPTIMIZE and VACUUM on your
# MAGIC Delta tables based on usage patterns. Zero manual maintenance.

# COMMAND ----------

# Enable predictive optimization on a table
# Note: requires Unity Catalog managed tables

try:
    spark.sql(f"""
        ALTER TABLE {TEAM_NAME}_heart_lc
        SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)
    """)
    print("✅ Deletion vectors enabled (foundation for predictive optimization)")
    
    # Predictive optimization is typically enabled at the catalog/schema level:
    # ALTER SCHEMA dataops_olympics.competition ENABLE PREDICTIVE OPTIMIZATION;
    print("\nTo enable predictive optimization at the schema level:")
    print("ALTER SCHEMA dataops_olympics.competition ENABLE PREDICTIVE OPTIMIZATION;")
    
except Exception as e:
    print(f"⚠️  Feature not available: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 5: AI Functions in SQL
# MAGIC
# MAGIC Databricks **AI Functions** let you call LLMs directly from SQL.
# MAGIC Available functions: `ai_query()`, `ai_generate()`, `ai_classify()`, `ai_extract()`, etc.
# MAGIC
# MAGIC > ⚠️ Availability depends on your workspace configuration and model endpoints.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Classify drug reviews using AI
# MAGIC
# MAGIC ```sql
# MAGIC -- Classify sentiment of drug reviews using AI
# MAGIC SELECT
# MAGIC     drug_name,
# MAGIC     review,
# MAGIC     ai_classify(review, ARRAY('positive', 'negative', 'neutral')) as sentiment
# MAGIC FROM drug_reviews
# MAGIC LIMIT 10;
# MAGIC ```

# COMMAND ----------

# Try AI Functions (may not be available on Free Edition)
try:
    result = spark.sql("""
        SELECT 
            drug_name,
            review,
            ai_classify(review, ARRAY('positive', 'negative', 'neutral')) as sentiment
        FROM drug_reviews
        LIMIT 5
    """)
    display(result)
    print("✅ AI Functions available!")
except Exception as e:
    print(f"⚠️  AI Functions not available: {str(e)[:100]}")
    print("\nFallback: Use Python-based sentiment analysis instead")
    
    # Fallback: Simple keyword-based sentiment
    from pyspark.sql import functions as F
    
    df_reviews = spark.table("drug_reviews")
    df_sentiment = (
        df_reviews
        .withColumn("sentiment",
            F.when(F.col("rating") >= 7, "positive")
             .when(F.col("rating") <= 3, "negative")
             .otherwise("neutral"))
        .select("drug_name", "review", "rating", "sentiment")
    )
    display(df_sentiment.limit(10))
    print("✅ Fallback sentiment classification based on rating")

# COMMAND ----------

# MAGIC %md
# MAGIC ### More AI Function Examples
# MAGIC
# MAGIC ```sql
# MAGIC -- Extract entities from clinical notes
# MAGIC SELECT
# MAGIC     patient_id,
# MAGIC     ai_extract(text, ARRAY('medication', 'condition', 'dosage')) as entities
# MAGIC FROM clinical_notes;
# MAGIC
# MAGIC -- Summarize clinical notes
# MAGIC SELECT
# MAGIC     patient_id,
# MAGIC     ai_query(
# MAGIC         'databricks-meta-llama-3-3-70b-instruct',
# MAGIC         CONCAT('Summarize this clinical note in 2 sentences: ', text)
# MAGIC     ) as summary
# MAGIC FROM clinical_notes;
# MAGIC
# MAGIC -- Generate SQL from natural language
# MAGIC SELECT ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     'Write a SQL query to find the top 5 drugs with the highest average rating from the drug_reviews table'
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 6: Delta Time Travel & Reproducibility
# MAGIC
# MAGIC Delta Lake maintains a full history of every change. You can query
# MAGIC any past version of a table — essential for ML reproducibility and auditing.

# COMMAND ----------

# View table history
print("📜 Table History:")
display(spark.sql(f"DESCRIBE HISTORY {TEAM_NAME}_heart_cdf"))

# COMMAND ----------

# Query a specific version of the table
spark.sql(f"""
    SELECT COUNT(*) as record_count, AVG(age) as avg_age
    FROM {TEAM_NAME}_heart_cdf VERSION AS OF 0
""").display()

print("↑ Table at Version 0 (original)")

spark.sql(f"""
    SELECT COUNT(*) as record_count, AVG(age) as avg_age
    FROM {TEAM_NAME}_heart_cdf
""").display()

print("↑ Table at current version (after changes)")

# COMMAND ----------

# Query by timestamp
spark.sql(f"""
    SELECT COUNT(*) as record_count
    FROM {TEAM_NAME}_heart_cdf TIMESTAMP AS OF '2025-01-01'
""")

# This is critical for ML: "What data was the model trained on?"
print("""
Use cases:
  - Audit: "What did the table look like last Tuesday?"
  - ML: "Reproduce the exact training dataset from 3 months ago"
  - Recovery: "Restore the table to before the bad update"
  
  -- Restore command:
  RESTORE TABLE my_table TO VERSION AS OF 2;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Bonus Challenges Scorecard

# COMMAND ----------

print("=" * 60)
print("BONUS CHALLENGES SCORECARD")
print("=" * 60)

sections = {
    "Section 1 - Liquid Clustering": False,
    "Section 2 - Change Data Feed": False,
    "Section 3 - UC Volumes": False,
    "Section 4 - Predictive Optimization": False,
    "Section 5 - AI Functions": False,
    "Section 6 - Time Travel": False,
}

# Auto-check what was completed
try:
    spark.sql(f"DESCRIBE DETAIL {TEAM_NAME}_heart_lc").collect()
    sections["Section 1 - Liquid Clustering"] = True
except:
    pass

try:
    df_cdf = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 0).table(f"{TEAM_NAME}_heart_cdf")
    if df_cdf.count() > 0:
        sections["Section 2 - Change Data Feed"] = True
except:
    pass

bonus_pts = 0
for section, completed in sections.items():
    status = "✅" if completed else "⬜"
    pts = 2 if completed else 0
    bonus_pts += pts
    print(f"  {status} {section} ({pts} pts)")

print(f"\nTotal Bonus Points: {bonus_pts}/12")
print("=" * 60)
