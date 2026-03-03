# Databricks notebook source
# MAGIC %md
# MAGIC # Event 1: Data Engineering — Speed Sprint
# MAGIC
# MAGIC ## Challenge: Fastest End-to-End Pipeline
# MAGIC **Build Time: ~20 minutes**
# MAGIC
# MAGIC ### Objective
# MAGIC Build a complete data pipeline that:
# MAGIC 1. Ingests CSV and JSON healthcare data
# MAGIC 2. Creates governed Delta tables with Unity Catalog
# MAGIC 3. Adds table and column comments for governance
# MAGIC 4. Builds a basic medallion flow (Bronze -> Silver -> Gold)
# MAGIC
# MAGIC ### How You Win
# MAGIC **First team to complete all steps wins Gold!**
# MAGIC
# MAGIC ### Datasets
# MAGIC - **Heart Disease** (`/tmp/dataops_olympics/raw/heart_disease/heart.csv`) — 500 rows CSV
# MAGIC - **Life Expectancy** (`/tmp/dataops_olympics/raw/life_expectancy/life_expectancy_sample.json`) — 100 rows JSON
# MAGIC
# MAGIC > **Tip:** Use the Databricks Assistant (`Cmd+I`) to generate code from prompts!
# MAGIC
# MAGIC ### Databricks Assistant — Prompt Gallery
# MAGIC
# MAGIC Try these prompts in the Assistant panel to speed through the challenge:
# MAGIC
# MAGIC | Task | Prompt to Try |
# MAGIC |------|--------------|
# MAGIC | **Read CSV** | "Read the CSV at `file:/tmp/dataops_olympics/raw/heart_disease/heart.csv` with headers and infer schema into a DataFrame" |
# MAGIC | **Create Delta** | "Write this DataFrame as a Delta table called `team_XX_heart_disease`" |
# MAGIC | **Add governance** | "Add a table comment and column comments for age, chol, and target on the `team_XX_heart_disease` table" |
# MAGIC | **Silver table** | "Create a Silver table from `team_XX_heart_disease` that filters out rows with age outside 0-120 or blood pressure outside 50-300" |
# MAGIC | **Gold table** | "Create a Gold aggregation table from the Silver table grouped by age group and heart disease status with avg cholesterol" |
# MAGIC | **Explain code** | Select any cell and ask "Explain what this code does" |
# MAGIC
# MAGIC *The more context you give the Assistant (table names, column meanings, what output you want), the better the result.*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Ingest CSV Data (Heart Disease)
# MAGIC
# MAGIC **TODO:** Read the Heart Disease CSV file and create a Delta table.

# COMMAND ----------

# TODO: Read the heart disease CSV into a Spark DataFrame
# Hint: spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(...)

TEAM_NAME = "_____"  # e.g., "team_01"

csv_path = "file:/tmp/dataops_olympics/raw/heart_disease/heart.csv"

df_heart = _____  # YOUR CODE HERE

display(df_heart)

# COMMAND ----------

# TODO: Write as a Delta table
# Hint: df.write.format("delta").mode("overwrite").saveAsTable("table_name")

df_heart.write.format("delta").mode("overwrite").saveAsTable(f"{TEAM_NAME}_heart_disease")

print(f"Created table: {TEAM_NAME}_heart_disease")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Ingest JSON Data (Life Expectancy)
# MAGIC
# MAGIC **TODO:** Read the Life Expectancy JSON file and create a Delta table.

# COMMAND ----------

# TODO: Read the JSON file and write as Delta
json_path = "file:/tmp/dataops_olympics/raw/life_expectancy/life_expectancy_sample.json"

df_life = _____  # YOUR CODE HERE

df_life.write.format("delta").mode("overwrite").saveAsTable(f"{TEAM_NAME}_life_expectancy")

display(df_life)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Add Governance — Table & Column Comments
# MAGIC
# MAGIC **TODO:** Add descriptive comments to your tables and key columns.
# MAGIC This is how data teams document their assets in Unity Catalog.

# COMMAND ----------

# TODO: Add table-level comments
spark.sql(f"""
    ALTER TABLE {TEAM_NAME}_heart_disease
    SET TBLPROPERTIES ('comment' = '______')
""")

# TODO: Add column-level comments (at least 3)
spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN age COMMENT '______'")
spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN chol COMMENT '______'")
spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN target COMMENT '______'")

print("Governance comments added!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Build a Medallion Flow (Bronze -> Silver -> Gold)
# MAGIC
# MAGIC **TODO:** Transform raw data through medallion layers.
# MAGIC
# MAGIC - **Bronze:** Raw ingested data (already done above)
# MAGIC - **Silver:** Cleaned data — remove invalid records, standardize types
# MAGIC - **Gold:** Aggregated/business-ready data

# COMMAND ----------

# TODO: Create Silver table — clean the heart disease data
# Hints:
#   - Filter out rows where age < 0 or age > 120
#   - Filter out rows where trestbps (resting blood pressure) > 300 or < 50
#   - Cast columns to proper types if needed

df_silver = spark.sql(f"""
    SELECT *
    FROM {TEAM_NAME}_heart_disease
    WHERE _____
""")

df_silver.write.format("delta").mode("overwrite").saveAsTable(f"{TEAM_NAME}_heart_disease_silver")

print(f"Silver table: {df_silver.count()} rows (cleaned from raw)")

# COMMAND ----------

# TODO: Create Gold table — aggregated view for analytics
# Hint: Aggregate by age group and heart disease status

spark.sql(f"""
    CREATE OR REPLACE TABLE {TEAM_NAME}_heart_disease_gold AS
    SELECT
        CASE
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END as age_group,
        target as heart_disease,
        COUNT(*) as patient_count,
        AVG(chol) as avg_cholesterol,
        AVG(trestbps) as avg_blood_pressure,
        AVG(thalach) as avg_max_heart_rate
    FROM {TEAM_NAME}_heart_disease_silver
    GROUP BY 1, 2
    ORDER BY 1, 2
""")

display(spark.table(f"{TEAM_NAME}_heart_disease_gold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Your Pipeline
# MAGIC
# MAGIC Run this cell to validate your work and signal completion!

# COMMAND ----------

print("=" * 60)
print(f"  DATA ENGINEERING VALIDATION — {TEAM_NAME}")
print("=" * 60)

checks = []
score = 0

# Check 1: Heart disease table
try:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {TEAM_NAME}_heart_disease").collect()[0].cnt
    checks.append(f"  Bronze (heart_disease): {cnt} rows")
    score += 1
except:
    checks.append("  Bronze (heart_disease): MISSING")

# Check 2: Life expectancy table
try:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {TEAM_NAME}_life_expectancy").collect()[0].cnt
    checks.append(f"  Bronze (life_expectancy): {cnt} rows")
    score += 1
except:
    checks.append("  Bronze (life_expectancy): MISSING")

# Check 3: Silver table
try:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {TEAM_NAME}_heart_disease_silver").collect()[0].cnt
    checks.append(f"  Silver (cleaned): {cnt} rows")
    score += 1
except:
    checks.append("  Silver table: MISSING")

# Check 4: Gold table
try:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {TEAM_NAME}_heart_disease_gold").collect()[0].cnt
    checks.append(f"  Gold (aggregated): {cnt} rows")
    score += 1
except:
    checks.append("  Gold table: MISSING")

# Check 5: Governance
try:
    props = spark.sql(f"DESCRIBE TABLE EXTENDED {TEAM_NAME}_heart_disease").collect()
    has_comment = any("comment" in str(row).lower() and row[1] and len(str(row[1])) > 5 for row in props)
    if has_comment:
        checks.append("  Governance: Comments configured")
        score += 1
    else:
        checks.append("  Governance: Comments NOT found")
except:
    checks.append("  Governance: Could not verify")

for c in checks:
    print(c)

print(f"\nChecks passed: {score}/5")
if score == 5:
    print("\nALL CHECKS PASSED! RAISE YOUR HAND!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stretch Goals (Extra Credit)
# MAGIC
# MAGIC Finished early? Try these with the Databricks Assistant:
# MAGIC
# MAGIC 1. **Liquid Clustering** — Ask: *"Alter the Silver table to use liquid clustering on age and target columns"*
# MAGIC 2. **Change Data Feed** — Ask: *"Enable Change Data Feed on the Silver table and show me how to query the changes"*
# MAGIC 3. **Time Travel** — Ask: *"Show me how to query the previous version of my Silver table using Delta time travel"*
# MAGIC 4. **AI Functions** — Ask: *"Use ai_classify() in SQL to classify heart disease risk as 'High', 'Medium', 'Low' based on the cholesterol and age columns"*
# MAGIC 5. **Data Quality Dashboard** — Ask: *"Create a data quality report showing null counts, value ranges, and outliers for each column in the Silver table"*
