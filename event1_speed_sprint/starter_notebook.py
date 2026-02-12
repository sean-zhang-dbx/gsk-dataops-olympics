# Databricks notebook source
# MAGIC %md
# MAGIC # 🏃 Event 1: Speed Sprint
# MAGIC
# MAGIC ## Challenge: Fastest End-to-End Pipeline
# MAGIC **Time Limit: 15 minutes**
# MAGIC
# MAGIC ### Objective
# MAGIC Build a complete data pipeline that:
# MAGIC 1. ✅ Ingests CSV and JSON data into your workspace
# MAGIC 2. ✅ Creates Delta tables with proper schema
# MAGIC 3. ✅ Sets up data governance (table comments, column descriptions)
# MAGIC 4. ✅ Runs analytical queries
# MAGIC 5. ✅ Creates visualizations
# MAGIC
# MAGIC ### Scoring
# MAGIC | Place | Points |
# MAGIC |-------|--------|
# MAGIC | 1st   | 10 pts |
# MAGIC | 2nd   | 8 pts  |
# MAGIC | 3rd   | 6 pts  |
# MAGIC | 4th   | 5 pts  |
# MAGIC | 5th   | 4 pts  |
# MAGIC | 6th+  | 2 pts  |
# MAGIC
# MAGIC ### Dataset
# MAGIC - **Heart Disease UCI** (CSV format)
# MAGIC - **WHO Life Expectancy** (JSON format)
# MAGIC
# MAGIC > ⏱️ START YOUR TIMER NOW!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Ingest CSV Data (Heart Disease)
# MAGIC
# MAGIC **TODO:** Read the Heart Disease CSV file and display the first few rows.
# MAGIC
# MAGIC - File location: `/tmp/dataops_olympics/raw/heart_disease/heart.csv`
# MAGIC - Use Spark to read the CSV with headers

# COMMAND ----------

# TODO: Read the heart disease CSV file into a Spark DataFrame
# Hint: spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(...)

csv_path = "file:/tmp/dataops_olympics/raw/heart_disease/heart.csv"

df_heart = _____  # YOUR CODE HERE

# Display the data
display(df_heart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Ingest JSON Data (Life Expectancy)
# MAGIC
# MAGIC **TODO:** Read the Life Expectancy JSON file.
# MAGIC
# MAGIC - File location: `/tmp/dataops_olympics/raw/life_expectancy/life_expectancy_sample.json`

# COMMAND ----------

# TODO: Read the life expectancy JSON file into a Spark DataFrame
# Hint: spark.read.format("json").option("multiLine", "true").load(...)

json_path = "file:/tmp/dataops_olympics/raw/life_expectancy/life_expectancy_sample.json"

df_life = _____  # YOUR CODE HERE

# Display the data
display(df_life)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Delta Tables with Governance
# MAGIC
# MAGIC **TODO:** Save both DataFrames as Delta tables AND add table/column comments for governance.

# COMMAND ----------

# TODO: Write heart disease data as a Delta table
# Hint: df_heart.write.format("delta").mode("overwrite").saveAsTable("team_XX_heart_disease")
# Replace XX with your team number!

TEAM_NAME = "_____"  # e.g., "team_01"

df_heart.write.format("delta").mode("overwrite").saveAsTable(f"{TEAM_NAME}_heart_disease")

# COMMAND ----------

# TODO: Write life expectancy data as a Delta table

df_life.write.format("delta").mode("overwrite").saveAsTable(f"{TEAM_NAME}_life_expectancy")

# COMMAND ----------

# TODO: Add table comments for governance
# Hint: spark.sql("ALTER TABLE <table> SET TBLPROPERTIES ('comment' = 'your comment')")
# Hint: spark.sql("ALTER TABLE <table> ALTER COLUMN <col> COMMENT 'description'")

# Add table-level comment
spark.sql(f"""
    ALTER TABLE {TEAM_NAME}_heart_disease 
    SET TBLPROPERTIES ('comment' = '______')
""")  # YOUR COMMENT HERE

# Add at least 3 column-level comments
spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN age COMMENT '______'")
spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN chol COMMENT '______'")
spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN target COMMENT '______'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Analytical Queries
# MAGIC
# MAGIC **TODO:** Write SQL queries to answer these questions:
# MAGIC 1. What is the average age of patients WITH heart disease vs WITHOUT?
# MAGIC 2. What is the distribution of chest pain types?
# MAGIC 3. For the life expectancy data: Which are the top 5 countries by avg life expectancy?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query 1 - Average age by heart disease status
# MAGIC -- Hint: target = 1 means heart disease present
# MAGIC
# MAGIC SELECT 
# MAGIC     _____ 
# MAGIC FROM _____ 
# MAGIC GROUP BY _____

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query 2 - Distribution of chest pain types
# MAGIC -- cp: 0 = typical angina, 1 = atypical, 2 = non-anginal, 3 = asymptomatic
# MAGIC
# MAGIC SELECT 
# MAGIC     _____
# MAGIC FROM _____
# MAGIC GROUP BY _____
# MAGIC ORDER BY _____

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query 3 - Top 5 countries by average life expectancy
# MAGIC
# MAGIC SELECT 
# MAGIC     _____
# MAGIC FROM _____
# MAGIC GROUP BY _____
# MAGIC ORDER BY _____
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Visualizations
# MAGIC
# MAGIC **TODO:** Create at least 2 visualizations using the data.

# COMMAND ----------

# TODO: Visualization 1 - Heart disease distribution by age group
# Hint: Use plotly.express or the built-in Databricks visualization

import plotly.express as px

df_viz = spark.sql(f"""
    SELECT 
        CASE 
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END as age_group,
        target,
        COUNT(*) as count
    FROM {TEAM_NAME}_heart_disease
    GROUP BY 1, 2
    ORDER BY 1
""").toPandas()

# YOUR VISUALIZATION CODE HERE
# Hint: fig = px.bar(df_viz, x="age_group", y="count", color="target", barmode="group")

fig = _____  # YOUR CODE HERE
fig.show()

# COMMAND ----------

# TODO: Visualization 2 - Life expectancy trends
# Create a line chart showing life expectancy over time for select countries

df_viz2 = spark.sql(f"""
    SELECT country, year, life_expectancy
    FROM {TEAM_NAME}_life_expectancy
    WHERE country IN ('India', 'United States', 'Japan', 'Brazil', 'South Africa')
    ORDER BY country, year
""").toPandas()

# YOUR VISUALIZATION CODE HERE
# Hint: fig = px.line(df_viz2, x="year", y="life_expectancy", color="country")

fig2 = _____  # YOUR CODE HERE
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Completion Checklist
# MAGIC
# MAGIC Before calling the judge, verify:
# MAGIC - [ ] CSV data ingested into Spark DataFrame
# MAGIC - [ ] JSON data ingested into Spark DataFrame
# MAGIC - [ ] Both Delta tables created successfully
# MAGIC - [ ] Table and column comments added (governance)
# MAGIC - [ ] At least 3 analytical queries working
# MAGIC - [ ] At least 2 visualizations created
# MAGIC
# MAGIC > 🏁 **RAISE YOUR HAND when complete! Time will be recorded.**

# COMMAND ----------

# Validation cell - Run this to check your work!
import time

print("=" * 60)
print("SPEED SPRINT VALIDATION")
print("=" * 60)

score = 0
checks = []

# Check 1: Heart disease table exists
try:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {TEAM_NAME}_heart_disease").collect()[0].cnt
    checks.append(f"✅ Heart disease table: {cnt} rows")
    score += 1
except:
    checks.append("❌ Heart disease table not found")

# Check 2: Life expectancy table exists
try:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {TEAM_NAME}_life_expectancy").collect()[0].cnt
    checks.append(f"✅ Life expectancy table: {cnt} rows")
    score += 1
except:
    checks.append("❌ Life expectancy table not found")

# Check 3: Table comments
try:
    props = spark.sql(f"DESCRIBE TABLE EXTENDED {TEAM_NAME}_heart_disease").collect()
    has_comment = any("comment" in str(row).lower() for row in props)
    if has_comment:
        checks.append("✅ Table governance (comments) configured")
        score += 1
    else:
        checks.append("⚠️  Table comments not found")
except:
    checks.append("⚠️  Could not verify table comments")

# Check 4: Delta format
try:
    detail = spark.sql(f"DESCRIBE DETAIL {TEAM_NAME}_heart_disease").collect()[0]
    if detail.format == "delta":
        checks.append("✅ Delta format confirmed")
        score += 1
    else:
        checks.append("❌ Not in Delta format")
except:
    checks.append("⚠️  Could not verify format")

for c in checks:
    print(f"  {c}")

print(f"\nAutomated Score: {score}/4 checks passed")
print("=" * 60)
